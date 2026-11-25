import json
import re
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from open_data_store.contracts.rules import conform
from open_data_store.contracts.standard.market import INSTRUMENT_IDENTIFIERS
from open_data_store.market._mapping import resolve, symbol_mappings
from open_data_store.market.asof import latest
from open_data_store.market.membership import membership_intervals, unresolved_members
from open_data_store.market.returns import resolve_prices
from open_data_store.sources import discover_sources, filter_sources, restricted_codes

from market_data.adapters.base import RAW_SYMBOL_SCHEMA
from market_data.adapters.file import FileHistory
from market_data.resolve.mapping import (
    AMBIGUOUS_SEVERAL_COMPOSITES,
    MANUAL_MAPPING_ENV,
    MANUAL_SCHEMA,
    MANUAL_SOURCE,
    OBSERVATION_INPUT_SCHEMA,
    REPORT_SCHEMA,
    SEC_SOURCE,
    SPAN_SCHEMA,
    UNRESOLVED_NO_COMPOSITE,
    UNRESOLVED_NO_SPAN,
    build_mapping,
    combine_spans,
    link_issuers,
    load_manual_mapping,
    spans_from_dates,
    spans_from_fetch_log,
)
from market_data.resolve.openfigi import figi_results
from market_data.tokens import token

LOAD = datetime(2024, 6, 3, 8, 0, tzinfo=UTC)
LATER = LOAD + timedelta(days=1)
END = date(2024, 5, 31)  # last date of every synthetic span
RECORDED = Path(__file__).parent / "fixtures" / "openfigi" / "mapping_response.json"
FIXTURE = Path(__file__).parent / "fixtures" / "history_file"
SALT = b"synthetic-test-salt-0123456789"
MOF_ID = re.compile(r"mof:[0-9a-f]{32}")

# Synthetic composite FIGIs (format only; no real security)
CX, CY, CV, C1, C2, CL, CD, CA, CB = (
    "BBG000SYNX01",
    "BBG000SYNY02",
    "BBG000SYNV03",
    "BBG000SYN104",
    "BBG000SYN205",
    "BBG000SYNL06",
    "BBG000SYND07",
    "BBG000SYNA08",
    "BBG000SYNB09",
)


def ob(src, sym, scheme, value, start=None, end=None, name=None) -> dict:
    return {
        "source_code": src,
        "source_symbol": sym,
        "id_scheme": scheme,
        "id_value": value,
        "valid_date": start,
        "valid_to": end,
        "name": name,
    }


def observations(*rows: dict) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=OBSERVATION_INPUT_SCHEMA)


def spans(*rows: tuple) -> pa.Table:
    keys = ("source_code", "source_symbol", "first_date", "last_date")
    return pa.Table.from_pylist([dict(zip(keys, r, strict=True)) for r in rows], schema=SPAN_SCHEMA)


def build(obs=None, figi=None, **kw):
    kw.setdefault("load_time", LOAD)
    return build_mapping(obs if obs is not None else observations(), figi, **kw)


def rows_of(table: pa.Table, **match) -> list[dict]:
    return [r for r in table.to_pylist() if all(r[k] == v for k, v in match.items())]


def mapping_rows(result, value: str) -> list[dict]:
    rows = rows_of(result.identifiers, id_scheme="source_symbol", id_value=value)
    return sorted(rows, key=lambda r: r["valid_date"])


def instrument_of(identifiers: pa.Table, composite: str) -> str:
    found = {r["instrument_id"] for r in rows_of(identifiers, id_scheme="composite_figi")
             if r["id_value"] == composite}  # fmt: skip
    assert len(found) == 1, found
    return found.pop()


def report(table: pa.Table) -> dict:
    return {(r["source_code"], r["source_symbol"]): r for r in table.to_pylist()}


# --- recycled ticker, venue change -------------------------------------------------------------


def recycled_inputs():
    obs = observations(
        ob("massive", "ABC", "composite_figi", CX, date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "ticker", "ABC", date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "composite_figi", CY, date(2012, 3, 1)),
        ob("massive", "ABC", "ticker", "ABC", date(2012, 3, 1)),
    )
    return obs, spans(("massive", "ABC", date(2005, 1, 3), END))


def test_recycled_ticker_two_instruments():
    obs, sp = recycled_inputs()
    result = build(obs, spans=sp)
    x, y = instrument_of(result.identifiers, CX), instrument_of(result.identifiers, CY)
    assert x != y and MOF_ID.fullmatch(x) and MOF_ID.fullmatch(y)
    rows = mapping_rows(result, "massive:ABC")
    assert [(r["instrument_id"], r["valid_date"], r["valid_to"]) for r in rows] == [
        (x, date(2005, 1, 3), date(2010, 7, 1)),
        (y, date(2012, 3, 1), END + timedelta(days=1)),  # span end + 1 day, half-open
    ]
    assert all(r["link_reason"] is None and r["source_code"] == "massive" for r in rows)
    # identifier observations land on the instrument whose window they fall in
    tickers = rows_of(result.identifiers, id_scheme="ticker")
    assert {(r["instrument_id"], r["valid_date"]) for r in tickers} == {
        (x, date(2005, 1, 3)),
        (y, date(2012, 3, 1)),
    }
    # the store resolves the symbol to each instrument on its own dates
    m = symbol_mappings(result.identifiers)
    assert resolve(m, "massive:ABC", date(2008, 5, 1)) == x
    assert resolve(m, "massive:ABC", date(2011, 5, 2)) is None
    assert resolve(m, "massive:ABC", date(2020, 5, 1)) == y
    # the gap between the two holders is mapped to nobody, and reported
    assert result.unresolved.to_pylist() == [
        {"source_code": "massive", "source_symbol": "ABC", "reason": "uncovered_span",
         "candidates": 2}
    ]  # fmt: skip
    assert result.ambiguous.num_rows == 0 and result.labels_left_out.num_rows == 0
    assert set(result.minted) == {x, y}


def test_venue_change_same_instrument():
    obs = observations(
        ob("private_history", "t_aaaa", "composite_figi", CV, date(2024, 5, 31)),
        ob("private_history", "t_bbbb", "composite_figi", CV, date(2024, 5, 31)),
        ob("tiingo", "VVV", "composite_figi", CV),
    )
    sp = spans(
        ("private_history", "t_aaaa", date(2001, 1, 2), date(2010, 12, 31)),
        ("private_history", "t_bbbb", date(2011, 1, 3), END),
        ("tiingo", "VVV", date(2001, 1, 2), END),
    )
    result = build(obs, spans=sp)
    v = instrument_of(result.identifiers, CV)
    a, b = (
        mapping_rows(result, "private_history:t_aaaa"),
        mapping_rows(result, "private_history:t_bbbb"),
    )
    assert [(r["instrument_id"], r["link_reason"]) for r in a + b] == [(v, "venue_change")] * 2
    assert (a[0]["valid_date"], a[0]["valid_to"]) == (date(2001, 1, 2), date(2011, 1, 1))
    # a single symbol of another source on the same instrument is a plain observation
    assert [(r["instrument_id"], r["link_reason"]) for r in mapping_rows(result, "tiingo:VVV")] == [
        (v, None)
    ]
    # one instrument, one id; the store resolves both tokens to it
    assert len(set(result.identifiers["instrument_id"].to_pylist())) == 1
    m = symbol_mappings(result.identifiers)
    assert resolve(m, "private_history:t_aaaa", date(2005, 1, 3)) == v
    assert resolve(m, "private_history:t_bbbb", date(2015, 1, 2)) == v


# --- ambiguous, unresolved ---------------------------------------------------------------------


def test_ambiguous_not_mapped():
    obs = observations(
        ob("massive", "AMB", "composite_figi", C1, date(2010, 1, 4)),
        ob("massive", "AMB", "composite_figi", C2, date(2015, 1, 2)),
        ob("massive", "AMB", "ticker", "AMB", date(2010, 1, 4)),
    )
    series = [("massive", "SYNA"), ("massive", "GONE"), ("massive", "SYNB"), ("tiingo", "SYNC")]
    figi = figi_results(series, json.loads(RECORDED.read_text()))
    sp = spans(*[(s, sym, date(2010, 1, 4), END) for s, sym in [("massive", "AMB"), *series]])
    result = build(obs, figi, spans=sp)
    amb = report(result.ambiguous)
    assert set(amb) == {("massive", "AMB"), ("massive", "SYNB")}
    assert amb[("massive", "AMB")] == {
        "source_code": "massive",
        "source_symbol": "AMB",
        "reason": AMBIGUOUS_SEVERAL_COMPOSITES,
        "candidates": 2,
    }
    assert amb[("massive", "SYNB")]["candidates"] == 2
    assert result.ambiguous.schema == REPORT_SCHEMA
    ids = result.identifiers
    for value in ("massive:AMB", "massive:SYNB"):
        assert not mapping_rows(result, value)
    assert not {C1, C2, "BBG000SYNC20", "BBG000SYNC21", "AMB"} & set(ids["id_value"].to_pylist())
    # SYNC: two FIGIs of one composite are one candidate, resolved
    assert len(mapping_rows(result, "tiingo:SYNC")) == 1


def test_unresolved_reported():
    obs = observations(
        ob("massive", "NODATE", "composite_figi", CX),  # no span, no date
        ob("massive", "NOFIGI", "ticker", "NOFIGI", date(2020, 1, 2)),
    )
    series = [("massive", "SYNA"), ("massive", "GONE"), ("massive", "SYNB"), ("tiingo", "SYNC")]
    figi = figi_results(series, json.loads(RECORDED.read_text()))
    sp = spans(("massive", "GONE", date(2010, 1, 4), END), ("tiingo", "ONLYSPAN", None, END))
    result = build(obs, figi, spans=sp)
    unr = report(result.unresolved)
    assert unr[("massive", "GONE")]["reason"] == UNRESOLVED_NO_COMPOSITE
    assert unr[("massive", "GONE")]["candidates"] == 0
    assert unr[("massive", "NOFIGI")]["reason"] == UNRESOLVED_NO_COMPOSITE
    assert unr[("tiingo", "ONLYSPAN")]["reason"] == UNRESOLVED_NO_COMPOSITE
    assert unr[("massive", "NODATE")]["reason"] == UNRESOLVED_NO_SPAN
    assert unr[("massive", "NODATE")]["candidates"] == 1
    # SYNA (no span) and SYNC (no span) have an OpenFIGI composite but nothing dates them
    assert unr[("massive", "SYNA")]["reason"] == UNRESOLVED_NO_SPAN
    assert result.unresolved.schema == REPORT_SCHEMA
    assert not mapping_rows(result, "massive:GONE")
    assert "NOFIGI" not in result.identifiers["id_value"].to_pylist()


def test_single_composite_takes_the_whole_span():
    # A file-adapter label is dated at the export; the span, not the label date, bounds it.
    obs = observations(ob("private_history", "t_cccc", "composite_figi", CX, date(2024, 5, 31)))
    sp = spans(("private_history", "t_cccc", date(1999, 1, 4), date(2003, 6, 30)))
    rows = mapping_rows(build(obs, spans=sp), "private_history:t_cccc")
    assert [(r["valid_date"], r["valid_to"]) for r in rows] == [
        (date(1999, 1, 4), date(2003, 7, 1))
    ]


def test_without_span_a_dated_observation_bounds_the_mapping():
    obs = observations(ob("massive", "DTD", "composite_figi", CX, date(2020, 1, 2)))
    rows = mapping_rows(build(obs), "massive:DTD")
    assert [(r["valid_date"], r["valid_to"]) for r in rows] == [(date(2020, 1, 2), None)]


# --- ids, existing, manual ---------------------------------------------------------------------


def test_ids_stable_across_runs():
    obs, sp = recycled_inputs()
    first = build(obs, spans=sp)
    x = instrument_of(first.identifiers, CX)
    more = pa.concat_tables(
        [obs, observations(ob("tiingo", "NEW", "composite_figi", CL, date(2020, 1, 2)))]
    )
    sp2 = pa.concat_tables([sp, spans(("tiingo", "NEW", date(2020, 1, 2), END))])
    second = build(more, spans=sp2, existing=first.identifiers, load_time=LATER)
    assert instrument_of(second.identifiers, CX) == x
    assert instrument_of(second.identifiers, CY) == instrument_of(first.identifiers, CY)
    assert second.minted == (instrument_of(second.identifiers, CL),)
    key = ["instrument_id", "id_scheme", "id_value", "valid_date", "valid_to", "source_code"]
    assert sorted(first.identifiers.select(key).to_pylist(), key=str) == sorted(
        rows_of(second.identifiers.select(key), source_code="massive"), key=str
    )
    # a third run with the published history (both runs) still re-mints nothing
    published = pa.concat_tables([first.identifiers, second.identifiers])
    third = build(more, spans=sp2, existing=published, load_time=LATER + timedelta(days=1))
    assert third.minted == ()


def test_existing_with_one_composite_on_two_instruments_refused():
    obs, sp = recycled_inputs()
    first = build(obs, spans=sp)
    broken = first.identifiers.to_pylist()
    twin = dict(next(r for r in broken if r["id_value"] == CX))
    twin["instrument_id"] = "mof:" + "0" * 32
    twin["source_code"] = "openfigi"
    existing = pa.Table.from_pylist([*broken, twin], schema=INSTRUMENT_IDENTIFIERS.schema)
    with pytest.raises(ValueError, match="composite FIGI on several instruments"):
        build(obs, spans=sp, existing=existing, load_time=LATER)


def write_manual(path: Path, rows: list[dict]) -> Path:
    cols = MANUAL_SCHEMA.names
    lines = [",".join(cols)]
    for r in rows:
        lines.append(",".join("" if r.get(c) is None else str(r[c]) for c in cols))
    path.write_text("\n".join(lines) + "\n")
    return path


def test_manual_mapping_wins(tmp_path):
    obs = observations(ob("massive", "HWX", "composite_figi", CA, date(2016, 11, 1)))
    sp = spans(("massive", "HWX", date(2005, 1, 3), END))
    first = build(obs, spans=sp)
    a = instrument_of(first.identifiers, CA)
    path = write_manual(
        tmp_path / "manual.csv",
        [
            {
                "source_code": "massive",
                "source_symbol": "HWX",
                "composite_figi": CB,
                "valid_date": "2005-01-03",
                "valid_to": "2016-11-01",
                "link_reason": "successor",
            },
            {
                "source_code": "massive",
                "source_symbol": "HWX",
                "instrument_id": a,
                "valid_date": "2016-11-01",
            },
        ],
    )
    assert load_manual_mapping(env={}) is None
    manual = load_manual_mapping(env={MANUAL_MAPPING_ENV: str(path)})
    second = build(obs, spans=sp, existing=first.identifiers, manual=manual, load_time=LATER)
    b = instrument_of(second.identifiers, CB)
    new = [r for r in mapping_rows(second, "massive:HWX") if r["valid_to"] != r["valid_date"]]
    assert [
        (r["instrument_id"], r["source_code"], r["link_reason"], r["valid_to"]) for r in new
    ] == [
        (b, MANUAL_SOURCE, "successor", date(2016, 11, 1)),
        (a, MANUAL_SOURCE, "manual", None),
    ]
    # the automatic row published by the first run is closed (empty window), not left to clash
    closed = [r for r in mapping_rows(second, "massive:HWX") if r["valid_to"] == r["valid_date"]]
    assert [(r["source_code"], r["instrument_id"]) for r in closed] == [("massive", a)]
    published = pa.concat_tables([first.identifiers, second.identifiers])
    m = symbol_mappings(published)
    assert resolve(m, "massive:HWX", date(2010, 1, 4)) == b
    assert resolve(m, "massive:HWX", date(2020, 1, 2)) == a
    # a run without the manual file keeps the published manual rows winning
    third = build(obs, spans=sp, existing=published, load_time=LATER + timedelta(days=1))
    live = [r for r in mapping_rows(third, "massive:HWX") if r["valid_to"] != r["valid_date"]]
    assert {r["source_code"] for r in live} == {MANUAL_SOURCE}
    m = symbol_mappings(pa.concat_tables([published, third.identifiers]))
    assert resolve(m, "massive:HWX", date(2010, 1, 4)) == b
    assert third.minted == ()


def test_manual_mapping_refuses_bad_rows_without_echoing(tmp_path):
    secret_symbol = "t_privatetoken0001"
    good = {
        "source_code": "private_history",
        "source_symbol": secret_symbol,
        "composite_figi": CB,
        "valid_date": "2005-01-03",
    }
    cases = [
        ({**good, "link_reason": "because"}, "link_reason"),
        ({**good, "instrument_id": "mof:" + "1" * 32}, "exactly one of"),
        ({**good, "composite_figi": None}, "exactly one of"),
        ({**good, "valid_date": None}, "valid_date"),
        ({**good, "composite_figi": "not-a-figi"}, "composite_figi"),
        ({**good, "valid_to": "2005-01-03"}, "valid_to"),
        ({**good, "source_code": "Nope!"}, "source_code"),
    ]
    for row, match in cases:
        path = write_manual(tmp_path / "m.csv", [good, row])
        with pytest.raises(ValueError, match=match) as exc:
            load_manual_mapping(path)
        assert secret_symbol not in str(exc.value) and "row 2" in str(exc.value)
    (tmp_path / "short.csv").write_text("source_code,source_symbol\nmassive,A\n")
    with pytest.raises(ValueError, match="missing column"):
        load_manual_mapping(tmp_path / "short.csv")
    # an unknown instrument_id is refused at build time (typo guard)
    path = write_manual(
        tmp_path / "m.csv", [{**good, "composite_figi": None, "instrument_id": "mof:" + "2" * 32}]
    )
    with pytest.raises(ValueError, match="unknown instrument_id") as exc:
        build(manual=load_manual_mapping(path))
    assert secret_symbol not in str(exc.value)
    # one series, two targets, overlapping dates: refused before the store would raise
    path = write_manual(tmp_path / "m.csv", [good, {**good, "composite_figi": CA}])
    with pytest.raises(ValueError, match="rows 1 and 2: overlapping") as exc:
        build(manual=load_manual_mapping(path))
    assert secret_symbol not in str(exc.value)


# --- sources and labels ------------------------------------------------------------------------


def test_private_labels_keep_source_private_history():
    obs = observations(
        ob("private_history", "t_dddd", "composite_figi", CX, date(2024, 5, 31)),
        ob("private_history", "t_dddd", "export_member_id", "SYN UN Equity", date(2024, 5, 31)),
        ob("private_history", "t_dddd", "export_name", "Synthetic Co", date(2024, 5, 31)),
        ob("private_history", "t_dddd", "ticker", "SYN", date(2024, 5, 31)),
    )
    sp = spans(("private_history", "t_dddd", date(2001, 1, 2), END))
    ids = build(obs, spans=sp).identifiers
    assert {r["source_code"] for r in ids.to_pylist()} == {"private_history"}
    # the default read (restricted sources denied) sees none of the labels, nor the mapping
    registry = discover_sources()
    public = filter_sources(
        ids,
        allowed_restricted=frozenset(),
        restricted=restricted_codes(registry),
        strict=True,
        registered=frozenset(registry),
    )
    assert public.num_rows == 0


def test_openfigi_rows_dated_at_load_under_the_most_restrictive_source():
    series = [("massive", "SYNA"), ("massive", "GONE"), ("massive", "SYNB"), ("tiingo", "SYNC")]
    figi = figi_results(series, json.loads(RECORDED.read_text()))
    sp = spans(
        ("massive", "SYNA", date(2010, 1, 4), END), ("tiingo", "SYNC", date(2012, 1, 3), END)
    )
    for restricted, source in ((None, "massive"), (frozenset(), "openfigi")):
        result = build(None, figi, spans=sp, restricted=restricted)
        syna = instrument_of(result.identifiers, "BBG000SYNC10")
        rows = [r for r in result.identifiers.to_pylist() if r["instrument_id"] == syna]
        labels = {(r["id_scheme"], r["id_value"], r["source_code"]) for r in rows}
        assert labels == {
            ("source_symbol", "massive:SYNA", "massive"),
            ("composite_figi", "BBG000SYNC10", source),
            ("figi", "BBG000SYNC10", source),
            ("share_class_figi", "BBG001SYNS10", source),
            ("ticker", "SYNA", source),
        }
        # present-time answers: dated at the load, open-ended, never at the span start
        derived = [r for r in rows if r["id_scheme"] != "source_symbol"]
        assert {(r["valid_date"], r["valid_to"]) for r in derived} == {(LOAD.date(), None)}
    sync = instrument_of(result.identifiers, "BBG000SYNC30")
    figis = rows_of(result.identifiers, instrument_id=sync, id_scheme="figi")
    assert {r["id_value"] for r in figis} == {"BBG000SYNC30", "BBG000SYNX31"}


def test_observation_composite_wins_over_openfigi():
    figi = figi_results([("massive", "SYNA")], [json.loads(RECORDED.read_text())[0]])
    obs = observations(ob("massive", "SYNA", "composite_figi", CX, date(2010, 1, 4)))
    result = build(obs, figi, spans=spans(("massive", "SYNA", date(2010, 1, 4), END)))
    assert [r["instrument_id"] for r in mapping_rows(result, "massive:SYNA")] == [
        instrument_of(result.identifiers, CX)
    ]
    assert "openfigi" not in result.identifiers["source_code"].to_pylist()


def test_unknown_observation_scheme_refused():
    obs = observations(ob("massive", "A", "source_symbol", "massive:A", date(2020, 1, 2)))
    with pytest.raises(ValueError, match="id_scheme"):
        build(obs)


def test_outputs_conform_and_known_at():
    obs, sp = recycled_inputs()
    result = build(obs, spans=sp)
    out = conform(INSTRUMENT_IDENTIFIERS, result.identifiers, now=LATER)
    assert out.num_rows == result.identifiers.num_rows
    assert set(result.identifiers["known_at"].to_pylist()) == {LOAD}
    assert set(result.identifiers["known_at_kind"].to_pylist()) == {"collected"}
    assert set(result.identifiers["known_at_basis"].to_pylist()) == {"load_time"}
    with pytest.raises(ValueError, match="timezone"):
        build(obs, spans=sp, load_time=datetime(2024, 6, 3))
    empty = build()
    assert empty.identifiers.num_rows == 0
    assert empty.identifiers.schema == INSTRUMENT_IDENTIFIERS.schema


# --- issuer link -------------------------------------------------------------------------------


def issuer_inputs():
    obs = observations(
        ob("private_history", "t_live", "composite_figi", CL, date(2024, 5, 31)),
        ob("private_history", "t_live", "ticker", "LIV", date(2024, 5, 31)),
        ob("private_history", "t_dead", "composite_figi", CD, date(2024, 5, 31)),
        # a dead member's last ticker, since recycled by a current company
        ob("private_history", "t_dead", "ticker", "ZZZ", date(2024, 5, 31)),
        ob("massive", "MOVED", "composite_figi", CX, date(2015, 1, 2)),
        ob("massive", "MOVED", "ticker", "OLDT", date(2015, 1, 2), date(2020, 1, 2)),
        ob("massive", "MOVED", "ticker", "NEWT", date(2020, 1, 2)),
    )
    sp = spans(
        ("private_history", "t_live", date(2001, 1, 2), END),
        ("private_history", "t_dead", date(2001, 1, 2), date(2015, 6, 30)),
        ("massive", "MOVED", date(2015, 1, 2), date(2024, 5, 30)),
    )
    sec = pa.table(
        {
            "cik": [320193, 1234, 777, 888],
            "ticker": ["LIV", "ZZZ", "OLDT", "NEWT"],
            "title": ["Synthetic Live", "Synthetic Z", "Synthetic Old", "Synthetic New"],
        }
    )
    return obs, sp, sec


def test_cik_linked_for_current_ticker_only():
    obs, sp, sec = issuer_inputs()
    ids = build(obs, spans=sp).identifiers
    linked = link_issuers(ids, sec, load_time=LATER, as_of=END)
    ciks = rows_of(linked, id_scheme="cik")
    live, moved = instrument_of(ids, CL), instrument_of(ids, CX)
    assert {(r["instrument_id"], r["id_value"]) for r in ciks} == {
        (live, "0000320193"),
        (moved, "0000000888"),  # its current ticker, not the closed one
    }
    assert all(r["source_code"] == SEC_SOURCE and r["known_at"] == LATER for r in ciks)
    assert instrument_of(ids, CD) not in {r["instrument_id"] for r in ciks}  # dead: no link
    # input rows kept; the result conforms
    assert linked.num_rows == ids.num_rows + 2
    conform(INSTRUMENT_IDENTIFIERS, linked, now=LATER)


def test_cik_not_linked_when_two_current_instruments_claim_a_ticker():
    obs, sp, sec = issuer_inputs()
    obs = pa.concat_tables(
        [
            obs,
            observations(
                ob("tiingo", "LV2", "composite_figi", CY), ob("tiingo", "LV2", "ticker", "LIV")
            ),
        ]
    )
    sp = pa.concat_tables([sp, spans(("tiingo", "LV2", date(2010, 1, 4), END))])
    ids = build(obs, spans=sp).identifiers
    linked = link_issuers(ids, sec, load_time=LATER, as_of=END)
    assert {r["id_value"] for r in rows_of(linked, id_scheme="cik")} == {"0000000888"}


def test_cik_staleness_is_a_parameter():
    obs, sp, sec = issuer_inputs()
    ids = build(obs, spans=sp).identifiers
    linked = link_issuers(ids, sec, load_time=LATER, as_of=date(2015, 7, 1), max_staleness_days=5)
    dead = instrument_of(ids, CD)
    assert dead in {r["instrument_id"] for r in rows_of(linked, id_scheme="cik")}


# --- spans and the end-to-end path ------------------------------------------------------------


def test_spans_from_fetch_log_and_dates():
    from open_data_store.contracts.standard.market import FETCH_LOG

    base = {
        "fetch_id": "f",
        "row_count": 0,
        "archive_ref": None,
        "known_at_basis": "fetch_time",
        "valid_date": date(2024, 6, 1),
        "known_at": LOAD,
        "known_at_kind": "collected",
        "source_doc_id": None,
    }
    log = pa.Table.from_pylist(
        [
            {**base, "source_code": "s", "source_symbol": "A", "requested_from": date(2001, 1, 2),
             "requested_to": date(2024, 6, 1), "returned_from": date(2001, 1, 3),
             "returned_to": date(2010, 5, 3), "status": "ok"},
            {**base, "source_code": "s", "source_symbol": "B", "requested_from": date(2005, 1, 3),
             "requested_to": date(2024, 6, 1), "returned_from": None, "returned_to": None,
             "status": "empty"},
            {**base, "source_code": "s", "source_symbol": "A", "fetch_id": "g",
             "requested_from": None, "requested_to": None, "returned_from": date(2010, 5, 4),
             "returned_to": date(2011, 1, 3), "status": "ok"},
        ],
        schema=FETCH_LOG.schema,
    )  # fmt: skip
    got = {(r["source_symbol"]): (r["first_date"], r["last_date"])
           for r in spans_from_fetch_log(log).to_pylist()}  # fmt: skip
    assert got == {
        "A": (date(2001, 1, 2), date(2011, 1, 3)),
        "B": (date(2005, 1, 3), date(2024, 6, 1)),
    }
    dated = pa.table(
        {
            "source_code": ["s", "s"],
            "source_symbol": ["A", "A"],
            "valid_date": [date(2000, 6, 30), date(2012, 1, 31)],
        }
    )
    merged = combine_spans(spans_from_fetch_log(log), spans_from_dates(dated))
    got = {r["source_symbol"]: (r["first_date"], r["last_date"]) for r in merged.to_pylist()}
    assert got["A"] == (date(2000, 6, 30), date(2012, 1, 31))
    assert merged.schema == SPAN_SCHEMA


def test_end_to_end_file_adapter_resolves_in_the_store(tmp_path):
    src = tmp_path / "export"
    shutil.copytree(FIXTURE, src)
    load = datetime(2020, 4, 3, 9, 0, tzinfo=UTC)
    fh = FileHistory(src, salt=SALT, max_non_session_share=0.5)
    loaded = fh.build_all(load)
    sp = combine_spans(
        spans_from_fetch_log(loaded.fetch_log), spans_from_dates(loaded.index_snapshots)
    )
    obs = loaded.observations.select(OBSERVATION_INPUT_SCHEMA.names)
    result = build_mapping(obs, None, load_time=load, spans=sp)
    ids = conform(INSTRUMENT_IDENTIFIERS, result.identifiers, now=load)

    def tok(member: str) -> str:
        return token("private_history", member, SALT)

    # EEE has no composite FIGI: reported, never dropped
    assert report(result.unresolved) == {
        ("private_history", tok("EEE ZZ Equity")): {
            "source_code": "private_history",
            "source_symbol": tok("EEE ZZ Equity"),
            "reason": UNRESOLVED_NO_COMPOSITE,
            "candidates": 0,
        }
    }
    by_member = {m: instrument_of(ids, f"BBG000FAKC0{i}") for i, m in
                 [(1, "AAA"), (2, "BBB"), (3, "CCC"), (4, "DDD"), (6, "FFF")]}  # fmt: skip

    # membership: every resolvable member of IDX1 resolves, EEE stays unresolved
    intervals = membership_intervals(loaded.index_snapshots, ids, index_code="IDX1", as_of=load)
    assert set(intervals["instrument_id"].to_pylist()) == {
        by_member["AAA"],
        by_member["BBB"],
        by_member["DDD"],
        by_member["FFF"],
    }
    bbb = rows_of(intervals, instrument_id=by_member["BBB"])
    assert [(r["valid_from"], r["valid_to"]) for r in bbb] == [
        (date(2020, 1, 31), date(2020, 2, 29))
    ]
    unres = unresolved_members(loaded.index_snapshots, ids, index_code="IDX1", as_of=load)
    assert unres["source_symbol"].to_pylist() == [tok("EEE ZZ Equity")]
    idx2 = membership_intervals(loaded.index_snapshots, ids, index_code="IDX2", as_of=load)
    assert by_member["CCC"] in idx2["instrument_id"].to_pylist()

    # prices: every loaded row resolves to its member's instrument
    resolved = resolve_prices(loaded.prices, ids, as_of=load, precedence=())
    assert resolved.num_rows == loaded.prices.num_rows
    member_of = {tok(m + suffix): m for m, suffix in
                 [("AAA", " UN Equity"), ("BBB", " UW Equity"), ("DDD", " UQ Equity"),
                  ("FFF", " UN Equity")]}  # fmt: skip
    expected = sorted(
        (by_member[member_of[s]], d)
        for s, d in zip(
            loaded.prices["source_symbol"].to_pylist(),
            loaded.prices["valid_date"].to_pylist(),
            strict=True,
        )
    )
    assert sorted(zip(resolved["instrument_id"].to_pylist(),
                      resolved["session_date"].to_pylist(), strict=True)) == expected  # fmt: skip
    # labels stay under the restricted source; a mapping row's id_value is a token, not a label
    labels = rows_of(ids, id_scheme="export_member_id")
    assert {r["source_code"] for r in labels} == {"private_history"}
    # every mapped member keeps its labels, the dead one included (labels dated at the export)
    assert {r["instrument_id"] for r in labels} == set(by_member.values())
    bbb_ticker = rows_of(ids, instrument_id=by_member["BBB"], id_scheme="ticker")
    assert [(r["id_value"], r["valid_date"]) for r in bbb_ticker] == [("BBBQ", date(2020, 4, 2))]
    symbols = pc.equal(ids["id_scheme"], "source_symbol")
    assert all(
        v.startswith("private_history:t_") for v in ids.filter(symbols)["id_value"].to_pylist()
    )


def test_observation_input_schema_extends_raw_symbol_schema():
    assert OBSERVATION_INPUT_SCHEMA.names[: len(RAW_SYMBOL_SCHEMA)] == RAW_SYMBOL_SCHEMA.names
    assert "source_code" in OBSERVATION_INPUT_SCHEMA.names


# --- review fixes: no splicing, one-sided recycling, OpenFIGI age, private-derived rows ---------


def manual_table(*rows: dict) -> pa.Table:
    base = {"instrument_id": None, "composite_figi": None, "valid_to": None, "link_reason": None}
    return pa.Table.from_pylist([{**base, **r} for r in rows], schema=MANUAL_SCHEMA)


def test_composite_observation_never_lands_on_another_instrument():
    # the manual says HWX is CY over its whole span; its CX observation names another company
    obs = observations(
        ob("massive", "HWX", "composite_figi", CX, date(2016, 11, 1)),
        ob("tiingo", "REAL", "composite_figi", CX, date(2016, 11, 1)),
    )
    sp = spans(
        ("massive", "HWX", date(2005, 1, 3), END), ("tiingo", "REAL", date(2016, 11, 1), END)
    )
    man = manual_table(
        {"source_code": "massive", "source_symbol": "HWX", "composite_figi": CY,
         "valid_date": date(2005, 1, 3)}
    )  # fmt: skip
    first = build(obs, spans=sp, manual=man)
    x, y = instrument_of(first.identifiers, CX), instrument_of(first.identifiers, CY)
    assert x != y
    assert {r["instrument_id"] for r in rows_of(first.identifiers, id_value=CX)} == {x}
    assert first.labels_left_out.to_pylist() == [
        {"source_code": "massive", "source_symbol": "HWX", "reason": "composite_not_mapped",
         "rows": 1}
    ]  # fmt: skip
    published = first.identifiers
    for k, again in enumerate((None, man, man.slice(0, 0)), start=1):
        nxt = build(obs, spans=sp, existing=published, manual=again,
                    load_time=LOAD + timedelta(days=k))  # fmt: skip
        assert nxt.minted == ()
        assert instrument_of(nxt.identifiers, CX) == x
        published = pa.concat_tables([published, nxt.identifiers])
        m = symbol_mappings(published)
        expected = x if again is not None and again.num_rows == 0 else y
        assert resolve(m, "massive:HWX", date(2020, 1, 2)) == expected


def test_composite_outside_the_span_is_left_out_and_never_minted():
    obs = observations(
        ob("massive", "ABC", "composite_figi", CX, date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "composite_figi", CY, date(2012, 3, 1)),
    )
    sp = spans(("massive", "ABC", date(2011, 1, 3), END))
    first = build(obs, spans=sp)
    assert len(first.minted) == 1 and CX not in first.identifiers["id_value"].to_pylist()
    assert [r["reason"] for r in first.labels_left_out.to_pylist()] == ["composite_not_mapped"]
    old = observations(ob("tiingo", "OLDX", "composite_figi", CX, date(2005, 1, 3)))
    second = build(
        pa.concat_tables([obs, old]),
        spans=pa.concat_tables(
            [sp, spans(("tiingo", "OLDX", date(2005, 1, 3), date(2010, 6, 30)))]
        ),
        existing=first.identifiers,
        load_time=LATER,
    )
    assert instrument_of(second.identifiers, CX) != instrument_of(second.identifiers, CY)


def test_recycled_symbol_with_a_composite_on_one_side_only():
    sp = spans(("massive", "ABC", date(2005, 1, 3), END))
    # composite known for the first holder only
    first_only = observations(
        ob("massive", "ABC", "composite_figi", CX, date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "ticker", "ABC", date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "ticker", "ABC", date(2012, 3, 1)),
    )
    result = build(first_only, spans=sp)
    rows = mapping_rows(result, "massive:ABC")
    assert [(r["valid_date"], r["valid_to"]) for r in rows] == [
        (date(2005, 1, 3), date(2010, 7, 1))
    ]
    assert report(result.unresolved)[("massive", "ABC")]["reason"] == "uncovered_span"
    assert resolve(symbol_mappings(result.identifiers), "massive:ABC", date(2020, 1, 2)) is None
    # the second holder's ticker is not given to the first holder: counted, left out
    assert result.labels_left_out.to_pylist() == [
        {"source_code": "massive", "source_symbol": "ABC", "reason": "outside_windows", "rows": 1}
    ]
    # composite known for the second holder only
    second_only = observations(
        ob("massive", "ABC", "ticker", "ABC", date(2005, 1, 3), date(2010, 7, 1)),
        ob("massive", "ABC", "composite_figi", CY, date(2012, 3, 1)),
    )
    result = build(second_only, spans=sp)
    rows = mapping_rows(result, "massive:ABC")
    assert [(r["valid_date"], r["valid_to"]) for r in rows] == [
        (date(2012, 3, 1), END + timedelta(days=1))
    ]
    assert report(result.unresolved)[("massive", "ABC")]["reason"] == "uncovered_span"


def test_label_overlapping_several_windows_is_reported():
    obs, sp = recycled_inputs()
    obs = pa.concat_tables(
        [obs, observations(ob("massive", "ABC", "isin", "XS0000000AA7", date(2009, 1, 2)))]
    )
    result = build(obs, spans=sp)
    assert result.labels_left_out.to_pylist() == [
        {"source_code": "massive", "source_symbol": "ABC", "reason": "several_windows", "rows": 1}
    ]


def openfigi_one(series: list[tuple[str, str]], composite: str = CX, ticker: str = "ZZZ"):
    entry = {"figi": composite, "compositeFIGI": composite, "ticker": ticker, "exchCode": "US"}
    return figi_results(series, [{"data": [entry]} for _ in series])


def test_openfigi_composite_shared_or_stale_is_not_mapped():
    both = [("private_history", "t_dead"), ("private_history", "t_live")]
    sp = spans(
        ("private_history", "t_dead", date(2001, 1, 2), date(2008, 9, 15)),
        ("private_history", "t_live", date(2015, 1, 2), END),
    )
    result = build(None, openfigi_one(both), spans=sp)
    assert {k: r["reason"] for k, r in report(result.ambiguous).items()} == dict.fromkeys(
        both, "openfigi_shared"
    )
    assert result.identifiers.num_rows == 0
    # alone, the dead series is too old for a present-time answer; the live one maps
    for sym, reason in (("t_dead", "openfigi_stale"), ("t_live", None)):
        result = build(None, openfigi_one([("private_history", sym)]), spans=sp)
        got = report(result.ambiguous).get(("private_history", sym))
        assert (got["reason"] if got else None) == reason
    # the age is a parameter
    result = build(
        None, openfigi_one([("private_history", "t_dead")]), spans=sp, max_openfigi_age_days=10**5
    )
    assert mapping_rows(result, "private_history:t_dead")


def test_openfigi_rows_of_a_private_token_stay_private():
    obs = observations(ob("private_history", "t_live", "ticker", "LIVE", END))
    sp = spans(("private_history", "t_live", date(2015, 1, 2), END))
    ids = build(obs, openfigi_one([("private_history", "t_live")], ticker="LIVE"), spans=sp)
    ids = ids.identifiers
    derived = [r for r in ids.to_pylist() if r["id_scheme"] in ("figi", "composite_figi")]
    assert derived and {r["source_code"] for r in derived} == {"private_history"}
    assert "openfigi" not in ids["source_code"].to_pylist()
    registry = discover_sources()
    public = filter_sources(
        ids,
        allowed_restricted=frozenset(),
        restricted=restricted_codes(registry),
        strict=True,
        registered=frozenset(registry),
    )
    assert public.num_rows == 0


def test_load_time_must_follow_existing():
    obs, sp = recycled_inputs()
    first = build(obs, spans=sp)
    for when in (LOAD, LOAD - timedelta(seconds=1)):
        with pytest.raises(ValueError, match="after every known_at"):
            build(obs, spans=sp, existing=first.identifiers, load_time=when)


def test_run_without_spans_keeps_the_published_window():
    obs = observations(ob("massive", "Q", "composite_figi", CX, date(2010, 1, 4)))
    first = build(obs, spans=spans(("massive", "Q", date(2010, 1, 4), date(2015, 6, 30))))
    second = build(obs, existing=first.identifiers, load_time=LATER)
    assert not mapping_rows(second, "massive:Q")  # nothing rewritten, nothing closed
    m = symbol_mappings(pa.concat_tables([first.identifiers, second.identifiers]))
    assert resolve(m, "massive:Q", date(2016, 1, 4)) is None
    assert resolve(m, "massive:Q", date(2012, 1, 3)) == instrument_of(first.identifiers, CX)
    # its labels still land on the published instrument
    assert instrument_of(second.identifiers, CX) == instrument_of(first.identifiers, CX)


def test_link_issuers_needs_as_of_and_is_idempotent():
    obs, sp, sec = issuer_inputs()
    ids = build(obs, spans=sp).identifiers
    with pytest.raises(TypeError):
        link_issuers(ids, sec, load_time=LATER)  # as_of is required
    a = link_issuers(ids, sec, load_time=LATER, as_of=END)
    # fed back at the same load_time, or later: nothing new, no duplicate key
    assert link_issuers(a, sec, load_time=LATER, as_of=END).num_rows == a.num_rows
    l3 = LATER + timedelta(days=1)
    b = link_issuers(a, sec, load_time=l3, as_of=END)
    assert b.num_rows == a.num_rows
    # a changed CIK closes the published row and adds the new one
    moved = pa.table({"cik": [320193, 999], "ticker": ["LIV", "NEWT"]})
    l4 = l3 + timedelta(days=1)
    c = link_issuers(b, moved, load_time=l4, as_of=END)
    conform(INSTRUMENT_IDENTIFIERS, c, now=l4)
    now = latest(c, INSTRUMENT_IDENTIFIERS.natural_key)
    live = {
        r["id_value"]
        for r in rows_of(now, id_scheme="cik", instrument_id=instrument_of(ids, CX))
        if r["valid_to"] is None
    }
    assert live == {"0000000999"}
    with pytest.raises(ValueError, match="after the published cik row"):
        link_issuers(c, sec, load_time=l4, as_of=END)

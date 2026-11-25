import json
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from open_data_store.contracts.rules import conform
from open_data_store.contracts.standard.market import CORPORATE_ACTIONS, FETCH_LOG, PRICES
from open_data_store.market.asof import asof
from open_data_store.writer.batch import content_hash

from market_data.adapters.base import (
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    RAW_SYMBOL_SCHEMA,
    WHOLE_MARKET_SYMBOL,
    FetchResult,
)
from market_data.archive import RawArchive
from market_data.build import (
    DROP_INTRADAY_PARTIAL,
    DROP_NON_SESSION,
    build_actions,
    build_fetch_log,
    build_prices,
)

SRC = "synth"
PRICE_KEY = ("source_code", "source_symbol", "valid_date")


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class FakeAdapter:
    """Parses JSON payloads: a list of row dicts (dates as ISO strings)."""

    source_code = SRC
    info = None

    def __init__(self):
        self.parsed: list[FetchResult] = []

    def fetch(self, kind, symbols, start, end):  # pragma: no cover - never called by the builder
        raise NotImplementedError

    def parse(self, result: FetchResult) -> pa.Table:
        self.parsed.append(result)
        schema = {
            "prices": RAW_PRICE_SCHEMA,
            "actions": RAW_ACTION_SCHEMA,
            "symbols": RAW_SYMBOL_SCHEMA,
        }[result.kind]
        rows = json.loads(result.payload)
        for row in rows:
            for k, v in row.items():
                if k.endswith("date") and isinstance(v, str):
                    row[k] = date.fromisoformat(v)
            if result.kind != "symbols":
                row.setdefault("source_code", SRC)
        return pa.Table.from_pylist(rows, schema=schema)


def bar(sym, day, close, **kw):
    row = {"source_symbol": sym, "valid_date": day, "close": close, "currency": "USD"}
    row.update(kw)
    return row


def put(arc, rows, at, symbol="AAA", kind="prices", status="ok", frm=None, to=None):
    return arc.put(
        FetchResult(
            source_code=SRC,
            source_symbol=symbol,
            kind=kind,
            requested_from=frm,
            requested_to=to,
            fetched_at=at,
            status=status,
            payload=json.dumps(rows).encode(),
            content_type="application/json",
        )
    )


@pytest.fixture
def arc(tmp_path):
    return RawArchive(tmp_path / "arc")


def rows_of(table):
    return table.to_pylist()


# --- prices ------------------------------------------------------------------------------------


def test_first_value_known_at_session_close(arc):
    # Regular winter day, an early close (day after Thanksgiving) and a summer-time day.
    put(
        arc,
        [
            bar("AAA", "2026-01-05", 10.0, volume=100),
            bar("AAA", "2025-11-28", 9.0),
            bar("AAA", "2026-03-09", 11.0),
        ],
        utc(2026, 3, 10, 2),
    )
    res = build_prices(arc, FakeAdapter(), ["AAA"])
    got = {r["valid_date"]: r for r in rows_of(res.table)}
    assert got[date(2026, 1, 5)]["known_at"] == utc(2026, 1, 5, 21)
    assert got[date(2025, 11, 28)]["known_at"] == utc(2025, 11, 28, 18)
    assert got[date(2026, 3, 9)]["known_at"] == utc(2026, 3, 9, 20)
    for r in got.values():
        assert r["known_at_kind"] == "published"
        assert r["known_at_basis"] == "session_close"
        assert r["price_basis"] == "raw"
        assert r["source_doc_id"] is None
        assert r["source_code"] == SRC
    assert res.table.schema.equals(PRICES.schema)


def test_correction_gets_fetch_time(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 6, 2))
    fix_at = utc(2026, 2, 10, 15, 30, 0, 123456)
    put(arc, [bar("AAA", "2026-01-05", 10.5)], fix_at)
    t = build_prices(arc, FakeAdapter(), ["AAA"]).table
    assert [
        (r["close"], r["known_at"], r["known_at_kind"], r["known_at_basis"]) for r in rows_of(t)
    ] == [
        (10.0, utc(2026, 1, 5, 21), "published", "session_close"),
        (10.5, fix_at, "collected", "fetch_time"),
    ]
    # As-of reads before the correction see the old value, after it the new one.
    before = asof(t, PRICE_KEY, fix_at - timedelta(microseconds=1))
    after = asof(t, PRICE_KEY, fix_at)
    assert before["close"].to_pylist() == [10.0]
    assert after["close"].to_pylist() == [10.5]
    assert asof(t, PRICE_KEY, utc(2026, 1, 5, 20, 59)).num_rows == 0


def test_rebuild_is_identical(arc, tmp_path):
    put(arc, [bar("AAA", "2026-01-05", 10.0), bar("AAA", "2026-01-06", 10.2)], utc(2026, 1, 7, 1))
    put(arc, [bar("BBB", "2026-01-05", 20.0)], utc(2026, 1, 7, 1), symbol="BBB")
    put(
        arc,
        [bar("AAA", "2026-01-06", 10.3), bar("BBB", "2026-01-06", 21.0)],
        utc(2026, 1, 8, 1),
        symbol=WHOLE_MARKET_SYMBOL,
    )
    put(
        arc,
        [
            {
                "source_symbol": "AAA",
                "valid_date": "2026-01-06",
                "action_type": "split",
                "action_seq": 0,
                "ratio_old": 1.0,
                "ratio_new": 2.0,
            }
        ],
        utc(2026, 1, 2, 1),
        kind="actions",
    )
    first = build_prices(arc, FakeAdapter(), None).table
    again = build_prices(RawArchive(tmp_path / "arc"), FakeAdapter(), None).table
    assert first.equals(again)
    assert content_hash(first) == content_hash(again)
    assert first.num_rows == 5
    a1 = build_actions(arc, FakeAdapter(), None).table
    a2 = build_actions(RawArchive(tmp_path / "arc"), FakeAdapter(), None).table
    assert a1.equals(a2) and content_hash(a1) == content_hash(a2)
    f1 = build_fetch_log(arc, FakeAdapter(), SRC)
    f2 = build_fetch_log(RawArchive(tmp_path / "arc"), FakeAdapter(), SRC)
    assert f1.equals(f2) and content_hash(f1) == content_hash(f2)


def test_unchanged_value_not_reemitted(arc):
    for day in (6, 7, 8):
        put(arc, [bar("AAA", "2026-01-05", 10.0, volume=None)], utc(2026, 1, day, 1))
    t = build_prices(arc, FakeAdapter(), ["AAA"]).table
    assert t.num_rows == 1
    assert t["known_at"].to_pylist() == [utc(2026, 1, 5, 21)]


def test_value_reverting_is_a_change(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 6, 1))
    put(arc, [bar("AAA", "2026-01-05", 11.0)], utc(2026, 1, 7, 1))
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 8, 1))
    t = build_prices(arc, FakeAdapter(), ["AAA"]).table
    assert [(r["close"], r["known_at"]) for r in rows_of(t)] == [
        (10.0, utc(2026, 1, 5, 21)),
        (11.0, utc(2026, 1, 7, 1)),
        (10.0, utc(2026, 1, 8, 1)),
    ]


def test_every_field_and_nulls_compared_as_values(arc):
    base = bar("AAA", "2026-01-05", 10.0, volume=None)
    put(arc, [base], utc(2026, 1, 6, 1))
    put(arc, [{**base, "volume": 100}], utc(2026, 1, 7, 1))
    put(arc, [{**base, "volume": 100, "currency": "CAD"}], utc(2026, 1, 8, 1))
    put(arc, [{**base, "volume": 100, "currency": "CAD", "open": 9.5}], utc(2026, 1, 9, 1))
    put(arc, [{**base, "volume": 100, "currency": "CAD", "open": 9.5}], utc(2026, 1, 10, 1))
    t = build_prices(arc, FakeAdapter(), ["AAA"]).table
    assert t["volume"].to_pylist() == [None, 100, 100, 100]
    assert t["currency"].to_pylist() == ["USD", "USD", "CAD", "CAD"]
    assert t["open"].to_pylist() == [None, None, None, 9.5]


def test_nan_is_not_a_change(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0, open=float("nan"))], utc(2026, 1, 6, 1))
    put(arc, [bar("AAA", "2026-01-05", 10.0, open=float("nan"))], utc(2026, 1, 7, 1))
    assert build_prices(arc, FakeAdapter(), ["AAA"]).table.num_rows == 1


def test_absence_does_not_retract(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0), bar("AAA", "2026-01-06", 10.1)], utc(2026, 1, 7, 1))
    put(arc, [bar("AAA", "2026-01-06", 10.1)], utc(2026, 1, 8, 1))
    put(arc, [], utc(2026, 1, 9, 1), status="empty")
    t = build_prices(arc, FakeAdapter(), ["AAA"]).table
    assert [(r["valid_date"], r["close"]) for r in rows_of(t)] == [
        (date(2026, 1, 5), 10.0),
        (date(2026, 1, 6), 10.1),
    ]
    assert asof(t, PRICE_KEY, utc(2026, 1, 10)).num_rows == 2


def test_whole_market_fetch_filtered_to_requested_symbols(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 6, 1))
    put(
        arc,
        [
            bar("AAA", "2026-01-05", 10.0),
            bar("BBB", "2026-01-05", 20.0),
            bar("CCC", "2026-01-05", 30.0),
        ],
        utc(2026, 1, 6, 2),
        symbol=WHOLE_MARKET_SYMBOL,
    )
    put(arc, [bar("DDD", "2026-01-05", 40.0)], utc(2026, 1, 6, 1), symbol="DDD")
    some = build_prices(arc, FakeAdapter(), ["AAA", "BBB"]).table
    # AAA seen in its own fetch, then unchanged in the grouped fetch: one row.
    assert [(r["source_symbol"], r["close"]) for r in rows_of(some)] == [
        ("AAA", 10.0),
        ("BBB", 20.0),
    ]
    every = build_prices(arc, FakeAdapter(), None).table
    assert every["source_symbol"].to_pylist() == ["AAA", "BBB", "CCC", "DDD"]
    assert WHOLE_MARKET_SYMBOL not in every["source_symbol"].to_pylist()
    assert build_prices(arc, FakeAdapter(), []).table.num_rows == 0


def test_non_session_date_dropped_and_counted(arc):
    put(
        arc,
        [
            bar("AAA", "2026-01-03", 9.0),  # a Saturday
            bar("AAA", "2026-01-05", 10.0),
            bar("AAA", "2026-01-01", 9.5),  # New Year's Day
        ],
        utc(2026, 1, 6, 1),
    )
    put(arc, [bar("AAA", "2026-01-03", 9.1)], utc(2026, 1, 7, 1))
    res = build_prices(arc, FakeAdapter(), ["AAA"])
    assert res.table["valid_date"].to_pylist() == [date(2026, 1, 5)]
    assert res.dropped[DROP_NON_SESSION] == (
        ("AAA", date(2026, 1, 1)),
        ("AAA", date(2026, 1, 3)),
    )
    assert res.counts() == {DROP_NON_SESSION: 2, DROP_INTRADAY_PARTIAL: 0}


def test_intraday_partial_bar_dropped_and_counted(arc):
    put(arc, [bar("AAA", "2026-01-05", 9.9)], utc(2026, 1, 5, 18))  # before the 21:00 close
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 5, 21))  # at the close: accepted
    put(arc, [bar("BBB", "2026-01-05", 5.0)], utc(2026, 1, 5, 20, 59), symbol="BBB")
    res = build_prices(arc, FakeAdapter(), ["AAA", "BBB"])
    assert [(r["source_symbol"], r["close"], r["known_at"]) for r in rows_of(res.table)] == [
        ("AAA", 10.0, utc(2026, 1, 5, 21)),
    ]
    assert res.dropped[DROP_INTRADAY_PARTIAL] == (
        ("AAA", date(2026, 1, 5)),
        ("BBB", date(2026, 1, 5)),
    )
    assert res.counts()[DROP_INTRADAY_PARTIAL] == 2


def test_failed_fetch_contributes_no_rows(arc):
    put(arc, [bar("AAA", "2026-01-05", 99.0)], utc(2026, 1, 6, 1), status="error")
    put(arc, [bar("AAA", "2026-01-05", 99.0)], utc(2026, 1, 6, 2), status="not_found")
    put(arc, [bar("AAA", "2026-01-06", 10.0)], utc(2026, 1, 7, 1), status="partial")
    adapter = FakeAdapter()
    t = build_prices(arc, adapter, ["AAA"]).table
    assert t["valid_date"].to_pylist() == [date(2026, 1, 6)]
    assert [r.status for r in adapter.parsed] == ["partial"]


def test_conflicting_values_within_one_fetch_raise(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 6, 1))
    ref = put(
        arc, [bar("AAA", "2026-01-05", 10.0), bar("AAA", "2026-01-05", 10.4)], utc(2026, 1, 7)
    )
    with pytest.raises(ValueError, match="conflicting values") as err:
        build_prices(arc, FakeAdapter(), ["AAA"])
    assert ref in str(err.value) and "'AAA'" in str(err.value)


def test_conflicting_first_values_within_one_fetch_raise(arc):
    ref = put(
        arc, [bar("AAA", "2026-01-05", 10.0), bar("AAA", "2026-01-05", 10.4)], utc(2026, 1, 7)
    )
    with pytest.raises(ValueError, match="conflicting values") as err:
        build_prices(arc, FakeAdapter(), ["AAA"])
    assert ref in str(err.value) and "'AAA'" in str(err.value)


def test_tied_conflict_across_fetches_first_sighting_raises(arc):
    at = utc(2026, 1, 6, 1)
    own = put(arc, [bar("AAA", "2026-01-05", 10.0)], at)
    mkt = put(arc, [bar("AAA", "2026-01-05", 10.5)], at, symbol=WHOLE_MARKET_SYMBOL)
    with pytest.raises(ValueError, match="conflicting values") as err:
        build_prices(arc, FakeAdapter(), ["AAA"])
    assert own in str(err.value) and mkt in str(err.value)
    assert "2026, 1, 5" in str(err.value) or "2026-01-05" in str(err.value)


def test_tied_conflict_across_fetches_already_seen_raises(arc):
    put(arc, [bar("AAA", "2026-01-05", 10.0)], utc(2026, 1, 6, 1))
    at = utc(2026, 1, 8, 1)
    own = put(arc, [bar("AAA", "2026-01-05", 10.2)], at)
    mkt = put(arc, [bar("AAA", "2026-01-05", 10.3)], at, symbol=WHOLE_MARKET_SYMBOL)
    with pytest.raises(ValueError, match="conflicting values") as err:
        build_prices(arc, FakeAdapter(), ["AAA"])
    assert own in str(err.value) and mkt in str(err.value)


def test_tied_equal_values_across_fetches_give_one_row(arc):
    at = utc(2026, 1, 6, 1)
    put(arc, [bar("AAA", "2026-01-05", 10.0)], at)
    put(arc, [bar("AAA", "2026-01-05", 10.0)], at, symbol=WHOLE_MARKET_SYMBOL)
    assert build_prices(arc, FakeAdapter(), ["AAA"]).table.num_rows == 1


def test_tied_conflict_across_action_fetches_raises(arc):
    at = utc(2026, 1, 7, 1)
    own = put(arc, [action("AAA", "2026-01-06", amount=0.5)], at, kind="actions")
    mkt = put(
        arc,
        [action("AAA", "2026-01-06", amount=0.6)],
        at,
        symbol=WHOLE_MARKET_SYMBOL,
        kind="actions",
    )
    with pytest.raises(ValueError, match="conflicting values") as err:
        build_actions(arc, FakeAdapter(), ["AAA"])
    assert own in str(err.value) and mkt in str(err.value)


def test_rows_of_another_source_refused(arc):
    put(arc, [{**bar("AAA", "2026-01-05", 10.0), "source_code": "other"}], utc(2026, 1, 6, 1))
    with pytest.raises(ValueError, match="source_code"):
        build_prices(arc, FakeAdapter(), ["AAA"])


def test_empty_archive_gives_empty_contract_tables(arc):
    assert build_prices(arc, FakeAdapter(), None).table.schema.equals(PRICES.schema)
    assert build_actions(arc, FakeAdapter(), None).table.schema.equals(CORPORATE_ACTIONS.schema)
    assert build_fetch_log(arc, FakeAdapter(), SRC).schema.equals(FETCH_LOG.schema)
    assert build_prices(arc, FakeAdapter(), None).table.num_rows == 0


# --- actions -----------------------------------------------------------------------------------


def action(sym, ex, typ="cash_dividend_regular", seq=0, **kw):
    row = {"source_symbol": sym, "valid_date": ex, "action_type": typ, "action_seq": seq}
    row.update(kw)
    return row


def test_action_identity_and_final_seq(arc):
    put(
        arc,
        [
            action("AAA", "2026-02-02", seq=0, amount=0.5, currency="USD"),
            action("AAA", "2026-02-02", seq=1, amount=0.1, currency="USD"),
            action("AAA", "2026-02-02", typ="split", seq=0, ratio_old=1.0, ratio_new=2.0),
        ],
        utc(2026, 2, 3, 1),
        kind="actions",
    )
    fix_at = utc(2026, 2, 20, 1)
    put(
        arc,
        [
            action("AAA", "2026-02-02", seq=0, amount=0.5, currency="USD"),
            action("AAA", "2026-02-02", seq=1, amount=0.12, currency="USD"),
            action("AAA", "2026-02-02", typ="split", seq=0, ratio_old=1.0, ratio_new=2.0),
            action("AAA", "2026-02-09", seq=0, amount=0.3, currency="USD"),
        ],
        fix_at,
        kind="actions",
    )
    t = build_actions(arc, FakeAdapter(), ["AAA"]).table
    got = [
        (
            r["valid_date"],
            r["action_type"],
            r["action_seq"],
            r["amount"],
            r["known_at_kind"],
            r["known_at_basis"],
        )
        for r in rows_of(t)
    ]
    assert got == [
        (date(2026, 2, 2), "cash_dividend_regular", 0, 0.5, "published", "ex_date_assumed"),
        (date(2026, 2, 2), "cash_dividend_regular", 1, 0.1, "published", "ex_date_assumed"),
        (date(2026, 2, 2), "cash_dividend_regular", 1, 0.12, "collected", "fetch_time"),
        (date(2026, 2, 2), "split", 0, None, "published", "ex_date_assumed"),
        (date(2026, 2, 9), "cash_dividend_regular", 0, 0.3, "published", "ex_date_assumed"),
    ]
    assert t["known_at"].to_pylist()[2] == fix_at
    assert t.schema.equals(CORPORATE_ACTIONS.schema)


def test_action_announce_date_path(arc):
    # Announced 2026-01-15: end of day New York = 2026-01-16 04:59:59.999999 UTC.
    a = action("AAA", "2026-02-02", announce_date="2026-01-15", amount=0.5, currency="USD")
    put(arc, [a], utc(2026, 1, 20, 1), kind="actions")
    # Same, fetched during the announce day: capped at the fetch time.
    b = action("BBB", "2026-02-02", announce_date="2026-01-15", amount=0.5, currency="USD")
    put(arc, [b], utc(2026, 1, 15, 15), symbol="BBB", kind="actions")
    t = build_actions(arc, FakeAdapter(), None).table
    got = [
        (r["source_symbol"], r["known_at"], r["known_at_kind"], r["known_at_basis"])
        for r in rows_of(t)
    ]
    assert got == [
        ("AAA", utc(2026, 1, 16, 4, 59, 59, 999999), "published", "announce_date"),
        ("BBB", utc(2026, 1, 15, 15), "collected", "fetch_time"),
    ]


def test_action_ex_date_open_path(arc):
    put(
        arc,
        [
            action("AAA", "2026-01-06", amount=0.5, currency="USD"),  # no announce date
            # announced after the ex-date: not trusted
            action("AAA", "2026-01-07", announce_date="2026-01-08", amount=0.5, currency="USD"),
            action("AAA", "2026-01-10", amount=0.5, currency="USD"),  # Saturday: next session
            action("AAA", "2026-07-06", amount=0.5, currency="USD"),  # summer time
        ],
        utc(2026, 7, 7, 1),
        kind="actions",
    )
    t = build_actions(arc, FakeAdapter(), ["AAA"]).table
    assert [(r["valid_date"], r["known_at"], r["known_at_basis"]) for r in rows_of(t)] == [
        (date(2026, 1, 6), utc(2026, 1, 6, 14, 30), "ex_date_assumed"),
        (date(2026, 1, 7), utc(2026, 1, 7, 14, 30), "ex_date_assumed"),
        (date(2026, 1, 10), utc(2026, 1, 12, 14, 30), "ex_date_assumed"),
        (date(2026, 7, 6), utc(2026, 7, 6, 13, 30), "ex_date_assumed"),
    ]


def test_future_ex_date_never_gives_future_known_at(arc):
    fetched = utc(2026, 1, 7, 1)
    put(
        arc,
        [
            action("AAA", "2027-03-15", amount=0.5, currency="USD"),
            action("AAA", "2027-03-16", announce_date="2027-03-01", amount=0.5, currency="USD"),
        ],
        fetched,
        kind="actions",
    )
    t = build_actions(arc, FakeAdapter(), ["AAA"]).table
    assert t["known_at"].to_pylist() == [fetched, fetched]
    assert t["known_at_kind"].to_pylist() == ["collected", "collected"]
    assert t["known_at_basis"].to_pylist() == ["fetch_time", "fetch_time"]
    conform(CORPORATE_ACTIONS, t, now=fetched)


def test_announce_on_ex_date_known_at_ex_date_open(arc):
    # Announced on its own ex-date: end of day comes after the open, so the open is used and a
    # total return as of the ex-date close sees the action.
    a = action("AAA", "2026-01-06", announce_date="2026-01-06", amount=0.5, currency="USD")
    put(arc, [a], utc(2026, 1, 7, 1), kind="actions")
    (row,) = rows_of(build_actions(arc, FakeAdapter(), ["AAA"]).table)
    assert (row["known_at"], row["known_at_kind"], row["known_at_basis"]) == (
        utc(2026, 1, 6, 14, 30),
        "published",
        "ex_date_assumed",
    )


def test_announce_day_before_ex_date_uses_announce_date(arc):
    a = action("AAA", "2026-01-06", announce_date="2026-01-05", amount=0.5, currency="USD")
    put(arc, [a], utc(2026, 1, 7, 1), kind="actions")
    (row,) = rows_of(build_actions(arc, FakeAdapter(), ["AAA"]).table)
    assert (row["known_at"], row["known_at_kind"], row["known_at_basis"]) == (
        utc(2026, 1, 6, 4, 59, 59, 999999),
        "published",
        "announce_date",
    )


def test_action_from_whole_market_fetch_filtered(arc):
    put(
        arc,
        [
            action("AAA", "2026-01-06", amount=0.5, currency="USD"),
            action("ZZZ", "2026-01-06", amount=0.5, currency="USD"),
        ],
        utc(2026, 1, 7, 1),
        symbol=WHOLE_MARKET_SYMBOL,
        kind="actions",
    )
    t = build_actions(arc, FakeAdapter(), ["AAA"]).table
    assert t["source_symbol"].to_pylist() == ["AAA"]


# --- fetch log ---------------------------------------------------------------------------------


def test_fetch_log_rows(arc):
    at = utc(2026, 1, 7, 1, 2, 3, 4)
    r_ok = put(
        arc,
        [bar("AAA", "2026-01-06", 10.0), bar("AAA", "2026-01-05", 9.0)],
        at,
        frm=date(2026, 1, 1),
        to=date(2026, 1, 6),
    )
    r_mkt = put(
        arc,
        [
            bar("AAA", "2026-01-06", 10.0),
            bar("BBB", "2026-01-06", 20.0),
            bar("CCC", "2026-01-06", 30.0),
        ],
        utc(2026, 1, 7, 2),
        symbol=WHOLE_MARKET_SYMBOL,
    )
    r_empty = put(
        arc, [], utc(2026, 1, 8, 1), status="empty", frm=date(2026, 1, 7), to=date(2026, 1, 7)
    )
    r_err = put(arc, [bar("AAA", "2026-01-07", 1.0)], utc(2026, 1, 8, 2), status="error")
    r_act = put(arc, [action("AAA", "2026-01-06")], utc(2026, 1, 9, 1), kind="actions")
    adapter = FakeAdapter()
    t = build_fetch_log(arc, adapter, SRC)
    got = {r["fetch_id"]: r for r in rows_of(t)}
    assert set(got) == {r_ok, r_mkt, r_empty, r_err, r_act}
    ok = got[r_ok]
    assert ok == {
        "source_symbol": "AAA",
        "fetch_id": r_ok,
        "requested_from": date(2026, 1, 1),
        "requested_to": date(2026, 1, 6),
        "returned_from": date(2026, 1, 5),
        "returned_to": date(2026, 1, 6),
        "status": "ok",
        "row_count": 2,
        "archive_ref": r_ok,
        "known_at_basis": "fetch_time",
        "valid_date": date(2026, 1, 7),
        "known_at": at,
        "known_at_kind": "collected",
        "source_code": SRC,
        "source_doc_id": None,
    }
    assert (got[r_mkt]["source_symbol"], got[r_mkt]["row_count"]) == (WHOLE_MARKET_SYMBOL, 3)
    for ref in (r_empty, r_err):
        assert got[ref]["row_count"] == 0
        assert got[ref]["returned_from"] is None and got[ref]["returned_to"] is None
    assert got[r_err]["status"] == "error"
    assert got[r_act]["row_count"] == 1
    assert "error" not in [r.status for r in adapter.parsed]
    only_prices = build_fetch_log(arc, FakeAdapter(), SRC, kinds=("prices",))
    assert r_act not in only_prices["fetch_id"].to_pylist()
    with pytest.raises(ValueError, match="adapter"):
        build_fetch_log(arc, FakeAdapter(), "other")


# --- contracts ---------------------------------------------------------------------------------


def test_every_table_passes_conform(arc):
    put(
        arc,
        [bar("AAA", "2026-01-05", 10.0, open=9.8, high=10.2, low=9.7, volume=1000)],
        utc(2026, 1, 6, 1),
    )
    put(
        arc,
        [bar("AAA", "2026-01-05", 10.1, open=9.8, high=10.2, low=9.7, volume=1000)],
        utc(2026, 1, 9, 1),
    )
    put(
        arc,
        [
            action(
                "AAA",
                "2026-01-06",
                announce_date="2025-12-15",
                amount=0.5,
                currency="USD",
                amount_basis="gross",
                per_share_basis="old_share",
                source_action_type="CD",
                record_date="2026-01-07",
                pay_date="2026-01-20",
            ),
            action("AAA", "2026-01-06", typ="split", ratio_old=1.0, ratio_new=4.0),
        ],
        utc(2026, 1, 7, 1),
        kind="actions",
    )
    prices = build_prices(arc, FakeAdapter(), ["AAA"]).table
    actions = build_actions(arc, FakeAdapter(), ["AAA"]).table
    log = build_fetch_log(arc, FakeAdapter(), SRC)
    for contract, table in ((PRICES, prices), (CORPORATE_ACTIONS, actions), (FETCH_LOG, log)):
        assert table.num_rows > 0
        assert conform(contract, table).equals(table)


@pytest.mark.parametrize(
    ("ex", "next_open"),
    [("2026-01-10", utc(2026, 1, 12, 14, 30)), ("2026-01-19", utc(2026, 1, 20, 14, 30))],
    ids=["saturday", "holiday"],
)
@pytest.mark.parametrize("announced", [None, "2026-01-02"], ids=["no-announce", "announce"])
def test_earliest_ex_date_not_a_session(arc, ex, next_open, announced):
    # The earliest (and only) ex-date of the build is a Saturday / Martin Luther King Day.
    a = action("AAA", ex, announce_date=announced, amount=0.5, currency="USD")
    put(arc, [a], utc(2026, 2, 1, 1), kind="actions")
    (row,) = rows_of(build_actions(arc, FakeAdapter(), ["AAA"]).table)
    if announced is None:
        assert (row["known_at"], row["known_at_basis"]) == (next_open, "ex_date_assumed")
    else:
        assert (row["known_at"], row["known_at_basis"]) == (
            utc(2026, 1, 3, 4, 59, 59, 999999),
            "announce_date",
        )


def test_next_session_margin_parameter(arc):
    put(arc, [action("AAA", "2026-01-10", amount=0.5)], utc(2026, 2, 1, 1), kind="actions")
    # Saturday + 3 days reaches Monday's session.
    t = build_actions(arc, FakeAdapter(), ["AAA"], next_session_margin=timedelta(days=3)).table
    assert t["known_at"].to_pylist() == [utc(2026, 1, 12, 14, 30)]
    # Saturday + 1 day: no next session inside the calendar -> a clear ValueError.
    with pytest.raises(ValueError, match="calendar"):
        build_actions(arc, FakeAdapter(), ["AAA"], next_session_margin=timedelta(days=1))


@pytest.mark.parametrize("margin", [timedelta(0), timedelta(days=-1)])
def test_next_session_margin_must_be_positive(arc, margin):
    with pytest.raises(ValueError, match="next_session_margin"):
        build_actions(arc, FakeAdapter(), ["AAA"], next_session_margin=margin)


def test_calendar_out_of_range_is_a_value_error(arc):
    put(arc, [action("AAA", "1600-01-04", amount=0.5)], utc(2026, 2, 1, 1), kind="actions")
    with pytest.raises(ValueError, match="calendar"):
        build_actions(arc, FakeAdapter(), ["AAA"])

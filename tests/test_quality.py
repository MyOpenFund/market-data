from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    FETCH_LOG,
    INSTRUMENT_IDENTIFIERS,
    PRICES,
)
from open_data_store.market.coverage import FETCH_COVERAGE_SCHEMA, coverage

from market_data.adapters.tiingo import VENDOR_ADJUSTED_SCHEMA
from market_data.quality import (
    CHECKS,
    QUALITY_FLAGS_SCHEMA,
    check_prices,
    fetch_coverage,
    unsuperseded_failures,
)

SRC = "synth"
D = date(2024, 1, 2)
# Synthetic sessions: Tue 2 .. Fri 5, Mon 8 .. Fri 12 January 2024.
SESSION_DAYS = [D + timedelta(days=i) for i in (0, 1, 2, 3, 6, 7, 8, 9, 10)]
SESSIONS = pa.table({"session_date": pa.array(SESSION_DAYS, pa.date32())})


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def close_at(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 21, tzinfo=UTC)


def prices(rows, source_code=SRC) -> pa.Table:
    """rows: (symbol, day, close) or (symbol, day, close, known_at)."""
    out = []
    for row in rows:
        symbol, day, close = row[:3]
        known = row[3] if len(row) > 3 else close_at(day)
        out.append(
            {
                "source_symbol": symbol,
                "open": None,
                "high": None,
                "low": None,
                "close": close,
                "volume": None,
                "currency": "USD",
                "price_basis": "raw",
                "known_at_basis": "session_close",
                "valid_date": day,
                "known_at": known,
                "known_at_kind": "published",
                "source_code": source_code,
                "source_doc_id": None,
            }
        )
    return pa.Table.from_pylist(out, schema=PRICES.schema)


def actions(rows, source_code=SRC) -> pa.Table:
    """rows: dicts with source_symbol, valid_date, action_type and optional fields."""
    out = []
    for i, row in enumerate(rows):
        base = {name: None for name in CORPORATE_ACTIONS.schema.names}
        base.update(
            {
                "action_seq": 0,
                "known_at_basis": "announce_date",
                "known_at": utc(2023, 12, 1) + timedelta(seconds=i),
                "known_at_kind": "published",
                "source_code": source_code,
            }
        )
        base.update(row)
        out.append(base)
    return pa.Table.from_pylist(out, schema=CORPORATE_ACTIONS.schema)


NO_ACTIONS = actions([])


def flags_of(table: pa.Table) -> list[tuple[str, date, str]]:
    return list(
        zip(
            table["source_symbol"].to_pylist(),
            table["session_date"].to_pylist(),
            table["check"].to_pylist(),
            strict=True,
        )
    )


def series(symbol, closes, days=SESSION_DAYS):
    return [(symbol, day, c) for day, c in zip(days, closes, strict=False)]


# --- check_prices -------------------------------------------------------------------------------


def test_flags_schema_and_vocabulary():
    out = check_prices(prices(series("AAA", [10, 10.1])), NO_ACTIONS, SESSIONS)
    assert out.schema == QUALITY_FLAGS_SCHEMA
    assert out.num_rows == 0
    assert CHECKS == ("non_positive", "jump_without_action", "missing_session", "vendor_divergence")


def test_split_jump_not_flagged_when_action_present():
    p = prices(series("AAA", [100.0, 101.0, 50.0, 50.5]))
    a = actions(
        [
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[2],
                "action_type": "split",
                "ratio_old": 1.0,
                "ratio_new": 2.0,
            }
        ]
    )
    assert check_prices(p, a, SESSIONS).num_rows == 0


def test_jump_flagged_without_action():
    p = prices(series("AAA", [100.0, 101.0, 50.0, 50.5]) + series("BBB", [10.0, 10.2, 10.1, 10.3]))
    out = check_prices(p, NO_ACTIONS, SESSIONS)
    assert flags_of(out) == [("AAA", SESSION_DAYS[2], "jump_without_action")]
    assert "0.5" in out["detail"][0].as_py()


def test_jump_threshold_is_a_parameter_and_strict():
    p = prices(series("AAA", [100.0, 150.0]))
    assert check_prices(p, NO_ACTIONS, SESSIONS).num_rows == 0  # exactly 0.5: not above
    out = check_prices(p, NO_ACTIONS, SESSIONS, jump_threshold=0.2)
    assert flags_of(out) == [("AAA", SESSION_DAYS[1], "jump_without_action")]


def test_cash_dividend_does_not_excuse_a_jump():
    p = prices(series("AAA", [100.0, 30.0]))
    a = actions(
        [
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[1],
                "action_type": "cash_dividend_regular",
                "amount": 1.0,
                "currency": "USD",
            }
        ]
    )
    assert [f[2] for f in flags_of(check_prices(p, a, SESSIONS))] == ["jump_without_action"]


def test_action_of_another_symbol_does_not_excuse_a_jump():
    p = prices(series("AAA", [100.0, 30.0]))
    a = actions(
        [
            {
                "source_symbol": "BBB",
                "valid_date": SESSION_DAYS[1],
                "action_type": "split",
                "ratio_old": 1.0,
                "ratio_new": 3.0,
            }
        ]
    )
    assert len(flags_of(check_prices(p, a, SESSIONS))) == 1


def test_action_between_price_rows_excuses_the_jump():
    # The ex-date (Saturday 6th) is not a session: the jump shows on the next price row.
    p = prices([("AAA", SESSION_DAYS[3], 100.0), ("AAA", SESSION_DAYS[4], 25.0)])
    a = actions(
        [
            {
                "source_symbol": "AAA",
                "valid_date": date(2024, 1, 6),
                "action_type": "spin_off",
                "spinoff_symbol": "CCC",
            }
        ]
    )
    assert check_prices(p, a, SESSIONS).num_rows == 0


def test_non_positive_flagged_and_no_jump_computed_around_it():
    p = prices(series("AAA", [10.0, 0.0, 10.0, -1.0]))
    out = check_prices(p, NO_ACTIONS, SESSIONS)
    assert flags_of(out) == [
        ("AAA", SESSION_DAYS[1], "non_positive"),
        ("AAA", SESSION_DAYS[3], "non_positive"),
    ]


def test_non_finite_close_flagged_non_positive():
    p = prices(series("AAA", [10.0, float("nan"), 10.0]))
    assert flags_of(check_prices(p, NO_ACTIONS, SESSIONS)) == [
        ("AAA", SESSION_DAYS[1], "non_positive")
    ]


def test_missing_session_between_first_and_last_price():
    days = [SESSION_DAYS[i] for i in (1, 2, 4, 6)]  # 3 and 5 missing; 0, 7, 8 out of span
    p = prices([("AAA", d, 10.0) for d in days])
    out = check_prices(p, NO_ACTIONS, SESSIONS)
    assert flags_of(out) == [
        ("AAA", SESSION_DAYS[3], "missing_session"),
        ("AAA", SESSION_DAYS[5], "missing_session"),
    ]


def test_latest_revision_is_checked():
    later = utc(2024, 2, 1)
    p = prices(series("AAA", [100.0, 101.0, 50.0]) + [("AAA", SESSION_DAYS[2], 100.5, later)])
    assert check_prices(p, NO_ACTIONS, SESSIONS).num_rows == 0


def test_vendor_divergence_flagged():
    # Raw: 100, 101, 99 (ex-dividend 2.0 on day 2: our return (99 + 2) / 101 - 1 = 0), 49.5
    # (split 2:1 on day 3: 49.5 * 2 / 99 - 1 = 0).
    p = prices(series("AAA", [100.0, 101.0, 99.0, 49.5]))
    a = actions(
        [
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[2],
                "action_type": "cash_dividend_regular",
                "amount": 2.0,
                "currency": "USD",
            },
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[3],
                "action_type": "split",
                "ratio_old": 1.0,
                "ratio_new": 2.0,
            },
        ]
    )
    # Vendor agrees on days 1 and 2 (returns 0.01, 0.0) and says -3 % on day 3 (we say 0).
    adj = [40.0, 40.4, 40.4, 40.4 * 0.97]
    vendor = pa.Table.from_pylist(
        [
            {"source_symbol": "AAA", "valid_date": d, "adj_close": v}
            for d, v in zip(SESSION_DAYS, adj, strict=False)
        ],
        schema=VENDOR_ADJUSTED_SCHEMA,
    )
    out = check_prices(p, a, SESSIONS, vendor_adjusted=vendor)
    assert flags_of(out) == [("AAA", SESSION_DAYS[3], "vendor_divergence")]
    # Not checked at all without the vendor series.
    assert check_prices(p, a, SESSIONS).num_rows == 0
    # The threshold is a parameter.
    assert (
        check_prices(p, a, SESSIONS, vendor_adjusted=vendor, divergence_threshold=0.05).num_rows
        == 0
    )


def test_vendor_divergence_compares_matching_days_only():
    p = prices(series("AAA", [100.0, 101.0, 102.0]))
    vendor = pa.Table.from_pylist(
        [
            {"source_symbol": "AAA", "valid_date": SESSION_DAYS[0], "adj_close": 1.0},
            {"source_symbol": "AAA", "valid_date": SESSION_DAYS[2], "adj_close": 5.0},
            {"source_symbol": "ZZZ", "valid_date": SESSION_DAYS[1], "adj_close": 9.0},
        ],
        schema=VENDOR_ADJUSTED_SCHEMA,
    )
    # Pairs (0, 1) and (1, 2) lack a vendor value on day 1: neither is compared, whatever the
    # vendor says on day 2; other symbols' vendor rows are ignored.
    assert check_prices(p, NO_ACTIONS, SESSIONS, vendor_adjusted=vendor).num_rows == 0


@pytest.mark.parametrize(("basis", "flagged"), [("old_share", False), ("new_share", True)])
def test_vendor_divergence_same_day_split_and_dividend_basis(basis, flagged):
    # Split 2:1 and a 2.0 dividend on day 2. Per old share: (2 * 49.5 + 2) / 101 - 1 = 0;
    # per new share: (2 * 49.5 + 2 * 2) / 101 - 1 = +1.98 %. The vendor says 0.
    p = prices(series("AAA", [100.0, 101.0, 49.5]))
    a = actions(
        [
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[2],
                "action_type": "split",
                "ratio_old": 1.0,
                "ratio_new": 2.0,
            },
            {
                "source_symbol": "AAA",
                "valid_date": SESSION_DAYS[2],
                "action_type": "cash_dividend_regular",
                "amount": 2.0,
                "currency": "USD",
                "per_share_basis": basis,
            },
        ]
    )
    vendor = pa.Table.from_pylist(
        [
            {"source_symbol": "AAA", "valid_date": d, "adj_close": v}
            for d, v in zip(SESSION_DAYS, [0.99, 1.0, 1.0], strict=False)
        ],
        schema=VENDOR_ADJUSTED_SCHEMA,
    )
    out = check_prices(p, a, SESSIONS, vendor_adjusted=vendor)
    assert flags_of(out) == ([("AAA", SESSION_DAYS[2], "vendor_divergence")] if flagged else [])


def test_mixed_sources_refused():
    p = pa.concat_tables(
        [prices(series("AAA", [1.0])), prices(series("AAA", [1.0]), source_code="other")]
    )
    with pytest.raises(ValueError, match="one source"):
        check_prices(p, NO_ACTIONS, SESSIONS)
    other = actions(
        [{"source_symbol": "A", "valid_date": D, "action_type": "split"}], source_code="other"
    )
    with pytest.raises(ValueError, match="one source"):
        check_prices(prices(series("AAA", [1.0])), other, SESSIONS)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"jump_threshold": 0.0},
        {"jump_threshold": float("nan")},
        {"divergence_threshold": -0.1},
        {"divergence_threshold": float("inf")},
    ],
)
def test_thresholds_validated(kwargs):
    with pytest.raises(ValueError, match="threshold"):
        check_prices(prices(series("AAA", [1.0])), NO_ACTIONS, SESSIONS, **kwargs)


def test_empty_prices_give_no_flags():
    assert check_prices(prices([]), NO_ACTIONS, SESSIONS).num_rows == 0


# --- fetch_coverage -----------------------------------------------------------------------------

AS_OF = utc(2024, 3, 1)
INST_A, INST_B, INST_C = "mof:" + "a" * 32, "mof:" + "b" * 32, "mof:" + "c" * 32


def identifiers(rows) -> pa.Table:
    """rows: (instrument_id, id_value, valid_date, valid_to)."""
    return pa.Table.from_pylist(
        [
            {
                "instrument_id": inst,
                "id_scheme": "source_symbol",
                "id_value": value,
                "valid_to": end,
                "link_reason": None,
                "known_at_basis": "load_time",
                "valid_date": start,
                "known_at": utc(2024, 2, 1),
                "known_at_kind": "collected",
                "source_code": "openfigi",
                "source_doc_id": None,
            }
            for inst, value, start, end in rows
        ],
        schema=INSTRUMENT_IDENTIFIERS.schema,
    )


def fetch_log(rows, source_code=SRC) -> pa.Table:
    """rows: (symbol, from, to, status, fetched_at)."""
    out = []
    for i, (symbol, start, end, status, at) in enumerate(rows):
        out.append(
            {
                "source_symbol": symbol,
                "fetch_id": f"ref{i}",
                "requested_from": start,
                "requested_to": end,
                "returned_from": None,
                "returned_to": None,
                "status": status,
                "row_count": 0,
                "archive_ref": f"ref{i}",
                "known_at_basis": "fetch_time",
                "valid_date": at.date(),
                "known_at": at,
                "known_at_kind": "collected",
                "source_code": source_code,
                "source_doc_id": None,
            }
        )
    return pa.Table.from_pylist(out, schema=FETCH_LOG.schema)


def cov_rows(table: pa.Table):
    assert table.schema == FETCH_COVERAGE_SCHEMA
    return sorted(
        zip(
            table["instrument_id"].to_pylist(),
            table["covered_from"].to_pylist(),
            table["covered_to"].to_pylist(),
            table["status"].to_pylist(),
            strict=True,
        )
    )


IDS = identifiers(
    [
        (INST_A, f"{SRC}:AAA", date(2024, 1, 1), date(2024, 1, 8)),  # AAA -> A until the 8th
        (INST_C, f"{SRC}:AAA", date(2024, 1, 8), None),  # then AAA -> C (recycled ticker)
        (INST_B, f"{SRC}:BBB", date(2023, 1, 1), None),
        (INST_A, "other:AAA", date(2023, 1, 1), None),  # another source
    ]
)


def test_symbol_rows_mapped_through_mapping_windows():
    log = fetch_log([("AAA", date(2024, 1, 2), date(2024, 1, 12), "ok", utc(2024, 1, 13))])
    assert cov_rows(fetch_coverage(log, IDS, as_of=AS_OF)) == [
        (INST_A, date(2024, 1, 2), date(2024, 1, 7), "ok"),
        (INST_C, date(2024, 1, 8), date(2024, 1, 12), "ok"),
    ]


def test_unmapped_symbol_covers_nothing():
    log = fetch_log([("ZZZ", date(2024, 1, 2), date(2024, 1, 12), "not_found", utc(2024, 1, 13))])
    assert fetch_coverage(log, IDS, as_of=AS_OF).num_rows == 0


def test_whole_market_row_covers_every_mapped_instrument_of_its_source():
    log = fetch_log([("*", date(2024, 1, 5), date(2024, 1, 9), "ok", utc(2024, 1, 10))])
    assert cov_rows(fetch_coverage(log, IDS, as_of=AS_OF)) == [
        (INST_A, date(2024, 1, 5), date(2024, 1, 7), "ok"),
        (INST_B, date(2024, 1, 5), date(2024, 1, 9), "ok"),
        (INST_C, date(2024, 1, 8), date(2024, 1, 9), "ok"),
    ]


def test_whole_market_rows_merged_per_status_across_non_sessions():
    # Grouped daily fetches Fri 5 and Mon 8 merge across the weekend when sessions are given.
    log = fetch_log(
        [
            ("*", date(2024, 1, 5), date(2024, 1, 5), "ok", utc(2024, 1, 6)),
            ("*", date(2024, 1, 8), date(2024, 1, 8), "ok", utc(2024, 1, 9)),
        ]
    )
    ids = identifiers([(INST_B, f"{SRC}:BBB", date(2023, 1, 1), None)])
    assert cov_rows(fetch_coverage(log, ids, as_of=AS_OF, sessions=SESSIONS)) == [
        (INST_B, date(2024, 1, 5), date(2024, 1, 8), "ok"),
    ]
    assert len(cov_rows(fetch_coverage(log, ids, as_of=AS_OF))) == 2


def test_superseded_error_dropped_and_unsuperseded_kept():
    log = fetch_log(
        [
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "error", utc(2024, 1, 6)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 12), "ok", utc(2024, 1, 13)),  # supersedes
            ("BBB", date(2024, 1, 9), date(2024, 1, 10), "partial", utc(2024, 1, 14)),  # later
        ]
    )
    rows = cov_rows(fetch_coverage(log, IDS, as_of=AS_OF))
    assert rows == [
        (INST_B, date(2024, 1, 2), date(2024, 1, 12), "ok"),
        (INST_B, date(2024, 1, 9), date(2024, 1, 10), "partial"),
    ]
    left = unsuperseded_failures(log)
    assert left["status"].to_pylist() == ["partial"]


def test_error_not_superseded_by_partial_or_earlier_cover():
    log = fetch_log(
        [
            ("BBB", date(2024, 1, 2), date(2024, 1, 12), "ok", utc(2024, 1, 5)),  # earlier
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "error", utc(2024, 1, 6)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 4), "ok", utc(2024, 1, 7)),  # part only
            ("*", date(2024, 1, 2), date(2024, 1, 5), "ok", utc(2024, 1, 8)),  # other key
        ]
    )
    assert unsuperseded_failures(log)["fetch_id"].to_pylist() == ["ref1"]
    statuses = {r[3] for r in cov_rows(fetch_coverage(log, IDS, as_of=AS_OF))}
    assert "error" in statuses
    same_time = fetch_log(
        [
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "error", utc(2024, 1, 9)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "ok", utc(2024, 1, 9)),  # not later
        ]
    )
    assert unsuperseded_failures(same_time)["fetch_id"].to_pylist() == ["ref0"]


def test_error_superseded_by_union_of_later_rows():
    log = fetch_log(
        [
            ("BBB", date(2024, 1, 2), date(2024, 1, 12), "error", utc(2024, 1, 13)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "ok", utc(2024, 1, 14)),
            ("BBB", date(2024, 1, 6), date(2024, 1, 12), "not_found", utc(2024, 1, 15)),
        ]
    )
    assert unsuperseded_failures(log).num_rows == 0
    # With a hole (Saturday 6th uncovered) the union covers it only through sessions.
    log = fetch_log(
        [
            ("*", date(2024, 1, 5), date(2024, 1, 8), "error", utc(2024, 1, 9)),
            ("*", date(2024, 1, 5), date(2024, 1, 5), "ok", utc(2024, 1, 10)),
            ("*", date(2024, 1, 8), date(2024, 1, 8), "empty", utc(2024, 1, 10)),
        ]
    )
    assert unsuperseded_failures(log).num_rows == 1
    assert unsuperseded_failures(log, sessions=SESSIONS).num_rows == 0


def test_null_range_failure_counts_but_covers_nothing():
    log = fetch_log([("BBB", None, None, "error", utc(2024, 1, 6))])
    assert unsuperseded_failures(log).num_rows == 1
    assert fetch_coverage(log, IDS, as_of=AS_OF).num_rows == 0


def test_as_of_bounds_fetches_and_ranges():
    log = fetch_log(
        [
            ("BBB", date(2024, 1, 2), date(2024, 3, 31), "ok", utc(2024, 2, 1)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "error", utc(2024, 1, 1)),
            ("BBB", date(2024, 1, 2), date(2024, 1, 5), "ok", utc(2024, 3, 2)),  # after as_of
        ]
    )
    # The January error is superseded by the February row; the March row is after as_of.
    assert cov_rows(fetch_coverage(log, IDS, as_of=AS_OF)) == [
        (INST_B, date(2024, 1, 2), date(2024, 3, 1), "ok")
    ]
    with pytest.raises(ValueError, match="timezone-aware"):
        fetch_coverage(log, IDS, as_of=datetime(2024, 3, 1))


def test_ambiguous_mapping_refused():
    ids = identifiers(
        [
            (INST_A, f"{SRC}:AAA", date(2024, 1, 1), None),
            (INST_B, f"{SRC}:AAA", date(2024, 1, 5), None),
        ]
    )
    log = fetch_log([("AAA", date(2024, 1, 2), date(2024, 1, 12), "ok", utc(2024, 1, 13))])
    with pytest.raises(ValueError, match="ambiguous"):
        fetch_coverage(log, ids, as_of=AS_OF)


def test_unknown_status_refused():
    log = fetch_log([("BBB", date(2024, 1, 2), date(2024, 1, 5), "ok", utc(2024, 1, 6))])
    log = log.set_column(
        log.schema.get_field_index("status"), "status", pa.array(["weird"], pa.string())
    )
    with pytest.raises(ValueError, match="status"):
        fetch_coverage(log, IDS, as_of=AS_OF)


def test_superseded_error_no_longer_vetoes_a_delisting():
    """End to end with the store's coverage: a transient grouped-day error, retried ok later."""
    ids = identifiers([(INST_B, f"{SRC}:BBB", date(2023, 1, 1), None)])
    intervals = pa.table(
        {
            "instrument_id": pa.array([INST_B]),
            "valid_from": pa.array([date(2024, 1, 2)], pa.date32()),
            "valid_to": pa.array([None], pa.date32()),
        }
    )
    resolved = pa.table(
        {
            "instrument_id": pa.array([INST_B, INST_B, "mof:" + "d" * 32]),
            "session_date": pa.array([SESSION_DAYS[0], SESSION_DAYS[1], SESSION_DAYS[8]]),
            "close": pa.array([10.0, 10.0, 5.0]),
        }
    )
    intervals = pa.concat_tables(
        [
            intervals,
            pa.table(
                {
                    "instrument_id": pa.array(["mof:" + "d" * 32]),
                    "valid_from": pa.array([date(2024, 1, 2)], pa.date32()),
                    "valid_to": pa.array([None], pa.date32()),
                }
            ),
        ]
    )
    days = [(d, d) for d in SESSION_DAYS[2:]]
    rows = [("*", a, b, "ok", close_at(a) + timedelta(hours=4)) for a, b in days]
    rows.append(("*", SESSION_DAYS[4], SESSION_DAYS[4], "error", close_at(SESSION_DAYS[4])))
    log = fetch_log(rows)
    cov = coverage(intervals, resolved, SESSIONS, fetch_coverage(log, ids, as_of=AS_OF))
    reasons = cov.per_member.filter(pa.compute.equal(cov.per_member["instrument_id"], INST_B))[
        "reason"
    ].to_pylist()
    assert reasons == ["ok", "ok"] + ["delisted"] * 7

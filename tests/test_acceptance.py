"""Acceptance computations (spec §11) on synthetic data: no real member, no real price."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from open_data_store.market.asof import sessions as calendar_sessions

from market_data.acceptance import (
    INDEX_SCHEMA,
    ComparisonResult,
    SurvivorsResult,
    compare_series,
    equal_weight_index,
    rebalance_dates,
    survivors_only_delta,
    truncation_check,
)

D = date.fromisoformat


def _sessions(*days: str) -> pa.Table:
    return pa.table({"session_date": pa.array([D(d) for d in days], pa.date32())})


def _intervals(rows: list[tuple[str, str, str | None]]) -> pa.Table:
    return pa.table(
        {
            "instrument_id": pa.array([r[0] for r in rows], pa.string()),
            "index_code": pa.array(["synthetic"] * len(rows), pa.string()),
            "valid_from": pa.array([D(r[1]) for r in rows], pa.date32()),
            "valid_to": pa.array([None if r[2] is None else D(r[2]) for r in rows], pa.date32()),
            "source_code": pa.array(["synthetic"] * len(rows), pa.string()),
        }
    )


def _tr(series: dict[str, dict[str, float]]) -> pa.Table:
    rows = [(i, D(d), v) for i, by_day in series.items() for d, v in by_day.items()]
    return pa.table(
        {
            "instrument_id": pa.array([r[0] for r in rows], pa.string()),
            "session_date": pa.array([r[1] for r in rows], pa.date32()),
            "tr_index": pa.array([r[2] for r in rows], pa.float64()),
            "flags": pa.array([None] * len(rows), pa.string()),
        }
    )


def _levels(index: pa.Table) -> dict[date, float]:
    return dict(
        zip(
            index.column("session_date").to_pylist(), index.column("level").to_pylist(), strict=True
        )
    )


def _series(levels: dict[str, float], column: str = "level") -> pa.Table:
    return pa.table(
        {
            "session_date": pa.array([D(d) for d in levels], pa.date32()),
            column: pa.array(list(levels.values()), pa.float64()),
        }
    )


# --- rebalance dates ----------------------------------------------------------------------------


def test_quarterly_rebalance_dates():
    year = calendar_sessions("XNYS", D("2024-01-01"), D("2024-12-31"))
    assert rebalance_dates(year, "quarterly") == [
        D("2024-01-02"),
        D("2024-04-01"),
        D("2024-07-01"),
        D("2024-10-01"),
    ]
    monthly = rebalance_dates(year, "monthly")
    assert len(monthly) == 12
    assert monthly[:3] == [D("2024-01-02"), D("2024-02-01"), D("2024-03-01")]
    assert monthly[8] == D("2024-09-03")  # Labor Day: the first session of September


def test_rebalance_dates_start_on_the_first_session_mid_period():
    part = calendar_sessions("XNYS", D("2024-02-15"), D("2024-07-31"))
    assert rebalance_dates(part, "quarterly") == [D("2024-02-15"), D("2024-04-01"), D("2024-07-01")]


def test_rebalance_dates_refuse_an_unknown_frequency():
    with pytest.raises(ValueError, match="rebalance"):
        rebalance_dates(_sessions("2024-01-02"), "weekly")  # type: ignore[arg-type]


# --- equal-weight index -------------------------------------------------------------------------

DRIFT_SESSIONS = _sessions(
    "2024-01-30", "2024-01-31", "2024-02-01", "2024-03-01", "2024-04-01", "2024-04-02"
)
DRIFT_DAYS = DRIFT_SESSIONS.column("session_date").to_pylist()
DRIFT_TR = _tr(
    {
        "mof:a": dict(zip(map(str, DRIFT_DAYS), [1.0, 1.0, 2.0, 2.0, 2.0, 2.0], strict=True)),
        "mof:b": dict(zip(map(str, DRIFT_DAYS), [1.0, 1.0, 1.0, 0.5, 0.5, 1.0], strict=True)),
    }
)
BOTH = _intervals([("mof:a", "2024-01-01", None), ("mof:b", "2024-01-01", None)])


def test_quarterly_rebalance_lets_weights_drift_until_the_quarter():
    index = equal_weight_index(BOTH, DRIFT_TR, DRIFT_SESSIONS, rebalance="quarterly")
    assert index.schema == INDEX_SCHEMA
    assert list(_levels(index).values()) == pytest.approx([1.0, 1.0, 1.5, 1.25, 1.25, 1.875])


def test_monthly_rebalance_resets_weights_each_month():
    index = equal_weight_index(BOTH, DRIFT_TR, DRIFT_SESSIONS, rebalance="monthly")
    assert list(_levels(index).values()) == pytest.approx([1.0, 1.0, 1.5, 1.125, 1.125, 1.6875])


def test_equal_weight_index_is_deterministic_whatever_the_row_order():
    shuffled = DRIFT_TR.take(list(reversed(range(DRIFT_TR.num_rows))))
    a = equal_weight_index(BOTH, DRIFT_TR, DRIFT_SESSIONS, rebalance="quarterly")
    b = equal_weight_index(BOTH, shuffled, DRIFT_SESSIONS, rebalance="quarterly")
    assert a.equals(b)


def test_company_grouping_merges_share_lines():
    sessions = _sessions("2024-01-02", "2024-01-03")
    tr = _tr(
        {
            "mof:g1": {"2024-01-02": 1.0, "2024-01-03": 1.0},
            "mof:g2": {"2024-01-02": 1.0, "2024-01-03": 1.0},
            "mof:h": {"2024-01-02": 1.0, "2024-01-03": 2.0},
        }
    )
    members = _intervals(
        [
            ("mof:g1", "2024-01-01", None),
            ("mof:g2", "2024-01-01", None),
            ("mof:h", "2024-01-01", None),
        ]
    )
    groups = pa.table(
        {"instrument_id": ["mof:g1", "mof:g2", "mof:h"], "company_id": ["x", "x", "y"]}
    )
    by_line = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    by_company = equal_weight_index(
        members, tr, sessions, rebalance="quarterly", company_groups=groups
    )
    assert _levels(by_line)[D("2024-01-03")] == pytest.approx(1 / 3 + 1 / 3 + 2 / 3)
    # Company x (two lines) weighs 1/2 like company y: 1/4 + 1/4 + 1/2 * 2.
    assert _levels(by_company)[D("2024-01-03")] == pytest.approx(1.5)


def test_company_with_one_member_line_puts_its_whole_weight_on_it():
    sessions = _sessions("2024-01-02", "2024-01-03")
    tr = _tr(
        {
            "mof:g1": {"2024-01-02": 1.0, "2024-01-03": 3.0},
            "mof:g2": {"2024-01-02": 1.0, "2024-01-03": 1.0},
            "mof:h": {"2024-01-02": 1.0, "2024-01-03": 1.0},
        }
    )
    members = _intervals([("mof:g1", "2024-01-01", None), ("mof:h", "2024-01-01", None)])
    groups = pa.table({"instrument_id": ["mof:g1", "mof:g2"], "company_id": ["x", "x"]})
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly", company_groups=groups)
    # mof:h is in no group: a company of its own. mof:g2 is not a member: not held.
    assert _levels(index)[D("2024-01-03")] == pytest.approx(0.5 * 3 + 0.5 * 1)


def test_company_groups_refuse_an_instrument_in_two_companies():
    groups = pa.table({"instrument_id": ["mof:a", "mof:a"], "company_id": ["x", "y"]})
    with pytest.raises(ValueError, match="company"):
        equal_weight_index(
            BOTH, DRIFT_TR, DRIFT_SESSIONS, rebalance="quarterly", company_groups=groups
        )


REMOVAL_SESSIONS = _sessions(
    "2024-01-02", "2024-01-03", "2024-01-31", "2024-02-01", "2024-02-02", "2024-04-01"
)
# mof:a falls to 0.5 and stops trading (delisted) on 2024-01-03; it leaves the index on
# 2024-02-01, mid-quarter. A later stray row for mof:a must never be used once it is sold.
REMOVAL_TR = _tr(
    {
        "mof:a": {"2024-01-02": 1.0, "2024-01-03": 0.5, "2024-02-02": 9.0},
        "mof:b": {
            "2024-01-02": 1.0,
            "2024-01-03": 1.0,
            "2024-01-31": 1.0,
            "2024-02-01": 1.0,
            "2024-02-02": 2.0,
            "2024-04-01": 2.0,
        },
    }
)
REMOVAL = _intervals([("mof:a", "2023-12-29", "2024-02-01"), ("mof:b", "2023-12-29", None)])


def test_member_removed_mid_quarter_exits_at_its_last_level():
    index = equal_weight_index(REMOVAL, REMOVAL_TR, REMOVAL_SESSIONS, rebalance="quarterly")
    # Carried at 0.5 until it leaves, sold at 0.5 into cash (0.25), never at the stray 9.0.
    assert list(_levels(index).values()) == pytest.approx([1.0, 0.75, 0.75, 0.75, 1.25, 1.25])


def test_exit_proceeds_pro_rata_reinvest_in_the_remaining_holdings():
    index = equal_weight_index(
        REMOVAL, REMOVAL_TR, REMOVAL_SESSIONS, rebalance="quarterly", exit_proceeds="pro_rata"
    )
    assert list(_levels(index).values()) == pytest.approx([1.0, 0.75, 0.75, 0.75, 1.5, 1.5])


def test_exit_proceeds_refuse_an_unknown_mode():
    with pytest.raises(ValueError, match="exit_proceeds"):
        equal_weight_index(
            REMOVAL, REMOVAL_TR, REMOVAL_SESSIONS, rebalance="quarterly", exit_proceeds="spread"
        )


def test_member_added_mid_period_waits_for_the_next_rebalance():
    sessions = _sessions("2024-01-02", "2024-01-31", "2024-02-01", "2024-02-02")
    tr = _tr(
        {
            "mof:a": {d: 1.0 for d in ("2024-01-02", "2024-01-31", "2024-02-01", "2024-02-02")},
            "mof:c": {"2024-01-31": 1.0, "2024-02-01": 2.0, "2024-02-02": 4.0},
        }
    )
    members = _intervals([("mof:a", "2024-01-01", None), ("mof:c", "2024-01-15", None)])
    quarterly = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    assert list(_levels(quarterly).values()) == pytest.approx([1.0, 1.0, 1.0, 1.0])
    monthly = equal_weight_index(members, tr, sessions, rebalance="monthly")
    # Bought on 2024-02-01 at level 2.0 (the level of that session), never earlier.
    assert list(_levels(monthly).values()) == pytest.approx([1.0, 1.0, 1.0, 1.5])


def test_member_without_a_level_on_a_session_carries_its_last_level():
    sessions = _sessions("2024-01-02", "2024-01-03", "2024-01-04")
    tr = _tr(
        {
            "mof:a": {"2024-01-02": 1.0, "2024-01-04": 2.0},  # no row on 2024-01-03
            "mof:b": {"2024-01-02": 1.0, "2024-01-03": 1.0, "2024-01-04": 1.0},
        }
    )
    members = _intervals([("mof:a", "2024-01-01", None), ("mof:b", "2024-01-01", None)])
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    assert list(_levels(index).values()) == pytest.approx([1.0, 1.0, 1.5])


def test_member_without_any_level_is_skipped_or_refused():
    sessions = _sessions("2024-01-02", "2024-01-03")
    tr = _tr({"mof:a": {"2024-01-02": 1.0, "2024-01-03": 2.0}})
    members = _intervals([("mof:a", "2024-01-01", None), ("mof:z", "2024-01-01", None)])
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    assert list(_levels(index).values()) == pytest.approx([1.0, 2.0])
    with pytest.raises(ValueError, match="1 member"):
        equal_weight_index(members, tr, sessions, rebalance="quarterly", on_missing="raise")


def test_rows_off_the_sessions_apply_from_the_next_session():
    sessions = _sessions("2024-01-03", "2024-01-05", "2024-01-08")
    tr = _tr(
        {
            # Before the first session: the latest gives the starting level.
            "mof:a": {"2023-12-28": 4.0, "2024-01-02": 2.0, "2024-01-05": 2.0, "2024-01-06": 1.0},
            "mof:b": {"2024-01-03": 1.0, "2024-01-05": 1.0, "2024-01-08": 1.0},
        }
    )
    members = _intervals([("mof:a", "2024-01-01", None), ("mof:b", "2024-01-01", None)])
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    # mof:a bought at 2.0; its 2024-01-06 (a Saturday) row applies on 2024-01-08: 0.25 + 0.5.
    assert list(_levels(index).values()) == pytest.approx([1.0, 1.0, 0.75])


def test_invalid_levels_are_ignored():
    sessions = _sessions("2024-01-02", "2024-01-03")
    tr = _tr({"mof:a": {"2024-01-02": 1.0, "2024-01-03": float("nan")}})
    members = _intervals([("mof:a", "2024-01-01", None)])
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly")
    assert list(_levels(index).values()) == pytest.approx([1.0, 1.0])


def test_duplicate_total_return_rows_are_refused():
    tr = pa.concat_tables([DRIFT_TR, DRIFT_TR.slice(0, 1)])
    with pytest.raises(ValueError, match="duplicate"):
        equal_weight_index(BOTH, tr, DRIFT_SESSIONS, rebalance="quarterly")


def test_annual_fee_is_deducted_by_calendar_days():
    sessions = _sessions("2023-01-03", "2024-01-03")
    tr = _tr({"mof:a": {"2023-01-03": 1.0, "2024-01-03": 1.0}})
    members = _intervals([("mof:a", "2023-01-01", None)])
    index = equal_weight_index(members, tr, sessions, rebalance="quarterly", annual_fee=0.1)
    assert _levels(index)[D("2024-01-03")] == pytest.approx(0.9 ** (365 / 365.25))
    with pytest.raises(ValueError, match="annual_fee"):
        equal_weight_index(members, tr, sessions, rebalance="quarterly", annual_fee=1.0)


def test_no_member_keeps_the_index_flat():
    index = equal_weight_index(
        _intervals([]), _tr({}), _sessions("2024-01-02", "2024-01-03"), rebalance="monthly"
    )
    assert list(_levels(index).values()) == [1.0, 1.0]


def test_no_session_gives_an_empty_index():
    index = equal_weight_index(BOTH, DRIFT_TR, _sessions(), rebalance="monthly")
    assert index.num_rows == 0
    assert index.schema == INDEX_SCHEMA


# --- truncation ---------------------------------------------------------------------------------

AS_OF = datetime(2024, 1, 5, 23, 0, tzinfo=UTC)
TRUNC_DAYS = [D("2024-01-02"), D("2024-01-03"), D("2024-01-04"), D("2024-01-05"), D("2024-01-08")]


def _close(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 21, 0, tzinfo=UTC)


# (session_date, daily return, known_at): first values known at their session close; one
# correction of 2024-01-03 planted with a known_at after AS_OF (a fact from the future).
FACTS = [(d, 0.01, _close(d)) for d in TRUNC_DAYS] + [
    (D("2024-01-03"), 0.05, AS_OF + timedelta(days=3))
]


def _facts(truncate_at: datetime | None) -> list[tuple[date, float, datetime]]:
    return [f for f in FACTS if truncate_at is None or f[2] <= truncate_at]


def _levels_from(values: dict[date, float]) -> pa.Table:
    """Cumulated levels over the sessions that have a value (a truncated build stops early)."""
    days = [d for d in TRUNC_DAYS if d in values]
    level, out = 1.0, []
    for d in days:
        level *= 1.0 + values[d]
        out.append(level)
    return pa.table({"session_date": pa.array(days, pa.date32()), "level": out})


def _latest_build(as_of: datetime, *, truncate: bool) -> pa.Table:
    """Look-ahead: every session uses the latest value of its fact, whenever it was known."""
    latest: dict[date, float] = {}
    for d, value, _ in sorted(_facts(as_of if truncate else None), key=lambda f: f[2]):
        latest[d] = value
    return _levels_from(latest)


def _asof_build(as_of: datetime, *, truncate: bool) -> pa.Table:
    """Point in time: session t uses the facts known at t's close only."""
    facts = _facts(as_of if truncate else None)
    values = {}
    for d in TRUNC_DAYS:
        known = sorted((f for f in facts if f[0] == d and f[2] <= _close(d)), key=lambda f: f[2])
        if known:
            values[d] = known[-1][1]
    return _levels_from(values)


def test_truncation_detects_lookahead():
    differing = truncation_check(_latest_build, AS_OF)
    # The planted correction moves 2024-01-03 and every later level up to AS_OF; 2024-01-08 is
    # after AS_OF and never compared.
    assert differing == ["2024-01-03", "2024-01-04", "2024-01-05"]


def test_truncation_passes_a_point_in_time_build():
    assert truncation_check(_asof_build, AS_OF) == []


def test_truncation_horizon_compares_the_last_sessions_only():
    assert truncation_check(_latest_build, AS_OF, horizon_sessions=1) == ["2024-01-05"]
    with pytest.raises(ValueError, match="horizon_sessions"):
        truncation_check(_latest_build, AS_OF, horizon_sessions=0)


def test_truncation_reports_a_session_present_in_one_build_only():
    def build(as_of: datetime, *, truncate: bool) -> pa.Table:
        table = _asof_build(as_of, truncate=truncate)
        return table.slice(1) if truncate else table

    assert truncation_check(build, AS_OF) == ["2024-01-02"]


def test_truncation_tolerance_absorbs_float_noise():
    def build(as_of: datetime, *, truncate: bool) -> pa.Table:
        table = _asof_build(as_of, truncate=truncate)
        if truncate:
            return table
        bumped = [v + 1e-12 for v in table.column("level").to_pylist()]
        return table.set_column(1, "level", pa.array(bumped))

    assert truncation_check(build, AS_OF) != []
    assert truncation_check(build, AS_OF, tolerance=1e-9) == []


def test_truncation_refuses_a_naive_as_of_and_mismatched_columns():
    with pytest.raises(ValueError, match="timezone"):
        truncation_check(_asof_build, datetime(2024, 1, 5))

    def build(as_of: datetime, *, truncate: bool) -> pa.Table:
        table = _asof_build(as_of, truncate=truncate)
        return table.rename_columns(["session_date", "other"]) if truncate else table

    with pytest.raises(ValueError, match="columns"):
        truncation_check(build, AS_OF)


# --- series comparison --------------------------------------------------------------------------


def test_compare_identical_series_passes():
    a = _series({"2024-01-02": 1.0, "2024-01-03": 1.1})
    result = compare_series(a, a, tolerance=1e-9)
    assert result == ComparisonResult(
        max_abs_diff=0.0,
        sessions_compared=2,
        sessions_over=0,
        passed=True,
        tracking_difference=pytest.approx(0.0),
        only_a=0,
        only_b=0,
    )


def test_compare_series_rebases_and_counts_sessions_over_tolerance():
    ours = _series({"2024-01-02": 1.0, "2024-01-03": 1.1, "2024-01-04": 1.2})
    vendor = _series(
        {"2024-01-02": 100.0, "2024-01-03": 110.0, "2024-01-04": 125.0, "2024-01-05": 1}
    )
    result = compare_series(ours, vendor, tolerance=0.01)
    assert result.sessions_compared == 3
    assert result.max_abs_diff == pytest.approx(0.05)
    assert result.sessions_over == 1
    assert not result.passed
    assert (result.only_a, result.only_b) == (0, 1)
    raw = compare_series(ours, ours, tolerance=0.0, rebase=False)
    assert raw.passed


def test_compare_series_reports_the_annualised_tracking_difference():
    a = _series({"2023-01-03": 1.0, "2024-01-03": 1.1})
    b = _series({"2023-01-03": 1.0, "2024-01-03": 1.0})
    result = compare_series(a, b, tolerance=1.0)
    assert result.tracking_difference == pytest.approx(1.1 ** (365.25 / 365) - 1.0)


def test_compare_series_on_another_column_and_key():
    a = pa.table({"day": [D("2024-01-02"), D("2024-01-03")], "tr_index": [1.0, 2.0]})
    result = compare_series(a, a, tolerance=0.0, on="day", column="tr_index")
    assert result.passed and result.sessions_compared == 2


def test_compare_series_with_nothing_in_common_fails():
    a = _series({"2024-01-02": 1.0})
    b = _series({"2024-01-03": 1.0})
    result = compare_series(a, b, tolerance=1.0)
    assert result.sessions_compared == 0
    assert not result.passed
    assert result.tracking_difference is None


def test_compare_series_refuses_duplicate_sessions_and_bad_tolerance():
    a = _series({"2024-01-02": 1.0})
    with pytest.raises(ValueError, match="duplicate"):
        compare_series(pa.concat_tables([a, a]), a, tolerance=0.0)
    with pytest.raises(ValueError, match="tolerance"):
        compare_series(a, a, tolerance=-1.0)


# --- survivors-only delta -----------------------------------------------------------------------


def test_survivors_delta_positive_when_a_loser_is_dropped():
    days = ["2023-01-03", "2023-03-01", "2023-06-01", "2023-09-01", "2024-01-03"]
    sessions = _sessions(*days)
    tr = _tr(
        {
            "mof:loser": dict(zip(days, [1.0, 0.2, 0.1, 0.1, 0.1], strict=True)),
            "mof:s1": dict(zip(days, [1.0, 1.0, 1.1, 1.2, 1.3], strict=True)),
            "mof:s2": dict(zip(days, [1.0, 1.05, 1.1, 1.15, 1.2], strict=True)),
        }
    )
    members = _intervals(
        [
            ("mof:loser", "2023-01-01", "2023-06-01"),
            ("mof:s1", "2023-01-01", None),
            ("mof:s2", "2023-05-01", None),  # joined later: back-filled for the survivors
        ]
    )
    result = survivors_only_delta(
        members, tr, sessions, as_of=datetime(2024, 1, 3, 22, tzinfo=UTC), rebalance="quarterly"
    )
    assert isinstance(result, SurvivorsResult)
    assert result.full_index.schema == INDEX_SCHEMA
    assert result.survivors_index.num_rows == len(days)
    full_end = _levels(result.full_index)[D("2024-01-03")]
    surv_end = _levels(result.survivors_index)[D("2024-01-03")]
    assert surv_end > full_end
    assert result.annualised_delta is not None and result.annualised_delta > 0


def test_survivors_are_the_members_at_as_of_and_sessions_stop_there():
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    tr = _tr({"mof:a": dict.fromkeys(days, 1.0), "mof:b": dict.fromkeys(days, 1.0)})
    members = _intervals([("mof:a", "2024-01-01", "2024-01-03"), ("mof:b", "2024-01-01", None)])
    result = survivors_only_delta(
        members, tr, _sessions(*days), as_of=D("2024-01-03"), rebalance="monthly"
    )
    assert result.full_index.column("session_date").to_pylist() == [D(d) for d in days[:2]]
    with pytest.raises(ValueError, match="timezone"):
        survivors_only_delta(
            members, tr, _sessions(*days), as_of=datetime(2024, 1, 3), rebalance="monthly"
        )

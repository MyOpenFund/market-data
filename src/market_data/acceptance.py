"""Acceptance computations (spec §11): pure functions over the store recipes' Arrow outputs.

They prove, on real data, that a backtest universe built from this repo's tables is
look-ahead-free and survivorship-free. Inputs are the outputs of open-data-store's market recipes:
``membership_intervals`` (``INTERVALS_SCHEMA``: half-open ``[valid_from, valid_to)``, ``valid_to``
null while open), ``total_return`` (``TOTAL_RETURN_SCHEMA``: instrument_id, session_date,
tr_index, flags) and ``sessions`` (a table with a ``session_date`` column). Nothing here reads a
store, a file or the network; nothing names a member other than by its opaque ``instrument_id``.

``equal_weight_index`` (acceptance test 4, vs the RSP ETF; test 5 through
``survivors_only_delta``)
    An equal-weight index level per session, based 1.0 on the first session:

    - Rebalance dates (``rebalance_dates``): the first session of the sessions table, then the
      first session of each month (``"monthly"``) or of January, April, July and October
      (``"quarterly"``) present in the table. At the close of a rebalance date the portfolio is
      valued, then its value is split equally across the **member companies** of that date (each
      instrument is its own company unless ``company_groups`` [instrument_id, company_id] puts
      share lines together; a company's weight is split equally across its member lines), every
      member bought at its total-return level of that date. Membership is the intervals' as of
      that date: nothing later is looked at.
    - Between rebalances, holdings drift with each one's total-return level (buy and hold). A
      member added between rebalances waits for the next rebalance.
    - A holding that leaves the index between rebalances (the first session not covered by its
      intervals) is sold on that session at its last available total-return level (a delisted
      name's last level: delisting exit, spec D8). ``exit_proceeds`` (default ``"cash"``): the
      proceeds stay in cash, at zero return, until the next rebalance; ``"pro_rata"``: they are
      reinvested in the remaining holdings in proportion to their value (no holding left: cash).
    - A member with no total-return row on a session carries its last level (a gap in the
      recipe's output is a zero return there, never a guess of a later value). A ``tr_index``
      that is null, not finite or not positive is ignored (the previous level carries). A row
      dated on a non-session (should not happen) applies from the next session; rows before the
      first session give the starting levels.
    - A member with no level at all on or before a rebalance date cannot be bought:
      ``on_missing="skip"`` (default) leaves it out of that rebalance (its weight goes to the
      others: measure coverage first, acceptance test 1); ``"raise"`` refuses with
      ``ValueError``.
    - ``annual_fee`` (default 0.0; e.g. the ETF's expense ratio for test 4) is deducted
      continuously: each session the level is multiplied by ``(1 - annual_fee) **
      (calendar days since the previous session / DAYS_PER_YEAR)``.
    - The recipe's ``flags`` are not read: check them in the coverage and quality reports.

    Deterministic: the same inputs in any row order give the same table (``INDEX_SCHEMA``).

``truncation_check`` (acceptance test 2)
    "Recompute with every fact known after T removed; nothing up to T may move." The caller
    supplies ``build_fn(as_of, *, truncate)`` returning a table keyed by ``session_date`` (one row
    per session, other columns compared):

    - ``truncate=True``: built from inputs restricted to the facts known at ``as_of``
      (``known_at <= as_of``, e.g. the store read and every recipe run as of ``as_of``);
    - ``truncate=False``: built from every input available now (facts known after ``as_of``
      included), as a production run would be.

    Sessions after the UTC date of ``as_of`` are ignored (``horizon_sessions``: only the last N
    sessions up to it are compared). The result lists the differing sessions as ISO dates: a
    session present in one build only, or a column differing by more than ``tolerance``
    (floats; NaN equals NaN; other types must be equal). Empty means no look-ahead. Identifier
    mappings are exempt by design (spec §9.3): read latest in both builds.

``compare_series`` (acceptance tests 3, 4 and 6)
    Two series keyed by ``on``, value ``column`` (default ``level``), compared on their common
    sessions; with ``rebase`` (default) each is divided by its value on the first common session,
    so a vendor index based 100 compares with ours based 1.0. ``ComparisonResult``: maximum
    absolute difference, sessions compared, sessions over ``tolerance``, ``passed`` (at least one
    session compared and none over), the annualised tracking difference (annualised return of
    ``a`` minus that of ``b`` between the first and last common sessions, calendar days /
    ``DAYS_PER_YEAR``; None with fewer than two common sessions) and the sessions found in one
    series only (reported, never a failure: RSP starts in 2003). Null and NaN values count as
    absent.

``survivors_only_delta`` (acceptance test 5)
    The same equal-weight computation twice over the sessions up to ``as_of``: on the true
    point-in-time membership, and on the survivors only (the members at ``as_of``, treated as
    members over the whole history: the biased universe). ``annualised_delta`` = annualised return
    of the survivors index minus that of the full index (positive: the bias the point-in-time
    universe avoids).
"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

__all__ = [
    "DAYS_PER_YEAR",
    "INDEX_SCHEMA",
    "ComparisonResult",
    "SurvivorsResult",
    "compare_series",
    "equal_weight_index",
    "rebalance_dates",
    "survivors_only_delta",
    "truncation_check",
]

DAYS_PER_YEAR = 365.25
INDEX_SCHEMA = pa.schema([("session_date", pa.date32()), ("level", pa.float64())])
REBALANCES = ("monthly", "quarterly")
EXIT_PROCEEDS = ("cash", "pro_rata")
ON_MISSING = ("skip", "raise")
MAX_LISTED = 5

Rebalance = Literal["monthly", "quarterly"]
ExitProceeds = Literal["cash", "pro_rata"]
OnMissing = Literal["skip", "raise"]


# --- helpers ------------------------------------------------------------------------------------


def _require_columns(table: pa.Table, what: str, columns: Sequence[str]) -> None:
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        raise ValueError(f"{what} lacks column(s) {missing}")


def _session_dates(sessions: pa.Table) -> list[date]:
    _require_columns(sessions, "sessions", ["session_date"])
    values = sessions.column("session_date").to_pylist()
    if any(v is None for v in values):
        raise ValueError("sessions has a null session_date")
    return sorted(set(values))


def _as_of_date(as_of: date) -> date:
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be a date or a timezone-aware datetime")
        return as_of.astimezone(UTC).date()
    return as_of


def _annualised(first: float, last: float, days: int) -> float | None:
    if days <= 0 or not (first > 0 and last > 0):
        return None
    return (last / first) ** (DAYS_PER_YEAR / days) - 1.0


def _index_return(index: pa.Table) -> float | None:
    if index.num_rows < 2:
        return None
    days = index.column("session_date").to_pylist()
    levels = index.column("level").to_pylist()
    return _annualised(levels[0], levels[-1], (days[-1] - days[0]).days)


# --- equal-weight index -------------------------------------------------------------------------


def rebalance_dates(sessions: pa.Table, rebalance: Rebalance) -> list[date]:
    """The rebalance dates of ``sessions``: its first session, then the first session of each
    month (``"monthly"``) or of January, April, July and October (``"quarterly"``)."""
    if rebalance not in REBALANCES:
        raise ValueError(f"rebalance must be one of {REBALANCES}, got {rebalance!r}")
    out: list[date] = []
    seen: set[tuple[int, int]] = set()
    for d in _session_dates(sessions):
        period = (d.year, d.month if rebalance == "monthly" else (d.month - 1) // 3)
        if period not in seen:
            seen.add(period)
            out.append(d)
    return out


def _companies(instruments: list[str], company_groups: pa.Table | None) -> np.ndarray:
    """Company number of each instrument (an ungrouped instrument is a company of its own)."""
    company_of: dict[str, str] = {}
    if company_groups is not None:
        _require_columns(company_groups, "company_groups", ["instrument_id", "company_id"])
        ids = company_groups.column("instrument_id").to_pylist()
        companies = company_groups.column("company_id").to_pylist()
        for instrument, company in zip(ids, companies, strict=True):
            if instrument is None or company is None:
                raise ValueError("company_groups has a null instrument_id or company_id")
            if company_of.setdefault(instrument, company) != company:
                raise ValueError(f"company_groups puts {instrument!r} in two companies")
    keys = [("group", company_of[i]) if i in company_of else ("own", i) for i in instruments]
    numbers = {k: n for n, k in enumerate(sorted(set(keys)))}
    return np.array([numbers[k] for k in keys], dtype=np.int64)


def _level_matrix(tr: pa.Table, instruments: list[str], days: list[date]) -> np.ndarray:
    """Total-return levels, sessions x instruments, carried forward (NaN before the first)."""
    _require_columns(tr, "tr", ["instrument_id", "session_date", "tr_index"])
    levels = np.full((len(days), len(instruments)), np.nan)
    position = pc.index_in(tr.column("instrument_id"), value_set=pa.array(instruments, pa.string()))
    keep = pc.is_valid(position)
    tr = tr.filter(keep)
    if tr.num_rows == 0 or not days:
        return levels
    if tr.column("session_date").null_count:
        raise ValueError("tr has a null session_date")
    cols = pc.filter(position, keep).to_numpy(zero_copy_only=False).astype(np.int64)
    dates = tr.column("session_date").cast(pa.date32()).to_numpy(zero_copy_only=False)
    dates = dates.astype("datetime64[D]").astype(np.int64)
    values = pc.fill_null(tr.column("tr_index").cast(pa.float64()), math.nan).to_numpy(
        zero_copy_only=False
    )
    # By (date, instrument): a later row overwrites an earlier one mapped to the same session.
    order = np.lexsort((cols, dates))
    cols, dates, values = cols[order], dates[order], values[order]
    same = (dates[1:] == dates[:-1]) & (cols[1:] == cols[:-1])
    if same.any():
        k = int(np.flatnonzero(same)[0])
        day = date.fromordinal(date(1970, 1, 1).toordinal() + int(dates[k]))
        raise ValueError(f"tr has duplicate rows for {instruments[cols[k]]!r} on {day.isoformat()}")
    valid = np.isfinite(values) & (values > 0)
    session_days = np.array(days, dtype="datetime64[D]").astype(np.int64)
    rows = np.searchsorted(session_days, dates[valid], side="left")
    inside = rows < len(days)
    rows, cols, values = rows[inside], cols[valid][inside], values[valid][inside]
    # Keep the last (latest-dated) row per cell explicitly: the reversed arrays' first occurrence.
    _, first = np.unique((rows * len(instruments) + cols)[::-1], return_index=True)
    pick = len(rows) - 1 - first
    levels[rows[pick], cols[pick]] = values[pick]
    # Carry forward: index of the last valid row at or before each session (0 before the first,
    # whose cell is then NaN unless row 0 itself is valid).
    last = np.where(~np.isnan(levels), np.arange(len(days))[:, None], 0)
    np.maximum.accumulate(last, axis=0, out=last)
    return np.take_along_axis(levels, last, axis=0)


def _membership_matrix(intervals: pa.Table, instruments: list[str], days: list[date]) -> np.ndarray:
    column = {i: j for j, i in enumerate(instruments)}
    member = np.zeros((len(days), len(instruments)), dtype=bool)
    ids = intervals.column("instrument_id").to_pylist()
    starts = intervals.column("valid_from").to_pylist()
    ends = intervals.column("valid_to").to_pylist()
    for instrument, start, end in zip(ids, starts, ends, strict=True):
        if start is None:
            raise ValueError("intervals has a null valid_from")
        lo = bisect_left(days, start)
        hi = len(days) if end is None else bisect_left(days, end)
        member[lo:hi, column[instrument]] = True
    return member


def _weights(members: np.ndarray, company: np.ndarray) -> np.ndarray:
    """Equal weight per member company, split equally across its member lines."""
    weights = np.zeros(len(members))
    if not members.any():
        return weights
    lines = np.bincount(company[members], minlength=int(company.max()) + 1)
    n_companies = int((lines > 0).sum())
    weights[members] = 1.0 / (n_companies * lines[company[members]])
    return weights


def equal_weight_index(
    intervals: pa.Table,
    tr: pa.Table,
    sessions: pa.Table,
    *,
    rebalance: Rebalance,
    company_groups: pa.Table | None = None,
    exit_proceeds: ExitProceeds = "cash",
    on_missing: OnMissing = "skip",
    annual_fee: float = 0.0,
) -> pa.Table:
    """Equal-weight index level per session of ``sessions`` (``INDEX_SCHEMA``), based 1.0 on the
    first session. See the module docstring for the rules."""
    if exit_proceeds not in EXIT_PROCEEDS:
        raise ValueError(f"exit_proceeds must be one of {EXIT_PROCEEDS}, got {exit_proceeds!r}")
    if on_missing not in ON_MISSING:
        raise ValueError(f"on_missing must be one of {ON_MISSING}, got {on_missing!r}")
    if not (math.isfinite(annual_fee) and 0.0 <= annual_fee < 1.0):
        raise ValueError(f"annual_fee must be in [0, 1), got {annual_fee!r}")
    _require_columns(intervals, "intervals", ["instrument_id", "valid_from", "valid_to"])
    days = _session_dates(sessions)
    rebalances = set(rebalance_dates(sessions, rebalance))
    ids = intervals.column("instrument_id").to_pylist()
    if any(i is None for i in ids):
        raise ValueError("intervals has a null instrument_id")
    instruments = sorted(set(ids))
    company = _companies(instruments, company_groups)
    levels = _level_matrix(tr, instruments, days)
    member = _membership_matrix(intervals, instruments, days)

    shares = np.zeros(len(instruments))
    held = np.zeros(len(instruments), dtype=bool)
    cash = 1.0
    out: list[float] = []
    for t, day in enumerate(days):
        level = levels[t]
        if t > 0 and annual_fee > 0.0:
            factor = (1.0 - annual_fee) ** ((day - days[t - 1]).days / DAYS_PER_YEAR)
            shares *= factor
            cash *= factor
        value = cash + float(np.sum(shares[held] * level[held]))
        if day in rebalances:
            buyable = member[t] & ~np.isnan(level)
            missing = member[t] & np.isnan(level)
            if missing.any() and on_missing == "raise":
                listed = [instruments[j] for j in np.flatnonzero(missing)[:MAX_LISTED]]
                raise ValueError(
                    f"{int(missing.sum())} member(s) without a total-return level on "
                    f"{day.isoformat()} (rebalance), e.g. {listed}"
                )
            weights = _weights(buyable, company)
            shares = np.zeros(len(instruments))
            shares[buyable] = value * weights[buyable] / level[buyable]
            held = buyable
            cash = 0.0 if buyable.any() else value
        else:
            leaving = held & ~member[t]
            if leaving.any():
                proceeds = float(np.sum(shares[leaving] * level[leaving]))
                shares[leaving] = 0.0
                held = held & ~leaving
                remaining = float(np.sum(shares[held] * level[held]))
                if exit_proceeds == "pro_rata" and remaining > 0.0:
                    shares[held] *= (remaining + proceeds) / remaining
                else:
                    cash += proceeds
        out.append(value)
    return pa.table(
        {"session_date": pa.array(days, pa.date32()), "level": pa.array(out, pa.float64())},
        schema=INDEX_SCHEMA,
    )


# --- truncation ---------------------------------------------------------------------------------


def _by_session(table: pa.Table, on: str, what: str) -> dict[date, dict[str, object]]:
    _require_columns(table, what, [on])
    rows: dict[date, dict[str, object]] = {}
    for row in table.to_pylist():
        key = row.pop(on)
        if key is None:
            raise ValueError(f"{what} has a null {on}")
        if key in rows:
            raise ValueError(f"{what} has duplicate rows for {on} {key.isoformat()}")
        rows[key] = row
    return rows


def _same(a: object, b: object, tolerance: float) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return abs(a - b) <= tolerance
    return a == b


def truncation_check(
    build_fn: Callable[..., pa.Table],
    as_of: datetime,
    *,
    horizon_sessions: int | None = None,
    tolerance: float = 0.0,
    on: str = "session_date",
) -> list[str]:
    """Sessions up to ``as_of`` (tz-aware) where ``build_fn(as_of, truncate=True)`` and
    ``build_fn(as_of, truncate=False)`` differ, as sorted ISO dates; empty means no look-ahead.
    See the module docstring for the ``build_fn`` contract."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be a timezone-aware datetime")
    if horizon_sessions is not None and (
        isinstance(horizon_sessions, bool) or horizon_sessions <= 0
    ):
        raise ValueError(f"horizon_sessions must be a positive integer, got {horizon_sessions!r}")
    if not (math.isfinite(tolerance) and tolerance >= 0.0):
        raise ValueError(f"tolerance must be finite and >= 0, got {tolerance!r}")
    truncated = build_fn(as_of, truncate=True)
    full = build_fn(as_of, truncate=False)
    if sorted(truncated.column_names) != sorted(full.column_names):
        raise ValueError(
            f"the two builds have different columns: {sorted(truncated.column_names)} vs "
            f"{sorted(full.column_names)}"
        )
    cut = as_of.astimezone(UTC).date()
    a = _by_session(truncated, on, "the truncated build")
    b = _by_session(full, on, "the full build")
    days = sorted(d for d in set(a) | set(b) if d <= cut)
    if horizon_sessions is not None:
        days = days[-horizon_sessions:]
    differing = []
    for d in days:
        ra, rb = a.get(d), b.get(d)
        if ra is None or rb is None or any(not _same(ra[c], rb[c], tolerance) for c in ra):
            differing.append(d.isoformat())
    return differing


# --- series comparison --------------------------------------------------------------------------


@dataclass(frozen=True)
class ComparisonResult:
    max_abs_diff: float | None  # None when no session is compared
    sessions_compared: int
    sessions_over: int
    passed: bool
    tracking_difference: float | None  # annualised return of a minus b
    only_a: int  # sessions with a value in a only
    only_b: int


def _values(table: pa.Table, on: str, column: str, what: str) -> dict[date, float]:
    _require_columns(table, what, [on, column])
    out: dict[date, float] = {}
    keys = table.column(on).to_pylist()
    values = table.column(column).to_pylist()
    for key, value in zip(keys, values, strict=True):
        if key is None:
            raise ValueError(f"{what} has a null {on}")
        if key in out:
            raise ValueError(f"{what} has duplicate rows for {on} {key.isoformat()}")
        out[key] = math.nan if value is None else float(value)
    return {k: v for k, v in out.items() if not math.isnan(v)}


def compare_series(
    a: pa.Table,
    b: pa.Table,
    *,
    tolerance: float,
    on: str = "session_date",
    column: str = "level",
    rebase: bool = True,
) -> ComparisonResult:
    """Compare two series on their common sessions; see the module docstring."""
    if not (math.isfinite(tolerance) and tolerance >= 0.0):
        raise ValueError(f"tolerance must be finite and >= 0, got {tolerance!r}")
    va = _values(a, on, column, "a")
    vb = _values(b, on, column, "b")
    common = sorted(set(va) & set(vb))
    only_a, only_b = len(set(va) - set(vb)), len(set(vb) - set(va))
    if not common:
        return ComparisonResult(None, 0, 0, False, None, only_a, only_b)
    xa = np.array([va[d] for d in common])
    xb = np.array([vb[d] for d in common])
    if rebase:
        if xa[0] == 0.0 or xb[0] == 0.0:
            raise ValueError(f"cannot rebase: a value is 0 on {common[0].isoformat()}")
        xa, xb = xa / xa[0], xb / xb[0]
    diff = np.abs(xa - xb)
    over = int((diff > tolerance).sum())
    days = (common[-1] - common[0]).days
    ra, rb = _annualised(xa[0], xa[-1], days), _annualised(xb[0], xb[-1], days)
    tracking = None if ra is None or rb is None else ra - rb
    return ComparisonResult(
        max_abs_diff=float(diff.max()),
        sessions_compared=len(common),
        sessions_over=over,
        passed=over == 0,
        tracking_difference=tracking,
        only_a=only_a,
        only_b=only_b,
    )


# --- survivors-only delta -----------------------------------------------------------------------


@dataclass(frozen=True)
class SurvivorsResult:
    full_index: pa.Table
    survivors_index: pa.Table
    annualised_delta: float | None  # survivors minus full; positive = the bias avoided


def survivors_only_delta(
    intervals: pa.Table,
    tr: pa.Table,
    sessions: pa.Table,
    *,
    as_of: date,
    rebalance: Rebalance,
    company_groups: pa.Table | None = None,
    exit_proceeds: ExitProceeds = "cash",
    on_missing: OnMissing = "skip",
    annual_fee: float = 0.0,
) -> SurvivorsResult:
    """Equal-weight index on the point-in-time membership vs on the members at ``as_of`` (a date,
    or a tz-aware datetime read as its UTC date) back-filled; see the module docstring."""
    cut = _as_of_date(as_of)
    _require_columns(intervals, "intervals", ["instrument_id", "valid_from", "valid_to"])
    days = [d for d in _session_dates(sessions) if d <= cut]
    window = pa.table({"session_date": pa.array(days, pa.date32())})
    survivors = sorted(
        {
            i
            for i, start, end in zip(
                intervals.column("instrument_id").to_pylist(),
                intervals.column("valid_from").to_pylist(),
                intervals.column("valid_to").to_pylist(),
                strict=True,
            )
            if start <= cut and (end is None or cut < end)
        }
    )
    start = days[0] if days else cut
    backfilled = pa.table(
        {
            "instrument_id": pa.array(survivors, pa.string()),
            "valid_from": pa.array([start] * len(survivors), pa.date32()),
            "valid_to": pa.array([None] * len(survivors), pa.date32()),
        }
    )
    options = {
        "rebalance": rebalance,
        "company_groups": company_groups,
        "exit_proceeds": exit_proceeds,
        "on_missing": on_missing,
        "annual_fee": annual_fee,
    }
    full = equal_weight_index(intervals, tr, window, **options)
    biased = equal_weight_index(backfilled, tr, window, **options)
    rf, rs = _index_return(full), _index_return(biased)
    delta = None if rf is None or rs is None else rs - rf
    return SurvivorsResult(full_index=full, survivors_index=biased, annualised_delta=delta)

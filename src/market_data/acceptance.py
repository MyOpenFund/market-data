"""Acceptance computations (spec §11): pure functions over the store recipes' Arrow outputs.

They prove, on real data, that a backtest universe built from this repo's tables is
look-ahead-free and survivorship-free. Inputs are the outputs of open-data-store's market recipes:
``membership_intervals`` (``INTERVALS_SCHEMA``: half-open ``[valid_from, valid_to)``, ``valid_to``
null while open; **one index per call**: an ``index_code`` column with several codes is refused),
``total_return`` (``TOTAL_RETURN_SCHEMA``: instrument_id, session_date, tr_index, flags) and
``sessions`` (a table with a ``session_date`` column). Every date column must be an Arrow date
(``date32``): a timestamp column is refused with a ``ValueError``. Nothing here reads a store, a
file or the network; nothing names a member other than by its opaque ``instrument_id``.

None of these functions reports a clean result on missing evidence: members that could not be
bought are counted and returned, a truncation check that compared nothing raises, and a
comparison on fewer than ``min_sessions`` common sessions does not pass.

``equal_weight_index`` (acceptance test 4, vs the equal-weight ETF; test 5 through
``survivors_only_delta``)
    An equal-weight index level per session, based 1.0 on the first session:

    - Rebalance dates (``rebalance_dates``): the first session of the sessions table, then the
      first session of each month (``"monthly"``) or of each period starting in one of
      ``rebalance_months`` (``"quarterly"``; default ``QUARTER_MONTHS`` = January, April, July,
      October; a period with no session in its first month rebalances on its first session) in
      the table. At the close of a rebalance date the portfolio is valued, then its value is split
      equally across the **member companies** of that date (each instrument is its own company
      unless ``company_groups`` [instrument_id, company_id] puts share lines together; a
      company's weight is split equally across its member lines), every member bought at its
      total-return level of that date. Membership is the intervals' as of that date: nothing
      later is looked at.
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
      first session give the starting levels (counted as dated on the first session).
    - A member cannot be bought at a rebalance when it has no level on or before that date, or,
      with ``max_stale_sessions`` = N (default None: unlimited), when its last total-return row is
      more than N sessions old (a delisted name still listed by a stale snapshot). Held positions
      are always valued at their carried level. ``on_missing`` decides: ``"skip"`` (default)
      leaves such members out of that rebalance (their weight goes to the others) and counts
      them; ``"raise"`` refuses with ``ValueError``. Measure coverage first (acceptance test 1).
    - ``annual_fee`` (default 0.0; e.g. the ETF's expense ratio for test 4) is deducted
      continuously: each session the level is multiplied by ``(1 - annual_fee) **
      (calendar days since the previous session / DAYS_PER_YEAR)``.
    - The recipe's ``flags`` are not read: check them in the coverage and quality reports.

    Returns an ``IndexResult``: ``levels`` (``INDEX_SCHEMA``: session_date, level) and
    ``rebalances`` (``REBALANCES_SCHEMA``, one row per rebalance date: ``members``, ``bought``,
    ``skipped`` = members - bought, of which ``stale`` (refused by ``max_stale_sessions``),
    ``carried`` (bought at a level from an earlier session) and ``max_carry_sessions`` (the
    oldest level bought, in sessions; 0 when none is carried)); ``skipped`` and ``stale`` also as
    totals. Deterministic: the same inputs in any row order give the same tables.

``truncation_check`` (acceptance test 2)
    "Recompute with every fact known after T removed; nothing up to T may move." Your
    ``build_fn(as_of, *, truncate)`` returns a table keyed by ``session_date`` (one row per
    session, other columns compared):

    - ``truncate=True``: your build_fn removes every fact known after ``as_of`` from its inputs
      (``known_at <= as_of``: the store read, and every recipe run, as of ``as_of``);
    - ``truncate=False``: it builds from every input available now (facts known after ``as_of``
      included), as a production run would.

    The check is only as good as that contract: a build_fn that ignores ``truncate`` builds the
    same table twice and always passes. ``as_of`` must be at or after the close of the last
    session it covers (a T inside a session sees that session's close in one build only: a
    false difference). Sessions after the UTC date of ``as_of`` are ignored
    (``horizon_sessions``: only the last N sessions up to it are compared). The result lists the
    differing sessions as ISO dates: a session present in one build only, or a column differing
    by more than ``tolerance`` (numbers, int or float; NaN equals NaN; other types must be
    equal). Empty means no look-ahead; no session compared at all raises ``ValueError``
    (nothing proven). Identifier mappings are exempt by design (spec §9.3): read latest in both
    builds.

``compare_series`` (acceptance tests 3, 4 and 6)
    Two series keyed by ``on``, value ``column`` (default ``level``), compared on their common
    sessions; with ``rebase`` (default) each is divided by its value on the first common session,
    so a vendor index based 100 compares with ours based 1.0. ``ComparisonResult``: maximum
    absolute difference, sessions compared, sessions over ``tolerance``, ``passed`` (at least
    ``min_sessions`` (default 2) sessions compared and none over), ``reason`` (why it did not
    pass; None when it passed), the annualised tracking difference (annualised return of ``a``
    minus that of ``b`` between the first and last common sessions, calendar days /
    ``DAYS_PER_YEAR``; None with fewer than two common sessions) and the sessions found in one
    series only (reported, never a failure: the ETF starts in 2003). Null and NaN values count as
    absent.

``survivors_only_delta`` (acceptance test 5)
    The same equal-weight computation twice over the sessions up to ``as_of``: on the true
    point-in-time membership, and on the survivors only (the members at ``as_of``, treated as
    members over the whole history: the biased universe). The two get separate missing-level
    policies: ``full_on_missing="raise"`` (default: a dead member with no total return would
    silently remove the very bias measured) and ``survivors_on_missing="skip"`` (default: a
    survivor listed after the start cannot be bought before its first price). Both indices'
    skipped counts are returned. ``annualised_delta`` = annualised return of the survivors index
    minus that of the full index (positive: the bias the point-in-time universe avoids).
"""

from __future__ import annotations

import math
import operator
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
    "QUARTER_MONTHS",
    "REBALANCES_SCHEMA",
    "ComparisonResult",
    "IndexResult",
    "SurvivorsResult",
    "compare_series",
    "equal_weight_index",
    "rebalance_dates",
    "survivors_only_delta",
    "truncation_check",
]

DAYS_PER_YEAR = 365.25
QUARTER_MONTHS = (1, 4, 7, 10)
INDEX_SCHEMA = pa.schema([("session_date", pa.date32()), ("level", pa.float64())])
REBALANCES_SCHEMA = pa.schema(
    [
        ("rebalance_date", pa.date32()),
        ("members", pa.int64()),
        ("bought", pa.int64()),
        ("skipped", pa.int64()),
        ("stale", pa.int64()),
        ("carried", pa.int64()),
        ("max_carry_sessions", pa.int64()),
    ]
)
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


def _require_date(table: pa.Table, column: str, what: str, *, null_type_ok: bool = False) -> None:
    kind = table.schema.field(column).type
    if pa.types.is_date(kind) or (null_type_ok and pa.types.is_null(kind)):
        return
    raise ValueError(f"{what}.{column} must be a date column (date32), got {kind}")


def _session_dates(sessions: pa.Table) -> list[date]:
    _require_columns(sessions, "sessions", ["session_date"])
    _require_date(sessions, "session_date", "sessions")
    values = sessions.column("session_date").to_pylist()
    if any(v is None for v in values):
        raise ValueError("sessions has a null session_date")
    return sorted(set(values))


def _check_intervals(intervals: pa.Table) -> list[str]:
    """Validate the intervals; return their instrument ids (sorted, distinct)."""
    _require_columns(intervals, "intervals", ["instrument_id", "valid_from", "valid_to"])
    _require_date(intervals, "valid_from", "intervals", null_type_ok=True)
    _require_date(intervals, "valid_to", "intervals", null_type_ok=True)
    if intervals.column("valid_from").null_count:
        raise ValueError("intervals has a null valid_from")
    if intervals.column("instrument_id").null_count:
        raise ValueError("intervals has a null instrument_id")
    if "index_code" in intervals.column_names:
        codes = set(intervals.column("index_code").drop_null().to_pylist())
        if len(codes) > 1:
            raise ValueError(f"intervals hold {len(codes)} indices: pass one index per call")
    return sorted(set(intervals.column("instrument_id").to_pylist()))


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


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


# --- equal-weight index -------------------------------------------------------------------------


def _months(rebalance: Rebalance, rebalance_months: Sequence[int] | None) -> tuple[int, ...]:
    if rebalance not in REBALANCES:
        raise ValueError(f"rebalance must be one of {REBALANCES}, got {rebalance!r}")
    if rebalance == "monthly":
        if rebalance_months is not None:
            raise ValueError("rebalance_months applies to rebalance='quarterly' only")
        return tuple(range(1, 13))
    months = QUARTER_MONTHS if rebalance_months is None else tuple(rebalance_months)
    if (
        not months
        or any(not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= 12 for m in months)
        or len(set(months)) != len(months)
    ):
        raise ValueError(
            f"rebalance_months must be distinct months 1..12, got {rebalance_months!r}"
        )
    return tuple(sorted(months))


def rebalance_dates(
    sessions: pa.Table, rebalance: Rebalance, *, rebalance_months: Sequence[int] | None = None
) -> list[date]:
    """The rebalance dates of ``sessions``: its first session, then the first session of each
    month (``"monthly"``) or of each period starting in one of ``rebalance_months``
    (``"quarterly"``, default ``QUARTER_MONTHS``)."""
    months = _months(rebalance, rebalance_months)
    out: list[date] = []
    seen: set[tuple[int, int]] = set()
    for d in _session_dates(sessions):
        started = [m for m in months if m <= d.month]
        period = (d.year, started[-1]) if started else (d.year - 1, months[-1])
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


def _level_matrix(
    tr: pa.Table, instruments: list[str], days: list[date]
) -> tuple[np.ndarray, np.ndarray]:
    """Total-return levels, sessions x instruments, carried forward (NaN before the first), and
    the session number of the row each level comes from."""
    _require_columns(tr, "tr", ["instrument_id", "session_date", "tr_index"])
    _require_date(tr, "session_date", "tr")
    levels = np.full((len(days), len(instruments)), np.nan)
    source = np.zeros((len(days), len(instruments)), dtype=np.int64)
    position = pc.index_in(tr.column("instrument_id"), value_set=pa.array(instruments, pa.string()))
    keep = pc.is_valid(position)
    tr = tr.filter(keep)
    if tr.num_rows == 0 or not days:
        return levels, source
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
    return np.take_along_axis(levels, last, axis=0), last


def _membership_matrix(intervals: pa.Table, instruments: list[str], days: list[date]) -> np.ndarray:
    column = {i: j for j, i in enumerate(instruments)}
    member = np.zeros((len(days), len(instruments)), dtype=bool)
    ids = intervals.column("instrument_id").to_pylist()
    starts = intervals.column("valid_from").to_pylist()
    ends = intervals.column("valid_to").to_pylist()
    for instrument, start, end in zip(ids, starts, ends, strict=True):
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


@dataclass(frozen=True)
class IndexResult:
    levels: pa.Table  # INDEX_SCHEMA
    rebalances: pa.Table  # REBALANCES_SCHEMA

    @property
    def skipped(self) -> int:
        """Members left out of a rebalance, summed over the rebalances."""
        return int(pc.sum(self.rebalances.column("skipped")).as_py() or 0)

    @property
    def stale(self) -> int:
        """Of ``skipped``, those refused by ``max_stale_sessions``."""
        return int(pc.sum(self.rebalances.column("stale")).as_py() or 0)


def _check_options(
    exit_proceeds: str, on_missing: str, annual_fee: float, max_stale_sessions: int | None
) -> None:
    if exit_proceeds not in EXIT_PROCEEDS:
        raise ValueError(f"exit_proceeds must be one of {EXIT_PROCEEDS}, got {exit_proceeds!r}")
    if on_missing not in ON_MISSING:
        raise ValueError(f"on_missing must be one of {ON_MISSING}, got {on_missing!r}")
    if not (math.isfinite(annual_fee) and 0.0 <= annual_fee < 1.0):
        raise ValueError(f"annual_fee must be in [0, 1), got {annual_fee!r}")
    if max_stale_sessions is not None:
        try:
            ok = not isinstance(max_stale_sessions, bool) and (
                operator.index(max_stale_sessions) >= 0
            )
        except TypeError:
            ok = False
        if not ok:
            raise ValueError(
                f"max_stale_sessions must be None or an integer >= 0, got {max_stale_sessions!r}"
            )


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
    max_stale_sessions: int | None = None,
    rebalance_months: Sequence[int] | None = None,
) -> IndexResult:
    """Equal-weight index level per session of ``sessions``, based 1.0 on the first session, and
    what each rebalance could not buy. See the module docstring for the rules."""
    _check_options(exit_proceeds, on_missing, annual_fee, max_stale_sessions)
    instruments = _check_intervals(intervals)
    days = _session_dates(sessions)
    rebalances = set(rebalance_dates(sessions, rebalance, rebalance_months=rebalance_months))
    company = _companies(instruments, company_groups)
    levels, source = _level_matrix(tr, instruments, days)
    member = _membership_matrix(intervals, instruments, days)

    shares = np.zeros(len(instruments))
    held = np.zeros(len(instruments), dtype=bool)
    cash = 1.0
    out: list[float] = []
    log: list[tuple[date, int, int, int, int, int, int]] = []
    for t, day in enumerate(days):
        level = levels[t]
        if t > 0 and annual_fee > 0.0:
            factor = (1.0 - annual_fee) ** ((day - days[t - 1]).days / DAYS_PER_YEAR)
            shares *= factor
            cash *= factor
        value = cash + float(np.sum(shares[held] * level[held]))
        if day in rebalances:
            known = ~np.isnan(level)
            age = t - source[t]
            if max_stale_sessions is None:
                stale = np.zeros(len(instruments), dtype=bool)
            else:
                stale = member[t] & known & (age > max_stale_sessions)
            buyable = member[t] & known & ~stale
            skipped = member[t] & ~buyable
            if skipped.any() and on_missing == "raise":
                listed = [instruments[j] for j in np.flatnonzero(skipped)[:MAX_LISTED]]
                raise ValueError(
                    f"{int(skipped.sum())} member(s) without a usable total-return level on "
                    f"{day.isoformat()} (rebalance; {int(stale.sum())} stale beyond "
                    f"{max_stale_sessions} sessions), e.g. {listed}"
                )
            carried = buyable & (age > 0)
            log.append(
                (
                    day,
                    int(member[t].sum()),
                    int(buyable.sum()),
                    int(skipped.sum()),
                    int(stale.sum()),
                    int(carried.sum()),
                    int(age[buyable].max()) if buyable.any() else 0,
                )
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
    index = pa.table(
        {"session_date": pa.array(days, pa.date32()), "level": pa.array(out, pa.float64())},
        schema=INDEX_SCHEMA,
    )
    columns = list(zip(*log, strict=True)) if log else [[] for _ in REBALANCES_SCHEMA]
    rebalance_table = pa.table(
        {
            f.name: pa.array(list(c), f.type)
            for f, c in zip(REBALANCES_SCHEMA, columns, strict=True)
        },
        schema=REBALANCES_SCHEMA,
    )
    return IndexResult(levels=index, rebalances=rebalance_table)


# --- truncation ---------------------------------------------------------------------------------


def _by_session(table: pa.Table, on: str, what: str) -> dict[date, dict[str, object]]:
    _require_columns(table, what, [on])
    _require_date(table, on, what)
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
    if _is_number(a) and _is_number(b):
        x, y = float(a), float(b)  # type: ignore[arg-type]
        if math.isnan(x) or math.isnan(y):
            return math.isnan(x) and math.isnan(y)
        return abs(x - y) <= tolerance
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
    ``build_fn(as_of, truncate=False)`` differ, as sorted ISO dates; empty means no look-ahead,
    and no session compared raises ``ValueError``. See the module docstring for the ``build_fn``
    contract and its limits."""
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be a timezone-aware datetime")
    if horizon_sessions is not None and (
        isinstance(horizon_sessions, bool)
        or not isinstance(horizon_sessions, int)
        or horizon_sessions <= 0
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
    if not days:
        raise ValueError(
            f"no session at or before {cut.isoformat()} in either build: nothing was compared, "
            "so no look-ahead can be ruled out"
        )
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
    reason: str | None = None  # why it did not pass; None when it passed


def _values(table: pa.Table, on: str, column: str, what: str) -> dict[date, float]:
    _require_columns(table, what, [on, column])
    _require_date(table, on, what)
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
    min_sessions: int = 2,
) -> ComparisonResult:
    """Compare two series on their common sessions; see the module docstring."""
    if not (math.isfinite(tolerance) and tolerance >= 0.0):
        raise ValueError(f"tolerance must be finite and >= 0, got {tolerance!r}")
    if isinstance(min_sessions, bool) or not isinstance(min_sessions, int) or min_sessions < 1:
        raise ValueError(f"min_sessions must be an integer >= 1, got {min_sessions!r}")
    va = _values(a, on, column, "a")
    vb = _values(b, on, column, "b")
    common = sorted(set(va) & set(vb))
    only_a, only_b = len(set(va) - set(vb)), len(set(vb) - set(va))
    if not common:
        return ComparisonResult(None, 0, 0, False, None, only_a, only_b, "no common session")
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
    reason = None
    if len(common) < min_sessions:
        reason = f"{len(common)} common session(s), fewer than min_sessions={min_sessions}"
    elif over:
        reason = f"{over} session(s) over tolerance {tolerance}"
    return ComparisonResult(
        max_abs_diff=float(diff.max()),
        sessions_compared=len(common),
        sessions_over=over,
        passed=reason is None,
        tracking_difference=tracking,
        only_a=only_a,
        only_b=only_b,
        reason=reason,
    )


# --- survivors-only delta -----------------------------------------------------------------------


@dataclass(frozen=True)
class SurvivorsResult:
    full: IndexResult  # point-in-time membership
    survivors: IndexResult  # members at as_of, back-filled
    annualised_delta: float | None  # survivors minus full; positive = the bias avoided

    @property
    def full_index(self) -> pa.Table:
        return self.full.levels

    @property
    def survivors_index(self) -> pa.Table:
        return self.survivors.levels

    @property
    def full_skipped(self) -> int:
        return self.full.skipped

    @property
    def survivors_skipped(self) -> int:
        return self.survivors.skipped


def survivors_only_delta(
    intervals: pa.Table,
    tr: pa.Table,
    sessions: pa.Table,
    *,
    as_of: date,
    rebalance: Rebalance,
    company_groups: pa.Table | None = None,
    exit_proceeds: ExitProceeds = "cash",
    full_on_missing: OnMissing = "raise",
    survivors_on_missing: OnMissing = "skip",
    annual_fee: float = 0.0,
    max_stale_sessions: int | None = None,
    rebalance_months: Sequence[int] | None = None,
) -> SurvivorsResult:
    """Equal-weight index on the point-in-time membership vs on the members at ``as_of`` (a date,
    or a tz-aware datetime read as its UTC date) back-filled; see the module docstring."""
    cut = _as_of_date(as_of)
    _check_intervals(intervals)
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
        "annual_fee": annual_fee,
        "max_stale_sessions": max_stale_sessions,
        "rebalance_months": rebalance_months,
    }
    full = equal_weight_index(intervals, tr, window, on_missing=full_on_missing, **options)
    biased = equal_weight_index(backfilled, tr, window, on_missing=survivors_on_missing, **options)
    rf, rs = _index_return(full.levels), _index_return(biased.levels)
    delta = None if rf is None or rs is None else rs - rf
    return SurvivorsResult(full=full, survivors=biased, annualised_delta=delta)

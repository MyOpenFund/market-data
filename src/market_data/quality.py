"""Producer-side quality checks and fetch coverage (spec §12, §7.4, G4).

``check_prices``: parameterised price checks, one source at a time. Rows are **flagged, never
withheld**: the function returns flags (``QUALITY_FLAGS_SCHEMA``) and never touches its inputs.

Inputs are contract tables (``prices``, ``corporate_actions``) of one ``source_code`` (several
raise ``ValueError``: ``"AAA"`` of two sources are two series); each is read latest per natural
key first (the latest ``known_at`` revision is the one checked). ``sessions`` is a table with a
``session_date`` column (e.g. ``open_data_store.market.asof.sessions``). A close is *valid* when
finite and > 0. Checks (``CHECKS``):

- ``non_positive``: a close that is not valid (<= 0, NaN or infinite).
- ``jump_without_action``: ``|close(t) / close(p) - 1| > jump_threshold`` between a valid close t
  and the symbol's previous valid close p, with no ``split``, ``stock_dividend`` or ``spin_off``
  action of that symbol whose ex-date lies in ``(p, t]`` (an ex-date that is not a session shows
  its jump on the next price row). Cash actions never excuse a jump.
- ``missing_session``: a session of ``sessions`` between a symbol's first and last price row
  (inclusive) with no price row; flagged on the missing session.
- ``vendor_divergence`` (only with ``vendor_adjusted``, e.g. ``TiingoAdapter.parse_vendor_adjusted``
  output ``[source_symbol, valid_date, adj_close]`` of the same source): for each pair (p, t) of
  consecutive valid closes where the vendor has an adjusted close on both dates,
  ``|ours - vendor| > divergence_threshold`` with ``vendor = adj(t) / adj(p) - 1`` and ``ours`` the
  recipe-free return from raw closes and the actions with ex-date in ``(p, t]``, applied per
  ex-date as the store's total-return recipe does: splits and stock dividends first
  (``n *= ratio_new / ratio_old``; ratios not finite and positive are ignored), then cash
  (regular and special dividends, return of capital, a spin-off with an amount; amounts not finite
  and positive are ignored) credited per share held, ``cash += n * amount`` (``old_share`` basis:
  per share held before that ex-date's splits); ``ours = (n * close(t) + cash) / close(p) - 1``.
  Currencies are not compared; rights and unvalued spin-offs are not applied (a divergence there
  is a real difference to look at, and is flagged).

Thresholds are named parameters (``DEFAULT_JUMP_THRESHOLD`` 0.5 and
``DEFAULT_DIVERGENCE_THRESHOLD`` 0.01 are placeholders to set from measurements on real data).

``fetch_coverage``: fetch-log ranges mapped to instruments, the store coverage recipe's
``FETCH_COVERAGE_SCHEMA`` input. See its docstring. ``unsuperseded_failures``: the failed rows of
a fetch log that no later fetch replaced, for the run report.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    FETCH_STATUSES,
    PRICES,
)
from open_data_store.market._mapping import MAX_LISTED, symbol_mappings
from open_data_store.market.asof import latest
from open_data_store.market.coverage import (
    FAILED_FETCH_STATUSES,
    FETCH_COVERAGE_SCHEMA,
    NO_DATA_STATUSES,
)

from market_data.adapters.base import WHOLE_MARKET_SYMBOL

__all__ = [
    "CASH_ACTIONS",
    "CASH_WITHOUT_CURRENCY",
    "CHECKS",
    "CORROBORATING_STATUSES",
    "DEFAULT_CORROBORATING_SOURCES",
    "DEFAULT_DIVERGENCE_THRESHOLD",
    "DEFAULT_JUMP_THRESHOLD",
    "JUMP_EXCUSING_ACTIONS",
    "QUALITY_FLAGS_SCHEMA",
    "SHARE_ACTIONS",
    "SPLIT_DAY_CASH_WITHOUT_BASIS",
    "UNUSABLE_CASH_REASONS",
    "ambiguous_mappings",
    "check_prices",
    "fetch_coverage",
    "unsuperseded_failures",
    "unusable_cash",
]

CHECKS = ("non_positive", "jump_without_action", "missing_session", "vendor_divergence")
DEFAULT_JUMP_THRESHOLD = 0.5
DEFAULT_DIVERGENCE_THRESHOLD = 0.01
JUMP_EXCUSING_ACTIONS = ("split", "stock_dividend", "spin_off")
SHARE_ACTIONS = ("split", "stock_dividend")
CASH_ACTIONS = ("cash_dividend_regular", "cash_dividend_special", "return_of_capital", "spin_off")
# Cash actions the total-return recipe never applies (it flags them; the cash is lost): a cash
# amount without a currency, and one without ``per_share_basis`` on the ex-date of a split or
# stock dividend of the same series.
CASH_WITHOUT_CURRENCY = "cash_without_currency"
SPLIT_DAY_CASH_WITHOUT_BASIS = "split_day_cash_without_basis"
UNUSABLE_CASH_REASONS = (CASH_WITHOUT_CURRENCY, SPLIT_DAY_CASH_WITHOUT_BASIS)

QUALITY_FLAGS_SCHEMA = pa.schema(
    [
        ("source_symbol", pa.string()),
        ("session_date", pa.date32()),
        ("check", pa.string()),
        ("detail", pa.string()),
    ]
)

# Statuses of a symbol-level fetch that, past the end of the symbol's last window, corroborate
# that end (the source was asked and had nothing): ``fetch_coverage`` rule 6.
CORROBORATING_STATUSES = ("ok", "empty")
# Sources whose windows' end corroborates a delisting (``fetch_coverage`` rule 6): none by
# default, rule 6 off. The pipeline passes the codes of the adapters that declare
# ``corroborates_delisting`` (the history file).
DEFAULT_CORROBORATING_SOURCES: frozenset[str] = frozenset()

_EPOCH = date(1970, 1, 1)
# Composite (symbol, day) keys: code * _SHIFT + day + _OFFSET, day in int32 range.
_OFFSET = 1 << 32
_SHIFT = 1 << 34


def _days(column: pa.ChunkedArray | pa.Array) -> np.ndarray:
    arr = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    return arr.cast(pa.int32()).to_numpy(zero_copy_only=False).astype(np.int64)


def _codes(column: pa.ChunkedArray | pa.Array, names: pa.Array) -> np.ndarray:
    idx = pc.index_in(column, value_set=names)
    idx = idx.combine_chunks() if isinstance(idx, pa.ChunkedArray) else idx
    return idx.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)


def _floats(column: pa.ChunkedArray) -> np.ndarray:
    return column.combine_chunks().fill_null(math.nan).to_numpy(zero_copy_only=False)


def _key(code: np.ndarray, day: np.ndarray) -> np.ndarray:
    return code * _SHIFT + day + _OFFSET


def _check_threshold(name: str, value: float, *, positive: bool) -> None:
    ok = isinstance(value, (int, float)) and math.isfinite(value)
    ok = ok and (value > 0 if positive else value >= 0)
    if not ok:
        bound = "> 0" if positive else ">= 0"
        raise ValueError(f"{name} must be a finite number {bound}, got {value!r}")


def _one_source(*tables: pa.Table) -> None:
    codes: set[str] = set()
    for table in tables:
        if table.num_rows:
            codes.update(pc.unique(table["source_code"]).to_pylist())
    if len(codes) > 1:
        raise ValueError(
            f"check_prices checks one source at a time, got {len(codes)} source codes: "
            f"{sorted(codes)[:MAX_LISTED]}"
        )


class _Flags:
    def __init__(self) -> None:
        self.code: list[np.ndarray] = []
        self.day: list[np.ndarray] = []
        self.check: list[str] = []
        self.detail: list[str] = []

    def add(self, code: np.ndarray, day: np.ndarray, check: str, details: Sequence[str]) -> None:
        self.code.append(code)
        self.day.append(day)
        self.check.extend([check] * code.size)
        self.detail.extend(details)

    def table(self, names: pa.Array) -> pa.Table:
        code = np.concatenate(self.code) if self.code else np.zeros(0, np.int64)
        day = np.concatenate(self.day) if self.day else np.zeros(0, np.int64)
        out = pa.table(
            [
                names.take(pa.array(code, pa.int64())) if code.size else pa.array([], pa.string()),
                pa.array(day.astype(np.int32), pa.int32()).cast(pa.date32()),
                pa.array(self.check, pa.string()),
                pa.array(self.detail, pa.string()),
            ],
            schema=QUALITY_FLAGS_SCHEMA,
        )
        return out.sort_by(
            [("source_symbol", "ascending"), ("session_date", "ascending"), ("check", "ascending")]
        )


def check_prices(
    prices: pa.Table,
    actions: pa.Table,
    sessions: pa.Table,
    *,
    jump_threshold: float = DEFAULT_JUMP_THRESHOLD,
    divergence_threshold: float = DEFAULT_DIVERGENCE_THRESHOLD,
    vendor_adjusted: pa.Table | None = None,
) -> pa.Table:
    """Quality flags of one source's prices (``QUALITY_FLAGS_SCHEMA``, sorted by source_symbol,
    session_date, check). See the module docstring for the checks."""
    _check_threshold("jump_threshold", jump_threshold, positive=True)
    _check_threshold("divergence_threshold", divergence_threshold, positive=False)
    _one_source(prices, actions)
    if prices.num_rows == 0:
        return QUALITY_FLAGS_SCHEMA.empty_table()
    prices = latest(prices, PRICES.natural_key)
    actions = latest(actions, CORPORATE_ACTIONS.natural_key) if actions.num_rows else actions

    names = pc.unique(prices["source_symbol"].combine_chunks())
    names = names.take(pc.sort_indices(names))
    code = _codes(prices["source_symbol"], names)
    day = _days(prices["valid_date"])
    close = _floats(prices["close"])
    order = np.lexsort((day, code))
    code, day, close = code[order], day[order], close[order]
    flags = _Flags()

    valid = np.isfinite(close) & (close > 0)
    bad = np.flatnonzero(~valid)
    flags.add(code[bad], day[bad], "non_positive", [f"close={close[i]!r}" for i in bad])

    # Pairs (p, t) of consecutive valid closes of one symbol.
    v = np.flatnonzero(valid)
    same = code[v][1:] == code[v][:-1]
    p_row, t_row = v[:-1][same], v[1:][same]
    p_key, t_key = _key(code[p_row], day[p_row]), _key(code[t_row], day[t_row])
    ret = close[t_row] / close[p_row] - 1.0

    a_code = _codes(actions["source_symbol"], names) if actions.num_rows else np.zeros(0, np.int64)
    a_day = _days(actions["valid_date"]) if actions.num_rows else np.zeros(0, np.int64)
    a_type = (
        np.asarray(actions["action_type"].to_pylist(), dtype=object)
        if actions.num_rows
        else np.zeros(0, object)
    )
    a_known = a_code >= 0

    # jump_without_action
    excusing = a_known & np.isin(a_type, JUMP_EXCUSING_ACTIONS)
    ex_keys = np.sort(_key(a_code[excusing], a_day[excusing]))
    excused = np.searchsorted(ex_keys, t_key, "right") > np.searchsorted(ex_keys, p_key, "right")
    jump = np.flatnonzero((np.abs(ret) > jump_threshold) & ~excused)
    flags.add(
        code[t_row[jump]],
        day[t_row[jump]],
        "jump_without_action",
        [f"return={ret[j]:+.6f} threshold={jump_threshold}" for j in jump],
    )

    _missing_sessions(flags, code, day, sessions)

    if vendor_adjusted is not None:
        ours = _our_returns(
            actions, a_code, a_day, a_type, a_known, p_key, t_key, close, p_row, t_row
        )
        vendor = _vendor_returns(vendor_adjusted, names, p_key, t_key)
        diff = np.abs(ours - vendor)
        div = np.flatnonzero(np.isfinite(vendor) & ~(diff <= divergence_threshold))
        flags.add(
            code[t_row[div]],
            day[t_row[div]],
            "vendor_divergence",
            [
                f"ours={ours[j]:+.6f} vendor={vendor[j]:+.6f} threshold={divergence_threshold}"
                for j in div
            ],
        )
    return flags.table(names)


def _missing_sessions(flags: _Flags, code: np.ndarray, day: np.ndarray, sessions: pa.Table) -> None:
    if sessions["session_date"].null_count:
        raise ValueError("sessions.session_date has null values")
    s_days = np.unique(_days(sessions["session_date"]))
    if not s_days.size:
        return
    starts = np.flatnonzero(np.r_[True, code[1:] != code[:-1]])
    ends = np.r_[starts[1:], code.size] - 1
    sym = code[starts]
    lo = np.searchsorted(s_days, day[starts], "left")
    hi = np.searchsorted(s_days, day[ends], "right")
    lengths = np.maximum(hi - lo, 0)
    total = int(lengths.sum())
    offsets = np.repeat(lo - np.r_[0, np.cumsum(lengths)[:-1]], lengths)
    n = s_days.size
    cells = np.repeat(sym, lengths) * n + offsets + np.arange(total, dtype=np.int64)
    pos = np.searchsorted(s_days, day)
    on_session = (pos < n) & (s_days[np.minimum(pos, n - 1)] == day)
    present = code[on_session] * n + pos[on_session]
    missing = cells[~np.isin(cells, present)]
    m_code, m_idx = np.divmod(missing, n)
    flags.add(
        m_code,
        s_days[m_idx],
        "missing_session",
        ["no price row on a calendar session"] * missing.size,
    )


def _our_returns(
    actions: pa.Table,
    a_code: np.ndarray,
    a_day: np.ndarray,
    a_type: np.ndarray,
    a_known: np.ndarray,
    p_key: np.ndarray,
    t_key: np.ndarray,
    close: np.ndarray,
    p_row: np.ndarray,
    t_row: np.ndarray,
) -> np.ndarray:
    """Recipe-free return per pair (p, t) from raw closes and actions (see the module doc)."""
    n_total = np.ones(p_key.size)
    cash = np.zeros(p_key.size)
    if actions.num_rows and p_key.size:
        ratio = _floats(actions["ratio_new"]) / _floats(actions["ratio_old"])
        amount = _floats(actions["amount"])
        old_basis = np.asarray(
            [b == "old_share" for b in actions["per_share_basis"].to_pylist()], bool
        )
        is_share = np.isin(a_type, SHARE_ACTIONS) & np.isfinite(ratio) & (ratio > 0)
        is_cash = np.isin(a_type, CASH_ACTIONS) & np.isfinite(amount) & (amount > 0)
        a_key = _key(a_code, a_day)
        pair = np.searchsorted(t_key, a_key, "left")
        inside = a_known & (is_share | is_cash) & (pair < t_key.size)
        inside[inside] &= p_key[pair[inside]] < a_key[inside]
        rows = np.flatnonzero(inside)
        if not rows.size:
            return (n_total * close[t_row] + cash) / close[p_row] - 1.0
        # Order: pair, ex-date, shares before cash.
        rows = rows[np.lexsort((is_cash[rows], a_day[rows], pair[rows]))]
        r_pair, r_day = pair[rows], a_day[rows]
        log_f = np.where(is_share[rows], np.log(np.where(is_share[rows], ratio[rows], 1.0)), 0.0)
        cum = np.cumsum(log_f)
        before = np.r_[0.0, cum[:-1]]  # cumulative log before each row
        group_start = np.r_[True, r_pair[1:] != r_pair[:-1]]
        base = np.maximum.accumulate(np.where(group_start, np.arange(rows.size), 0))
        day_start = group_start | np.r_[True, r_day[1:] != r_day[:-1]]
        day_first = np.maximum.accumulate(np.where(day_start, np.arange(rows.size), 0))
        n_after = np.exp(cum - before[base])
        n_before_day = np.exp(before[day_first] - before[base])
        held = np.where(old_basis[rows], n_before_day, n_after)
        credit = np.where(is_cash[rows], amount[rows] * held, 0.0)
        np.add.at(cash, r_pair, credit)
        np.multiply.at(n_total, r_pair, np.exp(log_f))
    return (n_total * close[t_row] + cash) / close[p_row] - 1.0


def _vendor_returns(
    vendor: pa.Table, names: pa.Array, p_key: np.ndarray, t_key: np.ndarray
) -> np.ndarray:
    """Vendor return per pair (p, t); NaN when the vendor lacks either date."""
    out = np.full(p_key.size, np.nan)
    v_code = _codes(vendor["source_symbol"], names)
    keep = v_code >= 0
    v_key = _key(v_code[keep], _days(vendor["valid_date"])[keep])
    v_val = _floats(vendor["adj_close"])[keep]
    order = np.argsort(v_key, kind="stable")
    v_key, v_val = v_key[order], v_val[order]
    if np.any(v_key[1:] == v_key[:-1]):
        raise ValueError("vendor_adjusted has several rows for one (source_symbol, valid_date)")
    if not v_key.size or not p_key.size:
        return out

    def lookup(keys: np.ndarray) -> np.ndarray:
        pos = np.minimum(np.searchsorted(v_key, keys), v_key.size - 1)
        return np.where(v_key[pos] == keys, v_val[pos], np.nan)

    with np.errstate(divide="ignore", invalid="ignore"):
        out = lookup(t_key) / lookup(p_key) - 1.0
    return np.where(np.isfinite(out), out, np.nan)


# --- fetch coverage -----------------------------------------------------------------------------


def _check_statuses(fetch_log: pa.Table) -> None:
    status = fetch_log["status"]
    unknown = pc.invert(pc.is_in(status, value_set=pa.array(FETCH_STATUSES))).fill_null(True)
    if pc.any(unknown).as_py():
        bad = sorted(set(status.filter(unknown).to_pylist()), key=str)
        raise ValueError(f"fetch_log status not in {FETCH_STATUSES}: {bad[:MAX_LISTED]}")


def _session_days(sessions: pa.Table | None) -> np.ndarray | None:
    if sessions is None:
        return None
    if sessions["session_date"].null_count:
        raise ValueError("sessions.session_date has null values")
    return np.unique(_days(sessions["session_date"]))


def _space(start: np.ndarray, end: np.ndarray, s_days: np.ndarray | None):
    """Inclusive day ranges as half-open [lo, hi) in the merge space, where two ranges touch
    when ``lo`` of one <= ``hi`` of the other.

    Without sessions the space is calendar days. With ``s_days``, days within [first session,
    last session] map to session indices (ranges with no session between them touch: Friday and
    Monday), and days outside keep calendar-day steps on either side (never collapsed: the
    calendar says nothing there, so only calendar adjacency merges or supersedes)."""
    if s_days is None or not s_days.size:
        return start, end + 1
    first, last, n = s_days[0], s_days[-1], s_days.size
    lo = np.where(
        start < first,
        start - first,
        np.where(start > last, n + (start - last) - 1, np.searchsorted(s_days, start, "left")),
    )
    hi = np.where(
        end < first,
        end - first + 1,
        np.where(end > last, n + (end - last), np.searchsorted(s_days, end, "right")),
    )
    return lo, hi


def _bounds(fetch_log: pa.Table) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Day numbers of requested_from / requested_to and a mask of usable (non-null) ranges."""
    has = pc.and_(
        pc.is_valid(fetch_log["requested_from"]), pc.is_valid(fetch_log["requested_to"])
    ).to_numpy(zero_copy_only=False)
    start = fetch_log["requested_from"].combine_chunks().cast(pa.int32()).fill_null(0)
    end = fetch_log["requested_to"].combine_chunks().cast(pa.int32()).fill_null(-1)
    start = start.to_numpy(zero_copy_only=False).astype(np.int64)
    end = end.to_numpy(zero_copy_only=False).astype(np.int64)
    return start, end, has & (start <= end)


def _superseded(fetch_log: pa.Table, s_days: np.ndarray | None) -> np.ndarray:
    """Mask of failed rows (``error``/``partial``) whose whole range a strictly later
    ``ok``/``empty``/``not_found`` row (or the union of such rows) of the same (source_code,
    source_symbol) covers."""
    out = np.zeros(fetch_log.num_rows, bool)
    status = np.asarray(fetch_log["status"].to_pylist(), dtype=object)
    failed = np.flatnonzero(np.isin(status, FAILED_FETCH_STATUSES))
    if not failed.size:
        return out
    start, end, usable = _bounds(fetch_log)
    lo, hi = _space(start, end, s_days)
    known = fetch_log["known_at"].combine_chunks().cast(pa.int64()).to_numpy(zero_copy_only=False)
    keys = list(
        zip(
            fetch_log["source_code"].to_pylist(),
            fetch_log["source_symbol"].to_pylist(),
            strict=True,
        )
    )
    by_key: dict[tuple[str, str], list[int]] = defaultdict(list)
    for i in np.flatnonzero(np.isin(status, NO_DATA_STATUSES) & usable):
        by_key[keys[i]].append(int(i))
    for i in failed:
        if not usable[i]:
            continue
        rows = np.asarray(by_key.get(keys[i], ()), np.int64)
        rows = rows[known[rows] > known[i]] if rows.size else rows
        if not rows.size:
            continue
        rows = rows[np.argsort(lo[rows], kind="stable")]
        r_lo, r_hi = lo[rows], np.maximum.accumulate(hi[rows])
        new_run = np.r_[True, r_lo[1:] > r_hi[:-1]]
        run_lo = r_lo[new_run]
        run_hi = r_hi[np.r_[np.flatnonzero(new_run)[1:] - 1, rows.size - 1]]
        pos = np.searchsorted(run_lo, lo[i], "right") - 1
        out[i] = pos >= 0 and run_hi[pos] >= hi[i]
    return out


def unusable_cash(actions: pa.Table) -> dict[str, dict[str, int]]:
    """source_code -> reason -> rows: the cash actions (``CASH_ACTIONS`` with an amount) of
    ``actions`` (``CORPORATE_ACTIONS`` or ``RAW_ACTION_SCHEMA`` rows) the total-return recipe
    would never apply: no currency (``CASH_WITHOUT_CURRENCY``), or no ``per_share_basis`` on
    the ex-date of a ``SHARE_ACTIONS`` row of the same (source_code, source_symbol)
    (``SPLIT_DAY_CASH_WITHOUT_BASIS``; counted once when both apply, as no currency). Empty
    when every cash action is usable."""
    cols = ["source_code", "source_symbol", "valid_date", "action_type", "amount", "currency",
            "per_share_basis"]  # fmt: skip
    rows = actions.select(cols).to_pylist() if actions.num_rows else []
    split_days = {
        (r["source_code"], r["source_symbol"], r["valid_date"])
        for r in rows
        if r["action_type"] in SHARE_ACTIONS
    }
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        if r["action_type"] not in CASH_ACTIONS or r["amount"] is None:
            continue
        if not (r["currency"] or "").strip():
            reason = CASH_WITHOUT_CURRENCY
        elif r["per_share_basis"] is None and (
            (r["source_code"], r["source_symbol"], r["valid_date"]) in split_days
        ):
            reason = SPLIT_DAY_CASH_WITHOUT_BASIS
        else:
            continue
        per = out.setdefault(r["source_code"], {})
        per[reason] = per.get(reason, 0) + 1
    return out


def unsuperseded_failures(fetch_log: pa.Table, *, sessions: pa.Table | None = None) -> pa.Table:
    """The ``error``/``partial`` rows of ``fetch_log`` that no later fetch superseded (see
    ``fetch_coverage``), for the run report. A failed row with a null range (a whole-list
    ``symbols`` fetch) is superseded only by a strictly later ``ok``/``empty``/``not_found``
    row of the same (source_code, source_symbol) with a null range too (the same list fetched
    again).

    Pass the fetch log of one kind (``build_fetch_log(kinds=(kind,))``): it has no kind column,
    so an ``ok`` actions fetch would otherwise supersede a failed prices fetch."""
    _check_statuses(fetch_log)
    failed = pc.is_in(fetch_log["status"], value_set=pa.array(FAILED_FETCH_STATUSES))
    keep = failed.to_numpy(zero_copy_only=False) & ~_superseded(fetch_log, _session_days(sessions))
    keep &= ~_null_range_superseded(fetch_log)
    return fetch_log.filter(pa.array(keep))


def _null_range_superseded(fetch_log: pa.Table) -> np.ndarray:
    """Mask of failed rows with a null range followed (strictly later ``known_at``) by a
    good row with a null range of the same (source_code, source_symbol)."""
    out = np.zeros(fetch_log.num_rows, bool)
    null = pc.and_(
        pc.is_null(fetch_log["requested_from"]), pc.is_null(fetch_log["requested_to"])
    ).to_numpy(zero_copy_only=False)
    if not null.any():
        return out
    status = np.asarray(fetch_log["status"].to_pylist(), dtype=object)
    known = fetch_log["known_at"].combine_chunks().cast(pa.int64()).to_numpy(zero_copy_only=False)
    keys = list(
        zip(
            fetch_log["source_code"].to_pylist(),
            fetch_log["source_symbol"].to_pylist(),
            strict=True,
        )
    )
    latest_good: dict[tuple[str, str], int] = {}
    for i in np.flatnonzero(null & np.isin(status, NO_DATA_STATUSES)):
        latest_good[keys[i]] = max(latest_good.get(keys[i], known[i]), known[i])
    for i in np.flatnonzero(null & np.isin(status, FAILED_FETCH_STATUSES)):
        good = latest_good.get(keys[i])
        out[i] = good is not None and good > known[i]
    return out


def _windows(identifiers: pa.Table) -> tuple[pa.Table, set[str]]:
    """Mapping windows of ``source_symbol`` identifiers (read latest, store cover rule):
    token, source_code, w_from, w_to (inclusive; null = open), instrument_id; and the tokens
    whose windows overlap while naming different instruments (ambiguous)."""
    rows: dict[str, list] = {k: [] for k in ("token", "w_source", "w_from", "w_to", "inst")}
    ambiguous: set[str] = set()
    mappings = symbol_mappings(identifiers) if identifiers.num_rows else {}
    for token, windows in sorted(mappings.items()):
        windows = sorted(windows, key=lambda w: w[0])
        for a, (start_a, end_a, inst_a) in enumerate(windows):
            for start_b, _end_b, inst_b in windows[a + 1 :]:
                if inst_b != inst_a and (end_a is None or start_b < end_a):
                    ambiguous.add(token)
            rows["token"].append(token)
            rows["w_source"].append(token.split(":", 1)[0])
            rows["w_from"].append(start_a)
            rows["w_to"].append(None if end_a is None else end_a - timedelta(days=1))
            rows["inst"].append(inst_a)
    table = pa.table(
        {
            "token": pa.array(rows["token"], pa.string()),
            "w_source": pa.array(rows["w_source"], pa.string()),
            "w_from": pa.array(rows["w_from"], pa.date32()),
            "w_to": pa.array(rows["w_to"], pa.date32()),
            "instrument_id": pa.array(rows["inst"], pa.string()),
        }
    )
    return table, ambiguous


def ambiguous_mappings(identifiers: pa.Table) -> int:
    """Number of ``source_symbol`` tokens whose mapping windows (read latest) overlap while
    naming different instruments. ``fetch_coverage`` refuses them only when a fetch-log row
    touches them; this count lets a run report the untouched ones."""
    return len(_windows(identifiers)[1])


def _merged_runs(log: pa.Table, start, end, s_days: np.ndarray | None) -> pa.Table:
    """Ranges merged per (source_code, source_symbol, status): touching or overlapping ranges
    (in the merge space of ``_space``) become one run [first from, max to]."""
    group = pc.binary_join_element_wise(
        log["source_code"], log["source_symbol"], log["status"], "\x00"
    ).combine_chunks()
    encoded = pc.dictionary_encode(group)
    g = encoded.indices.to_numpy(zero_copy_only=False).astype(np.int64)
    lo, hi = _space(start, end, s_days)
    k_lo, k_hi = _key(g, lo), _key(g, hi)
    order = np.lexsort((start, k_lo))
    k_lo, k_hi, g, start, end = k_lo[order], k_hi[order], g[order], start[order], end[order]
    reach = np.maximum.accumulate(k_hi)
    new_run = np.r_[True, k_lo[1:] > reach[:-1]]
    first = np.flatnonzero(new_run)
    run_to = np.maximum.reduceat(end, first)
    taken = log.take(pa.array(order[first], pa.int64()))
    return pa.table(
        {
            "source_code": taken["source_code"],
            "source_symbol": taken["source_symbol"],
            "status": taken["status"],
            "f_from": pa.array(start[first].astype(np.int32), pa.int32()).cast(pa.date32()),
            "f_to": pa.array(run_to.astype(np.int32), pa.int32()).cast(pa.date32()),
        }
    )


def _last_returned(log: pa.Table) -> dict[tuple[str, str], date | None]:
    """Per (source_code, source_symbol): the latest ``returned_to`` of its rows (None when no
    row returned anything)."""
    out: dict[tuple[str, str], date | None] = {}
    for code, symbol, day in zip(
        log["source_code"].to_pylist(),
        log["source_symbol"].to_pylist(),
        log["returned_to"].to_pylist(),
        strict=True,
    ):
        seen = out.get((code, symbol))
        out[(code, symbol)] = day if seen is None else (seen if day is None else max(seen, day))
    return out


def _extend_last_window(
    part: pa.Table,
    hi: pa.ChunkedArray,
    last_start: Mapping[str, date],
    last_returned: Mapping[tuple[str, str], date | None],
    corroborating_sources: Collection[str],
) -> pa.Array:
    """``hi`` with the symbol-level rows of a token's last (closed) window extended to the
    fetch's end when the row's source is in ``corroborating_sources``, the fetch says nothing
    came after the window (status in ``CORROBORATING_STATUSES``) and no row of the symbol
    returned data after the window's end: the source was asked and had nothing, which
    corroborates the end (a delisting)."""
    out = hi.to_pylist()
    rows = zip(
        part["token"].to_pylist(),
        part["source_code"].to_pylist(),
        part["source_symbol"].to_pylist(),
        part["status"].to_pylist(),
        part["f_to"].to_pylist(),
        part["w_from"].to_pylist(),
        part["w_to"].to_pylist(),
        strict=True,
    )
    for i, (token, code, symbol, status, f_to, w_from, w_to) in enumerate(rows):
        if code not in corroborating_sources:
            continue
        if w_to is None or f_to <= w_to or status not in CORROBORATING_STATUSES:
            continue
        if w_from != last_start[token]:
            continue  # a later window of the symbol names whoever came next
        data_to = last_returned.get((code, symbol))
        if data_to is None or data_to <= w_to:
            out[i] = f_to
    return pa.array(out, pa.date32())


def fetch_coverage(
    fetch_log: pa.Table,
    identifiers: pa.Table,
    *,
    as_of: datetime,
    sessions: pa.Table | None = None,
    corroborating_sources: Collection[str] = DEFAULT_CORROBORATING_SOURCES,
) -> pa.Table:
    """Fetch-log ranges mapped to instruments: the store's ``FETCH_COVERAGE_SCHEMA``
    [instrument_id, covered_from, covered_to, status] (inclusive bounds), sorted.

    ``fetch_log`` must hold **prices fetches only** (``build_fetch_log(kinds=("prices",))``): the
    contract has no kind column, and coverage is about prices. Rules:

    1. Rows known after ``as_of`` (tz-aware) are ignored; ranges are clipped to ``as_of``'s UTC
       date. An unknown status raises ``ValueError``.
    2. A failed row (``error``/``partial``) is dropped when strictly later ``ok``/``empty``/
       ``not_found`` rows of the same (source_code, source_symbol or ``"*"``) cover its whole
       range (one row or their union): a transient failure must not veto delistings forever.
       Unsuperseded failures are kept (the store's coverage lets them veto). Keys never mix: a
       failed symbol-level fetch is not cleared by a later ``"*"`` ``ok``, nor the reverse.
       With ``sessions``, a part of the failed range outside the calendar must be covered day
       by day (see ``_space``).
    3. Rows with a null bound cover nothing and are dropped (``unsuperseded_failures`` still
       reports such failures).
    4. Each ``<source_code>:<source_symbol>`` maps through the ``source_symbol`` identifiers, read
       latest, a window ``[valid_date, valid_to)`` covering the day (the store's ``_mapping``
       rule); a range spanning several windows is split; an unmapped symbol covers nothing.
       Overlapping windows of one symbol naming different instruments raise ``ValueError`` when
       a fetch-log row touches that symbol (its own row, or a ``"*"`` row of its source);
       untouched ones are skipped (count them with ``ambiguous_mappings``).
    5. A ``"*"`` (whole-market) row covers every mapped instrument of its source over its range.
    6. Only for the sources in ``corroborating_sources`` (default
       ``DEFAULT_CORROBORATING_SOURCES``, empty: rule 6 off): a symbol-level ``ok``/``empty``
       row reaching past the end of the symbol's last (closed) window covers that window's
       instrument through the row's ``requested_to`` when no row of the symbol returned data
       after the window's end (``returned_to``): the source was asked and had nothing, which
       corroborates the end (a history export's delisted member: its last price ends the
       window, the export date ends the request). Pass the codes of the sources whose windows
       end at an export date (adapters declaring ``corroborates_delisting``, see
       ``market_data.adapters.base.Adapter``); never a live provider's: one ``empty`` answer
       for the next session (a halt, a late publication) or a renamed ticker would read as a
       delisting. ``"*"`` rows are unchanged (clipped to every window).

    Ranges of one (source_code, source_symbol, status) are merged first; with ``sessions`` (pass
    the coverage recipe's own), ranges with no session between them merge too (daily grouped
    fetches Friday and Monday become one range), which keeps a whole-market history compact;
    outside the calendar's first and last session only calendar-adjacent ranges merge."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be a timezone-aware datetime")
    _check_statuses(fetch_log)
    s_days = _session_days(sessions)
    known = fetch_log["known_at"]
    log = fetch_log.filter(pc.less_equal(known, pa.scalar(as_of.astimezone(UTC), known.type)))
    last_returned = _last_returned(log)
    log = log.filter(pa.array(~_superseded(log, s_days)))
    start, end, usable = _bounds(log)
    end = np.minimum(end, (as_of.astimezone(UTC).date() - _EPOCH).days)
    usable &= start <= end
    keep = np.flatnonzero(usable)
    if not keep.size:
        return FETCH_COVERAGE_SCHEMA.empty_table()
    log = log.take(pa.array(keep, pa.int64()))
    runs = _merged_runs(log, start[keep], end[keep], s_days)
    windows, ambiguous = _windows(identifiers)
    if not windows.num_rows:
        return FETCH_COVERAGE_SCHEMA.empty_table()

    whole = pc.equal(runs["source_symbol"], WHOLE_MARKET_SYMBOL)
    own = runs.filter(pc.invert(whole))
    own = own.append_column(
        "token", pc.binary_join_element_wise(own["source_code"], own["source_symbol"], ":")
    )
    whole_sources = set(runs.filter(whole)["source_code"].to_pylist())
    own_tokens = set(own["token"].to_pylist())
    touched = sorted(t for t in ambiguous if t in own_tokens or t.split(":", 1)[0] in whole_sources)
    if touched:
        raise ValueError(
            f"ambiguous mapping for {len(touched)} symbol(s) of the fetch log (overlapping "
            f"windows naming several instruments); fix the mapping upstream; first "
            f"{min(len(touched), MAX_LISTED)}: {touched[:MAX_LISTED]}"
        )
    parts = [
        (own.join(windows, "token", join_type="inner"), True),
        (runs.filter(whole).join(windows, "source_code", "w_source", join_type="inner"), False),
    ]
    last_start: dict[str, date] = {}
    for token, start_day in zip(
        windows["token"].to_pylist(), windows["w_from"].to_pylist(), strict=True
    ):
        last_start[token] = max(last_start.get(token, start_day), start_day)
    out = []
    for part, symbol_level in parts:
        if not part.num_rows:
            continue
        lo = pc.max_element_wise(part["f_from"], part["w_from"])
        hi = pc.min_element_wise(part["f_to"], part["w_to"])  # null w_to (open) is skipped
        if symbol_level:
            hi = _extend_last_window(
                part, hi, last_start, last_returned, frozenset(corroborating_sources)
            )
        table = pa.table(
            [part["instrument_id"], lo, hi, part["status"]], schema=FETCH_COVERAGE_SCHEMA
        )
        out.append(table.filter(pc.less_equal(lo, hi)))
    if not out:
        return FETCH_COVERAGE_SCHEMA.empty_table()
    return pa.concat_tables(out).sort_by(
        [(name, "ascending") for name in ("instrument_id", "covered_from", "covered_to", "status")]
    )

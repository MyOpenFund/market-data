"""Batch builder: contract rows as a pure, deterministic function of the raw archive (spec §7.3).

The same archive always gives the same tables (rows sorted by natural key, then ``known_at``), so
re-publishing history is idempotent. ``known_at`` makes a backtest look-ahead-free:

Prices (``build_prices``, contract ``prices``)
    The first value seen for (source_symbol, session) gets ``known_at`` = that session's close in
    UTC from the exchange calendar (``open_data_store.market.asof.sessions``: DST and early
    closes), ``published`` / ``session_close``. A later fetch with a different value (any of
    open, high, low, close, volume, currency; nulls compared as values, NaN equal to NaN) gives a
    new row at that fetch's ``fetched_at``, ``collected`` / ``fetch_time``. An unchanged value is
    not re-emitted; a value reverting to an earlier one is a change. Dropped and reported, never
    guessed: a row whose valid_date is not a session of ``mic`` (``non_session``), and a row whose
    first sighting comes from a fetch made before that session's close (``intraday_partial``: a
    partial bar; a later fetch after the close is accepted normally).

Corporate actions (``build_actions``, contract ``corporate_actions``)
    Identity across fetches = (source_symbol, ex-date, action_type, raw within-payload
    action_seq); the final ``action_seq`` is that raw seq as first seen. First value: the
    announce date at 23:59:59.999999 in the exchange time zone (``announce_date``) when known and
    not after the ex-date, else the open of the ex-date session (``ex_date_assumed``; an ex-date
    that is not a session uses the next session's open), ``published``; in both cases capped at
    the ``fetched_at`` of the first fetch holding it, so a future ex-date never yields a future
    ``known_at`` (the basis keeps the path that was taken). A later changed value (any non-identity
    field) gives a new row at ``fetched_at``, ``collected`` / ``fetch_time``.

Fetch log (``build_fetch_log``, contract ``fetch_log``)
    One row per archived fetch of a source, ``fetch_id`` = ``archive_ref``.

Inputs: for each requested symbol, its own history and the whole-market history (``"*"``) of the
source and kind; rows from a ``"*"`` fetch are kept for requested symbols only (``symbols=None``:
every symbol seen). Only fetches whose status is in ``ROW_STATUSES`` contribute rows.

Known limit (V0): absence never retracts. A row missing from a later fetch keeps its last value;
there is no retraction convention in the contracts yet.

Drop report: ``build_prices`` and ``build_actions`` return a ``BuildResult``: ``table`` (the
contract table) and ``dropped``, a mapping reason -> sorted tuple of distinct
``(source_symbol, valid_date)`` keys (reasons ``DROP_NON_SESSION`` and
``DROP_INTRADAY_PARTIAL``; always both present, empty tuples when nothing was dropped; actions
drop nothing today). ``counts()`` gives reason -> number of distinct keys, for the B8 report.

Session opens and the exchange time zone come from ``exchange_calendars`` directly (the same
pinned version open-data-store's ``recipes`` extra installs), since ``sessions`` returns closes
only.
"""

from __future__ import annotations

import math
import warnings
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from open_data_store.contracts.standard.market import CORPORATE_ACTIONS, FETCH_LOG, PRICES
from open_data_store.market.asof import sessions

from market_data.adapters.base import (
    FETCH_KINDS,
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    RAW_SYMBOL_SCHEMA,
    WHOLE_MARKET_SYMBOL,
    Adapter,
    FetchResult,
)
from market_data.archive import ArchivedFetch, RawArchive

__all__ = [
    "DROP_INTRADAY_PARTIAL",
    "DROP_NON_SESSION",
    "ROW_STATUSES",
    "BuildResult",
    "build_actions",
    "build_fetch_log",
    "build_prices",
]

# Fetch statuses whose payload contributes rows (rulings: not_found and error never do).
ROW_STATUSES = ("ok", "empty", "partial")
DROP_NON_SESSION = "non_session"
DROP_INTRADAY_PARTIAL = "intraday_partial"
DROP_REASONS = (DROP_NON_SESSION, DROP_INTRADAY_PARTIAL)
# Calendar margin past the last ex-date, so a non-session ex-date finds its next session.
_NEXT_SESSION_MARGIN = timedelta(days=14)

_RAW_SCHEMAS = {
    "prices": RAW_PRICE_SCHEMA,
    "actions": RAW_ACTION_SCHEMA,
    "symbols": RAW_SYMBOL_SCHEMA,
}
_PRICE_VALUE = ("open", "high", "low", "close", "volume", "currency")
_ACTION_IDENTITY = ("source_symbol", "valid_date", "action_type", "action_seq")
_ACTION_VALUE = tuple(
    n for n in RAW_ACTION_SCHEMA.names if n not in (*_ACTION_IDENTITY, "source_code")
)

Key = tuple[str, date]


@dataclass(frozen=True)
class BuildResult:
    """A contract table and the rows dropped while building it (see the module docstring)."""

    table: pa.Table
    dropped: Mapping[str, tuple[Key, ...]] = field(
        default_factory=lambda: {r: () for r in DROP_REASONS}
    )

    def counts(self) -> dict[str, int]:
        """Reason -> number of distinct (source_symbol, valid_date) keys dropped."""
        return {reason: len(keys) for reason, keys in self.dropped.items()}


# --- reading the archive -----------------------------------------------------------------------


def _fetches(
    archive: RawArchive, source_code: str, kind: str, symbols: Sequence[str] | None
) -> list[ArchivedFetch]:
    if symbols is None:
        names = archive.symbols(source_code, kind)
    else:
        if WHOLE_MARKET_SYMBOL in symbols:
            raise ValueError(f"{WHOLE_MARKET_SYMBOL!r} is not a requestable symbol")
        names = sorted({*symbols, WHOLE_MARKET_SYMBOL})
    entries = [e for n in names for e in archive.history_with_refs(source_code, n, kind)]
    entries.sort(key=lambda e: (e.result.fetched_at, e.archive_ref))
    return entries


def _parse(adapter: Adapter, result: FetchResult) -> pa.Table:
    """The adapter's rows for one fetch, checked and cast to the raw schema of its kind."""
    schema = _RAW_SCHEMAS[result.kind]
    table = adapter.parse(result)
    missing = [n for n in schema.names if n not in table.column_names]
    if missing:
        raise ValueError(f"adapter {adapter.source_code!r} parse is missing columns {missing}")
    table = table.select(schema.names).cast(schema)
    for f in schema:
        if not f.nullable and table[f.name].null_count:
            raise ValueError(f"adapter {adapter.source_code!r} parse has nulls in {f.name!r}")
    if "source_code" in table.column_names and table.num_rows:
        codes = set(table["source_code"].to_pylist())
        if codes != {adapter.source_code}:
            raise ValueError(
                f"rows of source_code {sorted(codes)} in a fetch of {adapter.source_code!r}"
            )
    return table


def _rows(
    archive: RawArchive,
    adapter: Adapter,
    kind: str,
    symbols: Sequence[str] | None,
) -> list[tuple[datetime, list[dict[str, Any]]]]:
    """(fetched_at UTC, rows) per contributing fetch, in archive order, filtered to symbols."""
    wanted = None if symbols is None else pa.array(sorted(set(symbols)), pa.string())
    out = []
    for entry in _fetches(archive, adapter.source_code, kind, symbols):
        if entry.result.status not in ROW_STATUSES:
            continue
        table = _parse(adapter, entry.result)
        if wanted is not None:
            table = table.filter(pc.is_in(table["source_symbol"], value_set=wanted))
        out.append((entry.result.fetched_at.astimezone(UTC), table.to_pylist()))
    return out


# --- change detection --------------------------------------------------------------------------


def _comparable(values: Iterable[Any]) -> tuple:
    """Values as compared across fetches: nulls are values, NaN equals NaN."""
    return tuple(("<nan>",) if isinstance(v, float) and math.isnan(v) else v for v in values)


class _Tracker:
    """First-seen / changed logic shared by prices and actions."""

    def __init__(self) -> None:
        self._last: dict[tuple, tuple[tuple, datetime]] = {}
        self._in_fetch: dict[tuple, tuple] = {}

    def start_fetch(self) -> None:
        self._in_fetch = {}

    def seen(self, key: tuple) -> bool:
        return key in self._last

    def is_new_value(self, key: tuple, value: tuple) -> bool:
        """Whether ``value`` must be emitted; raises on two values for one key in one fetch."""
        prior = self._in_fetch.get(key)
        if prior is not None:
            if prior != value:
                raise ValueError(f"conflicting values for {key} within one fetch")
            return False
        self._in_fetch[key] = value
        last = self._last.get(key)
        return last is None or last[0] != value

    def record(self, key: tuple, value: tuple, known_at: datetime) -> None:
        last = self._last.get(key)
        if last is not None and last[1] == known_at:
            raise ValueError(f"conflicting values for {key} at one fetch time {known_at}")
        self._last[key] = (value, known_at)


# --- prices ------------------------------------------------------------------------------------


def build_prices(
    archive: RawArchive,
    adapter: Adapter,
    symbols: Sequence[str] | None,
    *,
    mic: str = "XNYS",
) -> BuildResult:
    """``prices`` rows of ``adapter``'s source for ``symbols`` (None: every symbol archived)."""
    fetches = _rows(archive, adapter, "prices", symbols)
    days = [r["valid_date"] for _, rows in fetches for r in rows]
    closes: dict[date, datetime] = {}
    if days:
        cal = sessions(mic, min(days), max(days))
        closes = dict(
            zip(cal["session_date"].to_pylist(), cal["close_utc"].to_pylist(), strict=True)
        )
    tracker = _Tracker()
    dropped: dict[str, set[Key]] = {r: set() for r in DROP_REASONS}
    out: list[dict[str, Any]] = []
    for fetched_at, rows in fetches:
        tracker.start_fetch()
        for r in rows:
            key = (r["source_symbol"], r["valid_date"])
            close = closes.get(key[1])
            if close is None:
                dropped[DROP_NON_SESSION].add(key)
                continue
            if not tracker.seen(key) and fetched_at < close:
                dropped[DROP_INTRADAY_PARTIAL].add(key)
                continue
            value = _comparable(r[c] for c in _PRICE_VALUE)
            if not tracker.is_new_value(key, value):
                continue
            if tracker.seen(key):
                known_at, kind, basis = fetched_at, "collected", "fetch_time"
            else:
                known_at, kind, basis = close, "published", "session_close"
            tracker.record(key, value, known_at)
            out.append(
                {
                    **{c: r[c] for c in ("source_symbol", "valid_date", *_PRICE_VALUE)},
                    "price_basis": "raw",
                    "known_at_basis": basis,
                    "known_at": known_at,
                    "known_at_kind": kind,
                    "source_code": adapter.source_code,
                    "source_doc_id": None,
                }
            )
    return BuildResult(
        _table(out, PRICES.schema, ("source_symbol", "valid_date", "known_at")),
        {reason: tuple(sorted(keys)) for reason, keys in dropped.items()},
    )


# --- corporate actions -------------------------------------------------------------------------


def _calendar(mic: str, start: date, end: date):
    import exchange_calendars as xcals

    with warnings.catch_warnings():
        # Upstream: exchange_calendars 4.13.2 calls Series.values on tz-aware data while
        # building a calendar, which pandas 3.1 flags (same filter as asof.sessions).
        warnings.filterwarnings(
            "ignore", message="Series.values returning an ndarray", category=Warning
        )
        return xcals.get_calendar(mic, start=start.isoformat(), end=end.isoformat())


def build_actions(
    archive: RawArchive,
    adapter: Adapter,
    symbols: Sequence[str] | None,
    *,
    mic: str = "XNYS",
) -> BuildResult:
    """``corporate_actions`` rows of ``adapter``'s source for ``symbols`` (None: every symbol)."""
    import pandas as pd

    fetches = _rows(archive, adapter, "actions", symbols)
    ex_dates = [r["valid_date"] for _, rows in fetches for r in rows]
    cal = _calendar(mic, min(ex_dates), max(ex_dates) + _NEXT_SESSION_MARGIN) if ex_dates else None
    tracker = _Tracker()
    out: list[dict[str, Any]] = []
    for fetched_at, rows in fetches:
        tracker.start_fetch()
        for r in rows:
            key = tuple(r[c] for c in _ACTION_IDENTITY)
            value = _comparable(r[c] for c in _ACTION_VALUE)
            if not tracker.is_new_value(key, value):
                continue
            if tracker.seen(key):
                known_at, kind, basis = fetched_at, "collected", "fetch_time"
            else:
                ex, announced = r["valid_date"], r["announce_date"]
                if announced is not None and announced <= ex:
                    eod = datetime.combine(announced, time.max, tzinfo=cal.tz)
                    first, basis = eod.astimezone(UTC), "announce_date"
                else:
                    session = cal.date_to_session(pd.Timestamp(ex), direction="next")
                    opened = cal.session_open(session).to_pydatetime().astimezone(UTC)
                    first, basis = opened, "ex_date_assumed"
                known_at, kind = min(first, fetched_at), "published"
            tracker.record(key, value, known_at)
            out.append(
                {
                    **{c: r[c] for c in (*_ACTION_IDENTITY, *_ACTION_VALUE)},
                    "known_at_basis": basis,
                    "known_at": known_at,
                    "known_at_kind": kind,
                    "source_code": adapter.source_code,
                    "source_doc_id": None,
                }
            )
    return BuildResult(_table(out, CORPORATE_ACTIONS.schema, (*_ACTION_IDENTITY, "known_at")))


# --- fetch log ---------------------------------------------------------------------------------


def build_fetch_log(
    archive: RawArchive,
    adapter: Adapter,
    source_code: str,
    *,
    kinds: Collection[str] = FETCH_KINDS,
) -> pa.Table:
    """``fetch_log`` rows: one per archived fetch of ``source_code`` of the given ``kinds``.

    ``row_count`` and ``returned_from``/``returned_to`` come from the adapter's parse of the whole
    payload (a ``"*"`` fetch counts every symbol); fetches whose status is not in
    ``ROW_STATUSES`` are not parsed (0 rows, null returned range)."""
    if adapter.source_code != source_code:
        raise ValueError(
            f"adapter for {adapter.source_code!r} cannot parse fetches of {source_code!r}"
        )
    out: list[dict[str, Any]] = []
    for kind in sorted(kinds):
        for symbol in archive.symbols(source_code, kind):
            for ref, result in archive.history_with_refs(source_code, symbol, kind):
                returned: tuple[date | None, date | None] = (None, None)
                count = 0
                if result.status in ROW_STATUSES:
                    table = _parse(adapter, result)
                    count = table.num_rows
                    if count:
                        span = pc.min_max(table["valid_date"]).as_py()
                        returned = (span["min"], span["max"])
                fetched_at = result.fetched_at.astimezone(UTC)
                out.append(
                    {
                        "source_symbol": result.source_symbol,
                        "fetch_id": ref,
                        "requested_from": result.requested_from,
                        "requested_to": result.requested_to,
                        "returned_from": returned[0],
                        "returned_to": returned[1],
                        "status": result.status,
                        "row_count": count,
                        "archive_ref": ref,
                        "known_at_basis": "fetch_time",
                        "valid_date": fetched_at.date(),
                        "known_at": fetched_at,
                        "known_at_kind": "collected",
                        "source_code": source_code,
                        "source_doc_id": None,
                    }
                )
    return _table(out, FETCH_LOG.schema, ("source_symbol", "fetch_id", "valid_date"))


def _table(rows: list[dict[str, Any]], schema: pa.Schema, order: Sequence[str]) -> pa.Table:
    rows = sorted(rows, key=lambda r: tuple(r[c] for c in order))
    return pa.Table.from_pylist(rows, schema=schema)

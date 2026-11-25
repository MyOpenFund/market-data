"""Massive (formerly Polygon.io) adapter: our free daily US source (Stocks Basic plan).

Terms (https://massive.com/legal/market-data-terms-of-service): personal, non-commercial use
with the user's own key; no redistribution or public display (§5(c)), no AI/ML training (§5(g)).
The source is registered ``restricted`` with ``no_public_display`` and ``no_ml_training``.

Endpoints (documented, verified on the free plan 2026-11-25), all under ``base_url``
(default ``BASE_URL``):

``prices``, whole market (``symbols`` empty or ``["*"]``)
    ``GET /v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=false&include_otc=false``,
    one call per session of ``mic`` in ``[start, end]`` whose close is not after ``now``
    (weekends, holidays and sessions not closed yet are never requested; a range ending in the
    future is clipped), one FetchResult per session: ``source_symbol="*"``, requested_from =
    requested_to = the session, the raw answer as payload; no result rows gives status ``empty``.
``prices``, per symbol (backfill inside the plan's history window)
    ``GET /v2/aggs/ticker/{T}/range/1/day/{start}/{end}?adjusted=false&sort=asc&limit=50000``,
    ``end`` clipped to the last session of ``[start, end]`` closed by ``now`` (none: no request),
    one FetchResult per symbol.
``actions``, whole market
    ``GET /stocks/v1/dividends`` (``ex_dividend_date`` in ``[start, end]``), then
    ``GET /stocks/v1/splits`` (``execution_date`` in ``[start, end]``), ``limit=5000``: one
    FetchResult each, requested range ``[start, end]`` (future ex-dates are legitimate).
``symbols``, whole market
    ``GET /v3/reference/tickers?market=stocks&active=true`` then ``active=false``,
    ``limit=1000``: one FetchResult each.

Lists are atomic (ruling 68): every list (per-symbol aggregates, dividends, splits, tickers)
follows ``next_url`` exactly as Massive gives it and is yielded as ONE FetchResult only once all
its pages arrived, its payload one merged JSON object ``{"status": "OK", "results": [...]}``
(plus ``"adjusted": false`` for aggregates), the rows of every page in page order; the raw page
bytes are not kept. A ``next_url`` on another scheme or host than ``base_url`` is refused, so the
key never leaves for another server; a ``next_url`` repeating a page already fetched, or a list
longer than ``max_pages`` (default 1000) pages, is a failure (no runaway cursor).

Failures are logged (rulings 68, 69): when a request fails (after the bounded retries), or an
answer is unusable as a whole (not JSON, ``status: ERROR``, ``results`` not a list, prices
flagged ``adjusted: true``), ``fetch`` yields a FetchResult with status ``error``, the full
requested range and the payload ``ERROR_PAYLOAD`` (``{"results": []}``), then raises
``MassiveError``: the fetch log records the failure, nothing is ever archived as ``ok`` that
``parse`` would refuse (``fetch`` runs ``parse_report`` on each payload before yielding it).
Any other exception while fetching (an invalid URL, a bug) takes the same path: an ``error``
FetchResult, then a ``MassiveError`` naming the exception type only (no ``__cause__`` or
``__context__``, whose text could hold a URL or payload data).

Key and quota (spec §8.3: respected, never worked around)
    The key comes from ``MASSIVE_API_KEY`` (``from_env``), is held in a ``Secret`` and sent only
    in the ``Authorization: Bearer`` header: never in a URL, a log line, an error message, a
    repr or an archived FetchResult. ``fetch`` checks the key before returning (no request
    otherwise): missing → ``MassiveKeyError`` saying how to get a free one; characters outside
    printable ASCII → ``MassiveKeyError`` echoing none of them. ``parse`` needs no key (builds
    from the archive work offline). The free plan allows ``FREE_PLAN_RPM`` (5) requests a minute:
    ``rpm`` may be set lower, never higher; requests (retries included) are spaced evenly, one
    every ``60 / rpm`` seconds. A 429, a 5xx or a transport error is retried: wait
    ``Retry-After`` seconds when sent, else ``backoff_s * 2**n`` (n = retries so far), never less
    than the spacing; after ``max_retries`` retries, or when a wait would exceed ``max_wait_s``,
    ``MassiveError`` (no retry storm). 401/403 are ``MassiveKeyError`` naming the HTTP status and
    Massive's status word (e.g. ``NOT_AUTHORIZED``: a refused key, or a request outside the plan);
    any other non-200 answer (redirects included, never followed) is a ``MassiveError`` naming the
    HTTP status. ``max_history_days`` (default 730, the free plan's 2 years; None disables)
    refuses a ``start`` further back before any request. Clock, sleep, wall time (``now``) and
    the HTTP transport are injectable. The adapter holds an HTTP client: ``close()`` it, or use
    it as a context manager.

Parse (``parse`` / ``parse_report``)
    prices → ``RAW_PRICE_SCHEMA``: ``T`` (per-symbol fetch: the fetch's symbol) → source_symbol;
    valid_date = the requested session for a grouped fetch, else the bar timestamp ``t`` (ms UTC)
    as a calendar date in ``exchange_tz``; o/h/l/c as floats; ``v`` → int64 (missing → null);
    currency ``USD``.
    dividends → ``RAW_ACTION_SCHEMA``: ``ex_dividend_date`` → valid_date, ``declaration_date`` →
    announce_date, ``record_date``, ``pay_date``, raw ``cash_amount`` (never
    ``split_adjusted_cash_amount``) → amount, ``currency`` stripped and upper-cased (3 letters,
    never a minor unit: ``GBp``-style as given, ``GBX``, ``ZAX``, ``ILA``); ``distribution_type``
    ``recurring`` →
    ``cash_dividend_regular``, ``special``/``supplemental``/``irregular`` →
    ``cash_dividend_special``. ``source_action_type`` = the raw type.
    splits → ``execution_date`` → valid_date, ``split_from`` → ratio_old, ``split_to`` →
    ratio_new; ``adjustment_type`` ``forward_split``/``reverse_split`` → ``split``,
    ``stock_dividend`` → ``stock_dividend``.
    A row is a dividend when its ``ex_dividend_date`` is not null, a split when its
    ``execution_date`` is not null. Dates are plain ``YYYY-MM-DD`` only.
    action_seq: the rows are first sorted by the vendor's stable ``id`` (as text; rows without
    one after, by all their fields) and rows sharing one ``id`` are one vendor action
    (identical: kept once, ``duplicate_row``; different: all kept out,
    ``conflicting_duplicate``); then numbered from 0 per (symbol, ex-date, action_type), so page
    boundaries and a vendor reordering from one night to the next never change an identity. A
    row kept out once its identity is known (symbol, ex-date and a known type: bad amount,
    currency, ratio, date, or a conflicting id) still takes its slot, so a later vendor repair of
    a lower-id row does not shift the others; a row of unknown type takes none. A seq beyond the
    int16 column is kept out (``seq_overflow``).
    symbols → ``RAW_SYMBOL_SCHEMA``: per ticker one row per present scheme among ``ticker``,
    ``composite_figi``, ``share_class_figi``, ``cik`` (zero-padded to 10 digits), with ``name``;
    valid_date = ``list_date`` when present, else null; valid_to = the UTC calendar date of
    ``delisted_utc`` + 1 day when present, else null.

Kept out, never blocking (ruling 69): a bad row never blocks a source. It is kept out and
counted under a reason: ``not_an_object``; ``missing_field`` (a required field absent, of the
wrong type or not a valid date: ``T``/``t``/``c``, ``ticker``, the ex-date, the currency of a
dividend, a ``list_date``/``delisted_utc``, including one out of the calendar's range);
``non_finite`` (NaN, infinite, or beyond the float range); ``non_positive_close``;
``bad_volume`` (not an integer in ``0..MAX_VOLUME``, the int64 column); ``bad_timestamp`` (a bar
``t`` outside ``TIMESTAMP_RANGE``); ``bad_ratio`` (a split ratio null, zero or negative);
``null_amount`` (a dividend without ``cash_amount``); ``non_positive_amount`` (zero or negative);
``bad_currency``; ``seq_overflow``;
``ambiguous_action`` (both an ex-dividend and an execution date); ``unknown_distribution_type``
and ``unknown_adjustment_type`` (any other or missing type: never guessed); for prices,
``duplicate_row`` (an exact repeat of a (symbol, date) bar, kept once) and
``conflicting_duplicate`` (one (symbol, date) with different values: every such bar out, since
there is no honest choice; for actions, the same per vendor ``id``); for symbols,
``bad_identifier`` (that one scheme dropped, the ticker's other schemes kept; the ticker is
stripped, the same in source_symbol and id_value). ``parse_report`` returns ``kept_out``:
reason -> sorted tuple of distinct ``(source_symbol, valid_date)`` KEYS (the builder's
drop-count shape; ``""`` / None where the row has no symbol / date), ``counts()`` the number of
distinct keys per reason and
``row_counts`` the number of rows per reason. Only a payload unusable as a whole raises
``ValueError``.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import httpx
import pyarrow as pa
from open_data_store.market.asof import sessions
from open_data_store.sources import SourceInfo

from market_data.adapters.base import (
    FETCH_KINDS,
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    RAW_SYMBOL_SCHEMA,
    WHOLE_MARKET_SYMBOL,
    FetchResult,
    Secret,
)
from market_data.sources import sources

__all__ = [
    "AGGS_LIMIT",
    "AMBIGUOUS_ACTION",
    "API_KEY_ENV",
    "BAD_CURRENCY",
    "BAD_IDENTIFIER",
    "BAD_RATIO",
    "BAD_TIMESTAMP",
    "BAD_VOLUME",
    "BASE_URL",
    "CONFLICTING_DUPLICATE",
    "DEFAULT_BACKOFF_S",
    "DEFAULT_MAX_HISTORY_DAYS",
    "DEFAULT_MAX_PAGES",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_MAX_WAIT_S",
    "DEFAULT_TIMEOUT_S",
    "DIVIDEND_TYPES",
    "DUPLICATE_ROW",
    "ERROR_PAYLOAD",
    "FREE_PLAN_RPM",
    "KEPT_OUT_REASONS",
    "LIST_LIMIT",
    "MAX_VOLUME",
    "MINOR_UNIT_CURRENCIES",
    "MISSING_FIELD",
    "NON_FINITE",
    "NON_POSITIVE_AMOUNT",
    "NON_POSITIVE_CLOSE",
    "NOT_AN_OBJECT",
    "NULL_AMOUNT",
    "SEQ_OVERFLOW",
    "SOURCE_CODE",
    "SPLIT_TYPES",
    "TICKERS_LIMIT",
    "TIMESTAMP_RANGE",
    "UNKNOWN_ADJUSTMENT_TYPE",
    "UNKNOWN_DISTRIBUTION_TYPE",
    "MassiveAdapter",
    "MassiveError",
    "MassiveKeyError",
    "ParseReport",
    "adapter",
]

SOURCE_CODE = "massive"
BASE_URL = "https://api.massive.com"
API_KEY_ENV = "MASSIVE_API_KEY"
FREE_PLAN_RPM = 5
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_S = 15.0
DEFAULT_MAX_WAIT_S = 300.0
DEFAULT_TIMEOUT_S = 30.0
DEFAULT_MAX_PAGES = 1000  # pages of one list before giving up (a runaway next_url)
DEFAULT_MAX_HISTORY_DAYS = 730  # the free plan's 2-year window
AGGS_LIMIT = 50000  # documented maximum of the aggregates endpoint
LIST_LIMIT = 5000  # documented maximum of /stocks/v1/dividends and /stocks/v1/splits
TICKERS_LIMIT = 1000  # documented maximum of /v3/reference/tickers
DEFAULT_MIC = "XNYS"
DEFAULT_EXCHANGE_TZ = "America/New_York"
CURRENCY = "USD"
CONTENT_TYPE = "application/json"
ERROR_PAYLOAD = b'{"results": []}'
_MINUTE_S = 60.0
_CIK_WIDTH = 10
_KEY_CHARS = re.compile(r"[\x21-\x7e]+")  # visible ASCII: what an HTTP header can carry
_STATUS_WORD = re.compile(r"[A-Z_]{1,32}")
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_CURRENCY = re.compile(r"[A-Z]{3}")
_MINOR_UNIT_GIVEN = re.compile(r"[A-Z]{2}[a-z]")  # GBp, ZAc, ILa: minor units as given
MAX_VOLUME = 2**63 - 1  # int64, the volume column
MAX_SEQ = 2**15 - 1  # int16, the action_seq column
# Minor-unit codes (pence, cents, agorot) a per-share amount must never be read in.
MINOR_UNIT_CURRENCIES = frozenset({"GBX", "ZAX", "ILA"})
# Bar timestamps outside this range (UTC) are not dates of a daily bar (``bad_timestamp``).
TIMESTAMP_RANGE = (datetime(1900, 1, 1, tzinfo=UTC), datetime(2200, 1, 1, tzinfo=UTC))

DIVIDEND_TYPES = {
    "recurring": "cash_dividend_regular",
    "special": "cash_dividend_special",
    "supplemental": "cash_dividend_special",
    "irregular": "cash_dividend_special",
}
SPLIT_TYPES = {
    "forward_split": "split",
    "reverse_split": "split",
    "stock_dividend": "stock_dividend",
}
NOT_AN_OBJECT = "not_an_object"
MISSING_FIELD = "missing_field"
NON_FINITE = "non_finite"
NON_POSITIVE_CLOSE = "non_positive_close"
BAD_VOLUME = "bad_volume"
BAD_RATIO = "bad_ratio"
NULL_AMOUNT = "null_amount"
NON_POSITIVE_AMOUNT = "non_positive_amount"
BAD_CURRENCY = "bad_currency"
BAD_TIMESTAMP = "bad_timestamp"
SEQ_OVERFLOW = "seq_overflow"
AMBIGUOUS_ACTION = "ambiguous_action"
UNKNOWN_DISTRIBUTION_TYPE = "unknown_distribution_type"
UNKNOWN_ADJUSTMENT_TYPE = "unknown_adjustment_type"
DUPLICATE_ROW = "duplicate_row"
CONFLICTING_DUPLICATE = "conflicting_duplicate"
BAD_IDENTIFIER = "bad_identifier"
KEPT_OUT_REASONS = (
    NOT_AN_OBJECT,
    MISSING_FIELD,
    NON_FINITE,
    NON_POSITIVE_CLOSE,
    BAD_VOLUME,
    BAD_RATIO,
    NULL_AMOUNT,
    NON_POSITIVE_AMOUNT,
    BAD_CURRENCY,
    BAD_TIMESTAMP,
    SEQ_OVERFLOW,
    AMBIGUOUS_ACTION,
    UNKNOWN_DISTRIBUTION_TYPE,
    UNKNOWN_ADJUSTMENT_TYPE,
    DUPLICATE_ROW,
    CONFLICTING_DUPLICATE,
    BAD_IDENTIFIER,
)
_SYMBOL_SCHEMES = ("ticker", "composite_figi", "share_class_figi", "cik")

_GET_A_KEY = (
    f"set {API_KEY_ENV} to your own Massive API key: create a free account at "
    "https://massive.com (Stocks Basic plan) and copy the key from the dashboard"
)

Key = tuple[str, date | None]


class MassiveError(RuntimeError):
    """A Massive request or answer that cannot be used. Never carries the key."""


class MassiveKeyError(MassiveError):
    """No usable key, or a key Massive refuses."""


def _no_kept_out() -> dict[str, tuple[Key, ...]]:
    return {r: () for r in KEPT_OUT_REASONS}


@dataclass(frozen=True)
class ParseReport:
    """Rows parsed from one fetch and the rows kept out (see the module docstring)."""

    table: pa.Table
    kept_out: Mapping[str, tuple[Key, ...]] = field(default_factory=_no_kept_out)
    row_counts: Mapping[str, int] = field(
        default_factory=lambda: dict.fromkeys(KEPT_OUT_REASONS, 0)
    )

    def counts(self) -> dict[str, int]:
        """Reason -> number of distinct (source_symbol, valid_date) keys kept out."""
        return {reason: len(keys) for reason, keys in self.kept_out.items()}


class _KeptOut(Exception):
    """One row kept out of a parse (internal control flow, never escapes)."""

    def __init__(
        self,
        reason: str,
        symbol: str = "",
        day: date | None = None,
        action_type: str | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason, self.key = reason, (symbol, day)
        # the action identity, once known: a kept-out action still takes its action_seq slot
        self.ident = (symbol, day, action_type) if action_type and day else None


class _Tally:
    def __init__(self) -> None:
        self.keys: dict[str, set[Key]] = {r: set() for r in KEPT_OUT_REASONS}
        self.rows: dict[str, int] = dict.fromkeys(KEPT_OUT_REASONS, 0)

    def add(self, reason: str, key: Key) -> None:
        self.keys[reason].add(key)
        self.rows[reason] += 1

    def report(self, table: pa.Table) -> ParseReport:
        def order(k: Key) -> tuple:
            return (k[0], k[1] is not None, k[1] or date.min)

        kept = {r: tuple(sorted(keys, key=order)) for r, keys in self.keys.items()}
        return ParseReport(table, kept, dict(self.rows))


def _info() -> SourceInfo:
    return next(s for s in sources() if s.code == SOURCE_CODE)


class MassiveAdapter:
    source_code = SOURCE_CODE

    def __init__(
        self,
        api_key: str | Secret | None,
        *,
        rpm: int = FREE_PLAN_RPM,
        include_otc: bool = False,
        base_url: str = BASE_URL,
        mic: str = DEFAULT_MIC,
        exchange_tz: str = DEFAULT_EXCHANGE_TZ,
        max_history_days: int | None = DEFAULT_MAX_HISTORY_DAYS,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_pages: int = DEFAULT_MAX_PAGES,
        backoff_s: float = DEFAULT_BACKOFF_S,
        max_wait_s: float = DEFAULT_MAX_WAIT_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        raw = api_key.reveal() if isinstance(api_key, Secret) else (api_key or "")
        self._key = Secret(raw.strip()) if raw.strip() else None
        if not 0 < rpm <= FREE_PLAN_RPM:
            raise ValueError(f"rpm must be in 1..{FREE_PLAN_RPM} (the free plan), got {rpm}")
        if max_retries < 0 or backoff_s <= 0 or max_wait_s <= 0:
            raise ValueError("max_retries must be >= 0, backoff_s and max_wait_s > 0")
        if max_pages <= 0:
            raise ValueError(f"max_pages must be > 0, got {max_pages}")
        if max_history_days is not None and max_history_days <= 0:
            raise ValueError(f"max_history_days must be > 0 or None, got {max_history_days}")
        self.info = _info()
        self.rpm = rpm
        self.include_otc = include_otc
        self.base_url = base_url.rstrip("/")
        self.mic = mic
        self.exchange_tz = ZoneInfo(exchange_tz)
        self.max_history_days = max_history_days
        self.max_retries = max_retries
        self.max_pages = max_pages
        self.backoff_s = backoff_s
        self.max_wait_s = max_wait_s
        self._interval = _MINUTE_S / rpm
        self._clock = clock
        self._sleep = sleep
        self._now = now
        self._next: float | None = None  # earliest clock time of the next request
        self._origin = _origin(self.base_url)
        self._http = httpx.Client(transport=transport, timeout=timeout_s)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **kw: Any) -> MassiveAdapter:
        """An adapter keyed from ``MASSIVE_API_KEY`` (unset: ``fetch`` raises, ``parse`` works)."""
        raw = (os.environ if env is None else env).get(API_KEY_ENV, "")
        return cls(Secret(raw.strip()) if raw.strip() else None, **kw)

    def __repr__(self) -> str:
        key = "Secret(***)" if self._key else "None"
        return f"MassiveAdapter(api_key={key}, rpm={self.rpm}, include_otc={self.include_otc})"

    __str__ = __repr__

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> MassiveAdapter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- fetch ---------------------------------------------------------------------------------

    def fetch(
        self, kind: str, symbols: Sequence[str], start: date | None, end: date | None
    ) -> Iterator[FetchResult]:
        """FetchResults of ``kind`` (see the module docstring). Arguments and the key are checked
        before this returns; requests are made as the iterator is consumed."""
        if kind not in FETCH_KINDS:
            raise ValueError(f"fetch kind must be one of {FETCH_KINDS}, got {kind!r}")
        names = list(symbols)
        whole = names in ([], [WHOLE_MARKET_SYMBOL])
        if not whole and WHOLE_MARKET_SYMBOL in names:
            raise ValueError(f"{WHOLE_MARKET_SYMBOL!r} cannot be mixed with symbols")
        if kind != "prices" and not whole:
            raise ValueError(f"Massive {kind} are fetched whole-market: pass no symbols")
        if kind != "symbols":
            if start is None:
                raise ValueError(f"Massive {kind} need a start date")
            end = start if end is None else end
            if start > end:
                raise ValueError(f"start {start.isoformat()} is after end {end.isoformat()}")
            if self.max_history_days is not None:
                today = self._now().astimezone(UTC).date()
                oldest = today - timedelta(days=self.max_history_days)
                if start < oldest:
                    raise ValueError(
                        f"start {start.isoformat()} is before {oldest.isoformat()}, outside "
                        f"max_history_days ({self.max_history_days}) of the plan"
                    )
        self._header()
        if kind == "symbols":
            return self._fetch_symbols()
        assert start is not None and end is not None
        if kind == "actions":
            return self._fetch_actions(start, end)
        if whole:
            return self._fetch_grouped(start, end)
        return self._fetch_aggs(names, start, end)

    def _header(self) -> dict[str, str]:
        if not self._key:
            raise MassiveKeyError(f"no Massive API key: {_GET_A_KEY}")
        value = self._key.reveal()
        if not _KEY_CHARS.fullmatch(value):
            raise MassiveKeyError(
                f"the Massive API key in {API_KEY_ENV} has characters an HTTP header cannot "
                "carry (printable ASCII only); check it was copied whole"
            )
        return {"Authorization": f"Bearer {value}"}

    def _closed_sessions(self, start: date, end: date) -> list[date]:
        """Sessions of ``mic`` in ``[start, end]`` whose close is not after now."""
        now = self._now()
        cal = sessions(self.mic, start, end)
        return [
            d
            for d, close in zip(
                cal["session_date"].to_pylist(), cal["close_utc"].to_pylist(), strict=True
            )
            if close <= now
        ]

    def _fetch_grouped(self, start: date, end: date) -> Iterator[FetchResult]:
        params = {"adjusted": "false", "include_otc": "true" if self.include_otc else "false"}
        for day in self._closed_sessions(start, end):
            url = f"{self.base_url}/v2/aggs/grouped/locale/us/market/stocks/{day.isoformat()}"
            yield from self._emit(
                WHOLE_MARKET_SYMBOL, "prices", day, day, lambda u=url: self._get(u, params)
            )

    def _fetch_aggs(self, names: list[str], start: date, end: date) -> Iterator[FetchResult]:
        closed = self._closed_sessions(start, end)
        if not closed:
            return
        end = closed[-1]
        params = {"adjusted": "false", "sort": "asc", "limit": str(AGGS_LIMIT)}
        for name in names:
            url = (
                f"{self.base_url}/v2/aggs/ticker/{quote(name, safe='')}/range/1/day/"
                f"{start.isoformat()}/{end.isoformat()}"
            )
            yield from self._emit(
                name, "prices", start, end, lambda u=url: self._list(u, params, aggs=True)
            )

    def _fetch_actions(self, start: date, end: date) -> Iterator[FetchResult]:
        for path, column in (
            ("/stocks/v1/dividends", "ex_dividend_date"),
            ("/stocks/v1/splits", "execution_date"),
        ):
            params = {
                f"{column}.gte": start.isoformat(),
                f"{column}.lte": end.isoformat(),
                "limit": str(LIST_LIMIT),
            }
            url = self.base_url + path
            yield from self._emit(
                WHOLE_MARKET_SYMBOL,
                "actions",
                start,
                end,
                lambda u=url, p=params: self._list(u, p),
            )

    def _fetch_symbols(self) -> Iterator[FetchResult]:
        url = self.base_url + "/v3/reference/tickers"
        for active in ("true", "false"):
            params = {"market": "stocks", "active": active, "limit": str(TICKERS_LIMIT)}
            yield from self._emit(
                WHOLE_MARKET_SYMBOL, "symbols", None, None, lambda p=params: self._list(url, p)
            )

    def _emit(
        self,
        symbol: str,
        kind: str,
        start: date | None,
        end: date | None,
        request: Callable[[], tuple[bytes, Mapping[str, Any]]],
    ) -> Iterator[FetchResult]:
        """One FetchResult from ``request``; on failure an ``error`` FetchResult, then raise."""
        failure: MassiveError | None = None
        try:
            payload, body = request()
            result = self._result(symbol, kind, start, end, payload, body)
            self.parse_report(result)
        except MassiveError as exc:
            failure = exc
        except ValueError as exc:  # parse_report: the payload is unusable as a whole
            failure = MassiveError(f"Massive {kind} answer refused: {exc}")
        except Exception as exc:  # noqa: BLE001 - any failure is logged as an error fetch
            # the type only: an unexpected exception's text may hold a URL or payload data
            failure = MassiveError(f"Massive {kind} fetch failed: {type(exc).__name__}")
        if failure is None:
            yield result
            return
        yield self._result(symbol, kind, start, end, ERROR_PAYLOAD, None)
        failure.__context__ = failure.__cause__ = None
        raise failure

    def _result(
        self,
        symbol: str,
        kind: str,
        start: date | None,
        end: date | None,
        payload: bytes,
        body: Mapping[str, Any] | None,
    ) -> FetchResult:
        if body is None:
            status = "error"
        else:
            status = "ok" if body.get("results") else "empty"
        return FetchResult(
            source_code=self.source_code,
            source_symbol=symbol,
            kind=kind,  # type: ignore[arg-type]
            requested_from=start,
            requested_to=end,
            fetched_at=self._now(),
            status=status,
            payload=payload,
            content_type=CONTENT_TYPE,
        )

    def _list(
        self, url: str, params: Mapping[str, str], *, aggs: bool = False
    ) -> tuple[bytes, Mapping[str, Any]]:
        """Every page of a list, following ``next_url`` as given (never on another host), merged
        into one JSON object."""
        rows: list[Any] = []
        page: tuple[str, Mapping[str, str] | None] | None = (url, params)
        seen: set[str] = set()
        while page is not None:
            if len(seen) >= self.max_pages:
                raise MassiveError(f"Massive list longer than max_pages ({self.max_pages})")
            seen.add(page[0])
            _, body = self._get(*page)
            results = body.get("results")
            if results is not None and not isinstance(results, list):
                raise MassiveError("Massive answer results is not a list")
            if aggs and body.get("adjusted") is True:
                raise MassiveError(
                    "Massive answered adjusted prices; only unadjusted prices are accepted"
                )
            rows.extend(results or ())
            nxt = body.get("next_url")
            if not nxt:
                page = None
            elif not isinstance(nxt, str) or _origin(nxt) != self._origin:
                raise MassiveError("Massive next_url points outside base_url; refused")
            elif nxt in seen:
                raise MassiveError("Massive next_url repeats a page already fetched; refused")
            else:
                page = (nxt, None)
        merged: dict[str, Any] = {"status": "OK", "results": rows}
        if aggs:
            merged["adjusted"] = False
        return json.dumps(merged, sort_keys=True).encode(), merged

    def _wait_turn(self) -> None:
        now = self._clock()
        if self._next is not None and now < self._next:
            self._sleep(self._next - now)

    def _get(self, url: str, params: Mapping[str, str] | None) -> tuple[bytes, Mapping[str, Any]]:
        headers = self._header()
        for attempt in range(self.max_retries + 1):
            self._wait_turn()
            self._next = self._clock() + self._interval
            retry_after = None
            try:
                response = self._http.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                error = f"Massive request failed: {type(exc).__name__}"
            else:
                status = response.status_code
                if status == 200:
                    return response.content, _body(response.content)
                if status in (401, 403):
                    raise MassiveKeyError(
                        f"Massive answered HTTP {status} ({_status_word(response.content)}): "
                        f"the key is refused or the request is outside your plan (history "
                        f"window, a session not published yet); check {API_KEY_ENV}"
                    )
                if status != 429 and status < 500:
                    raise MassiveError(f"Massive request failed with HTTP {status}")
                error = f"Massive answered HTTP {status}"
                retry_after = _retry_after(response)
            if attempt == self.max_retries:
                raise MassiveError(f"{error}; gave up after {self.max_retries} retries")
            wait = retry_after or self.backoff_s * 2**attempt
            if wait > self.max_wait_s:
                raise MassiveError(
                    f"{error}; asked to wait {wait:g} s, beyond max_wait_s ({self.max_wait_s:g} s)"
                )
            self._next = max(self._next, self._clock() + wait)
        raise AssertionError("unreachable")  # pragma: no cover

    # --- parse ---------------------------------------------------------------------------------

    def parse(self, result: FetchResult) -> pa.Table:
        return self.parse_report(result).table

    def parse_report(self, result: FetchResult) -> ParseReport:
        """Rows and kept-out rows of one fetch; ``ValueError`` only for a payload unusable as a
        whole (not a JSON object, ``status: ERROR``, ``results`` not a list, adjusted prices, a
        grouped fetch not of one session)."""
        try:
            body = _body(result.payload)
        except MassiveError as exc:
            raise ValueError(f"Massive {result.kind} payload: {exc}") from None
        rows = body.get("results")
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise ValueError(f"Massive {result.kind} payload results is not a list")
        if result.kind == "prices":
            _check_unadjusted(body)
            return self._prices(result, rows)
        if result.kind == "actions":
            return self._actions(rows)
        return _symbols(rows)

    def _prices(self, result: FetchResult, rows: list[Any]) -> ParseReport:
        grouped = result.source_symbol == WHOLE_MARKET_SYMBOL
        if grouped and (
            result.requested_from is None or result.requested_from != result.requested_to
        ):
            raise ValueError("a grouped daily fetch must request exactly one session")
        tally = _Tally()
        bars: dict[Key, list[dict[str, Any]]] = {}
        for row in rows:
            try:
                bar = self._bar(result, grouped, row)
            except _KeptOut as out:
                tally.add(out.reason, out.key)
                continue
            bars.setdefault((bar["source_symbol"], bar["valid_date"]), []).append(bar)
        out_rows = []
        for key, same in bars.items():
            if any(b != same[0] for b in same[1:]):
                for _ in same:
                    tally.add(CONFLICTING_DUPLICATE, key)
                continue
            for _ in same[1:]:
                tally.add(DUPLICATE_ROW, key)
            out_rows.append(same[0])
        return tally.report(pa.Table.from_pylist(out_rows, schema=RAW_PRICE_SCHEMA))

    def _bar(self, result: FetchResult, grouped: bool, row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            raise _KeptOut(NOT_AN_OBJECT)
        symbol = row.get("T") if grouped else result.source_symbol
        day = result.requested_from if grouped else None
        if not isinstance(symbol, str) or not symbol:
            raise _KeptOut(MISSING_FIELD, "", day)
        if not grouped:
            stamp = _number(row.get("t"), symbol, None)
            if stamp is None:
                raise _KeptOut(MISSING_FIELD, symbol)
            low, high = TIMESTAMP_RANGE
            if not low.timestamp() * 1000 <= stamp < high.timestamp() * 1000:
                raise _KeptOut(BAD_TIMESTAMP, symbol)
            day = datetime.fromtimestamp(stamp / 1000, UTC).astimezone(self.exchange_tz).date()
        close = _number(row.get("c"), symbol, day)
        if close is None:
            raise _KeptOut(MISSING_FIELD, symbol, day)
        if close <= 0:
            raise _KeptOut(NON_POSITIVE_CLOSE, symbol, day)
        return {
            "source_code": self.source_code,
            "source_symbol": symbol,
            "valid_date": day,
            "open": _number(row.get("o"), symbol, day),
            "high": _number(row.get("h"), symbol, day),
            "low": _number(row.get("l"), symbol, day),
            "close": close,
            "volume": _volume(row.get("v"), symbol, day),
            "currency": CURRENCY,
        }

    def _actions(self, rows: list[Any]) -> ParseReport:
        tally = _Tally()
        objects = []
        for row in rows:
            if isinstance(row, dict):
                objects.append(row)
            else:
                tally.add(NOT_AN_OBJECT, ("", None))
        objects.sort(key=_stable_order)
        seq: dict[tuple[str, date, str], int] = {}
        out = []
        for group in _by_vendor_id(objects):
            row, conflicting = group[0], any(r != group[0] for r in group[1:])
            try:
                action = self._action(row)
                ident = (action["source_symbol"], action["valid_date"], action["action_type"])
            except _KeptOut as kept:
                action, ident = None, kept.ident
                if not conflicting:
                    tally.add(kept.reason, kept.key)
            if ident is not None:  # one vendor action, one slot, kept or not
                seq[ident] = seq.get(ident, -1) + 1
            if conflicting:
                for r in group:
                    tally.add(CONFLICTING_DUPLICATE, _row_key(r))
                continue
            for r in group[1:]:
                tally.add(DUPLICATE_ROW, _row_key(r))
            if action is None or ident is None:
                continue
            if seq[ident] > MAX_SEQ:
                tally.add(SEQ_OVERFLOW, (ident[0], ident[1]))
                continue
            out.append({**action, "action_seq": seq[ident]})
        return tally.report(pa.Table.from_pylist(out, schema=RAW_ACTION_SCHEMA))

    def _action(self, row: dict[str, Any]) -> dict[str, Any]:
        symbol = row.get("ticker")
        if not isinstance(symbol, str) or not symbol:
            raise _KeptOut(MISSING_FIELD)
        is_dividend = row.get("ex_dividend_date") is not None
        is_split = row.get("execution_date") is not None
        if is_dividend and is_split:
            raise _KeptOut(AMBIGUOUS_ACTION, symbol, _date(row["ex_dividend_date"], symbol))
        if not (is_dividend or is_split):
            raise _KeptOut(MISSING_FIELD, symbol)
        day = _date(row["ex_dividend_date" if is_dividend else "execution_date"], symbol)
        if day is None:
            raise _KeptOut(MISSING_FIELD, symbol)
        common = {
            "source_code": self.source_code,
            "source_symbol": symbol,
            "valid_date": day,
            "amount_basis": None,
            "per_share_basis": None,
        }
        if is_dividend:
            raw = row.get("distribution_type")
            action_type = DIVIDEND_TYPES.get(raw) if isinstance(raw, str) else None
            if action_type is None:
                raise _KeptOut(UNKNOWN_DISTRIBUTION_TYPE, symbol, day)
            try:
                amount = _number(row.get("cash_amount"), symbol, day)
            except _KeptOut as kept:
                raise _KeptOut(kept.reason, symbol, day, action_type) from None
            if amount is None:
                raise _KeptOut(NULL_AMOUNT, symbol, day, action_type)
            if amount <= 0:
                raise _KeptOut(NON_POSITIVE_AMOUNT, symbol, day, action_type)
            given = row.get("currency")
            if not isinstance(given, str) or not given.strip():
                raise _KeptOut(MISSING_FIELD, symbol, day, action_type)
            given = given.strip()
            currency = given.upper()
            if (
                _MINOR_UNIT_GIVEN.fullmatch(given)
                or not _CURRENCY.fullmatch(currency)
                or currency in MINOR_UNIT_CURRENCIES
            ):
                raise _KeptOut(BAD_CURRENCY, symbol, day, action_type)
            dates = {}
            for column, name in (
                ("announce_date", "declaration_date"),
                ("record_date", "record_date"),
                ("pay_date", "pay_date"),
            ):
                value = row.get(name)
                try:
                    dates[column] = None if value in (None, "") else _date(value, symbol)
                except _KeptOut:
                    raise _KeptOut(MISSING_FIELD, symbol, day, action_type) from None
            return {
                **common,
                **dates,
                "action_type": action_type,
                "source_action_type": raw,
                "amount": amount,
                "currency": currency,
            }
        raw = row.get("adjustment_type")
        action_type = SPLIT_TYPES.get(raw) if isinstance(raw, str) else None
        if action_type is None:
            raise _KeptOut(UNKNOWN_ADJUSTMENT_TYPE, symbol, day)
        ratios = []
        for name in ("split_from", "split_to"):
            try:
                ratio = _number(row.get(name), symbol, day)
            except _KeptOut as kept:
                reason = NON_FINITE if kept.reason == NON_FINITE else BAD_RATIO
                raise _KeptOut(reason, symbol, day, action_type) from None
            if ratio is None or ratio <= 0:
                raise _KeptOut(BAD_RATIO, symbol, day, action_type)
            ratios.append(ratio)
        return {
            **common,
            "action_type": action_type,
            "source_action_type": raw,
            "ratio_old": ratios[0],
            "ratio_new": ratios[1],
        }


def adapter() -> MassiveAdapter:
    """Entry point ``market_data.adapters:massive``: an adapter keyed from the environment.

    The adapter holds an HTTP client: the caller closes it (``close()``, or ``with``)."""
    return MassiveAdapter.from_env()


# --- helpers -------------------------------------------------------------------------------------


def _origin(url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    return parts.scheme.lower(), parts.netloc.lower()


def _status_word(payload: bytes) -> str:
    """Massive's status word of an answer (``NOT_AUTHORIZED``...), never free text."""
    try:
        word = json.loads(payload).get("status")
    except (ValueError, AttributeError):
        return "no status"
    return word if isinstance(word, str) and _STATUS_WORD.fullmatch(word) else "no status"


def _body(payload: bytes) -> Mapping[str, Any]:
    try:
        body = json.loads(payload)
    except ValueError:
        raise MassiveError("Massive answer is not JSON") from None
    if not isinstance(body, dict):
        raise MassiveError("Massive answer is not a JSON object")
    if body.get("status") == "ERROR":
        raise MassiveError("Massive answered status ERROR with HTTP 200")
    return body


def _check_unadjusted(body: Mapping[str, Any]) -> None:
    if body.get("adjusted") is True:
        raise ValueError("Massive answered adjusted prices; only unadjusted prices are accepted")


def _stable_order(row: dict[str, Any]) -> tuple:
    """Vendor id first (stable across pages and nights), else every field."""
    ident = row.get("id")
    if isinstance(ident, str | int) and not isinstance(ident, bool):
        return (0, str(ident), "")
    return (1, "", json.dumps(row, sort_keys=True, default=str))


def _by_vendor_id(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Rows (already in stable order) grouped by vendor id; a row without an id is alone."""
    groups: list[list[dict[str, Any]]] = []
    by_id: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ident = row.get("id")
        if isinstance(ident, str | int) and not isinstance(ident, bool):
            if str(ident) in by_id:
                by_id[str(ident)].append(row)
                continue
            by_id[str(ident)] = [row]
            groups.append(by_id[str(ident)])
        else:
            groups.append([row])
    return groups


def _row_key(row: dict[str, Any]) -> Key:
    """Best-effort (symbol, ex-date) of an action row, for counting."""
    symbol = row.get("ticker") if isinstance(row.get("ticker"), str) else ""
    for name in ("ex_dividend_date", "execution_date"):
        value = row.get(name)
        if isinstance(value, str) and _ISO_DATE.fullmatch(value):
            try:
                return symbol, date.fromisoformat(value)
            except ValueError:
                break
    return symbol, None


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After", "").strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # an HTTP date or garbage: fall back to exponential backoff
    return value if value > 0 and math.isfinite(value) else None


def _number(value: Any, symbol: str, day: date | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _KeptOut(MISSING_FIELD, symbol, day)
    try:
        number = float(value)  # an integer beyond the float range overflows
    except OverflowError:
        raise _KeptOut(NON_FINITE, symbol, day) from None
    if not math.isfinite(number):
        raise _KeptOut(NON_FINITE, symbol, day)
    return number


def _volume(value: Any, symbol: str, day: date | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _KeptOut(BAD_VOLUME, symbol, day)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _KeptOut(NON_FINITE, symbol, day)
        if not value.is_integer():
            raise _KeptOut(BAD_VOLUME, symbol, day)
    if not 0 <= value <= MAX_VOLUME:
        raise _KeptOut(BAD_VOLUME, symbol, day)
    return int(value)


def _date(value: Any, symbol: str) -> date | None:
    """A ``YYYY-MM-DD`` date; anything else is a ``missing_field``."""
    if not isinstance(value, str) or not _ISO_DATE.fullmatch(value):
        raise _KeptOut(MISSING_FIELD, symbol)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise _KeptOut(MISSING_FIELD, symbol) from None


def _identifier(scheme: str, value: Any) -> str | None:
    """The id value of a scheme, None when absent; ``_KeptOut`` reason only for a bad one."""
    if value is None or value == "":
        return None
    if scheme == "cik":
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            value = str(value)
        if not isinstance(value, str) or not value.strip().isdigit():
            raise _KeptOut(BAD_IDENTIFIER)
        value = value.strip()
        if len(value) > _CIK_WIDTH:
            raise _KeptOut(BAD_IDENTIFIER)
        return value.zfill(_CIK_WIDTH)
    if not isinstance(value, str) or not value.strip():
        raise _KeptOut(BAD_IDENTIFIER)
    return value.strip()


def _symbols(rows: list[Any]) -> ParseReport:
    tally = _Tally()
    out = []
    for row in rows:
        if not isinstance(row, dict):
            tally.add(NOT_AN_OBJECT, ("", None))
            continue
        symbol = row.get("ticker")
        if not isinstance(symbol, str) or not symbol.strip():
            tally.add(MISSING_FIELD, ("", None))
            continue
        symbol = symbol.strip()  # the same in source_symbol and the ticker id_value
        try:
            listed = row.get("list_date")
            valid_date = None if listed in (None, "") else _date(listed, symbol)
            valid_to = None
            delisted = row.get("delisted_utc")
            if delisted not in (None, ""):
                if not isinstance(delisted, str):
                    raise _KeptOut(MISSING_FIELD, symbol)
                try:
                    stamp = datetime.fromisoformat(delisted.replace("Z", "+00:00"))
                    if stamp.tzinfo is not None:
                        stamp = stamp.astimezone(UTC)
                    valid_to = stamp.date() + timedelta(days=1)
                except (ValueError, OverflowError):
                    raise _KeptOut(MISSING_FIELD, symbol) from None
        except _KeptOut as kept:
            tally.add(kept.reason, kept.key)
            continue
        name = row.get("name") if isinstance(row.get("name"), str) else None
        for scheme in _SYMBOL_SCHEMES:
            try:
                value = _identifier(scheme, row.get(scheme))
            except _KeptOut as kept:
                tally.add(kept.reason, (symbol, valid_date))
                continue
            if value is None:
                continue
            out.append(
                {
                    "source_symbol": symbol,
                    "id_scheme": scheme,
                    "id_value": value,
                    "valid_date": valid_date,
                    "valid_to": valid_to,
                    "name": name,
                }
            )
    return tally.report(pa.Table.from_pylist(out, schema=RAW_SYMBOL_SCHEMA))

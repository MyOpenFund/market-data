"""Tiingo adapter: the default source of the README "Try it" (a user's own free key pulls deep
daily history for a few US companies).

Terms (``sources.py``, terms URL still to verify in the live spike): personal, internal use with
the user's own key; no display or sharing with another person. The source is registered
``restricted`` with ``no_public_display`` and ``no_ml_training``.

Endpoint (documented end-of-day API, ``base_url`` default ``BASE_URL``)
    ``GET /tiingo/daily/{ticker}/prices?startDate=..&endDate=..&resampleFreq=daily&format=json``,
    one request and one FetchResult (kind ``prices``) per symbol and range. The ticker is
    percent-encoded and never rewritten: the caller passes Tiingo's own symbol (``BRK-B`` for a
    class share). ``end`` (default ``start``) is clipped to the last session of ``mic`` in
    ``[start, end]`` whose close is not after ``now``; none left: no request, no FetchResult.
    requested_from = ``start``, requested_to = the clipped end; the answer (a JSON list) is
    archived as Tiingo sent it; an empty list is status ``empty``. Tiingo has no history window
    on the free plan (30+ years): any ``start`` is one request.

Actions ride on the prices payload (ruling B7.4): each end-of-day row carries ``divCash`` and
``splitFactor``, so there is no separate actions request. ``fetch("actions", ...)`` is refused
(it would spend the hourly quota twice for the same bytes); the adapter declares
``actions_fetch_kind = "prices"`` and ``build.build_actions`` reads the archived ``prices``
fetches of such an adapter, parsing each as kind ``actions``. ``parse`` of a FetchResult of kind
``actions`` (the builder's view) and ``parse_actions`` of a prices FetchResult (the in-memory
path) both read the same payload. Kind ``symbols`` is not offered in V0 (the "Try it" needs
none): ``fetch("symbols", ...)`` is refused and ``parse`` refuses that kind.

Failures are logged (as in the Massive adapter): when a request fails (after the bounded
retries), a quota stops it, or the answer is unusable as a whole (not JSON, not a list: Tiingo
answers errors as a JSON object ``{"detail": ...}``), ``fetch`` yields a FetchResult with status
``error``, the full requested range and the payload ``ERROR_PAYLOAD`` (``[]``), then raises
``TiingoError``. Any other exception takes the same path, naming the exception type only (no
``__cause__``/``__context__``, whose text could hold a URL or payload data). Tiingo's free text
(``detail``) is never echoed. An HTTP 404 whose ``detail`` names a ticker not found (ruling 72) is a
complete answer, not a failure: status ``not_found``, payload ``ERROR_PAYLOAD``, and the fetch goes
on with the next symbol (any other 404, e.g. a wrong ``base_url``, is an error). ``fetch`` runs
``parse_report`` and ``parse_actions`` on each payload before yielding it, so nothing is archived
as ``ok`` that ``parse`` would refuse.

Key and limits (spec §8.3: respected, never worked around)
    The key comes from ``TIINGO_API_KEY`` (``from_env``), is held in a ``Secret`` and set on each
    request as it is sent (an ``httpx.Auth`` hook) in the ``Authorization: Token <key>`` header:
    never in a URL (no ``token=`` parameter), a log line, an error message, a repr, the quota
    state or an archived FetchResult. ``fetch`` itself checks the key before returning (no
    request otherwise): missing → ``TiingoKeyError`` saying how to get a free one; characters
    outside printable ASCII → ``TiingoKeyError`` echoing none of them; 401/403 →
    ``TiingoKeyError`` naming the HTTP status only. ``parse`` needs no key.
    Free-plan limits, each a parameter that may be set lower, never higher (no paid plan, D14):
    ``per_hour`` (``FREE_PLAN_PER_HOUR`` = 50) requests, retries included, in any trailing
    3600 s: a rolling-hour budget, so a "Try it" of a few companies runs in seconds and the
    51st request of an hour waits until the oldest of the last 50 is an hour old; two requests
    are always at least ``min_spacing_s`` apart (``DEFAULT_MIN_SPACING_S`` = 1.5 s, never a
    burst); ``per_day`` (``FREE_PLAN_PER_DAY`` = 1000) requests in any trailing 86,400 s;
    ``unique_symbols_per_month`` (``FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH`` = 500) distinct symbols
    in the current UTC month. The budget is kept across runs in a small JSON file
    (``market_data.adapters._quota``): ``state_dir`` (default: ``MOF_MARKET_STATE_DIR``, else the
    platform user cache directory + ``myopenfund/market-data``; None or ``"off"`` disables it) holds
    ``tiingo-quota.json``, the request times of the last 24 hours and the month's symbols only
    (never the key), written atomically and re-read before each request; times more than 60 s ahead
    of now (clock jumps) are dropped, so a wait never exceeds the rolling window. A corrupt or
    unreadable file (or one over 1 MB) is ignored without a warning (the budget starts empty from it
    and the next request rewrites it); a failed write is ignored (best effort). The state is per
    user cache, not per key, and two processes running at the same moment can race (no lock):
    Tiingo's own limits then answer 429. Times come from ``clock`` (the wall clock by default, so
    they survive a restart). A fetch that would exceed the daily or symbol cap is refused before any
    request (``TiingoQuotaError``); a cap reached during a fetch (retries) stops it before the next
    request, without waiting (an ``error`` FetchResult, then ``TiingoQuotaError``). The adapter is
    **silent while it waits** (the hourly budget can hold a request for up to an hour): progress is
    the caller's job (B9). A 429, a 5xx or a transport error is retried: wait ``Retry-After``
    seconds when sent, else ``backoff_s * 2**n`` (n = retries so far); after ``max_retries``
    retries, or when a wait would exceed ``max_wait_s``, ``TiingoError`` (no retry storm; after a
    429 it says the free hourly allocation is probably spent and to retry in about an hour). Any
    other non-200 answer (redirects included, never followed) is a ``TiingoError`` naming the HTTP
    status. Clock, sleep, wall time (``now``), the state directory and the HTTP transport are
    injectable. The adapter holds an HTTP client: ``close()`` it, or use it as a context manager.

Parse (``parse`` / ``parse_report`` / ``parse_actions`` / ``parse_vendor_adjusted``)
    source_symbol = the fetch's symbol; valid_date = the calendar date part of ``date``
    (``YYYY-MM-DD``, optionally followed by ``T`` and a time) taken as given, never shifted
    through a time zone (Tiingo stamps a session at midnight UTC).
    prices → ``RAW_PRICE_SCHEMA``: raw ``open``/``high``/``low``/``close`` (floats; a missing
    open/high/low is null) and raw ``volume`` (int64, missing → null); currency ``USD``. The
    adjusted fields (``adjOpen``/``adjHigh``/``adjLow``/``adjClose``/``adjVolume``) never enter
    ``prices``.
    control series → ``parse_vendor_adjusted`` (``VENDOR_ADJUSTED_SCHEMA``: source_symbol,
    valid_date, adj_close) for the B8 vendor-divergence check only; never written into
    ``prices``. ``vendor_adjusted_report`` gives its kept-out rows.
    actions → ``RAW_ACTION_SCHEMA``: ``divCash > 0`` → ``cash_dividend_regular``
    (``source_action_type`` ``divCash``), amount = divCash as given, currency ``USD``, announce,
    record and pay dates null. Tiingo does not tell a special dividend from a regular one: every
    divCash is ``cash_dividend_regular``. ``splitFactor != 1`` → ``split``
    (``source_action_type`` ``splitFactor``) with ratio_old = 1.0 and ratio_new = splitFactor.
    Tiingo's splitFactor is a **price factor** that may include distributions: it is stored as
    given and never applied to volume anywhere in this repo (raw volume stays raw). A null or
    absent divCash/splitFactor, divCash 0 and splitFactor 1 are no action. At most one action of
    each type per (symbol, date), so action_seq is 0.
    Kept out, never blocking (same vocabulary as Massive): ``not_an_object``; ``missing_field``
    (``date`` absent or not a string, ``close`` absent, a price or divCash of the wrong type,
    ``adjClose`` absent for the control series); ``bad_timestamp`` (a ``date`` that is not an ISO
    date, or outside ``DATE_RANGE``); ``non_finite``; ``non_positive_close`` (close, or adjClose
    for the control series); ``bad_volume`` (not an integer in ``0..MAX_VOLUME``);
    ``non_positive_amount`` (divCash < 0); ``bad_ratio`` (a splitFactor not a finite positive
    number); ``duplicate_row`` (an exact repeat of a (symbol, date) value, kept once) and
    ``conflicting_duplicate`` (one (symbol, date) with different values: all out). A row is
    counted in the actions report only when it carries an action (a plain row with a bad date is
    counted by the prices report). ``kept_out``: reason -> sorted tuple of distinct
    ``(source_symbol, valid_date)`` keys (None when the date is unusable), ``counts()`` and
    ``row_counts`` as in Massive. Only a payload unusable as a whole raises ``ValueError``.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pyarrow as pa
from open_data_store.market.asof import sessions
from open_data_store.sources import SourceInfo

from market_data.adapters._quota import (
    STATE_DIR_ENV,
    STATE_OFF,
    QuotaState,
    default_state_dir,
)
from market_data.adapters.base import (
    FETCH_KINDS,
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    WHOLE_MARKET_SYMBOL,
    FetchResult,
    Secret,
)
from market_data.sources import sources

__all__ = [
    "ACTIONS_FETCH_KIND",
    "API_KEY_ENV",
    "BAD_RATIO",
    "BAD_TIMESTAMP",
    "BAD_VOLUME",
    "BASE_URL",
    "CONFLICTING_DUPLICATE",
    "DATE_RANGE",
    "DEFAULT_BACKOFF_S",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_MAX_WAIT_S",
    "DEFAULT_MIN_SPACING_S",
    "DEFAULT_TIMEOUT_S",
    "DUPLICATE_ROW",
    "ERROR_PAYLOAD",
    "FREE_PLAN_PER_DAY",
    "FREE_PLAN_PER_HOUR",
    "FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH",
    "KEPT_OUT_REASONS",
    "MAX_VOLUME",
    "MISSING_FIELD",
    "NON_FINITE",
    "NON_POSITIVE_AMOUNT",
    "NON_POSITIVE_CLOSE",
    "NOT_AN_OBJECT",
    "SOURCE_CODE",
    "STATE_DIR_ENV",
    "VENDOR_ADJUSTED_SCHEMA",
    "ParseReport",
    "TiingoAdapter",
    "TiingoError",
    "TiingoKeyError",
    "TiingoQuotaError",
    "adapter",
]

SOURCE_CODE = "tiingo"
BASE_URL = "https://api.tiingo.com"
API_KEY_ENV = "TIINGO_API_KEY"
FREE_PLAN_PER_HOUR = 50
FREE_PLAN_PER_DAY = 1000
FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH = 500
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_S = 15.0
DEFAULT_MAX_WAIT_S = 300.0
DEFAULT_TIMEOUT_S = 30.0
DEFAULT_MIN_SPACING_S = 1.5  # between two requests, inside the hourly budget
DEFAULT_MIC = "XNYS"
ACTIONS_FETCH_KIND = "prices"  # the archived kind whose payload also holds the actions
RESAMPLE_FREQ = "daily"
CURRENCY = "USD"
CONTENT_TYPE = "application/json"
ERROR_PAYLOAD = b"[]"
MAX_VOLUME = 2**63 - 1  # int64, the volume column
# Dates outside this range are not dates of a daily bar (``bad_timestamp``).
DATE_RANGE = (date(1900, 1, 1), date(2200, 1, 1))
_HOUR_S = 3600.0
_DAY_S = 86400.0
_KEY_CHARS = re.compile(r"[\x21-\x7e]+")  # visible ASCII: what an HTTP header can carry
# ``YYYY-MM-DD``, optionally followed by a time: the date part is taken as given
_DATE = re.compile(
    r"(\d{4}-\d{2}-\d{2})(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?)?"
)

NOT_AN_OBJECT = "not_an_object"
MISSING_FIELD = "missing_field"
NON_FINITE = "non_finite"
NON_POSITIVE_CLOSE = "non_positive_close"
BAD_VOLUME = "bad_volume"
BAD_TIMESTAMP = "bad_timestamp"
BAD_RATIO = "bad_ratio"
NON_POSITIVE_AMOUNT = "non_positive_amount"
DUPLICATE_ROW = "duplicate_row"
CONFLICTING_DUPLICATE = "conflicting_duplicate"
KEPT_OUT_REASONS = (
    NOT_AN_OBJECT,
    MISSING_FIELD,
    NON_FINITE,
    NON_POSITIVE_CLOSE,
    BAD_VOLUME,
    BAD_TIMESTAMP,
    BAD_RATIO,
    NON_POSITIVE_AMOUNT,
    DUPLICATE_ROW,
    CONFLICTING_DUPLICATE,
)

# The vendor's adjusted close, a control series for quality.vendor_divergence; never prices.
VENDOR_ADJUSTED_SCHEMA = pa.schema(
    [
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("valid_date", pa.date32(), nullable=False),
        pa.field("adj_close", pa.float64(), nullable=False),
    ]
)

_GET_A_KEY = (
    f"set {API_KEY_ENV} to your own Tiingo API token: create a free account at "
    "https://www.tiingo.com and copy the token from your account's API page"
)

Key = tuple[str, date | None]


class TiingoError(RuntimeError):
    """A Tiingo request or answer that cannot be used. Never carries the key."""


class TiingoKeyError(TiingoError):
    """No usable key, or a key Tiingo refuses."""


class TiingoQuotaError(TiingoError):
    """A request beyond this adapter's daily or unique-symbol cap; nothing was sent."""


class _FromEnv:
    """``state_dir`` default: ``MOF_MARKET_STATE_DIR``, else the user cache directory."""

    def __repr__(self) -> str:
        return "<state dir from the environment>"


_STATE_FROM_ENV = _FromEnv()


class _TokenAuth(httpx.Auth):
    """Sets ``Authorization: Token <key>`` on each request as it is sent: the clear key is never
    kept in a headers dict or a local of the fetch frames."""

    def __init__(self, key: Secret) -> None:
        self._key = key

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = "Token " + self._key.reveal()
        yield request

    def __repr__(self) -> str:
        return "_TokenAuth(Secret(***))"


class _NotFound(Exception):
    """Tiingo knows no such ticker (internal control flow, never escapes)."""


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

    def __init__(self, reason: str, symbol: str, day: date | None = None) -> None:
        super().__init__(reason)
        self.reason, self.key = reason, (symbol, day)


class _Tally:
    def __init__(self) -> None:
        self.keys: dict[str, set[Key]] = {r: set() for r in KEPT_OUT_REASONS}
        self.rows: dict[str, int] = dict.fromkeys(KEPT_OUT_REASONS, 0)

    def add(self, reason: str, key: Key) -> None:
        self.keys[reason].add(key)
        self.rows[reason] += 1

    def dedupe(self, items: list[tuple[tuple, dict[str, Any]]]) -> list[dict[str, Any]]:
        """Records grouped by identity (first two parts: the kept-out key): an exact repeat is
        kept once (``duplicate_row``), different values are all kept out
        (``conflicting_duplicate``)."""
        groups: dict[tuple, list[dict[str, Any]]] = {}
        for ident, record in items:
            groups.setdefault(ident, []).append(record)
        out = []
        for ident, same in groups.items():
            key = (ident[0], ident[1])
            if any(r != same[0] for r in same[1:]):
                for _ in same:
                    self.add(CONFLICTING_DUPLICATE, key)
                continue
            for _ in same[1:]:
                self.add(DUPLICATE_ROW, key)
            out.append(same[0])
        return out

    def report(self, table: pa.Table) -> ParseReport:
        def order(k: Key) -> tuple:
            return (k[0], k[1] is not None, k[1] or date.min)

        kept = {r: tuple(sorted(keys, key=order)) for r, keys in self.keys.items()}
        return ParseReport(table, kept, dict(self.rows))


def _info() -> SourceInfo:
    return next(s for s in sources() if s.code == SOURCE_CODE)


def _check_limit(name: str, value: int, ceiling: int) -> None:
    if not 0 < value <= ceiling:
        raise ValueError(f"{name} must be in 1..{ceiling} (the free plan), got {value}")


class TiingoAdapter:
    source_code = SOURCE_CODE
    # build.build_actions reads the actions from the archived fetches of this kind
    actions_fetch_kind = ACTIONS_FETCH_KIND

    def __init__(
        self,
        api_key: str | Secret | None,
        *,
        per_hour: int = FREE_PLAN_PER_HOUR,
        per_day: int = FREE_PLAN_PER_DAY,
        unique_symbols_per_month: int = FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH,
        min_spacing_s: float = DEFAULT_MIN_SPACING_S,
        state_dir: Path | str | None | _FromEnv = _STATE_FROM_ENV,
        base_url: str = BASE_URL,
        mic: str = DEFAULT_MIC,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_s: float = DEFAULT_BACKOFF_S,
        max_wait_s: float = DEFAULT_MAX_WAIT_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        raw = api_key.reveal() if isinstance(api_key, Secret) else (api_key or "")
        self._key = Secret(raw.strip()) if raw.strip() else None
        _check_limit("per_hour", per_hour, FREE_PLAN_PER_HOUR)
        _check_limit("per_day", per_day, FREE_PLAN_PER_DAY)
        _check_limit(
            "unique_symbols_per_month", unique_symbols_per_month, FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH
        )
        if max_retries < 0 or backoff_s <= 0 or max_wait_s <= 0:
            raise ValueError("max_retries must be >= 0, backoff_s and max_wait_s > 0")
        if not (math.isfinite(min_spacing_s) and min_spacing_s >= 0):
            raise ValueError(f"min_spacing_s must be >= 0, got {min_spacing_s}")
        if isinstance(state_dir, _FromEnv):
            directory = default_state_dir()
        elif state_dir is None or (isinstance(state_dir, str) and state_dir.lower() == STATE_OFF):
            directory = None
        else:
            directory = Path(state_dir)
        self.info = _info()
        self.per_hour = per_hour
        self.per_day = per_day
        self.unique_symbols_per_month = unique_symbols_per_month
        self.base_url = base_url.rstrip("/")
        self.mic = mic
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.max_wait_s = max_wait_s
        self.min_spacing_s = min_spacing_s
        self._clock = clock
        self._sleep = sleep
        self._now = now
        self._next: float | None = None  # earliest clock time of a retry
        # request times of the last 24 hours and the month's symbols, shared across runs
        self._quota = QuotaState(SOURCE_CODE, directory)
        self._auth = None if self._key is None else _TokenAuth(self._key)
        self._http = httpx.Client(transport=transport, timeout=timeout_s)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **kw: Any) -> TiingoAdapter:
        """An adapter keyed from ``TIINGO_API_KEY`` (unset: ``fetch`` raises, ``parse`` works)."""
        raw = (os.environ if env is None else env).get(API_KEY_ENV, "")
        return cls(Secret(raw.strip()) if raw.strip() else None, **kw)

    def __repr__(self) -> str:
        key = "Secret(***)" if self._key else "None"
        return f"TiingoAdapter(api_key={key}, per_hour={self.per_hour}, per_day={self.per_day})"

    __str__ = __repr__

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> TiingoAdapter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- fetch ---------------------------------------------------------------------------------

    def fetch(
        self, kind: str, symbols: Sequence[str], start: date | None, end: date | None
    ) -> Iterator[FetchResult]:
        """Prices FetchResults, one per symbol (see the module docstring). Arguments, the key and
        the caps are checked before this returns; requests are made as the iterator is
        consumed."""
        if kind not in FETCH_KINDS:
            raise ValueError(f"fetch kind must be one of {FETCH_KINDS}, got {kind!r}")
        if kind == "actions":
            raise ValueError(
                "Tiingo actions come in the prices payload: fetch kind 'prices'; "
                "build_actions reads those fetches (TiingoAdapter.actions_fetch_kind)"
            )
        if kind == "symbols":
            raise ValueError("Tiingo symbols are not fetched in V0: pass Tiingo's own tickers")
        names: list[str] = []
        for name in symbols:
            if not isinstance(name, str) or not name:
                raise ValueError("every Tiingo symbol must be a non-empty string")
            if name not in names:
                names.append(name)
        if not names or WHOLE_MARKET_SYMBOL in names:
            raise ValueError("Tiingo prices are fetched per symbol: pass the tickers")
        if start is None:
            raise ValueError("Tiingo prices need a start date")
        end = start if end is None else end
        if start > end:
            raise ValueError(f"start {start.isoformat()} is after end {end.isoformat()}")
        self._check_key()
        self._refresh_quota()
        spent = self._quota.symbols
        new = len({n for n in names if n not in spent})
        if len(spent) + new > self.unique_symbols_per_month:
            raise TiingoQuotaError(
                f"{new} new symbol(s) would exceed unique_symbols_per_month "
                f"({self.unique_symbols_per_month}): {len(spent)} already requested this "
                "month; nothing was sent"
            )
        left = self.per_day - self._sent_today()
        if len(names) > left:
            raise TiingoQuotaError(
                f"{len(names)} request(s) would exceed per_day ({self.per_day}): "
                f"{left} left in the last 24 hours; nothing was sent"
            )
        return self._fetch_prices(names, start, end)

    def _check_key(self) -> None:
        if not self._key:
            raise TiingoKeyError(f"no Tiingo API key: {_GET_A_KEY}")
        if not _KEY_CHARS.fullmatch(self._key.reveal()):
            raise TiingoKeyError(
                f"the Tiingo API key in {API_KEY_ENV} has characters an HTTP header cannot "
                "carry (printable ASCII only); check it was copied whole"
            )

    def _last_closed_session(self, start: date, end: date) -> date | None:
        """The last session of ``mic`` in ``[start, end]`` whose close is not after now."""
        now = self._now()
        cal = sessions(self.mic, start, end)
        closed = [
            d
            for d, close in zip(
                cal["session_date"].to_pylist(), cal["close_utc"].to_pylist(), strict=True
            )
            if close <= now
        ]
        return closed[-1] if closed else None

    def _fetch_prices(self, names: list[str], start: date, end: date) -> Iterator[FetchResult]:
        last = self._last_closed_session(start, end)
        if last is None:
            return
        params = {
            "startDate": start.isoformat(),
            "endDate": last.isoformat(),
            "resampleFreq": RESAMPLE_FREQ,
            "format": "json",
        }
        for name in names:
            url = f"{self.base_url}/tiingo/daily/{quote(name, safe='')}/prices"
            yield from self._emit(name, start, last, lambda u=url, n=name: self._get(u, params, n))

    def _emit(
        self, symbol: str, start: date, end: date, request: Callable[[], bytes]
    ) -> Iterator[FetchResult]:
        """One FetchResult from ``request``; on failure an ``error`` FetchResult, then raise."""
        failure: TiingoError | None = None
        try:
            payload = request()
            rows = _payload_rows(payload)
            result = self._result(symbol, start, end, "ok" if rows else "empty", payload)
            self.parse_report(result)
            self.parse_actions(result)
        except _NotFound:
            yield self._result(symbol, start, end, "not_found", ERROR_PAYLOAD)
            return
        except TiingoError as exc:
            failure = exc
        except ValueError as exc:  # the payload is unusable as a whole
            failure = TiingoError(f"Tiingo prices answer refused: {exc}")
        except Exception as exc:  # noqa: BLE001 - any failure is logged as an error fetch
            # the type only: an unexpected exception's text may hold a URL or payload data
            failure = TiingoError(f"Tiingo prices fetch failed: {type(exc).__name__}")
        if failure is None:
            yield result
            return
        yield self._result(symbol, start, end, "error", ERROR_PAYLOAD)
        failure.__context__ = failure.__cause__ = None
        raise failure

    def _result(
        self, symbol: str, start: date, end: date, status: str, payload: bytes
    ) -> FetchResult:
        return FetchResult(
            source_code=self.source_code,
            source_symbol=symbol,
            kind="prices",
            requested_from=start,
            requested_to=end,
            fetched_at=self._now(),
            status=status,
            payload=payload,
            content_type=CONTENT_TYPE,
        )

    def _month(self) -> str:
        return self._now().astimezone(UTC).strftime("%Y-%m")

    def _refresh_quota(self) -> None:
        self._quota.refresh(self._clock(), self._month())

    def _sent_today(self) -> int:
        """Requests of the last 24 hours (this run and earlier ones sharing the state)."""
        now = self._clock()
        return len(self._quota.since(now - _DAY_S, now))

    def _check_caps(self, symbol: str) -> None:
        self._refresh_quota()
        if self._sent_today() >= self.per_day:
            raise TiingoQuotaError(
                f"per_day ({self.per_day}) reached in the last 24 hours; no request sent"
            )
        spent = self._quota.symbols
        if symbol not in spent and len(spent) >= self.unique_symbols_per_month:
            raise TiingoQuotaError(
                f"unique_symbols_per_month ({self.unique_symbols_per_month}) reached this month; "
                "no request sent"
            )

    def _wait_turn(self) -> None:
        """Sleep until the hourly budget, the minimum spacing and any retry wait allow a
        request (silently: the caller shows progress)."""
        now = self._clock()
        ready = now if self._next is None else max(now, self._next)
        recent = self._quota.since(now - _HOUR_S, now)  # times ahead count as now
        if recent:
            ready = max(ready, recent[-1] + self.min_spacing_s)
        if len(recent) >= self.per_hour:
            ready = max(ready, recent[-self.per_hour] + _HOUR_S)
        if ready > now:
            self._sleep(ready - now)

    def _get(self, url: str, params: Mapping[str, str], symbol: str) -> bytes:
        self._check_key()
        for attempt in range(self.max_retries + 1):
            self._check_caps(symbol)  # before waiting: a refused request never waits
            self._wait_turn()
            self._quota.record(self._clock(), symbol, self._month())
            retry_after = None
            spent = ""
            try:
                response = self._http.get(url, params=params, auth=self._auth)
            except httpx.HTTPError as exc:
                error = f"Tiingo request failed: {type(exc).__name__}"
            else:
                status = response.status_code
                if status == 200:
                    return response.content
                if status in (401, 403):
                    raise TiingoKeyError(
                        f"Tiingo answered HTTP {status}: the key is refused or the request is "
                        f"outside your plan; check {API_KEY_ENV}"
                    )
                if status == 404 and _says_not_found(response.content):
                    raise _NotFound
                if status != 429 and status < 500:
                    raise TiingoError(f"Tiingo request failed with HTTP {status}")
                error = f"Tiingo answered HTTP {status}"
                retry_after = _retry_after(response)
                if status == 429:
                    spent = "; the free hourly allocation is probably spent: retry in about an hour"
            if attempt == self.max_retries:
                raise TiingoError(f"{error}; gave up after {self.max_retries} retries{spent}")
            wait = retry_after or self.backoff_s * 2**attempt
            if wait > self.max_wait_s:
                raise TiingoError(
                    f"{error}; asked to wait {wait:g} s, beyond max_wait_s "
                    f"({self.max_wait_s:g} s){spent}"
                )
            self._next = self._clock() + wait
        raise AssertionError("unreachable")  # pragma: no cover

    # --- parse ---------------------------------------------------------------------------------

    def parse(self, result: FetchResult) -> pa.Table:
        return self.parse_report(result).table

    def parse_report(self, result: FetchResult) -> ParseReport:
        """Rows and kept-out rows of one fetch, by ``result.kind`` (``prices``, or ``actions``
        for the builder's view of a prices payload); ``ValueError`` only for a payload unusable
        as a whole."""
        if result.kind == "prices":
            symbol, rows = _symbol_rows(result)
            return self._prices(symbol, rows)
        if result.kind == "actions":
            return self.parse_actions(result)
        raise ValueError("Tiingo symbols are not parsed in V0")

    def parse_actions(self, result: FetchResult) -> ParseReport:
        """Actions (``divCash``, ``splitFactor``) of a prices payload, whatever ``result.kind``
        says (``prices`` in memory, ``actions`` through the builder)."""
        if result.kind == "symbols":
            raise ValueError("Tiingo symbols are not parsed in V0")
        symbol, rows = _symbol_rows(result)
        return self._actions(symbol, rows)

    def parse_vendor_adjusted(self, result: FetchResult) -> pa.Table:
        """The vendor's adjusted close (``VENDOR_ADJUSTED_SCHEMA``), for the vendor-divergence
        check only: never written into ``prices``."""
        return self.vendor_adjusted_report(result).table

    def vendor_adjusted_report(self, result: FetchResult) -> ParseReport:
        if result.kind != "prices":
            raise ValueError("the vendor-adjusted series is parsed from a prices fetch only")
        symbol, rows = _symbol_rows(result)
        tally = _Tally()
        items = []
        for row in rows:
            try:
                day = _row_date(row, symbol)
                value = _number(row.get("adjClose"), symbol, day)
                if value is None:
                    raise _KeptOut(MISSING_FIELD, symbol, day)
                if value <= 0:
                    raise _KeptOut(NON_POSITIVE_CLOSE, symbol, day)
            except _KeptOut as out:
                tally.add(out.reason, out.key)
                continue
            items.append(
                ((symbol, day), {"source_symbol": symbol, "valid_date": day, "adj_close": value})
            )
        out_rows = tally.dedupe(items)
        return tally.report(pa.Table.from_pylist(out_rows, schema=VENDOR_ADJUSTED_SCHEMA))

    def _prices(self, symbol: str, rows: list[Any]) -> ParseReport:
        tally = _Tally()
        items = []
        for row in rows:
            try:
                bar = self._bar(symbol, row)
            except _KeptOut as out:
                tally.add(out.reason, out.key)
                continue
            items.append(((symbol, bar["valid_date"]), bar))
        out_rows = tally.dedupe(items)
        return tally.report(pa.Table.from_pylist(out_rows, schema=RAW_PRICE_SCHEMA))

    def _bar(self, symbol: str, row: Any) -> dict[str, Any]:
        day = _row_date(row, symbol)
        close = _number(row.get("close"), symbol, day)
        if close is None:
            raise _KeptOut(MISSING_FIELD, symbol, day)
        if close <= 0:
            raise _KeptOut(NON_POSITIVE_CLOSE, symbol, day)
        return {
            "source_code": self.source_code,
            "source_symbol": symbol,
            "valid_date": day,
            "open": _number(row.get("open"), symbol, day),
            "high": _number(row.get("high"), symbol, day),
            "low": _number(row.get("low"), symbol, day),
            "close": close,
            "volume": _volume(row.get("volume"), symbol, day),
            "currency": CURRENCY,
        }

    def _actions(self, symbol: str, rows: list[Any]) -> ParseReport:
        tally = _Tally()
        items = []
        for row in rows:
            if not isinstance(row, dict):
                tally.add(NOT_AN_OBJECT, (symbol, None))
                continue
            if _neutral(row.get("divCash"), 0) and _neutral(row.get("splitFactor"), 1):
                continue  # no action on this row
            try:
                day = _row_date(row, symbol)
            except _KeptOut as out:
                tally.add(out.reason, out.key)
                continue
            for parse in (_dividend, _split):
                try:
                    action = parse(row, symbol, day)
                except _KeptOut as out:
                    tally.add(out.reason, out.key)
                    continue
                if action is None:
                    continue
                action.update(
                    source_code=self.source_code,
                    source_symbol=symbol,
                    valid_date=day,
                    action_seq=0,
                    amount_basis=None,
                    per_share_basis=None,
                )
                items.append(((symbol, day, action["action_type"]), action))
        out_rows = tally.dedupe(items)
        return tally.report(pa.Table.from_pylist(out_rows, schema=RAW_ACTION_SCHEMA))


def adapter() -> TiingoAdapter:
    """Entry point ``market_data.adapters:tiingo``: an adapter keyed from the environment.

    The adapter holds an HTTP client: the caller closes it (``close()``, or ``with``)."""
    return TiingoAdapter.from_env()


# --- helpers -------------------------------------------------------------------------------------


def _payload_rows(payload: bytes) -> list[Any]:
    """The rows of a Tiingo end-of-day answer: a JSON list (an error is a JSON object)."""
    try:
        body = json.loads(payload)
    except ValueError:
        raise TiingoError("Tiingo answer is not JSON") from None
    if not isinstance(body, list):
        raise TiingoError("Tiingo answer is not a JSON list (an error object?)")
    return body


def _symbol_rows(result: FetchResult) -> tuple[str, list[Any]]:
    if result.source_symbol == WHOLE_MARKET_SYMBOL or not result.source_symbol:
        raise ValueError("a Tiingo fetch is of one symbol, never the whole market")
    try:
        return result.source_symbol, _payload_rows(result.payload)
    except TiingoError as exc:
        raise ValueError(f"Tiingo {result.kind} payload: {exc}") from None


def _says_not_found(payload: bytes) -> bool:
    """Tiingo's 404 for an unknown ticker: ``{"detail": "Error: Ticker 'X' not found"}``
    (both "ticker" and "not found" required)."""
    try:
        detail = json.loads(payload).get("detail")
    except (ValueError, AttributeError):
        return False
    if not isinstance(detail, str):
        return False
    detail = detail.lower()
    return "not found" in detail and "ticker" in detail


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After", "").strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # an HTTP date or garbage: fall back to exponential backoff
    return value if value > 0 and math.isfinite(value) else None


def _row_date(row: Any, symbol: str) -> date:
    """The calendar date part of ``date``, as given."""
    if not isinstance(row, dict):
        raise _KeptOut(NOT_AN_OBJECT, symbol)
    value = row.get("date")
    if not isinstance(value, str):
        raise _KeptOut(MISSING_FIELD, symbol)
    match = _DATE.fullmatch(value)
    if match is None:
        raise _KeptOut(BAD_TIMESTAMP, symbol)
    try:
        day = date.fromisoformat(match.group(1))
    except ValueError:
        raise _KeptOut(BAD_TIMESTAMP, symbol) from None
    if not DATE_RANGE[0] <= day < DATE_RANGE[1]:
        raise _KeptOut(BAD_TIMESTAMP, symbol)
    return day


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


def _neutral(value: Any, neutral: int) -> bool:
    """Absent, null, or the number meaning "no action" (a bool is never a number)."""
    if value is None:
        return True
    return isinstance(value, int | float) and not isinstance(value, bool) and value == neutral


def _dividend(row: dict[str, Any], symbol: str, day: date) -> dict[str, Any] | None:
    amount = _number(row.get("divCash"), symbol, day)
    if amount is None or amount == 0:
        return None
    if amount < 0:
        raise _KeptOut(NON_POSITIVE_AMOUNT, symbol, day)
    return {
        "action_type": "cash_dividend_regular",  # Tiingo does not flag special dividends
        "source_action_type": "divCash",
        "amount": amount,
        "currency": CURRENCY,
    }


def _split(row: dict[str, Any], symbol: str, day: date) -> dict[str, Any] | None:
    raw = row.get("splitFactor")
    if raw is None:
        return None
    try:
        factor = _number(raw, symbol, day)
    except _KeptOut:
        raise _KeptOut(BAD_RATIO, symbol, day) from None
    assert factor is not None
    if factor <= 0:
        raise _KeptOut(BAD_RATIO, symbol, day)
    if factor == 1:
        return None
    # a price factor (it may include distributions): stored as given, never applied to volume
    return {
        "action_type": "split",
        "source_action_type": "splitFactor",
        "ratio_old": 1.0,
        "ratio_new": factor,
    }

"""Massive (formerly Polygon.io) adapter: our free daily US source (Stocks Basic plan).

Terms (https://massive.com/legal/market-data-terms-of-service): personal, non-commercial use
with the user's own key; no redistribution or public display (§5(c)), no AI/ML training (§5(g)).
The source is registered ``restricted`` with ``no_public_display`` and ``no_ml_training``.

Endpoints (documented, verified on the free plan 2026-11-25), all under ``base_url``
(default ``BASE_URL``):

``prices``, whole market (``symbols`` empty or ``["*"]``)
    ``GET /v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=false&include_otc=false``,
    one call per session of ``mic`` in ``[start, end]`` (weekends and holidays are never
    requested), one FetchResult per session: ``source_symbol="*"``, requested_from =
    requested_to = the session; no result rows (a session not published yet, an unscheduled
    closure) gives status ``empty``.
``prices``, per symbol (backfill inside the plan's history window)
    ``GET /v2/aggs/ticker/{T}/range/1/day/{start}/{end}?adjusted=false&sort=asc&limit=50000``,
    one FetchResult per page per symbol.
``actions``, whole market
    ``GET /stocks/v1/dividends`` (``ex_dividend_date`` in ``[start, end]``) then
    ``GET /stocks/v1/splits`` (``execution_date`` in ``[start, end]``), ``limit=5000``.
``symbols``, whole market
    ``GET /v3/reference/tickers?market=stocks&active=true`` then ``active=false``, ``limit=1000``.

Every list follows ``next_url`` exactly as Massive gives it (one archived FetchResult per page);
a ``next_url`` on another scheme or host than ``base_url`` is refused, so the key never leaves
for another server. Prices are always requested unadjusted; an answer flagged ``adjusted: true``
is refused (never archived, never parsed).

Key and quota (spec §8.3: respected, never worked around)
    The key comes from ``MASSIVE_API_KEY`` (``from_env``), is held in a ``Secret`` and sent only
    in the ``Authorization: Bearer`` header: never in a URL, a log line, an error message, a
    repr or an archived FetchResult. Without a key, ``fetch`` raises ``MassiveKeyError`` saying
    how to get a free one, before any request; ``parse`` needs no key (builds from the archive
    work offline). The free plan allows ``FREE_PLAN_RPM`` (5) requests a minute: ``rpm`` may be
    set lower, never higher; requests are spaced evenly (one every ``60 / rpm`` seconds). A 429
    waits ``Retry-After`` seconds when sent, else ``backoff_s * 2**n`` (n = retries so far),
    never less than the spacing; after ``max_retries`` retries, or when a wait would exceed
    ``max_wait_s``, ``MassiveError`` (no retry storm). 401/403 are ``MassiveKeyError``; any other
    non-200 answer, a non-JSON body or a body whose ``status`` is ``ERROR`` is a ``MassiveError``
    naming the HTTP status only. ``max_history_days`` (default 730, the free plan's 2 years;
    None disables) refuses a ``start`` further back before any request, so no quota is spent on
    calls the plan would refuse. Clock, sleep, wall time and the HTTP transport are injectable.

Parse (``parse`` / ``parse_report``)
    prices → ``RAW_PRICE_SCHEMA``: ``T`` (per-symbol fetch: the fetch's symbol) → source_symbol;
    valid_date = the requested session for a grouped fetch, else the bar timestamp ``t`` (ms UTC)
    as a calendar date in ``exchange_tz``; o/h/l/c as floats; ``v`` → int64 (a non-integral or
    negative volume is a ValueError naming the symbol and date; missing → null); a bar without
    ``c`` is a ValueError; currency ``USD``.
    dividends → ``RAW_ACTION_SCHEMA``: ``ex_dividend_date`` → valid_date, ``declaration_date`` →
    announce_date, ``record_date``, ``pay_date``, raw ``cash_amount`` (never
    ``split_adjusted_cash_amount``) → amount, ``currency``; ``distribution_type`` ``recurring`` →
    ``cash_dividend_regular``, ``special``/``supplemental``/``irregular`` →
    ``cash_dividend_special``; any other or missing type is kept out and counted
    (``UNKNOWN_DISTRIBUTION_TYPE``). ``source_action_type`` = the raw type.
    splits → ``execution_date`` → valid_date, ``split_from`` → ratio_old, ``split_to`` →
    ratio_new; ``adjustment_type`` ``forward_split``/``reverse_split`` → ``split``,
    ``stock_dividend`` → ``stock_dividend``, any other kept out and counted
    (``UNKNOWN_ADJUSTMENT_TYPE``). A row is a dividend when it has ``ex_dividend_date``, a split
    when it has ``execution_date``; a row with neither, or without ``ticker``, is a ValueError.
    action_seq = order within the payload per (symbol, ex-date, action_type), from 0.
    symbols → ``RAW_SYMBOL_SCHEMA``: per ticker one row per present scheme among ``ticker``,
    ``composite_figi``, ``share_class_figi``, ``cik`` (zero-padded to 10 digits), with ``name``;
    valid_date = ``list_date`` when present, else null; valid_to = the UTC calendar date of
    ``delisted_utc`` + 1 day when present, else null.
    Kept-out rows: ``parse_report`` returns them as reason -> sorted tuple of distinct
    ``(source_symbol, valid_date)`` keys, the builder's drop-count shape (``counts()`` likewise).
"""

from __future__ import annotations

import json
import math
import os
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
    "API_KEY_ENV",
    "BASE_URL",
    "DEFAULT_BACKOFF_S",
    "DEFAULT_MAX_HISTORY_DAYS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_MAX_WAIT_S",
    "DEFAULT_TIMEOUT_S",
    "DIVIDEND_TYPES",
    "FREE_PLAN_RPM",
    "KEPT_OUT_REASONS",
    "LIST_LIMIT",
    "SOURCE_CODE",
    "SPLIT_TYPES",
    "TICKERS_LIMIT",
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
DEFAULT_MAX_HISTORY_DAYS = 730  # the free plan's 2-year window
AGGS_LIMIT = 50000  # documented maximum of the aggregates endpoint
LIST_LIMIT = 5000  # documented maximum of /stocks/v1/dividends and /stocks/v1/splits
TICKERS_LIMIT = 1000  # documented maximum of /v3/reference/tickers
DEFAULT_MIC = "XNYS"
DEFAULT_EXCHANGE_TZ = "America/New_York"
CURRENCY = "USD"
CONTENT_TYPE = "application/json"
_MINUTE_S = 60.0
_CIK_WIDTH = 10

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
UNKNOWN_DISTRIBUTION_TYPE = "unknown_distribution_type"
UNKNOWN_ADJUSTMENT_TYPE = "unknown_adjustment_type"
KEPT_OUT_REASONS = (UNKNOWN_DISTRIBUTION_TYPE, UNKNOWN_ADJUSTMENT_TYPE)
_SYMBOL_SCHEMES = ("ticker", "composite_figi", "share_class_figi", "cik")

_GET_A_KEY = (
    f"set {API_KEY_ENV} to your own Massive API key: create a free account at "
    "https://massive.com (Stocks Basic plan) and copy the key from the dashboard"
)

Key = tuple[str, date]


class MassiveError(RuntimeError):
    """A Massive request or answer that cannot be used. Never carries the key."""


class MassiveKeyError(MassiveError):
    """No key, or a key Massive refuses."""


@dataclass(frozen=True)
class ParseReport:
    """Rows parsed from one fetch and the rows kept out (reason -> distinct keys)."""

    table: pa.Table
    kept_out: Mapping[str, tuple[Key, ...]] = field(
        default_factory=lambda: {r: () for r in KEPT_OUT_REASONS}
    )

    def counts(self) -> dict[str, int]:
        """Reason -> number of distinct (source_symbol, valid_date) keys kept out."""
        return {reason: len(keys) for reason, keys in self.kept_out.items()}


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
        backoff_s: float = DEFAULT_BACKOFF_S,
        max_wait_s: float = DEFAULT_MAX_WAIT_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        key = api_key if isinstance(api_key, Secret) else Secret((api_key or "").strip())
        self._key = key if key.reveal().strip() else None
        if not 0 < rpm <= FREE_PLAN_RPM:
            raise ValueError(f"rpm must be in 1..{FREE_PLAN_RPM} (the free plan), got {rpm}")
        if max_retries < 0 or backoff_s <= 0 or max_wait_s <= 0:
            raise ValueError("max_retries must be >= 0, backoff_s and max_wait_s > 0")
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

    # --- fetch ---------------------------------------------------------------------------------

    def fetch(
        self, kind: str, symbols: Sequence[str], start: date | None, end: date | None
    ) -> Iterator[FetchResult]:
        if kind not in FETCH_KINDS:
            raise ValueError(f"fetch kind must be one of {FETCH_KINDS}, got {kind!r}")
        names = list(symbols)
        whole = names in ([], [WHOLE_MARKET_SYMBOL])
        if not whole and WHOLE_MARKET_SYMBOL in names:
            raise ValueError(f"{WHOLE_MARKET_SYMBOL!r} cannot be mixed with symbols")
        if kind != "prices" and not whole:
            raise ValueError(f"Massive {kind} are fetched whole-market: pass no symbols")
        if kind == "symbols":
            return self._fetch_symbols()
        if start is None:
            raise ValueError(f"Massive {kind} need a start date")
        end = start if end is None else end
        if start > end:
            raise ValueError(f"start {start.isoformat()} is after end {end.isoformat()}")
        if self.max_history_days is not None:
            oldest = self._now().astimezone(UTC).date() - timedelta(days=self.max_history_days)
            if start < oldest:
                raise ValueError(
                    f"start {start.isoformat()} is before {oldest.isoformat()}, outside "
                    f"max_history_days ({self.max_history_days}) of the plan"
                )
        if kind == "actions":
            return self._fetch_actions(start, end)
        if whole:
            return self._fetch_grouped(start, end)
        return self._fetch_aggs(names, start, end)

    def _require_key(self) -> Secret:
        if not self._key:
            raise MassiveKeyError(f"no Massive API key: {_GET_A_KEY}")
        return self._key

    def _fetch_grouped(self, start: date, end: date) -> Iterator[FetchResult]:
        self._require_key()
        days = sessions(self.mic, start, end)["session_date"].to_pylist()
        params = {"adjusted": "false", "include_otc": "true" if self.include_otc else "false"}
        for day in days:
            url = f"{self.base_url}/v2/aggs/grouped/locale/us/market/stocks/{day.isoformat()}"
            payload, body = self._get(url, params)
            _check_unadjusted(body, MassiveError)
            yield self._result(WHOLE_MARKET_SYMBOL, "prices", day, day, payload, body)

    def _fetch_aggs(self, names: list[str], start: date, end: date) -> Iterator[FetchResult]:
        self._require_key()
        params = {"adjusted": "false", "sort": "asc", "limit": str(AGGS_LIMIT)}
        for name in names:
            url = (
                f"{self.base_url}/v2/aggs/ticker/{quote(name, safe='')}/range/1/day/"
                f"{start.isoformat()}/{end.isoformat()}"
            )
            for payload, body in self._pages(url, params):
                _check_unadjusted(body, MassiveError)
                yield self._result(name, "prices", start, end, payload, body)

    def _fetch_actions(self, start: date, end: date) -> Iterator[FetchResult]:
        self._require_key()
        for path, column in (
            ("/stocks/v1/dividends", "ex_dividend_date"),
            ("/stocks/v1/splits", "execution_date"),
        ):
            params = {
                f"{column}.gte": start.isoformat(),
                f"{column}.lte": end.isoformat(),
                "limit": str(LIST_LIMIT),
            }
            for payload, body in self._pages(self.base_url + path, params):
                yield self._result(WHOLE_MARKET_SYMBOL, "actions", start, end, payload, body)

    def _fetch_symbols(self) -> Iterator[FetchResult]:
        self._require_key()
        for active in ("true", "false"):
            params = {"market": "stocks", "active": active, "limit": str(TICKERS_LIMIT)}
            url = self.base_url + "/v3/reference/tickers"
            for payload, body in self._pages(url, params):
                yield self._result(WHOLE_MARKET_SYMBOL, "symbols", None, None, payload, body)

    def _result(
        self,
        symbol: str,
        kind: str,
        start: date | None,
        end: date | None,
        payload: bytes,
        body: Mapping[str, Any],
    ) -> FetchResult:
        return FetchResult(
            source_code=self.source_code,
            source_symbol=symbol,
            kind=kind,  # type: ignore[arg-type]
            requested_from=start,
            requested_to=end,
            fetched_at=self._now(),
            status="ok" if body.get("results") else "empty",
            payload=payload,
            content_type=CONTENT_TYPE,
        )

    def _pages(
        self, url: str, params: Mapping[str, str]
    ) -> Iterator[tuple[bytes, Mapping[str, Any]]]:
        """Each page of a list, following ``next_url`` as given (never on another host)."""
        page: tuple[str, Mapping[str, str] | None] | None = (url, params)
        while page is not None:
            payload, body = self._get(*page)
            yield payload, body
            nxt = body.get("next_url")
            if not nxt:
                page = None
            elif not isinstance(nxt, str) or _origin(nxt) != self._origin:
                raise MassiveError("Massive next_url points outside base_url; refused")
            else:
                page = (nxt, None)

    def _wait_turn(self) -> None:
        now = self._clock()
        if self._next is not None and now < self._next:
            self._sleep(self._next - now)

    def _get(self, url: str, params: Mapping[str, str] | None) -> tuple[bytes, Mapping[str, Any]]:
        headers = {"Authorization": f"Bearer {self._require_key().reveal()}"}
        for attempt in range(self.max_retries + 1):
            self._wait_turn()
            self._next = self._clock() + self._interval
            try:
                response = self._http.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                error = MassiveError(f"Massive request failed: {type(exc).__name__}")
            else:
                status = response.status_code
                if status == 200:
                    return response.content, _body(response.content)
                if status in (401, 403):
                    raise MassiveKeyError(
                        f"Massive refused the key with HTTP {status}: check {API_KEY_ENV} and "
                        "that the request is within your plan"
                    )
                if status != 429:
                    raise MassiveError(f"Massive request failed with HTTP {status}")
                if attempt == self.max_retries:
                    raise MassiveError(
                        f"Massive still answers HTTP 429 after {self.max_retries} retries"
                    )
                wait = _retry_after(response) or self.backoff_s * 2**attempt
                if wait > self.max_wait_s:
                    raise MassiveError(
                        f"Massive asks to wait {wait:g} s after HTTP 429, beyond max_wait_s "
                        f"({self.max_wait_s:g} s)"
                    )
                self._next = max(self._next, self._clock() + wait)
                continue
            raise error  # outside the except block: no chained transport exception
        raise AssertionError("unreachable")  # pragma: no cover

    # --- parse ---------------------------------------------------------------------------------

    def parse(self, result: FetchResult) -> pa.Table:
        return self.parse_report(result).table

    def parse_report(self, result: FetchResult) -> ParseReport:
        try:
            body = _body(result.payload)
        except MassiveError:
            raise ValueError(f"Massive {result.kind} payload is not a JSON object") from None
        rows = body.get("results") or []
        if not isinstance(rows, list):
            raise ValueError(f"Massive {result.kind} payload results is not a list")
        if result.kind == "prices":
            _check_unadjusted(body, ValueError)
            return ParseReport(self._prices(result, rows))
        if result.kind == "actions":
            return self._actions(rows)
        return ParseReport(_symbols(rows))

    def _prices(self, result: FetchResult, rows: list[dict[str, Any]]) -> pa.Table:
        grouped = result.source_symbol == WHOLE_MARKET_SYMBOL
        if grouped and (
            result.requested_from is None or result.requested_from != result.requested_to
        ):
            raise ValueError("a grouped daily fetch must request exactly one session")
        out = []
        for row in rows:
            symbol = row.get("T") if grouped else result.source_symbol
            if not isinstance(symbol, str) or not symbol:
                raise ValueError("Massive grouped bar without a ticker (T)")
            if grouped:
                day = result.requested_from
            else:
                stamp = row.get("t")
                if not isinstance(stamp, int | float) or isinstance(stamp, bool):
                    raise ValueError(f"Massive bar of {symbol} without a timestamp (t)")
                day = datetime.fromtimestamp(stamp / 1000, UTC).astimezone(self.exchange_tz)
                day = day.date()
            where = f"{symbol} on {day.isoformat()}"
            close = _float(row.get("c"), f"close of {where}")
            if close is None:
                raise ValueError(f"Massive bar of {where} has no close (c)")
            out.append(
                {
                    "source_code": self.source_code,
                    "source_symbol": symbol,
                    "valid_date": day,
                    "open": _float(row.get("o"), f"open of {where}"),
                    "high": _float(row.get("h"), f"high of {where}"),
                    "low": _float(row.get("l"), f"low of {where}"),
                    "close": close,
                    "volume": _volume(row.get("v"), where),
                    "currency": CURRENCY,
                }
            )
        return pa.Table.from_pylist(out, schema=RAW_PRICE_SCHEMA)

    def _actions(self, rows: list[dict[str, Any]]) -> ParseReport:
        kept_out: dict[str, set[Key]] = {r: set() for r in KEPT_OUT_REASONS}
        seq: dict[tuple[str, date, str], int] = {}
        out = []
        for row in rows:
            symbol = row.get("ticker")
            if not isinstance(symbol, str) or not symbol:
                raise ValueError(f"Massive action row without a ticker: id {row.get('id')!r}")
            if "ex_dividend_date" in row:
                day = _date(row["ex_dividend_date"], f"ex_dividend_date of {symbol}")
                raw = row.get("distribution_type")
                action_type = DIVIDEND_TYPES.get(raw) if isinstance(raw, str) else None
                reason = UNKNOWN_DISTRIBUTION_TYPE
                where = f"{symbol} on {day.isoformat()}"
                values = {
                    "announce_date": _date(row.get("declaration_date"), f"declaration of {where}"),
                    "record_date": _date(row.get("record_date"), f"record_date of {where}"),
                    "pay_date": _date(row.get("pay_date"), f"pay_date of {where}"),
                    "amount": _float(row.get("cash_amount"), f"cash_amount of {where}"),
                    "currency": row.get("currency"),
                }
            elif "execution_date" in row:
                day = _date(row["execution_date"], f"execution_date of {symbol}")
                raw = row.get("adjustment_type")
                action_type = SPLIT_TYPES.get(raw) if isinstance(raw, str) else None
                reason = UNKNOWN_ADJUSTMENT_TYPE
                where = f"{symbol} on {day.isoformat()}"
                values = {
                    "ratio_old": _float(row.get("split_from"), f"split_from of {where}"),
                    "ratio_new": _float(row.get("split_to"), f"split_to of {where}"),
                }
            else:
                raise ValueError(
                    f"Massive action row of {symbol} has neither ex_dividend_date nor "
                    "execution_date"
                )
            if day is None:
                raise ValueError(f"Massive action row of {symbol} has a null date")
            if action_type is None:
                kept_out[reason].add((symbol, day))
                continue
            ident = (symbol, day, action_type)
            seq[ident] = seq.get(ident, -1) + 1
            out.append(
                {
                    "source_code": self.source_code,
                    "source_symbol": symbol,
                    "valid_date": day,
                    "action_type": action_type,
                    "action_seq": seq[ident],
                    "source_action_type": raw,
                    "amount_basis": None,
                    "per_share_basis": None,
                    **values,
                }
            )
        return ParseReport(
            pa.Table.from_pylist(out, schema=RAW_ACTION_SCHEMA),
            {reason: tuple(sorted(keys)) for reason, keys in kept_out.items()},
        )


def adapter() -> MassiveAdapter:
    """Entry point ``market_data.adapters:massive``: an adapter keyed from the environment."""
    return MassiveAdapter.from_env()


# --- helpers -------------------------------------------------------------------------------------


def _origin(url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    return parts.scheme.lower(), parts.netloc.lower()


def _body(payload: bytes) -> Mapping[str, Any]:
    try:
        body = json.loads(payload)
    except ValueError:
        raise MassiveError("Massive answer is not JSON") from None
    if not isinstance(body, dict):
        raise MassiveError("Massive answer is not a JSON object")
    if body.get("status") == "ERROR":
        raise MassiveError("Massive answered status ERROR")
    return body


def _check_unadjusted(body: Mapping[str, Any], error: type[Exception]) -> None:
    if body.get("adjusted") is True:
        raise error("Massive answered adjusted prices; only unadjusted prices are accepted")


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After", "").strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # an HTTP date or garbage: fall back to exponential backoff
    return value if value > 0 and math.isfinite(value) else None


def _float(value: Any, what: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"Massive {what} is not a number")
    return float(value)


def _volume(value: Any, where: str) -> int | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value != int(value)
        or value < 0
    ):
        raise ValueError(f"Massive volume of {where} is not a non-negative integer: {value!r}")
    return int(value)


def _date(value: Any, what: str) -> date | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"Massive {what} is not a date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Massive {what} is not a date: {value!r}") from None


def _symbols(rows: list[dict[str, Any]]) -> pa.Table:
    out = []
    for row in rows:
        symbol = row.get("ticker")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("Massive ticker row without a ticker")
        valid_date = _date(row.get("list_date"), f"list_date of {symbol}")
        valid_to = None
        delisted = row.get("delisted_utc")
        if delisted:
            try:
                stamp = datetime.fromisoformat(str(delisted).replace("Z", "+00:00"))
            except ValueError:
                raise ValueError(f"Massive delisted_utc of {symbol} is not a timestamp") from None
            if stamp.tzinfo is not None:
                stamp = stamp.astimezone(UTC)
            valid_to = stamp.date() + timedelta(days=1)
        name = row.get("name") if isinstance(row.get("name"), str) else None
        for scheme in _SYMBOL_SCHEMES:
            value = row.get(scheme)
            if not isinstance(value, str) or not value.strip():
                continue
            value = value.strip()
            if scheme == "cik":
                if not value.isdigit() or len(value) > _CIK_WIDTH:
                    raise ValueError(f"Massive cik of {symbol} is not up to 10 digits")
                value = value.zfill(_CIK_WIDTH)
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
    return pa.Table.from_pylist(out, schema=RAW_SYMBOL_SCHEMA)

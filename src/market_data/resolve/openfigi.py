"""OpenFIGI mapping client (https://www.openfigi.com/api, v3 ``/mapping``).

OpenFIGI is a resolver, not a price source: it turns a US ticker root into FIGIs (composite FIGI,
share-class FIGI) from public identifiers. Jobs for US roots use the composite exchange code
``US`` with ``includeUnlistedEquities`` (``us_ticker_job``): the point-in-time venue codes
(``UQ``...) return nothing for dead names (spec §8.2).

Quota (documented by OpenFIGI, spec §8.3: respected, never worked around)
    Without a key: at most ``NO_KEY_RPM`` (25) requests a minute and ``NO_KEY_MAX_JOBS`` (10)
    jobs per request. With a key (``OPENFIGI_API_KEY``, sent in the ``X-OPENFIGI-APIKEY``
    header): ``KEY_RPM`` (250) and ``KEY_MAX_JOBS`` (100). ``rpm``/``max_jobs`` may be set lower,
    never higher. Requests are spaced evenly (one every ``60 / rpm`` seconds), never in bursts.
    A 429 waits ``Retry-After`` seconds when the server sends it, else ``backoff_s * 2**n``
    (n = retries so far), never less than the spacing; after ``max_retries`` retries, or when a
    wait would exceed ``max_wait_s``, ``OpenFigiError`` (no retry storm). Any other non-200
    answer is an ``OpenFigiError`` naming the status only.

The key is a ``Secret``: never in a repr, a log line or an error message. Clock, sleep and the
HTTP transport are injectable (tests use recorded synthetic responses, no network).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Generator, Mapping, Sequence
from typing import Any

import httpx
import pyarrow as pa

from market_data.adapters.base import Secret

__all__ = [
    "API_KEY_ENV",
    "KEY_HELP",
    "API_KEY_HEADER",
    "DEFAULT_BACKOFF_S",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_MAX_WAIT_S",
    "DEFAULT_TIMEOUT_S",
    "FIGI_RESULT_SCHEMA",
    "KEY_MAX_JOBS",
    "KEY_RPM",
    "NO_KEY_MAX_JOBS",
    "NO_KEY_RPM",
    "OPENFIGI_URL",
    "OpenFigiClient",
    "OpenFigiError",
    "figi_results",
    "us_ticker_job",
]

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
API_KEY_ENV = "OPENFIGI_API_KEY"
# Said when the client runs without a key (the keyless quota works, only slower).
KEY_HELP = (
    f"OpenFIGI runs without a key at its keyless quota; a key is optional and free: create an "
    f"account at https://www.openfigi.com/api and set {API_KEY_ENV} for a higher quota"
)
API_KEY_HEADER = "X-OPENFIGI-APIKEY"
NO_KEY_RPM = 25
NO_KEY_MAX_JOBS = 10
KEY_RPM = 250
KEY_MAX_JOBS = 100
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_S = 2.0
DEFAULT_MAX_WAIT_S = 300.0
DEFAULT_TIMEOUT_S = 30.0
_MINUTE_S = 60.0

# One row per FIGI an OpenFIGI job returned, tagged with the provider series the job was for.
# No name or description: labels stay with the caller.
FIGI_RESULT_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("figi", pa.string()),
        pa.field("composite_figi", pa.string()),
        pa.field("share_class_figi", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("exch_code", pa.string()),
        pa.field("security_type", pa.string()),
        pa.field("market_sector", pa.string()),
    ]
)
_RESULT_FIELDS = {
    "figi": "figi",
    "composite_figi": "compositeFIGI",
    "share_class_figi": "shareClassFIGI",
    "ticker": "ticker",
    "exch_code": "exchCode",
    "security_type": "securityType",
    "market_sector": "marketSector",
}


class OpenFigiError(RuntimeError):
    """An OpenFIGI request or answer that cannot be used. Never carries the key."""


def us_ticker_job(root: str) -> dict[str, Any]:
    """The mapping job for a US ticker root (composite exchange ``US``, unlisted included)."""
    return {"idType": "TICKER", "idValue": root, "exchCode": "US", "includeUnlistedEquities": True}


class _KeyAuth(httpx.Auth):
    """Sets the ``X-OPENFIGI-APIKEY`` header on each request as it is sent: the clear key is
    never kept in a headers dict or a local of the request frames."""

    def __init__(self, key: Secret) -> None:
        self._key = key

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers[API_KEY_HEADER] = self._key.reveal()
        yield request

    def __repr__(self) -> str:
        return "_KeyAuth(Secret(***))"


class OpenFigiClient:
    """``map(jobs)`` posts jobs in batches within the quota and returns one result per job,
    aligned with ``jobs``: ``{"data": [...]}``, ``{"warning": ...}`` or ``{"error": ...}`` as
    OpenFIGI answers them."""

    def __init__(
        self,
        api_key: str | Secret | None,
        *,
        rpm: int | None = None,
        max_jobs: int | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_s: float = DEFAULT_BACKOFF_S,
        max_wait_s: float = DEFAULT_MAX_WAIT_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        url: str = OPENFIGI_URL,
    ) -> None:
        key = api_key if isinstance(api_key, Secret) else Secret((api_key or "").strip())
        self._key = key if key.reveal().strip() else None
        rpm_cap, jobs_cap = (KEY_RPM, KEY_MAX_JOBS) if self._key else (NO_KEY_RPM, NO_KEY_MAX_JOBS)
        self.rpm = rpm_cap if rpm is None else rpm
        self.max_jobs = jobs_cap if max_jobs is None else max_jobs
        which = "with" if self._key else "without"
        if not 0 < self.rpm <= rpm_cap:
            raise ValueError(f"rpm must be in 1..{rpm_cap} {which} a key, got {self.rpm}")
        if not 0 < self.max_jobs <= jobs_cap:
            raise ValueError(
                f"max_jobs must be in 1..{jobs_cap} {which} a key, got {self.max_jobs}"
            )
        if max_retries < 0 or backoff_s <= 0 or max_wait_s <= 0:
            raise ValueError("max_retries must be >= 0, backoff_s and max_wait_s > 0")
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.max_wait_s = max_wait_s
        self._interval = _MINUTE_S / self.rpm
        self._clock = clock
        self._sleep = sleep
        self._next: float | None = None  # earliest clock time of the next request
        self._url = url
        self._http = httpx.Client(transport=transport, timeout=timeout_s)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **kw: Any) -> OpenFigiClient:
        """A client keyed from ``OPENFIGI_API_KEY`` when set (else the keyless quota)."""
        raw = (os.environ if env is None else env).get(API_KEY_ENV, "")
        return cls(Secret(raw.strip()) if raw.strip() else None, **kw)

    @property
    def keyed(self) -> bool:
        """True when the client sends a key (else the keyless quota)."""
        return self._key is not None

    def __repr__(self) -> str:
        key = "Secret(***)" if self._key else "None"
        return f"OpenFigiClient(api_key={key}, rpm={self.rpm}, max_jobs={self.max_jobs})"

    __str__ = __repr__

    def close(self) -> None:
        self._http.close()

    def map(self, jobs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i in range(0, len(jobs), self.max_jobs):
            batch = [dict(j) for j in jobs[i : i + self.max_jobs]]
            answer = self._post(batch)
            if not isinstance(answer, list) or len(answer) != len(batch):
                raise OpenFigiError(
                    f"OpenFIGI answer is not aligned with the {len(batch)} job(s) sent"
                )
            out.extend(answer)
        return out

    def _wait_turn(self) -> None:
        now = self._clock()
        if self._next is not None and now < self._next:
            self._sleep(self._next - now)

    def _post(self, batch: list[dict[str, Any]]) -> Any:
        headers = {"Content-Type": "application/json"}
        auth = _KeyAuth(self._key) if self._key else None
        body = json.dumps(batch).encode()
        for attempt in range(self.max_retries + 1):
            self._wait_turn()
            sent = self._clock()
            self._next = sent + self._interval
            try:
                response = self._http.post(self._url, content=body, headers=headers, auth=auth)
            except httpx.HTTPError as exc:
                error = OpenFigiError(f"OpenFIGI request failed: {type(exc).__name__}")
            else:
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError:
                        raise OpenFigiError("OpenFIGI answer is not JSON") from None
                if response.status_code != 429:
                    raise OpenFigiError(
                        f"OpenFIGI mapping request failed with HTTP {response.status_code}"
                    )
                if attempt == self.max_retries:
                    raise OpenFigiError(
                        f"OpenFIGI still answers HTTP 429 after {self.max_retries} retries"
                    )
                wait = _retry_after(response) or self.backoff_s * 2**attempt
                if wait > self.max_wait_s:
                    raise OpenFigiError(
                        f"OpenFIGI asks to wait {wait:g} s after HTTP 429, beyond max_wait_s "
                        f"({self.max_wait_s:g} s)"
                    )
                self._next = max(self._next, self._clock() + wait)
                continue
            raise error  # outside the except block: no chained transport exception
        raise AssertionError("unreachable")  # pragma: no cover


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After", "").strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # an HTTP date or garbage: fall back to exponential backoff
    return value if value > 0 else None


def figi_results(
    series: Sequence[tuple[str, str]], results: Sequence[Mapping[str, Any]]
) -> pa.Table:
    """``FIGI_RESULT_SCHEMA`` rows from ``OpenFigiClient.map`` results.

    ``series[i]`` is the (source_code, source_symbol) job i was made for. A ``warning`` (no
    identifier found) gives no row (the mapping reports the series unresolved); an ``error`` is a
    malformed job and raises ``OpenFigiError``."""
    if len(series) != len(results):
        raise ValueError(f"{len(series)} series not aligned with {len(results)} result(s)")
    rows = []
    for i, ((source_code, symbol), result) in enumerate(zip(series, results, strict=True)):
        if "error" in result:
            raise OpenFigiError(f"OpenFIGI refused job {i}; check the job's fields")
        for entry in result.get("data") or ():
            row = {"source_code": source_code, "source_symbol": symbol}
            for column, key in _RESULT_FIELDS.items():
                value = entry.get(key)
                row[column] = value if isinstance(value, str) and value.strip() else None
            rows.append(row)
    return pa.Table.from_pylist(rows, schema=FIGI_RESULT_SCHEMA)

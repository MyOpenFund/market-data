"""Adapter interface: fetch raw payloads, parse them into a source-neutral intermediate schema.

An adapter lives in a package of its own (or here, once terms-compliant) and is declared under the
``market_data.adapters`` entry-point group: the entry-point name is the source code, the value a
zero-argument callable returning the Adapter. Only adapters named in ``MOF_MARKET_ADAPTERS``
(comma-separated) are loaded.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from importlib.metadata import entry_points
from typing import Literal, Protocol, runtime_checkable

import pyarrow as pa
from open_data_store.contracts.standard.market import FETCH_STATUSES
from open_data_store.sources import SourceInfo, discover_sources

GROUP = "market_data.adapters"
ADAPTER_ENV = "MOF_MARKET_ADAPTERS"

# Source-neutral rows produced by Adapter.parse, before identifier mapping and known_at.
RAW_PRICE_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("valid_date", pa.date32(), nullable=False),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64()),
        pa.field("currency", pa.string(), nullable=False),
    ]
)

# valid_date is the ex-date. action_seq is the order within one payload; the builder (B3) assigns
# the final cross-fetch action_seq.
RAW_ACTION_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("valid_date", pa.date32(), nullable=False),
        pa.field("action_type", pa.string(), nullable=False),
        pa.field("action_seq", pa.int16(), nullable=False),
        pa.field("source_action_type", pa.string()),
        pa.field("announce_date", pa.date32()),
        pa.field("record_date", pa.date32()),
        pa.field("pay_date", pa.date32()),
        pa.field("amount", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("amount_basis", pa.string()),
        pa.field("per_share_basis", pa.string()),
        pa.field("ratio_old", pa.float64()),
        pa.field("ratio_new", pa.float64()),
        pa.field("spinoff_symbol", pa.string()),
    ]
)

# Identifier observations (kind "symbols"): INSTRUMENT_IDENTIFIERS minus instrument_id and the
# bitemporal columns.
RAW_SYMBOL_SCHEMA = pa.schema(
    [
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("id_scheme", pa.string(), nullable=False),
        pa.field("id_value", pa.string(), nullable=False),
        pa.field("valid_date", pa.date32()),
        pa.field("valid_to", pa.date32()),
        pa.field("name", pa.string()),
    ]
)

FETCH_KINDS = ("prices", "actions", "symbols")
WHOLE_MARKET_SYMBOL = "*"


class Secret:
    """A credential that never shows itself: repr and str are masked, use ``reveal()``."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret(***)"

    __str__ = __repr__

    def __bool__(self) -> bool:
        return bool(self._value)


@dataclass(frozen=True)
class FetchResult:
    source_code: str
    # "*" for a whole-market fetch (e.g. a grouped daily of every US stock).
    source_symbol: str
    kind: Literal["prices", "actions", "symbols"]
    requested_from: date | None
    requested_to: date | None
    fetched_at: datetime
    status: str
    payload: bytes = field(repr=False)
    content_type: str

    def __post_init__(self) -> None:
        if self.kind not in FETCH_KINDS:
            raise ValueError(f"fetch kind must be one of {FETCH_KINDS}, got {self.kind!r}")
        if self.status not in FETCH_STATUSES:
            raise ValueError(f"fetch status must be one of {FETCH_STATUSES}, got {self.status!r}")
        if self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")


@runtime_checkable
class Adapter(Protocol):
    """A source adapter. Optional attributes (not part of the structural check):

    ``actions_fetch_kind``: the archived fetch kind whose payload also holds the corporate
    actions, for a source with no separate actions request (Tiingo: ``"prices"``);
    ``build.build_actions`` reads those fetches and parses each as kind ``actions``. Absent:
    ``"actions"``.

    ``corroborates_delisting``: True when the end of the source's mapping windows corroborates a
    delisting, i.e. its windows end at an export date (a history file: a dead member's last
    price ends its window, the export date ends its fetch-log row), so an ``ok``/``empty``
    symbol fetch past the window means the name is gone (``quality.fetch_coverage`` rule 6).
    Absent: False, which a live provider must keep (Massive and Tiingo declare False): one
    ``empty`` answer for the next session (a halt, a late publication) or a renamed ticker is
    no delisting. ``pipeline.corroborating_codes`` reads it.

    ``symbols_list(result) -> str | None``: for a source fetching several ``symbols`` lists
    (Massive: active and inactive tickers), the list one fetch belongs to, read from what the
    adapter archived in its payload; ``pipeline.archive_observations`` lets a newer fetch replace
    an older one of the same list only. Absent, or None: one list."""

    source_code: str
    info: SourceInfo

    def fetch(
        self, kind: str, symbols: Sequence[str], start: date | None, end: date | None
    ) -> Iterator[FetchResult]: ...

    def parse(self, result: FetchResult) -> pa.Table:
        """Rows without known_at, by ``result.kind``: RAW_PRICE_SCHEMA ("prices"),
        RAW_ACTION_SCHEMA ("actions") or RAW_SYMBOL_SCHEMA ("symbols": identifier observations
        shaped like INSTRUMENT_IDENTIFIERS minus instrument_id and the bitemporal columns)."""
        ...


def allowed_adapters(env: Mapping[str, str] | None = None) -> list[str]:
    """Source codes listed in ``MOF_MARKET_ADAPTERS`` (comma-separated); none when unset."""
    raw = (os.environ if env is None else env).get(ADAPTER_ENV, "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def load_adapters(allow: Sequence[str]) -> dict[str, Adapter]:
    """Instantiate the installed adapters whose source code is in ``allow``.

    An adapter not in ``allow`` is not even imported. Fails loudly: an allowed name without an
    entry point, a duplicated entry-point name, an object that is not an Adapter, a code mismatch
    or an unregistered source is a ValueError (no silent fallback).
    """
    allowed = set(allow)
    eps = [ep for ep in entry_points(group=GROUP) if ep.name in allowed]
    names = [ep.name for ep in eps]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise ValueError(f"adapter entry point declared more than once: {dup}")
    missing = sorted(allowed - set(names))
    if missing:
        raise ValueError(f"no adapter installed for allowed source(s): {missing}")
    registry = {s.code for s in discover_sources().values()}
    out: dict[str, Adapter] = {}
    for ep in eps:
        adapter = ep.load()()
        if not isinstance(adapter, Adapter):
            raise ValueError(f"adapter entry point {ep.name!r} does not return an Adapter")
        if adapter.source_code != ep.name or adapter.info.code != adapter.source_code:
            raise ValueError(
                f"adapter entry point {ep.name!r} returned an adapter for "
                f"{adapter.source_code!r} (info code {adapter.info.code!r})"
            )
        if adapter.source_code not in registry:
            raise ValueError(f"source {adapter.source_code!r} is not in the source registry")
        out[ep.name] = adapter
    return out

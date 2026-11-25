"""Adapter interface: fetch raw payloads, parse them into a source-neutral intermediate schema.

An adapter lives in a package of its own (or here, once terms-compliant) and is declared under the
``market_data.adapters`` entry-point group: the entry-point name is the source code, the value a
zero-argument callable returning the Adapter. Only adapters named in ``MOF_MARKET_ADAPTERS``
(comma-separated) are loaded.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from importlib.metadata import entry_points
from typing import Literal, Protocol, runtime_checkable

import pyarrow as pa
from open_data_store.sources import SourceInfo

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

# valid_date is the ex-date.
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
    source_symbol: str
    kind: Literal["prices", "actions", "symbols"]
    requested_from: date | None
    requested_to: date | None
    fetched_at: datetime
    status: str
    payload: bytes
    content_type: str


@runtime_checkable
class Adapter(Protocol):
    source_code: str
    info: SourceInfo

    def fetch(
        self, kind: str, symbols: Sequence[str], start: date | None, end: date | None
    ) -> Iterator[FetchResult]: ...

    def parse(self, result: FetchResult) -> pa.Table:
        """Rows in RAW_PRICE_SCHEMA or RAW_ACTION_SCHEMA (by ``result.kind``), without known_at."""
        ...


def allowed_adapters(env: Mapping[str, str] | None = None) -> list[str]:
    """Source codes listed in ``MOF_MARKET_ADAPTERS`` (comma-separated); none when unset."""
    raw = (os.environ if env is None else env).get(ADAPTER_ENV, "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def load_adapters(allow: Sequence[str]) -> dict[str, Adapter]:
    """Instantiate the installed adapters whose source code is in ``allow``.

    An adapter not in ``allow`` is not even imported.
    """
    allowed = set(allow)
    out: dict[str, Adapter] = {}
    for ep in entry_points(group=GROUP):
        if ep.name not in allowed:
            continue
        adapter = ep.load()()
        if adapter.source_code != ep.name:
            raise ValueError(
                f"adapter entry point {ep.name!r} returned an adapter for {adapter.source_code!r}"
            )
        out[ep.name] = adapter
    return out

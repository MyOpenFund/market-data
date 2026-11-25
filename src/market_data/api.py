"""Python API: daily prices for a few instruments, raw or as a total-return index (spec §10).

``prices(instruments, start, end, ...)`` is the one code path the MCP tool ``market_prices`` and a
notebook call; the profile is configuration.

Profile ``none`` (default: the README "Try it")
    A live fetch with the caller's own key (``api_key``, else ``TIINGO_API_KEY`` /
    ``MASSIVE_API_KEY``) through the chosen adapter (``source``: ``tiingo`` by default, the deep
    free history; ``massive`` allowed, inside its plan's history window). Inputs are the
    adapter's own symbols (tickers, e.g. ``BRK-B`` on Tiingo), used as given. The payloads are
    parsed and built in memory (``archive.MemoryArchive`` read by the same builder as the store
    path, so ``known_at`` follows spec §7.3); nothing is archived, catalogued or written, except
    the adapter's own quota state. Each input maps to an in-memory id ``"live:<source>:<symbol>"``
    (an identity mapping): **these ids are not store instrument ids** and mean nothing outside
    the returned table. Rows carry that id and the ``input`` label (a present-time lookup:
    the caller's own words). With ``total_return`` the store's recipes run on those rows
    (``resolve_prices``, ``resolve_actions``, ``total_return``). Returning to the caller data
    fetched with the caller's own key is not redistribution: no licence gate (spec §13). Massive
    actions are whole-market lists: a total return through Massive pages through every dividend
    and split of the range (slow at the free plan's 5 requests a minute). ``as_of`` here dates
    the identity rows and bounds the recipe's read, nothing more: the data is fetched now, with
    what the source knows today (actions and corrections included).

Profile ``server``
    Reads a store (``store``, else ``Store.open()`` from the environment) through the store
    binding's strict licence gate (``load_market_inputs``' rule): restricted sources are
    dropped unless listed in ``allowed_restricted``, unregistered ones always. The identifiers
    are read first (latest); then only the requested instruments' series are read from
    ``prices`` (and ``corporate_actions`` for a total return), as known at ``as_of``, through
    natural-key filters: a call never reads the whole store. Inputs are tickers, CIKs (digits,
    zero-padded to 10), FIGIs (``figi``, ``composite_figi``, ``share_class_figi``) or
    instrument ids (``mof:...``), resolved through the latest mapping (spec §9.3: the
    identifier rows the gate lets through, open today; a label closed since names nothing,
    whatever ``as_of``). ``source`` filters to one source and must be allowed when
    restricted; ``precedence`` orders several (the first with a row wins per instrument and
    session, so a series spans the private history then a daily provider; each restricted one
    must be allowed); with neither, two sources for one (instrument, session) raise (the
    recipe's rule). Output is keyed by ``instrument_id`` only: an as-of answer carries no
    label.

Output (sorted by instrument_id, session_date; ``start``..``end`` inclusive)
    ``adjust="raw"``: ``RAW_LIVE_SCHEMA`` / ``RAW_SERVER_SCHEMA`` (raw close, volume, currency,
    source_code). ``adjust="total_return"``: ``TOTAL_RETURN_LIVE_SCHEMA`` /
    ``TOTAL_RETURN_SERVER_SCHEMA`` (``tr_index`` based 1.0 on the first session of the range
    with a valid price, and the recipe's ``flags``).

Errors (``ApiError``, a ``ValueError``): ``MissingKeyError`` (no key for the live source; the
key itself is never shown), ``UnknownInstrumentError`` (inputs nothing names, listed as the
caller wrote them, never a stored label), ``AmbiguousInstrumentError`` (an input naming several
instruments), ``RestrictedSourceError`` (a restricted or unregistered source not allowed),
``NoDataError`` (none: a symbol the source returned no row for over the range, either adapter;
server: an input ``source=`` has no price for over the range), and a plain ``ApiError`` when two
sources (or two series of one source) give one instrument different rows on one session
(server, without ``source=``; only the caller's inputs are named). The server profile resolves
only the requested instruments' series and returns only the requested instruments (a symbol
reused by another instrument on other dates contributes its rows of the requested windows
only); an unrelated instrument fails a request only through a series it shares with a requested
one, and the message still names the caller's inputs only.
"""

from __future__ import annotations

import os
from collections.abc import Collection, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
from open_data_store import sources as registry
from open_data_store.contracts.base import Contract
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    INSTRUMENT_IDENTIFIERS,
    PRICES,
)
from open_data_store.market.asof import latest, sessions
from open_data_store.market.returns import resolve_actions, resolve_prices, total_return

from market_data.adapters.base import Adapter, Secret
from market_data.archive import MemoryArchive
from market_data.build import build_actions, build_prices

__all__ = [
    "DEFAULT_LIVE_SOURCE",
    "DEFAULT_MIC",
    "LIVE_PREFIX",
    "LIVE_SOURCES",
    "LABEL_SCHEMES",
    "RAW_LIVE_SCHEMA",
    "RAW_SERVER_SCHEMA",
    "TOTAL_RETURN_LIVE_SCHEMA",
    "TOTAL_RETURN_SERVER_SCHEMA",
    "AmbiguousInstrumentError",
    "ApiError",
    "MissingKeyError",
    "NoDataError",
    "RestrictedSourceError",
    "UnknownInstrumentError",
    "prices",
]

DEFAULT_LIVE_SOURCE = "tiingo"
DEFAULT_MIC = "XNYS"
LIVE_PREFIX = "live:"
# Identifier schemes an input may name in the server profile (instrument ids are taken as is).
LABEL_SCHEMES = ("ticker", "cik", "figi", "composite_figi", "share_class_figi")
_CIK_WIDTH = 10
_ADJUST = ("raw", "total_return")
_PROFILES = ("none", "server")
_IDENTITY_FROM = date(1900, 1, 1)  # the in-memory identity mapping covers every row
_MAX_LISTED = 20  # inputs quoted in an error message

_RAW = [
    pa.field("session_date", pa.date32()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("currency", pa.string()),
    pa.field("source_code", pa.string()),
]
_TR = [pa.field("session_date", pa.date32()), pa.field("tr_index", pa.float64())]
_TR += [pa.field("flags", pa.string())]
_ID = pa.field("instrument_id", pa.string())
_INPUT = pa.field("input", pa.string())
RAW_LIVE_SCHEMA = pa.schema([_ID, _INPUT, *_RAW])
RAW_SERVER_SCHEMA = pa.schema([_ID, *_RAW])
TOTAL_RETURN_LIVE_SCHEMA = pa.schema([_ID, _INPUT, *_TR])
TOTAL_RETURN_SERVER_SCHEMA = pa.schema([_ID, *_TR])


class ApiError(ValueError):
    """A request ``prices`` cannot answer; the message says why and never holds a key."""


class MissingKeyError(ApiError):
    """No key for the live source."""


class RestrictedSourceError(ApiError):
    """A restricted (or unregistered) source the caller did not allow."""


class UnknownInstrumentError(ApiError):
    """Inputs that name no instrument; ``inputs`` lists them as the caller wrote them."""

    def __init__(self, inputs: Sequence[str], why: str) -> None:
        self.inputs = tuple(inputs)
        shown = list(self.inputs[:_MAX_LISTED])
        super().__init__(f"{len(self.inputs)} unknown instrument(s) ({why}): {shown}")


class NoDataError(ApiError):
    """Inputs the source returned no row for over the range; ``inputs`` lists them."""

    def __init__(self, message: str, inputs: Sequence[str]) -> None:
        self.inputs = tuple(inputs)
        super().__init__(message)


class AmbiguousInstrumentError(ApiError):
    """Inputs that name several instruments; ``inputs`` lists them."""

    def __init__(self, inputs: Sequence[str]) -> None:
        self.inputs = tuple(inputs)
        super().__init__(
            f"{len(self.inputs)} input(s) name several instruments; pass an instrument id or a "
            f"FIGI: {list(self.inputs[:_MAX_LISTED])}"
        )


def _key_envs() -> dict[str, str]:
    from market_data.adapters import massive, tiingo

    return {tiingo.SOURCE_CODE: tiingo.API_KEY_ENV, massive.SOURCE_CODE: massive.API_KEY_ENV}


LIVE_SOURCES = ("tiingo", "massive")


def _live_adapter(source: str, api_key: str | Secret | None) -> Adapter:
    """The live adapter of ``source`` keyed with the caller's key (argument, else env)."""
    env = _key_envs()[source]
    raw = api_key.reveal() if isinstance(api_key, Secret) else (api_key or "")
    if not raw.strip():
        raw = os.environ.get(env, "")
    if not raw.strip():
        raise MissingKeyError(
            f"no {source} API key: pass api_key= or set {env} (your own free key; the 'none' "
            "profile never uses a shared one)"
        )
    key = Secret(raw.strip())
    if source == "tiingo":
        from market_data.adapters.tiingo import TiingoAdapter

        return TiingoAdapter(key)
    from market_data.adapters.massive import MassiveAdapter

    return MassiveAdapter(key)


def _inputs(instruments: Sequence[str]) -> list[str]:
    if isinstance(instruments, str) or not isinstance(instruments, Iterable):
        raise ValueError("instruments must be a sequence of strings, e.g. ['AAPL']")
    out: list[str] = []
    for item in instruments:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("every instrument must be a non-empty string")
        if item.strip() not in out:
            out.append(item.strip())
    if not out:
        raise ValueError("pass at least one instrument")
    return out


def _aware(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be a timezone-aware datetime")
    return value


def prices(
    instruments: Sequence[str],
    start: date,
    end: date,
    *,
    source: str | None = None,
    adjust: Literal["raw", "total_return"] = "raw",
    profile: Literal["none", "server"] = "none",
    store: Any = None,
    api_key: str | Secret | None = None,
    as_of: datetime | None = None,
    allowed_restricted: Collection[str] = frozenset(),
    mic: str = DEFAULT_MIC,
    adapter: Adapter | None = None,
    precedence: Sequence[str] | None = None,
) -> pa.Table:
    """Daily prices of ``instruments`` from ``start`` to ``end`` (inclusive); see the module
    docstring for the profiles, the output and the errors.

    ``as_of`` (tz-aware, default now): server, the store read as known then; none, the
    recipe's read of the in-memory rows and the date of the identity rows only: the data is
    fetched now, so actions and corrections the source knows today apply whatever ``as_of``
    (only a store keeps earlier vintages). ``allowed_restricted``: restricted source codes the
    server profile may read. ``mic``: the session calendar. ``adapter``: an adapter to use
    instead of building the live one from ``source`` and the key (tests, a custom adapter);
    the caller closes it. ``precedence`` (server): source codes in order, the first with a row
    wins per (instrument, session), so one series spans a junction (the private history,
    then a daily provider); sources it leaves out are ignored, each restricted one must be
    allowed; ``source=`` still filters to one source."""
    if adjust not in _ADJUST:
        raise ValueError(f"adjust must be one of {_ADJUST}, got {adjust!r}")
    if profile not in _PROFILES:
        raise ValueError(f"profile must be one of {_PROFILES}, got {profile!r}")
    if start > end:
        raise ValueError(f"start {start.isoformat()} is after end {end.isoformat()}")
    labels = _inputs(instruments)
    at = _aware(as_of)
    order = _precedence(precedence)
    if profile == "none":
        if order:
            raise ValueError("precedence applies to the server profile only (one live source)")
        return _live(labels, start, end, source, adjust, at, mic, api_key, adapter)
    return _server(labels, start, end, source, adjust, at, mic, store, allowed_restricted, order)


def _precedence(precedence: Sequence[str] | None) -> tuple[str, ...]:
    if precedence is None:
        return ()
    if isinstance(precedence, str):
        raise TypeError("precedence must be a sequence of source codes, not a string")
    order = tuple(dict.fromkeys(precedence))
    if not all(isinstance(c, str) and c for c in order):
        raise ValueError("precedence must hold source codes (non-empty strings)")
    return order


# --- profile none ------------------------------------------------------------------------------


def _identity(source: str, symbols: Sequence[str], known_at: datetime) -> pa.Table:
    rows = [
        {
            "instrument_id": f"{LIVE_PREFIX}{source}:{s}",
            "id_scheme": "source_symbol",
            "id_value": f"{source}:{s}",
            "valid_to": None,
            "link_reason": None,
            "known_at_basis": "load_time",
            "valid_date": _IDENTITY_FROM,
            "known_at": known_at,
            "known_at_kind": "collected",
            "source_code": source,
            "source_doc_id": None,
        }
        for s in symbols
    ]
    return pa.Table.from_pylist(rows, schema=INSTRUMENT_IDENTIFIERS.schema)


def _live(
    symbols: list[str],
    start: date,
    end: date,
    source: str | None,
    adjust: str,
    as_of: datetime,
    mic: str,
    api_key: str | Secret | None,
    adapter: Adapter | None,
) -> pa.Table:
    if adapter is None:
        code = source or DEFAULT_LIVE_SOURCE
        if code not in LIVE_SOURCES:
            raise ValueError(f"source must be one of {LIVE_SOURCES} in profile 'none'")
        live = _live_adapter(code, api_key)
        try:
            return _live_with(live, symbols, start, end, adjust, as_of, mic)
        finally:
            live.close()  # type: ignore[attr-defined]
    if source is not None and source != adapter.source_code:
        raise ValueError(f"source {source!r} does not match the adapter's {adapter.source_code!r}")
    return _live_with(adapter, symbols, start, end, adjust, as_of, mic)


def _live_with(
    adapter: Adapter,
    symbols: list[str],
    start: date,
    end: date,
    adjust: str,
    as_of: datetime,
    mic: str,
) -> pa.Table:
    code = adapter.source_code
    memory = MemoryArchive()
    for result in adapter.fetch("prices", symbols, start, end):
        memory.put(result)
    if adjust == "total_return" and getattr(adapter, "actions_fetch_kind", "actions") == "actions":
        for result in adapter.fetch("actions", [], start, end):
            memory.put(result)
    missing = [
        s
        for s in symbols
        if any(r.status == "not_found" for r in memory.history(code, s, "prices"))
    ]
    if missing:
        raise UnknownInstrumentError(missing, f"{code} knows no such symbol")
    built = build_prices(memory, adapter, symbols, mic=mic).table  # type: ignore[arg-type]
    built = _in_range(built, "valid_date", start, end)
    present = set(built["source_symbol"].to_pylist())
    empty = [s for s in symbols if s not in present]
    if empty:
        raise NoDataError(
            f"no data for {empty[:_MAX_LISTED]} from {code} between {start.isoformat()} and "
            f"{end.isoformat()}",
            empty,
        )
    identifiers = _identity(code, symbols, as_of)
    resolved = resolve_prices(built, identifiers, as_of=as_of, precedence=())
    label = {f"{LIVE_PREFIX}{code}:{s}": s for s in symbols}
    if adjust == "raw":
        out = _raw(resolved, start, end)
        return _with_input(out, label, RAW_LIVE_SCHEMA)
    actions = resolve_actions(
        build_actions(memory, adapter, symbols, mic=mic).table,  # type: ignore[arg-type]
        identifiers,
        as_of=as_of,
        precedence=(),
    )
    out = _total_return(resolved, actions, start, end, mic)
    return _with_input(out, label, TOTAL_RETURN_LIVE_SCHEMA)


def _with_input(table: pa.Table, label: dict[str, str], schema: pa.Schema) -> pa.Table:
    inputs = pa.array([label[i] for i in table["instrument_id"].to_pylist()], pa.string())
    return table.add_column(1, "input", inputs).cast(schema)


# --- profile server ----------------------------------------------------------------------------


def _check_source(source: str | None, allowed: frozenset[str]) -> None:
    if source is None:
        return
    known = registry.discover_sources()
    if source not in known:
        raise RestrictedSourceError(f"source {source!r} is not in the source registry")
    if source in registry.restricted_codes(known) and source not in allowed:
        raise RestrictedSourceError(
            f"source {source!r} is restricted: pass allowed_restricted={{{source!r}}} to read it "
            "(personal use on your own store only)"
        )


def _normalised(value: str) -> list[tuple[str, str]]:
    """(scheme, value) pairs an input may stand for."""
    if value.isdigit():
        return [("cik", value.zfill(_CIK_WIDTH))]
    upper = value.upper()
    return [(s, upper) for s in LABEL_SCHEMES if s != "cik"]


def _resolve_inputs(labels: list[str], identifiers: pa.Table, on: date) -> dict[str, str]:
    """input -> instrument_id, through the identifier rows open on ``on``."""
    known: dict[tuple[str, str], set[str]] = {}
    instruments: set[str] = set()
    if identifiers.num_rows:
        rows = latest(identifiers, INSTRUMENT_IDENTIFIERS.natural_key).select(
            ["instrument_id", "id_scheme", "id_value", "valid_date", "valid_to"]
        )
        for r in rows.to_pylist():
            instruments.add(r["instrument_id"])
            if r["id_scheme"] not in LABEL_SCHEMES:
                continue
            if r["valid_date"] > on or (r["valid_to"] is not None and r["valid_to"] <= on):
                continue
            value = r["id_value"].upper() if r["id_scheme"] != "cik" else r["id_value"]
            known.setdefault((r["id_scheme"], value), set()).add(r["instrument_id"])
    out: dict[str, str] = {}
    unknown, ambiguous = [], []
    for label in labels:
        if label.startswith("mof:"):
            if label in instruments:
                out[label] = label
            else:
                unknown.append(label)
            continue
        found = set().union(*(known.get(k, set()) for k in _normalised(label)))
        if len(found) == 1:
            out[label] = found.pop()
        elif found:
            ambiguous.append(label)
        else:
            unknown.append(label)
    if ambiguous:
        raise AmbiguousInstrumentError(ambiguous)
    if unknown:
        raise UnknownInstrumentError(
            unknown, f"no identifier readable here names them on {on.isoformat()}"
        )
    return out


def _read_gated(
    store: Any,
    contract: Contract,
    at: datetime,
    allowed: frozenset[str],
    known: Mapping[str, Any],
    filters: dict[str, list[str]] | None = None,
) -> pa.Table:
    """``contract``'s rows as known at ``at`` (only those ``filters`` select), through the
    store binding's strict licence gate (``load_market_inputs``' rule): restricted sources
    dropped unless allowed, unregistered ones always."""
    from open_data_store.reader.api import UnknownDataset

    name = contract.table_name
    try:
        published = store.is_published(name)
    except UnknownDataset:
        published = False
    if not published:
        return contract.schema.empty_table()
    table = store.read_asof(name, at, filters=filters)
    table = registry.filter_sources(
        table,
        allowed_restricted=allowed,
        restricted=registry.restricted_codes(known),
        strict=True,
        registered=frozenset(known),
    )
    return table.select(contract.schema.names).cast(contract.schema)


def _server(
    labels: list[str],
    start: date,
    end: date,
    source: str | None,
    adjust: str,
    as_of: datetime,
    mic: str,
    store: Any,
    allowed_restricted: Collection[str],
    order: tuple[str, ...] = (),
) -> pa.Table:
    allowed = frozenset(allowed_restricted)
    _check_source(source, allowed)
    for code in order:
        _check_source(code, allowed)
    known = registry.discover_sources()
    own = store is None
    if own:
        from open_data_store.reader import Store

        store = Store.open()
    try:
        # the mapping first (read latest, spec 9.3), then only the requested series' facts
        now = datetime.now(UTC)
        identifiers = _read_gated(store, INSTRUMENT_IDENTIFIERS, now, allowed, known)
        named = _resolve_inputs(labels, identifiers, now.date())
        series = _series_of(identifiers, set(named.values()))
        tokens = set().union(*series.values())
        if source is not None:
            tokens = {t for t in tokens if t.partition(":")[0] == source}
        elif order:
            tokens = {t for t in tokens if t.partition(":")[0] in order}
        filters = None
        if tokens:
            parts = [t.partition(":") for t in tokens]
            filters = {
                "source_code": sorted({p[0] for p in parts}),
                "source_symbol": sorted({p[2] for p in parts}),
            }
        prices_in = _facts(store, PRICES, as_of, allowed, known, filters, tokens)
        actions_in = None
        if adjust != "raw":
            actions_in = _facts(store, CORPORATE_ACTIONS, as_of, allowed, known, filters, tokens)
    finally:
        if own:
            store.close()
    precedence = (source,) if source else order
    wanted = pa.array(sorted(set(named.values())), pa.string())

    def requested(table: pa.Table) -> pa.Table:
        # a series reused by another instrument on other dates resolves to it there: keep
        # only the requested instruments
        return table.filter(pc.is_in(table["instrument_id"], value_set=wanted))

    def resolved_for(tokens: set[str]) -> tuple[pa.Table, pa.Table | None]:
        rp = resolve_prices(
            _of_series(prices_in, tokens), identifiers, as_of=as_of, precedence=precedence
        )
        if actions_in is None:
            return requested(rp), None
        ra = resolve_actions(
            _of_series(actions_in, tokens), identifiers, as_of=as_of, precedence=precedence
        )
        return requested(rp), requested(ra)

    try:
        resolved, actions = resolved_for(set().union(*series.values()))
    except ValueError:
        # name the caller's inputs only: the recipe's message names other series
        clashing = []
        for label, inst in named.items():
            try:
                resolved_for(series[inst])
            except ValueError:
                clashing.append(label)
        raise ApiError(
            f"several sources (or conflicting series) for {clashing[:_MAX_LISTED]} on one "
            "session; pass source= to choose one, or precedence= to order them"
        ) from None
    if source is not None:
        window = _in_range(resolved, "session_date", start, end)
        have = set(window["instrument_id"].to_pylist())
        empty = [label for label, inst in named.items() if inst not in have]
        if empty:
            raise NoDataError(
                f"no {source} prices for {empty[:_MAX_LISTED]} between {start.isoformat()} "
                f"and {end.isoformat()}: try another source, or none to take any allowed one",
                empty,
            )
    if adjust == "raw":
        return _raw(resolved, start, end).cast(RAW_SERVER_SCHEMA)
    assert actions is not None
    return _total_return(resolved, actions, start, end, mic).cast(TOTAL_RETURN_SERVER_SCHEMA)


def _facts(
    store: Any,
    contract: Contract,
    as_of: datetime,
    allowed: frozenset[str],
    known: Mapping[str, Any],
    filters: dict[str, list[str]] | None,
    tokens: set[str],
) -> pa.Table:
    """The rows of ``tokens``' series as known at ``as_of`` (none read without a series)."""
    if not tokens:
        return contract.schema.empty_table()
    table = _read_gated(store, contract, as_of, allowed, known, filters)
    return _of_series(table, tokens)


def _series_of(identifiers: pa.Table, instruments: set[str]) -> dict[str, set[str]]:
    """instrument_id -> its ``<source_code>:<symbol>`` series (mapping rows, read latest)."""
    out: dict[str, set[str]] = {i: set() for i in instruments}
    if not identifiers.num_rows:
        return out
    rows = latest(identifiers, INSTRUMENT_IDENTIFIERS.natural_key)
    rows = rows.filter(pc.equal(rows["id_scheme"], "source_symbol"))
    for r in rows.select(["instrument_id", "id_value"]).to_pylist():
        if r["instrument_id"] in out:
            out[r["instrument_id"]].add(r["id_value"])
    return out


def _of_series(table: pa.Table, tokens: set[str]) -> pa.Table:
    """The rows of ``table`` (source_code, source_symbol) belonging to ``tokens``."""
    token = pc.binary_join_element_wise(table["source_code"], table["source_symbol"], ":")
    return table.filter(pc.is_in(token, value_set=pa.array(sorted(tokens), pa.string())))


# --- shared ------------------------------------------------------------------------------------


def _in_range(table: pa.Table, column: str, start: date, end: date) -> pa.Table:
    day = table[column]
    keep = pc.and_(
        pc.greater_equal(day, pa.scalar(start, pa.date32())),
        pc.less_equal(day, pa.scalar(end, pa.date32())),
    )
    return table.filter(keep)


def _sorted(table: pa.Table) -> pa.Table:
    return table.sort_by([("instrument_id", "ascending"), ("session_date", "ascending")])


def _raw(resolved: pa.Table, start: date, end: date) -> pa.Table:
    out = _in_range(resolved, "session_date", start, end)
    return _sorted(out.select(["instrument_id", *(f.name for f in _RAW)]))


def _total_return(
    resolved: pa.Table, actions: pa.Table, start: date, end: date, mic: str
) -> pa.Table:
    """The recipe over ``start``..``end``: based 1.0 on the first valid price of the range."""
    window = _in_range(resolved, "session_date", start, end)
    acts = _in_range(actions, "ex_date", start, end)
    return _sorted(total_return(window, acts, sessions(mic, start, end)))

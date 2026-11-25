"""Instrument mapping: which source symbol is which instrument, over time (spec §7.1).

``build_mapping`` turns identifier observations (any adapter's ``symbols`` rows, the history-file
adapter's ``observations``) and parsed OpenFIGI results into ``INSTRUMENT_IDENTIFIERS`` rows:
the identifier observations of each instrument and the dated ``source_symbol`` mapping rows
the store resolves provider series through (``open_data_store.market._mapping``).

Rules
    - One instrument per composite FIGI. Its ``instrument_id`` is opaque, ``"mof:" +
      uuid4().hex`` (random); ids already published (``existing``, found through their
      ``composite_figi`` rows) are reused, never re-minted: determinism across runs comes from
      passing ``existing``. New ids are listed in ``MappingResult.minted``. ``load_time`` must
      be after every ``known_at`` of ``existing`` (a later run supersedes an earlier one).
    - A series' candidate composite FIGIs come from its ``composite_figi`` observations (each
      observation window kept, overlapping or touching windows of one composite merged: A, gap,
      A leaves the gap unmapped; A, B, A gives three rows; an undated sighting next to a dated
      window of the same composite is dropped as stale), else from its OpenFIGI results
      (observations win: OpenFIGI results of a series that has a composite observation are
      ignored).
    - Export-dated sources (``export_dated_sources``, default ``private_history``: a
      history-file label is dated at the export) with one candidate: the mapping row covers the
      series' span, ``valid_date`` = span start, ``valid_to`` = span end + 1 day (half-open);
      without a span the series is ``no_span`` (never bounded by the export date).
    - Every other series: each candidate maps over its own observation window (a missing start
      or end taken from the span), clipped to the span; a closed observation window is honoured.
      Disjoint windows give one mapping row per instrument (a recycled ticker); part of the
      span covered by none is reported ``unresolved`` (``uncovered_span``) while the covered
      parts are mapped. Overlapping windows, or several undated candidates, make the series
      ``ambiguous`` (``several_composite_figis``): not mapped. Without a span the observation
      window is used; with no start at all: ``unresolved`` (``no_span``). When ``spans`` are
      given, a series without one, without published windows, a manual row or an OpenFIGI
      answer is a symbol only listed (a symbols fetch), never priced: not a series, neither
      mapped nor reported, counted in ``MappingResult.listed_not_priced``. Candidates entirely
      outside the span are dropped; none left: ``outside_span``. No candidate:
      ``no_composite_figi``.
    - A present-time candidate (a composite known only from OpenFIGI, or the single composite
      observation of a non-export source with neither ``valid_date`` nor ``valid_to``) never
      re-dates, closes or replaces a published window. For a series without published
      windows it is not mapped (``ambiguous``) when another series of the same source claims
      it (``openfigi_shared``; claimants: this run's observations and OpenFIGI answers, the
      published open mapping rows of ``existing`` and the manual targets) or when the series'
      span ended more than ``max_openfigi_age_days`` (default 30) before the load date
      (``openfigi_stale``: the symbol may name someone else now). For a series with published
      windows: when it names the instrument of the newest published window and is neither
      shared nor stale, it may only extend that window's end to this run's span end;
      otherwise nothing changes and the series is reported (``openfigi_changed`` when it names
      another instrument, else ``openfigi_shared`` / ``openfigi_stale``). Only a window the
      data closed is extended (it ended at the series' previous span end): a window the
      source closed at a date is kept and reported ``window_closed``, and a span without a
      ``last_date`` extends nothing. Previous spans are not stored, so "closed at a date" is
      read from the published ``composite_figi`` labels of the series' source on that
      instrument: when none around the window is open, their latest ``valid_to`` caps the
      extension (at or before the window's end: nothing moves). Only a manual row closes a
      published mapping.
    - A series with published windows whose candidates this run cannot be applied (several
      composites over overlapping or undated ranges: ``several_composite_figis``; none
      inside the span, no usable start: ``outside_span`` / ``no_span``) is reported as any
      other and keeps its published windows as they are.
    - A series with published open mapping rows and no span or no candidate this run keeps
      its published windows as they are (absence of evidence never unmaps nor re-dates); the
      part of this run's span they leave uncovered is reported ``uncovered_span`` (a span
      without a ``first_date``: ``no_span``, the coverage cannot be checked).
    - Reports carry [source_code, source_symbol, reason, candidates] and never a label; write
      them to private reports only.
    - Mapping rows carry the series' ``source_code`` (``id_value`` =
      ``"<source_code>:<symbol>"``). ``link_reason`` is null, except ``venue_change`` on the
      rows of two or more symbols of one source mapped to one instrument in the same run: that
      covers a plain rename too, and depends on the batch (a symbol added in a later run alone
      is not relabelled; a published row whose end a present-time answer extends keeps its
      published ``link_reason``). Their windows may overlap (a venue change with duplicated
      history): the store's ``resolve_prices`` keeps identical rows once and raises when any
      value of the overlapping rows differs (close, volume, currency...), so such overlaps
      must carry identical rows or be made disjoint with a manual row.
    - Identifier observations (``OBSERVED_SCHEMES``) keep the observation's ``source_code`` (a
      label from the private export stays under the restricted ``private_history`` source). A
      ``composite_figi`` observation is emitted only on its own instrument (the one that
      composite names), never on another company's window; when that instrument has no window
      in the series (a manual override, a candidate outside the span) it is left out. Other
      labels: on an export-dated series that is one instrument, all of them whatever their
      dates; otherwise on the one instrument whose window the label overlaps. A series that
      keeps its published windows without this run's answer applied (a present-time answer
      naming another instrument, shared, stale or capped by a dated end; several composites;
      no composite; no span, unless every composite it observes names a kept instrument) gets
      none of this run's labels (``answer_not_applied``): they may describe whoever trades
      the symbol now. An undated composite observation of a non-export source never rewrites
      a published label of the same instrument, source and value (re-emitted open-ended it
      would erase the dated end a later run's cap reads). Labels left out
      (``composite_not_mapped``, ``outside_windows``, ``several_windows``,
      ``answer_not_applied``) are counted in ``MappingResult.labels_left_out``; labels of an
      unmapped series are not (the series is reported). A manually mapped series gets none
      of its observed labels (``manual_override``): the review overrides what the source
      says. An observation without a date takes its instrument's window start.
    - OpenFIGI-derived rows (``composite_figi``, ``figi``, ``share_class_figi``, ``ticker``)
      are dated at the load date and open-ended (present-time answers: never a span start or
      an exit date), under the series' own source when it is restricted or unregistered (fail
      closed: a private token's FIGIs stay ``private_history``), else ``openfigi``; they are
      emitted only for a series whose answer this run applied. A row already published open (same
      instrument, scheme, value, source) is not emitted again.
    - Manual mapping (``load_manual_mapping``; a reviewed private CSV at
      ``MOF_MARKET_MANUAL_MAPPING``) wins for every series it names: that series gets only the
      manual rows (source ``manual``, ``link_reason`` as filed, default ``manual``), whatever
      the observations and OpenFIGI say. A row names an ``instrument_id`` already known
      (published or minted in this run) or a ``composite_figi`` (its instrument, minted when
      new, with a ``manual`` ``composite_figi`` row so the id is found next run). With
      ``manual=None`` the published manual rows (from ``existing``) keep winning, so a run
      without the file never flips a reviewed mapping; with a manual table (even empty) it is
      the full truth and published manual rows it no longer holds are closed.
    - Closing: a published mapping row (in ``existing``, read latest) of a series this run
      covers that the run does not emit again is re-emitted with ``valid_to = valid_date``, an
      empty window the store never applies, so an outdated mapping (a series now manual or
      re-dated by dated data) cannot clash with the new one. A series without a span this run
      keeps its published windows as they are (never rewritten open-ended).
    - Dated data (dated composite observations; a later export giving an export-dated token
      another composite) is the source of truth correcting itself: it is applied, the
      published rows it replaces are closed, and every move of a series to another
      instrument is reported in ``MappingResult.changed`` (``export_composite_changed`` for
      export-dated sources, else ``composite_changed``; candidates = published instruments
      the series left or whose dates now belong to another instrument; the series only, no
      label). The open labels the series left on an instrument it no longer maps to (member
      id, ticker, composite...; not ``cik``) are closed with it, under the series' sources:
      all of them when no other series of that source still maps to the instrument, else
      only those the moved series states this run (the rest may be the other series'). The
      closed ``composite_figi`` row still names the instrument: its id is reused.
    - ``known_at`` = ``load_time`` (``collected`` / ``load_time``) for every row (spec §7.3).

Spans: ``spans_from_fetch_log`` (per series, earliest ``requested_from`` (else
``returned_from``) to the later of the last ``returned_to`` and the last snapshot date when
``snapshots`` are given; never ``requested_to``, the export date for a history file) and
``spans_from_dates`` (first/last ``valid_date`` of any dated rows, e.g. index snapshots, so a
member whose prices stop before its last snapshot stays mapped on that snapshot), merged by
``combine_spans``.

Issuer link (spec §7.1, V0 call): ``link_issuers`` adds ``cik`` rows (source ``sec``) for
instruments whose current ticker is in the SEC ``company_tickers.json`` map. Current: the
instrument's latest mapping window ends within ``max_staleness_days`` of ``as_of`` (required),
and its ticker is the latest ticker row still open at ``as_of``. Dead names get none. A ticker
claimed by two current instruments, or mapped to two CIKs, links nothing (never a guess).
Re-runs are idempotent (an unchanged published cik row is not re-emitted; a changed one is
closed and replaced).
"""

from __future__ import annotations

import csv
import os
import re
import uuid
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from open_data_store.contracts.rules import conform
from open_data_store.contracts.standard.market import INSTRUMENT_IDENTIFIERS, LINK_REASONS
from open_data_store.market.asof import latest
from open_data_store.sources import (
    SOURCE_CODE_PATTERN,
    SourceInfo,
    discover_sources,
    restricted_codes,
)

from market_data.adapters.base import RAW_SYMBOL_SCHEMA

__all__ = [
    "AMBIGUOUS_OPENFIGI_CHANGED",
    "AMBIGUOUS_OPENFIGI_SHARED",
    "AMBIGUOUS_OPENFIGI_STALE",
    "AMBIGUOUS_SEVERAL_COMPOSITES",
    "AMBIGUOUS_WINDOW_CLOSED",
    "CURRENT_MAX_STALENESS_DAYS",
    "EXPORT_DATED_SOURCES",
    "LABELS_LEFT_OUT_SCHEMA",
    "LABEL_ANSWER_NOT_APPLIED",
    "LABEL_COMPOSITE_NOT_MAPPED",
    "LABEL_MANUAL_OVERRIDE",
    "LABEL_OUTSIDE_WINDOWS",
    "LABEL_SEVERAL_WINDOWS",
    "LISTED_NOT_PRICED",
    "MANUAL_MAPPING_ENV",
    "MANUAL_SCHEMA",
    "MANUAL_SOURCE",
    "MAX_OPENFIGI_AGE_DAYS",
    "OBSERVATION_INPUT_SCHEMA",
    "OBSERVED_SCHEMES",
    "OPENFIGI_SOURCE",
    "REPORT_SCHEMA",
    "SEC_SOURCE",
    "SPAN_SCHEMA",
    "SYMBOL_SCHEME",
    "UNRESOLVED_NO_COMPOSITE",
    "UNRESOLVED_NO_SPAN",
    "UNRESOLVED_OUTSIDE_SPAN",
    "UNRESOLVED_UNCOVERED_SPAN",
    "CHANGED_COMPOSITE",
    "CHANGED_EXPORT_COMPOSITE",
    "MappingResult",
    "build_mapping",
    "combine_spans",
    "link_issuers",
    "load_manual_mapping",
    "spans_from_dates",
    "spans_from_fetch_log",
]

MANUAL_MAPPING_ENV = "MOF_MARKET_MANUAL_MAPPING"
MANUAL_SOURCE = "manual"
OPENFIGI_SOURCE = "openfigi"
SEC_SOURCE = "sec"
SYMBOL_SCHEME = "source_symbol"
OBSERVED_SCHEMES = (
    "figi",
    "composite_figi",
    "share_class_figi",
    "isin",
    "ticker",
    "cik",
    "export_member_id",
    "export_name",
)
UNRESOLVED_NO_COMPOSITE = "no_composite_figi"
UNRESOLVED_NO_SPAN = "no_span"
UNRESOLVED_OUTSIDE_SPAN = "outside_span"
UNRESOLVED_UNCOVERED_SPAN = "uncovered_span"
CHANGED_COMPOSITE = "composite_changed"
CHANGED_EXPORT_COMPOSITE = "export_composite_changed"
AMBIGUOUS_SEVERAL_COMPOSITES = "several_composite_figis"
AMBIGUOUS_OPENFIGI_SHARED = "openfigi_shared"
AMBIGUOUS_OPENFIGI_STALE = "openfigi_stale"
AMBIGUOUS_OPENFIGI_CHANGED = "openfigi_changed"
AMBIGUOUS_WINDOW_CLOSED = "window_closed"
LABEL_MANUAL_OVERRIDE = "manual_override"
LABEL_COMPOSITE_NOT_MAPPED = "composite_not_mapped"
LABEL_OUTSIDE_WINDOWS = "outside_windows"
LABEL_SEVERAL_WINDOWS = "several_windows"
LABEL_ANSWER_NOT_APPLIED = "answer_not_applied"
# A non-export symbol observed in a list (a symbols fetch) but never priced: not a series.
LISTED_NOT_PRICED = "listed_not_priced"
# Sources whose identifier observations are dated at an export, not at the dates they describe
# (the history-file adapter's default source code): their spans, not their dates, bound them.
EXPORT_DATED_SOURCES = ("private_history",)
# An OpenFIGI answer describes the present: it maps a series only while the series' span ended
# at most this many days before the load (a dead series' symbol may name someone else now).
MAX_OPENFIGI_AGE_DAYS = 30
# An instrument whose last mapped date is this many days before ``as_of`` still counts as
# current for the issuer link (a weekend, a holiday week, a late fetch).
CURRENT_MAX_STALENESS_DAYS = 7

# Identifier observations as adapters produce them, with their source.
OBSERVATION_INPUT_SCHEMA = pa.schema(
    [*RAW_SYMBOL_SCHEMA, pa.field("source_code", pa.string(), nullable=False)]
)
# Per provider series, the first and last date it has data for (or was requested).
SPAN_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("first_date", pa.date32()),
        pa.field("last_date", pa.date32()),
    ]
)
# Identifier observations left out of the output, counted per series and reason.
LABELS_LEFT_OUT_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("rows", pa.int64(), nullable=False),
    ]
)
REPORT_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("candidates", pa.int32(), nullable=False),
    ]
)
# The reviewed manual mapping: exactly one of instrument_id / composite_figi per row.
MANUAL_SCHEMA = pa.schema(
    [
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_symbol", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string()),
        pa.field("composite_figi", pa.string()),
        pa.field("valid_date", pa.date32(), nullable=False),
        pa.field("valid_to", pa.date32()),
        pa.field("link_reason", pa.string()),
    ]
)

# Labels a series leaves on an instrument (closed there when the series moves away); ``cik``
# rows are the issuer link's (``link_issuers`` closes them when the CIK changes).
_MOVED_LABEL_SCHEMES = tuple(x for x in OBSERVED_SCHEMES if x != "cik")

_FIGI = re.compile(r"BBG[0-9A-Z]{9}")  # always fullmatch
_MOF_ID = re.compile(r"mof:[0-9a-f]{32}")  # always fullmatch
_DAY = timedelta(days=1)
_KEY = INSTRUMENT_IDENTIFIERS.natural_key
_SCHEMA = INSTRUMENT_IDENTIFIERS.schema

Series = tuple[str, str]
Window = tuple[date | None, date | None]  # [start, end); None = unbounded


@dataclass(frozen=True)
class MappingResult:
    identifiers: pa.Table  # INSTRUMENT_IDENTIFIERS, conformed (empty schema'd table when none)
    unresolved: pa.Table  # REPORT_SCHEMA
    ambiguous: pa.Table  # REPORT_SCHEMA
    minted: tuple[str, ...]  # instrument ids created by this run
    labels_left_out: pa.Table  # LABELS_LEFT_OUT_SCHEMA (counts, never a label)
    changed: pa.Table  # REPORT_SCHEMA: mapped series moved to another instrument (operator review)
    # REPORT_SCHEMA: symbols only listed, never priced (not series: neither mapped nor reported)
    listed_not_priced: pa.Table = field(default_factory=REPORT_SCHEMA.empty_table)


@dataclass(frozen=True)
class _Window:
    instrument_id: str
    start: date
    end: date | None  # exclusive; None = open
    composite: str | None  # the candidate that made it (None: manual instrument_id, published)


# -- small helpers -----------------------------------------------------------------------------


def _check_load_time(load_time: datetime) -> datetime:
    if load_time.tzinfo is None or load_time.utcoffset() is None:
        raise ValueError("load_time must be timezone-aware")
    return load_time


def _require(table: pa.Table, columns: Iterable[str], what: str) -> None:
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        raise ValueError(f"{what} lacks column(s) {missing}")


def _overlaps(a: tuple[date | None, date | None], b: tuple[date | None, date | None]) -> bool:
    lo = max(a[0] or date.min, b[0] or date.min)
    hi = min(a[1] or date.max, b[1] or date.max)
    return lo < hi


def _row(
    instrument_id: str,
    scheme: str,
    value: str,
    start: date,
    end: date | None,
    source_code: str,
    load: datetime,
    link_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "instrument_id": instrument_id,
        "id_scheme": scheme,
        "id_value": value,
        "valid_to": end,
        "link_reason": link_reason,
        "known_at_basis": "load_time",
        "valid_date": start,
        "known_at": load,
        "known_at_kind": "collected",
        "source_code": source_code,
        "source_doc_id": None,
    }


def _natural_key(row: Mapping[str, Any]) -> tuple:
    return tuple(row[k] for k in _KEY)


def _later_end(a: date | None, b: date | None) -> date | None:
    return None if a is None or b is None else max(a, b)


def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per natural key; identical keys keep the widest ``valid_to``."""
    out: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        key = _natural_key(r)
        if key in out:
            out[key]["valid_to"] = _later_end(out[key]["valid_to"], r["valid_to"])
        else:
            out[key] = dict(r)
    return list(out.values())


def _identifiers_table(rows: list[dict[str, Any]]) -> pa.Table:
    rows = sorted(
        rows,
        key=lambda r: (
            r["instrument_id"],
            r["id_scheme"],
            r["id_value"],
            r["source_code"],
            r["valid_date"],
        ),
    )
    table = pa.Table.from_pylist(rows, schema=_SCHEMA)
    return conform(INSTRUMENT_IDENTIFIERS, table) if rows else table


def _report(rows: list[tuple[Series, str, int]]) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"source_code": s[0], "source_symbol": s[1], "reason": reason, "candidates": n}
            for s, reason, n in sorted(set(rows))
        ],
        schema=REPORT_SCHEMA,
    )


# -- spans -------------------------------------------------------------------------------------


def _spans_table(spans: Mapping[Series, tuple[date | None, date | None]]) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"source_code": s[0], "source_symbol": s[1], "first_date": f, "last_date": la}
            for s, (f, la) in sorted(spans.items())
        ],
        schema=SPAN_SCHEMA,
    )


def _min(a: date | None, b: date | None) -> date | None:
    return b if a is None else a if b is None else min(a, b)


def _max(a: date | None, b: date | None) -> date | None:
    return b if a is None else a if b is None else max(a, b)


def spans_from_fetch_log(fetch_log: pa.Table, *, snapshots: pa.Table | None = None) -> pa.Table:
    """``SPAN_SCHEMA`` rows from ``fetch_log`` rows: per series, the earliest ``requested_from``
    (else ``returned_from``) to the latest date the data shows: the last ``returned_to`` (last
    price) and the last snapshot date of the series in ``snapshots`` (any table with
    source_code, source_symbol, valid_date), whichever is later. ``requested_to`` is never a
    span end (for a history file it is the export date, which would make a dead member look
    current): a series with neither prices nor snapshots gets no span (the mapping reports it
    ``no_span``)."""
    cols = ("source_code", "source_symbol", "requested_from", "returned_from", "returned_to")
    _require(fetch_log, cols, "fetch_log")
    seen = _span_map(spans_from_dates(snapshots)) if snapshots is not None else {}
    out: dict[Series, tuple[date | None, date | None]] = {}
    for r in fetch_log.select(list(cols)).to_pylist():
        s = (r["source_code"], r["source_symbol"])
        f0, l0 = out.get(s, (None, None))
        out[s] = (_min(f0, r["requested_from"] or r["returned_from"]), _max(l0, r["returned_to"]))
    for s, (first, last) in list(out.items()):
        last = _max(last, seen.get(s, (None, None))[1])
        if last is None:
            del out[s]
        else:
            out[s] = (_min(first, seen.get(s, (None, None))[0]), last)
    return _spans_table(out)


def spans_from_dates(table: pa.Table) -> pa.Table:
    """``SPAN_SCHEMA`` rows: first and last ``valid_date`` per (source_code, source_symbol)."""
    _require(table, ("source_code", "source_symbol", "valid_date"), "dated table")
    if not table.num_rows:
        return _spans_table({})
    agg = table.group_by(["source_code", "source_symbol"]).aggregate(
        [("valid_date", "min"), ("valid_date", "max")]
    )
    return combine_spans(
        pa.table(
            {
                "source_code": agg["source_code"],
                "source_symbol": agg["source_symbol"],
                "first_date": agg["valid_date_min"].cast(pa.date32()),
                "last_date": agg["valid_date_max"].cast(pa.date32()),
            }
        )
    )


def combine_spans(*spans: pa.Table) -> pa.Table:
    """One ``SPAN_SCHEMA`` row per series: the widest span of every input."""
    out: dict[Series, tuple[date | None, date | None]] = {}
    for table in spans:
        for s, (first, last) in _span_map(table).items():
            f0, l0 = out.get(s, (None, None))
            out[s] = (_min(f0, first), _max(l0, last))
    return _spans_table(out)


def _span_map(spans: pa.Table | None) -> dict[Series, tuple[date | None, date | None]]:
    if spans is None:
        return {}
    _require(spans, SPAN_SCHEMA.names, "spans")
    out: dict[Series, tuple[date | None, date | None]] = {}
    for r in spans.select(SPAN_SCHEMA.names).to_pylist():
        s = (r["source_code"], r["source_symbol"])
        first, last = r["first_date"], r["last_date"]
        if first is not None and last is not None and last < first:
            raise ValueError(f"span of a {s[0]!r} series ends before it starts")
        f0, l0 = out.get(s, (None, None))
        out[s] = (_min(f0, first), _max(l0, last))
    return out


# -- manual mapping ----------------------------------------------------------------------------


def _manual_problems(row: Mapping[str, Any]) -> list[str]:
    """What is wrong with one typed manual row (never quoting a value: the file is private)."""
    out = []
    code = row.get("source_code")
    if not isinstance(code, str) or not SOURCE_CODE_PATTERN.fullmatch(code):
        out.append("source_code is not a source code")
    if not row.get("source_symbol"):
        out.append("source_symbol is empty")
    inst, comp = row.get("instrument_id"), row.get("composite_figi")
    if (inst is None) == (comp is None):
        out.append("exactly one of instrument_id and composite_figi must be filled")
    elif inst is not None and not _MOF_ID.fullmatch(inst):
        out.append("instrument_id is not 'mof:' + 32 hex digits")
    elif comp is not None and not _FIGI.fullmatch(comp):
        out.append("composite_figi is not a FIGI (BBG + 9 characters)")
    start, end = row.get("valid_date"), row.get("valid_to")
    if start is None:
        out.append("valid_date is missing")
    elif end is not None and end <= start:
        out.append("valid_to is not after valid_date")
    reason = row.get("link_reason")
    if reason is not None and reason not in LINK_REASONS:
        out.append(f"link_reason must be one of {LINK_REASONS}")
    return out


def _parse_date(value: str | None, column: str, problems: list[str]) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        problems.append(f"{column} is not a date (YYYY-MM-DD)")
        return None


def load_manual_mapping(
    path: str | Path | None = None, *, env: Mapping[str, str] | None = None
) -> pa.Table | None:
    """The reviewed manual mapping (``MANUAL_SCHEMA``) from ``path``, else from the private path
    in ``MOF_MARKET_MANUAL_MAPPING``; None when neither is set.

    CSV (UTF-8, comma, header): ``source_code``, ``source_symbol``, ``instrument_id`` and/or
    ``composite_figi`` (exactly one filled per row), ``valid_date`` (YYYY-MM-DD), optional
    ``valid_to`` (exclusive, empty = open) and ``link_reason`` (one of ``LINK_REASONS``, empty =
    ``manual``). Any problem raises ``ValueError`` naming the data row, never a cell's value."""
    if path is None:
        raw = (os.environ if env is None else env).get(MANUAL_MAPPING_ENV, "").strip()
        if not raw:
            return None
        path = raw
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = [h.strip() for h in next(reader, [])]
        missing = [c for c in ("source_code", "source_symbol", "valid_date") if c not in header]
        if not {"instrument_id", "composite_figi"} & set(header):
            missing.append("instrument_id or composite_figi")
        if missing:
            raise ValueError(f"manual mapping: missing column(s) {missing}")
        rows, problems = [], []
        for i, cells in enumerate(reader, start=1):
            if not any(c.strip() for c in cells):
                continue
            if len(cells) != len(header):
                problems.append(f"data row {i}: {len(cells)} fields, header has {len(header)}")
                continue
            raw_row = {h: (c.strip() or None) for h, c in zip(header, cells, strict=True)}
            row_problems: list[str] = []
            row = {
                "source_code": raw_row.get("source_code"),
                "source_symbol": raw_row.get("source_symbol"),
                "instrument_id": raw_row.get("instrument_id"),
                "composite_figi": raw_row.get("composite_figi"),
                "valid_date": _parse_date(raw_row.get("valid_date"), "valid_date", row_problems),
                "valid_to": _parse_date(raw_row.get("valid_to"), "valid_to", row_problems),
                "link_reason": raw_row.get("link_reason"),
            }
            row_problems += [p for p in _manual_problems(row) if p not in row_problems]
            problems += [f"data row {i}: {p}" for p in row_problems]
            rows.append(row)
    if problems:
        raise ValueError("manual mapping refused: " + "; ".join(problems))
    return pa.Table.from_pylist(rows, schema=MANUAL_SCHEMA)


def _manual_rows(manual: pa.Table) -> list[dict[str, Any]]:
    _require(manual, MANUAL_SCHEMA.names, "manual mapping")
    rows = manual.select(MANUAL_SCHEMA.names).to_pylist()
    problems = [f"row {i}: {p}" for i, r in enumerate(rows, 1) for p in _manual_problems(r)]
    # two rows of one series naming different targets on overlapping dates: the store would
    # raise on every covered date, so refuse it here
    by_series: dict[Series, list[tuple[int, Any, date, date | None]]] = defaultdict(list)
    for i, r in enumerate(rows, 1):
        target = r["instrument_id"] or r["composite_figi"]
        by_series[(r["source_code"], r["source_symbol"])].append(
            (i, target, r["valid_date"], r["valid_to"])
        )
    for entries in by_series.values():
        for k, (i, t1, s1, e1) in enumerate(entries):
            for j, t2, s2, e2 in entries[k + 1 :]:
                if t1 != t2 and _overlaps((s1, e1), (s2, e2)):
                    problems.append(f"rows {i} and {j}: overlapping windows, different targets")
    if problems:
        raise ValueError("manual mapping refused: " + "; ".join(problems))
    return rows


def _manual_from(prior: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The published manual mapping rows still in force, as manual rows."""
    out = []
    for r in prior:
        if r["source_code"] != MANUAL_SOURCE or r["id_scheme"] != SYMBOL_SCHEME:
            continue
        if r["valid_to"] is not None and r["valid_to"] <= r["valid_date"]:
            continue  # closed
        source_code, _, symbol = r["id_value"].partition(":")
        out.append(
            {
                "source_code": source_code,
                "source_symbol": symbol,
                "instrument_id": r["instrument_id"],
                "composite_figi": None,
                "valid_date": r["valid_date"],
                "valid_to": r["valid_to"],
                "link_reason": r["link_reason"],
            }
        )
    return out


# -- ids ---------------------------------------------------------------------------------------


class _Ids:
    """composite FIGI -> instrument_id, from the published rows, minting new ids on demand."""

    def __init__(self, prior: list[dict[str, Any]]) -> None:
        self.by_composite: dict[str, str] = {}
        self.known: set[str] = set()
        self.minted: list[str] = []
        clashes = set()
        for r in prior:
            self.known.add(r["instrument_id"])
            if r["id_scheme"] != "composite_figi":
                continue
            seen = self.by_composite.setdefault(r["id_value"], r["instrument_id"])
            if seen != r["instrument_id"]:
                clashes.add(r["id_value"])
        if clashes:
            raise ValueError(
                f"existing identifiers put {len(clashes)} composite FIGI on several instruments; "
                "fix them (a manual mapping) before building a new mapping"
            )

    def for_composite(self, composite: str) -> str:
        if composite not in self.by_composite:
            new = "mof:" + uuid.uuid4().hex
            self.by_composite[composite] = new
            self.known.add(new)
            self.minted.append(new)
        return self.by_composite[composite]


# -- inputs ------------------------------------------------------------------------------------


def _observation_rows(observations: pa.Table | None) -> list[dict[str, Any]]:
    if observations is None or not observations.num_rows:
        return []
    cols = ["source_code", "source_symbol", "id_scheme", "id_value", "valid_date", "valid_to"]
    _require(observations, cols, "observations")
    rows = observations.select(cols).to_pylist()
    bad_schemes = sorted({r["id_scheme"] for r in rows} - set(OBSERVED_SCHEMES), key=str)
    if bad_schemes:
        raise ValueError(
            f"observation id_scheme(s) {bad_schemes} are not ones the mapping takes "
            f"({OBSERVED_SCHEMES})"
        )
    for r in rows:
        if not (r["source_code"] and r["source_symbol"] and r["id_value"]):
            raise ValueError("an observation lacks its source_code, source_symbol or id_value")
        if r["id_scheme"] == "composite_figi" and not _FIGI.fullmatch(r["id_value"]):
            raise ValueError(f"a {r['source_code']!r} observation holds an invalid composite FIGI")
        if r["valid_date"] and r["valid_to"] and r["valid_to"] <= r["valid_date"]:
            raise ValueError(f"a {r['source_code']!r} observation window ends before it starts")
    return rows


def _figi_rows(figi_results: pa.Table | None) -> list[dict[str, Any]]:
    if figi_results is None or not figi_results.num_rows:
        return []
    cols = ["source_code", "source_symbol", "figi", "composite_figi", "share_class_figi", "ticker"]
    _require(figi_results, cols, "figi_results")
    rows = [r for r in figi_results.select(cols).to_pylist() if r["composite_figi"]]
    for r in rows:
        if not _FIGI.fullmatch(r["composite_figi"]):
            raise ValueError("an OpenFIGI result holds an invalid composite FIGI")
    return rows


# -- the mapping -------------------------------------------------------------------------------


def _gap(windows: Iterable[tuple[date, date | None]], first: date, end: date | None) -> bool:
    """True when the disjoint ``windows`` leave part of [first, end) uncovered."""
    cursor = first
    for start, stop in sorted(windows):
        if start > cursor:
            return True
        cursor = max(cursor, stop or date.max)
    return cursor < (end or date.max)


def _merge(windows: Iterable[Window]) -> list[Window]:
    """One composite's observation windows, overlapping or touching ones merged. An undated
    observation (no start, no end: "seen", nothing more) never widens a dated one: next to a
    window with an end it is a stale sighting (an archived answer from before the end)."""
    windows = list(windows)
    if any(end is not None for _, end in windows):
        windows = [w for w in windows if w != (None, None)]
    out: list[Window] = []
    for start, end in sorted(windows, key=lambda w: w[0] or date.min):
        if out and (out[-1][1] is None or (start or date.min) <= out[-1][1]):
            prev = out[-1]
            out[-1] = (prev[0], None if prev[1] is None or end is None else max(prev[1], end))
        else:
            out.append((start, end))
    return out


def _candidate_windows(
    candidates: Mapping[str, list[Window]],
    span: Window | None,
    *,
    span_rule: bool,
) -> tuple[str, list[tuple[str, date, date | None]], bool]:
    """("ok" | a report reason, [(composite, start, end)] by start, part of the span left
    uncovered). One composite may hold several windows (A, gap, A: the gap stays unmapped;
    A, B, A: three rows, A's two on one instrument)."""
    first, last = span if span else (None, None)
    span_end = last + _DAY if last else None
    if span_rule and len(candidates) == 1:
        # export-dated labels: the span, not the label date, bounds the one candidate
        (composite,) = candidates
        if not span or first is None:
            return UNRESOLVED_NO_SPAN, [], False  # never bounded by the export date
        return "ok", [(composite, first, span_end)], False
    pairs = [(c, w) for c, ws in candidates.items() for w in ws]
    windows: list[tuple[str, date, date | None]] = []
    for composite, (start, end) in pairs:
        start = _max(start, first) if start else first
        end = (min(end, span_end) if span_end else end) if end else span_end
        if start is None:
            reason = AMBIGUOUS_SEVERAL_COMPOSITES if len(candidates) > 1 else UNRESOLVED_NO_SPAN
            return reason, [], False
        if end is not None and end <= start:
            continue  # the source never served this window's dates
        windows.append((composite, start, end))
    if not windows:
        return UNRESOLVED_OUTSIDE_SPAN, [], False
    items = [(w[1], w[2]) for w in windows]
    if any(_overlaps(a, b) for i, a in enumerate(items) for b in items[i + 1 :]):
        return AMBIGUOUS_SEVERAL_COMPOSITES, [], False
    windows.sort(key=lambda w: w[1])
    return "ok", windows, bool(first) and _gap(items, first, span_end)


def _place_label(
    row: Mapping[str, Any], windows: list[_Window], ids: _Ids, span_rule: bool
) -> tuple[_Window | None, str | None]:
    """The window a label belongs to, or why it belongs to none."""
    if row["id_scheme"] == "composite_figi":
        # a composite FIGI only ever names its own instrument: never another company's
        inst = ids.by_composite.get(row["id_value"])
        own = [w for w in windows if w.instrument_id == inst]
        if not own:
            return None, LABEL_COMPOSITE_NOT_MAPPED
        return min(own, key=lambda w: w.start), None
    if span_rule and len({w.instrument_id for w in windows}) == 1:
        # an export-dated label of a one-instrument series is its own, whatever its date
        return min(windows, key=lambda w: w.start), None
    label = (row["valid_date"], row["valid_to"])
    hits = [w for w in windows if _overlaps(label, (w.start, w.end))]
    found = {w.instrument_id for w in hits}
    if not found:
        return None, LABEL_OUTSIDE_WINDOWS
    if len(found) > 1:
        return None, LABEL_SEVERAL_WINDOWS
    return min(hits, key=lambda w: w.start), None


def _published_windows(prior: list[dict[str, Any]], value: str, source_code: str) -> list[_Window]:
    """The open published automatic mapping windows of one series."""
    return [
        _Window(r["instrument_id"], r["valid_date"], r["valid_to"], None)
        for r in prior
        if r["id_scheme"] == SYMBOL_SCHEME
        and r["id_value"] == value
        and r["source_code"] == source_code
        and (r["valid_to"] is None or r["valid_to"] > r["valid_date"])
    ]


def _is_open(row: Mapping[str, Any]) -> bool:
    return row["valid_to"] is None or row["valid_to"] > row["valid_date"]


def _labels_left_behind(
    left: Mapping[Series, set[str]],
    prior: list[dict[str, Any]],
    labels: list[dict[str, Any]],
    obs: list[dict[str, Any]],
    windows_of: Mapping[Series, list[_Window]],
    in_scope: set[str],
    manual_given: bool,
    derived_source: Any,
    load: datetime,
) -> list[dict[str, Any]]:
    """Closing rows (``valid_to = valid_date``) for the open published labels a moved series
    left on the instruments it no longer maps to (``left``). Labels carry a source, not a
    series: under the moved series' sources (its own, and where its OpenFIGI-derived rows
    go), every open label of ``_MOVED_LABEL_SCHEMES`` is closed when no other series of that
    source still maps to the instrument after this run; when one does, only the labels the
    moved series states this run (now on its new instrument) are closed. A label this run
    emits on the instrument is never closed."""
    if not any(left.values()):
        return []
    stays: dict[str, set[Series]] = defaultdict(set)  # instrument -> series mapped after the run
    for s, windows in windows_of.items():
        for w in windows:
            stays[w.instrument_id].add(s)
    for r in prior:
        if r["id_scheme"] != SYMBOL_SCHEME or not _is_open(r) or r["id_value"] in in_scope:
            continue
        if manual_given and r["source_code"] == MANUAL_SOURCE:
            continue
        code, _, symbol = r["id_value"].partition(":")
        stays[r["instrument_id"]].add((code, symbol))
    emitted = {
        (r["instrument_id"], r["source_code"], r["id_scheme"], r["id_value"]) for r in labels
    }
    said: dict[Series, set[tuple[str, str]]] = defaultdict(set)
    for r in obs:
        said[(r["source_code"], r["source_symbol"])].add((r["id_scheme"], r["id_value"]))
    out: dict[tuple, dict[str, Any]] = {}
    for s, gone in left.items():
        for source in {s[0], derived_source(s[0])}:
            for r in prior:
                inst, scheme = r["instrument_id"], r["id_scheme"]
                if inst not in gone or r["source_code"] != source or not _is_open(r):
                    continue
                if scheme not in _MOVED_LABEL_SCHEMES:
                    continue
                if (inst, source, scheme, r["id_value"]) in emitted:
                    continue
                shared = any(source in (o[0], derived_source(o[0])) for o in stays[inst] - {s})
                if shared and (scheme, r["id_value"]) not in said[s]:
                    continue  # may be the label of a series still mapped here
                out[_natural_key(r)] = _row(
                    inst, scheme, r["id_value"], r["valid_date"], r["valid_date"], source, load,
                    r["link_reason"],
                )  # fmt: skip
    return list(out.values())


def build_mapping(
    observations: pa.Table | None,
    figi_results: pa.Table | None = None,
    *,
    existing: pa.Table | None = None,
    load_time: datetime,
    spans: pa.Table | None = None,
    manual: pa.Table | None = None,
    export_dated_sources: Collection[str] = EXPORT_DATED_SOURCES,
    max_openfigi_age_days: int = MAX_OPENFIGI_AGE_DAYS,
    restricted: frozenset[str] | None = None,
    registered: frozenset[str] | None = None,
) -> MappingResult:
    """``INSTRUMENT_IDENTIFIERS`` rows (identifier observations and ``source_symbol`` mapping
    rows) plus the unresolved and ambiguous series and the labels left out. See the module
    docstring for the rules.

    ``observations``: ``OBSERVATION_INPUT_SCHEMA`` rows (extra columns ignored);
    ``figi_results``: ``openfigi.FIGI_RESULT_SCHEMA`` rows; ``existing``: the published
    ``INSTRUMENT_IDENTIFIERS`` rows (read latest here; ``load_time`` must be after every one of
    their ``known_at``); ``spans``: ``SPAN_SCHEMA``; ``manual``: ``MANUAL_SCHEMA``
    (``load_manual_mapping``); ``export_dated_sources``: sources whose labels are dated at an
    export (the span rule); ``max_openfigi_age_days``: how long after a series' span ends an
    OpenFIGI (present-time) answer may still map it; ``restricted`` / ``registered``:
    restricted and registered source codes (default: the registry's)."""
    load = _check_load_time(load_time)
    if max_openfigi_age_days < 0:
        raise ValueError("max_openfigi_age_days must be >= 0")
    load_date = load.astimezone(UTC).date()
    registry = discover_sources() if registered is None else None
    registered = frozenset(registry) if registry is not None else frozenset(registered)
    restricted = restricted_codes(registry) if restricted is None else frozenset(restricted)
    export_dated = frozenset(export_dated_sources)
    obs = _observation_rows(observations)
    figi = _figi_rows(figi_results)
    span_of = _span_map(spans)
    prior: list[dict[str, Any]] = []
    if existing is not None and existing.num_rows:
        newest = pc.max(existing["known_at"]).as_py()
        if newest is not None and load <= newest:
            raise ValueError(
                "load_time must be after every known_at of the existing identifiers "
                "(a later run supersedes an earlier one)"
            )
        prior = latest(existing, _KEY).to_pylist()
    ids = _Ids(prior)
    manual_given = manual is not None
    manual_rows = _manual_rows(manual) if manual_given else _manual_from(prior)
    manual_series = {(r["source_code"], r["source_symbol"]) for r in manual_rows}

    observed: dict[Series, dict[str, list[Window]]] = defaultdict(lambda: defaultdict(list))
    for r in obs:
        if r["id_scheme"] == "composite_figi":
            seen = observed[(r["source_code"], r["source_symbol"])][r["id_value"]]
            seen.append((r["valid_date"], r["valid_to"]))
    for cands in observed.values():
        for c in cands:
            cands[c] = _merge(cands[c])
    from_figi: dict[Series, set[str]] = defaultdict(set)
    for r in figi:
        from_figi[(r["source_code"], r["source_symbol"])].add(r["composite_figi"])
    # who claims a composite, per source: observed composites, OpenFIGI-only answers, the
    # published open mapping rows and the manual targets (across runs, not only this batch)
    composites_of: dict[str, set[str]] = defaultdict(set)
    for c, inst in ids.by_composite.items():
        composites_of[inst].add(c)
    claims: dict[tuple[str, str], set[Series]] = defaultdict(set)
    for s, cands in observed.items():
        for c in cands:
            claims[(s[0], c)].add(s)
    for s, cands in from_figi.items():
        if s not in observed and s not in manual_series:
            for c in cands:
                claims[(s[0], c)].add(s)
    for r in prior:
        if r["id_scheme"] == SYMBOL_SCHEME and r["source_code"] != MANUAL_SOURCE:
            if r["valid_to"] is None or r["valid_to"] > r["valid_date"]:
                src, _, sym = r["id_value"].partition(":")
                for c in composites_of[r["instrument_id"]]:
                    claims[(src, c)].add((src, sym))
    for m in manual_rows:
        targets = (
            {m["composite_figi"]} if m["composite_figi"] else composites_of[m["instrument_id"]]
        )
        for c in targets:
            claims[(m["source_code"], c)].add((m["source_code"], m["source_symbol"]))

    scope = (
        {(r["source_code"], r["source_symbol"]) for r in obs}
        | set(from_figi)
        | set(span_of)
        | manual_series
    )
    unresolved: list[tuple[Series, str, int]] = []
    ambiguous: list[tuple[Series, str, int]] = []
    windows_of: dict[Series, list[_Window]] = defaultdict(list)
    via_openfigi: set[Series] = set()
    kept: set[str] = set()  # id_values whose published windows this run keeps as they are
    mapping: list[dict[str, Any]] = []

    changed: list[tuple[Series, str, int]] = []
    listed: list[tuple[Series, str, int]] = []
    applied: dict[Series, _Window] = {}  # present-time answers applied this run, per series
    extended: set[tuple] = set()  # natural keys of published rows whose end this run extends
    left: dict[Series, set[str]] = {}  # instruments a moved series no longer maps to
    unapplied: set[Series] = set()  # series kept without this run's answer applied

    def names_kept(s: Series, published: list[_Window]) -> bool:
        """True when the series observes a composite this run and each one names an
        instrument of its published windows (the observations describe the kept instrument)."""
        kept_ids = {w.instrument_id for w in published}
        return s in observed and all(ids.by_composite.get(c) in kept_ids for c in observed[s])

    def keep(
        s: Series,
        value: str,
        published: list[_Window],
        candidates: int = 0,
        *,
        answer_applied: bool = False,
    ) -> None:
        """Keep the published windows as they are; report the part of this run's span they
        leave uncovered. Unless ``answer_applied``, this run's observations of the series say
        nothing about the kept instrument: none of its labels is attached."""
        windows_of[s] = published
        kept.add(value)
        if not answer_applied:
            unapplied.add(s)
        first, last = span_of.get(s, (None, None))
        if s in span_of and first is None:
            unresolved.append((s, UNRESOLVED_NO_SPAN, candidates))  # coverage unknown
        elif first is not None:
            items = [(w.start, w.end) for w in published]
            if _gap(items, first, last + _DAY if last else None):
                unresolved.append((s, UNRESOLVED_UNCOVERED_SPAN, candidates))

    def dated_cap(s: Series, w: _Window) -> date | None:
        """The dated end the source itself gave the instrument of the published window ``w``:
        the latest ``valid_to`` of the published composite labels of the series' source on
        that instrument around the window; None when one of them is open or none is dated
        (the window was closed only because the data stopped)."""
        ends = [
            r["valid_to"]
            for r in prior
            if r["id_scheme"] == "composite_figi"
            and r["instrument_id"] == w.instrument_id
            and r["source_code"] == s[0]
            and (r["valid_to"] is None or r["valid_to"] > max(r["valid_date"], w.start))
            and (w.end is None or r["valid_date"] <= w.end)
        ]
        return None if not ends or None in ends else max(ends)

    for s in sorted(scope - manual_series):
        value = f"{s[0]}:{s[1]}"
        published = _published_windows(prior, value, s[0])
        if (
            spans is not None
            and s not in span_of
            and not published
            and s[0] not in export_dated
            and s not in from_figi
        ):
            # listed (a symbols fetch) but never priced: not a series, nothing to map
            listed.append((s, LISTED_NOT_PRICED, len(observed.get(s, ()))))
            continue
        if s not in span_of and published:
            # no span this run: never rewrite a published (bounded) window open-ended
            keep(s, value, published, answer_applied=names_kept(s, published))
            continue
        if s in observed:
            candidates = observed[s]
        elif s in from_figi:
            candidates = {c: [(None, None)] for c in from_figi[s]}
            via_openfigi.add(s)
        elif published:
            keep(s, value, published)  # no evidence this run: the published mapping stands
            continue
        else:
            unresolved.append((s, UNRESOLVED_NO_COMPOSITE, 0))
            continue
        present_time = len(candidates) == 1 and (
            s in via_openfigi
            or (s[0] not in export_dated and next(iter(candidates.values())) == [(None, None)])
        )
        if present_time:
            # an undated answer describes today: never applied to someone else's series or an
            # old series, and never re-dating, closing or replacing a published window
            (c,) = candidates
            last = span_of.get(s, (None, None))[1]
            doubt = None
            if claims[(s[0], c)] - {s}:
                doubt = AMBIGUOUS_OPENFIGI_SHARED
            elif last is not None and (load_date - last).days > max_openfigi_age_days:
                doubt = AMBIGUOUS_OPENFIGI_STALE
            if published:
                newest = max(published, key=lambda w: w.start)
                if ids.by_composite.get(c) != newest.instrument_id:
                    doubt = AMBIGUOUS_OPENFIGI_CHANGED
                if doubt:
                    # reported, never withdrawn: only a manual row closes a published mapping
                    ambiguous.append((s, doubt, 1))
                    keep(s, value, published, 1)
                    continue
                # the answer confirms the newest published window: it may only extend the end
                # of a window the data closed (never one the source closed at a date, never
                # without a span end, never past a dated end)
                end = last + _DAY if last else None
                grown = newest.end is not None and end is not None and end > newest.end
                cap = dated_cap(s, newest) if grown else None
                if cap is not None:
                    ambiguous.append((s, AMBIGUOUS_WINDOW_CLOSED, 1))
                    end = min(end, cap)
                    if end <= newest.end:
                        keep(s, value, published, 1)
                        continue
                if grown:
                    reason = next(
                        r["link_reason"]
                        for r in prior
                        if r["id_scheme"] == SYMBOL_SCHEME
                        and r["id_value"] == value
                        and r["source_code"] == s[0]
                        and r["valid_date"] == newest.start
                        and r["instrument_id"] == newest.instrument_id
                    )
                    mapping.append(
                        _row(newest.instrument_id, SYMBOL_SCHEME, value, newest.start, end,
                             s[0], load, reason)
                    )  # fmt: skip
                    extended.add(_natural_key(mapping[-1]))
                    newest = _Window(newest.instrument_id, newest.start, end, c)
                else:
                    newest = _Window(newest.instrument_id, newest.start, newest.end, c)
                keep(
                    s,
                    value,
                    [w for w in published if w.start != newest.start] + [newest],
                    1,
                    answer_applied=True,
                )
                applied[s] = newest
                continue
            if doubt:
                ambiguous.append((s, doubt, 1))
                continue
        status, windows, uncovered = _candidate_windows(
            candidates, span_of.get(s), span_rule=s[0] in export_dated
        )
        if status != "ok":
            several = status == AMBIGUOUS_SEVERAL_COMPOSITES
            (ambiguous if several else unresolved).append((s, status, len(candidates)))
            if published:
                # reported, never withdrawn: only a manual row closes a published mapping
                keep(s, value, published, len(candidates))
            continue
        if uncovered:
            unresolved.append((s, UNRESOLVED_UNCOVERED_SPAN, len(candidates)))
        if published:
            new = [(ids.by_composite.get(c), start, end) for c, start, end in windows]
            gone = {w.instrument_id for w in published} - {inst for inst, _, _ in new}
            moved = gone | {
                w.instrument_id
                for w in published
                for inst, start, end in new
                if inst != w.instrument_id and _overlaps((w.start, w.end), (start, end))
            }
            if moved:
                # dated data is the source of truth: applied, but every move is shown
                export = s[0] in export_dated
                reason = CHANGED_EXPORT_COMPOSITE if export else CHANGED_COMPOSITE
                changed.append((s, reason, len(moved)))
                left[s] = gone
        for composite, start, end in windows:
            inst = ids.for_composite(composite)
            windows_of[s].append(_Window(inst, start, end, composite))
            mapping.append(_row(inst, SYMBOL_SCHEME, value, start, end, s[0], load))
        if present_time:
            applied[s] = windows_of[s][0]

    # two or more symbols of one source on one instrument in this batch: a venue change (or a
    # rename); a symbol added in a later run alone is not relabelled
    symbols_of: dict[tuple[str, str], set[str]] = defaultdict(set)
    for r in mapping:
        symbols_of[(r["source_code"], r["instrument_id"])].add(r["id_value"])
    for r in mapping:
        if _natural_key(r) in extended:
            continue  # a published row whose end grew keeps its published link_reason
        if len(symbols_of[(r["source_code"], r["instrument_id"])]) > 1:
            r["link_reason"] = "venue_change"

    labels: list[dict[str, Any]] = []
    for i, m in enumerate(manual_rows, 1):
        if m["instrument_id"] is not None:
            if m["instrument_id"] not in ids.known:
                raise ValueError(
                    f"manual mapping row {i}: unknown instrument_id (neither published nor "
                    "minted in this run)"
                )
            inst = m["instrument_id"]
        else:
            inst = ids.for_composite(m["composite_figi"])
            labels.append(
                _row(inst, "composite_figi", m["composite_figi"], m["valid_date"], None,
                     MANUAL_SOURCE, load)
            )  # fmt: skip
        s = (m["source_code"], m["source_symbol"])
        windows_of[s].append(_Window(inst, m["valid_date"], m["valid_to"], m["composite_figi"]))
        mapping.append(
            _row(inst, SYMBOL_SCHEME, f"{s[0]}:{s[1]}", m["valid_date"], m["valid_to"],
                 MANUAL_SOURCE, load, m["link_reason"] or "manual")
        )  # fmt: skip

    def derived_source(code: str) -> str:
        """Where the OpenFIGI-derived rows of a series of source ``code`` go."""
        return code if code in restricted or code not in registered else OPENFIGI_SOURCE

    left_out: dict[tuple[Series, str], int] = defaultdict(int)
    published_labels = {
        (r["instrument_id"], r["source_code"], r["id_value"])
        for r in prior
        if r["id_scheme"] == "composite_figi" and _is_open(r)
    }
    for r in obs:
        s = (r["source_code"], r["source_symbol"])
        windows = windows_of.get(s)
        if not windows:
            continue  # the series itself is reported unresolved or ambiguous
        if s in unapplied:
            # kept as published, this run's answer not applied: its labels may describe
            # someone else (a recycled symbol), so none is attached
            left_out[(s, LABEL_ANSWER_NOT_APPLIED)] += 1
            continue
        if s in manual_series:
            # the reviewed mapping overrides what the source says: its labels may describe
            # another instrument, so none is attached
            left_out[(s, LABEL_MANUAL_OVERRIDE)] += 1
            continue
        span_rule = s[0] in export_dated
        w, why = _place_label(r, windows, ids, span_rule)
        if w is None:
            left_out[(s, why)] += 1
            continue
        if (
            r["id_scheme"] == "composite_figi"
            and not span_rule
            and r["valid_date"] is None
            and r["valid_to"] is None
            and (w.instrument_id, r["source_code"], r["id_value"]) in published_labels
        ):
            # an undated (present-time) composite never rewrites a published label: re-emitted
            # open-ended it would erase the dated end the next run's window cap reads
            continue
        labels.append(
            _row(w.instrument_id, r["id_scheme"], r["id_value"], r["valid_date"] or w.start,
                 r["valid_to"], r["source_code"], load)
        )  # fmt: skip
    published_open = {
        (r["instrument_id"], r["id_scheme"], r["id_value"], r["source_code"])
        for r in prior
        if r["valid_to"] is None or r["valid_to"] > r["valid_date"]
    }
    for r in figi:
        s = (r["source_code"], r["source_symbol"])
        w = applied.get(s)
        if s not in via_openfigi or w is None or r["composite_figi"] != w.composite:
            continue
        # present-time answers: dated at the load, under the series' source when restricted
        # or unregistered (fail closed), emitted only when not already published open
        source = derived_source(s[0])
        for scheme in ("composite_figi", "figi", "share_class_figi", "ticker"):
            if r[scheme] and (w.instrument_id, scheme, r[scheme], source) not in published_open:
                labels.append(
                    _row(w.instrument_id, scheme, r[scheme], load_date, None, source, load)
                )

    in_scope = {f"{s[0]}:{s[1]}" for s in scope}
    closing = _labels_left_behind(
        left, prior, labels, obs, windows_of, in_scope, manual_given, derived_source, load
    )

    # close published mapping rows this run supersedes
    emitted = {_natural_key(r) for r in mapping}
    for r in prior:
        if r["id_scheme"] != SYMBOL_SCHEME or _natural_key(r) in emitted:
            continue
        if r["valid_to"] is not None and r["valid_to"] <= r["valid_date"]:
            continue  # already closed
        if r["id_value"] in kept and r["source_code"] != MANUAL_SOURCE:
            continue
        if r["id_value"] in in_scope or (manual_given and r["source_code"] == MANUAL_SOURCE):
            mapping.append(
                _row(r["instrument_id"], SYMBOL_SCHEME, r["id_value"], r["valid_date"],
                     r["valid_date"], r["source_code"], load, r["link_reason"])
            )  # fmt: skip

    return MappingResult(
        identifiers=_identifiers_table(mapping + _dedupe(labels) + closing),
        unresolved=_report(unresolved),
        ambiguous=_report(ambiguous),
        minted=tuple(ids.minted),
        changed=_report(changed),
        listed_not_priced=_report(listed),
        labels_left_out=pa.Table.from_pylist(
            [
                {"source_code": s[0], "source_symbol": s[1], "reason": why, "rows": n}
                for (s, why), n in sorted(left_out.items())
            ],
            schema=LABELS_LEFT_OUT_SCHEMA,
        ),
    )


# -- issuer link -------------------------------------------------------------------------------


def _norm_ticker(value: str) -> str:
    return value.strip().upper().replace(".", "-").replace("/", "-")


def link_issuers(
    identifiers: pa.Table,
    sec_tickers: pa.Table,
    *,
    load_time: datetime,
    as_of: date,
    max_staleness_days: int = CURRENT_MAX_STALENESS_DAYS,
    sources: Mapping[str, SourceInfo] | None = None,
) -> pa.Table:
    """``identifiers`` plus a ``cik`` row (source ``sec``) for each current instrument whose
    current ticker is in ``sec_tickers`` (the SEC ``company_tickers.json`` map: columns ``cik``,
    ``ticker``; others ignored). See the module docstring for "current" (``as_of`` is required:
    the date the SEC map describes, normally the load date). Tickers compare
    case-insensitively with ``.`` and ``/`` read as ``-`` (``BRK.B`` = ``BRK-B``). The CIK is
    written as 10 digits; ``valid_date`` is the load date (the map is a present-time
    observation), open-ended; ``known_at`` = ``load_time`` (``collected`` / ``load_time``).

    Idempotent: an instrument whose published ``sec`` cik row (read latest) already holds the
    CIK gets nothing new; open cik rows holding another CIK, whatever their source (``sec``,
    one of the instrument's series sources, or a source its series no longer have), are
    closed (re-emitted with ``valid_to = valid_date`` under their own source) and the new one
    added. Feeding the output back in at the same ``load_time`` adds nothing; closing a row
    published at this very ``load_time`` raises.

    Closed (empty-window) mapping rows never count as series of an instrument. Source of the
    row: ``sec`` when the instrument has at least one live series (an open
    ``source_symbol`` window reaching ``as_of - max_staleness_days``) of an open source (a dead
    open series never publishes a link found through a private one); otherwise (known only
    through restricted series, e.g. a private token) the most restrictive of its series'
    sources, so the link never makes a private-only instrument visible to a default read.
    Most restrictive: no_public_display, then no_ml_training, then the code (alphabetical).
    ``sources`` is the registry (default: ``discover_sources()``); an unregistered code counts
    as restricted."""
    load = _check_load_time(load_time)
    if not isinstance(as_of, date) or isinstance(as_of, datetime):
        raise TypeError("as_of must be a date")
    if max_staleness_days < 0:
        raise ValueError("max_staleness_days must be >= 0")
    _require(sec_tickers, ("cik", "ticker"), "sec_tickers")
    rows = latest(identifiers, _KEY).to_pylist() if identifiers.num_rows else []
    registry = discover_sources() if sources is None else dict(sources)
    floor = as_of - timedelta(days=max_staleness_days)
    # series sources per instrument, from open (non-empty) windows; "live" ones reach floor
    series_sources: dict[str, set[str]] = defaultdict(set)
    live_sources: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r["id_scheme"] != SYMBOL_SCHEME:
            continue
        if r["valid_to"] is not None and r["valid_to"] <= r["valid_date"]:
            continue  # closed: says nothing about the instrument any more
        code = r["id_value"].partition(":")[0]
        series_sources[r["instrument_id"]].add(code)
        if r["valid_to"] is None or r["valid_to"] - _DAY >= floor:
            live_sources[r["instrument_id"]].add(code)

    def link_source(inst: str) -> str:
        if any(c in registry and registry[c].access == "open" for c in live_sources[inst]):
            return SEC_SOURCE  # an open series trades it now
        codes = series_sources.get(inst, set())
        if not codes:
            return SEC_SOURCE  # pragma: no cover - a current instrument has a series

        def strictness(code: str) -> tuple:
            info = registry.get(code)
            if info is None:
                return (False, False, code)
            return (not info.no_public_display, not info.no_ml_training, code)

        return min(codes, key=strictness)

    last: dict[str, date] = {}  # instrument -> last mapped date (date.max while open)
    for r in rows:
        if r["id_scheme"] != SYMBOL_SCHEME:
            continue
        if r["valid_to"] is not None and r["valid_to"] <= r["valid_date"]:
            continue
        end = date.max if r["valid_to"] is None else r["valid_to"] - _DAY
        last[r["instrument_id"]] = max(last.get(r["instrument_id"], date.min), end)
    current = {i for i, d in last.items() if d >= floor}

    latest_ticker: dict[str, tuple[date, set[str]]] = {}
    for r in rows:
        if r["id_scheme"] != "ticker" or r["instrument_id"] not in current:
            continue
        if r["valid_to"] is not None and r["valid_to"] <= as_of:
            continue  # closed before as_of
        inst, value = r["instrument_id"], _norm_ticker(r["id_value"])
        start, values = latest_ticker.get(inst, (date.min, set()))
        if r["valid_date"] > start:
            latest_ticker[inst] = (r["valid_date"], {value})
        elif r["valid_date"] == start:
            values.add(value)
            latest_ticker[inst] = (start, values)
    claims: dict[str, set[str]] = defaultdict(set)
    for inst, (_, values) in latest_ticker.items():
        if len(values) == 1:
            claims[next(iter(values))].add(inst)

    ciks: dict[str, set[str]] = defaultdict(set)
    for cik, ticker in zip(
        sec_tickers["cik"].to_pylist(), sec_tickers["ticker"].to_pylist(), strict=True
    ):
        if cik is None or not ticker:
            continue
        try:
            number = int(str(cik).strip())
        except ValueError:
            raise ValueError("sec_tickers holds a CIK that is not a number") from None
        ciks[_norm_ticker(ticker)].add(f"{number:010d}")

    # the open cik rows of each instrument, whatever their source: an earlier link may sit
    # under sec, under one of the instrument's series sources or under a source the
    # instrument has since lost (the link's source moves as series come and go)
    published: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        if r["id_scheme"] == "cik" and _is_open(r):
            published[r["instrument_id"]].append(r)

    load_date = load.astimezone(UTC).date()
    new: list[dict[str, Any]] = []
    for t, insts in sorted(claims.items()):
        if len(insts) != 1 or len(ciks.get(t, ())) != 1:
            continue
        inst, cik = next(iter(insts)), next(iter(ciks[t]))
        source = link_source(inst)
        old = published.get(inst, [])
        same = [
            r
            for r in old
            if r["id_value"] == cik
            and (r["source_code"] == SEC_SOURCE or r["source_code"] in series_sources[inst])
        ]
        if any(r["source_code"] == source for r in same) or (same and source != SEC_SOURCE):
            continue  # unchanged (a restricted link never moves for a mere source change)
        for r in old:
            if r["id_value"] == cik:
                continue
            if r["known_at"] >= load:
                raise ValueError("load_time must be after the published cik row it would close")
            new.append(
                _row(inst, "cik", r["id_value"], r["valid_date"], r["valid_date"],
                     r["source_code"], load)
            )  # fmt: skip
        new.append(_row(inst, "cik", cik, load_date, None, source, load))
    base = identifiers.select(_SCHEMA.names).cast(_SCHEMA)
    table = pa.concat_tables([base, pa.Table.from_pylist(new, schema=_SCHEMA)])
    return conform(INSTRUMENT_IDENTIFIERS, table) if table.num_rows else table

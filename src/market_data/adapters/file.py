"""History-file adapter: loads an export in the public history-file format (spec §9.2).

The history-file format is the door for anyone with their own data, and the path for a private
one-shot export. It is not a network adapter: it has no ``fetch``/``parse`` and does not implement
the ``Adapter`` protocol, so it is not declared under the ``market_data.adapters`` entry point; a
caller builds ``FileHistory(path)`` and publishes the tables it returns.

Format (a directory)
    ``manifest.json`` (an object) or ``manifest.csv`` (header ``key,value``), required keys
    ``export_time`` (ISO 8601 with offset), ``adjustment`` (must be ``none``: prices raw,
    unadjusted; anything else refuses the whole load), ``dates_timezone``, ``dividends``
    (``gross`` or ``net``), ``source_description``; optional ``fields``, ``notes``.
    Then ``securities`` (required), ``prices_daily``, ``index_snapshots`` (at least one of them),
    ``corporate_actions`` and ``total_return_index`` (optional), each as Parquet (preferred) or
    CSV (UTF-8, comma, header), possibly split into parts (``prices_daily_part001.parquet``, any
    ``<name>*.parquet|csv``, read in file-name order; one format per table). Columns in
    ``FILE_COLUMNS``; dates ``YYYY-MM-DD``; ``.`` decimal; empty = unknown.

Strict input (``validate()`` lists every problem; each table method raises ``HistoryFileError``
while there is one): a tab-separated file or an unnamed row-number column is refused (re-export as
comma CSV or Parquet without the row number); ``-``, ``N/A``, ``NA``, ``#N/A`` in any cell and
NaN or infinite numbers are refused with the file, column and first row; unknown files or
columns, missing required columns or values, duplicate keys and member ids absent from
``securities`` are refused. Problems never quote a member id or a name.

Look-ahead guard (spec §9.3)
    Every dated row (prices, actions, vendor total return, snapshots, fetch log) is keyed on
    ``token(source_code, member_id, salt)``; labels (member id, name, FIGIs, ISIN, last ticker)
    appear only in ``observations(load_time)``, known at load time.

``known_at`` (spec §7.3)
    Prices and vendor total return: the session close in UTC of the row's date on the member's
    exchange calendar (``published`` / ``session_close``). A date that is not a session is
    dropped (``non_session``); a session closing after ``export_time`` is dropped
    (``after_export``: a partial bar). Snapshots: snapshot date 23:59:59.999999 UTC
    (``published`` / ``snapshot_date``), capped at ``export_time`` (then ``collected`` /
    ``fetch_time``). Actions: the provider builder's rule (``market_data._calendars``), capped at
    ``export_time``. Observations and fetch log: ``load_time`` (``collected``).

Exchange calendars
    A member's exchange code is ``securities.exchange_code``, else the second word of its
    ``member_id`` (``AAA UW Equity`` -> ``UW``); ``exchange_mics`` maps codes to calendar MICs
    (default ``DEFAULT_EXCHANGE_MICS``: US codes -> XNYS). Rows of a member whose code is not
    mapped are kept out of every dated table (``unmapped_exchange``) and counted per code.
    Snapshots keep ``snapshot_size`` = every filed member of the (index, date), so a member kept
    out shows as a gap.

Load report: ``FileHistory.report`` (counts only, no label) is filled as tables are built.
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from importlib import resources
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq
from open_data_store.contracts.standard.market import (
    ACTION_TYPES,
    CORPORATE_ACTIONS,
    FETCH_LOG,
    INDEX_SNAPSHOTS,
    PRICES,
    VENDOR_TOTAL_RETURN,
)
from open_data_store.market.asof import latest, sessions

from market_data._calendars import (
    NEXT_SESSION_MARGIN,
    action_known_at,
    actions_calendar,
    check_margin,
)
from market_data.adapters.base import RAW_SYMBOL_SCHEMA
from market_data.tokens import MIN_SALT_BYTES, salt_from_env, token

__all__ = [
    "DEFAULT_EXCHANGE_MICS",
    "DROP_AFTER_EXPORT",
    "DROP_NON_SESSION",
    "DROP_UNKNOWN_ACTION_TYPE",
    "DROP_UNMAPPED_EXCHANGE",
    "FILE_COLUMNS",
    "OBSERVATION_SCHEMA",
    "SENTINELS",
    "FileHistory",
    "HistoryFileError",
    "LoadReport",
    "Manifest",
    "default_action_types",
]

# V0 default: US exchange codes -> NYSE calendar (Europe after the V0).
DEFAULT_EXCHANGE_MICS: Mapping[str, str] = MappingProxyType(
    {code: "XNYS" for code in ("UN", "UW", "UQ", "UA", "UP", "UF", "UR", "UV", "US")}
)
# Cell values that mean "unknown" in some exports; the format says empty, so they are refused.
SENTINELS = ("-", "N/A", "NA", "#N/A")
DROP_NON_SESSION = "non_session"
DROP_AFTER_EXPORT = "after_export"
DROP_UNMAPPED_EXCHANGE = "unmapped_exchange"
DROP_UNKNOWN_ACTION_TYPE = "unknown_action_type"
NO_EXCHANGE_CODE = "<none>"

_ACTION_TYPES_FILE = "file_action_types.json"
_MANIFEST_REQUIRED = (
    "export_time",
    "adjustment",
    "dates_timezone",
    "dividends",
    "source_description",
)
_MANIFEST_OPTIONAL = ("fields", "notes")
_DIVIDENDS = ("gross", "net")
_ROW_NUMBER = re.compile(r"^(|Unnamed: ?\d+|__index_level_\d+__)$")
_EOD = timedelta(days=1) - timedelta(microseconds=1)
_TS = pa.timestamp("us", tz="UTC")
_MEMBER = pa.dictionary(pa.int32(), pa.string())

# Identifier observations: RAW_SYMBOL_SCHEMA, then the bitemporal columns.
OBSERVATION_SCHEMA = pa.schema(
    [
        *RAW_SYMBOL_SCHEMA,
        pa.field("known_at_basis", pa.string(), nullable=False),
        pa.field("known_at", _TS, nullable=False),
        pa.field("known_at_kind", pa.string(), nullable=False),
        pa.field("source_code", pa.string(), nullable=False),
        pa.field("source_doc_id", pa.string()),
    ]
)
# securities column -> observation id_scheme
_OBSERVED = (
    ("member_id", "export_member_id"),
    ("name", "export_name"),
    ("figi", "figi"),
    ("composite_figi", "composite_figi"),
    ("share_class_figi", "share_class_figi"),
    ("isin", "isin"),
    ("last_ticker", "ticker"),
)

_S, _D, _F, _I = pa.string(), pa.date32(), pa.float64(), pa.int64()


@dataclass(frozen=True)
class _Spec:
    columns: Mapping[str, pa.DataType]
    required: tuple[str, ...]  # columns that must be present
    non_null: tuple[str, ...]  # columns that must have a value in every row
    key: tuple[str, ...] = ()  # duplicates refused
    dictionary: tuple[str, ...] = ()  # read dictionary-encoded (large files)


_SPECS: Mapping[str, _Spec] = {
    "securities": _Spec(
        {
            "member_id": _S,
            "name": _S,
            "figi": _S,
            "composite_figi": _S,
            "share_class_figi": _S,
            "isin": _S,
            "country": _S,
            "exchange_code": _S,
            "security_type": _S,
            "price_currency": _S,
            "status": _S,
            "delisting_date": _D,
            "delisting_reason": _S,
            "last_ticker": _S,
        },
        ("member_id", "name"),
        ("member_id", "name"),
        ("member_id",),
    ),
    "index_snapshots": _Spec(
        {"date": _D, "index": _S, "member_id": _S, "weight": _F},
        ("date", "index", "member_id"),
        ("date", "index", "member_id"),
        ("index", "date", "member_id"),
        ("member_id",),
    ),
    "prices_daily": _Spec(
        {
            "member_id": _S,
            "date": _D,
            "close": _F,
            "volume": _I,
            "currency": _S,
            "open": _F,
            "high": _F,
            "low": _F,
        },
        ("member_id", "date", "close", "volume", "currency"),
        ("member_id", "date", "close", "currency"),
        ("member_id", "date"),
        ("member_id", "currency"),
    ),
    "corporate_actions": _Spec(
        {
            "member_id": _S,
            "action_type": _S,
            "source_action_type": _S,
            "announce_date": _D,
            "ex_date": _D,
            "record_date": _D,
            "pay_date": _D,
            "amount": _F,
            "currency": _S,
            "ratio_old": _F,
            "ratio_new": _F,
            "spinoff_member_id": _S,
        },
        ("member_id", "source_action_type", "ex_date"),
        ("member_id", "source_action_type", "ex_date"),
    ),
    "total_return_index": _Spec(
        {"member_id": _S, "date": _D, "tr_index": _F},
        ("member_id", "date", "tr_index"),
        ("member_id", "date", "tr_index"),
        ("member_id", "date"),
        ("member_id",),
    ),
}
FILE_COLUMNS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {name: tuple(spec.columns) for name, spec in _SPECS.items()}
)
_REQUIRED_FILES = ("securities",)
_ONE_OF_FILES = ("prices_daily", "index_snapshots")


class HistoryFileError(ValueError):
    """The export does not follow the history-file format; ``problems`` lists every issue."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        super().__init__("history file refused:\n- " + "\n- ".join(self.problems))


@dataclass(frozen=True)
class Manifest:
    export_time: datetime
    adjustment: str
    dates_timezone: str
    dividends: str
    source_description: str
    fields: str | None = None
    notes: str | None = None

    @property
    def export_date(self) -> date:
        """The export's calendar date, in the offset the export time was written with."""
        return self.export_time.date()


@dataclass
class LoadReport:
    """Counts only (never a label): rows kept out per table and reason, per unmapped exchange
    code, and per unknown ``source_action_type``."""

    dropped: dict[str, dict[str, int]] = field(default_factory=dict)
    unmapped_exchanges: dict[str, dict[str, int]] = field(default_factory=dict)
    unknown_action_types: dict[str, int] = field(default_factory=dict)


def _norm_type(label: str) -> str:
    return " ".join(label.split()).casefold()


def default_action_types() -> dict[str, str]:
    """``source_action_type`` (case-insensitive) -> ``action_type``, from the package data file."""
    raw = resources.files("market_data.adapters").joinpath(_ACTION_TYPES_FILE).read_text()
    return dict(json.loads(raw))


# --- reading -----------------------------------------------------------------------------------


def _is_text(t: pa.DataType) -> bool:
    if pa.types.is_dictionary(t):
        t = t.value_type
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _first(mask: pa.ChunkedArray | pa.Array) -> int | None:
    """Index of the first true value of a boolean mask (nulls are false), else None."""
    mask = pc.fill_null(mask, False)
    if not pc.any(mask).as_py():
        return None
    return pc.index(mask, True).as_py()


def _sentinel_problems(fname: str, table: pa.Table) -> list[str]:
    out = []
    values = pa.array(SENTINELS, pa.string())
    for name in table.column_names:
        col = table[name]
        if not _is_text(col.type):
            continue
        row = _first(pc.is_in(col, value_set=values))
        if row is not None:
            out.append(
                f"{fname}: column {name!r} holds {col[row].as_py()!r} at row {row + 1}: empty "
                f"means unknown; never {', '.join(SENTINELS)} (re-export with empty cells)"
            )
    return out


def _header_problems(fname: str, names: list[str], spec: _Spec) -> list[str]:
    out = []
    numbered = [n for n in names if _ROW_NUMBER.match(n.strip())]
    if numbered:
        out.append(
            f"{fname}: unnamed row-number column {numbered[0]!r}: re-export as comma-separated "
            "CSV or Parquet without the row number"
        )
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        out.append(f"{fname}: duplicated columns {dup}")
    unknown = [n for n in names if n not in spec.columns and n not in numbered]
    if unknown:
        out.append(f"{fname}: unknown columns {unknown}; expected some of {list(spec.columns)}")
    missing = [c for c in spec.required if c not in names]
    if missing:
        out.append(f"{fname}: missing required columns {missing}")
    return out


def _csv_header(path: Path) -> tuple[list[str], list[str]]:
    """(column names, problems) from the first line of a CSV file."""
    with path.open("rb") as fh:
        line = fh.readline().decode("utf-8-sig", errors="replace").rstrip("\r\n")
    if not line:
        return [], [f"{path.name}: empty file (a header line is required)"]
    names = next(csv.reader(io.StringIO(line)))
    if "\t" in line:
        fields = line.split("\t")
        numbered = bool(_ROW_NUMBER.match(fields[0].strip()))
        return [], [
            f"{path.name}: the file is tab-separated (despite its name): re-export as "
            "comma-separated CSV or Parquet"
            + (", without the leading row-number column" if numbered else "")
            + "; a row-number column is refused too"
        ]
    return names, []


def _read_csv(path: Path, spec: _Spec) -> tuple[pa.Table | None, list[str]]:
    names, problems = _csv_header(path)
    if problems:
        return None, problems
    problems = _header_problems(path.name, names, spec)
    if problems:
        return None, problems

    def options(types: Mapping[str, pa.DataType]) -> pv.ConvertOptions:
        return pv.ConvertOptions(
            column_types=types,
            null_values=[""],
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
            include_columns=names,
        )

    read_types = {}
    for n in names:
        t = spec.columns[n]
        # integers read as float first: a "1234.0" volume is accepted, a fractional one refused
        read_types[n] = _MEMBER if n in spec.dictionary else (_F if t == _I else t)
    try:
        table = pv.read_csv(path, convert_options=options(read_types))
    except pa.ArrowInvalid as exc:
        strings = pv.read_csv(path, convert_options=options({n: _S for n in names}))
        found = _sentinel_problems(path.name, strings)
        return None, found or [f"{path.name}: cannot be read: {exc}"]
    return table, []


def _to_date(col: pa.ChunkedArray) -> pa.ChunkedArray:
    t = col.type
    if pa.types.is_timestamp(t):
        if t.tz is not None:
            raise ValueError("dates must be plain dates (YYYY-MM-DD), not timezone-aware times")
        floor = pc.floor_temporal(col, unit="day")
        if not pc.all(pc.fill_null(pc.equal(floor, col), True)).as_py():
            raise ValueError("dates must be plain dates (YYYY-MM-DD), found a time of day")
        return pc.cast(col, _D, safe=False)
    return pc.cast(col, _D)


def _normalise(fname: str, table: pa.Table, spec: _Spec) -> tuple[pa.Table | None, list[str]]:
    """Cast every column to its spec type; check sentinels, NaN and required values."""
    problems = _sentinel_problems(fname, table)
    if problems:
        return None, problems
    cols = {}
    for name in table.column_names:
        target, col = spec.columns[name], table[name]
        try:
            if target == _D:
                col = _to_date(col)
            elif target == _S:
                col = pc.cast(col, _S)
                # an empty string is an empty cell (Parquet writers often keep "")
                col = pc.if_else(pc.equal(col, ""), pa.scalar(None, _S), col)
            else:
                col = pc.cast(col, target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
            problems.append(f"{fname}: column {name!r} cannot be read as {target}: {exc}")
            continue
        if pa.types.is_floating(col.type):
            row = _first(pc.invert(pc.is_finite(col)))
            if row is not None:
                problems.append(
                    f"{fname}: column {name!r} holds NaN or an infinite number at row {row + 1}"
                )
        if name in spec.dictionary:
            col = pc.cast(col, _MEMBER) if not pa.types.is_dictionary(col.type) else col
        cols[name] = col
    for name in spec.non_null:
        if name in cols and cols[name].null_count:
            row = _first(pc.is_null(cols[name]))
            problems.append(
                f"{fname}: required column {name!r} is empty in {cols[name].null_count} row(s), "
                f"first at row {row + 1}"
            )
    if problems:
        return None, problems
    return pa.table(cols), []


def _read_parquet(path: Path, spec: _Spec) -> tuple[pa.Table | None, list[str]]:
    try:
        names = pq.read_schema(path).names
    except (pa.ArrowInvalid, OSError) as exc:
        return None, [f"{path.name}: cannot be read as Parquet: {exc}"]
    problems = _header_problems(path.name, names, spec)
    if problems:
        return None, problems
    return pq.read_table(path, columns=names), []


def _read_manifest(directory: Path) -> tuple[Manifest | None, list[str]]:
    found = [directory / n for n in ("manifest.json", "manifest.csv") if (directory / n).exists()]
    if not found:
        return None, ["manifest: manifest.json or manifest.csv is missing"]
    if len(found) > 1:
        return None, ["manifest: both manifest.json and manifest.csv exist; keep one"]
    path = found[0]
    try:
        if path.suffix == ".json":
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return None, [f"{path.name}: must be a JSON object"]
        else:
            rows = list(csv.reader(io.StringIO(path.read_text(encoding="utf-8-sig"))))
            if not rows or [c.strip() for c in rows[0]] != ["key", "value"]:
                return None, [f"{path.name}: header must be 'key,value'"]
            raw = {}
            for row in rows[1:]:
                if not row:
                    continue
                if len(row) != 2:
                    return None, [f"{path.name}: each line must be 'key,value'"]
                if row[0].strip() in raw:
                    return None, [f"{path.name}: key {row[0].strip()!r} given twice"]
                raw[row[0].strip()] = row[1]
    except (ValueError, UnicodeDecodeError) as exc:
        return None, [f"{path.name}: cannot be read: {exc}"]
    problems = []
    unknown = sorted(set(raw) - {*_MANIFEST_REQUIRED, *_MANIFEST_OPTIONAL})
    if unknown:
        problems.append(f"{path.name}: unknown manifest key(s) {unknown}")
    values: dict[str, str] = {}
    for key in (*_MANIFEST_REQUIRED, *_MANIFEST_OPTIONAL):
        v = raw.get(key)
        if v is not None and not isinstance(v, str):
            problems.append(f"{path.name}: {key} must be text")
        elif v is not None and v.strip():
            values[key] = v.strip()
        elif key in _MANIFEST_REQUIRED:
            problems.append(f"{path.name}: required key {key!r} is missing or empty")
    export_time = None
    if "export_time" in values:
        try:
            export_time = datetime.fromisoformat(values["export_time"])
        except ValueError:
            problems.append(f"{path.name}: export_time is not ISO 8601")
        else:
            if export_time.tzinfo is None or export_time.utcoffset() is None:
                problems.append(f"{path.name}: export_time needs a UTC offset (e.g. +00:00)")
    if "adjustment" in values and values["adjustment"].casefold() != "none":
        problems.append(
            f"{path.name}: adjustment must be 'none' (raw, unadjusted prices), got "
            f"{values['adjustment']!r}: the whole load is refused"
        )
    if "dividends" in values and values["dividends"].casefold() not in _DIVIDENDS:
        problems.append(f"{path.name}: dividends must be 'gross' or 'net'")
    if problems:
        return None, problems
    return (
        Manifest(
            export_time=export_time,
            adjustment=values["adjustment"].casefold(),
            dates_timezone=values["dates_timezone"],
            dividends=values["dividends"].casefold(),
            source_description=values["source_description"],
            fields=values.get("fields"),
            notes=values.get("notes"),
        ),
        [],
    )


def _discover(directory: Path) -> tuple[dict[str, list[Path]], list[str]]:
    files: dict[str, list[Path]] = {}
    problems = []
    data = sorted(p for p in directory.iterdir() if p.suffix in (".csv", ".parquet"))
    claimed: set[Path] = set()
    for name in _SPECS:
        parts = [p for p in data if p.stem.startswith(name)]
        claimed.update(parts)
        if {p.suffix for p in parts} == {".csv", ".parquet"}:
            problems.append(f"{name}: both CSV and Parquet files; keep one format per table")
        if parts:
            files[name] = parts
    stray = [p.name for p in data if p not in claimed and p.stem != "manifest"]
    if stray:
        problems.append(f"unknown files {stray}; expected {list(_SPECS)} (and manifest)")
    for name in _REQUIRED_FILES:
        if name not in files:
            problems.append(f"{name}: required file is missing")
    if not any(name in files for name in _ONE_OF_FILES):
        problems.append(f"one of {list(_ONE_OF_FILES)} is required")
    return files, problems


def _read_table(name: str, parts: list[Path]) -> tuple[pa.Table | None, list[str]]:
    spec = _SPECS[name]
    tables, problems = [], []
    for path in parts:
        reader = _read_parquet if path.suffix == ".parquet" else _read_csv
        table, found = reader(path, spec)
        if table is not None:
            table, found = _normalise(path.name, table, spec)
        problems.extend(found)
        if table is not None:
            tables.append(table)
    if problems:
        return None, problems
    present = [c for c in spec.columns if any(c in t.column_names for t in tables)]
    aligned = []
    for t in tables:
        cols = {}
        for c in present:
            typ = _MEMBER if c in spec.dictionary else spec.columns[c]
            cols[c] = t[c] if c in t.column_names else pa.nulls(t.num_rows, typ)
        aligned.append(pa.table(cols))
    table = pa.concat_tables(aligned)
    if spec.key and table.num_rows:
        keyed = table.select(list(spec.key))
        for c in spec.dictionary:
            if c in spec.key:
                keyed = keyed.set_column(keyed.schema.get_field_index(c), c, pc.cast(keyed[c], _S))
        counts = keyed.group_by(list(spec.key)).aggregate([([], "count_all")])
        n = pc.sum(pc.greater(counts["count_all"], 1)).as_py() or 0
        if n:
            return None, [f"{name}: {n} duplicate ({', '.join(spec.key)}) key(s)"]
    return table, []


# --- vectorised helpers ------------------------------------------------------------------------


def _const(value: Any, n: int, typ: pa.DataType) -> pa.Array:
    if value is None:
        return pa.nulls(n, typ)
    return pc.cast(
        pa.DictionaryArray.from_arrays(pa.array(np.zeros(n, np.int32)), pa.array([value], typ)),
        typ,
    )


def _member_index(col: pa.ChunkedArray) -> tuple[pa.ChunkedArray, list[str]]:
    """(per-row int32 index into the member list, member list) of a dictionary column."""
    col = pa.table({"m": col}).unify_dictionaries()["m"]
    if col.num_chunks == 0:
        return pa.chunked_array([], pa.int32()), []
    members = col.chunk(0).dictionary.to_pylist()
    return pa.chunked_array([c.indices for c in col.chunks], pa.int32()), members


def _count(mask) -> int:
    return int(pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0)


# --- the adapter -------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Member:
    token: str
    exchange_code: str
    mic: str | None


class FileHistory:
    """An export directory in the history-file format (module docstring).

    ``salt``: token salt bytes; None reads ``MOF_MARKET_TOKEN_SALT`` (missing or shorter than
    ``MIN_SALT_BYTES`` bytes: ``ValueError``). ``exchange_mics``: exchange code -> calendar MIC.
    ``action_types``: ``source_action_type`` (case-insensitive) -> ``action_type`` (default: the
    package data file ``file_action_types.json``). ``next_session_margin``: the actions calendar
    margin (as the provider builder's)."""

    def __init__(
        self,
        directory: Path | str,
        source_code: str = "private_history",
        *,
        salt: bytes | None = None,
        exchange_mics: Mapping[str, str] = DEFAULT_EXCHANGE_MICS,
        action_types: Mapping[str, str] | None = None,
        next_session_margin: timedelta = NEXT_SESSION_MARGIN,
    ) -> None:
        self._salt = salt_from_env() if salt is None else bytes(salt)
        if len(self._salt) < MIN_SALT_BYTES:
            raise ValueError(f"the token salt must be at least {MIN_SALT_BYTES} bytes")
        check_margin(next_session_margin)
        types = default_action_types() if action_types is None else dict(action_types)
        bad = sorted(v for v in types.values() if v not in ACTION_TYPES)
        if bad:
            raise ValueError(f"{bad} is not an action type; expected one of {ACTION_TYPES}")
        self.directory = Path(directory)
        self.source_code = source_code
        self.exchange_mics = {k.strip().upper(): v for k, v in exchange_mics.items()}
        self.action_types = {_norm_type(k): v for k, v in types.items()}
        self.next_session_margin = next_session_margin
        self.report = LoadReport()
        self._problems: list[str] | None = None
        self._manifest: Manifest | None = None
        self._raw: dict[str, pa.Table] = {}
        self._members: dict[str, _Member] = {}
        self._cache: dict[str, pa.Table] = {}

    def __repr__(self) -> str:
        return f"FileHistory({self.directory.name!r}, source_code={self.source_code!r})"

    # -- validation ---------------------------------------------------------------------------

    def validate(self) -> list[str]:
        """Every problem of the export (empty when it can be loaded). Reads every file once."""
        if self._problems is None:
            self._problems = self._validate()
        return list(self._problems)

    def _validate(self) -> list[str]:
        if not self.directory.is_dir():
            return [f"{self.directory.name}: not a directory"]
        manifest, problems = _read_manifest(self.directory)
        files, found = _discover(self.directory)
        problems += found
        raw: dict[str, pa.Table] = {}
        for name, parts in files.items():
            table, found = _read_table(name, parts)
            problems += found
            if table is not None:
                raw[name] = table
        if "securities" in raw:
            known = raw["securities"]["member_id"].combine_chunks()
            for name, table in raw.items():
                if name == "securities":
                    continue
                n = _count(pc.invert(pc.is_in(table["member_id"], value_set=known)))
                if n:
                    problems.append(f"{name}: {n} row(s) whose member_id is not in securities")
        if "corporate_actions" in raw:
            problems += self._check_action_types(raw["corporate_actions"])
        if not problems:
            self._manifest, self._raw = manifest, raw
            self._members = self._member_table(raw["securities"])
        return problems

    def _check_action_types(self, table: pa.Table) -> list[str]:
        if "action_type" not in table.column_names:
            return []
        out = []
        given = table["action_type"].to_pylist()
        source = table["source_action_type"].to_pylist()
        for i, (a, s) in enumerate(zip(given, source, strict=True)):
            if a is None:
                continue
            mapped = self.action_types.get(_norm_type(s))
            if a not in ACTION_TYPES:
                out.append(
                    f"corporate_actions: row {i + 1}: action_type {a!r} is not one of "
                    f"{ACTION_TYPES}"
                )
            elif mapped is not None and mapped != a:
                out.append(
                    f"corporate_actions: row {i + 1}: action_type {a!r} disagrees with "
                    f"source_action_type {s!r} (mapped to {mapped!r})"
                )
        return out

    def _member_table(self, securities: pa.Table) -> dict[str, _Member]:
        ids = securities["member_id"].to_pylist()
        codes = (
            securities["exchange_code"].to_pylist()
            if "exchange_code" in securities.column_names
            else [None] * len(ids)
        )
        out = {}
        for member_id, code in zip(ids, codes, strict=True):
            out[member_id] = self._member(member_id, code)
        return out

    def _member(self, member_id: str, code: str | None = None) -> _Member:
        if code is None or not code.strip():
            words = member_id.split()
            code = words[1] if len(words) >= 2 else NO_EXCHANGE_CODE
        code = code.strip().upper()
        return _Member(
            token(self.source_code, member_id, self._salt), code, self.exchange_mics.get(code)
        )

    def _load(self) -> None:
        problems = self.validate()
        if problems:
            raise HistoryFileError(problems)

    @property
    def manifest(self) -> Manifest:
        self._load()
        assert self._manifest is not None
        return self._manifest

    def _check_load_time(self, load_time: datetime) -> datetime:
        if load_time.tzinfo is None or load_time.utcoffset() is None:
            raise ValueError("load_time must be a timezone-aware datetime")
        if load_time < self.manifest.export_time:
            raise ValueError("load_time is before the manifest's export_time")
        return load_time.astimezone(UTC)

    # -- dated tables --------------------------------------------------------------------------

    def _members_of(self, col: pa.ChunkedArray) -> tuple[pa.ChunkedArray, list[_Member]]:
        idx, ids = _member_index(col)
        return idx, [self._members[m] for m in ids]

    def _unmapped(
        self, table: str, idx: pa.ChunkedArray, members: list[_Member]
    ) -> tuple[pa.Array, int]:
        """Per-row "member is on a mapped exchange" mask; counts per unmapped code reported."""
        counts = np.bincount(
            idx.to_numpy() if idx.num_chunks else np.zeros(0, np.int32), minlength=len(members)
        )
        per_code: dict[str, int] = {}
        for m, n in zip(members, counts, strict=True):
            if m.mic is None and n:
                per_code[m.exchange_code] = per_code.get(m.exchange_code, 0) + int(n)
        self.report.unmapped_exchanges[table] = dict(sorted(per_code.items()))
        mapped = pa.array([m.mic is not None for m in members], pa.bool_())
        return pc.take(mapped, idx), sum(per_code.values())

    def _session_closes(
        self, raw: pa.Table, idx: pa.ChunkedArray, members: list[_Member]
    ) -> pa.ChunkedArray:
        """Per-row session close in UTC on the member's calendar (null: no session, unmapped)."""
        dates = raw["date"]
        known = pa.chunked_array([pa.nulls(raw.num_rows, _TS)])
        for mic in sorted({m.mic for m in members if m.mic is not None}):
            codes = pa.array([i for i, m in enumerate(members) if m.mic == mic], pa.int32())
            on_mic = pc.is_in(idx, value_set=codes)
            span = pc.min_max(pc.filter(dates, on_mic)).as_py()
            if span["min"] is None:
                continue
            cal = sessions(mic, span["min"], span["max"])
            pos = pc.index_in(dates, value_set=cal["session_date"])
            close = pc.take(cal["close_utc"].combine_chunks(), pos)
            known = pc.if_else(on_mic, close, known)
        return known

    def _dated(self, table: str, raw: pa.Table, values: Mapping[str, pa.DataType], contract):
        """Prices / vendor total return: session-close known_at, drops counted, sorted rows."""
        export = self.manifest.export_time.astimezone(UTC)
        idx, members = self._members_of(raw["member_id"])
        mapped, n_unmapped = self._unmapped(table, idx, members)
        known = self._session_closes(raw, idx, members)
        after = pc.greater(known, pa.scalar(export, _TS))
        self.report.dropped[table] = {
            DROP_UNMAPPED_EXCHANGE: n_unmapped,
            DROP_NON_SESSION: _count(pc.and_(mapped, pc.is_null(known))),
            DROP_AFTER_EXPORT: _count(after),
        }
        keep = pc.fill_null(pc.invert(after), False)  # null known (unmapped, non-session) out
        idx_k = pc.filter(idx, keep)
        # sort by (token, date) through integer ranks of the tokens
        tokens = [m.token for m in members]
        rank = np.empty(len(tokens), np.int32)
        rank[np.argsort(np.array(tokens, dtype=object), kind="stable")] = np.arange(len(tokens))
        dates = pc.filter(raw["date"], keep)
        order = pc.sort_indices(
            pa.table({"r": pc.take(pa.array(rank), idx_k), "d": dates}),
            sort_keys=[("r", "ascending"), ("d", "ascending")],
        )
        n = len(order)
        cols: dict[str, Any] = {
            "source_symbol": pc.take(pc.take(pa.array(tokens, _S), idx_k), order),
            "valid_date": pc.take(dates, order),
        }
        for name, typ in values.items():
            if name in raw.column_names:
                cols[name] = pc.take(pc.cast(pc.filter(raw[name], keep), typ), order)
            else:
                cols[name] = pa.nulls(n, typ)
        if "price_basis" in contract.schema.names:
            cols["price_basis"] = _const("raw", n, _S)
        cols["known_at_basis"] = _const("session_close", n, _S)
        cols["known_at"] = pc.take(pc.filter(known, keep), order)
        cols["known_at_kind"] = _const("published", n, _S)
        cols["source_code"] = _const(self.source_code, n, _S)
        cols["source_doc_id"] = pa.nulls(n, _S)
        return pa.table(cols, schema=contract.schema)

    def _cached(self, name: str, build: Callable[[], pa.Table]) -> pa.Table:
        if name not in self._cache:
            self._cache[name] = build()
        return self._cache[name]

    def prices(self) -> pa.Table:
        """``prices`` rows (contract schema), raw values as filed, keyed on tokens."""
        self._load()
        if "prices_daily" not in self._raw:
            self.report.dropped["prices"] = {}
            return PRICES.schema.empty_table()
        values = {"open": _F, "high": _F, "low": _F, "close": _F, "volume": _I, "currency": _S}
        return self._cached(
            "prices", lambda: self._dated("prices", self._raw["prices_daily"], values, PRICES)
        )

    def vendor_total_return(self) -> pa.Table:
        """``vendor_total_return`` rows (contract schema), keyed on tokens."""
        self._load()
        if "total_return_index" not in self._raw:
            self.report.dropped["vendor_total_return"] = {}
            return VENDOR_TOTAL_RETURN.schema.empty_table()
        return self._cached(
            "vendor_total_return",
            lambda: self._dated(
                "vendor_total_return",
                self._raw["total_return_index"],
                {"tr_index": _F},
                VENDOR_TOTAL_RETURN,
            ),
        )

    # -- snapshots -----------------------------------------------------------------------------

    def snapshots(
        self,
        previous_snapshots: pa.Table | None = None,
        *,
        load_time: datetime | None = None,
    ) -> pa.Table:
        """``index_snapshots`` rows (contract schema), keyed on tokens.

        ``previous_snapshots``: the ``index_snapshots`` rows already published for this source
        (any source_code: others are ignored). For each (index, date) filed both there and here,
        a published member absent now gets an ``is_member=false`` row, and a new or changed row
        is emitted at ``load_time`` (``collected`` / ``load_time``, so a restatement never
        reaches back to the snapshot date); unchanged rows are not re-emitted. ``load_time`` is
        required with ``previous_snapshots``."""
        self._load()
        if "index_snapshots" not in self._raw:
            self.report.dropped["index_snapshots"] = {}
            return INDEX_SNAPSHOTS.schema.empty_table()
        base = self._cached("index_snapshots", self._snapshots)
        if previous_snapshots is None:
            return base
        if load_time is None:
            raise ValueError("load_time is required with previous_snapshots")
        return self._restate(base, previous_snapshots, self._check_load_time(load_time))

    def _snapshots(self) -> pa.Table:
        raw = self._raw["index_snapshots"]
        export = self.manifest.export_time.astimezone(UTC)
        sizes = (
            raw.select(["index", "date"]).group_by(["index", "date"]).aggregate([([], "count_all")])
        )
        idx, members = self._members_of(raw["member_id"])
        mapped, n_unmapped = self._unmapped("index_snapshots", idx, members)
        self.report.dropped["index_snapshots"] = {DROP_UNMAPPED_EXCHANGE: n_unmapped}
        kept = pa.table(
            {
                "index_code": pc.filter(raw["index"], mapped),
                "source_symbol": pc.take(
                    pa.array([m.token for m in members], _S), pc.filter(idx, mapped)
                ),
                "valid_date": pc.filter(raw["date"], mapped),
                "weight": pc.filter(raw["weight"], mapped)
                if "weight" in raw.column_names
                else pa.nulls(_count(mapped), _F),
            }
        )
        sizes = sizes.rename_columns(["index_code", "valid_date", "snapshot_size"])
        kept = kept.join(sizes, ["index_code", "valid_date"], join_type="left outer")
        n = kept.num_rows
        eod = pc.add(pc.cast(pc.cast(kept["valid_date"], pa.timestamp("us")), _TS), _EOD)
        capped = pc.greater(eod, pa.scalar(export, _TS))
        cols = {
            "index_code": kept["index_code"],
            "source_symbol": kept["source_symbol"],
            "snapshot_size": pc.cast(kept["snapshot_size"], pa.int32()),
            "weight": kept["weight"],
            "is_member": _const(True, n, pa.bool_()),
            "known_at_basis": pc.if_else(capped, "fetch_time", "snapshot_date"),
            "valid_date": kept["valid_date"],
            "known_at": pc.if_else(capped, pa.scalar(export, _TS), eod),
            "known_at_kind": pc.if_else(capped, "collected", "published"),
            "source_code": _const(self.source_code, n, _S),
            "source_doc_id": pa.nulls(n, _S),
        }
        table = pa.table(cols).select(INDEX_SNAPSHOTS.schema.names).cast(INDEX_SNAPSHOTS.schema)
        return table.sort_by(
            [(c, "ascending") for c in ("index_code", "source_symbol", "valid_date", "known_at")]
        )

    def _restate(self, base: pa.Table, previous: pa.Table, load_time: datetime) -> pa.Table:
        pair = ["index_code", "valid_date"]
        key = ["index_code", "source_symbol", "valid_date"]
        prev = previous.filter(pc.equal(previous["source_code"], self.source_code))
        prev = latest(prev.select(INDEX_SNAPSHOTS.schema.names), ["source_code", *key])
        prev = prev.select([*key, "snapshot_size", "weight", "is_member"]).rename_columns(
            [*key, "p_size", "p_weight", "p_member"]
        )
        filed = self._raw["index_snapshots"].select(["index", "date"]).rename_columns(pair)
        sizes = filed.group_by(pair).aggregate([([], "count_all")])  # every filed member
        restated = prev.select(pair).group_by(pair).aggregate([])
        restated = restated.join(sizes.select(pair), pair, join_type="inner")
        restated = restated.append_column("in_prev", _const(True, restated.num_rows, pa.bool_()))

        # current rows: unchanged inside a restated pair -> dropped; new or changed -> load_time
        cur = base.join(restated, pair, join_type="left outer").join(
            prev, key, join_type="left outer"
        )
        in_prev = pc.fill_null(cur["in_prev"], False)
        same_weight = pc.or_(
            pc.fill_null(pc.equal(cur["weight"], cur["p_weight"]), False),
            pc.and_(pc.is_null(cur["weight"]), pc.is_null(cur["p_weight"])),
        )
        same = pc.and_(
            pc.and_(
                pc.fill_null(pc.equal(cur["snapshot_size"], cur["p_size"]), False), same_weight
            ),
            pc.fill_null(pc.equal(cur["is_member"], cur["p_member"]), False),
        )
        unchanged = pc.and_(in_prev, same)
        cur = cur.filter(pc.invert(unchanged))
        moved = pc.fill_null(cur["in_prev"], False)
        load = pa.scalar(load_time, _TS)
        cur = (
            cur.set_column(
                cur.schema.get_field_index("known_at"),
                "known_at",
                pc.if_else(moved, load, cur["known_at"]),
            )
            .set_column(
                cur.schema.get_field_index("known_at_kind"),
                "known_at_kind",
                pc.if_else(moved, "collected", cur["known_at_kind"]),
            )
            .set_column(
                cur.schema.get_field_index("known_at_basis"),
                "known_at_basis",
                pc.if_else(moved, "load_time", cur["known_at_basis"]),
            )
        )

        # published members absent from a restated pair -> is_member=false at load_time
        gone = prev.filter(pc.fill_null(prev["p_member"], False)).join(
            restated, pair, join_type="inner"
        )
        gone = gone.join(base.select(key), key, join_type="left anti")
        gone = gone.join(sizes, pair, join_type="left outer")
        n = gone.num_rows
        absent = pa.table(
            {
                "index_code": gone["index_code"],
                "source_symbol": gone["source_symbol"],
                "snapshot_size": pc.cast(gone["count_all"], pa.int32()),
                "weight": pa.nulls(n, _F),
                "is_member": _const(False, n, pa.bool_()),
                "known_at_basis": _const("load_time", n, _S),
                "valid_date": gone["valid_date"],
                "known_at": _const(load_time, n, _TS),
                "known_at_kind": _const("collected", n, _S),
                "source_code": _const(self.source_code, n, _S),
                "source_doc_id": pa.nulls(n, _S),
            }
        )
        out = pa.concat_tables(
            [
                cur.select(INDEX_SNAPSHOTS.schema.names).cast(INDEX_SNAPSHOTS.schema),
                absent.cast(INDEX_SNAPSHOTS.schema),
            ]
        )
        return out.sort_by(
            [(c, "ascending") for c in ("index_code", "source_symbol", "valid_date", "known_at")]
        )

    # -- corporate actions ---------------------------------------------------------------------

    def actions(self) -> pa.Table:
        """``corporate_actions`` rows (contract schema), keyed on tokens."""
        self._load()
        if "corporate_actions" not in self._raw:
            self.report.dropped["corporate_actions"] = {}
            self.report.unknown_action_types = {}
            return CORPORATE_ACTIONS.schema.empty_table()
        return self._cached("corporate_actions", self._actions)

    def _actions(self) -> pa.Table:
        manifest = self.manifest
        export = manifest.export_time.astimezone(UTC)
        rows = self._raw["corporate_actions"].to_pylist()
        unknown: dict[str, int] = {}
        unmapped: dict[str, int] = {}
        kept: list[tuple[dict[str, Any], _Member, str]] = []
        for r in rows:
            member = self._members[r["member_id"]]
            action_type = r.get("action_type") or self.action_types.get(
                _norm_type(r["source_action_type"])
            )
            if action_type is None:
                unknown[r["source_action_type"]] = unknown.get(r["source_action_type"], 0) + 1
            elif member.mic is None:
                unmapped[member.exchange_code] = unmapped.get(member.exchange_code, 0) + 1
            else:
                kept.append((r, member, action_type))
        self.report.unknown_action_types = dict(sorted(unknown.items()))
        self.report.unmapped_exchanges["corporate_actions"] = dict(sorted(unmapped.items()))
        self.report.dropped["corporate_actions"] = {
            DROP_UNKNOWN_ACTION_TYPE: sum(unknown.values()),
            DROP_UNMAPPED_EXCHANGE: sum(unmapped.values()),
        }
        by_mic: dict[str, list[date]] = {}
        for r, member, _ in kept:
            by_mic.setdefault(member.mic, []).append(r["ex_date"])
        calendars = {
            mic: actions_calendar(mic, ex, self.next_session_margin) for mic, ex in by_mic.items()
        }
        first: dict[tuple, tuple[datetime, str, str]] = {}
        seq: dict[tuple, int] = {}
        out = []
        for r, member, action_type in kept:
            ex, announced = r["ex_date"], r.get("announce_date")
            memo = (member.mic, ex, announced)
            if memo not in first:
                first[memo] = action_known_at(
                    calendars[member.mic], member.mic, ex, announced, export
                )
            known_at, kind, basis = first[memo]
            ident = (member.token, ex, action_type)
            seq[ident] = seq.get(ident, -1) + 1
            amount = r.get("amount")
            spin = r.get("spinoff_member_id")
            out.append(
                {
                    "source_symbol": member.token,
                    "action_type": action_type,
                    "action_seq": seq[ident],
                    "source_action_type": r["source_action_type"],
                    "announce_date": announced,
                    "record_date": r.get("record_date"),
                    "pay_date": r.get("pay_date"),
                    "amount": amount,
                    "currency": r.get("currency"),
                    "amount_basis": manifest.dividends if amount is not None else None,
                    "per_share_basis": None,
                    "ratio_old": r.get("ratio_old"),
                    "ratio_new": r.get("ratio_new"),
                    "spinoff_symbol": None if spin is None else self._member(spin).token,
                    "known_at_basis": basis,
                    "valid_date": ex,
                    "known_at": known_at,
                    "known_at_kind": kind,
                    "source_code": self.source_code,
                    "source_doc_id": None,
                }
            )
        out.sort(
            key=lambda r: (r["source_symbol"], r["valid_date"], r["action_type"], r["action_seq"])
        )
        return pa.Table.from_pylist(out, schema=CORPORATE_ACTIONS.schema)

    # -- labels and fetch log ------------------------------------------------------------------

    def observations(self, load_time: datetime) -> pa.Table:
        """Identifier observations (``OBSERVATION_SCHEMA``: ``RAW_SYMBOL_SCHEMA`` then the
        bitemporal columns), the only place labels appear: one row per member and non-empty
        label (schemes export_member_id, export_name, figi, composite_figi, share_class_figi,
        isin, ticker from ``last_ticker``). Labels describe the export: ``valid_date`` = export
        date, ``known_at`` = ``load_time`` (``collected`` / ``load_time``)."""
        self._load()
        load = self._check_load_time(load_time)
        securities = self._raw["securities"]
        cols = {c: securities[c].to_pylist() for c, _ in _OBSERVED if c in securities.column_names}
        names = cols["name"]
        out = []
        for i, member_id in enumerate(cols["member_id"]):
            tok = self._members[member_id].token
            for column, scheme in _OBSERVED:
                value = cols.get(column, [None] * len(names))[i]
                if value is None or not value.strip():
                    continue
                out.append(
                    {
                        "source_symbol": tok,
                        "id_scheme": scheme,
                        "id_value": value,
                        "valid_date": self.manifest.export_date,
                        "valid_to": None,
                        "name": names[i],
                        "known_at_basis": "load_time",
                        "known_at": load,
                        "known_at_kind": "collected",
                        "source_code": self.source_code,
                        "source_doc_id": None,
                    }
                )
        out.sort(key=lambda r: (r["source_symbol"], r["id_scheme"], r["id_value"]))
        return pa.Table.from_pylist(out, schema=OBSERVATION_SCHEMA)

    def fetch_log(self, load_time: datetime) -> pa.Table:
        """``fetch_log`` rows: one per member with loaded prices or snapshots, so the coverage
        ledger can corroborate a delisting (status ``ok`` with prices, ``empty`` without)."""
        self._load()
        load = self._check_load_time(load_time)
        manifest = self.manifest
        spans: dict[str, dict[str, Any]] = {}
        prices = self.prices()
        if prices.num_rows:
            agg = prices.group_by("source_symbol").aggregate(
                [("valid_date", "min"), ("valid_date", "max"), ("valid_date", "count")]
            )
            for r in agg.to_pylist():
                spans[r["source_symbol"]] = {
                    "first": r["valid_date_min"],
                    "returned": (r["valid_date_min"], r["valid_date_max"]),
                    "count": r["valid_date_count"],
                }
        snaps = self.snapshots()
        if snaps.num_rows:
            agg = snaps.group_by("source_symbol").aggregate([("valid_date", "min")])
            for r in agg.to_pylist():
                s = spans.setdefault(
                    r["source_symbol"],
                    {"first": r["valid_date_min"], "returned": (None, None), "count": 0},
                )
                s["first"] = min(s["first"], r["valid_date_min"])
        stamp = manifest.export_time.isoformat()
        out = [
            {
                "source_symbol": tok,
                "fetch_id": f"{stamp}:{tok}",
                "requested_from": s["first"],
                "requested_to": manifest.export_date,
                "returned_from": s["returned"][0],
                "returned_to": s["returned"][1],
                "status": "ok" if s["count"] else "empty",
                "row_count": s["count"],
                "archive_ref": None,
                "known_at_basis": "fetch_time",
                "valid_date": manifest.export_date,
                "known_at": load,
                "known_at_kind": "collected",
                "source_code": self.source_code,
                "source_doc_id": None,
            }
            for tok, s in sorted(spans.items())
        ]
        return pa.Table.from_pylist(out, schema=FETCH_LOG.schema)

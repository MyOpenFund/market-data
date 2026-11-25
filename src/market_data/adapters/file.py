"""History-file adapter: loads an export in the public history-file format (spec §9.2).

The history-file format is the door for anyone with their own data, and the path for a private
one-shot export. It is not a network adapter: it has no ``fetch``/``parse`` and does not implement
the ``Adapter`` protocol, so it is not declared under the ``market_data.adapters`` entry point.
Entry point: ``FileHistory(path).build_all(load_time)`` returns every table and the filled load
report (``LoadResult``); the caller publishes the tables.

Format (a directory)
    ``manifest.json`` (an object) or ``manifest.csv`` (header ``key,value``), required keys
    ``export_time`` (ISO 8601 with offset), ``adjustment`` (must be ``none``: prices raw,
    unadjusted; anything else refuses the whole load), ``dates_timezone`` (must be
    ``exchange``: every date is the local trading date of the member's exchange, the only
    convention supported), ``dividends`` (``gross`` or ``net``), ``source_description``;
    optional ``fields``, ``notes``, ``per_share_basis_default`` (``old_share`` or
    ``new_share``: the ``per_share_basis`` of a cash action whose row leaves it empty).
    Then ``securities`` (required), ``prices_daily``, ``index_snapshots`` (at least one of
    them), ``corporate_actions`` and
    ``total_return_index`` (optional), each as Parquet (preferred) or CSV (UTF-8, comma,
    header), possibly split into parts: ``<name>.<ext>`` or ``<name>_part<digits>.<ext>``
    (``prices_daily_part001.parquet``), read in natural file-name order (part2 before part10);
    one format per table. Any other file is listed in ``report.ignored.files``.
    Columns in ``FILE_COLUMNS``; dates ``YYYY-MM-DD``; ``.`` decimal; empty = unknown (also an
    empty string in a Parquet column of any type). A Parquet ``member_id`` dictionary may hold
    entries no row uses (e.g. unused pandas categories); they are ignored.

Strict input (``validate()`` lists every problem; each table method raises ``HistoryFileError``
while there is one). Refused:
    - a tab- or semicolon-separated file, an unnamed row-number column, a header ending with a
      trailing comma, duplicated column names (named by position), a first line holding none of
      the required columns ("no header line?": nothing of it is recorded), a header or
      manifest field too long to parse (re-export as comma CSV or Parquet); a malformed CSV line
      (field count); invalid UTF-8; a blank (whitespace-only) member id;
    - "unknown" placeholders, compared after trim + casefold: in numeric and date columns
      ``NUMERIC_SENTINELS`` (``-``, ``n/a``, ``na``, ``#n/a``); in text columns
      ``TEXT_SENTINELS`` (``-``, ``--``, ``n/a``, ``null``) or any ``#n/a...`` prefix, but
      not ``na`` (Amsterdam's exchange code, Namibia) nor ``nan`` (a ticker);
    - NaN or infinite numbers; a fractional volume; a timezone-aware or non-midnight timestamp as
      a date; an ISIN not matching ``^[A-Z]{2}[A-Z0-9]{9}[0-9]$`` with a valid check digit; a
      FIGI (``figi``, ``composite_figi``, ``share_class_figi``) not matching
      ``^BBG[0-9A-Z]{9}$``;
    - missing required files, manifest keys, columns or values; CSV and Parquet parts mixed for
      one table; duplicate keys; member ids absent from ``securities``; an ``action_type``
      column outside ``ACTION_TYPES`` or disagreeing with the mapped ``source_action_type``.
Messages name the file, the column and the row ("data row N (file line N+1)" for CSV), never a
cell's content (pyarrow's own messages, which quote rows, are never shown); only the matched
placeholder itself is quoted. Tolerated and listed in ``report.ignored`` (they cannot change
the meaning of known columns): files that are not part of the format, columns of a known file
outside ``FILE_COLUMNS`` (not read; listed by name only from a sound header whose unknown
names all look like column names, ``[a-z][a-z0-9_]{0,62}``, else only counted, so a wide or
transposed file never puts member ids in the report), unknown manifest keys.

Token salt: ``MOF_MARKET_TOKEN_SALT`` (or ``salt=``), at least ``MIN_SALT_BYTES`` (16) bytes.

Look-ahead guard (spec §9.3)
    Every dated row (prices, actions, vendor total return, snapshots, fetch log) is keyed on
    ``token(source_code, member_id, salt)``; labels (member id, name, FIGIs, ISIN, last ticker)
    appear only in ``observations(load_time)``, known at load time.

``known_at`` (spec §7.3)
    Prices and vendor total return: the session close in UTC of the row's date on the member's
    exchange calendar (``published`` / ``session_close``). A date that is not a session is
    dropped (``non_session``); when those drops exceed ``max_non_session_share`` (default
    0.5 %) of the loaded rows the load is refused (tripwire for dates that are not
    exchange-local). A session closing after ``export_time`` is dropped (``after_export``: a
    partial bar, as the builder's intraday drop). Snapshots (no calendar needed): snapshot date
    23:59:59.999999 UTC (``published`` / ``snapshot_date``), capped at ``export_time`` (then
    ``collected`` / ``fetch_time``). Actions: the provider builder's rule
    (``market_data._calendars``), capped at ``export_time``. Observations and fetch log:
    ``load_time`` (``collected``).

Exchange calendars
    A member's exchange code is ``securities.exchange_code``, else the second word of its
    ``member_id`` (``AAA UW Equity`` -> ``UW``); ``exchange_mics`` maps codes to calendar MICs
    (default ``DEFAULT_EXCHANGE_MICS``: US codes -> XNYS; every MIC must be an
    ``exchange_calendars`` calendar, checked in the constructor). A code that is not
    ``[A-Z0-9]{1,4}`` is reported as ``<invalid>``, never verbatim. Prices, vendor total return and
    actions of a member whose code is not mapped are kept out (``unmapped_exchange``) and
    counted per code. Snapshots, observations and the fetch log keep every member (a European
    composition loads before its prices; its fetch-log rows are ``empty``).

Corporate actions: an explicit ``action_type`` wins, else ``source_action_type`` through
``action_types`` (case-insensitive); unknown labels are kept out and counted, never guessed.
The optional ``per_share_basis`` column (``old_share`` / ``new_share``, else the manifest's
``per_share_basis_default``) is set on rows with an ``amount``. Cash actions the total-return
recipe could never apply (no currency; no basis on the ex-date of a split or stock dividend
of the member) are kept and counted in ``report.unusable_cash`` (``quality.unusable_cash``).
An exact duplicate row is kept once and counted (``duplicate_row``). Spin-off targets absent
from ``securities`` get a token but no label; their number is reported.

Restatement (``snapshots(previous_snapshots, load_time=)``): ``load_time`` must be strictly
after every ``known_at`` already published; for an (index, date) filed again, published
members now absent get ``is_member=false``, and new or changed members are re-emitted, all at
``load_time`` (``collected`` / ``load_time``); unchanged rows are not re-emitted.

Load report: ``FileHistory.report`` (``LoadReport``: counts, ignored names, never a label) is
filled as tables are built.

Memory: everything is held in memory. Measured on synthetic data, 20M price rows peak at about
4.4 GB (raw + ``prices``); with a vendor total-return file of the same size, about 6.6 GB.
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
    PER_SHARE_BASES,
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
from market_data.quality import unusable_cash
from market_data.tokens import MIN_SALT_BYTES, salt_from_env, token

__all__ = [
    "DEFAULT_EXCHANGE_MICS",
    "DATES_TIMEZONE",
    "DROP_AFTER_EXPORT",
    "DROP_DUPLICATE_ROW",
    "DROP_NON_SESSION",
    "DROP_UNKNOWN_ACTION_TYPE",
    "DROP_UNMAPPED_EXCHANGE",
    "FILE_COLUMNS",
    "OBSERVATION_SCHEMA",
    "NUMERIC_SENTINELS",
    "TEXT_SENTINELS",
    "TEXT_SENTINEL_PREFIX",
    "FileHistory",
    "INVALID_EXCHANGE_CODE",
    "HistoryFileError",
    "Ignored",
    "LoadReport",
    "LoadResult",
    "MAX_NON_SESSION_SHARE",
    "Manifest",
    "default_action_types",
]

# V0 default: US exchange codes -> NYSE calendar (Europe after the V0).
DEFAULT_EXCHANGE_MICS: Mapping[str, str] = MappingProxyType(
    {code: "XNYS" for code in ("UN", "UW", "UQ", "UA", "UP", "UF", "UR", "UV", "US")}
)
# Cell values that mean "unknown" in some exports; the format says empty, so they are refused
# (compared after trim + casefold). In text columns "na" is a real value (Amsterdam's exchange
# code, Namibia's country code) and so is "nan" (a ticker): neither is a text sentinel.
NUMERIC_SENTINELS = ("-", "n/a", "na", "#n/a")
TEXT_SENTINELS = ("-", "--", "n/a", "null")
TEXT_SENTINEL_PREFIX = "#n/a"
# Tripwire: price (or vendor TR) rows dropped as non-session above this share of the loaded rows
# refuse the load (dates that are not exchange-local trading dates).
MAX_NON_SESSION_SHARE = 0.005
DATES_TIMEZONE = "exchange"
DROP_DUPLICATE_ROW = "duplicate_row"
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
_MANIFEST_OPTIONAL = ("fields", "notes", "per_share_basis_default")
_DIVIDENDS = ("gross", "net")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")  # always fullmatch
_FIGI = re.compile(r"BBG[0-9A-Z]{9}")
_COLUMN_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")  # always fullmatch, after casefold
_EXCHANGE_CODE = re.compile(r"[A-Z0-9]{1,4}")  # always fullmatch
INVALID_EXCHANGE_CODE = "<invalid>"
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
            "per_share_basis": _S,
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
    # the per_share_basis of a cash action whose row leaves it empty (None: left empty)
    per_share_basis_default: str | None = None

    @property
    def export_date(self) -> date:
        """The export's calendar date, in the offset the export time was written with."""
        return self.export_time.date()


@dataclass
class Ignored:
    """What the loader read past: files in the directory that are not part of the format,
    columns of a known file outside its spec (file name -> columns), unknown manifest keys."""

    files: list[str] = field(default_factory=list)
    # file -> ignored column names, or "N unknown column(s)" when they may not be names
    columns: dict[str, list[str] | str] = field(default_factory=dict)
    manifest_keys: list[str] = field(default_factory=list)


@dataclass
class LoadReport:
    """Never a label: rows kept out per table and reason, per unmapped exchange code and per
    unknown ``source_action_type``; what was ignored (``validate``); the fetch-log summary."""

    dropped: dict[str, dict[str, int]] = field(default_factory=dict)
    unmapped_exchanges: dict[str, dict[str, int]] = field(default_factory=dict)
    unknown_action_types: dict[str, int] = field(default_factory=dict)
    ignored: Ignored = field(default_factory=Ignored)
    fetch_log: dict[str, int] = field(default_factory=dict)
    # distinct spinoff_member_id values absent from securities (tokens computed, no labels)
    unlisted_spinoff_targets: int = 0
    # cash actions the total-return recipe would never apply, per ``quality`` reason (rows)
    unusable_cash: dict[str, int] = field(default_factory=dict)


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


def _where(fname: str, i: int) -> str:
    """Position of the 0-based data row ``i`` of a file, for messages (never its content)."""
    if fname.endswith(".csv"):
        return f"data row {i + 1} (file line {i + 2})"
    return f"row {i + 1}"


def _chunks(col: pa.ChunkedArray | pa.Array) -> list[pa.Array]:
    return col.chunks if isinstance(col, pa.ChunkedArray) else [col]


def _text_map(col, fn: Callable[[pa.Array], pa.Array], typ: pa.DataType) -> pa.ChunkedArray:
    """``fn`` over the plain-string values of a text column (dictionary-aware, per chunk)."""
    out = []
    for c in _chunks(col):
        if pa.types.is_dictionary(c.type):
            out.append(pc.take(fn(pc.cast(c.dictionary, _S)), c.indices))
        else:
            out.append(fn(pc.cast(c, _S)))
    return pa.chunked_array(out, typ)


def _sentinel_mask(col, text: bool) -> pa.ChunkedArray:
    """Cells that are an "unknown" placeholder, compared after trim + casefold: in text columns
    ``TEXT_SENTINELS`` or a ``#n/a`` prefix (``na`` is a real code there); in numeric and date
    columns ``NUMERIC_SENTINELS``."""

    def fn(arr: pa.Array) -> pa.Array:
        norm = pc.utf8_lower(pc.utf8_trim_whitespace(arr))
        found = pc.is_in(norm, value_set=pa.array(TEXT_SENTINELS if text else NUMERIC_SENTINELS))
        if text:
            found = pc.or_(found, pc.starts_with(norm, TEXT_SENTINEL_PREFIX))
        return found

    return _text_map(col, fn, pa.bool_())


def _sentinel_problems(fname: str, table: pa.Table, spec: _Spec) -> list[str]:
    out = []
    for name in table.column_names:
        col = table[name]
        if not _is_text(col.type):
            continue
        row = _first(_sentinel_mask(col, spec.columns[name] == _S))
        if row is not None:
            cell = str(col[row].as_py()).strip()
            shown = cell if len(cell) <= 4 else cell[:4] + "..."  # a sentinel, never a label
            out.append(
                f"{fname}: column {name!r} holds {shown!r} at {_where(fname, row)}: empty means "
                "unknown (re-export with empty cells)"
            )
    return out


def _parses(value: str, target: pa.DataType) -> bool:
    try:
        if target == _D:
            date.fromisoformat(value.strip())
        elif target == _I:
            f = float(value)
            return f == int(f)
        else:
            float(value)
    except (ValueError, OverflowError):
        return False
    return True


def _first_unparsable(col, target: pa.DataType) -> int | None:
    """First row of a text column that does not parse as ``target`` (error path only)."""
    for i, v in enumerate(pc.cast(col, _S).to_pylist()):
        if v is not None and not _parses(v, target):
            return i
    return None


def _first_bad_utf8(col: pa.ChunkedArray) -> int | None:
    for i, v in enumerate(col.to_pylist()):
        if v is None:
            continue
        try:
            v.decode("utf-8")
        except UnicodeDecodeError:
            return i
    return None


_MEMBER_COLUMNS = ("member_id", "spinoff_member_id")
_TYPE_NAMES = {_D: "a date (YYYY-MM-DD)", _F: "a number", _I: "a whole number", _S: "text"}


def _isin_ok(v: str) -> bool:
    if not _ISIN.fullmatch(v):
        return False
    digits = "".join(str(int(ch, 36)) for ch in v)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch) * (2 if i % 2 else 1)
        total += n - 9 if n > 9 else n
    return total % 10 == 0


def _figi_ok(v: str) -> bool:
    return bool(_FIGI.fullmatch(v))


_FORMATS: Mapping[str, tuple[Callable[[str], bool], str]] = {
    "isin": (_isin_ok, "ISIN (2 letters, 9 letters or digits, a valid check digit)"),
    "figi": (_figi_ok, "FIGI (BBG + 9 letters or digits)"),
    "composite_figi": (_figi_ok, "FIGI (BBG + 9 letters or digits)"),
    "share_class_figi": (_figi_ok, "FIGI (BBG + 9 letters or digits)"),
}


def _format_problems(fname: str, table: pa.Table) -> list[str]:
    out = []
    for name, (ok, expected) in _FORMATS.items():
        if name not in table.column_names:
            continue
        for i, v in enumerate(table[name].to_pylist()):
            if v is not None and not ok(v):
                out.append(
                    f"{fname}: column {name!r} holds an invalid {expected} at {_where(fname, i)}"
                )
                break
    return out


def _header_problems(fname: str, names: list[str], spec: _Spec) -> list[str]:
    out = []
    if len(names) > 1 and names[-1] == "":
        out.append(
            f"{fname}: the header ends with a trailing comma (an empty column name): re-export "
            "without it"
        )
        names = names[:-1]
    numbered = [i + 1 for i, n in enumerate(names) if _ROW_NUMBER.match(n.strip())]
    if numbered:
        out.append(
            f"{fname}: unnamed row-number column at position {numbered[0]}: re-export as "
            "comma-separated CSV or Parquet without the row number"
        )
    dup = [i + 1 for i, n in enumerate(names) if names.index(n) != i]
    if dup:
        out.append(f"{fname}: duplicated column name(s) at position(s) {dup}")
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
    try:
        names = next(csv.reader(io.StringIO(line)))
    except csv.Error:
        return [], [
            f"{path.name}: the first line cannot be parsed as a CSV header (a field too long, "
            "or broken quoting)"
        ]
    if "\t" in line:
        numbered = bool(_ROW_NUMBER.match(line.split("\t")[0].strip()))
        return [], [
            f"{path.name}: the file is tab-separated (despite its name): re-export as "
            "comma-separated CSV or Parquet"
            + (", without the leading row-number column" if numbered else "")
            + "; a row-number column is refused too"
        ]
    if ";" in line and len(names) == 1:
        return [], [
            f"{path.name}: the file is semicolon-separated: re-export as comma-separated CSV "
            "(with '.' decimals) or Parquet"
        ]
    return names, []


def _columns(
    fname: str, names: list[str], spec: _Spec, ignored: Ignored
) -> tuple[list[str], list[str]]:
    """(spec columns among ``names``, problems). Other names are listed in ``ignored`` and not
    read, but only once the line is known to be a header: with none of the required columns
    it is probably a data row, and nothing of it is recorded or echoed."""
    if not any(c in names for c in spec.required):
        return [], [
            f"{fname}: no header line? none of the required columns {list(spec.required)} is in "
            "the first line"
        ]
    problems = _header_problems(fname, names, spec)
    unknown = [n for n in names if n not in spec.columns and n != ""]
    if unknown:
        # a wide or transposed file has member ids as header names: names are recorded only
        # from a sound header and when every one looks like a column name; else only a count
        named = not problems and all(_COLUMN_NAME.fullmatch(n.casefold()) for n in unknown)
        ignored.columns[fname] = unknown if named else f"{len(unknown)} unknown column(s)"
    return [n for n in names if n in spec.columns], problems


def _diagnose_csv(path: Path, names: list[str], spec: _Spec) -> list[str]:
    """Why a CSV file failed to convert, by position only (error path: pyarrow's own message
    quotes the offending row or value, so it is never shown)."""
    bad: list[tuple[int | None, int, int]] = []

    def handler(row) -> str:
        if not bad:
            bad.append((row.number, row.expected_columns, row.actual_columns))
        return "skip"

    try:
        raw = pv.read_csv(
            path,
            read_options=pv.ReadOptions(use_threads=False),
            parse_options=pv.ParseOptions(invalid_row_handler=handler),
            convert_options=pv.ConvertOptions(
                column_types={n: pa.binary() for n in names},
                include_columns=names,
                null_values=[""],
                strings_can_be_null=True,
            ),
        )
    except pa.ArrowInvalid:
        return [f"{path.name}: cannot be parsed as comma-separated CSV (check quoting)"]
    if bad:
        line, expected, actual = bad[0]
        at = f"line {line}" if line is not None else "a line"
        return [
            f"{path.name}: malformed {at}: {actual} field(s) where the header has {expected} "
            "(check separators, quoting and trailing commas)"
        ]
    out, strings = [], {}
    for n in names:
        try:
            strings[n] = pc.cast(raw[n], _S)
        except pa.ArrowInvalid:
            row = _first_bad_utf8(raw[n])
            at = _where(path.name, row) if row is not None else "some row"
            out.append(f"{path.name}: column {n!r} is not valid UTF-8 at {at}")
    if out:
        return out
    out = _sentinel_problems(path.name, pa.table(strings), spec)
    if out:
        return out
    for n in names:
        target = spec.columns[n]
        if target == _S:
            continue
        row = _first_unparsable(strings[n], target)
        if row is not None:
            out.append(
                f"{path.name}: column {n!r} cannot be read as {_TYPE_NAMES[target]} at "
                f"{_where(path.name, row)}"
            )
    return out or [f"{path.name}: cannot be read as comma-separated CSV"]


def _read_csv(path: Path, spec: _Spec, ignored: Ignored) -> tuple[pa.Table | None, list[str]]:
    names, problems = _csv_header(path)
    if problems:
        return None, problems
    names, problems = _columns(path.name, names, spec, ignored)
    if problems:
        return None, problems
    read_types = {}
    for n in names:
        t = spec.columns[n]
        # integers read as float first: a "1234.0" volume is accepted, a fractional one refused
        read_types[n] = _MEMBER if n in spec.dictionary else (_F if t == _I else t)
    try:
        table = pv.read_csv(
            path,
            convert_options=pv.ConvertOptions(
                column_types=read_types,
                null_values=[""],
                strings_can_be_null=True,
                quoted_strings_can_be_null=True,
                include_columns=names,
            ),
        )
    except pa.ArrowInvalid:
        return None, _diagnose_csv(path, names, spec)
    return table, []


def _to_date(col: pa.ChunkedArray) -> pa.ChunkedArray:
    t = col.type
    if pa.types.is_timestamp(t):
        if t.tz is not None:
            raise ValueError("timezone-aware times are not dates")
        floor = pc.floor_temporal(col, unit="day")
        if not pc.all(pc.fill_null(pc.equal(floor, col), True)).as_py():
            raise ValueError("a time of day is not a date")
        return pc.cast(col, _D, safe=False)
    return pc.cast(col, _D)


def _empty_to_null(col) -> pa.ChunkedArray:
    """An empty string is an empty cell (Parquet writers often keep ""); dictionary-aware."""
    out = []
    for c in _chunks(col):
        if pa.types.is_dictionary(c.type):
            values = pc.cast(c.dictionary, _S)
            empty = pc.take(pc.equal(values, ""), c.indices)
            indices = pc.if_else(empty, pa.scalar(None, c.indices.type), c.indices)
            out.append(pa.DictionaryArray.from_arrays(indices, values))
        else:
            c = pc.cast(c, _S)
            out.append(pc.if_else(pc.equal(c, ""), pa.scalar(None, _S), c))
    return pa.chunked_array(out, out[0].type) if out else pa.chunked_array([], _S)


def _normalise(fname: str, table: pa.Table, spec: _Spec) -> tuple[pa.Table | None, list[str]]:
    """Cast every column to its spec type; check sentinels, NaN, formats and required values.
    Messages give the file, column and row, never a cell's content."""
    problems = _sentinel_problems(fname, table, spec)
    if problems:
        return None, problems
    cols = {}
    for name in table.column_names:
        target, col = spec.columns[name], table[name]
        try:
            if target != _S and _is_text(col.type):
                col = _empty_to_null(col)  # empty = unknown, whatever the column's type
            if target == _D:
                col = _to_date(col)
            elif target == _S:
                if not _is_text(col.type):
                    col = pc.cast(col, _S)
                col = _empty_to_null(col)
                if name not in spec.dictionary and pa.types.is_dictionary(col.type):
                    col = pc.cast(col, _S)  # e.g. a categorical member_id in securities
            else:
                col = pc.cast(col, target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
            # only _to_date's own ValueError text is shown; pyarrow's may quote a cell
            reason = str(exc) if type(exc) is ValueError else ""
            row = _first_unparsable(col, target) if _is_text(col.type) else None
            at = f" at {_where(fname, row)}" if row is not None else ""
            problems.append(
                f"{fname}: column {name!r} cannot be read as {_TYPE_NAMES[target]}{at}"
                + (f" ({reason})" if reason else "")
            )
            continue
        if name in _MEMBER_COLUMNS:
            blank = _text_map(col, lambda a: pc.equal(pc.utf8_trim_whitespace(a), ""), pa.bool_())
            row = _first(blank)
            if row is not None:
                problems.append(
                    f"{fname}: column {name!r} is blank (whitespace only) at {_where(fname, row)}"
                )
        if pa.types.is_floating(col.type):
            row = _first(pc.invert(pc.is_finite(col)))
            if row is not None:
                problems.append(
                    f"{fname}: column {name!r} holds NaN or an infinite number at "
                    f"{_where(fname, row)}"
                )
        if name in spec.dictionary and col.type != _MEMBER:
            col = pc.cast(col, _MEMBER)
        cols[name] = col
    for name in spec.non_null:
        if name in cols and cols[name].null_count:
            row = _first(pc.is_null(cols[name]))
            problems.append(
                f"{fname}: required column {name!r} is empty in {cols[name].null_count} row(s), "
                f"first at {_where(fname, row)}"
            )
    if not problems:
        problems = _format_problems(fname, pa.table(cols))
    if problems:
        return None, problems
    return pa.table(cols), []


def _read_parquet(path: Path, spec: _Spec, ignored: Ignored) -> tuple[pa.Table | None, list[str]]:
    try:
        schema = pq.read_schema(path)
    except (pa.ArrowInvalid, OSError):
        return None, [f"{path.name}: cannot be read as Parquet (not Parquet, or corrupted)"]
    known, problems = _columns(path.name, schema.names, spec, ignored)
    if problems:
        return None, problems
    dictionary = [c for c in spec.dictionary if c in known and _is_text(schema.field(c).type)]
    try:
        return pq.read_table(path, columns=known, read_dictionary=dictionary), []
    except (pa.ArrowInvalid, OSError):
        return None, [f"{path.name}: cannot be read as Parquet (corrupted, or invalid UTF-8 text)"]


def _read_manifest(directory: Path, ignored: Ignored) -> tuple[Manifest | None, list[str]]:
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
            try:
                rows = list(csv.reader(io.StringIO(path.read_text(encoding="utf-8-sig"))))
            except csv.Error:
                return None, [
                    f"{path.name}: cannot be parsed as CSV (a field too long, or broken quoting)"
                ]
            if not rows or [c.strip() for c in rows[0]] != ["key", "value"]:
                return None, [f"{path.name}: header must be 'key,value'"]
            raw = {}
            for row in rows[1:]:
                if not row:
                    continue
                if len(row) != 2:
                    return None, [f"{path.name}: each line must be 'key,value'"]
                if row[0].strip() in raw:
                    return None, [f"{path.name}: a key is given twice"]
                raw[row[0].strip()] = row[1]
    except UnicodeDecodeError as exc:
        return None, [f"{path.name}: not valid UTF-8 (byte offset {exc.start})"]
    except ValueError as exc:  # JSON syntax: position only
        where = f" (line {exc.lineno}, column {exc.colno})" if hasattr(exc, "lineno") else ""
        return None, [f"{path.name}: cannot be read{where}"]
    problems = []
    ignored.manifest_keys = sorted(set(raw) - {*_MANIFEST_REQUIRED, *_MANIFEST_OPTIONAL})
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
            f"{path.name}: adjustment must be 'none' (raw, unadjusted prices): the whole load "
            "is refused"
        )
    if "dates_timezone" in values and values["dates_timezone"].casefold() != DATES_TIMEZONE:
        problems.append(
            f"{path.name}: dates_timezone must be {DATES_TIMEZONE!r} (dates are each exchange's "
            "local trading dates): no other convention is supported"
        )
    if "dividends" in values and values["dividends"].casefold() not in _DIVIDENDS:
        problems.append(f"{path.name}: dividends must be 'gross' or 'net'")
    basis = values.get("per_share_basis_default")
    if basis is not None and basis.casefold() not in PER_SHARE_BASES:
        problems.append(f"{path.name}: per_share_basis_default must be one of {PER_SHARE_BASES}")
    if problems:
        return None, problems
    return (
        Manifest(
            export_time=export_time,
            adjustment=values["adjustment"].casefold(),
            dates_timezone=values["dates_timezone"].casefold(),
            dividends=values["dividends"].casefold(),
            source_description=values["source_description"],
            fields=values.get("fields"),
            notes=values.get("notes"),
            per_share_basis_default=None if basis is None else basis.casefold(),
        ),
        [],
    )


def _natural(path: Path) -> list:
    """Natural order of part files: part2 before part10."""
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", path.name)]


def _discover(directory: Path, ignored: Ignored) -> tuple[dict[str, list[Path]], list[str]]:
    files: dict[str, list[Path]] = {}
    problems = []
    data = sorted(
        (p for p in directory.iterdir() if p.suffix in (".csv", ".parquet")), key=_natural
    )
    claimed: set[Path] = set()
    for name in _SPECS:
        pattern = re.compile(rf"{re.escape(name)}(_part[0-9]+)?\.(csv|parquet)")
        parts = [p for p in data if pattern.fullmatch(p.name)]
        claimed.update(parts)
        if {p.suffix for p in parts} == {".csv", ".parquet"}:
            problems.append(f"{name}: both CSV and Parquet files; keep one format per table")
        if parts:
            files[name] = parts
    manifests = {"manifest.json", "manifest.csv"}
    ignored.files = sorted(
        p.name for p in directory.iterdir() if p not in claimed and p.name not in manifests
    )
    for name in _REQUIRED_FILES:
        if name not in files:
            problems.append(f"{name}: required file is missing")
    if not any(name in files for name in _ONE_OF_FILES):
        problems.append(f"one of {list(_ONE_OF_FILES)} is required")
    return files, problems


def _read_table(
    name: str, parts: list[Path], ignored: Ignored
) -> tuple[pa.Table | None, list[str]]:
    spec = _SPECS[name]
    tables, problems = [], []
    for path in parts:
        reader = _read_parquet if path.suffix == ".parquet" else _read_csv
        table, found = reader(path, spec, ignored)
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
            if c in spec.key:  # group on unified integer codes, not on 20M strings
                keyed = keyed.set_column(
                    keyed.schema.get_field_index(c), c, _member_index(keyed[c])[0]
                )
        counts = keyed.group_by(list(spec.key)).aggregate([([], "count_all")])
        n = pc.sum(pc.greater(counts["count_all"], 1)).as_py() or 0
        if n:
            return None, [f"{name}: {n} duplicate ({', '.join(spec.key)}) key(s)"]
    return table, []


# --- vectorised helpers ------------------------------------------------------------------------


def _const(value: Any, n: int, typ: pa.DataType) -> pa.Array:
    if value is None:
        return pa.nulls(n, typ)
    if (
        typ == _S and n * len(value.encode()) < 2**31
    ):  # built from buffers: a 20M-row constant column in a fraction of a second
        raw = value.encode()
        offsets = np.arange(n + 1, dtype=np.int32) * len(raw)
        return pa.Array.from_buffers(_S, n, [None, pa.py_buffer(offsets), pa.py_buffer(raw * n)])
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
class LoadResult:
    """Every table of one load (contract schemas; observations in ``OBSERVATION_SCHEMA``) and
    the filled load report."""

    prices: pa.Table
    corporate_actions: pa.Table
    vendor_total_return: pa.Table
    index_snapshots: pa.Table
    observations: pa.Table
    fetch_log: pa.Table
    report: LoadReport


@dataclass(frozen=True)
class _Member:
    token: str
    exchange_code: str
    mic: str | None


def _check_mics(mics) -> None:
    import exchange_calendars as xcals

    known = set(xcals.get_calendar_names(include_aliases=True))
    unknown = sorted({m for m in mics if m not in known})
    if unknown:
        raise ValueError(f"exchange_mics names unknown calendar MIC(s) {unknown}")


_UNUSED = _Member(token="", exchange_code=NO_EXCHANGE_CODE, mic=None)


class FileHistory:
    """An export directory in the history-file format (module docstring).

    ``salt``: token salt bytes; None reads ``MOF_MARKET_TOKEN_SALT`` (missing or shorter than
    ``MIN_SALT_BYTES`` bytes: ``ValueError``). ``exchange_mics``: exchange code -> calendar MIC.
    ``action_types``: ``source_action_type`` (case-insensitive) -> ``action_type`` (default: the
    package data file ``file_action_types.json``). ``next_session_margin``: the actions calendar
    margin (as the provider builder's). ``max_non_session_share``: the non-session tripwire
    (default ``MAX_NON_SESSION_SHARE``, 0.5 %).

    Entry point: ``build_all(load_time)`` validates, builds every table and returns them with the
    filled ``LoadReport``; the per-table methods are its parts.

    ``corroborates_delisting`` is True: a member's window ends at its last filed price and its
    fetch-log row at the export date, so an ``ok``/``empty`` row past the window corroborates a
    delisting (``quality.fetch_coverage`` rule 6)."""

    corroborates_delisting = True

    def __init__(
        self,
        directory: Path | str,
        source_code: str = "private_history",
        *,
        salt: bytes | None = None,
        exchange_mics: Mapping[str, str] = DEFAULT_EXCHANGE_MICS,
        action_types: Mapping[str, str] | None = None,
        next_session_margin: timedelta = NEXT_SESSION_MARGIN,
        max_non_session_share: float = MAX_NON_SESSION_SHARE,
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
        _check_mics(self.exchange_mics.values())
        self.action_types = {_norm_type(k): v for k, v in types.items()}
        self.next_session_margin = next_session_margin
        if not 0 <= max_non_session_share <= 1:
            raise ValueError("max_non_session_share must be within [0, 1]")
        self.max_non_session_share = max_non_session_share
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
        ignored = self.report.ignored
        manifest, problems = _read_manifest(self.directory, ignored)
        files, found = _discover(self.directory, ignored)
        problems += found
        raw: dict[str, pa.Table] = {}
        for name, parts in files.items():
            table, found = _read_table(name, parts, ignored)
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
        out = []
        if "per_share_basis" in table.column_names:
            for i, v in enumerate(table["per_share_basis"].to_pylist()):
                if v is not None and v.strip().casefold() not in PER_SHARE_BASES:
                    out.append(
                        f"corporate_actions: row {i + 1} (over all parts): per_share_basis is "
                        f"not one of {PER_SHARE_BASES}"
                    )
        if "action_type" not in table.column_names:
            return out
        given = table["action_type"].to_pylist()
        source = table["source_action_type"].to_pylist()
        for i, (a, s) in enumerate(zip(given, source, strict=True)):
            if a is None:
                continue
            mapped = self.action_types.get(_norm_type(s))
            if a not in ACTION_TYPES:
                out.append(
                    f"corporate_actions: row {i + 1} (over all parts): action_type is not one "
                    f"of {ACTION_TYPES}"
                )
            elif mapped is not None and mapped != a:
                out.append(
                    f"corporate_actions: row {i + 1} (over all parts): action_type disagrees "
                    f"with its source_action_type, which maps to {mapped!r}"
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
        if code != NO_EXCHANGE_CODE and not _EXCHANGE_CODE.fullmatch(code):
            code = INVALID_EXCHANGE_CODE  # never a raw word of a member id in the report
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
        # validation guarantees every row's member is in securities; a dictionary entry that no
        # row uses (e.g. an unused pandas category) may not be, and is never referenced
        return idx, [self._members.get(m, _UNUSED) for m in ids]

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
        non_session = _count(pc.and_(mapped, pc.is_null(known)))
        self.report.dropped[table] = {
            DROP_UNMAPPED_EXCHANGE: n_unmapped,
            DROP_NON_SESSION: non_session,
            DROP_AFTER_EXPORT: _count(after),
        }
        keep = pc.fill_null(pc.invert(after), False)  # null known (unmapped, non-session) out
        loaded = _count(keep)
        if non_session > self.max_non_session_share * loaded:
            raise HistoryFileError(
                [
                    f"{table}: {non_session} row(s) on dates that are not sessions of their "
                    f"exchange for {loaded} loaded row(s), above max_non_session_share "
                    f"{self.max_non_session_share:.2%}: are the dates exchange-local trading "
                    "dates? (raise max_non_session_share only after checking)"
                ]
            )
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
        # no calendar needed: every filed row is kept, whatever the member's exchange code
        idx, members = self._members_of(raw["member_id"])
        self.report.dropped["index_snapshots"] = {}
        kept = pa.table(
            {
                "index_code": raw["index"],
                "source_symbol": pc.take(pa.array([m.token for m in members], _S), idx),
                "valid_date": raw["date"],
                "weight": raw["weight"]
                if "weight" in raw.column_names
                else pa.nulls(raw.num_rows, _F),
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
        newest = pc.max(prev["known_at"]).as_py() if prev.num_rows else None
        if newest is not None and load_time <= newest:
            raise ValueError(
                "load_time must be strictly after every known_at of previous_snapshots "
                f"(latest {newest.isoformat()})"
            )
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
        seen: set[tuple] = set()
        duplicates = 0
        self.report.unlisted_spinoff_targets = len(
            {
                r["spinoff_member_id"]
                for r in rows
                if r.get("spinoff_member_id") is not None
                and r["spinoff_member_id"] not in self._members
            }
        )
        for r in rows:
            whole = tuple(r.items())
            if whole in seen:  # an exact duplicate row: kept once, counted
                duplicates += 1
                continue
            seen.add(whole)
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
            DROP_DUPLICATE_ROW: duplicates,
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
            share_basis = None
            if amount is not None:
                given = r.get("per_share_basis")
                share_basis = (
                    given.strip().casefold() if given else manifest.per_share_basis_default
                )
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
                    "per_share_basis": share_basis,
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
        table = pa.Table.from_pylist(out, schema=CORPORATE_ACTIONS.schema)
        self.report.unusable_cash = dict(
            sorted(unusable_cash(table).get(self.source_code, {}).items())
        )
        return table

    # -- entry point ---------------------------------------------------------------------------

    def build_all(
        self, load_time: datetime, *, previous_snapshots: pa.Table | None = None
    ) -> LoadResult:
        """The entry point: validate (``HistoryFileError`` listing every problem), then build
        every table at ``load_time`` (observations, fetch log, and a restatement against
        ``previous_snapshots`` when given) with the filled ``LoadReport``."""
        self._load()
        self._check_load_time(load_time)
        snapshots = (
            self.snapshots()
            if previous_snapshots is None
            else self.snapshots(previous_snapshots, load_time=load_time)
        )
        return LoadResult(
            prices=self.prices(),
            corporate_actions=self.actions(),
            vendor_total_return=self.vendor_total_return(),
            index_snapshots=snapshots,
            observations=self.observations(load_time),
            fetch_log=self.fetch_log(load_time),
            report=self.report,
        )

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
        """``fetch_log`` rows: one per member with filed prices or snapshots (unmapped exchange
        codes included), so the coverage ledger can corroborate a delisting. ``requested_from``
        = the member's earliest filed price or snapshot date; ``status`` ``ok`` with loaded
        prices (``row_count`` and ``returned_from``/``to`` over the loaded rows), ``empty``
        without. ``report.fetch_log`` counts the members, the ``empty`` ones whose filed prices
        were not loaded and those unloaded price rows."""
        self._load()
        load = self._check_load_time(load_time)
        manifest = self.manifest
        spans: dict[str, dict[str, Any]] = {}

        def span(tok: str, first: date) -> dict[str, Any]:
            s = spans.setdefault(
                tok, {"first": first, "returned": (None, None), "count": 0, "filed": 0}
            )
            s["first"] = min(s["first"], first)
            return s

        for name in ("prices_daily", "index_snapshots"):
            if name not in self._raw:
                continue
            raw = self._raw[name]
            idx, members = self._members_of(raw["member_id"])
            agg = (
                pa.table({"i": idx, "d": raw["date"]})
                .group_by("i")
                .aggregate([("d", "min"), ("d", "count")])
            )
            for r in agg.to_pylist():
                s = span(members[r["i"]].token, r["d_min"])
                if name == "prices_daily":
                    s["filed"] = r["d_count"]
        prices = self.prices()
        if prices.num_rows:
            agg = prices.group_by("source_symbol").aggregate(
                [("valid_date", "min"), ("valid_date", "max"), ("valid_date", "count")]
            )
            for r in agg.to_pylist():
                s = spans[r["source_symbol"]]
                s["returned"] = (r["valid_date_min"], r["valid_date_max"])
                s["count"] = r["valid_date_count"]
        empty = [s for s in spans.values() if not s["count"]]
        self.report.fetch_log = {
            "members": len(spans),
            "ok": len(spans) - len(empty),
            "empty": len(empty),
            "empty_with_unloaded_prices": sum(1 for s in empty if s["filed"]),
            "unloaded_price_rows": sum(s["filed"] for s in empty),
        }
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

"""Run reports (spec §12): what one run built, kept out, could not map, flagged and covered.

Silent-failure doctrine: a run is never ``ok`` by default. ``RunSummary.outcome`` is

- ``error`` when the run raised (``error`` holds the exception's type name only: messages can
  quote keys, i.e. symbols);
- ``ok`` only when there is no error and the evidence is there and clean: a mapping was recorded
  with no unresolved or ambiguous identifier (``MappingResult.unresolved`` / ``ambiguous``, and
  overlapping mapping windows); a prices fetch log was recorded (a non-empty
  ``fetch_logs["prices"]``, or the history-file load's fetch-log summary with members) with no
  unsuperseded failed fetch of any kind; the coverage, when given, has sessions and passed (a
  whole-run report, ``coverage_required``, must have one: else ``coverage_not_run``);
- ``degraded`` otherwise; ``reasons`` says why (``REASONS``).

Kept-out rows, builder drops, changed mappings and quality flags are counted but do not degrade a
run: they are flagged, not hidden (§12).

Label-free: a summary holds counts, closed vocabularies, source codes, contract names, session
dates and booleans only — never a source symbol, a member id, an exchange code, an export's
action-type text, a column or file name. ``summarize`` maps every caller-provided key to a known
vocabulary (unknown reasons become ``"other"``) or to a count; ``RunSummary`` validates its
fields at construction (only known keys, counts as non-negative ints, source codes as short
lowercase codes), so no free text reaches a report even though reports live on a private path
(``MOF_MARKET_REPORTS``).

``write_report(run, dir)`` writes ``<run_id>-report.json`` (machine) and ``<run_id>-report.md``
(human), atomically, into ``dir`` or ``MOF_MARKET_REPORTS`` (no default), never inside a git
checkout.
"""

from __future__ import annotations

import dataclasses
import json
import math
import os
import re
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from open_data_store.contracts.standard.market import LINK_REASONS, MARKET_CONTRACTS
from open_data_store.market.coverage import CoverageResult

from market_data.adapters import file as _file
from market_data.adapters import massive as _massive
from market_data.adapters import tiingo as _tiingo
from market_data.adapters.base import FETCH_KINDS, FetchResult
from market_data.build import DROP_REASONS, ROW_STATUSES, BuildResult
from market_data.quality import CHECKS, ambiguous_mappings, unsuperseded_failures
from market_data.resolve import mapping as _mapping

__all__ = [
    "DEFAULT_MAX_LISTED_SESSIONS",
    "OTHER",
    "OUTCOMES",
    "REASONS",
    "REPORTS_ENV",
    "RunSummary",
    "kept_out_counts",
    "summarize",
    "write_report",
]

REPORTS_ENV = "MOF_MARKET_REPORTS"
OUTCOMES = ("ok", "degraded", "error")
REASONS = (
    "mapping_not_recorded",
    "unresolved_identifiers",
    "ambiguous_identifiers",
    "fetch_logs_not_recorded",
    "failed_fetches",
    "coverage_empty",
    "coverage_failed",
    "coverage_not_run",
)
# Over-threshold session dates listed in a report (the count is always given).
DEFAULT_MAX_LISTED_SESSIONS = 20
OTHER = "other"  # any key outside a known vocabulary

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SOURCE = re.compile(r"[a-z][a-z0-9_]{0,31}")
_CONTRACTS = tuple(c.name for c in MARKET_CONTRACTS)
_KEPT_OUT = tuple(sorted({*_massive.KEPT_OUT_REASONS, *_tiingo.KEPT_OUT_REASONS, OTHER}))
_FILE_TABLES = (*_CONTRACTS, OTHER)
_FILE_DROPS = (
    _file.DROP_DUPLICATE_ROW,
    _file.DROP_NON_SESSION,
    _file.DROP_AFTER_EXPORT,
    _file.DROP_UNMAPPED_EXCHANGE,
    _file.DROP_UNKNOWN_ACTION_TYPE,
    OTHER,
)
_FILE_FETCH_LOG = (
    "members",
    "ok",
    "empty",
    "empty_with_unloaded_prices",
    "unloaded_price_rows",
    OTHER,
)
_MAPPING_REASONS = tuple(
    sorted(
        {
            getattr(_mapping, name)
            for name in dir(_mapping)
            if name.startswith(("UNRESOLVED_", "AMBIGUOUS_", "CHANGED_", "LABEL_", "LISTED_"))
            and isinstance(getattr(_mapping, name), str)
        }
        | set(LINK_REASONS)
        | {OTHER}
    )
)
_MAPPING_TABLES = ("unresolved", "ambiguous", "changed", "labels_left_out", "listed_not_priced")
_QUALITY = (*CHECKS, OTHER)
_BUILD_DROPS = (*DROP_REASONS, OTHER)


def _aware(value: datetime | None, name: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{name} must be timezone-aware")


# --- validation ---------------------------------------------------------------------------------

Check = Callable[[Any, str], None]


def _fail(path: str, what: str) -> None:
    raise ValueError(f"RunSummary.{path}: {what}")


def _count(value: Any, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(path, f"expected a non-negative int, got {type(value).__name__}")


def _source_key(key: Any) -> bool:
    return isinstance(key, str) and bool(_SOURCE.fullmatch(key))


def _in(vocab: Iterable[str]) -> Callable[[Any], bool]:
    allowed = frozenset(vocab)
    return lambda key: key in allowed


def _tree(*levels: Callable[[Any], bool], leaf: Check = _count) -> Check:
    """A nested mapping whose keys at depth i satisfy levels[i] and whose leaves pass ``leaf``."""

    def check(value: Any, path: str) -> None:
        if not levels:
            leaf(value, path)
            return
        if not isinstance(value, Mapping):
            _fail(path, "expected a mapping")
        for key, sub in value.items():
            if not levels[0](key):
                _fail(path, "unknown key (keys come from a closed vocabulary)")
            _tree(*levels[1:], leaf=leaf)(sub, f"{path}[{key}]")

    return check


def _record(fields: Mapping[str, Check]) -> Check:
    """A mapping with exactly ``fields`` (each validated)."""

    def check(value: Any, path: str) -> None:
        if not isinstance(value, Mapping) or set(value) != set(fields):
            _fail(path, f"expected exactly the keys {sorted(fields)}")
        for key, sub_check in fields.items():
            sub_check(value[key], f"{path}.{key}")

    return check


def _share(value: Any, path: str) -> None:
    if value is not None and not (isinstance(value, float) and math.isfinite(value)):
        _fail(path, "expected a finite float or None")


def _flag(value: Any, path: str) -> None:
    if not isinstance(value, bool):
        _fail(path, "expected a bool")


def _iso_dates(value: Any, path: str) -> None:
    if not isinstance(value, list):
        _fail(path, "expected a list of ISO dates")
    for v in value:
        try:
            ok = isinstance(v, str) and date.fromisoformat(v).isoformat() == v
        except ValueError:
            ok = False
        if not ok:
            _fail(path, "expected ISO dates (YYYY-MM-DD)")


_KEYS_ROWS = _record({"keys": _count, "rows": _count})
_DISTINCT_ROWS = _record({"distinct": _count, "rows": _count})
_PER_SOURCE_REASON = _tree(_source_key, _in(_MAPPING_REASONS))
_PER_KIND_SOURCE = _tree(_in(FETCH_KINDS), _source_key)
_SECTIONS: dict[str, Check] = {
    "builder_drops": _tree(_in(_CONTRACTS), _in(_BUILD_DROPS)),
    "kept_out": _tree(_source_key, _in(FETCH_KINDS), _in(_KEPT_OUT), leaf=_KEYS_ROWS),
    "file_load": _record(
        {
            "dropped": _tree(_in(_FILE_TABLES), _in(_FILE_DROPS)),
            "unmapped_exchanges": _tree(_in(_FILE_TABLES), leaf=_DISTINCT_ROWS),
            "unknown_action_types": _DISTINCT_ROWS,
            "ignored": _record(
                {
                    "files": _count,
                    "column_files": _count,
                    "columns": _count,
                    "manifest_keys": _count,
                }
            ),
            "fetch_log": _tree(_in(_FILE_FETCH_LOG)),
            "unlisted_spinoff_targets": _count,
        }
    ),
    "mapping": _record(
        {
            **dict.fromkeys(_MAPPING_TABLES, _PER_SOURCE_REASON),
            "minted": _count,
            "overlapping_windows": _count,
        }
    ),
    "quality": _tree(_in(_QUALITY)),
    "coverage": _record(
        {
            "passed": _flag,
            "worst_missing_share": _share,
            "sessions": _count,
            "sessions_over_threshold": _count,
            "over_threshold_sessions": _iso_dates,
        }
    ),
    "fetches": _PER_KIND_SOURCE,
    "failed_fetches": _PER_KIND_SOURCE,
    "written": _tree(_in(_CONTRACTS)),
}


@dataclass(frozen=True)
class RunSummary:
    """Counts of one run (see the module docstring). Build one with ``summarize``; every field
    is validated at construction.

    Sections left ``None`` were not recorded: they neither pass nor fail a check of their own,
    but missing mapping or fetch-log evidence keeps the run from ``ok``.

    - ``builder_drops``: contract -> drop reason -> distinct keys (``BuildResult.counts()``).
    - ``kept_out``: source_code -> kind -> reason -> ``{"keys": n, "rows": n}``
      (``kept_out_counts``: distinct (symbol, date) keys vs payload rows).
    - ``file_load``: the history-file ``LoadReport`` as counts: ``dropped`` (contract -> reason
      -> rows), ``unmapped_exchanges`` (contract -> distinct codes and rows),
      ``unknown_action_types`` (distinct and rows), ``ignored`` (files, files with ignored
      columns, columns, manifest keys), ``fetch_log`` summary, ``unlisted_spinoff_targets``.
    - ``mapping``: ``unresolved`` / ``ambiguous`` / ``changed``: source_code -> reason -> series;
      ``labels_left_out``: source_code -> reason -> rows; ``listed_not_priced``: source_code
      -> reason -> symbols only listed, never priced (counted, never degrading); ``minted``:
      instruments created;
      ``overlapping_windows``: symbols whose windows overlap on different instruments.
    - ``quality``: check -> flags (every check of ``CHECKS``, plus ``other``).
    - ``coverage``: ``passed``, ``worst_missing_share`` (None without a finite share),
      ``sessions``, ``sessions_over_threshold``, ``over_threshold_sessions`` (ISO dates, the
      first ``DEFAULT_MAX_LISTED_SESSIONS``).
    - ``fetches``: kind -> source_code -> fetch-log rows recorded.
    - ``failed_fetches``: kind -> source_code -> unsuperseded failed fetches.
    - ``written``: contract -> rows written.
    - ``coverage_required``: a whole-run report (the CLI's ``report``): a missing coverage
      section is then the reason ``coverage_not_run``.
    """

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None
    builder_drops: Mapping[str, Any] | None = None
    kept_out: Mapping[str, Any] | None = None
    file_load: Mapping[str, Any] | None = None
    mapping: Mapping[str, Any] | None = None
    quality: Mapping[str, Any] | None = None
    coverage: Mapping[str, Any] | None = None
    fetches: Mapping[str, Any] | None = None
    failed_fetches: Mapping[str, Any] | None = None
    written: Mapping[str, Any] | None = None
    # True for a whole-run report: no coverage then degrades it (``coverage_not_run``).
    coverage_required: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not _RUN_ID.fullmatch(self.run_id):
            raise ValueError(
                "run_id must be 1-128 characters of letters, digits, '.', '_' or '-', "
                "starting with a letter or digit"
            )
        _aware(self.started_at, "started_at")
        if not isinstance(self.coverage_required, bool):
            raise ValueError("coverage_required must be a bool")
        _aware(self.finished_at, "finished_at")
        if self.error is not None and not (
            isinstance(self.error, str) and self.error.isidentifier()
        ):
            raise ValueError("error must be an exception type name (an identifier), not a message")
        for name, check in _SECTIONS.items():
            value = getattr(self, name)
            if value is not None:
                check(value, name)

    @property
    def reasons(self) -> tuple[str, ...]:
        """Why the run is not ``ok`` (empty when it is, or when it raised)."""
        if self.error is not None:
            return ()
        out = []
        if self.mapping is None:
            out.append("mapping_not_recorded")
        else:
            if _total(self.mapping["unresolved"]):
                out.append("unresolved_identifiers")
            if _total(self.mapping["ambiguous"]) or self.mapping["overlapping_windows"]:
                out.append("ambiguous_identifiers")
        provider = _total((self.fetches or {}).get("prices", {}))
        history = (self.file_load or {}).get("fetch_log", {}).get("members", 0)
        if not (provider or history):
            out.append("fetch_logs_not_recorded")
        if self.failed_fetches is not None and _total(self.failed_fetches):
            out.append("failed_fetches")
        if self.coverage is None:
            if self.coverage_required:
                out.append("coverage_not_run")
        else:
            if not self.coverage["sessions"]:
                out.append("coverage_empty")
            elif not self.coverage["passed"]:
                out.append("coverage_failed")
        return tuple(out)

    @property
    def outcome(self) -> str:
        if self.error is not None:
            return "error"
        return "degraded" if self.reasons else "ok"

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready dict: the fields plus ``outcome`` and ``reasons``."""
        out: dict[str, Any] = {
            "run_id": self.run_id,
            "outcome": self.outcome,
            "reasons": list(self.reasons),
            "started_at": self.started_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
            "error": self.error,
        }
        for f in dataclasses.fields(self):
            if f.name not in out:
                out[f.name] = _plain(getattr(self, f.name))
        return out


def _total(nested: Any) -> int:
    if isinstance(nested, Mapping):
        return sum(_total(v) for v in nested.values())
    return int(nested)


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


# --- section builders ---------------------------------------------------------------------------


def _known(key: Any, vocab: Iterable[str]) -> str:
    return key if key in vocab else OTHER


def _source(code: Any) -> str:
    return code if _source_key(code) else OTHER


def _require(key: Any, vocab: Iterable[str], what: str) -> str:
    if key not in vocab:
        raise ValueError(f"unknown {what} {key!r}; expected one of {sorted(vocab)}")
    return key


def _bump(out: dict, key: str, n: int) -> None:
    out[key] = out.get(key, 0) + int(n)


def _add(out: dict[str, dict[str, int]], reason: str, keys: int, rows: int) -> None:
    slot = out.setdefault(reason, {"keys": 0, "rows": 0})
    slot["keys"] += int(keys)
    slot["rows"] += int(rows)


def kept_out_counts(
    adapter: Any,
    fetches: Iterable[FetchResult],
    *,
    kinds: Iterable[str] = ("prices", "actions"),
) -> dict[str, dict[str, dict[str, int]]]:
    """kind -> reason -> ``{"keys": distinct (symbol, date) keys, "rows": payload rows}`` kept
    out by ``adapter.parse_report`` (Massive, Tiingo) over ``fetches`` (the run's archived
    fetches), summed. Only fetches whose status is in ``ROW_STATUSES`` are parsed (the others
    carry no rows). An adapter whose actions come in another kind's payload
    (``actions_fetch_kind``, Tiingo) has those payloads parsed as ``actions`` too. A payload
    unusable as a whole raises ``ValueError``, as it does in the builder. Reasons outside the
    adapters' vocabularies are counted under ``other``."""
    wanted = set(kinds)
    actions_kind = getattr(adapter, "actions_fetch_kind", None)
    out: dict[str, dict[str, dict[str, int]]] = {}
    for result in fetches:
        if result.status not in ROW_STATUSES:
            continue
        views = []
        if result.kind in wanted:
            views.append(result)
        if actions_kind and result.kind == actions_kind and actions_kind != "actions":
            if "actions" in wanted:
                views.append(dataclasses.replace(result, kind="actions"))
        for view in views:
            report = adapter.parse_report(view)
            keys, rows = report.counts(), report.row_counts
            per = out.setdefault(view.kind, {})
            for reason in sorted(set(keys) | set(rows)):
                _add(per, _known(reason, _KEPT_OUT), keys.get(reason, 0), rows.get(reason, 0))
    return out


def _per_source_reason(table: pa.Table, weight: str | None = None) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    if table.num_rows == 0:
        return out
    srcs = table["source_code"].to_pylist()
    reasons = table["reason"].to_pylist()
    weights = table[weight].to_pylist() if weight else [1] * len(srcs)
    for src, reason, n in zip(srcs, reasons, weights, strict=True):
        _bump(out.setdefault(_source(src), {}), _known(reason, _MAPPING_REASONS), n or 0)
    return {s: dict(sorted(r.items())) for s, r in sorted(out.items())}


def _mapping_section(mapping: Any) -> dict[str, Any]:
    return {
        "unresolved": _per_source_reason(mapping.unresolved),
        "ambiguous": _per_source_reason(mapping.ambiguous),
        "changed": _per_source_reason(mapping.changed),
        "labels_left_out": _per_source_reason(mapping.labels_left_out, "rows"),
        "listed_not_priced": _per_source_reason(mapping.listed_not_priced),
        "minted": len(mapping.minted),
        "overlapping_windows": ambiguous_mappings(mapping.identifiers)
        if mapping.identifiers.num_rows
        else 0,
    }


_LEADING_INT = re.compile(r"\s*(\d+)")


def _column_count(cols: Any) -> int:
    if isinstance(cols, (list, tuple)):
        return len(cols)
    match = _LEADING_INT.match(str(cols))  # "N unknown column(s)"
    return int(match.group(1)) if match else 1


def _load_section(report: Any) -> dict[str, Any]:
    dropped: dict[str, dict[str, int]] = {}
    for table, reasons in report.dropped.items():
        per = dropped.setdefault(_known(table, _FILE_TABLES), {})
        for reason, n in reasons.items():
            _bump(per, _known(reason, _FILE_DROPS), n)
    exchanges = {
        _known(table, _FILE_TABLES): {
            "distinct": len(codes),
            "rows": sum(int(n) for n in codes.values()),
        }
        for table, codes in report.unmapped_exchanges.items()
    }
    fetch_log: dict[str, int] = {}
    for key, n in report.fetch_log.items():
        _bump(fetch_log, _known(key, _FILE_FETCH_LOG), n)
    ignored = report.ignored
    return {
        "dropped": dropped,
        "unmapped_exchanges": exchanges,
        "unknown_action_types": {
            "distinct": len(report.unknown_action_types),
            "rows": sum(int(n) for n in report.unknown_action_types.values()),
        },
        "ignored": {
            "files": len(ignored.files),
            "column_files": len(ignored.columns),
            "columns": sum(_column_count(c) for c in ignored.columns.values()),
            "manifest_keys": len(ignored.manifest_keys),
        },
        "fetch_log": fetch_log,
        "unlisted_spinoff_targets": int(report.unlisted_spinoff_targets),
    }


def _quality_section(flags: pa.Table) -> dict[str, int]:
    counts = dict.fromkeys(_QUALITY, 0)
    if flags.num_rows:
        for item in pc.value_counts(flags["check"]).to_pylist():
            _bump(counts, _known(item["values"], CHECKS), item["counts"])
    return counts


def _coverage_section(result: CoverageResult, max_listed: int) -> dict[str, Any]:
    per = result.per_session
    shares = per["missing_share"].to_pylist() if per.num_rows else []
    finite = [s for s in shares if s is not None and math.isfinite(s)]
    over = per.filter(per["over_threshold"]) if per.num_rows else per
    return {
        "passed": bool(result.passed),
        "worst_missing_share": float(max(finite)) if finite else None,
        "sessions": per.num_rows,
        "sessions_over_threshold": over.num_rows,
        "over_threshold_sessions": [
            d.isoformat() for d in over["session_date"].to_pylist()[:max_listed]
        ],
    }


def _fetch_sections(
    fetch_logs: Mapping[str, pa.Table], sessions: pa.Table | None
) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    rows: dict[str, dict[str, int]] = {}
    failed: dict[str, dict[str, int]] = {}
    for kind, log in sorted(fetch_logs.items()):
        _require(kind, FETCH_KINDS, "fetch kind")
        per_rows = rows.setdefault(kind, {})
        for src in log["source_code"].to_pylist():
            _bump(per_rows, _source(src), 1)
        per_failed = failed.setdefault(kind, {})
        for src in unsuperseded_failures(log, sessions=sessions)["source_code"].to_pylist():
            _bump(per_failed, _source(src), 1)
    return rows, failed


def _kept_out_section(kept_out: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for src, kinds in kept_out.items():
        per_src = out.setdefault(_source(src), {})
        for kind, reasons in kinds.items():
            per_kind = per_src.setdefault(_require(kind, FETCH_KINDS, "fetch kind"), {})
            for reason, counts in reasons.items():
                _add(per_kind, _known(reason, _KEPT_OUT), counts["keys"], counts["rows"])
    return out


def summarize(
    run_id: str,
    *,
    started_at: datetime,
    finished_at: datetime | None = None,
    error: BaseException | None = None,
    builds: Mapping[str, BuildResult] | None = None,
    kept_out: Mapping[str, Mapping[str, Mapping[str, Mapping[str, int]]]] | None = None,
    load_report: Any = None,
    mapping: Any = None,
    quality: pa.Table | None = None,
    coverage: CoverageResult | None = None,
    fetch_logs: Mapping[str, pa.Table] | None = None,
    sessions: pa.Table | None = None,
    written: Mapping[str, int] | None = None,
    max_listed_sessions: int = DEFAULT_MAX_LISTED_SESSIONS,
) -> RunSummary:
    """A label-free ``RunSummary`` from one run's parts; each part is optional.

    ``builds``: contract name -> ``BuildResult``; ``kept_out``: source_code ->
    ``kept_out_counts(...)``; ``load_report``: the history-file ``LoadReport``; ``mapping``: a
    ``MappingResult`` (its ``identifiers`` are checked for overlapping windows); ``quality``:
    ``check_prices`` flags (several sources concatenated); ``coverage``: the store's
    ``CoverageResult``; ``fetch_logs``: kind -> that kind's fetch log
    (``build_fetch_log(kinds=(kind,))``, every source concatenated), failures superseded as in
    ``quality.fetch_coverage`` (``sessions``: the same merge rule); ``written``: contract -> rows
    written; ``error``: the exception the run raised (only its type name is kept).

    Contract names and fetch kinds must be known (``ValueError`` otherwise); reasons outside a
    known vocabulary are counted under ``other``; source codes that are not short lowercase
    codes are counted under ``other``; free-text keys (exchange codes, action-type labels,
    column and file names) become counts."""
    if max_listed_sessions < 0:
        raise ValueError(f"max_listed_sessions must be >= 0, got {max_listed_sessions!r}")
    fetches = failed = None
    if fetch_logs is not None:
        fetches, failed = _fetch_sections(fetch_logs, sessions)
    builder_drops = None
    if builds is not None:
        builder_drops = {}
        for name, build in sorted(builds.items()):
            per = builder_drops.setdefault(_require(name, _CONTRACTS, "contract"), {})
            for reason, n in build.counts().items():
                _bump(per, _known(reason, _BUILD_DROPS), n)
    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        error=None if error is None else type(error).__name__,
        builder_drops=builder_drops,
        kept_out=None if kept_out is None else _kept_out_section(kept_out),
        file_load=None if load_report is None else _load_section(load_report),
        mapping=None if mapping is None else _mapping_section(mapping),
        quality=None if quality is None else _quality_section(quality),
        coverage=None if coverage is None else _coverage_section(coverage, max_listed_sessions),
        fetches=fetches,
        failed_fetches=failed,
        written=(
            None
            if written is None
            else {_require(k, _CONTRACTS, "contract"): int(v) for k, v in sorted(written.items())}
        ),
    )


# --- writing ------------------------------------------------------------------------------------


def _reports_dir(directory: Path | str | None) -> Path:
    if directory is None:
        env = os.environ.get(REPORTS_ENV)
        if not env:
            raise ValueError(f"no report directory: pass one or set {REPORTS_ENV}")
        directory = env
    path = Path(directory).absolute()
    resolved = path.resolve()
    for parent in (resolved, *resolved.parents):
        if (parent / ".git").exists():
            raise ValueError(f"report directory {resolved} is inside a git checkout ({parent})")
    return path


def _write_atomic(path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _md_counts(title: str, value: Any) -> list[str]:
    lines = [f"## {title}", ""]
    if value is None:
        return [*lines, "Not part of this run.", ""]
    flat = list(_flatten(value))
    if not flat:
        return [*lines, "Nothing to report.", ""]
    lines += ["| what | value |", "|---|---|"]
    lines += [f"| {key} | {val} |" for key, val in flat]
    return [*lines, ""]


def _flatten(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for k, v in value.items():
            yield from _flatten(v, f"{prefix} / {k}" if prefix else str(k))
    elif isinstance(value, list):
        yield prefix, ", ".join(str(v) for v in value) or "-"
    else:
        yield prefix, value


def _markdown(data: Mapping[str, Any]) -> str:
    lines = [
        f"# Market data run {data['run_id']}",
        "",
        f"- Outcome: **{data['outcome']}**",
        f"- Reasons: {', '.join(data['reasons']) or 'none'}",
        f"- Started: {data['started_at']}",
        f"- Finished: {data['finished_at'] or 'not recorded'}",
        f"- Error: {data['error'] or 'none'}",
        "",
    ]
    for title, key in (
        ("Rows written", "written"),
        ("Coverage", "coverage"),
        ("Fetches recorded", "fetches"),
        ("Failed fetches (unsuperseded)", "failed_fetches"),
        ("Identifier mapping", "mapping"),
        ("Quality flags", "quality"),
        ("Builder drops", "builder_drops"),
        ("Rows kept out by adapters", "kept_out"),
        ("History-file load", "file_load"),
    ):
        lines += _md_counts(title, data[key])
    return "\n".join(lines)


def write_report(run: RunSummary, dir: Path | str | None = None) -> tuple[Path, Path]:
    """Write ``<run_id>-report.json`` and ``<run_id>-report.md`` into ``dir`` (default
    ``MOF_MARKET_REPORTS``, required), atomically (a rewrite of the same run replaces both).
    A directory inside a git checkout raises ``ValueError``. Returns (json path, md path)."""
    directory = _reports_dir(dir)
    directory.mkdir(parents=True, exist_ok=True)
    data = run.to_dict()
    json_path = directory / f"{run.run_id}-report.json"
    md_path = directory / f"{run.run_id}-report.md"
    _write_atomic(json_path, json.dumps(data, indent=2, sort_keys=False) + "\n")
    _write_atomic(md_path, _markdown(data) + "\n")
    return json_path, md_path

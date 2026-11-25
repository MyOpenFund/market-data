"""Run reports (spec §12): what one run built, kept out, could not map, flagged and covered.

Silent-failure doctrine: a run is never ``ok`` by default. ``RunSummary.outcome`` is

- ``error`` when the run raised (``error`` holds the exception's type name only: messages can
  quote keys, i.e. symbols);
- ``ok`` only when there is no error, something was recorded, no unresolved or ambiguous
  identifier in the mapping (when given), no unsuperseded failed fetch (when fetch logs are
  given) and the coverage passed (when given);
- ``degraded`` otherwise; ``reasons`` says why (``REASONS``).

Kept-out rows, builder drops and quality flags are counted but do not degrade a run: they are
flagged, not hidden (§12).

Label-free: a summary holds counts, closed vocabularies (reasons, checks, statuses), source codes,
contract names, session dates and opaque instrument ids only — never a source symbol, a member
id or an export name, even though reports live on a private path (``MOF_MARKET_REPORTS``). Mapping
tables are counted per (source_code, reason); quality flags per check; the history-file
``LoadReport`` is copied except ``ignored``, reduced to counts.

``write_report(run, dir)`` writes ``<run_id>-report.json`` (machine) and ``<run_id>-report.md``
(human), atomically, into ``dir`` or ``MOF_MARKET_REPORTS`` (no default), never inside a git
checkout.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from open_data_store.market.coverage import CoverageResult

from market_data.adapters.base import FetchResult
from market_data.build import ROW_STATUSES, BuildResult
from market_data.quality import CHECKS, unsuperseded_failures

__all__ = [
    "DEFAULT_MAX_LISTED_SESSIONS",
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
    "nothing_recorded",
    "unresolved_identifiers",
    "ambiguous_identifiers",
    "failed_fetches",
    "coverage_failed",
)
# Over-threshold session dates listed in a report (the count is always given).
DEFAULT_MAX_LISTED_SESSIONS = 20
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

Counts = Mapping[str, int]


def _aware(value: datetime | None, name: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True)
class RunSummary:
    """Counts of one run (see the module docstring). Build one with ``summarize``.

    Sections left ``None`` were not part of the run; they neither pass nor fail it.

    - ``builder_drops``: contract -> drop reason -> distinct keys (``BuildResult.counts()``).
    - ``kept_out``: source_code -> kind -> reason -> ``{"keys": n, "rows": n}``
      (``kept_out_counts``: distinct (symbol, date) keys vs payload rows).
    - ``file_load``: the history-file ``LoadReport`` as a dict (``ignored`` as counts).
    - ``mapping``: ``unresolved`` / ``ambiguous`` / ``changed``: source_code -> reason -> series;
      ``labels_left_out``: source_code -> reason -> rows; ``minted``: instruments created.
    - ``quality``: check -> flags (every check of ``CHECKS``).
    - ``coverage``: ``passed``, ``worst_missing_share``, ``sessions_over_threshold``,
      ``over_threshold_sessions`` (ISO dates, the first ``DEFAULT_MAX_LISTED_SESSIONS``).
    - ``failed_fetches``: kind -> source_code -> unsuperseded failed fetches.
    - ``written``: contract -> rows written.
    """

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None
    builder_drops: Mapping[str, Counts] | None = None
    kept_out: Mapping[str, Mapping[str, Mapping[str, Counts]]] | None = None
    file_load: Mapping[str, Any] | None = None
    mapping: Mapping[str, Any] | None = None
    quality: Counts | None = None
    coverage: Mapping[str, Any] | None = None
    failed_fetches: Mapping[str, Counts] | None = None
    written: Counts | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not _RUN_ID.fullmatch(self.run_id):
            raise ValueError(
                "run_id must be 1-128 characters of letters, digits, '.', '_' or '-', "
                "starting with a letter or digit"
            )
        _aware(self.started_at, "started_at")
        _aware(self.finished_at, "finished_at")
        if self.error is not None and not (
            isinstance(self.error, str) and self.error.isidentifier()
        ):
            raise ValueError("error must be an exception type name (an identifier), not a message")

    @property
    def reasons(self) -> tuple[str, ...]:
        """Why the run is not ``ok`` (empty when it is, or when it raised)."""
        if self.error is not None:
            return ()
        out = []
        sections = (
            self.builder_drops,
            self.kept_out,
            self.file_load,
            self.mapping,
            self.quality,
            self.coverage,
            self.failed_fetches,
            self.written,
        )
        if all(s is None for s in sections):
            out.append("nothing_recorded")
        if self.mapping is not None:
            if _total(self.mapping.get("unresolved", {})):
                out.append("unresolved_identifiers")
            if _total(self.mapping.get("ambiguous", {})):
                out.append("ambiguous_identifiers")
        if self.failed_fetches is not None and _total(self.failed_fetches):
            out.append("failed_fetches")
        if self.coverage is not None and not self.coverage.get("passed", False):
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
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


# --- section builders ---------------------------------------------------------------------------


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
    unusable as a whole raises ``ValueError``, as it does in the builder."""
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
                _add(per, reason, keys.get(reason, 0), rows.get(reason, 0))
    return out


def _per_source_reason(table: pa.Table, weight: str | None = None) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(dict)
    if table.num_rows == 0:
        return {}
    aggs = [(weight, "sum")] if weight else [("reason", "count")]
    grouped = table.group_by(["source_code", "reason"]).aggregate(aggs)
    value = f"{weight}_sum" if weight else "reason_count"
    for src, reason, n in zip(
        grouped["source_code"].to_pylist(),
        grouped["reason"].to_pylist(),
        grouped[value].to_pylist(),
        strict=True,
    ):
        out[src][reason] = int(n)
    return {s: dict(sorted(r.items())) for s, r in sorted(out.items())}


def _mapping_section(mapping: Any) -> dict[str, Any]:
    return {
        "unresolved": _per_source_reason(mapping.unresolved),
        "ambiguous": _per_source_reason(mapping.ambiguous),
        "changed": _per_source_reason(mapping.changed),
        "labels_left_out": _per_source_reason(mapping.labels_left_out, "rows"),
        "minted": len(mapping.minted),
    }


def _load_section(report: Any) -> dict[str, Any]:
    ignored = report.ignored
    return {
        "dropped": _plain(report.dropped),
        "unmapped_exchanges": _plain(report.unmapped_exchanges),
        "unknown_action_types": _plain(report.unknown_action_types),
        "ignored": {
            "files": len(ignored.files),
            "columns": {
                fname: (len(cols) if isinstance(cols, list) else 1)
                for fname, cols in sorted(ignored.columns.items())
            },
            "manifest_keys": len(ignored.manifest_keys),
        },
        "fetch_log": _plain(report.fetch_log),
        "unlisted_spinoff_targets": int(report.unlisted_spinoff_targets),
    }


def _quality_section(flags: pa.Table) -> dict[str, int]:
    counts = dict.fromkeys(CHECKS, 0)
    if flags.num_rows:
        values = pc.value_counts(flags["check"]).to_pylist()
        for item in values:
            counts[item["values"]] = int(item["counts"])
    return counts


def _coverage_section(result: CoverageResult, max_listed: int) -> dict[str, Any]:
    per = result.per_session
    shares = per["missing_share"].to_numpy(zero_copy_only=False) if per.num_rows else []
    over = per.filter(per["over_threshold"]) if per.num_rows else per
    return {
        "passed": bool(result.passed),
        "worst_missing_share": float(max(shares)) if len(shares) else 0.0,
        "sessions": per.num_rows,
        "sessions_over_threshold": over.num_rows,
        "over_threshold_sessions": [
            d.isoformat() for d in over["session_date"].to_pylist()[:max_listed]
        ],
    }


def _failed_section(
    fetch_logs: Mapping[str, pa.Table], sessions: pa.Table | None
) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for kind, log in sorted(fetch_logs.items()):
        left = unsuperseded_failures(log, sessions=sessions)
        per: dict[str, int] = {}
        for src in left["source_code"].to_pylist():
            per[src] = per.get(src, 0) + 1
        out[kind] = dict(sorted(per.items()))
    return out


def summarize(
    run_id: str,
    *,
    started_at: datetime,
    finished_at: datetime | None = None,
    error: BaseException | None = None,
    builds: Mapping[str, BuildResult] | None = None,
    kept_out: Mapping[str, Mapping[str, Mapping[str, Counts]]] | None = None,
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
    ``MappingResult``; ``quality``: ``check_prices`` flags (several sources concatenated);
    ``coverage``: the store's ``CoverageResult``; ``fetch_logs``: kind -> that kind's fetch log
    (``build_fetch_log(kinds=(kind,))``, every source concatenated), failures superseded as in
    ``quality.fetch_coverage`` (``sessions``: the same merge rule); ``written``: contract -> rows
    written; ``error``: the exception the run raised (only its type name is kept)."""
    if max_listed_sessions < 0:
        raise ValueError(f"max_listed_sessions must be >= 0, got {max_listed_sessions!r}")
    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        error=None if error is None else type(error).__name__,
        builder_drops=(
            None if builds is None else {k: dict(b.counts()) for k, b in sorted(builds.items())}
        ),
        kept_out=None if kept_out is None else _plain(kept_out),
        file_load=None if load_report is None else _load_section(load_report),
        mapping=None if mapping is None else _mapping_section(mapping),
        quality=None if quality is None else _quality_section(quality),
        coverage=None if coverage is None else _coverage_section(coverage, max_listed_sessions),
        failed_fetches=None if fetch_logs is None else _failed_section(fetch_logs, sessions),
        written=None if written is None else {k: int(v) for k, v in sorted(written.items())},
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

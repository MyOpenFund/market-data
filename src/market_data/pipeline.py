"""Operator pipeline: the plain functions behind the ``market-data`` CLI (one per command).

Steps and what ``--write`` (``write=True``) writes; without it a step computes everything it can
and writes nothing (a dry run):

``load_history``
    The history-file adapter (``FileHistory.build_all``): landing batches for ``prices``,
    ``corporate_actions``, ``vendor_total_return``, ``index_snapshots`` and ``fetch_log``
    (producer ``PRODUCER``, rows of source ``private_history``), and, in the step's private run
    directory, ``observations.parquet`` (the only place the export's labels live) and
    ``spans.parquet`` (per member, from the fetch log and every filed snapshot, a restated
    load included) for ``map``. Those two
    are written first: batches that landed without them could never be mapped.
``plan_fetch`` / ``fetch``
    The requests a fetch would make and the quota they would spend (dry run); with ``write``,
    every FetchResult the adapter yields is put in the raw archive as it arrives (an ``error``
    result included, before the adapter raises).
``map_identifiers``
    ``build_mapping`` over identifier observations (the history load's, the archived ``symbols``
    fetches) and series spans, the published identifiers (``existing``), the manual mapping and,
    when asked, OpenFIGI answers: an ``instrument_identifiers`` batch and the private mapping
    report (unresolved, ambiguous, changed series and labels left out, as Parquet in the run
    directory: they hold source symbols, never published).
``build``
    ``build_prices`` / ``build_actions`` / ``build_fetch_log`` over the archive of one source,
    restricted to the symbols given (the CLI passes the source's mapped universe): landing
    batches for ``prices``, ``corporate_actions`` and ``fetch_log``. The published fetch log
    holds **prices fetches only**: the contract has no kind column, and an actions or symbols
    fetch would read as price coverage; every kind is still counted in the run summary. An
    adapter with a vendor-adjusted series (Tiingo) gets ``check_prices``' vendor-divergence
    check from its archived payloads (``vendor_adjusted``).
``report``
    The newest piece of each step merged into one ``RunSummary`` with the store's coverage of
    one index (``store_coverage``) when the caller computed it, written as
    ``<run_id>-report.json`` / ``.md`` (``reports.write_report``). The report requires coverage:
    without it the reason ``coverage_not_run`` degrades it.

Run directory and pieces: a ``--write`` step keeps ``<reports>/runs/<run_id>/summary.json`` (its
``RunSummary`` as a dict, label-free) plus its private artifacts; ``reports`` is
``MOF_MARKET_REPORTS``, a private path outside any git checkout. ``report`` reads the newest
``summary.json`` per (step, source, kind) (the source of a history load is ``private_history``,
failed or not, so a later success supersedes a failure); a ``fetch`` piece is left out for a
source that has a ``build`` piece (the build re-reads every archived fetch of the source: a
superset).

Step outcome: ``error`` when the step raised; else ``degraded`` when the summary has a reason in
the step's scope (``STEP_SCOPES``: a fetch is judged on failed fetches and on having recorded a
fetch of its own kind, ``FETCHES_NOT_RECORDED``; a mapping not on the fetch logs); else
``ok``. ``report`` is judged on every reason (``reports.RunSummary``).

``summarize_safely`` wraps ``reports.summarize``: an exception (a caller bug, an unknown contract
name) yields an ``error`` summary naming its type only, so a run never ends without a report.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from open_data_store.config import ENV_DATA_ROOT
from open_data_store.contracts.base import Contract
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    FETCH_LOG,
    INDEX_SNAPSHOTS,
    INSTRUMENT_IDENTIFIERS,
    PRICES,
    VENDOR_TOTAL_RETURN,
)
from open_data_store.market.asof import latest, sessions
from open_data_store.market.coverage import (
    COVERAGE_PER_MEMBER_SCHEMA,
    COVERAGE_PER_SESSION_SCHEMA,
    DEFAULT_MAX_MISSING_SHARE,
    DEFAULT_MIN_DELISTING_GAP_SESSIONS,
    CoverageResult,
    coverage,
)
from open_data_store.market.membership import (
    DEFAULT_STALENESS_DAYS,
    membership_intervals,
    unresolved_members,
)
from open_data_store.market.returns import resolve_prices
from open_data_store.market.store import load_market_inputs
from open_data_store.writer.batch import write_batch

from market_data.adapters.base import FETCH_KINDS, WHOLE_MARKET_SYMBOL, Adapter, FetchResult
from market_data.archive import MemoryArchive, RawArchive
from market_data.build import ROW_STATUSES, build_actions, build_fetch_log, build_prices
from market_data.quality import check_prices, fetch_coverage, unusable_cash
from market_data.reports import REASONS, RunSummary, kept_out_counts, summarize, write_report
from market_data.resolve import mapping as _mapping
from market_data.resolve.openfigi import figi_results, us_ticker_job

__all__ = [
    "DEFAULT_MIC",
    "HISTORY_SOURCE",
    "PRODUCER",
    "STEPS",
    "STEP_SCOPES",
    "FetchPlan",
    "PlannedRequest",
    "StepResult",
    "archive_observations",
    "archive_spans",
    "build",
    "fetch",
    "landing_root",
    "load_history",
    "map_identifiers",
    "mapped_symbols",
    "new_run_id",
    "openfigi_series",
    "plan_fetch",
    "report",
    "save_piece",
    "store_coverage",
    "vendor_adjusted",
    "saved_artifacts",
    "summarize_safely",
]

PRODUCER = "market-data"
HISTORY_SOURCE = "private_history"
DEFAULT_MIC = "XNYS"
RUNS_DIR = "runs"
SUMMARY_FILE = "summary.json"
OBSERVATIONS_FILE = "observations.parquet"
SPANS_FILE = "spans.parquet"
STEPS = ("load-history", "fetch", "map", "build", "report")
# A fetch step that archived nothing of its kind (a step-level reason, not a report one).
FETCHES_NOT_RECORDED = "fetches_not_recorded"
# The reasons a step is judged on (``report``: all of them).
STEP_SCOPES: Mapping[str, tuple[str, ...]] = {
    "load-history": ("fetch_logs_not_recorded", "failed_fetches", "unusable_cash_actions"),
    "fetch": ("failed_fetches",),  # plus FETCHES_NOT_RECORDED for its own kind
    "map": ("mapping_not_recorded", "unresolved_identifiers", "ambiguous_identifiers"),
    "build": ("fetch_logs_not_recorded", "failed_fetches", "unusable_cash_actions"),
    "report": REASONS,
}
_RUN_STAMP = "%Y%m%dT%H%M%S%fZ"
_SUMMARY_SECTIONS = (
    "builder_drops",
    "kept_out",
    "file_load",
    "mapping",
    "quality",
    "fetches",
    "failed_fetches",
    "written",
    "unusable_cash",
)


# --- results and run bookkeeping ----------------------------------------------------------------


@dataclass(frozen=True)
class StepResult:
    """One step's summary and the counts its one-line summary shows (never a label)."""

    step: str
    summary: RunSummary
    write: bool
    source: str | None = None
    counts: Mapping[str, int] = field(default_factory=dict)
    run_dir: Path | None = None
    kind: str | None = None  # a fetch's kind (pieces are keyed by step, source and kind)
    # step-level reasons outside the summary's vocabulary (a fetch that recorded nothing)
    extra_reasons: tuple[str, ...] = ()

    @property
    def reasons(self) -> tuple[str, ...]:
        scope = STEP_SCOPES[self.step]
        return (*(r for r in self.summary.reasons if r in scope), *self.extra_reasons)

    @property
    def outcome(self) -> str:
        if self.summary.error is not None:
            return "error"
        return "degraded" if self.reasons else "ok"

    def line(self) -> str:
        """The one-line summary: step, outcome, counts, reasons, run id."""
        parts = [f"{self.step}: {self.outcome}" + ("" if self.write else " (dry run)")]
        if self.source:
            parts.append(f"source={self.source}")
        parts += [f"{k}={v}" for k, v in self.counts.items()]
        if self.summary.error:
            parts.append(f"error={self.summary.error}")
        if self.reasons:
            parts.append("reasons=" + ",".join(self.reasons))
        parts.append(f"run={self.summary.run_id}")
        return " ".join(parts)


def new_run_id(step: str, at: datetime) -> str:
    """``<UTC time>-<step>``: sorts by time, matches the report's run-id rule."""
    return f"{at.astimezone(UTC).strftime(_RUN_STAMP)}-{step}"


def landing_root(flag: Path | str | None = None, env: Mapping[str, str] | None = None) -> Path:
    """``--landing`` when given, else ``$OPEN_DATA_STORE_DATA_ROOT/landing`` (the store's
    ``StoreConfig.landing``); neither: ``ValueError``."""
    if flag:
        return Path(flag)
    root = (os.environ if env is None else env).get(ENV_DATA_ROOT, "").strip()
    if not root:
        raise ValueError(f"no landing root: pass --landing or set {ENV_DATA_ROOT}")
    return Path(root) / "landing"


def _run_dir(reports: Path | str, run_id: str) -> Path:
    path = Path(reports) / RUNS_DIR / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def summarize_safely(run_id: str, *, started_at: datetime, **parts: Any) -> RunSummary:
    """``reports.summarize``; if it raises, an ``error`` summary naming the exception's type."""
    finished = parts.pop("finished_at", None) or datetime.now(UTC)
    try:
        return summarize(run_id, started_at=started_at, finished_at=finished, **parts)
    except Exception as exc:  # noqa: BLE001 - doctrine: a run never ends without a report
        return summarize(run_id, started_at=started_at, finished_at=finished, error=exc)


def save_piece(reports: Path | str, result: StepResult) -> Path:
    """Keep the step's summary in its run directory for ``report`` (label-free JSON)."""
    path = _run_dir(reports, result.summary.run_id) / SUMMARY_FILE
    data = {
        "step": result.step,
        "source": result.source,
        "kind": result.kind,
        "summary": result.summary.to_dict(),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def saved_artifacts(reports: Path | str, step: str, name: str) -> list[Path]:
    """Artifact ``name`` of the newest successful run of ``step`` that kept one (empty when
    none). Successful: its run record (``summary.json``) exists and holds no error, so the
    artifacts of a run whose batches failed to land are never picked."""
    base = Path(reports) / RUNS_DIR
    if not base.is_dir():
        return []
    for run in sorted(base.glob(f"*-{step}"), reverse=True):
        record = run / SUMMARY_FILE
        if not (run / name).is_file() or not record.is_file():
            continue
        if json.loads(record.read_text(encoding="utf-8"))["summary"]["error"] is None:
            return [run / name]
    return []


def _write(
    contracts_tables: Iterable[tuple[Contract, pa.Table]],
    *,
    landing: Path,
    run_id: str,
) -> dict[str, int]:
    """One landing batch per non-empty table (an empty batch is refused by the writer)."""
    written: dict[str, int] = {}
    for contract, table in contracts_tables:
        written[contract.name] = 0
        if table.num_rows:
            ref = write_batch(
                contract, table, landing_root=landing, run_id=run_id, producer=PRODUCER
            )
            written[contract.name] = ref.rows
    return written


# --- load-history -------------------------------------------------------------------------------


def load_history(
    directory: Path | str,
    *,
    run_id: str,
    started_at: datetime,
    write: bool,
    landing: Path | None = None,
    reports: Path | str | None = None,
    previous_snapshots: pa.Table | None = None,
    load_time: datetime | None = None,
    **file_options: Any,
) -> StepResult:
    """Load a history-file export (see the module docstring). ``file_options`` go to
    ``FileHistory`` (``salt``, ``exchange_mics``, ``max_non_session_share``...)."""
    from market_data.adapters.file import FileHistory

    if write and (landing is None or reports is None):
        raise ValueError("load-history --write needs a landing root and a reports directory")
    load = load_time or datetime.now(UTC)
    history = FileHistory(directory, HISTORY_SOURCE, **file_options)
    loaded = history.build_all(load, previous_snapshots=previous_snapshots)
    tables = (
        (PRICES, loaded.prices),
        (CORPORATE_ACTIONS, loaded.corporate_actions),
        (VENDOR_TOTAL_RETURN, loaded.vendor_total_return),
        (INDEX_SNAPSHOTS, loaded.index_snapshots),
        (FETCH_LOG, loaded.fetch_log),
    )
    written = None
    run = None
    if write:
        assert landing is not None and reports is not None
        # the run's private artifacts first: batches without them could never be mapped
        run = _run_dir(reports, run_id)
        pq.write_table(loaded.observations, run / OBSERVATIONS_FILE)
        # spans from every filed snapshot: a restated load's index_snapshots hold only the
        # rows that changed, and would shorten (or drop) the span of every unchanged member
        spans = _mapping.spans_from_fetch_log(loaded.fetch_log, snapshots=history.snapshots())
        pq.write_table(spans, run / SPANS_FILE)
        written = _write(tables, landing=landing, run_id=run_id)
    summary = summarize_safely(
        run_id,
        started_at=started_at,
        load_report=loaded.report,
        written=written,
        unusable_cash={history.source_code: loaded.report.unusable_cash},
    )
    counts = {c.name: t.num_rows for c, t in tables}
    counts["observations"] = loaded.observations.num_rows
    return StepResult("load-history", summary, write, HISTORY_SOURCE, counts, run)


# --- fetch --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class PlannedRequest:
    source_symbol: str
    kind: str
    requested_from: date | None
    requested_to: date | None


@dataclass(frozen=True)
class FetchPlan:
    """The requests a fetch would make (lists counted as one request each: pages come on top)
    and a sentence on the quota they would spend."""

    source: str
    requests: tuple[PlannedRequest, ...]
    quota: str


def _closed_sessions(mic: str, start: date, end: date, now: datetime) -> list[date]:
    cal = sessions(mic, start, end)
    return [
        d
        for d, close in zip(
            cal["session_date"].to_pylist(), cal["close_utc"].to_pylist(), strict=True
        )
        if close <= now
    ]


def plan_fetch(
    source: str,
    kind: str,
    symbols: Sequence[str],
    start: date | None,
    end: date | None,
    *,
    now: datetime | None = None,
    mic: str = DEFAULT_MIC,
) -> FetchPlan:
    """The requests ``fetch`` would make with the shipped adapter of ``source`` (``tiingo`` or
    ``massive``), as that adapter plans them: sessions not closed at ``now`` are never
    requested."""
    from market_data.adapters import massive, tiingo

    now = now or datetime.now(UTC)
    if kind not in FETCH_KINDS:
        raise ValueError(f"fetch kind must be one of {FETCH_KINDS}, got {kind!r}")
    names = list(dict.fromkeys(symbols))
    if kind != "symbols":
        if start is None:
            raise ValueError(f"{source} {kind} need a start date (--date or --from/--to)")
        end = start if end is None else end
        if start > end:
            raise ValueError(f"start {start.isoformat()} is after end {end.isoformat()}")
    requests: list[PlannedRequest] = []
    if source == tiingo.SOURCE_CODE:
        if kind != "prices":
            raise ValueError("Tiingo is fetched as kind 'prices' (actions ride on that payload)")
        if not names or WHOLE_MARKET_SYMBOL in names:
            raise ValueError("Tiingo prices are fetched per symbol: pass --symbols")
        assert start is not None and end is not None
        closed = _closed_sessions(mic, start, end, now)
        if closed:
            requests = [PlannedRequest(n, kind, start, closed[-1]) for n in names]
        quota = (
            f"{len(requests)} request(s) of Tiingo's free plan ({tiingo.FREE_PLAN_PER_HOUR} an "
            f"hour, rolling; {tiingo.FREE_PLAN_PER_DAY} a day; "
            f"{tiingo.FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH} distinct symbols a month), "
            f"{len(requests)} distinct symbol(s)"
        )
        return FetchPlan(source, tuple(requests), quota)
    if source != massive.SOURCE_CODE:
        raise ValueError(f"no fetch plan for source {source!r}: expected 'tiingo' or 'massive'")
    whole = names in ([], [WHOLE_MARKET_SYMBOL])
    if not whole and WHOLE_MARKET_SYMBOL in names:
        raise ValueError(f"{WHOLE_MARKET_SYMBOL!r} cannot be mixed with symbols")
    if kind != "prices" and not whole:
        raise ValueError(f"Massive {kind} are fetched whole-market: pass no symbols")
    if kind == "symbols":
        requests = [PlannedRequest(WHOLE_MARKET_SYMBOL, kind, None, None)] * 2
    else:
        assert start is not None and end is not None
        oldest = now.astimezone(UTC).date() - timedelta(days=massive.DEFAULT_MAX_HISTORY_DAYS)
        if start < oldest:
            raise ValueError(
                f"start {start.isoformat()} is before {oldest.isoformat()}, outside the free "
                f"plan's {massive.DEFAULT_MAX_HISTORY_DAYS} days of history"
            )
        if kind == "actions":
            requests = [PlannedRequest(WHOLE_MARKET_SYMBOL, kind, start, end)] * 2
        elif whole:
            requests = [
                PlannedRequest(WHOLE_MARKET_SYMBOL, kind, d, d)
                for d in _closed_sessions(mic, start, end, now)
            ]
        else:
            closed = _closed_sessions(mic, start, end, now)
            if closed:
                requests = [PlannedRequest(n, kind, start, closed[-1]) for n in names]
    seconds = math.ceil(len(requests) * 60 / massive.FREE_PLAN_RPM)
    quota = (
        f"at least {len(requests)} request(s) at {massive.FREE_PLAN_RPM} a minute (the free "
        f"plan): about {seconds} s, more when a list pages"
    )
    return FetchPlan(source, tuple(requests), quota)


def fetch(
    adapter: Adapter,
    kind: str,
    symbols: Sequence[str],
    start: date | None,
    end: date | None,
    *,
    archive: RawArchive,
    run_id: str,
    started_at: datetime,
    progress: Callable[[int, FetchResult], None] | None = None,
) -> StepResult:
    """Fetch into the archive (always a write: the dry run is ``plan_fetch``). Each result is
    archived as it arrives; when the adapter raises, the results so far (its ``error`` result
    included) are archived and the exception propagates."""
    mine = MemoryArchive()
    for i, result in enumerate(adapter.fetch(kind, symbols, start, end), start=1):
        archive.put(result)
        mine.put(result)
        if progress is not None:
            progress(i, result)
    code = adapter.source_code
    fetched = [r for s in mine.symbols(code, kind) for r in mine.history(code, s, kind)]
    logs = {kind: build_fetch_log(mine, adapter, code, kinds=(kind,))}  # type: ignore[arg-type]
    summary = summarize_safely(
        run_id,
        started_at=started_at,
        fetch_logs=logs,
        kept_out={code: kept_out_counts(adapter, fetched)},
    )
    statuses: dict[str, int] = {}
    for r in fetched:
        statuses[r.status] = statuses.get(r.status, 0) + 1
    counts = {"fetches": len(fetched), **dict(sorted(statuses.items()))}
    recorded = sum(((summary.fetches or {}).get(kind) or {}).values())
    extra = () if recorded else (FETCHES_NOT_RECORDED,)
    return StepResult("fetch", summary, True, code, counts, kind=kind, extra_reasons=extra)


# --- map ----------------------------------------------------------------------------------------


def _with_source(table: pa.Table, code: str) -> pa.Table:
    return table.append_column("source_code", pa.array([code] * table.num_rows, pa.string()))


def archive_observations(archive: RawArchive, adapter: Adapter) -> pa.Table | None:
    """Identifier observations of the source's archived ``symbols`` fetches
    (``mapping.OBSERVATION_INPUT_SCHEMA``); None when it has none. Per (source, symbol, scheme,
    value) only the rows of the latest fetch holding it are kept: an older fetch's sighting
    (an "active" listing archived before the delisting) never stands next to a newer one."""
    code = adapter.source_code
    schema = _mapping.OBSERVATION_INPUT_SCHEMA
    entries = [
        e
        for symbol in archive.symbols(code, "symbols")
        for e in archive.history_with_refs(code, symbol, "symbols")
        if e.result.status in ROW_STATUSES
    ]
    if not entries:
        return None
    entries.sort(key=lambda e: (e.result.fetched_at.astimezone(UTC), e.archive_ref))
    newest: dict[tuple, tuple[int, list[dict[str, Any]]]] = {}
    for i, entry in enumerate(entries):
        rows = _with_source(adapter.parse(entry.result), code)
        for r in rows.select(schema.names).to_pylist():
            key = (r["source_code"], r["source_symbol"], r["id_scheme"], r["id_value"])
            seen = newest.get(key)
            if seen is None or seen[0] < i:
                newest[key] = (i, [r])
            else:
                seen[1].append(r)
    kept = sorted(newest.values(), key=lambda v: v[0])
    return pa.Table.from_pylist([r for _, rows in kept for r in rows], schema=schema)


def archive_spans(archive: RawArchive, adapter: Adapter) -> pa.Table:
    """``mapping.SPAN_SCHEMA`` per series of the source's archived prices: per-symbol fetches
    through their fetch log, whole-market fetches through the dates of their rows."""
    code = adapter.source_code
    log = build_fetch_log(archive, adapter, code, kinds=("prices",))
    own = log.filter(pc.not_equal(log["source_symbol"], WHOLE_MARKET_SYMBOL))
    spans = [_mapping.spans_from_fetch_log(own)]
    for result in archive.history(code, WHOLE_MARKET_SYMBOL, "prices"):
        if result.status in ROW_STATUSES:
            rows = adapter.parse(result).select(["source_symbol", "valid_date"])
            spans.append(_mapping.spans_from_dates(_with_source(rows, code)))
    return _mapping.combine_spans(*spans)


def openfigi_series(
    observations: pa.Table | None,
    spans: pa.Table,
    *,
    export_dated: Iterable[str] = _mapping.EXPORT_DATED_SOURCES,
) -> list[tuple[str, str]]:
    """Series worth an OpenFIGI job: a span, no composite observation, and not export-dated
    (a private export's members map through their own composite FIGIs; their labels never
    leave the host)."""
    skip = set(export_dated)
    composite = set()
    if observations is not None and observations.num_rows:
        rows = observations.filter(pc.equal(observations["id_scheme"], "composite_figi"))
        composite = set(
            zip(*(rows[c].to_pylist() for c in ("source_code", "source_symbol")), strict=True)
        )
    series = zip(*(spans[c].to_pylist() for c in ("source_code", "source_symbol")), strict=True)
    return sorted(s for s in series if s not in composite and s[0] not in skip and s[1] != "*")


def map_identifiers(
    *,
    observations: Sequence[pa.Table],
    spans: Sequence[pa.Table],
    existing: pa.Table | None,
    run_id: str,
    started_at: datetime,
    write: bool,
    manual: pa.Table | None = None,
    figi_client: Any = None,
    landing: Path | None = None,
    reports: Path | str | None = None,
    load_time: datetime | None = None,
) -> StepResult:
    """``build_mapping`` over the inputs (see the module docstring). ``figi_client``: an
    ``OpenFigiClient`` to ask for the series ``openfigi_series`` selects (None: no request)."""
    if write and (landing is None or reports is None):
        raise ValueError("map --write needs a landing root and a reports directory")
    # each input cast on its own: the history load's and the archive's schemas differ in
    # nullability, which concat_tables refuses
    schema = _mapping.OBSERVATION_INPUT_SCHEMA
    obs = (
        pa.concat_tables([t.select(schema.names).cast(schema) for t in observations])
        if observations
        else None
    )
    # always a table (empty without spans): a symbol only listed, never priced, is then no
    # series (``mapping.LISTED_NOT_PRICED``)
    span = _mapping.combine_spans(*spans)
    figi = None
    asked = 0
    if figi_client is not None:
        series = openfigi_series(obs, span)
        asked = len(series)
        if series:
            answers = figi_client.map([us_ticker_job(sym) for _, sym in series])
            figi = figi_results(series, answers)
    result = _mapping.build_mapping(
        obs,
        figi,
        existing=existing,
        load_time=load_time or datetime.now(UTC),
        spans=span,
        manual=manual,
    )
    written = None
    run = None
    if write:
        assert landing is not None and reports is not None
        written = _write(
            [(INSTRUMENT_IDENTIFIERS, result.identifiers)], landing=landing, run_id=run_id
        )
        run = _run_dir(reports, run_id)
        for name in ("unresolved", "ambiguous", "changed", "labels_left_out", "listed_not_priced"):
            pq.write_table(getattr(result, name), run / f"mapping_{name}.parquet")
    summary = summarize_safely(run_id, started_at=started_at, mapping=result, written=written)
    counts = {
        "identifiers": result.identifiers.num_rows,
        "minted": len(result.minted),
        "unresolved": result.unresolved.num_rows,
        "ambiguous": result.ambiguous.num_rows,
        "changed": result.changed.num_rows,
        "listed_not_priced": result.listed_not_priced.num_rows,
        "openfigi_jobs": asked,
    }
    return StepResult("map", summary, write, None, counts, run)


def mapped_symbols(identifiers: pa.Table, source_code: str) -> list[str]:
    """The source's symbols with a ``source_symbol`` mapping row (read latest) whose window is
    not empty: the mapped universe ``build`` is restricted to."""
    if not identifiers.num_rows:
        return []
    rows = latest(identifiers, INSTRUMENT_IDENTIFIERS.natural_key)
    rows = rows.filter(pc.equal(rows["id_scheme"], _mapping.SYMBOL_SCHEME))
    prefix = f"{source_code}:"
    out = set()
    for r in rows.select(["id_value", "valid_date", "valid_to"]).to_pylist():
        if r["id_value"].startswith(prefix) and (
            r["valid_to"] is None or r["valid_to"] > r["valid_date"]
        ):
            out.add(r["id_value"][len(prefix) :])
    out.discard(WHOLE_MARKET_SYMBOL)
    return sorted(out)


# --- build --------------------------------------------------------------------------------------


def _keep_symbols(log: pa.Table, symbols: Sequence[str] | None) -> pa.Table:
    if symbols is None:
        return log
    wanted = pa.array(sorted({*symbols, WHOLE_MARKET_SYMBOL}), pa.string())
    return log.filter(pc.is_in(log["source_symbol"], value_set=wanted))


def build(
    adapter: Adapter,
    *,
    archive: RawArchive,
    symbols: Sequence[str] | None,
    run_id: str,
    started_at: datetime,
    write: bool,
    landing: Path | None = None,
    mic: str = DEFAULT_MIC,
) -> StepResult:
    """Contract tables of one source from the archive (see the module docstring). ``symbols``:
    the mapped universe (None: every archived symbol, a caller's explicit choice)."""
    if write and landing is None:
        raise ValueError("build --write needs a landing root")
    code = adapter.source_code
    prices = build_prices(archive, adapter, symbols, mic=mic)
    actions = build_actions(archive, adapter, symbols, mic=mic)
    kinds = [k for k in FETCH_KINDS if archive.symbols(code, k)]
    logs = {
        k: _keep_symbols(build_fetch_log(archive, adapter, code, kinds=(k,)), symbols)
        for k in kinds
    }
    names = None if symbols is None else sorted({*symbols, WHOLE_MARKET_SYMBOL})
    fetched = (  # a generator: payloads are read one at a time
        r
        for k in kinds
        if k != "symbols"
        for s in (archive.symbols(code, k) if names is None else names)
        for r in archive.history(code, s, k)
    )
    quality = None
    vendor = vendor_adjusted(archive, adapter, symbols)
    if prices.table.num_rows:
        days = pc.min_max(prices.table["valid_date"]).as_py()
        quality = check_prices(
            prices.table,
            actions.table,
            sessions(mic, days["min"], days["max"]),
            vendor_adjusted=vendor,
        )
    prices_log = logs.get("prices", FETCH_LOG.schema.empty_table())
    tables = (
        (PRICES, prices.table),
        (CORPORATE_ACTIONS, actions.table),
        (FETCH_LOG, prices_log),
    )
    written = None
    if write:
        assert landing is not None
        written = _write(tables, landing=landing, run_id=run_id)
    summary = summarize_safely(
        run_id,
        started_at=started_at,
        builds={PRICES.name: prices, CORPORATE_ACTIONS.name: actions},
        kept_out={code: kept_out_counts(adapter, fetched)},
        quality=quality,
        fetch_logs=logs,
        written=written,
        unusable_cash=unusable_cash(actions.table),
    )
    counts = {c.name: t.num_rows for c, t in tables}
    if symbols is not None:
        counts["symbols"] = len(symbols)
    if vendor is not None and quality is not None:
        flagged = pc.equal(quality["check"], "vendor_divergence")
        counts["vendor_divergence"] = int(pc.sum(pc.cast(flagged, pa.int64())).as_py() or 0)
    return StepResult("build", summary, write, code, counts)


def vendor_adjusted(
    archive: RawArchive, adapter: Adapter, symbols: Sequence[str] | None
) -> pa.Table | None:
    """The vendor's adjusted closes of the archived prices fetches (adapters with
    ``parse_vendor_adjusted``, Tiingo), for ``check_prices``' divergence check: per
    (symbol, date) the value of the latest fetch. None when the adapter has no such series."""
    parse = getattr(adapter, "parse_vendor_adjusted", None)
    if parse is None:
        return None
    code = adapter.source_code
    names = archive.symbols(code, "prices") if symbols is None else sorted(set(symbols))
    latest_value: dict[tuple[str, date], float] = {}
    entries = [e for n in names for e in archive.history_with_refs(code, n, "prices")]
    entries.sort(key=lambda e: (e.result.fetched_at.astimezone(UTC), e.archive_ref))
    for entry in entries:
        if entry.result.status not in ROW_STATUSES:
            continue
        for r in parse(entry.result).to_pylist():
            latest_value[(r["source_symbol"], r["valid_date"])] = r["adj_close"]
    keys = sorted(latest_value)
    return pa.table(
        {
            "source_symbol": pa.array([k[0] for k in keys], pa.string()),
            "valid_date": pa.array([k[1] for k in keys], pa.date32()),
            "adj_close": pa.array([latest_value[k] for k in keys], pa.float64()),
        }
    )


# --- report -------------------------------------------------------------------------------------


def _pieces(reports: Path | str) -> list[dict[str, Any]]:
    """Newest piece per (step, source, kind); a fetch piece dropped where a build piece of its
    source exists."""
    base = Path(reports) / RUNS_DIR
    newest: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    if base.is_dir():
        for path in sorted(base.glob(f"*/{SUMMARY_FILE}")):
            data = json.loads(path.read_text(encoding="utf-8"))
            if data["step"] == "report":
                continue
            key = (data["step"], data["source"], data.get("kind"))
            newest[key] = data  # sorted by run id: newest last
    built = {source for step, source, _ in newest if step == "build"}
    return [
        d
        for (step, source, _), d in sorted(
            newest.items(), key=lambda kv: kv[1]["summary"]["run_id"]
        )
        if not (step == "fetch" and source in built)
    ]


def _merge(into: dict[str, Any], value: Mapping[str, Any]) -> None:
    for key, v in value.items():
        if isinstance(v, Mapping):
            _merge(into.setdefault(key, {}), v)
        elif isinstance(v, bool) or v is None or isinstance(v, (str, list)):
            into[key] = v
        else:
            into[key] = into.get(key, 0) + v


def store_coverage(
    store: Any,
    *,
    index_code: str,
    as_of: datetime,
    allowed_restricted: Iterable[str],
    precedence: Sequence[str] = (),
    snapshot_precedence: Sequence[str] = (),
    staleness_days: int = DEFAULT_STALENESS_DAYS,
    max_missing_share: float = DEFAULT_MAX_MISSING_SHARE,
    min_delisting_gap_sessions: int = DEFAULT_MIN_DELISTING_GAP_SESSIONS,
    mic: str = DEFAULT_MIC,
) -> CoverageResult:
    """The store's coverage of ``index_code`` as known at ``as_of``: ``load_market_inputs``
    (strict), ``membership_intervals`` and ``unresolved_members`` (``snapshot_precedence``,
    ``staleness_days``), ``resolve_prices`` (``precedence``), ``quality.fetch_coverage`` of the
    published (prices) fetch log, then ``coverage`` over the sessions from the first membership
    date to the last one closed at ``as_of`` (a session still open is left out). No membership
    (or no closed session) at all: an empty result that does not pass (the report's
    ``coverage_empty``)."""
    inputs = load_market_inputs(
        store, as_of=as_of, allowed_restricted=frozenset(allowed_restricted)
    )
    kw = dict(
        index_code=index_code,
        as_of=as_of,
        staleness_days=staleness_days,
        precedence=tuple(snapshot_precedence),
    )
    intervals = membership_intervals(inputs.snapshots, inputs.identifiers, **kw)
    first = pc.min(intervals["valid_from"]).as_py() if intervals.num_rows else None
    last = as_of.astimezone(UTC).date()
    cal = None
    if first is not None and first <= last:
        # only sessions closed at as_of: today's session is not over (nor published) yet
        cal = sessions(mic, first, last)
        closed = pc.less_equal(cal["close_utc"], pa.scalar(as_of, cal["close_utc"].type))
        cal = cal.filter(closed)
    if cal is None or not cal.num_rows:
        return CoverageResult(
            COVERAGE_PER_MEMBER_SCHEMA.empty_table(),
            COVERAGE_PER_SESSION_SCHEMA.empty_table(),
            passed=False,
        )
    unresolved = unresolved_members(inputs.snapshots, inputs.identifiers, **kw)
    try:
        resolved = resolve_prices(
            inputs.prices, inputs.identifiers, as_of=as_of, precedence=tuple(precedence)
        )
    except ValueError:
        # the recipe's message quotes series (tokens, symbols): never shown on stderr
        raise ValueError(
            "several price sources (or conflicting series of one source) for one instrument "
            "and session: pass --precedence (or make that source's mapping windows disjoint)"
        ) from None
    covered = fetch_coverage(inputs.fetch_log, inputs.identifiers, as_of=as_of, sessions=cal)
    return coverage(
        intervals,
        resolved,
        cal,
        covered,
        unresolved=unresolved,
        max_missing_share=max_missing_share,
        min_delisting_gap_sessions=min_delisting_gap_sessions,
    )


def report(
    reports: Path | str,
    *,
    run_id: str,
    started_at: datetime,
    write: bool,
    coverage_result: CoverageResult | None = None,
) -> StepResult:
    """Merge the newest pieces into one report (sections summed; an errored piece makes the
    report an error) with the store coverage when given (none: ``coverage_not_run``); with
    ``write``, ``write_report`` into ``reports``."""
    pieces = _pieces(reports)
    sections: dict[str, Any] = {}
    error = None
    for piece in pieces:
        data = piece["summary"]
        if data["error"]:
            error = data["error"]
        for name in _SUMMARY_SECTIONS:
            if data.get(name) is not None:
                _merge(sections.setdefault(name, {}), data[name])
    if "mapping" in sections:  # a map piece saved before this count existed
        sections["mapping"].setdefault("listed_not_priced", {})
    starts = [datetime.fromisoformat(p["summary"]["started_at"]) for p in pieces]
    if coverage_result is not None:
        sections["coverage"] = summarize(
            run_id, started_at=started_at, coverage=coverage_result
        ).coverage
    summary = RunSummary(
        run_id=run_id,
        started_at=min(starts) if starts else started_at,
        finished_at=datetime.now(UTC),
        error=error,
        coverage_required=True,
        **sections,
    )
    if write:
        write_report(summary, reports)
    counts = {"pieces": len(pieces)}
    return StepResult("report", summary, write, None, counts)

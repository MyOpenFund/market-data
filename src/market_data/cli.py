"""``market-data``: the operator's commands on the store host (a thin layer over ``pipeline``).

Every command is a dry run unless ``--write``: it computes what it can and writes nothing
(``fetch`` lists the requests it would make and the quota they would spend). Paths come from
flags, else the environment: ``--archive`` / ``MOF_MARKET_ARCHIVE`` (raw archive),
``--reports`` / ``MOF_MARKET_REPORTS`` (private run directories and reports, needed by every
``--write``), ``--landing`` / ``$OPEN_DATA_STORE_DATA_ROOT/landing`` (where batches are
dropped for the publisher), ``MOF_MARKET_TOKEN_SALT`` (history-file tokens),
``MOF_MARKET_MANUAL_MAPPING`` (reviewed manual mapping), ``MOF_MARKET_ADAPTERS`` (the provider
adapters allowed). Published identifiers are read from the store (``OPEN_DATA_STORE_DATA_ROOT`` and
``OPEN_DATA_STORE_CATALOG``) unless a Parquet file is given.

Commands:

- ``load-history --dir PATH [--previous-snapshots PARQUET]``: a history-file export.
- ``fetch --source massive|tiingo [--kind K] [--date D | --from D --to D] [--symbols ...]``.
- ``map [--observations PARQUET ...] [--spans PARQUET ...] [--existing PARQUET | --no-existing]
  [--openfigi]``: defaults to the newest history load's observations and spans, plus the
  archived symbols and prices of every allowed adapter; ``--no-existing`` is required for a
  first mapping (no published identifiers), so ids are never re-minted by accident.
- ``build --source massive|tiingo [--identifiers PARQUET] [--all-symbols]``: restricted to the
  source's mapped universe (``--all-symbols``: every archived symbol, an explicit choice).
- ``report [--index CODE [--as-of T] [--precedence ...] [--snapshot-precedence ...]
  [--allowed-restricted ...] [--staleness-days N] [--max-missing-share X]
  [--min-delisting-gap-sessions N]]``: the newest pieces merged into ``<run_id>-report.json`` /
  ``.md``, with the store's coverage of the index (defaults: the store recipes'); without
  ``--index`` the report is degraded (``coverage_not_run``).

A usage error (arguments, missing paths, an adapter not allowed, an unregistered source code)
is refused before any work: exit 2, a message on stderr, no run record and no report.
Otherwise each command prints one summary line (counts, never a label) and exits 0 (``ok``), 1
(``degraded``; 0 with ``--allow-degraded``) or 2 (``error``). An exception is caught: its type and
message go to stderr, and an ``error`` report naming its type only is written to the reports
directory (dry run included) before the exit. A successful ``--write`` whose run record cannot
be saved exits 2 ("batches landed but run record not saved", "archive written ..." for a
fetch).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from open_data_store.contracts.standard.market import INSTRUMENT_IDENTIFIERS
from open_data_store.market.coverage import (
    DEFAULT_MAX_MISSING_SHARE,
    DEFAULT_MIN_DELISTING_GAP_SESSIONS,
)
from open_data_store.market.membership import DEFAULT_STALENESS_DAYS

from market_data import pipeline
from market_data.adapters.base import Adapter, allowed_adapters, load_adapters
from market_data.archive import ARCHIVE_ENV, RawArchive
from market_data.reports import REPORTS_ENV, write_report
from market_data.resolve.mapping import load_manual_mapping

__all__ = ["EXIT_DEGRADED", "EXIT_ERROR", "EXIT_OK", "main", "make_adapter"]

EXIT_OK, EXIT_DEGRADED, EXIT_ERROR = 0, 1, 2
_LISTED = 20  # planned requests printed by a fetch dry run
PROVIDER_SOURCES = ("massive", "tiingo")


class UsageError(ValueError):
    """A command refused before any work (arguments, paths, allowed adapters): exit 2, no run
    record and no report."""


def make_adapter(source: str) -> Adapter:
    """The installed adapter of ``source``, which ``MOF_MARKET_ADAPTERS`` must allow."""
    if source not in allowed_adapters():
        raise UsageError(f"adapter {source!r} is not allowed: add it to MOF_MARKET_ADAPTERS")
    return load_adapters([source])[source]


def _close(adapter: Adapter) -> None:
    close = getattr(adapter, "close", None)
    if callable(close):
        close()


def _path(flag: Path | None, env: str) -> Path | None:
    if flag is not None:
        return flag
    raw = os.environ.get(env, "").strip()
    return Path(raw) if raw else None


def _require(path: Path | None, what: str, env: str) -> Path:
    if path is None:
        raise UsageError(f"no {what}: pass --{what.split()[0]} or set {env}")
    return path


def _reports(args: argparse.Namespace) -> Path | None:
    return _path(args.reports, REPORTS_ENV)


def _archive(args: argparse.Namespace) -> RawArchive:
    return RawArchive(_require(_path(args.archive, ARCHIVE_ENV), "archive", ARCHIVE_ENV))


def _write_needs(args: argparse.Namespace) -> tuple[Path, Path]:
    """(landing, reports) for a ``--write``."""
    reports = _require(_reports(args), "reports directory", REPORTS_ENV)
    try:
        return pipeline.landing_root(getattr(args, "landing", None)), reports
    except ValueError as exc:
        raise UsageError(str(exc)) from None


def _store_identifiers() -> pa.Table:
    """The published identifiers (every source: the mapper runs on the store host)."""
    from open_data_store.config import StoreConfig
    from open_data_store.reader import Store
    from open_data_store.reader.api import UnknownDataset

    name = INSTRUMENT_IDENTIFIERS.table_name
    with Store.open(StoreConfig.from_env()) as store:
        try:
            published = store.is_published(name)
        except UnknownDataset:
            published = False
        if not published:
            return INSTRUMENT_IDENTIFIERS.schema.empty_table()
        table = store.read_asof(name, datetime.now(UTC))
    schema = INSTRUMENT_IDENTIFIERS.schema
    return table.select(schema.names).cast(schema)


def _identifiers(path: Path | None) -> pa.Table:
    if path is not None:
        schema = INSTRUMENT_IDENTIFIERS.schema
        return pq.read_table(path).select(schema.names).cast(schema)
    return _store_identifiers()


# --- commands -----------------------------------------------------------------------------------

Body = Callable[[argparse.Namespace, str, datetime], pipeline.StepResult]


def _load_history(args: argparse.Namespace, run_id: str, started: datetime) -> pipeline.StepResult:
    landing = reports = None
    if args.write:
        landing, reports = _write_needs(args)
    previous = pq.read_table(args.previous_snapshots) if args.previous_snapshots else None
    options = {}
    if args.max_non_session_share is not None:
        options["max_non_session_share"] = args.max_non_session_share
    return pipeline.load_history(
        args.dir,
        run_id=run_id,
        started_at=started,
        write=args.write,
        landing=landing,
        reports=reports,
        previous_snapshots=previous,
        **options,
    )


def _range(args: argparse.Namespace) -> tuple[date | None, date | None]:
    if args.date is not None:
        if args.date_from is not None or args.date_to is not None:
            raise UsageError("pass --date or --from/--to, not both")
        return args.date, args.date
    if (args.date_from is None) != (args.date_to is None):
        raise UsageError("pass both --from and --to (or --date)")
    return args.date_from, args.date_to


def _plan(args: argparse.Namespace) -> pipeline.FetchPlan:
    """The fetch plan; an argument the adapter would refuse is a usage error."""
    start, end = _range(args)
    try:
        return pipeline.plan_fetch(args.source, args.kind, args.symbols, start, end)
    except ValueError as exc:
        raise UsageError(str(exc)) from None


def _fetch_plan(args: argparse.Namespace) -> int:
    plan = _plan(args)
    for r in plan.requests[:_LISTED]:
        span = "" if r.requested_from is None else f" {r.requested_from}..{r.requested_to}"
        print(f"  would request {r.kind} {r.source_symbol}{span}")
    if len(plan.requests) > _LISTED:
        print(f"  ... and {len(plan.requests) - _LISTED} more")
    print(f"fetch: plan (dry run) source={plan.source} requests={len(plan.requests)}; {plan.quota}")
    return EXIT_OK


def _fetch(args: argparse.Namespace, run_id: str, started: datetime) -> pipeline.StepResult:
    _require(_reports(args), "reports directory", REPORTS_ENV)
    _plan(args)  # refuse bad arguments before any request or record
    start, end = _range(args)
    archive = _archive(args)
    adapter = make_adapter(args.source)

    def progress(i: int, result) -> None:
        print(f"  [{i}] {result.kind} {result.status}", file=sys.stderr, flush=True)

    try:
        return pipeline.fetch(
            adapter,
            args.kind,
            args.symbols,
            start,
            end,
            archive=archive,
            run_id=run_id,
            started_at=started,
            progress=progress,
        )
    finally:
        _close(adapter)


def _map(args: argparse.Namespace, run_id: str, started: datetime) -> pipeline.StepResult:
    landing = None
    reports = _reports(args)
    if args.write:
        landing, reports = _write_needs(args)
    if args.existing is not None:
        existing = _identifiers(args.existing)
    elif args.no_existing:
        existing = None
    else:
        existing = _store_identifiers()
    observations = [pq.read_table(p) for p in args.observations or []]
    spans = [pq.read_table(p) for p in args.spans or []]
    if not args.observations and reports is not None:
        found = pipeline.saved_artifacts(reports, "load-history", pipeline.OBSERVATIONS_FILE)
        observations += [pq.read_table(p) for p in found]
    if not args.spans and reports is not None:
        found = pipeline.saved_artifacts(reports, "load-history", pipeline.SPANS_FILE)
        spans += [pq.read_table(p) for p in found]
    archive_path = _path(args.archive, ARCHIVE_ENV)
    if archive_path is not None:
        archive = RawArchive(archive_path)
        sources = [s for s in allowed_adapters() if s in PROVIDER_SOURCES]
        for code, adapter in load_adapters(sources).items():
            try:
                found_obs = pipeline.archive_observations(archive, adapter)
                if found_obs is not None:
                    observations.append(found_obs)
                if archive.symbols(code, "prices"):
                    spans.append(pipeline.archive_spans(archive, adapter))
            finally:
                _close(adapter)
    client = None
    if args.openfigi:
        from market_data.resolve.openfigi import OpenFigiClient

        client = OpenFigiClient.from_env()
        if not client.keyed:
            from market_data.resolve.openfigi import KEY_HELP

            print(f"map: {KEY_HELP}", file=sys.stderr)
    try:
        return pipeline.map_identifiers(
            observations=observations,
            spans=spans,
            existing=existing,
            manual=load_manual_mapping(),
            figi_client=client,
            run_id=run_id,
            started_at=started,
            write=args.write,
            landing=landing,
            reports=reports,
        )
    finally:
        if client is not None:
            client.close()


def _build(args: argparse.Namespace, run_id: str, started: datetime) -> pipeline.StepResult:
    landing = None
    if args.write:
        landing, _ = _write_needs(args)
    archive = _archive(args)
    symbols: list[str] | None = None
    if not args.all_symbols:
        symbols = pipeline.mapped_symbols(_identifiers(args.identifiers), args.source)
        if not symbols:
            raise ValueError(
                f"no mapped symbol of {args.source!r} in the identifiers: run `map` first, or "
                "pass --all-symbols to build every archived symbol"
            )
    adapter = make_adapter(args.source)
    try:
        return pipeline.build(
            adapter,
            archive=archive,
            symbols=symbols,
            run_id=run_id,
            started_at=started,
            write=args.write,
            landing=landing,
        )
    finally:
        _close(adapter)


def _coverage(args: argparse.Namespace):
    """The store's coverage of ``--index`` (None without ``--index``: ``coverage_not_run``)."""
    if args.index is None:
        return None
    from open_data_store import sources
    from open_data_store.config import StoreConfig
    from open_data_store.reader import Store

    as_of = args.as_of or datetime.now(UTC)
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise UsageError("--as-of must carry a UTC offset, e.g. 2026-11-20T21:00:00+00:00")
    registry = sources.discover_sources()
    allowed = args.allowed_restricted
    if allowed is None:  # the operator's own store: every registered restricted source
        allowed = sorted(sources.restricted_codes(registry))
    unknown = sorted(set(allowed) - set(registry))
    if unknown:  # source codes, not labels: safe to name
        raise UsageError(f"--allowed-restricted names unregistered source code(s) {unknown}")
    with Store.open(StoreConfig.from_env()) as store:
        return pipeline.store_coverage(
            store,
            index_code=args.index,
            as_of=as_of,
            allowed_restricted=allowed,
            precedence=args.precedence,
            snapshot_precedence=args.snapshot_precedence,
            staleness_days=args.staleness_days,
            max_missing_share=args.max_missing_share,
            min_delisting_gap_sessions=args.min_delisting_gap_sessions,
        )


def _report(args: argparse.Namespace, run_id: str, started: datetime) -> pipeline.StepResult:
    reports = _require(_reports(args), "reports directory", REPORTS_ENV)
    return pipeline.report(
        reports,
        run_id=run_id,
        started_at=started,
        write=args.write,
        coverage_result=_coverage(args),
    )


_BODIES: dict[str, Body] = {
    "load-history": _load_history,
    "fetch": _fetch,
    "map": _map,
    "build": _build,
    "report": _report,
}


# --- running ------------------------------------------------------------------------------------


def _exit_code(result: pipeline.StepResult, allow_degraded: bool) -> int:
    if result.outcome == "error":
        return EXIT_ERROR
    if result.outcome == "degraded" and not allow_degraded:
        return EXIT_DEGRADED
    return EXIT_OK


def _record(step: str, args: argparse.Namespace, result: pipeline.StepResult) -> bool:
    """Keep the step's piece (``--write``) and, for an ``error`` outcome, write its report
    (type name only) to the reports directory, dry run included. Returns False when the run
    record of a successful ``--write`` could not be kept (its batches did land): the caller
    exits with the error code."""
    reports = _reports(args)
    error = result.outcome == "error"
    if reports is None:
        if error:
            print(f"{step}: no error report written ({REPORTS_ENV} not set)", file=sys.stderr)
        return True
    report_written = False
    if error:
        try:
            write_report(result.summary, reports)
            report_written = True
        except Exception as exc:  # noqa: BLE001 - an error run exits 2 whatever happens here
            print(f"{step}: error report not written ({type(exc).__name__})", file=sys.stderr)
    if not (args.write and step != "report"):
        return True
    try:
        pipeline.save_piece(reports, result)
    except Exception as exc:  # noqa: BLE001 - said on stderr, and the exit code says it too
        name = type(exc).__name__
        if error:
            said = "error report written" if report_written else "no error report"
            print(f"{step}: {said}, but run record not saved: {name}", file=sys.stderr)
            return True
        done = "archive written" if step == "fetch" else "batches landed"
        print(f"{step}: {done} but run record not saved: {name}", file=sys.stderr)
        return False
    return True


def _step_source(step: str, args: argparse.Namespace) -> str | None:
    """The source a step's pieces are keyed by, the same whether it fails or succeeds."""
    if step == "load-history":
        return pipeline.HISTORY_SOURCE
    return getattr(args, "source", None)


def _run(step: str, args: argparse.Namespace) -> int:
    started = datetime.now(UTC)
    run_id = pipeline.new_run_id(step, started)
    try:
        if step == "fetch" and not args.write:
            return _fetch_plan(args)
        result = _BODIES[step](args, run_id, started)
    except UsageError as exc:
        print(f"{step}: usage: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 - doctrine: every failure ends in an error report
        print(f"{step}: {type(exc).__name__}: {exc}", file=sys.stderr)
        summary = pipeline.summarize_safely(run_id, started_at=started, error=exc)
        kind = args.kind if step == "fetch" else None
        result = pipeline.StepResult(step, summary, args.write, _step_source(step, args), kind=kind)
    kept = _record(step, args, result)
    print(result.line())
    if not kept:
        return EXIT_ERROR
    return _exit_code(result, args.allow_degraded)


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--write", action="store_true", help="write (default: a dry run)")
    common.add_argument(
        "--allow-degraded", action="store_true", help="exit 0 on a degraded outcome"
    )
    common.add_argument("--archive", type=Path, help=f"raw archive root (default ${ARCHIVE_ENV})")
    common.add_argument(
        "--reports", type=Path, help=f"private reports dir (default ${REPORTS_ENV})"
    )
    landing = argparse.ArgumentParser(add_help=False)
    landing.add_argument(
        "--landing", type=Path, help="landing root (default $OPEN_DATA_STORE_DATA_ROOT/landing)"
    )

    parser = argparse.ArgumentParser(prog="market-data", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("load-history", parents=[common, landing], help="load a history file")
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--previous-snapshots", type=Path)
    p.add_argument("--max-non-session-share", type=float)

    p = sub.add_parser("fetch", parents=[common], help="fetch into the raw archive")
    p.add_argument("--source", required=True, choices=PROVIDER_SOURCES)
    p.add_argument("--kind", default="prices", choices=("prices", "actions", "symbols"))
    p.add_argument("--date", type=date.fromisoformat)
    p.add_argument("--from", dest="date_from", type=date.fromisoformat)
    p.add_argument("--to", dest="date_to", type=date.fromisoformat)
    p.add_argument("--symbols", nargs="*", default=[])

    p = sub.add_parser("map", parents=[common, landing], help="map symbols to instruments")
    p.add_argument("--observations", type=Path, nargs="*")
    p.add_argument("--spans", type=Path, nargs="*")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--existing", type=Path)
    group.add_argument("--no-existing", action="store_true")
    p.add_argument("--openfigi", action="store_true")

    p = sub.add_parser("build", parents=[common, landing], help="build batches from the archive")
    p.add_argument("--source", required=True, choices=PROVIDER_SOURCES)
    p.add_argument("--identifiers", type=Path)
    p.add_argument("--all-symbols", action="store_true")

    p = sub.add_parser("report", parents=[common], help="merge the newest pieces into a report")
    p.add_argument("--index", help="index code whose store coverage the report runs")
    p.add_argument("--as-of", type=datetime.fromisoformat, help="coverage as known then")
    p.add_argument("--precedence", nargs="*", default=[], help="price source precedence")
    p.add_argument(
        "--snapshot-precedence",
        nargs="*",
        default=[],
        help="index-snapshot source precedence for membership (separate from prices')",
    )
    p.add_argument(
        "--allowed-restricted",
        nargs="*",
        help="restricted sources read (default: every registered one: your own store)",
    )
    p.add_argument("--staleness-days", type=int, default=DEFAULT_STALENESS_DAYS)
    p.add_argument("--max-missing-share", type=float, default=DEFAULT_MAX_MISSING_SHARE)
    p.add_argument(
        "--min-delisting-gap-sessions", type=int, default=DEFAULT_MIN_DELISTING_GAP_SESSIONS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return _run(args.command, args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from open_data_store.config import StoreConfig
from open_data_store.contracts.standard.market import INSTRUMENT_IDENTIFIERS, MARKET_CONTRACTS
from open_data_store.market.asof import sessions
from open_data_store.market.returns import resolve_actions, resolve_prices, total_return
from open_data_store.market.store import load_market_inputs
from open_data_store.publisher import approve_contract, publish_pending
from open_data_store.reader import Store
from open_data_store.writer.batch import write_batch

from market_data import api, cli, pipeline
from market_data.adapters._quota import STATE_DIR_ENV
from market_data.archive import RawArchive
from market_data.cli import EXIT_DEGRADED, EXIT_ERROR, EXIT_OK, main
from market_data.reports import RunSummary

HISTORY = Path(__file__).parent / "fixtures" / "history_file"
SALT = "synthetic-test-salt-0123456789"
HISTORY_AS_OF = datetime(2020, 4, 10, tzinfo=UTC)
NOW = datetime(2025, 6, 10, 12, 0, tzinfo=UTC)
I1 = "mof:" + "1" * 32


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Every path in tmp_path; a store with the six market contracts approved."""
    paths = {
        "archive": tmp_path / "archive",
        "reports": tmp_path / "reports",
        "store": tmp_path / "store",
        "export": tmp_path / "export",
    }
    shutil.copytree(HISTORY, paths["export"])
    cfg = StoreConfig(data_root=paths["store"], catalog=str(tmp_path / "meta.ducklake"))
    for c in MARKET_CONTRACTS:
        approve_contract(cfg, c, "test")
    monkeypatch.setenv("MOF_MARKET_ARCHIVE", str(paths["archive"]))
    monkeypatch.setenv("MOF_MARKET_REPORTS", str(paths["reports"]))
    monkeypatch.setenv("OPEN_DATA_STORE_DATA_ROOT", str(paths["store"]))
    monkeypatch.setenv("OPEN_DATA_STORE_CATALOG", cfg.catalog)
    monkeypatch.setenv("MOF_MARKET_TOKEN_SALT", SALT)
    monkeypatch.setenv("MOF_MARKET_ADAPTERS", "tiingo")
    monkeypatch.setenv(STATE_DIR_ENV, "off")
    for var in ("MOF_MARKET_MANUAL_MAPPING", "TIINGO_API_KEY", "MASSIVE_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    paths["cfg"] = cfg
    paths["root"] = tmp_path
    return paths


def tree(root: Path) -> list[tuple[str, int]]:
    return sorted(
        (p.relative_to(root).as_posix(), p.stat().st_size) for p in root.rglob("*") if p.is_file()
    )


@pytest.fixture
def run(env):
    """main() with "{name}" placeholders filled from env."""

    def call(*argv: str) -> int:
        return main([a.format(**{k: v for k, v in env.items() if k != "cfg"}) for a in argv])

    return call


HISTORY_ARGS = ("load-history", "--dir", "{export}", "--max-non-session-share", "0.5")


# --- dry runs ----------------------------------------------------------------------------------


def test_every_dry_run_writes_nothing(env, run, capsys):
    before = tree(env["root"])
    assert run(*HISTORY_ARGS) == EXIT_OK
    assert (
        run(
            "fetch",
            "--source",
            "tiingo",
            "--from",
            "2025-06-02",
            "--to",
            "2025-06-06",
            "--symbols",
            "AAA",
            "BBB",
        )
        == EXIT_OK
    )
    assert run("map", "--no-existing") == EXIT_OK
    assert run("build", "--source", "tiingo", "--all-symbols", "--allow-degraded") == EXIT_OK
    assert run("report", "--allow-degraded") == EXIT_OK
    assert tree(env["root"]) == before
    out = capsys.readouterr().out
    assert "load-history: ok (dry run)" in out
    assert "would request prices AAA 2025-06-02..2025-06-06" in out
    assert "requests=2" in out and "50 an hour" in out
    assert "map: ok (dry run)" in out
    assert "build: degraded (dry run)" in out
    assert "report: degraded (dry run)" in out


def test_build_dry_run_writes_nothing(env, run, monkeypatch):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    assert (
        run(
            "fetch",
            "--write",
            "--source",
            "tiingo",
            "--from",
            "2025-06-02",
            "--to",
            "2025-06-06",
            "--symbols",
            "AAA",
        )
        == EXIT_OK
    )
    ids = env["root"] / "ids.parquet"
    pq.write_table(identifiers({"tiingo:AAA": I1}), ids)
    before = tree(env["root"])
    assert run("build", "--source", "tiingo", "--identifiers", str(ids)) == EXIT_OK
    assert tree(env["root"]) == before


def test_load_history_dry_run_needs_no_landing(env, run, monkeypatch):
    monkeypatch.delenv("OPEN_DATA_STORE_DATA_ROOT")
    assert run(*HISTORY_ARGS) == EXIT_OK


# --- write round trips -------------------------------------------------------------------------


def test_history_round_trip_into_the_store(env, run, capsys):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    landing = env["store"] / "landing"
    written = {p.parent.parent.name for p in landing.rglob("batch.json")}
    assert written == {
        "prices_v1",
        "corporate_actions_v1",
        "vendor_total_return_v1",
        "index_snapshots_v1",
        "fetch_log_v1",
    }
    manifests = [json.loads(p.read_text()) for p in landing.rglob("batch.json")]
    assert {m["producer"] for m in manifests} == {"market-data"}
    runs = env["reports"] / "runs"
    (load_run,) = runs.iterdir()
    assert (load_run / "observations.parquet").is_file()
    assert (load_run / "spans.parquet").is_file()

    code = run("map", "--write", "--no-existing")
    assert code in (EXIT_OK, EXIT_DEGRADED)
    assert (landing / "instrument_identifiers_v1").is_dir()
    map_run = next(p for p in runs.iterdir() if p.name.endswith("-map"))
    assert (map_run / "mapping_unresolved.parquet").is_file()

    assert publish_pending(env["cfg"]).exit_code == 0
    with Store.open(env["cfg"]) as store:
        m = load_market_inputs(
            store, as_of=HISTORY_AS_OF, allowed_restricted=frozenset({"private_history"})
        )
    assert m.prices.num_rows and m.identifiers.num_rows
    rp = resolve_prices(m.prices, m.identifiers, as_of=HISTORY_AS_OF, precedence=())
    ra = resolve_actions(m.actions, m.identifiers, as_of=HISTORY_AS_OF, precedence=())
    assert rp.num_rows
    assert all(i.startswith("mof:") for i in rp["instrument_id"].to_pylist())
    days = rp["session_date"].to_pylist()
    tr = total_return(rp, ra, sessions("XNYS", min(days), max(days)))
    assert tr.num_rows == rp.num_rows

    # the default gate denies the restricted history source
    with Store.open(env["cfg"]) as store:
        assert load_market_inputs(store, as_of=HISTORY_AS_OF).prices.num_rows == 0

    capsys.readouterr()
    code = run("report", "--write", "--allow-degraded")
    assert code == EXIT_OK
    reports = sorted(env["reports"].glob("*-report.json"))
    assert len(reports) == 1
    data = json.loads(reports[0].read_text())
    assert data["file_load"] is not None and data["mapping"] is not None
    assert data["written"]["prices"] > 0 and data["written"]["instrument_identifiers"] > 0
    text = reports[0].read_text() + reports[0].with_suffix(".md").read_text()
    for label in ("AAA UN Equity", "Alpha Fake Corp", "BBG000FAKC01"):
        assert label not in text


def test_second_map_reads_the_published_identifiers(env, run):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    run("map", "--write", "--no-existing")
    assert publish_pending(env["cfg"]).exit_code == 0
    with Store.open(env["cfg"]) as store:
        first = set(store.read_asof("instrument_identifiers")["instrument_id"].to_pylist())
    # default existing = the store: no id is minted again
    run("map", "--write")
    assert publish_pending(env["cfg"]).exit_code == 0
    with Store.open(env["cfg"]) as store:
        second = set(store.read_asof("instrument_identifiers")["instrument_id"].to_pylist())
    assert second == first


def test_map_without_existing_evidence_is_refused(env, run, monkeypatch, capsys):
    monkeypatch.delenv("OPEN_DATA_STORE_CATALOG")
    assert run("map") == EXIT_ERROR
    assert "OPEN_DATA_STORE_CATALOG" in capsys.readouterr().err


def test_tiingo_fetch_build_round_trip(env, run, monkeypatch, capsys):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    assert (
        run(
            "fetch",
            "--write",
            "--source",
            "tiingo",
            "--from",
            "2025-06-02",
            "--to",
            "2025-06-06",
            "--symbols",
            "AAA",
            "BBB",
        )
        == EXIT_OK
    )
    archive = RawArchive(env["archive"])
    assert archive.symbols("tiingo", "prices") == ["AAA", "BBB"]
    assert "fetch: ok source=tiingo fetches=2 ok=2" in capsys.readouterr().out

    ids = env["root"] / "ids.parquet"
    pq.write_table(identifiers({"tiingo:AAA": I1}), ids)
    assert run("build", "--write", "--source", "tiingo", "--identifiers", str(ids)) == EXIT_OK
    assert "symbols=1" in capsys.readouterr().out
    # the mapping is map's job: publish this one by hand
    write_batch(
        INSTRUMENT_IDENTIFIERS,
        pq.read_table(ids),
        landing_root=env["store"] / "landing",
        run_id="ids",
        producer="test",
    )
    assert publish_pending(env["cfg"]).exit_code == 0

    with Store.open(env["cfg"]) as store:
        out = api.prices(
            [I1],
            date(2025, 6, 2),
            date(2025, 6, 6),
            profile="server",
            store=store,
            as_of=NOW,
            allowed_restricted={"tiingo"},
        )
        log = store.read_asof("fetch_log", NOW)
    assert out["close"].to_pylist() == [100.0, 101.0, 100.0, 51.0, 52.0]
    assert log["source_symbol"].to_pylist() == ["AAA"]  # BBB is not mapped: not built


def test_build_refuses_an_unmapped_source(env, run, capsys):
    assert run("build", "--source", "tiingo") == EXIT_ERROR
    assert "--all-symbols" in capsys.readouterr().err


def test_fetch_write_needs_reports(env, run, monkeypatch, capsys):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    monkeypatch.delenv("MOF_MARKET_REPORTS")
    code = run("fetch", "--write", "--source", "tiingo", "--date", "2025-06-02", "--symbols", "A")
    assert code == EXIT_ERROR
    assert not env["archive"].exists()


def test_fetch_failure_archives_the_error_then_reports(env, run, monkeypatch):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo(fail=True))
    code = run("fetch", "--write", "--source", "tiingo", "--date", "2025-06-02", "--symbols", "A")
    assert code == EXIT_ERROR
    (only,) = RawArchive(env["archive"]).history("tiingo", "A", "prices")
    assert only.status == "error"
    (report,) = env["reports"].glob("*-report.json")
    assert json.loads(report.read_text())["error"] == "TiingoError"


def test_fetch_argument_errors(env, run):
    assert run("fetch", "--source", "tiingo", "--date", "2025-06-02") == EXIT_ERROR
    assert (
        run(
            "fetch",
            "--source",
            "tiingo",
            "--date",
            "2025-06-02",
            "--from",
            "2025-06-02",
            "--to",
            "2025-06-03",
            "--symbols",
            "A",
        )
        == EXIT_ERROR
    )
    assert (
        run(
            "fetch",
            "--source",
            "massive",
            "--kind",
            "actions",
            "--date",
            "2025-06-02",
            "--symbols",
            "A",
        )
        == EXIT_ERROR
    )


# --- exit codes and error reports --------------------------------------------------------------


def test_exit_codes(env, run, capsys):
    assert run(*HISTORY_ARGS) == EXIT_OK
    assert run("build", "--source", "tiingo", "--all-symbols") == EXIT_DEGRADED
    assert run("build", "--source", "tiingo", "--all-symbols", "--allow-degraded") == EXIT_OK
    assert run("load-history", "--dir", "{root}/missing") == EXIT_ERROR
    lines = [x for x in capsys.readouterr().out.splitlines() if x]
    assert lines[1].startswith("build: degraded (dry run)")
    assert "reasons=fetch_logs_not_recorded" in lines[1]
    assert lines[3].startswith("load-history: error (dry run)")


def test_error_report_names_the_type_only(env, run, capsys):
    assert run("load-history", "--dir", "{root}/missing") == EXIT_ERROR
    (report,) = env["reports"].glob("*-report.json")
    data = json.loads(report.read_text())
    assert data["outcome"] == "error"
    assert data["error"].isidentifier()
    assert "missing" not in report.read_text()
    assert "missing" in capsys.readouterr().err  # the operator still sees why


def test_summarize_exception_yields_an_error_report(env, run, monkeypatch):
    real = pipeline.summarize

    def broken(run_id, **kw):
        if kw.get("error") is None:
            raise KeyError("a caller bug")
        return real(run_id, **kw)

    monkeypatch.setattr(pipeline, "summarize", broken)
    assert run(*HISTORY_ARGS) == EXIT_ERROR
    (report,) = env["reports"].glob("*-report.json")
    assert json.loads(report.read_text())["error"] == "KeyError"


def test_error_without_reports_dir_still_exits_2(env, run, monkeypatch, capsys):
    monkeypatch.delenv("MOF_MARKET_REPORTS")
    assert run("load-history", "--dir", "{root}/missing") == EXIT_ERROR
    assert "no error report written" in capsys.readouterr().err


def test_errored_piece_makes_the_report_an_error(env, run):
    assert run("load-history", "--write", "--dir", "{root}/missing") == EXIT_ERROR
    assert run("report", "--write") == EXIT_ERROR


def test_report_merges_newest_piece_per_step_and_source(env, run, monkeypatch):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    fetch = ("fetch", "--write", "--source", "tiingo", "--date", "2025-06-02", "--symbols")
    assert run(*fetch, "AAA") == EXIT_OK
    assert run(*fetch, "BBB") == EXIT_OK
    reports = env["reports"]
    result = pipeline.report(reports, run_id="r1", started_at=NOW, write=False)
    assert result.counts == {"pieces": 1}  # the newest fetch of tiingo
    assert result.summary.fetches == {"prices": {"tiingo": 1}}
    ids = env["root"] / "ids.parquet"
    pq.write_table(identifiers({"tiingo:AAA": I1, "tiingo:BBB": "mof:" + "2" * 32}), ids)
    assert run("build", "--write", "--source", "tiingo", "--identifiers", str(ids)) == EXIT_OK
    result = pipeline.report(reports, run_id="r2", started_at=NOW, write=False)
    assert result.counts == {"pieces": 1}  # the build supersedes the fetch pieces
    assert result.summary.fetches == {"prices": {"tiingo": 2}}
    assert isinstance(result.summary, RunSummary)


# --- pipeline helpers --------------------------------------------------------------------------


def test_plan_fetch_massive_and_closed_sessions():
    plan = pipeline.plan_fetch(
        "massive", "prices", [], date(2025, 6, 6), date(2025, 6, 10), now=NOW
    )
    # Friday and Monday closed; Tuesday 2025-06-10 closes at 20:00 UTC, after NOW
    assert [r.requested_from for r in plan.requests] == [date(2025, 6, 6), date(2025, 6, 9)]
    assert {r.source_symbol for r in plan.requests} == {"*"}
    assert "5 a minute" in plan.quota
    with pytest.raises(ValueError, match="history"):
        pipeline.plan_fetch("massive", "prices", [], date(2020, 1, 2), None, now=NOW)
    with pytest.raises(ValueError, match="tiingo"):
        pipeline.plan_fetch("yahoo", "prices", ["A"], date(2025, 6, 6), None, now=NOW)
    assert len(pipeline.plan_fetch("massive", "symbols", [], None, None, now=NOW).requests) == 2


def test_mapped_symbols_reads_latest_open_windows():
    t = identifiers({"tiingo:AAA": I1, "tiingo:BBB": I1, "massive:CCC": I1})
    later = datetime(2025, 6, 9, tzinfo=UTC)
    closed = identifiers({"tiingo:BBB": I1}, valid_to=date(2000, 1, 1), known_at=later)
    both = pa.concat_tables([t, closed])
    assert pipeline.mapped_symbols(both, "tiingo") == ["AAA"]
    assert pipeline.mapped_symbols(both, "massive") == ["CCC"]


def test_openfigi_series_skips_export_dated_and_observed():
    spans = pa.table(
        {
            "source_code": ["tiingo", "tiingo", "private_history", "massive"],
            "source_symbol": ["AAA", "BBB", "t_x", "*"],
            "first_date": pa.array([date(2025, 1, 1)] * 4, pa.date32()),
            "last_date": pa.array([date(2025, 2, 1)] * 4, pa.date32()),
        }
    )
    obs = pa.table(
        {
            "source_code": ["tiingo"],
            "source_symbol": ["BBB"],
            "id_scheme": ["composite_figi"],
            "id_value": ["BBG000FAKC02"],
        }
    )
    assert pipeline.openfigi_series(obs, spans) == [("tiingo", "AAA")]


def test_landing_root():
    assert pipeline.landing_root("/x") == Path("/x")
    assert pipeline.landing_root(None, {"OPEN_DATA_STORE_DATA_ROOT": "/d"}) == Path("/d/landing")
    with pytest.raises(ValueError, match="OPEN_DATA_STORE_DATA_ROOT"):
        pipeline.landing_root(None, {})


# --- helpers -----------------------------------------------------------------------------------


def identifiers(
    mapping: dict[str, str],
    valid_to: date | None = None,
    known_at: datetime = datetime(2025, 6, 8, tzinfo=UTC),
) -> pa.Table:
    rows = [
        dict(
            instrument_id=inst,
            id_scheme="source_symbol",
            id_value=value,
            valid_to=valid_to,
            link_reason=None,
            known_at_basis="load_time",
            valid_date=date(2000, 1, 1),
            known_at=known_at,
            known_at_kind="collected",
            source_code=value.split(":")[0],
            source_doc_id=None,
        )
        for value, inst in mapping.items()
    ]
    return pa.Table.from_pylist(rows, schema=INSTRUMENT_IDENTIFIERS.schema)


def fake_tiingo(fail: bool = False):
    import httpx
    from test_api import KEY, PAYLOADS

    from market_data.adapters.tiingo import TiingoAdapter

    def answer(request):
        if fail:
            return httpx.Response(500)
        ticker = request.url.path.split("/")[3]
        return httpx.Response(200, content=json.dumps(PAYLOADS.get(ticker, [])).encode())

    return TiingoAdapter(
        KEY,
        transport=httpx.MockTransport(answer),
        state_dir=None,
        min_spacing_s=0,
        sleep=lambda s: None,
        max_retries=0,
        now=lambda: NOW,
    )


# --- review fixes: run records, fetch scope, coverage, vendor divergence ------------------------


def pieces(env) -> list[dict]:
    return [
        json.loads(p.read_text()) for p in sorted((env["reports"] / "runs").glob("*/summary.json"))
    ]


def test_failed_history_load_is_superseded_by_a_later_success(env, run):
    assert run("load-history", "--write", "--dir", "{root}/missing") == EXIT_ERROR
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    run("map", "--write", "--no-existing")
    assert [(p["step"], p["source"]) for p in pieces(env)][:2] == [
        ("load-history", "private_history"),
        ("load-history", "private_history"),
    ]
    code = run("report", "--write", "--allow-degraded")
    assert code == EXIT_OK
    (report,) = env["reports"].glob("*-report-report.json")
    data = json.loads(report.read_text())
    assert data["outcome"] != "error" and data["error"] is None


def massive_actions(source):
    from test_massive import Recorder, fixture, make, ok

    return make(
        Recorder(
            [
                ok(fixture("dividends_page1.json")),
                ok(fixture("dividends_page2.json")),
                ok(fixture("splits.json")),
            ]
        )
    )


def test_fetch_of_another_kind_is_judged_on_its_own_kind(env, run, monkeypatch, capsys):
    monkeypatch.setenv("MOF_MARKET_ADAPTERS", "massive")
    monkeypatch.setattr(cli, "make_adapter", massive_actions)
    argv = ("fetch", "--write", "--source", "massive", "--kind", "actions")
    assert run(*argv, "--from", "2026-11-20", "--to", "2026-11-23") == EXIT_OK
    assert "fetch: ok source=massive fetches=2" in capsys.readouterr().out
    assert [(p["step"], p["source"], p["kind"]) for p in pieces(env)] == [
        ("fetch", "massive", "actions")
    ]


def test_fetch_that_records_nothing_is_degraded(env, run, monkeypatch, capsys):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    # a Saturday: no session, no request
    argv = ("fetch", "--write", "--source", "tiingo", "--date", "2025-06-07", "--symbols", "AAA")
    assert run(*argv) == EXIT_DEGRADED
    assert "reasons=fetches_not_recorded" in capsys.readouterr().out


def test_fetch_pieces_keyed_by_kind(env, run, monkeypatch):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    assert (
        run("fetch", "--write", "--source", "tiingo", "--date", "2025-06-02", "--symbols", "AAA")
        == EXIT_OK
    )
    monkeypatch.setenv("MOF_MARKET_ADAPTERS", "massive")
    monkeypatch.setattr(cli, "make_adapter", massive_actions)
    argv = ("fetch", "--write", "--source", "massive", "--kind", "actions")
    assert run(*argv, "--from", "2026-11-20", "--to", "2026-11-23") == EXIT_OK
    result = pipeline.report(env["reports"], run_id="r", started_at=NOW, write=False)
    assert result.counts == {"pieces": 2}
    assert result.summary.fetches == {"prices": {"tiingo": 1}, "actions": {"massive": 2}}


def test_report_without_coverage_is_degraded(env, run, capsys):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    assert run("report") == EXIT_DEGRADED
    assert "coverage_not_run" in capsys.readouterr().out


def test_report_runs_the_store_coverage(env, run, capsys):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    run("map", "--write", "--no-existing")
    assert publish_pending(env["cfg"]).exit_code == 0
    capsys.readouterr()
    code = run(
        "report",
        "--write",
        "--allow-degraded",
        "--index",
        "IDX1",
        "--as-of",
        "2020-04-10T00:00:00+00:00",
    )
    assert code == EXIT_OK
    assert "coverage_not_run" not in capsys.readouterr().out
    (report,) = env["reports"].glob("*-report-report.json")
    data = json.loads(report.read_text())
    assert data["coverage"]["sessions"] > 0
    assert "coverage_not_run" not in data["reasons"]


def test_report_coverage_of_an_unknown_index_is_empty(env, run, capsys):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    assert publish_pending(env["cfg"]).exit_code == 0
    assert run("report", "--index", "NOPE") == EXIT_DEGRADED
    assert "coverage_empty" in capsys.readouterr().out


def test_tiingo_build_checks_vendor_divergence(env, run, monkeypatch, capsys):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    assert (
        run(
            "fetch",
            "--write",
            "--source",
            "tiingo",
            "--from",
            "2025-06-02",
            "--to",
            "2025-06-06",
            "--symbols",
            "AAA",
        )
        == EXIT_OK
    )
    ids = env["root"] / "ids.parquet"
    pq.write_table(identifiers({"tiingo:AAA": I1}), ids)
    capsys.readouterr()
    assert run("build", "--write", "--source", "tiingo", "--identifiers", str(ids)) == EXIT_OK
    # the synthetic adjClose equals the raw close: the 2:1 split day diverges (the 0.5 dividend
    # day stays under the 1 % default threshold)
    assert "vendor_divergence=1" in capsys.readouterr().out
    (piece,) = [p for p in pieces(env) if p["step"] == "build"]
    assert piece["summary"]["quality"]["vendor_divergence"] == 1


def test_run_record_failure_after_landed_batches_exits_2(env, run, monkeypatch, capsys):
    def broken(reports, result):
        raise OSError("disk full")

    monkeypatch.setattr(pipeline, "save_piece", broken)
    assert run(*HISTORY_ARGS, "--write") == EXIT_ERROR
    err = capsys.readouterr().err
    assert "batches landed but run record not saved: OSError" in err
    assert list((env["store"] / "landing").rglob("batch.json"))


def test_history_artifacts_are_saved_before_batches(env, run, monkeypatch):
    def broken(*a, **kw):
        raise OSError("landing unavailable")

    monkeypatch.setattr(pipeline, "_write", broken)
    assert run(*HISTORY_ARGS, "--write") == EXIT_ERROR
    (load_run,) = [p for p in (env["reports"] / "runs").iterdir() if p.name.endswith("history")]
    assert (load_run / "observations.parquet").is_file()
    assert (load_run / "spans.parquet").is_file()


# --- re-review: usage errors, successful artifacts, record wording --------------------------------


def test_usage_error_leaves_no_record(env, run, monkeypatch, capsys):
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    argv = ("fetch", "--write", "--source", "tiingo", "--kind", "actions", "--date", "2025-06-02")
    assert run(*argv, "--symbols", "AAA") == EXIT_ERROR
    assert "usage:" in capsys.readouterr().err
    assert not env["reports"].exists() and not env["archive"].exists()
    assert run("report", "--index", "IDX1", "--allowed-restricted", "nosuch") == EXIT_ERROR
    assert "['nosuch']" in capsys.readouterr().err
    assert not env["reports"].exists()


def test_saved_artifacts_skip_failed_runs(env, run, monkeypatch):
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    (good,) = pipeline.saved_artifacts(env["reports"], "load-history", "observations.parquet")

    def broken(*a, **kw):
        raise OSError("landing unavailable")

    monkeypatch.setattr(pipeline, "_write", broken)
    assert run(*HISTORY_ARGS, "--write") == EXIT_ERROR
    assert len(list((env["reports"] / "runs").iterdir())) == 2
    assert pipeline.saved_artifacts(env["reports"], "load-history", "observations.parquet") == [
        good
    ]


def test_record_wording(env, run, monkeypatch, capsys):
    def broken(reports, result):
        raise OSError("disk full")

    monkeypatch.setattr(pipeline, "save_piece", broken)
    monkeypatch.setattr(cli, "make_adapter", lambda source: fake_tiingo())
    argv = ("fetch", "--write", "--source", "tiingo", "--date", "2025-06-02", "--symbols", "AAA")
    assert run(*argv) == EXIT_ERROR
    assert "fetch: archive written but run record not saved: OSError" in capsys.readouterr().err
    assert run("load-history", "--write", "--dir", "{root}/missing") == EXIT_ERROR
    err = capsys.readouterr().err
    assert "load-history: error report written, but run record not saved: OSError" in err


# --- final review fix wave -----------------------------------------------------------------------

MASSIVE_FIXTURES = Path(__file__).parent / "fixtures" / "massive"


def archive_massive(env, name: str, kind: str, *, day: date | None = None, at=NOW) -> None:
    """Put one synthetic Massive fixture in the archive as a whole-market fetch."""
    from market_data.adapters.base import FetchResult

    payload = (MASSIVE_FIXTURES / name).read_bytes()
    RawArchive(env["archive"]).put(
        FetchResult("massive", "*", kind, day, day, at, "ok", payload, "application/json")
    )


def test_map_takes_a_history_load_and_archived_massive_symbols(env, run, monkeypatch, capsys):
    monkeypatch.setenv("MOF_MARKET_ADAPTERS", "massive")
    assert run(*HISTORY_ARGS, "--write") == EXIT_OK
    archive_massive(env, "tickers_active.json", "symbols")
    archive_massive(env, "grouped_daily.json", "prices", day=date(2026, 11, 20))
    capsys.readouterr()
    assert run("map", "--write", "--no-existing") in (EXIT_OK, EXIT_DEGRADED)
    assert "map: error" not in capsys.readouterr().out
    assert publish_pending(env["cfg"]).exit_code == 0
    with Store.open(env["cfg"]) as store:
        values = set(store.read_asof("instrument_identifiers")["id_value"].to_pylist())
    assert "massive:AAA" in values  # the provider series mapped next to the history's
    assert any(v.startswith("private_history:") for v in values)

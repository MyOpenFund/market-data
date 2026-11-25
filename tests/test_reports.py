import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest
from open_data_store.contracts.standard.market import FETCH_LOG
from open_data_store.market.coverage import (
    COVERAGE_PER_MEMBER_SCHEMA,
    COVERAGE_PER_SESSION_SCHEMA,
    CoverageResult,
)

from market_data.adapters.base import FetchResult
from market_data.adapters.file import Ignored, LoadReport
from market_data.build import BuildResult
from market_data.quality import QUALITY_FLAGS_SCHEMA
from market_data.reports import (
    REPORTS_ENV,
    RunSummary,
    kept_out_counts,
    summarize,
    write_report,
)
from market_data.resolve.mapping import LABELS_LEFT_OUT_SCHEMA, REPORT_SCHEMA, MappingResult

SRC = "synth"
LABEL = "Zyxwv Fictional Holdings Member 0042"  # a synthetic restricted label
STARTED = datetime(2024, 1, 15, 22, tzinfo=UTC)


def report_table(rows) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"source_code": s, "source_symbol": sym, "reason": r, "candidates": 0}
            for s, sym, r in rows
        ],
        schema=REPORT_SCHEMA,
    )


def mapping(unresolved=(), ambiguous=(), changed=(), left_out=()) -> MappingResult:
    return MappingResult(
        identifiers=pa.table({}),
        unresolved=report_table(unresolved),
        ambiguous=report_table(ambiguous),
        minted=("mof:" + "a" * 32,),
        labels_left_out=pa.Table.from_pylist(
            [
                {"source_code": s, "source_symbol": sym, "reason": r, "rows": n}
                for s, sym, r, n in left_out
            ],
            schema=LABELS_LEFT_OUT_SCHEMA,
        ),
        changed=report_table(changed),
    )


def coverage_result(shares) -> CoverageResult:
    days = [date(2024, 1, 2 + i) for i in range(len(shares))]
    per_session = pa.table(
        {
            "session_date": pa.array(days, pa.date32()),
            "members": pa.array([100] * len(shares), pa.int64()),
            "unmapped": pa.array([0] * len(shares), pa.int64()),
            "delisted": pa.array([0] * len(shares), pa.int64()),
            "missing": pa.array([int(s * 100) for s in shares], pa.int64()),
            "missing_share": pa.array(shares, pa.float64()),
            "over_threshold": pa.array([s > 0.02 for s in shares]),
        },
        schema=COVERAGE_PER_SESSION_SCHEMA,
    )
    return CoverageResult(
        COVERAGE_PER_MEMBER_SCHEMA.empty_table(),
        per_session,
        not any(s > 0.02 for s in shares),
    )


def flags(rows) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"source_symbol": sym, "session_date": d, "check": c, "detail": "x"}
            for sym, d, c in rows
        ],
        schema=QUALITY_FLAGS_SCHEMA,
    )


def failed_log(status: str) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "source_symbol": LABEL,
                "fetch_id": "r1",
                "requested_from": date(2024, 1, 2),
                "requested_to": date(2024, 1, 5),
                "returned_from": None,
                "returned_to": None,
                "status": status,
                "row_count": 0,
                "archive_ref": "r1",
                "known_at_basis": "fetch_time",
                "valid_date": date(2024, 1, 6),
                "known_at": datetime(2024, 1, 6, tzinfo=UTC),
                "known_at_kind": "collected",
                "source_code": SRC,
                "source_doc_id": None,
            }
        ],
        schema=FETCH_LOG.schema,
    )


def clean(**overrides) -> RunSummary:
    parts = {
        "builds": {"prices": BuildResult(pa.table({}))},
        "mapping": mapping(),
        "quality": flags([]),
        "coverage": coverage_result([0.0, 0.01]),
        "fetch_logs": {"prices": failed_log("ok")},
        "written": {"prices": 10},
    }
    parts.update(overrides)
    return summarize("run-1", started_at=STARTED, **parts)


# --- outcome ------------------------------------------------------------------------------------


def test_outcome_ok_only_with_clean_evidence():
    run = clean()
    assert run.outcome == "ok"
    assert run.reasons == ()


def test_outcome_degraded_on_unresolved():
    run = clean(mapping=mapping(unresolved=[(SRC, LABEL, "no_answer")]))
    assert run.outcome == "degraded"
    assert "unresolved_identifiers" in run.reasons


def test_outcome_degraded_on_ambiguous():
    run = clean(mapping=mapping(ambiguous=[(SRC, LABEL, "several_composites")]))
    assert run.outcome == "degraded"
    assert "ambiguous_identifiers" in run.reasons


@pytest.mark.parametrize("status", ["error", "partial"])
def test_outcome_degraded_on_unsuperseded_failed_fetch(status):
    run = clean(fetch_logs={"prices": failed_log(status)})
    assert run.outcome == "degraded"
    assert run.reasons == ("failed_fetches",)
    assert run.failed_fetches == {"prices": {SRC: 1}}


def test_outcome_degraded_on_coverage_failure():
    run = clean(coverage=coverage_result([0.0, 0.5, 0.03]))
    assert run.outcome == "degraded"
    assert run.reasons == ("coverage_failed",)
    assert run.coverage["worst_missing_share"] == 0.5
    assert run.coverage["sessions_over_threshold"] == 2
    assert run.coverage["over_threshold_sessions"] == ["2024-01-03", "2024-01-04"]


def test_outcome_error_on_raised_failure_keeps_no_message():
    run = clean(error=ValueError(f"tied values for key ('{LABEL}', 2024-01-02)"))
    assert run.outcome == "error"
    assert run.error == "ValueError"
    assert LABEL not in json.dumps(run.to_dict())


def test_never_ok_by_default():
    run = summarize("run-1", started_at=STARTED)
    assert run.outcome == "degraded"
    assert run.reasons == ("nothing_recorded",)
    assert RunSummary(run_id="run-2", started_at=STARTED).outcome == "degraded"


def test_kept_out_rows_and_flags_do_not_degrade_but_are_counted():
    run = clean(
        quality=flags(
            [
                ("AAA", date(2024, 1, 2), "jump_without_action"),
                ("AAA", date(2024, 1, 3), "jump_without_action"),
                ("BBB", date(2024, 1, 3), "missing_session"),
            ]
        ),
        builds={
            "prices": BuildResult(
                pa.table({}),
                {"non_session": (("AAA", date(2024, 1, 6)),), "intraday_partial": ()},
            )
        },
    )
    assert run.outcome == "ok"
    assert run.quality == {
        "non_positive": 0,
        "jump_without_action": 2,
        "missing_session": 1,
        "vendor_divergence": 0,
    }
    assert run.builder_drops == {"prices": {"non_session": 1, "intraday_partial": 0}}


def test_run_id_and_error_validated():
    with pytest.raises(ValueError, match="run_id"):
        RunSummary(run_id="../x", started_at=STARTED)
    with pytest.raises(ValueError, match="error"):
        RunSummary(run_id="r", started_at=STARTED, error=f"boom {LABEL}")
    with pytest.raises(ValueError, match="timezone-aware"):
        RunSummary(run_id="r", started_at=datetime(2024, 1, 1))


# --- kept-out counts ----------------------------------------------------------------------------


@dataclass
class FakeReport:
    keys: dict
    rows: dict

    def counts(self):
        return dict(self.keys)

    @property
    def row_counts(self):
        return dict(self.rows)


@dataclass
class FakeAdapter:
    source_code: str = SRC
    actions_fetch_kind: str | None = None
    seen: list = field(default_factory=list)

    def parse_report(self, result):
        self.seen.append((result.kind, result.status))
        if result.kind == "prices":
            return FakeReport(
                {"non_finite": 1, "bad_volume": 0}, {"non_finite": 3, "bad_volume": 0}
            )
        return FakeReport({"bad_ratio": 2}, {"bad_ratio": 2})


def fetch(kind, status) -> FetchResult:
    return FetchResult(
        source_code=SRC,
        source_symbol="AAA",
        kind=kind,
        requested_from=date(2024, 1, 2),
        requested_to=date(2024, 1, 5),
        fetched_at=STARTED,
        status=status,
        payload=b"[]",
        content_type="application/json",
    )


def test_kept_out_counts_sum_keys_and_rows():
    adapter = FakeAdapter()
    out = kept_out_counts(
        adapter,
        [
            fetch("prices", "ok"),
            fetch("prices", "partial"),
            fetch("prices", "error"),
            fetch("actions", "ok"),
            fetch("symbols", "ok"),
        ],
    )
    assert out == {
        "prices": {"non_finite": {"keys": 2, "rows": 6}, "bad_volume": {"keys": 0, "rows": 0}},
        "actions": {"bad_ratio": {"keys": 2, "rows": 2}},
    }
    assert ("prices", "error") not in adapter.seen  # failed fetches carry no rows


def test_kept_out_counts_reads_actions_from_the_prices_payload():
    adapter = FakeAdapter(actions_fetch_kind="prices")
    out = kept_out_counts(adapter, [fetch("prices", "ok")])
    assert out["actions"] == {"bad_ratio": {"keys": 2, "rows": 2}}
    assert ("actions", "ok") in adapter.seen


# --- write_report -------------------------------------------------------------------------------


def full_run() -> RunSummary:
    load = LoadReport(
        dropped={"prices_daily": {"bad_close": 2}},
        unmapped_exchanges={"securities": {"ZZ": 1}},
        ignored=Ignored(
            files=[f"{LABEL}.csv"], columns={"prices.csv": [LABEL]}, manifest_keys=[LABEL]
        ),
        fetch_log={"ok": 3},
    )
    return clean(
        mapping=mapping(
            unresolved=[(SRC, LABEL, "no_answer"), ("private_history", "t_" + "0" * 20, "x")],
            ambiguous=[(SRC, LABEL, "several_composites")],
            changed=[(SRC, LABEL, "composite_changed")],
            left_out=[(SRC, LABEL, "stale", 4)],
        ),
        quality=flags([(LABEL, date(2024, 1, 2), "non_positive")]),
        fetch_logs={"prices": failed_log("error")},
        load_report=load,
        kept_out={SRC: {"prices": {"non_finite": {"keys": 1, "rows": 1}}}},
    )


def test_write_report_files(tmp_path):
    run = full_run()
    json_path, md_path = write_report(run, tmp_path / "reports")
    assert json_path.name == "run-1-report.json"
    assert md_path.name == "run-1-report.md"
    data = json.loads(json_path.read_text())
    assert data["outcome"] == "degraded"
    assert set(data["reasons"]) == {
        "unresolved_identifiers",
        "ambiguous_identifiers",
        "failed_fetches",
    }
    assert data["mapping"]["unresolved"] == {"private_history": {"x": 1}, SRC: {"no_answer": 1}}
    assert data["mapping"]["labels_left_out"] == {SRC: {"stale": 4}}
    assert data["file_load"]["ignored"] == {
        "files": 1,
        "columns": {"prices.csv": 1},
        "manifest_keys": 1,
    }
    md = md_path.read_text()
    assert "degraded" in md and "run-1" in md
    assert not list((tmp_path / "reports").glob(".*"))  # no temp file left behind


def test_report_never_contains_a_label(tmp_path):
    json_path, md_path = write_report(full_run(), tmp_path)
    for path in (json_path, md_path):
        text = path.read_text()
        assert LABEL not in text
        assert "Zyxwv" not in text


def test_report_dir_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv(REPORTS_ENV, str(tmp_path / "env"))
    json_path, _ = write_report(clean())
    assert json_path.parent == tmp_path / "env"
    monkeypatch.delenv(REPORTS_ENV)
    with pytest.raises(ValueError, match=REPORTS_ENV):
        write_report(clean())


def test_refuse_report_dir_in_a_git_checkout(tmp_path):
    (tmp_path / "repo" / ".git").mkdir(parents=True)
    with pytest.raises(ValueError, match="git checkout"):
        write_report(clean(), tmp_path / "repo" / "reports")
    assert not (tmp_path / "repo" / "reports").exists()


def test_rewrite_replaces_atomically(tmp_path):
    write_report(clean(error=RuntimeError("x")), tmp_path)
    json_path, _ = write_report(clean(), tmp_path)
    assert json.loads(json_path.read_text())["outcome"] == "ok"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["run-1-report.json", "run-1-report.md"]

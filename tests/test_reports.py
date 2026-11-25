import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest
from open_data_store.contracts.standard.market import FETCH_LOG, INSTRUMENT_IDENTIFIERS
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
    OTHER,
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


def mapping(
    unresolved=(), ambiguous=(), changed=(), left_out=(), identifiers=None
) -> MappingResult:
    return MappingResult(
        identifiers=INSTRUMENT_IDENTIFIERS.schema.empty_table()
        if identifiers is None
        else identifiers,
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


def failed_log(status: str, symbol: str = LABEL) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "source_symbol": symbol,
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
    run = clean(mapping=mapping(unresolved=[(SRC, LABEL, "no_composite_figi")]))
    assert run.outcome == "degraded"
    assert "unresolved_identifiers" in run.reasons


def test_outcome_degraded_on_ambiguous():
    run = clean(mapping=mapping(ambiguous=[(SRC, LABEL, "several_composite_figis")]))
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
    assert run.reasons == ("mapping_not_recorded", "fetch_logs_not_recorded")
    assert RunSummary(run_id="run-2", started_at=STARTED).outcome == "degraded"


def test_outcome_degraded_without_mapping_evidence():
    run = clean(mapping=None)
    assert run.reasons == ("mapping_not_recorded",)


@pytest.mark.parametrize(
    "fetch_logs",
    [
        None,
        {},
        {"actions": failed_log("ok")},
        {"prices": FETCH_LOG.schema.empty_table()},
    ],
)
def test_outcome_degraded_without_a_prices_fetch_log(fetch_logs):
    run = clean(fetch_logs=fetch_logs)
    assert run.outcome == "degraded"
    assert run.reasons == ("fetch_logs_not_recorded",)


def test_history_file_fetch_log_is_prices_evidence():
    load = LoadReport(fetch_log={"members": 3, "ok": 3, "empty": 0})
    assert clean(fetch_logs=None, load_report=load).outcome == "ok"
    empty = LoadReport(fetch_log={"members": 0})
    assert clean(fetch_logs=None, load_report=empty).reasons == ("fetch_logs_not_recorded",)


def test_outcome_degraded_on_empty_coverage():
    run = clean(coverage=coverage_result([]))
    assert run.reasons == ("coverage_empty",)
    assert run.coverage["worst_missing_share"] is None


def test_changed_mappings_counted_without_degrading():
    run = clean(mapping=mapping(changed=[(SRC, LABEL, "composite_changed")]))
    assert run.outcome == "ok"
    assert run.mapping["changed"] == {SRC: {"composite_changed": 1}}


def test_overlapping_mapping_windows_degrade_as_ambiguous():
    ids = pa.Table.from_pylist(
        [
            {
                "instrument_id": inst,
                "id_scheme": "source_symbol",
                "id_value": f"{SRC}:AAA",
                "valid_to": None,
                "link_reason": None,
                "known_at_basis": "load_time",
                "valid_date": start,
                "known_at": STARTED,
                "known_at_kind": "collected",
                "source_code": "openfigi",
                "source_doc_id": None,
            }
            for inst, start in (
                ("mof:" + "a" * 32, date(2024, 1, 1)),
                ("mof:" + "b" * 32, date(2024, 1, 5)),
            )
        ],
        schema=INSTRUMENT_IDENTIFIERS.schema,
    )
    run = clean(mapping=mapping(identifiers=ids))
    assert run.mapping["overlapping_windows"] == 1
    assert run.reasons == ("ambiguous_identifiers",)


def test_nan_missing_share_serialised_as_null_and_skipped():
    cov = coverage_result([0.0, 0.01])
    per = cov.per_session.set_column(
        5, "missing_share", pa.array([float("nan"), 0.01], pa.float64())
    )
    run = clean(coverage=CoverageResult(cov.per_member, per, True))
    assert run.coverage["worst_missing_share"] == 0.01
    per = per.set_column(5, "missing_share", pa.array([float("nan")] * 2, pa.float64()))
    run = clean(coverage=CoverageResult(cov.per_member, per, True))
    assert json.loads(json.dumps(run.to_dict()))["coverage"]["worst_missing_share"] is None


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
        OTHER: 0,
    }
    assert run.builder_drops == {"prices": {"non_session": 1, "intraday_partial": 0}}


def test_run_id_and_error_validated():
    with pytest.raises(ValueError, match="run_id"):
        RunSummary(run_id="../x", started_at=STARTED)
    with pytest.raises(ValueError, match="error"):
        RunSummary(run_id="r", started_at=STARTED, error=f"boom {LABEL}")
    with pytest.raises(ValueError, match="timezone-aware"):
        RunSummary(run_id="r", started_at=datetime(2024, 1, 1))


@pytest.mark.parametrize(
    "fields",
    [
        {"quality": {LABEL: 1}},
        {"quality": {"non_positive": "1"}},
        {"quality": {"non_positive": True}},
        {"written": {"prices": -1}},
        {"written": {LABEL: 1}},
        {"failed_fetches": {"prices": {LABEL: 1}}},
        {"kept_out": {SRC: {"prices": {"non_finite": {"keys": 1}}}}},
        {"mapping": {"unresolved": {}}},
        {"coverage": {"passed": True}},
    ],
)
def test_run_summary_fields_validated(fields):
    with pytest.raises(ValueError, match="RunSummary"):
        RunSummary(run_id="r", started_at=STARTED, **fields)


def test_summarize_maps_free_text_keys():
    with pytest.raises(ValueError, match="contract"):
        clean(written={LABEL: 1})
    with pytest.raises(ValueError, match="fetch kind"):
        clean(fetch_logs={LABEL: failed_log("ok")})
    with pytest.raises(ValueError, match="fetch kind"):
        clean(kept_out={SRC: {LABEL: {}}})
    run = clean(kept_out={LABEL: {"prices": {LABEL: {"keys": 1, "rows": 2}}}})
    assert run.kept_out == {OTHER: {"prices": {OTHER: {"keys": 1, "rows": 2}}}}


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
        dropped={"prices": {"non_session": 2, LABEL: 1}, LABEL: {"duplicate_row": 1}},
        unmapped_exchanges={"prices": {LABEL: 1, "ZZ": 2}, LABEL: {"YY": 1}},
        unknown_action_types={LABEL + " merger": 2, "Odd lot": 1},
        ignored=Ignored(
            files=[f"{LABEL}.csv"],
            columns={f"{LABEL}.csv": [LABEL, "x"], "prices.csv": "3 unknown column(s)"},
            manifest_keys=[LABEL],
        ),
        fetch_log={"members": 3, "ok": 3, LABEL: 1},
    )
    return clean(
        mapping=mapping(
            unresolved=[
                (SRC, LABEL, "no_composite_figi"),
                ("private_history", "t_" + "0" * 20, LABEL),
                (LABEL, "x", "no_span"),
            ],
            ambiguous=[(SRC, LABEL, "several_composite_figis")],
            changed=[(SRC, LABEL, "composite_changed")],
            left_out=[(SRC, LABEL, "outside_windows", 4)],
        ),
        quality=flags([(LABEL, date(2024, 1, 2), "non_positive"), ("X", date(2024, 1, 2), LABEL)]),
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
    assert data["mapping"]["unresolved"] == {
        OTHER: {"no_span": 1},
        "private_history": {OTHER: 1},
        SRC: {"no_composite_figi": 1},
    }
    assert data["mapping"]["labels_left_out"] == {SRC: {"outside_windows": 4}}
    assert data["quality"]["non_positive"] == 1 and data["quality"][OTHER] == 1
    load = data["file_load"]
    assert load["ignored"] == {"files": 1, "column_files": 2, "columns": 5, "manifest_keys": 1}
    assert load["unmapped_exchanges"] == {
        "prices": {"distinct": 2, "rows": 3},
        OTHER: {"distinct": 1, "rows": 1},
    }
    assert load["unknown_action_types"] == {"distinct": 2, "rows": 3}
    assert load["dropped"] == {"prices": {"non_session": 2, OTHER: 1}, OTHER: {"duplicate_row": 1}}
    assert load["fetch_log"] == {"members": 3, "ok": 3, OTHER: 1}
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

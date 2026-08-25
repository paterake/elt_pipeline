from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations import (
    BUILTIN_CHECKS_BACKEND_TYPE,
    BuiltinQualityHook,
    QualityCheckResult,
    QualityCheckStatus,
    QualityDatasetRef,
    QualityHookPolicy,
    QualityHookRequest,
    RowCountQualityHook,
    build_quality_hook,
    raise_for_blocking_quality_failures,
)
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.shared.quality import (
    BUILTIN_QUALITY_CHECK_ADAPTER,
    BuiltinQualityCheck,
    FreshnessCheck,
    NotNullCheck,
    RangeCheck,
    ReferentialIntegrityCheck,
    RegexFormatCheck,
    UniquenessCheck,
    evaluate_builtin_checks_for_dataset,
)
from elt_pipeline.shared.runtime import StageName, new_run_context


class _FailingQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]:
        raise RuntimeError("quality backend unavailable")


def test_row_count_quality_hook_skips_when_stage_has_no_datasets() -> None:
    results = RowCountQualityHook(row_count_min=1).evaluate(
        request=QualityHookRequest(
            run_id="run-001",
            stage="normalize",
            job_name="normalize-orders",
            environment="dev",
            datasets=[],
            metrics={},
        )
    )

    assert len(results) == 1
    assert results[0].status == QualityCheckStatus.skipped
    assert results[0].message == "No datasets were emitted for quality evaluation"


def test_row_count_quality_hook_returns_pass_and_fail_results_per_dataset() -> None:
    results = RowCountQualityHook(row_count_min=2).evaluate(
        request=QualityHookRequest(
            run_id="run-001",
            stage="sql",
            job_name="sql-run",
            environment="dev",
            datasets=[
                QualityDatasetRef(
                    dataset_id="level2.orders",
                    dataset_name="orders",
                    materialization_type="table",
                    target_name="orders",
                    row_count=3,
                ),
                QualityDatasetRef(
                    dataset_id="level2.items",
                    dataset_name="items",
                    materialization_type="table",
                    target_name="items",
                    row_count=1,
                ),
            ],
            metrics={},
        )
    )

    assert [result.status for result in results] == [
        QualityCheckStatus.pass_,
        QualityCheckStatus.fail,
    ]
    assert results[1].observed_value == 1
    assert results[1].expected_value == 2


def test_row_count_quality_hook_normalizes_stage_names() -> None:
    results = RowCountQualityHook(row_count_min=1, enabled_stages={" SQL "}).evaluate(
        request=QualityHookRequest(
            run_id="run-001",
            stage=" sql ",
            job_name="sql-run",
            environment="dev",
            datasets=[
                QualityDatasetRef(
                    dataset_id="level2.orders",
                    dataset_name="orders",
                    materialization_type="table",
                    target_name="orders",
                    row_count=1,
                )
            ],
            metrics={},
        )
    )

    assert len(results) == 1
    assert results[0].status == QualityCheckStatus.pass_


def test_row_count_quality_hook_rejects_negative_threshold() -> None:
    with pytest.raises(ConfigValidationError) as exc_info:
        RowCountQualityHook(row_count_min=-1)

    assert exc_info.value.context["row_count_min"] == -1


def test_row_count_quality_hook_rejects_unsupported_stages() -> None:
    with pytest.raises(ConfigValidationError) as exc_info:
        RowCountQualityHook(row_count_min=1, enabled_stages={"publish"})

    assert exc_info.value.context["invalid_stages"] == ["publish"]


def test_build_quality_hook_uses_env_configured_row_count_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", "row_count_threshold")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_ROW_COUNT_MIN", "2")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_POLICY", "blocking")

    adapter = build_quality_hook(str(tmp_path))
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    summary = adapter.evaluate(
        run_context=run_context,
        environment="dev",
        request=QualityHookRequest(
            run_id=run_context.run_id,
            stage="normalize",
            job_name=run_context.job_name,
            environment="dev",
            datasets=[],
            metrics={},
        ),
    )

    assert summary is not None
    assert summary.backend_type == "row_count_threshold"


def test_build_quality_hook_normalizes_env_configured_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", " ROW_COUNT_THRESHOLD ")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_ROW_COUNT_MIN", "2")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_POLICY", " BLOCKING ")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_STAGES", " Normalize , SQL ")

    adapter = build_quality_hook(str(tmp_path))
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    summary = adapter.evaluate(
        run_context=run_context,
        environment="dev",
        request=QualityHookRequest(
            run_id=run_context.run_id,
            stage="NORMALIZE",
            job_name=run_context.job_name,
            environment="dev",
            datasets=[
                QualityDatasetRef(
                    dataset_id="level2.orders",
                    dataset_name="orders",
                    materialization_type="table",
                    target_name="orders",
                    row_count=1,
                )
            ],
            metrics={},
        ),
    )

    assert summary is not None
    assert summary.backend_type == "row_count_threshold"
    assert summary.results[0].blocking is True
    with pytest.raises(PipelineError, match="Quality checks failed"):
        raise_for_blocking_quality_failures(summary)


def test_quality_hook_records_non_blocking_backend_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    adapter = build_quality_hook(
        str(tmp_path),
        backend=_FailingQualityBackend(),
        policy=QualityHookPolicy.best_effort,
    )

    summary = adapter.evaluate(
        run_context=run_context,
        environment="dev",
        request=QualityHookRequest(
            run_id=run_context.run_id,
            stage="normalize",
            job_name=run_context.job_name,
            environment="dev",
            datasets=[],
            metrics={},
        ),
    )

    assert summary is not None
    assert summary.results[0].status == QualityCheckStatus.warn

    run_dir = (
        tmp_path
        / "runs"
        / "stage=normalize"
        / "job=normalize-orders"
        / f"run_id={run_context.run_id}"
    )
    assert "environment=" not in str(run_dir)
    assert json.loads((run_dir / "errors.jsonl").read_text(encoding="utf-8").splitlines()[0])[
        "error_code"
    ] == "QUALITY_BACKEND_EXECUTION_FAILED"
    assert json.loads((run_dir / "logs.jsonl").read_text(encoding="utf-8").splitlines()[0])[
        "event_type"
    ] == "quality_hook_failed"


def test_build_quality_hook_rejects_invalid_env_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", "row_count_threshold")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_ROW_COUNT_MIN", "abc")

    with pytest.raises(ConfigValidationError) as exc_info:
        build_quality_hook(str(tmp_path))

    assert exc_info.value.context["row_count_min"] == "abc"


def test_quality_hook_raises_for_blocking_backend_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.sql, job_name="sql-run")
    adapter = build_quality_hook(
        str(tmp_path),
        backend=_FailingQualityBackend(),
        policy=QualityHookPolicy.blocking,
    )

    with pytest.raises(PipelineError) as exc_info:
        adapter.evaluate(
            run_context=run_context,
            environment="dev",
            request=QualityHookRequest(
                run_id=run_context.run_id,
                stage="sql",
                job_name=run_context.job_name,
                environment="dev",
                datasets=[],
                metrics={},
            ),
        )

    assert exc_info.value.error_code == "QUALITY_BACKEND_EXECUTION_FAILED"
    assert exc_info.value.context["quality_summary"]["results"][0]["status"] == "fail"


# ---------------------------------------------------------------------------
# G-8: Builtin check library + quarantine/DLQ write path
# ---------------------------------------------------------------------------


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_builtin_not_null_check_pass_and_fail() -> None:
    dataset = QualityDatasetRef(
        dataset_id="l2.users",
        dataset_name="users",
        materialization_type="table",
        target_name="users",
        row_count=3,
        records=[
            {"id": 1, "email": "a@x.com"},
            {"id": 2, "email": None},
            {"id": 3},
        ],
    )
    results = evaluate_builtin_checks_for_dataset(
        dataset=dataset,
        checks=[
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                {
                    "kind": "not_null",
                    "check_name": "email_not_null",
                    "column": "email",
                    "dataset_id": "l2.users",
                }
            )
        ],
    )
    assert len(results) == 1
    assert results[0].check_name == "email_not_null"
    assert results[0].status == QualityCheckStatus.fail
    assert results[0].observed_value == 2
    assert len(results[0].violated_records) == 2


def test_builtin_range_uniqueness_regex_passes_clean_dataset() -> None:
    records = [
        {"id": 1, "email": "a@x.com", "amount": 10.5},
        {"id": 2, "email": "b@x.com", "amount": 50.0},
        {"id": 3, "email": "c@x.com", "amount": 100.0},
    ]
    dataset = QualityDatasetRef(
        dataset_id="l2.sales",
        dataset_name="sales",
        materialization_type="table",
        target_name="sales",
        row_count=3,
        records=records,
    )
    checks: list[BuiltinQualityCheck] = [
        BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
            {
                "kind": "range",
                "check_name": "amount_range",
                "column": "amount",
                "min_value": 0.0,
                "max_value": 1000.0,
            }
        ),
        BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
            {
                "kind": "uniqueness",
                "check_name": "id_unique",
                "columns": ["id"],
            }
        ),
        BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
            {
                "kind": "regex_format",
                "check_name": "email_format",
                "column": "email",
                "pattern": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            }
        ),
    ]
    results = evaluate_builtin_checks_for_dataset(dataset=dataset, checks=checks)
    assert len(results) == 3
    statuses = {r.check_name: r.status for r in results}
    assert statuses == {
        "amount_range": QualityCheckStatus.pass_,
        "id_unique": QualityCheckStatus.pass_,
        "email_format": QualityCheckStatus.pass_,
    }


def test_builtin_referential_integrity_and_freshness_checks() -> None:
    order_records = [
        {"order_id": 1, "user_id": 1, "ordered_at": "2026-08-20T10:00:00Z"},
        {"order_id": 2, "user_id": 99, "ordered_at": "2026-08-20T11:00:00Z"},
        {"order_id": 3, "user_id": 2, "ordered_at": "2026-07-01T00:00:00Z"},
    ]
    user_records = [{"id": 1}, {"id": 2}, {"id": 3}]
    dataset = QualityDatasetRef(
        dataset_id="l2.orders",
        dataset_name="orders",
        materialization_type="table",
        target_name="orders",
        row_count=3,
        records=order_records,
    )
    now = datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC)
    results = evaluate_builtin_checks_for_dataset(
        dataset=dataset,
        checks=[
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                ReferentialIntegrityCheck(
                    check_name="orders_users_fk",
                    source_column="user_id",
                    target_dataset_id="l2.users",
                    target_column="id",
                ).model_dump()
            ),
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                FreshnessCheck(
                    check_name="orders_fresh_1week",
                    timestamp_column="ordered_at",
                    max_age_seconds=7 * 24 * 3600.0,  # 7 days
                ).model_dump()
            ),
        ],
        reference_datasets={"l2.users": user_records},
        reference_now=now,
    )
    statuses = {r.check_name: r for r in results}
    assert statuses["orders_users_fk"].status == QualityCheckStatus.fail
    assert statuses["orders_users_fk"].observed_value == 1
    assert statuses["orders_fresh_1week"].status == QualityCheckStatus.fail
    # Freshness: order_id=3 is ~55 days old → oldest exceeds 7 days window
    assert statuses["orders_fresh_1week"].observed_value is not None
    assert isinstance(statuses["orders_fresh_1week"].observed_value, (int, float))
    assert statuses["orders_fresh_1week"].observed_value >= 0


def test_builtin_hook_wires_violated_rows_through_adapter_and_writes_quarantine(
    tmp_path: Path,
) -> None:
    """Prove G-8 backlog acceptance criterion:

    "a run with bad rows quarantines them + proceeds (non-blocking)"
    """
    records = [
        {"id": 1, "email": "a@x.com"},
        {"id": 2, "email": None},  # fails not_null + format (None skipped for format)
        {"id": 1, "email": "INVALID-EMAIL"},  # fails uniqueness + format
        {"id": 3, "email": "b@x.com"},
    ]
    backend = BuiltinQualityHook(
        checks=[
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                NotNullCheck(check_name="email_not_null", column="email").model_dump()
            ),
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                UniquenessCheck(check_name="id_unique", columns=["id"]).model_dump()
            ),
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                RegexFormatCheck(
                    check_name="email_format",
                    column="email",
                    pattern=r"^[A-Za-z0-9._%+-]+@",
                ).model_dump()
            ),
        ]
    )
    run_context = new_run_context(stage=StageName.sql, job_name="sql-sales")
    adapter = build_quality_hook(
        str(tmp_path),
        backend=backend,
        policy=QualityHookPolicy.best_effort,
    )

    request = QualityHookRequest(
        run_id=run_context.run_id,
        stage="sql",
        job_name=run_context.job_name,
        environment="test",
        datasets=[
            QualityDatasetRef(
                dataset_id="l2.users",
                dataset_name="users",
                materialization_type="table",
                target_name="users",
                row_count=len(records),
                records=records,
            )
        ],
        metrics={},
    )

    summary = adapter.evaluate(
        run_context=run_context,
        environment="test",
        request=request,
    )

    assert summary is not None
    assert summary.backend_type == BUILTIN_CHECKS_BACKEND_TYPE
    # non-blocking: passed == False (failures exist) but exception NOT raised yet
    assert summary.passed is False
    # raise_for_blocking_quality_failures: zero blocking failures under best_effort
    # (adapter coerces blocking=True only if policy=blocking, here policy=best_effort)
    raise_for_blocking_quality_failures(summary)  # must NOT raise

    statuses = {r.check_name: r for r in summary.results}
    assert statuses["email_not_null"].status == QualityCheckStatus.fail
    assert statuses["id_unique"].status == QualityCheckStatus.fail
    assert statuses["email_format"].status == QualityCheckStatus.fail
    assert len(statuses["email_not_null"].violated_records) >= 1
    assert len(statuses["id_unique"].violated_records) >= 2

    # Quarantine artifacts were written via the B-6 path utilities
    run_dir = (
        tmp_path
        / "runs"
        / "stage=sql"
        / "job=sql-sales"
        / f"run_id={run_context.run_id}"
    )
    q_dir = run_dir / "quality_quarantine" / "sql"
    assert q_dir.exists()
    email_not_null_path = q_dir / "email_not_null__l2.users.jsonl"
    id_unique_path = q_dir / "id_unique__l2.users.jsonl"
    email_format_path = q_dir / "email_format__l2.users.jsonl"
    assert email_not_null_path.exists()
    assert id_unique_path.exists()
    assert email_format_path.exists()

    not_null_rows = _read_jsonl(email_not_null_path)
    assert len(not_null_rows) == statuses["email_not_null"].observed_value
    for row in not_null_rows:
        assert row["quarantine"]["check_name"] == "email_not_null"
        assert row["quarantine"]["stage"] == "sql"
        assert "record" in row
        assert row["quarantine"]["run_id"] == run_context.run_id

    unique_rows = _read_jsonl(id_unique_path)
    assert len(unique_rows) == statuses["id_unique"].observed_value

    # A quality_quarantine_written log event was emitted with paths + row counts
    logs = _read_jsonl(run_dir / "logs.jsonl")
    log_types = [log.get("event_type") for log in logs]
    assert "quality_quarantine_written" in log_types
    written = next(log for log in logs if log["event_type"] == "quality_quarantine_written")
    assert written["details"]["policy"] == "best_effort"
    assert len(written["details"]["quarantine_paths"]) == 3


def test_quarantine_artifacts_still_written_under_blocking_before_raise(
    tmp_path: Path,
) -> None:
    """Prove the blocking half of the G-8 acceptance criterion:

    blocking=blocking policy + bad rows → quarantine files are created BEFORE
    raise_for_blocking_quality_failures raises, so triage data survives the error.
    """
    records = [
        {"id": 1, "amount": 12.5},
        {"id": 2, "amount": -5.0},  # out of range
        {"id": 3, "amount": 9_999_999.0},  # out of range
        {"id": 4, "amount": 200.0},
    ]
    backend = BuiltinQualityHook(
        checks=[
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                RangeCheck(
                    check_name="amount_valid",
                    column="amount",
                    min_value=0.0,
                    max_value=1000.0,
                ).model_dump()
            )
        ]
    )
    run_context = new_run_context(stage=StageName.normalize, job_name="norm-inv")
    adapter = build_quality_hook(
        str(tmp_path),
        backend=backend,
        policy=QualityHookPolicy.blocking,
    )

    summary = adapter.evaluate(
        run_context=run_context,
        environment="prod",
        request=QualityHookRequest(
            run_id=run_context.run_id,
            stage="normalize",
            job_name=run_context.job_name,
            environment="prod",
            datasets=[
                QualityDatasetRef(
                    dataset_id="l2.invoices",
                    dataset_name="invoices",
                    materialization_type="table",
                    target_name="invoices",
                    row_count=len(records),
                    records=records,
                )
            ],
            metrics={},
        ),
    )

    assert summary is not None
    assert summary.blocking_failure_count > 0
    run_dir = (
        tmp_path
        / "runs"
        / "stage=normalize"
        / "job=norm-inv"
        / f"run_id={run_context.run_id}"
    )
    q_path = (
        run_dir
        / "quality_quarantine"
        / "normalize"
        / "amount_valid__l2.invoices.jsonl"
    )
    assert q_path.exists()
    quarantined = _read_jsonl(q_path)
    assert len(quarantined) == 2
    with pytest.raises(PipelineError, match="Quality checks failed"):
        raise_for_blocking_quality_failures(summary)


def test_build_quality_hook_loads_builtin_checks_from_json_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    specs = [
        {
            "kind": "not_null",
            "check_name": "user_email_not_null",
            "column": "email",
        },
        {
            "kind": "uniqueness",
            "check_name": "user_id_unique",
            "columns": ["id"],
        },
    ]
    checks_file = tmp_path / "builtin_checks.json"
    checks_file.write_text(json.dumps({"checks": specs}), encoding="utf-8")

    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", "builtin_checks")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_CHECKS_JSON", str(checks_file))
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_POLICY", "best_effort")

    adapter = build_quality_hook(str(tmp_path))
    assert isinstance(adapter._backend, BuiltinQualityHook)  # type: ignore[attr-defined]
    assert len(adapter._backend.checks) == 2  # type: ignore[attr-defined]

    # Run actual evaluation end-to-end via env factory to ensure full wiring works
    run_context = new_run_context(stage=StageName.sql, job_name="sql-users")
    summary = adapter.evaluate(
        run_context=run_context,
        environment="dev",
        request=QualityHookRequest(
            run_id=run_context.run_id,
            stage="sql",
            job_name=run_context.job_name,
            environment="dev",
            datasets=[
                QualityDatasetRef(
                    dataset_id="l2.users",
                    dataset_name="users",
                    materialization_type="table",
                    target_name="users",
                    row_count=3,
                    records=[
                        {"id": 1, "email": "ok@site.com"},
                        {"id": 2, "email": None},
                        {"id": 2, "email": "also-duplicate@site.com"},
                    ],
                )
            ],
            metrics={},
        ),
    )
    assert summary is not None
    assert summary.backend_type == BUILTIN_CHECKS_BACKEND_TYPE
    statuses = {r.check_name: r.status for r in summary.results}
    assert statuses["user_email_not_null"] == QualityCheckStatus.fail
    assert statuses["user_id_unique"] == QualityCheckStatus.fail


def test_builtin_check_discriminator_rejects_unknown_specs() -> None:
    with pytest.raises(ValidationError):
        BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
            {"kind": "does_not_exist", "check_name": "bad", "column": "x"}
        )


def test_builtin_hook_carries_kind_through_check_details() -> None:
    records = [{"id": 1, "amount": 12.5}]
    backend = BuiltinQualityHook(
        checks=[
            BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(
                RangeCheck(
                    check_name="amount_in_band",
                    column="amount",
                    min_value=0.0,
                    max_value=100.0,
                ).model_dump()
            )
        ]
    )
    results = backend.evaluate(
        request=QualityHookRequest(
            run_id="r1",
            stage="sql",
            job_name="j1",
            environment="dev",
            datasets=[
                QualityDatasetRef(
                    dataset_id="l2.x",
                    dataset_name="x",
                    materialization_type="table",
                    target_name="x",
                    row_count=1,
                    records=records,
                )
            ],
            metrics={},
        )
    )
    assert len(results) == 1
    assert results[0].check_details.get("kind") == "range"
    assert results[0].status == QualityCheckStatus.pass_


def test_artifact_store_quarantine_write_uses_scheme_aware_pathutils(tmp_path: Path) -> None:
    """Confirms quarantine writer plugs LocalArtifactStore → B-6 StorageBackend
    (scheme-aware path utilities: mkdir + append_jsonl → safe for local/S3/GCS/ADLS).
    """
    from elt_pipeline.shared.runtime import StageName as _S
    from elt_pipeline.shared.runtime import new_run_context as _N

    store = LocalArtifactStore(str(tmp_path))
    rc = _N(stage=_S.sql, job_name="j")
    rows = [{"k": 1, "bad": True}, {"k": 2, "bad": True}]
    wrote = store.append_quarantine_records(
        run_context=rc,
        environment="test",
        stage="sql",
        check_name="chk_x",
        dataset_id="l2.x",
        dataset_name=None,
        records=rows,
        extra_metadata={"backend": "unittest"},
    )
    assert isinstance(wrote, str)
    entries = _read_jsonl(Path(wrote))
    assert len(entries) == 2
    assert entries[0]["quarantine"]["extra"]["backend"] == "unittest"
    assert entries[0]["record"]["k"] == 1
    assert entries[1]["quarantine_row_index"] == 1


def test_builtin_checks_yaml_env_and_json_ambiguity_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Confirms YAML loader works and ambiguous JSON+YAML env raises."""
    yaml_specs = """
checks:
  - kind: not_null
    check_name: name_not_null
    column: name
  - kind: range
    check_name: score_range
    column: score
    min_value: 0.0
    max_value: 100.0
    """
    yaml_file = tmp_path / "dq.yaml"
    yaml_file.write_text(yaml_specs, encoding="utf-8")
    json_file = tmp_path / "dq.json"
    json_file.write_text(
        json.dumps(
            {"checks": [{"kind": "not_null", "check_name": "x", "column": "c"}]}
        ),
        encoding="utf-8",
    )

    # (a) YAML-only case → cleanly loads + evaluates
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", "builtin_checks")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_CHECKS_YAML", str(yaml_file))
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_POLICY", "best_effort")

    adapter = build_quality_hook(str(tmp_path))
    assert isinstance(adapter._backend, BuiltinQualityHook)  # type: ignore[attr-defined]
    assert len(adapter._backend.checks) == 2  # type: ignore[attr-defined]

    # (b) JSON+YAML both set → ConfigValidationError on ambiguity
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_CHECKS_JSON", str(json_file))
    with pytest.raises(ConfigValidationError) as exc_info:
        build_quality_hook(str(tmp_path))
    assert "ambiguous" in str(exc_info.value.message).lower() or (
        "json" in str(exc_info.value.message).lower()
        and "yaml" in str(exc_info.value.message).lower()
    )



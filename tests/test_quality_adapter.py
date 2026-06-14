from __future__ import annotations

import json
from pathlib import Path

import pytest

from elt_pipeline.integrations import (
    QualityCheckResult,
    QualityCheckStatus,
    QualityHookPolicy,
    QualityHookRequest,
    build_quality_hook,
)
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.shared.runtime import StageName, new_run_context


class _FailingQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]:
        raise RuntimeError("quality backend unavailable")


def test_build_quality_hook_uses_env_configured_row_count_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_BACKEND", "row_count_threshold")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_ROW_COUNT_MIN", "2")
    monkeypatch.setenv("ELT_PIPELINE_QUALITY_POLICY", "blocking")

    adapter = build_quality_hook(tmp_path)
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


def test_quality_hook_records_non_blocking_backend_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    adapter = build_quality_hook(
        tmp_path,
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
        / "environment=dev"
        / "job=normalize-orders"
        / f"run_id={run_context.run_id}"
    )
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
        build_quality_hook(tmp_path)

    assert exc_info.value.context["row_count_min"] == "abc"


def test_quality_hook_raises_for_blocking_backend_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.sql, job_name="sql-run")
    adapter = build_quality_hook(
        tmp_path,
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

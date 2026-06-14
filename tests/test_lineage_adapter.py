from __future__ import annotations

import json
from pathlib import Path

import pytest

from elt_pipeline.integrations import (
    LineageEmissionPolicy,
    LineageRemoteEmitter,
    build_lineage_adapter,
)
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.lineage import LineageEvent
from elt_pipeline.shared.runtime import StageName, new_run_context


class _FailingRemoteEmitter(LineageRemoteEmitter):
    backend_type = "test_backend"

    def emit(
        self,
        *,
        run_context,
        environment: str,
        lineage_event: LineageEvent,
    ) -> None:
        raise RuntimeError("backend unavailable")


def test_lineage_adapter_writes_local_lineage_artifact(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(tmp_path)

    lineage_path = adapter.emit(
        run_context=run_context,
        environment="default",
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
        ),
    )

    assert lineage_path.exists()
    events = _read_jsonl(lineage_path)
    assert events[0]["event_type"] == "START"
    assert events[0]["run_id"] == run_context.run_id


def test_lineage_adapter_records_non_blocking_remote_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(
        tmp_path,
        remote_emitter=_FailingRemoteEmitter(),
        emission_policy=LineageEmissionPolicy.best_effort,
    )

    lineage_path = adapter.emit(
        run_context=run_context,
        environment="default",
        lineage_event=LineageEvent(
            event_type="COMPLETE",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
        ),
    )

    run_dir = lineage_path.parent
    assert lineage_path.exists()
    assert _read_jsonl(run_dir / "lineage.jsonl")[0]["event_type"] == "COMPLETE"
    assert _read_jsonl(run_dir / "errors.jsonl")[0]["error_code"] == (
        "LINEAGE_BACKEND_EMISSION_FAILED"
    )
    assert _read_jsonl(run_dir / "logs.jsonl")[0]["event_type"] == "lineage_remote_emit_failed"


def test_lineage_adapter_raises_for_blocking_remote_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(
        tmp_path,
        remote_emitter=_FailingRemoteEmitter(),
        emission_policy=LineageEmissionPolicy.blocking,
    )

    with pytest.raises(PipelineError) as exc_info:
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )

    run_dir = tmp_path / "runs" / "stage=shared" / "environment=default" / "job=lineage-test"
    run_dir = run_dir / f"run_id={run_context.run_id}"
    assert exc_info.value.error_code == "LINEAGE_BACKEND_EMISSION_FAILED"
    assert (run_dir / "lineage.jsonl").exists()
    assert _read_jsonl(run_dir / "errors.jsonl")[0]["error_code"] == (
        "LINEAGE_BACKEND_EMISSION_FAILED"
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

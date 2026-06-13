from datetime import UTC, datetime

import pytest

from elt_pipeline.shared.audit import AuditRecord
from elt_pipeline.shared.errors import ErrorCategory, build_error_record
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import (
    ExecutionWindow,
    StageName,
    build_job_runtime,
    new_run_context,
)


def test_new_run_context_generates_core_fields() -> None:
    context = new_run_context(stage=StageName.ingest, job_name="demo", trigger_type="manual")
    assert context.run_id
    assert context.stage is StageName.ingest
    assert context.job_name == "demo"


def test_shared_models_can_correlate_on_run_id() -> None:
    context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    audit_record = AuditRecord(
        run_id=context.run_id,
        stage=context.stage.value,
        job_name=context.job_name,
        trigger_type=context.trigger_type,
        started_at=context.started_at,
        status="running",
    )
    log_event = build_log_event(
        run_context=context,
        severity="INFO",
        component="normalize.runner",
        event_type="run_started",
        message="Normalization run started",
    )
    error_record = build_error_record(
        run_id=context.run_id,
        error_code="PROCESSING_ERROR",
        error_category=ErrorCategory.processing_error,
        message="Example failure",
        retryable=True,
    )
    lineage_event = LineageEvent(
        event_type="COMPLETE",
        run_id=context.run_id,
        job_name=context.job_name,
        inputs=[DatasetRef(namespace="local", name="level1/orders")],
        outputs=[DatasetRef(namespace="local", name="level2/orders")],
    )

    assert audit_record.run_id == context.run_id
    assert log_event.run_id == context.run_id
    assert error_record.run_id == context.run_id
    assert lineage_event.run_id == context.run_id


def test_execution_window_derives_label_for_bounded_range() -> None:
    window = ExecutionWindow(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 3, tzinfo=UTC),
    )

    assert window.label == "2026-01-01_to_2026-01-03"


def test_build_job_runtime_resolves_backfill_trigger_and_checkpoint_seed() -> None:
    runtime = build_job_runtime(
        stage=StageName.ingest,
        job_name="ingest-orders",
        environment="default",
        source_name="orders_api",
        entity_name="orders",
        trigger_type="manual",
        window=ExecutionWindow(start=datetime(2026, 1, 5, tzinfo=UTC)),
        backfill=True,
        checkpoint_seed={"cursor": "abc"},
        attributes={"connector_type": "rest"},
    )

    context = runtime.to_run_context()

    assert runtime.trigger_type.value == "backfill"
    assert runtime.checkpoint_seed() == {"cursor": "abc"}
    assert context.attributes["checkpoint_mode"] == "backfill"
    assert context.attributes["checkpoint_seed"] == {"cursor": "abc"}
    assert context.attributes["window_label"] == "2026-01-05_to_open"


def test_build_job_runtime_requires_window_start_for_backfill() -> None:
    with pytest.raises(ValueError, match="backfill requires a window start"):
        build_job_runtime(
            stage=StageName.normalize,
            job_name="normalize-orders",
            environment="default",
            trigger_type="manual",
            backfill=True,
        )

from elt_pipeline.shared.audit import AuditRecord
from elt_pipeline.shared.errors import ErrorCategory, build_error_record
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import StageName, new_run_context


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

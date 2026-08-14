import json
from datetime import UTC, datetime
from pathlib import Path

from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalArtifactStore, LocalLevel1Writer
from elt_pipeline.shared.audit import AuditRecord
from elt_pipeline.shared.errors import ErrorCategory, build_error_record
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_local_level1_writer_persists_payload_and_manifest(tmp_path: Path) -> None:
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="orders-ingest",
        trigger_type="scheduled_batch",
    )
    writer = LocalLevel1Writer(str(tmp_path))

    manifest = writer.write_payload(
        run_context=run_context,
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        payload='{"items":[1,2,3]}',
        payload_format="json",
        extraction_mode="scheduled_batch",
        artifact_name="orders-batch",
        checkpoint_before={"cursor": "2026-01-01"},
        checkpoint_after={"cursor": "2026-01-02"},
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
        window_label="2026-01-01_to_2026-01-02",
        record_count_estimate=3,
        metadata={"endpoint": "/orders"},
    )

    data_path = Path(tmp_path) / manifest.data_path
    manifest_path = Path(tmp_path) / manifest.manifest_path

    assert data_path.exists()
    assert manifest_path.exists()
    assert data_path.read_text(encoding="utf-8") == '{"items":[1,2,3]}'
    assert "level1/source=rest_source/entity=orders" in manifest.data_path
    assert "environment=" not in manifest.data_path
    assert f"run_id={run_context.run_id}" in manifest.data_path

    stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert stored_manifest["run_id"] == run_context.run_id
    assert stored_manifest["payload_format"] == "json"
    assert stored_manifest["record_count_estimate"] == 3


def test_local_artifact_store_persists_run_artifacts(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.ingest, job_name="orders-ingest")
    store = LocalArtifactStore(str(tmp_path))

    audit_record = AuditRecord(
        run_id=run_context.run_id,
        stage=run_context.stage.value,
        job_name=run_context.job_name,
        trigger_type=run_context.trigger_type,
        started_at=run_context.started_at,
        completed_at=run_context.started_at,
        status="success",
        config_version="v1",
    )
    log_event = build_log_event(
        run_context=run_context,
        severity="INFO",
        component="ingest.runner",
        event_type="run_started",
        message="Run started",
    )
    error_record = build_error_record(
        run_id=run_context.run_id,
        error_code="SOURCE_TIMEOUT",
        error_category=ErrorCategory.processing_error,
        message="Request timed out",
        retryable=True,
    )
    lineage_event = LineageEvent(
        event_type="COMPLETE",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
        inputs=[DatasetRef(namespace="api", name="rest_source/orders")],
        outputs=[DatasetRef(namespace="local", name="level1/orders")],
    )

    audit_path = store.write_audit_record(
        run_context=run_context,
        environment="dev",
        audit_record=audit_record,
    )
    log_path = store.append_log_event(
        run_context=run_context,
        environment="dev",
        log_event=log_event,
    )
    error_path = store.append_error_record(
        run_context=run_context,
        environment="dev",
        error_record=error_record,
    )
    lineage_path = store.append_lineage_event(
        run_context=run_context,
        environment="dev",
        lineage_event=lineage_event,
    )

    assert Path(audit_path).exists()
    assert Path(log_path).exists()
    assert Path(error_path).exists()
    assert Path(lineage_path).exists()
    assert "runs/stage=ingest/job=orders-ingest" in audit_path
    assert "environment=" not in audit_path

    stored_audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    stored_log = [
        json.loads(line)
        for line in Path(log_path).read_text(encoding="utf-8").splitlines()
    ]
    stored_error = [
        json.loads(line) for line in Path(error_path).read_text(encoding="utf-8").splitlines()
    ]
    stored_lineage = [
        json.loads(line) for line in Path(lineage_path).read_text(encoding="utf-8").splitlines()
    ]

    assert stored_audit["status"] == "success"
    assert stored_log[0]["event_type"] == "run_started"
    assert stored_error[0]["error_code"] == "SOURCE_TIMEOUT"
    assert stored_lineage[0]["event_type"] == "COMPLETE"


def test_local_checkpoint_store_tracks_history_and_supports_replay_queries(
    tmp_path: Path,
) -> None:
    writer = LocalLevel1Writer(str(tmp_path))
    store = LocalCheckpointStore(str(tmp_path))

    first_run = new_run_context(stage=StageName.ingest, job_name="orders-ingest")
    first_manifest = writer.write_payload(
        run_context=first_run,
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        payload='{"items":[1]}',
        payload_format="json",
        extraction_mode="scheduled_batch",
        artifact_name="orders-day-1",
        checkpoint_after={"cursor": "2026-01-02"},
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
    store.commit(
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        run_id=first_run.run_id,
        checkpoint_before=None,
        checkpoint_after={"cursor": "2026-01-02"},
        recorded_at=first_run.started_at,
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
        manifest_paths=[first_manifest.manifest_path],
    )

    second_run = new_run_context(stage=StageName.ingest, job_name="orders-ingest")
    second_manifest = writer.write_payload(
        run_context=second_run,
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        payload='{"items":[2]}',
        payload_format="json",
        extraction_mode="scheduled_batch",
        artifact_name="orders-day-2",
        checkpoint_before={"cursor": "2026-01-02"},
        checkpoint_after={"cursor": "2026-01-03"},
        window_start=datetime(2026, 1, 2, tzinfo=UTC),
        window_end=datetime(2026, 1, 3, tzinfo=UTC),
    )
    store.commit(
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        run_id=second_run.run_id,
        checkpoint_before={"cursor": "2026-01-02"},
        checkpoint_after={"cursor": "2026-01-03"},
        recorded_at=second_run.started_at,
        window_start=datetime(2026, 1, 2, tzinfo=UTC),
        window_end=datetime(2026, 1, 3, tzinfo=UTC),
        manifest_paths=[second_manifest.manifest_path],
    )

    document = store.load(environment="dev", source_name="rest_source", entity_name="orders")
    replay_entries = store.find_replay_entries(
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 3, tzinfo=UTC),
    )
    backfill_seed = store.resolve_backfill_seed(
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
        window_start=datetime(2026, 1, 3, tzinfo=UTC),
    )

    assert document.current_checkpoint == {"cursor": "2026-01-03"}
    assert document.updated_by_run_id == second_run.run_id
    assert len(document.history) == 2
    assert [entry.run_id for entry in replay_entries] == [first_run.run_id, second_run.run_id]
    assert backfill_seed is not None
    assert backfill_seed.run_id == second_run.run_id

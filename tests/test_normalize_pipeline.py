import json
from datetime import UTC, datetime
from pathlib import Path

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.partitioning import PartitionMode, PartitionStrategy
from elt_pipeline.normalize.pipeline import normalize_level1_to_local_level2
from elt_pipeline.shared.runtime import StageName, new_run_context


def build_manifest(*, entity_name: str = "orders") -> Level1ArtifactManifest:
    return Level1ArtifactManifest(
        artifact_id="artifact-001",
        run_id="run-001",
        job_name="normalize-orders",
        trigger_type="manual",
        environment="dev",
        source_name="rest_source",
        entity_name=entity_name,
        extraction_mode="scheduled_batch",
        ingest_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ingest_completed_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_format="json",
        content_hash="abc123",
        file_size_bytes=128,
        data_path="level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/orders.json",
        manifest_path="level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/orders.json.manifest.json",
    )


def test_normalize_pipeline_writes_level2_tables_emits_lineage_and_audit(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    manifest = build_manifest()
    payload = {
        "order_id": "A-100",
        "items": [{"sku": "SKU-1", "quantity": 2}, {"sku": "SKU-2", "quantity": 1}],
    }

    summary = normalize_level1_to_local_level2(
        root_path=tmp_path,
        run_context=run_context,
        manifest=manifest,
        payload=payload,
    )

    assert summary.table_manifests
    for table_manifest in summary.table_manifests:
        data_path = tmp_path / table_manifest.data_path
        manifest_path = tmp_path / table_manifest.manifest_path
        assert data_path.exists()
        assert manifest_path.exists()
        assert "/ingest_date=2026-01-01/" in table_manifest.data_path
        written_lines = data_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(written_lines) == table_manifest.record_count

    audit_path = (
        tmp_path
        / "runs"
        / "stage=normalize"
        / "environment=dev"
        / "job=normalize-orders"
        / f"run_id={run_context.run_id}"
        / "audit.json"
    )
    assert audit_path.exists()
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit_payload["status"] == "success"
    assert audit_payload["metrics_summary"]["files_written"] == len(summary.table_manifests)

    lineage_path = audit_path.with_name("lineage.jsonl")
    assert lineage_path.exists()
    lineage_events = [
        json.loads(line) for line in lineage_path.read_text(encoding="utf-8").splitlines() if line
    ]
    assert [event["event_type"] for event in lineage_events] == ["START", "COMPLETE"]
    assert lineage_events[0]["inputs"][0]["name"] == manifest.data_path
    assert lineage_events[1]["outputs"]


def test_partition_strategy_supports_none_mode(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.normalize, job_name="normalize-orders")
    manifest = build_manifest()
    payload = {"order_id": "A-100"}

    summary = normalize_level1_to_local_level2(
        root_path=tmp_path,
        run_context=run_context,
        manifest=manifest,
        payload=payload,
        partition_strategy=PartitionStrategy(mode=PartitionMode.none),
    )

    assert summary.table_manifests
    assert all("/ingest_date=" not in table.data_path for table in summary.table_manifests)

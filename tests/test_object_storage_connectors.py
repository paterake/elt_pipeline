from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    LocalObjectStorageConnector,
    ObjectStorageConnectorBase,
    ObjectStorageConnectorConfig,
    ObjectStorageObject,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_object_storage_connector_config_builds_from_resolved_entity_config() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="file_drop",
        entity_name="daily_exports",
        connector_type="object_storage",
        trigger_mode="scheduled_batch",
        extraction={
            "bucket_path": "/tmp/object-store",
            "prefix": "exports/daily/",
            "recursive": True,
            "sync_mode": "delta",
            "max_objects": 25,
            "payload_format": "binary",
        },
    )

    connector_config = ObjectStorageConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.bucket_path == "/tmp/object-store"
    assert connector_config.prefix == "exports/daily/"
    assert connector_config.recursive is True
    assert connector_config.sync_mode.value == "delta"
    assert connector_config.max_objects == 25
    assert connector_config.execution_mode == "scheduled_batch"


def test_object_storage_connector_config_rejects_non_object_storage_connector() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_api",
        entity_name="orders",
        connector_type="rest",
    )

    with pytest.raises(ConfigValidationError, match="not an object storage connector"):
        ObjectStorageConnectorConfig.from_resolved_entity_config(resolved_config)


def test_object_storage_connector_base_persists_before_checkpoint_update(tmp_path: Path) -> None:
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="object-storage-ingest",
        trigger_type="scheduled_batch",
    )
    connector = FakeObjectStorageConnector(tmp_path=tmp_path, run_context=run_context)

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="file_drop",
        entity_name="daily_exports",
    )

    assert connector.call_order == [
        "resolve_checkpoint_before",
        "list_objects",
        "read_object",
        "persist_object",
        "update_checkpoint",
    ]
    assert result.objects_discovered == 1
    assert result.objects_copied == 1
    assert len(result.manifests) == 1
    assert result.checkpoint_before == {"last_synced_at": "2026-01-01T00:00:00+00:00"}
    assert "last_synced_at" in (result.checkpoint_after or {})
    assert checkpoint_document.history[0].manifest_paths == [result.manifests[0].manifest_path]


def test_local_object_storage_connector_delta_run_copies_files_and_updates_checkpoint(
    tmp_path: Path,
) -> None:
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    (bucket / "a.txt").write_text("alpha", encoding="utf-8")
    nested = bucket / "nested"
    nested.mkdir()
    (nested / "b.json").write_text('{"beta": 2}', encoding="utf-8")
    _set_mtime(bucket / "a.txt", datetime(2026, 1, 1, 9, 0, tzinfo=UTC))
    _set_mtime(nested / "b.json", datetime(2026, 1, 1, 10, 0, tzinfo=UTC))

    connector = LocalObjectStorageConnector(
        config=ObjectStorageConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="file_drop",
            entity_name="daily_exports",
            execution_mode="scheduled_batch",
            bucket_path=str(bucket),
            recursive=True,
            sync_mode="delta",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="object-storage-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=str(tmp_path),
    )

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="file_drop",
        entity_name="daily_exports",
    )

    assert result.objects_discovered == 2
    assert result.objects_copied == 2
    assert len(result.manifests) == 2
    assert result.checkpoint_before is None
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["last_synced_at"] == "2026-01-01T10:00:00+00:00"
    assert checkpoint_document.current_checkpoint["last_synced_at"] == "2026-01-01T10:00:00+00:00"

    manifest_paths = [tmp_path / Path(manifest.manifest_path) for manifest in result.manifests]
    data_paths = [tmp_path / Path(manifest.data_path) for manifest in result.manifests]
    assert all(path.exists() for path in manifest_paths)
    assert all(path.exists() for path in data_paths)

    payloads = {path.read_bytes() for path in data_paths}
    assert payloads == {b"alpha", b'{"beta": 2}'}


def test_local_object_storage_connector_uses_saved_checkpoint_on_next_delta_run(
    tmp_path: Path,
) -> None:
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    (bucket / "a.txt").write_text("alpha", encoding="utf-8")
    _set_mtime(bucket / "a.txt", datetime(2026, 1, 1, 9, 0, tzinfo=UTC))

    checkpoint_store = LocalCheckpointStore(str(tmp_path))
    checkpoint_store.commit(
        environment="dev",
        source_name="file_drop",
        entity_name="daily_exports",
        run_id="prior-run",
        checkpoint_before=None,
        checkpoint_after={"last_synced_at": "2026-01-01T09:00:00+00:00", "seen": {"a.txt": {}}},
        recorded_at=new_run_context(
            stage=StageName.ingest,
            job_name="prior-object-storage-ingest",
            trigger_type="scheduled_batch",
        ).started_at,
    )

    (bucket / "b.txt").write_text("bravo", encoding="utf-8")
    _set_mtime(bucket / "b.txt", datetime(2026, 1, 1, 11, 0, tzinfo=UTC))

    connector = LocalObjectStorageConnector(
        config=ObjectStorageConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="file_drop",
            entity_name="daily_exports",
            execution_mode="scheduled_batch",
            bucket_path=str(bucket),
            recursive=True,
            sync_mode="delta",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="object-storage-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=str(tmp_path),
    )

    result = connector.run()

    assert result.checkpoint_before == {
        "last_synced_at": "2026-01-01T09:00:00+00:00",
        "seen": {"a.txt": {}},
    }
    assert result.objects_copied == 1
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["last_synced_at"] == "2026-01-01T11:00:00+00:00"


class FakeObjectStorageConnector(ObjectStorageConnectorBase):
    def __init__(self, *, tmp_path: Path, run_context) -> None:
        self.call_order: list[str] = []
        self.writer = LocalLevel1Writer(str(tmp_path))
        self.checkpoint_store = LocalCheckpointStore(str(tmp_path))
        super().__init__(
            config=ObjectStorageConnectorConfig(
                schema_version="v1",
                environment="dev",
                source_name="file_drop",
                entity_name="daily_exports",
                execution_mode="scheduled_batch",
                bucket_path=str(tmp_path / "bucket"),
                sync_mode="delta",
            ),
            run_context=run_context,
        )

    def validate_config(self) -> ObjectStorageConnectorConfig:
        (Path(self.config.bucket_path)).mkdir(parents=True, exist_ok=True)
        return super().validate_config()

    def resolve_checkpoint_before(self) -> dict[str, object]:
        self.call_order.append("resolve_checkpoint_before")
        return {"last_synced_at": "2026-01-01T00:00:00+00:00"}

    def list_objects(
        self,
        *,
        checkpoint_before: dict[str, object] | None,
    ) -> list[ObjectStorageObject]:
        del checkpoint_before
        self.call_order.append("list_objects")
        return [
            ObjectStorageObject(
                key="exports/a.txt",
                size_bytes=5,
                last_modified=datetime(2026, 1, 2, tzinfo=UTC),
            )
        ]

    def read_object(self, obj: ObjectStorageObject) -> bytes:
        self.call_order.append("read_object")
        assert obj.key == "exports/a.txt"
        return b"alpha"

    def persist_object(
        self,
        *,
        obj: ObjectStorageObject,
        payload: bytes,
        checkpoint_before: dict[str, object] | None,
    ) -> Level1ArtifactManifest:
        self.call_order.append("persist_object")
        manifest = self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=payload,
            payload_format="txt",
            extraction_mode=self.config.execution_mode,
            artifact_name="exports-a",
            checkpoint_before=checkpoint_before,
            metadata={"object_key": obj.key},
            ingest_completed_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        assert (Path(self.writer.layout.root_path) / Path(manifest.data_path)).exists()
        return manifest

    def update_checkpoint(
        self,
        *,
        checkpoint_before,
        checkpoint_after,
        manifests,
    ) -> None:
        self.call_order.append("update_checkpoint")
        assert manifests
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
        )


def _set_mtime(path: Path, value: datetime) -> None:
    timestamp = value.timestamp()
    os.utime(path, (timestamp, timestamp))

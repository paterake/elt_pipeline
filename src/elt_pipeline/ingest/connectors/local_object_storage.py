from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from elt_pipeline.ingest.connectors.object_storage import (
    ObjectStorageConnectorBase,
    ObjectStorageConnectorConfig,
    ObjectStorageObject,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError
from elt_pipeline.shared.runtime import RunContext

_SAFE_ARTIFACT_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_artifact_fragment(value: Any) -> str:
    cleaned = _SAFE_ARTIFACT_FRAGMENT.sub("_", str(value).strip())
    return cleaned or "unknown"


class LocalObjectStorageConnector(ObjectStorageConnectorBase):
    def __init__(
        self,
        *,
        config: ObjectStorageConnectorConfig,
        run_context: RunContext,
        root_path: Path,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)

    def validate_config(self) -> ObjectStorageConnectorConfig:
        config = super().validate_config()
        bucket_path = Path(config.bucket_path)
        if not bucket_path.exists():
            raise ConfigValidationError(
                message="object_storage bucket_path does not exist",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                    "bucket_path": config.bucket_path,
                },
            )
        if not bucket_path.is_dir():
            raise ConfigValidationError(
                message="object_storage bucket_path must be a directory for local mode",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                    "bucket_path": config.bucket_path,
                },
            )
        return config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        checkpoint_document = self.checkpoint_store.load(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return checkpoint_document.current_checkpoint

    def list_objects(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
    ) -> list[ObjectStorageObject]:
        del checkpoint_before

        bucket_path = Path(self.config.bucket_path)
        base_dir = bucket_path
        search_dir = base_dir
        if self.config.prefix:
            search_dir = base_dir / self.config.prefix
            if not search_dir.exists():
                return []

        candidates = (
            search_dir.rglob("*") if self.config.recursive else search_dir.glob("*")
        )
        objects: list[ObjectStorageObject] = []
        for candidate in candidates:
            if not candidate.is_file():
                continue
            stat = candidate.stat()
            key = candidate.relative_to(base_dir).as_posix()
            objects.append(
                ObjectStorageObject(
                    key=key,
                    size_bytes=stat.st_size,
                    last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                    metadata={"source_path": str(candidate)},
                )
            )

        objects.sort(key=lambda obj: obj.key)
        return objects

    def read_object(self, obj: ObjectStorageObject) -> bytes:
        path = Path(self.config.bucket_path) / obj.key
        try:
            return path.read_bytes()
        except OSError as exc:
            raise PipelineError(
                message=f"Failed to read object {obj.key}: {exc}",
                error_code="OBJECT_STORAGE_READ_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "bucket_path": self.config.bucket_path,
                    "key": obj.key,
                    "error_type": type(exc).__name__,
                },
            ) from exc

    def persist_object(
        self,
        *,
        obj: ObjectStorageObject,
        payload: bytes,
        checkpoint_before: dict[str, Any] | None,
    ) -> Level1ArtifactManifest:
        suffix = Path(obj.key).suffix.lstrip(".")
        file_extension = suffix or None
        artifact_name = self._artifact_name_for_key(obj.key)
        return self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=payload,
            payload_format=self.config.payload_format,
            extraction_mode=self.config.execution_mode,
            artifact_name=artifact_name,
            file_extension=file_extension,
            checkpoint_before=checkpoint_before,
            metadata={
                "object_key": obj.key,
                "bucket_path": self.config.bucket_path,
                "object_size_bytes": obj.size_bytes,
                "object_last_modified": obj.last_modified.isoformat()
                if obj.last_modified is not None
                else None,
            },
            ingest_completed_at=datetime.now(tz=UTC),
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "object_storage"},
        )
        return None

    def _artifact_name_for_key(self, key: str) -> str:
        normalized = _safe_artifact_fragment(key.replace(os.sep, "/").replace("/", "_"))
        if len(normalized) <= 120:
            return normalized
        digest = abs(hash(key))
        prefix = normalized[:80]
        return f"{prefix}-{digest}"


__all__ = ["LocalObjectStorageConnector"]

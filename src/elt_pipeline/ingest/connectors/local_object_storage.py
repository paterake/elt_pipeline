from __future__ import annotations

import hashlib
import os
import posixpath
import re
from datetime import UTC, datetime
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
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_exists,
    path_glob,
    path_is_dir,
    path_read_bytes,
    path_relative_to,
    path_rglob,
    strip_file_scheme,
)
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
        root_path: str,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)

    def validate_config(self) -> ObjectStorageConnectorConfig:
        config = super().validate_config()
        bucket_path = config.bucket_path
        if not path_exists(bucket_path):
            raise ConfigValidationError(
                message="object_storage bucket_path does not exist",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                    "bucket_path": config.bucket_path,
                },
            )
        if not path_is_dir(bucket_path):
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

        base_dir = self.config.bucket_path
        search_dir = (
            join_paths(base_dir, self.config.prefix) if self.config.prefix else base_dir
        )
        if self.config.prefix and not path_exists(search_dir):
            return []

        candidate_paths = (
            path_rglob(search_dir, "*")
            if self.config.recursive
            else path_glob(search_dir, "*")
        )
        objects: list[ObjectStorageObject] = []
        for candidate in candidate_paths:
            if path_is_dir(candidate):
                continue
            last_modified: datetime | None = None
            size_bytes: int = 0
            scheme = detect_scheme(candidate)
            if scheme in (_StorageScheme.file, _StorageScheme.local_unschemed):
                local_path = strip_file_scheme(candidate)
                try:
                    import os

                    stat = os.stat(local_path)
                    size_bytes = stat.st_size
                    last_modified = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
                except OSError:
                    size_bytes = 0
            else:
                size_bytes = 0
            key = path_relative_to(candidate, base_dir)
            objects.append(
                ObjectStorageObject(
                    key=key,
                    size_bytes=size_bytes,
                    last_modified=last_modified,
                    metadata={"source_path": candidate},
                ),
            )

        objects.sort(key=lambda obj: obj.key)
        return objects

    def read_object(self, obj: ObjectStorageObject) -> bytes:
        path = join_paths(self.config.bucket_path, obj.key)
        try:
            return path_read_bytes(path)
        except (PipelineError, OSError) as exc:
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
        suffix = posixpath.splitext(obj.key)[1].lstrip(".")
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
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        prefix = normalized[:80]
        return f"{prefix}-{digest}"


__all__ = ["LocalObjectStorageConnector"]

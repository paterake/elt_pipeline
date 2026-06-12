from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import RunContext


class ObjectStorageSyncMode(str, Enum):
    full = "full"
    delta = "delta"


class ObjectStorageObject(BaseModel):
    key: str
    size_bytes: int | None = None
    last_modified: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("key")
    @classmethod
    def _validate_key(cls, value: str) -> str:
        normalized = value.strip().lstrip("/")
        if not normalized:
            raise ValueError("Object key must not be empty")
        return normalized


class ObjectStorageConnectorConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    execution_mode: str
    bucket_path: str
    prefix: str | None = None
    recursive: bool = True
    sync_mode: ObjectStorageSyncMode = ObjectStorageSyncMode.delta
    max_objects: int | None = Field(default=None, ge=1)
    payload_format: str = "binary"
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("bucket_path")
    @classmethod
    def _validate_bucket_path(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("bucket_path must not be empty")
        return normalized

    @field_validator("prefix")
    @classmethod
    def _normalize_prefix(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lstrip("/")
        return normalized or None

    @classmethod
    def from_resolved_entity_config(
        cls,
        resolved_config: ResolvedEntityConfig,
    ) -> ObjectStorageConnectorConfig:
        if resolved_config.connector_type != "object_storage":
            raise ConfigValidationError(
                message="Resolved entity config is not an object storage connector",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": resolved_config.connector_type,
                },
            )

        extraction = resolved_config.extraction
        bucket_path = (
            extraction.get("bucket_path")
            or extraction.get("root_path")
            or extraction.get("path")
            or extraction.get("bucket")
        )
        if not bucket_path:
            raise ConfigValidationError(
                message="object_storage extraction config must define bucket_path",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                },
            )

        payload = {
            "schema_version": resolved_config.schema_version,
            "environment": resolved_config.environment,
            "source_name": resolved_config.source_name,
            "entity_name": resolved_config.entity_name,
            "execution_mode": resolved_config.trigger_mode or "manual",
            "bucket_path": bucket_path,
            "prefix": extraction.get("prefix"),
            "recursive": extraction.get("recursive", True),
            "sync_mode": extraction.get("sync_mode", "delta"),
            "max_objects": extraction.get("max_objects"),
            "payload_format": extraction.get("payload_format", "binary"),
            "persistence": resolved_config.persistence,
            "state": resolved_config.state,
            "settings": resolved_config.settings,
            "raw": resolved_config.raw,
        }

        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            raise ConfigValidationError(
                message="object_storage connector configuration validation failed",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc


class ObjectStorageRunResult(BaseModel):
    manifests: list[Level1ArtifactManifest] = Field(default_factory=list)
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    objects_discovered: int = 0
    objects_copied: int = 0
    bytes_copied: int = 0


class ObjectStorageConnectorBase(ABC):
    def __init__(
        self,
        *,
        config: ObjectStorageConnectorConfig,
        run_context: RunContext,
    ) -> None:
        self.config = config
        self.run_context = run_context

    def validate_config(self) -> ObjectStorageConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    @abstractmethod
    def list_objects(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
    ) -> list[ObjectStorageObject]:
        raise NotImplementedError

    @abstractmethod
    def read_object(self, obj: ObjectStorageObject) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def persist_object(
        self,
        *,
        obj: ObjectStorageObject,
        payload: bytes,
        checkpoint_before: dict[str, Any] | None,
    ) -> Level1ArtifactManifest:
        raise NotImplementedError

    def should_sync_object(
        self,
        *,
        obj: ObjectStorageObject,
        checkpoint_before: dict[str, Any] | None,
    ) -> bool:
        if self.config.sync_mode == ObjectStorageSyncMode.full:
            return True

        checkpoint_before = checkpoint_before or {}
        last_synced_at_raw = checkpoint_before.get("last_synced_at")
        if last_synced_at_raw:
            try:
                last_synced_at = datetime.fromisoformat(str(last_synced_at_raw))
            except ValueError:
                last_synced_at = None
        else:
            last_synced_at = None

        seen = checkpoint_before.get("seen")
        if isinstance(seen, dict):
            entry = seen.get(obj.key)
            if isinstance(entry, dict):
                seen_size = entry.get("size_bytes")
                seen_mtime = entry.get("last_modified")
                if (
                    seen_size is not None
                    and obj.size_bytes is not None
                    and int(seen_size) != int(obj.size_bytes)
                ):
                    return True
                if seen_mtime is not None and obj.last_modified is not None:
                    try:
                        seen_dt = datetime.fromisoformat(str(seen_mtime))
                    except ValueError:
                        seen_dt = None
                    if seen_dt is not None and seen_dt != obj.last_modified:
                        return True
                if obj.last_modified is None:
                    return False
                if seen_size is not None or seen_mtime is not None:
                    return False

        if last_synced_at is None or obj.last_modified is None:
            return True
        if obj.last_modified.tzinfo is None:
            return True
        if last_synced_at.tzinfo is None:
            last_synced_at = last_synced_at.replace(tzinfo=UTC)
        return obj.last_modified > last_synced_at

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        synced_objects: list[ObjectStorageObject],
        manifests: list[Level1ArtifactManifest],
    ) -> dict[str, Any] | None:
        del manifests

        if self.config.sync_mode == ObjectStorageSyncMode.full:
            checkpoint_after: dict[str, Any] = dict(checkpoint_before or {})
        else:
            checkpoint_after = dict(checkpoint_before or {})

        seen_before = checkpoint_after.get("seen")
        seen = dict(seen_before) if isinstance(seen_before, dict) else {}

        max_modified: datetime | None = None
        for obj in synced_objects:
            entry = {}
            if obj.size_bytes is not None:
                entry["size_bytes"] = obj.size_bytes
            if obj.last_modified is not None:
                entry["last_modified"] = obj.last_modified.isoformat()
                max_modified = obj.last_modified if max_modified is None else max(
                    max_modified,
                    obj.last_modified,
                )
            if obj.metadata:
                entry["metadata"] = obj.metadata
            seen[obj.key] = entry

        if max_modified is not None:
            checkpoint_after["last_synced_at"] = max_modified.isoformat()
        elif checkpoint_before and "last_synced_at" in checkpoint_before:
            checkpoint_after["last_synced_at"] = checkpoint_before["last_synced_at"]

        checkpoint_after["seen"] = seen
        return checkpoint_after

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        del checkpoint_before
        del checkpoint_after
        del manifests
        return None

    def run(self) -> ObjectStorageRunResult:
        self.validate_config()
        checkpoint_before = self.resolve_checkpoint_before()
        discovered = self.list_objects(checkpoint_before=checkpoint_before)
        if self.config.max_objects is not None:
            discovered = list(discovered[: self.config.max_objects])

        manifests: list[Level1ArtifactManifest] = []
        synced_objects: list[ObjectStorageObject] = []
        bytes_copied = 0
        for obj in discovered:
            if not self.should_sync_object(obj=obj, checkpoint_before=checkpoint_before):
                continue
            payload = self.read_object(obj)
            bytes_copied += len(payload)
            manifests.append(
                self.persist_object(
                    obj=obj,
                    payload=payload,
                    checkpoint_before=checkpoint_before,
                )
            )
            synced_objects.append(obj)

        checkpoint_after = self.build_checkpoint_after(
            checkpoint_before=checkpoint_before,
            synced_objects=synced_objects,
            manifests=manifests,
        )
        self.update_checkpoint(
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            manifests=manifests,
        )

        return ObjectStorageRunResult(
            manifests=manifests,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            objects_discovered=len(discovered),
            objects_copied=len(synced_objects),
            bytes_copied=bytes_copied,
        )


__all__ = [
    "ObjectStorageConnectorBase",
    "ObjectStorageConnectorConfig",
    "ObjectStorageObject",
    "ObjectStorageRunResult",
    "ObjectStorageSyncMode",
]

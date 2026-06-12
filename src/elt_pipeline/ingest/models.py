from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class Level1ArtifactManifest(BaseModel):
    manifest_version: str = "v1"
    artifact_id: str
    run_id: str
    job_name: str
    trigger_type: str
    environment: str
    source_name: str
    entity_name: str
    extraction_mode: str
    ingest_started_at: datetime
    ingest_completed_at: datetime
    window_start: datetime | None = None
    window_end: datetime | None = None
    window_label: str | None = None
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    payload_format: str
    compression: str | None = None
    record_count_estimate: int | None = None
    content_hash: str
    file_size_bytes: int
    data_path: str
    manifest_path: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckpointHistoryEntry(BaseModel):
    run_id: str
    recorded_at: datetime
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any]
    window_start: datetime | None = None
    window_end: datetime | None = None
    window_label: str | None = None
    manifest_paths: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckpointStateDocument(BaseModel):
    schema_version: str = "v1"
    environment: str
    source_name: str
    entity_name: str
    current_checkpoint: dict[str, Any] | None = None
    updated_at: datetime | None = None
    updated_by_run_id: str | None = None
    history: list[CheckpointHistoryEntry] = Field(default_factory=list)

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TableColumnMapping(BaseModel):
    logical_path: str
    physical_name: str


class TableMappingEntry(BaseModel):
    logical_path: str
    physical_table_name: str
    parent_table_name: str | None = None
    join_key_columns: list[str] = Field(default_factory=list)
    column_mappings: list[TableColumnMapping] = Field(default_factory=list)


class MappingCatalog(BaseModel):
    source_name: str
    entity_name: str
    mapping_version: str
    root_table_name: str
    entries: list[TableMappingEntry] = Field(default_factory=list)


class NormalizedTable(BaseModel):
    logical_path: str
    physical_name: str
    parent_table_name: str | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)


class NormalizationResult(BaseModel):
    source_name: str
    entity_name: str
    artifact_id: str
    data_path: str
    mapping_version: str
    tables: list[NormalizedTable] = Field(default_factory=list)
    mapping_catalog: MappingCatalog


class Level2TableManifest(BaseModel):
    manifest_version: str = "v1"
    artifact_id: str
    run_id: str
    job_name: str
    trigger_type: str
    environment: str
    source_name: str
    entity_name: str
    mapping_version: str
    input_artifact_id: str
    input_data_path: str
    input_manifest_path: str
    table_name: str
    partition: dict[str, str] = Field(default_factory=dict)
    normalize_started_at: datetime
    normalize_completed_at: datetime
    record_count: int
    file_count: int
    total_file_size_bytes: int
    data_path: str
    manifest_path: str


class Level2WriteSummary(BaseModel):
    mapping_catalog_path: str
    table_manifests: list[Level2TableManifest] = Field(default_factory=list)

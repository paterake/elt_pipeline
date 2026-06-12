from __future__ import annotations

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

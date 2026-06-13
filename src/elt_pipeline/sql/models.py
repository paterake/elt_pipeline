from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator


class SqlModelStage(str, Enum):
    level3 = "level3"
    level4 = "level4"


class SqlMaterializationType(str, Enum):
    table = "table"


class SqlLoadMode(str, Enum):
    full_refresh = "full_refresh"
    append = "append"
    partition_overwrite = "partition_overwrite"


class SqlModelTarget(BaseModel):
    table_name: str
    partition_columns: list[str] = Field(default_factory=list)

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("target.table_name must not be empty")
        if not all(character.isalnum() or character == "_" for character in cleaned):
            raise ValueError("target.table_name may only contain letters, numbers, and underscores")
        if cleaned[0].isdigit():
            raise ValueError("target.table_name must not start with a digit")
        return cleaned


class SqlQualityExpectations(BaseModel):
    row_count_min: int | None = Field(default=None, ge=0)
    unique_columns: list[str] = Field(default_factory=list)
    not_null_columns: list[str] = Field(default_factory=list)


class SqlModelOwner(BaseModel):
    name: str
    email: str | None = None


class SqlModelManifest(BaseModel):
    manifest_version: str = "v1"
    name: str
    stage: SqlModelStage
    domain: str
    description: str | None = None
    materialization: SqlMaterializationType = SqlMaterializationType.table
    load_mode: SqlLoadMode = SqlLoadMode.full_refresh
    target: SqlModelTarget
    depends_on: list[str] = Field(default_factory=list)
    quality: SqlQualityExpectations = Field(default_factory=SqlQualityExpectations)
    owner: SqlModelOwner
    tags: list[str] = Field(default_factory=list)

    @field_validator("name", "domain")
    @classmethod
    def validate_required_identifier(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, values: list[str]) -> list[str]:
        seen: set[str] = set()
        cleaned_values: list[str] = []
        for value in values:
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("depends_on entries must not be empty")
            if cleaned not in seen:
                seen.add(cleaned)
                cleaned_values.append(cleaned)
        return cleaned_values

    @model_validator(mode="after")
    def validate_partition_requirements(self) -> "SqlModelManifest":
        if self.load_mode == SqlLoadMode.partition_overwrite and not self.target.partition_columns:
            raise ValueError(
                "target.partition_columns must be set when load_mode is partition_overwrite"
            )
        return self


class DiscoveredSqlModel(BaseModel):
    manifest: SqlModelManifest
    package_root: Path
    manifest_path: Path
    sql_path: Path
    sql_text: str

    @property
    def model_id(self) -> str:
        return f"{self.manifest.stage.value}.{self.manifest.domain}.{self.manifest.name}"


class CompiledSqlModel(BaseModel):
    model_id: str
    stage: SqlModelStage
    domain: str
    name: str
    target_table_name: str
    partition_columns: list[str] = Field(default_factory=list)
    load_mode: SqlLoadMode
    materialization: SqlMaterializationType
    manifest_path: Path
    sql_path: Path
    compiled_sql: str
    token_values: dict[str, str] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)


class SqlExecutionRecord(BaseModel):
    model_id: str
    target_table_name: str
    load_mode: SqlLoadMode
    row_count: int


class SqlExecutionResult(BaseModel):
    database_path: Path
    executed_models: list[SqlExecutionRecord] = Field(default_factory=list)

    @property
    def model_count(self) -> int:
        return len(self.executed_models)


class SqlRunArtifacts(BaseModel):
    artifact_root: Path
    run_dir: Path
    audit_path: Path | None = None
    log_path: Path | None = None
    lineage_path: Path | None = None
    error_path: Path | None = None


class SqlStageRunResult(BaseModel):
    execution_result: SqlExecutionResult
    artifacts: SqlRunArtifacts

from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator


class PublishStage(str, Enum):
    level5 = "level5"


class PublishSelectionMode(str, Enum):
    direct = "direct"
    query = "query"


class PublishOutputFormat(str, Enum):
    csv = "csv"
    jsonl = "jsonl"
    tsv = "tsv"
    zip = "zip"


class PublishArchiveFormat(str, Enum):
    zip = "zip"


class PublishPackaging(BaseModel):
    archive_format: PublishArchiveFormat | None = None


class PublishTargetType(str, Enum):
    local_filesystem = "local_filesystem"


class PublishReplacementMode(str, Enum):
    overwrite_in_place = "overwrite_in_place"
    append_new_artifact = "append_new_artifact"
    partition_replace = "partition_replace"
    versioned_delivery = "versioned_delivery"


class PublishSource(BaseModel):
    stage: str = "level4"
    dataset: str
    selection_mode: PublishSelectionMode = PublishSelectionMode.direct

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned != "level4":
            raise ValueError("source.stage must be 'level4' for the approved v1 publish contract")
        return cleaned

    @field_validator("dataset")
    @classmethod
    def validate_dataset(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("source.dataset must not be empty")
        if not all(character.isalnum() or character == "_" for character in cleaned):
            raise ValueError("source.dataset may only contain letters, numbers, and underscores")
        if cleaned[0].isdigit():
            raise ValueError("source.dataset must not start with a digit")
        return cleaned


class PublishDelivery(BaseModel):
    target_type: PublishTargetType = PublishTargetType.local_filesystem
    output_format: PublishOutputFormat
    path_template: str
    replacement_mode: PublishReplacementMode = PublishReplacementMode.versioned_delivery
    packaging: PublishPackaging | None = None

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, value: PublishOutputFormat) -> PublishOutputFormat:
        if value == PublishOutputFormat.zip:
            raise ValueError(
                "delivery.output_format must not be 'zip'; use delivery.packaging.archive_format"
            )
        return value

    @field_validator("path_template")
    @classmethod
    def validate_path_template(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("delivery.path_template must not be empty")
        template_path = Path(cleaned)
        if template_path.is_absolute():
            raise ValueError("delivery.path_template must be relative")
        if ".." in template_path.parts:
            raise ValueError("delivery.path_template must not escape the level5 artifact root")
        return cleaned


class PublishOwner(BaseModel):
    owning_domain: str
    owner_team: str

    @field_validator("owning_domain", "owner_team")
    @classmethod
    def validate_required_identifier(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned


class PublishValidationRules(BaseModel):
    require_non_empty: bool = True
    required_columns: list[str] = Field(default_factory=list)

    @field_validator("required_columns")
    @classmethod
    def validate_required_columns(cls, values: list[str]) -> list[str]:
        cleaned_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("validation.required_columns entries must not be empty")
            if cleaned not in seen:
                seen.add(cleaned)
                cleaned_values.append(cleaned)
        return cleaned_values


class PublishManifest(BaseModel):
    manifest_version: str = "v1"
    name: str
    stage: PublishStage = PublishStage.level5
    domain: str
    version: str = "v1"
    description: str | None = None
    source: PublishSource
    delivery: PublishDelivery
    owner: PublishOwner
    consumer_label: str
    delivery_purpose: str
    columns: list[str] = Field(default_factory=list)
    validation: PublishValidationRules = Field(default_factory=PublishValidationRules)
    tags: list[str] = Field(default_factory=list)

    @field_validator("name", "domain", "version", "consumer_label", "delivery_purpose")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, values: list[str]) -> list[str]:
        cleaned_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = value.strip()
            if not cleaned:
                raise ValueError("columns entries must not be empty")
            if cleaned not in seen:
                seen.add(cleaned)
                cleaned_values.append(cleaned)
        return cleaned_values

    @model_validator(mode="after")
    def validate_column_expectations(self) -> "PublishManifest":
        if self.validation.required_columns:
            missing_columns = [
                column
                for column in self.validation.required_columns
                if self.columns and column not in self.columns
            ]
            if missing_columns:
                raise ValueError(
                    "validation.required_columns must be a subset of columns "
                    "when columns are declared"
                )
        return self


class DiscoveredPublishDefinition(BaseModel):
    manifest: PublishManifest
    package_root: Path
    manifest_path: Path
    query_path: Path | None = None
    query_text: str | None = None

    @property
    def publish_id(self) -> str:
        return f"{self.manifest.domain}.{self.manifest.name}"


class PublishValidationResult(BaseModel):
    validation_type: str
    passed: bool
    message: str | None = None


class PublishArtifactRecord(BaseModel):
    output_format: PublishOutputFormat
    run_scoped_path: Path
    stable_delivery_path: Path | None = None
    file_size_bytes: int
    row_count: int
    checksum_sha256: str


class PublishOutputManifest(BaseModel):
    run_id: str
    rerun_of_run_id: str | None = None
    publish_name: str
    publish_version: str
    source_stage: str
    source_dataset: str
    execution_window: dict[str, str | None]
    output_format: PublishOutputFormat
    replacement_mode: PublishReplacementMode
    produced_at: str
    owning_domain: str
    owner_team: str
    consumer_label: str
    delivery_purpose: str
    validation_results: list[PublishValidationResult] = Field(default_factory=list)
    artifacts: list[PublishArtifactRecord] = Field(default_factory=list)


class PublishRunArtifacts(BaseModel):
    artifact_root: Path
    run_dir: Path
    export_manifest_path: Path | None = None
    audit_path: Path | None = None
    log_path: Path | None = None
    lineage_path: Path | None = None
    error_path: Path | None = None


class PublishRunResult(BaseModel):
    publish_id: str
    row_count: int
    artifacts: list[PublishArtifactRecord] = Field(default_factory=list)
    validations: list[PublishValidationResult] = Field(default_factory=list)


class PublishStageRunResult(BaseModel):
    results: list[PublishRunResult] = Field(default_factory=list)
    artifacts: PublishRunArtifacts

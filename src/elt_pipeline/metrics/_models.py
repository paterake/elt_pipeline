from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from elt_pipeline.shared.governance import DataClassification


class MetricAggregation(str, Enum):
    sum = "sum"
    count_distinct = "count_distinct"
    average = "average"
    cumulative_rolling = "cumulative_rolling"
    min = "min"
    max = "max"


class MetricDimensionSpec(BaseModel):
    name: str
    description: str | None = None
    is_time_dimension: bool = False

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("dimension name must not be empty")
        return cleaned


class MetricFilterSpec(BaseModel):
    predicate: str
    description: str | None = None

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("filter predicate must not be empty")
        return cleaned


class MetricOwner(BaseModel):
    name: str
    email: str | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return value.strip()


_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class MetricManifest(BaseModel):
    manifest_version: str = "v1"
    name: str
    domain: str
    description: str | None = None
    query_ref: str
    aggregation: MetricAggregation
    dimensions: list[MetricDimensionSpec] = Field(default_factory=list)
    filters: list[MetricFilterSpec] = Field(default_factory=list)
    required_role: DataClassification | None = None
    owners: list[MetricOwner] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name must not be empty")
        if not _NAME_PATTERN.match(cleaned):
            raise ValueError(
                "name may only contain letters, numbers, and underscores, "
                "and must not start with a digit"
            )
        return cleaned

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("domain must not be empty")
        return cleaned

    @field_validator("query_ref")
    @classmethod
    def validate_query_ref(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("query_ref must not be empty")
        return cleaned

    @field_validator("dimensions")
    @classmethod
    def validate_dimensions(cls, values: list[MetricDimensionSpec]) -> list[MetricDimensionSpec]:
        seen: set[str] = set()
        for spec in values:
            if spec.name in seen:
                raise ValueError(f"duplicate dimension name '{spec.name}'")
            seen.add(spec.name)
        return values

    @model_validator(mode="after")
    def validate_query_ref_format(self) -> "MetricManifest":
        parts = self.query_ref.split(".")
        if len(parts) != 4:
            raise ValueError(
                f"query_ref must have format 'stage.domain.model.column' "
                f"with 4 dot-separated parts, got {len(parts)}: {self.query_ref!r}"
            )
        if self.aggregation == MetricAggregation.cumulative_rolling:
            if not any(d.is_time_dimension for d in self.dimensions):
                raise ValueError(
                    "cumulative_rolling aggregation requires at least one "
                    "dimension with is_time_dimension=True"
                )
        return self


class DiscoveredMetric(BaseModel):
    manifest: MetricManifest
    package_root: Path
    manifest_path: Path

    @property
    def metric_id(self) -> str:
        return f"{self.manifest.domain}.{self.manifest.name}"


class CompiledMetric(BaseModel):
    metric_id: str
    domain: str
    name: str
    query_ref_model_id: str
    query_ref_column: str
    aggregation: MetricAggregation
    dimensions: list[MetricDimensionSpec]
    filters: list[MetricFilterSpec]
    required_role: DataClassification | None
    generated_sql_hash: str
    manifest_path: Path


class MetricAuditRecord(BaseModel):
    metric_id: str
    mode: Literal["materialize", "view", "prometheus"]
    started_at_iso: str
    finished_at_iso: str
    total_sum: float | None = None
    non_null_count: int | None = None
    min_value: float | None = None
    max_value: float | None = None
    generated_sql_hash: str
    output_location: str | None = None
    success: bool
    error_message: str | None = None

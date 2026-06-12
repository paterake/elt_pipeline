from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors.rest import (
    _render_string_template,
    _render_template_value,
    _serialize_template_scalar,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import RunContext


class SqlConnectionDriver(str, Enum):
    sqlite = "sqlite"


class SqlExtractionMode(str, Enum):
    snapshot = "snapshot"
    delta = "delta"


class SqlWatermarkSource(str, Enum):
    checkpoint = "checkpoint"
    static = "static"


class SqlConnectionConfig(BaseModel):
    driver: SqlConnectionDriver = SqlConnectionDriver.sqlite
    database: str
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("database")
    @classmethod
    def _validate_database(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("database must not be empty")
        return normalized


class SqlQueryTemplate(BaseModel):
    sql: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    fetch_size: int = Field(default=1000, ge=1)
    artifact_name: str | None = None

    @field_validator("sql")
    @classmethod
    def _validate_sql(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("sql must not be empty")
        return normalized


class SqlWatermarkConfig(BaseModel):
    column_name: str
    parameter_name: str = "watermark"
    checkpoint_key: str = "watermark"
    state_source: SqlWatermarkSource = SqlWatermarkSource.checkpoint
    default_value: Any = None

    @field_validator("column_name", "parameter_name", "checkpoint_key")
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Watermark fields must not be empty")
        return normalized

    @model_validator(mode="after")
    def _validate_static_source(self) -> "SqlWatermarkConfig":
        if self.state_source == SqlWatermarkSource.static and self.default_value is None:
            raise ValueError("static watermark state_source requires default_value")
        return self


class SqlConnectorConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    execution_mode: str
    extraction_mode: SqlExtractionMode = SqlExtractionMode.snapshot
    connection: SqlConnectionConfig
    query: SqlQueryTemplate
    watermark: SqlWatermarkConfig | None = None
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_delta_requirements(self) -> "SqlConnectorConfig":
        if self.extraction_mode == SqlExtractionMode.delta and self.watermark is None:
            raise ValueError("Delta SQL extraction requires watermark configuration")
        return self

    @classmethod
    def from_resolved_entity_config(
        cls,
        resolved_config: ResolvedEntityConfig,
    ) -> "SqlConnectorConfig":
        if resolved_config.connector_type != "sql":
            raise ConfigValidationError(
                message="Resolved entity config is not a SQL connector",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": resolved_config.connector_type,
                },
            )

        extraction = resolved_config.extraction
        query_payload = extraction.get("query", extraction.get("sql"))
        if isinstance(query_payload, str):
            query_payload = {"sql": query_payload}
        elif query_payload is None:
            query_payload = {
                "sql": extraction.get("statement") or extraction.get("base_query"),
                "parameters": extraction.get("query_parameters", {}),
                "fetch_size": extraction.get("fetch_size", 1000),
                "artifact_name": extraction.get("artifact_name"),
            }

        connection_payload = extraction.get("connection") or {
            "driver": extraction.get("driver", "sqlite"),
            "database": extraction.get("database") or resolved_config.settings.get("database"),
            "options": extraction.get("connection_options", {}),
        }

        extraction_mode = extraction.get("mode") or extraction.get("extraction_mode") or "snapshot"
        watermark_payload = extraction.get("watermark")

        try:
            return cls(
                schema_version=resolved_config.schema_version,
                environment=resolved_config.environment,
                source_name=resolved_config.source_name,
                entity_name=resolved_config.entity_name,
                execution_mode=resolved_config.trigger_mode or "manual",
                extraction_mode=SqlExtractionMode(extraction_mode),
                connection=SqlConnectionConfig.model_validate(connection_payload),
                query=SqlQueryTemplate.model_validate(query_payload),
                watermark=(
                    SqlWatermarkConfig.model_validate(watermark_payload)
                    if watermark_payload is not None
                    else None
                ),
                persistence=resolved_config.persistence,
                state=resolved_config.state,
                settings=resolved_config.settings,
                raw=resolved_config.raw,
            )
        except ValidationError as exc:
            raise ConfigValidationError(
                message="SQL connector configuration validation failed",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc


class SqlPreparedQuery(BaseModel):
    sql: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SqlQueryResult(BaseModel):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    executed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def row_count(self) -> int:
        return len(self.rows)


class SqlRunResult(BaseModel):
    manifests: list[Level1ArtifactManifest] = Field(default_factory=list)
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    query_count: int = 0
    row_count: int = 0


class SqlConnectorBase(ABC):
    def __init__(self, *, config: SqlConnectorConfig, run_context: RunContext) -> None:
        self.config = config
        self.run_context = run_context

    def validate_config(self) -> SqlConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    def resolve_watermark(self, *, checkpoint_before: dict[str, Any] | None) -> Any:
        if self.config.extraction_mode == SqlExtractionMode.snapshot:
            return None
        watermark = self.config.watermark
        if watermark is None:
            raise ConfigValidationError(
                message="Delta SQL extraction requires watermark configuration",
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                },
            )
        if watermark.state_source == SqlWatermarkSource.checkpoint:
            if checkpoint_before and watermark.checkpoint_key in checkpoint_before:
                return checkpoint_before[watermark.checkpoint_key]
            if watermark.default_value is not None:
                return watermark.default_value
            raise ConfigValidationError(
                message="SQL delta extraction could not resolve a starting watermark",
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "checkpoint_key": watermark.checkpoint_key,
                },
            )
        if watermark.state_source == SqlWatermarkSource.static:
            return watermark.default_value
        raise ConfigValidationError(
            message="SQL watermark state_source is not supported",
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "state_source": watermark.state_source.value,
            },
        )

    def build_query_plan(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        watermark_value: Any,
    ) -> list[SqlPreparedQuery]:
        template_context = _build_template_context(
            config=self.config,
            run_context=self.run_context,
            checkpoint_before=checkpoint_before,
            watermark_value=watermark_value,
        )
        compiled_sql = _render_string_template(
            self.config.query.sql,
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        compiled_parameters = _render_template_value(
            self.config.query.parameters,
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return [
            SqlPreparedQuery(
                sql=str(compiled_sql),
                parameters=dict(compiled_parameters),
                metadata={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "compiled_sql": str(compiled_sql),
                    "compiled_parameters": dict(compiled_parameters),
                    "watermark_value": _serialize_template_scalar(watermark_value),
                    "extraction_mode": self.config.extraction_mode.value,
                },
            )
        ]

    @abstractmethod
    def execute_query(self, query: SqlPreparedQuery) -> SqlQueryResult:
        """Execute a prepared SQL query and return its rows."""

    @abstractmethod
    def persist_query_result(
        self,
        *,
        query: SqlPreparedQuery,
        result: SqlQueryResult,
        checkpoint_before: dict[str, Any] | None,
    ) -> list[Level1ArtifactManifest]:
        """Persist query results into level1 and return written manifests."""

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        results: list[SqlQueryResult],
        manifests: list[Level1ArtifactManifest],
    ) -> dict[str, Any] | None:
        del manifests
        if self.config.extraction_mode == SqlExtractionMode.snapshot:
            return checkpoint_before
        watermark = self.config.watermark
        if watermark is None:
            return checkpoint_before
        watermark_values = [
            row.get(watermark.column_name)
            for result in results
            for row in result.rows
            if row.get(watermark.column_name) is not None
        ]
        if not watermark_values:
            return checkpoint_before
        max_watermark = _max_watermark_value(
            watermark_values,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            column_name=watermark.column_name,
        )
        checkpoint_after = dict(checkpoint_before or {})
        checkpoint_after[watermark.checkpoint_key] = max_watermark
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

    def run(self) -> SqlRunResult:
        self.validate_config()
        checkpoint_before = self.resolve_checkpoint_before()
        watermark_value = self.resolve_watermark(checkpoint_before=checkpoint_before)
        queries = self.build_query_plan(
            checkpoint_before=checkpoint_before,
            watermark_value=watermark_value,
        )
        manifests: list[Level1ArtifactManifest] = []
        results: list[SqlQueryResult] = []

        for query in queries:
            result = self.execute_query(query)
            results.append(result)
            manifests.extend(
                self.persist_query_result(
                    query=query,
                    result=result,
                    checkpoint_before=checkpoint_before,
                )
            )

        checkpoint_after = self.build_checkpoint_after(
            checkpoint_before=checkpoint_before,
            results=results,
            manifests=manifests,
        )
        self.update_checkpoint(
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            manifests=manifests,
        )
        return SqlRunResult(
            manifests=manifests,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            query_count=len(queries),
            row_count=sum(result.row_count for result in results),
        )


def _build_template_context(
    *,
    config: SqlConnectorConfig,
    run_context: RunContext,
    checkpoint_before: dict[str, Any] | None,
    watermark_value: Any,
) -> dict[str, Any]:
    watermark = config.watermark
    return {
        "run": {
            "id": run_context.run_id,
            "job_name": run_context.job_name,
            "trigger_type": run_context.trigger_type,
            "started_at": run_context.started_at.isoformat(),
        },
        "source": {"name": config.source_name},
        "entity": {"name": config.entity_name},
        "config": {
            "schema_version": config.schema_version,
            "environment": config.environment,
            "execution_mode": config.execution_mode,
            "driver": config.connection.driver.value,
            "database": config.connection.database,
            "extraction_mode": config.extraction_mode.value,
        },
        "checkpoint": checkpoint_before or {},
        "watermark": {
            "value": _serialize_template_scalar(watermark_value),
            "column_name": watermark.column_name if watermark else None,
            "checkpoint_key": watermark.checkpoint_key if watermark else None,
            "parameter_name": watermark.parameter_name if watermark else None,
        },
        "environment": config.environment,
    }


def _max_watermark_value(
    values: list[Any],
    *,
    source_name: str,
    entity_name: str,
    column_name: str,
) -> Any:
    if not values:
        return None
    first_type = type(values[0])
    if any(type(value) is not first_type for value in values[1:]):
        raise ConfigValidationError(
            message="SQL watermark values must share a comparable type",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "column_name": column_name,
                "value_types": sorted({type(value).__name__ for value in values}),
            },
        )
    try:
        return max(values)
    except TypeError as exc:
        raise ConfigValidationError(
            message="SQL watermark values are not comparable",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "column_name": column_name,
                "value_type": first_type.__name__,
            },
        ) from exc


__all__ = [
    "SqlConnectionConfig",
    "SqlConnectionDriver",
    "SqlConnectorBase",
    "SqlConnectorConfig",
    "SqlExtractionMode",
    "SqlPreparedQuery",
    "SqlQueryResult",
    "SqlQueryTemplate",
    "SqlRunResult",
    "SqlWatermarkConfig",
    "SqlWatermarkSource",
]

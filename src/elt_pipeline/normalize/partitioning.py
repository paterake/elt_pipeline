from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ErrorCategory, PipelineError


class PartitionMode(str, Enum):
    none = "none"
    ingest_date = "ingest_date"
    business_date = "business_date"
    source_key = "source_key"


def _format_partition_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


class PartitionStrategy(BaseModel):
    mode: PartitionMode = PartitionMode.ingest_date
    partition_key: str | None = None
    metadata_key: str | None = None
    defaults: dict[str, str] = Field(default_factory=dict)

    def resolve(
        self,
        *,
        manifest: Level1ArtifactManifest,
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        if self.mode == PartitionMode.none:
            return {}

        if self.mode == PartitionMode.ingest_date:
            return {"ingest_date": manifest.ingest_started_at.date().isoformat()}

        context = dict(self.defaults)
        if extra_context:
            context.update({k: _format_partition_value(v) for k, v in extra_context.items()})

        metadata = manifest.metadata or {}
        if self.mode == PartitionMode.business_date:
            metadata_key = self.metadata_key or "business_date"
            value = metadata.get(metadata_key)
            if value is None:
                value = context.get(metadata_key)
            if value is None:
                raise PipelineError(
                    message="Missing business date for partitioning strategy",
                    error_code="LEVEL2_PARTITION_BUSINESS_DATE_MISSING",
                    error_category=ErrorCategory.input_contract_error,
                    retryable=False,
                    context={"metadata_key": metadata_key, "artifact_id": manifest.artifact_id},
                )
            return {"business_date": _format_partition_value(value)}

        if self.mode == PartitionMode.source_key:
            metadata_key = self.metadata_key or self.partition_key
            partition_key = self.partition_key or metadata_key
            if not metadata_key or not partition_key:
                raise PipelineError(
                    message="Partition strategy requires metadata_key or partition_key",
                    error_code="LEVEL2_PARTITION_SOURCE_KEY_INVALID",
                    error_category=ErrorCategory.config_error,
                    retryable=False,
                )
            value = metadata.get(metadata_key)
            if value is None:
                value = context.get(metadata_key)
            if value is None:
                raise PipelineError(
                    message="Missing source key for partitioning strategy",
                    error_code="LEVEL2_PARTITION_SOURCE_KEY_MISSING",
                    error_category=ErrorCategory.input_contract_error,
                    retryable=False,
                    context={"metadata_key": metadata_key, "artifact_id": manifest.artifact_id},
                )
            return {partition_key: _format_partition_value(value)}

        raise PipelineError(
            message="Unsupported partition mode",
            error_code="LEVEL2_PARTITION_MODE_UNSUPPORTED",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"mode": self.mode.value},
        )


from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from pydantic import BaseModel, Field

from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.shared.errors import ConfigValidationError

_env = runtime_manifest.env
_QUALITY_BACKEND_ENV = _env.quality_backend
_QUALITY_POLICY_ENV = _env.quality_policy
_QUALITY_ROW_COUNT_MIN_ENV = _env.quality_row_count_min
_QUALITY_STAGES_ENV = _env.quality_stages
_QUALITY_CHECKS_JSON_ENV = _env.quality_checks_json
_QUALITY_CHECKS_YAML_ENV = _env.quality_checks_yaml
_QUALITY_ERROR_RECORDED_CONTEXT_KEY = "quality_error_recorded"
_SUPPORTED_QUALITY_STAGES = frozenset({"normalize", "sql"})
BUILTIN_CHECKS_BACKEND_TYPE = "builtin_checks"
ROW_COUNT_BACKEND_TYPE = "row_count_threshold"


class QualityHookPolicy(str, Enum):
    best_effort = "best_effort"
    blocking = "blocking"


class QualityCheckStatus(str, Enum):
    pass_ = "pass"
    warn = "warn"
    fail = "fail"
    skipped = "skipped"


class QualityDatasetRef(BaseModel):
    dataset_id: str
    dataset_name: str
    materialization_type: str
    target_name: str
    output_path: str | None = None
    row_count: int | None = None
    metrics: dict[str, int | float | str] = Field(default_factory=dict)
    records: list[dict[str, object]] = Field(default_factory=list)


class QualityHookRequest(BaseModel):
    run_id: str
    stage: str
    job_name: str
    environment: str
    datasets: list[QualityDatasetRef] = Field(default_factory=list)
    metrics: dict[str, int | float | str] = Field(default_factory=dict)
    reference_datasets: dict[str, list[dict[str, object]]] = Field(default_factory=dict)


class QualityCheckResult(BaseModel):
    backend_type: str
    check_name: str
    status: QualityCheckStatus
    blocking: bool = False
    dataset_id: str | None = None
    dataset_name: str | None = None
    message: str | None = None
    observed_value: int | float | str | None = None
    expected_value: int | float | str | None = None
    violated_records: list[dict[str, object]] = Field(default_factory=list)
    check_details: dict[str, object] = Field(default_factory=dict)


class QualityHookSummary(BaseModel):
    backend_type: str
    stage: str
    passed: bool
    results: list[QualityCheckResult] = Field(default_factory=list)

    @property
    def blocking_failure_count(self) -> int:
        return sum(
            1
            for result in self.results
            if result.status == QualityCheckStatus.fail and result.blocking
        )

    def counts_by_status(self) -> dict[str, int]:
        counts = {status.value: 0 for status in QualityCheckStatus}
        for result in self.results:
            counts[result.status.value] += 1
        return counts

    @property
    def log_severity(self) -> str:
        if any(
            result.status == QualityCheckStatus.fail and result.blocking
            for result in self.results
        ):
            return "ERROR"
        if any(
            result.status in {QualityCheckStatus.warn, QualityCheckStatus.fail}
            for result in self.results
        ):
            return "WARNING"
        return "INFO"


class QualityHookBackend(Protocol):
    backend_type: str

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]: ...


@dataclass(frozen=True)
class _RowCountBackendConfig:
    row_count_min: int
    enabled_stages: frozenset[str]
    policy: QualityHookPolicy


@dataclass(frozen=True)
class _BuiltinChecksBackendConfig:
    checks: Any  # list[BuiltinQualityCheck] from shared/quality; typed lazily
    enabled_stages: frozenset[str]
    policy: QualityHookPolicy


def _require_quality_env_value(variable_name: str) -> str:
    value = os.getenv(variable_name)
    if value is None or not value.strip():
        raise ConfigValidationError(
            message=f"{variable_name} is required when data-quality hooks are enabled",
            context={"variable_name": variable_name},
        )
    return value.strip()


def _validate_row_count_min(*, row_count_min: int, backend_type: str) -> int:
    if isinstance(row_count_min, bool) or not isinstance(row_count_min, int):
        raise ConfigValidationError(
            message="Data-quality row count minimum must be an integer",
            context={"backend_type": backend_type, "row_count_min": row_count_min},
        )
    if row_count_min < 0:
        raise ConfigValidationError(
            message="Data-quality row count minimum must be greater than or equal to zero",
            context={"backend_type": backend_type, "row_count_min": row_count_min},
        )
    return row_count_min


def _validate_enabled_stages(
    stages: set[str] | frozenset[str] | list[str],
    *,
    backend_type: str,
) -> frozenset[str]:
    enabled_stages = _normalize_stage_set(stages)
    invalid_stages = sorted(
        stage for stage in enabled_stages if stage not in _SUPPORTED_QUALITY_STAGES
    )
    if invalid_stages:
        raise ConfigValidationError(
            message="Data-quality hook stages contain unsupported values",
            context={
                "backend_type": backend_type,
                "configured_stages": sorted(enabled_stages),
                "invalid_stages": invalid_stages,
                "supported_stages": sorted(_SUPPORTED_QUALITY_STAGES),
            },
        )
    if not enabled_stages:
        raise ConfigValidationError(
            message="Data-quality hook stages must include at least one supported stage",
            context={"backend_type": backend_type},
        )
    return enabled_stages


def _normalize_stage_set(stages: set[str] | frozenset[str] | list[str]) -> frozenset[str]:
    return frozenset(stage.strip().lower() for stage in stages if stage.strip())

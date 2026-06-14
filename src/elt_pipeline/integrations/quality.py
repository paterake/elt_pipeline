from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, Field

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext

_QUALITY_BACKEND_ENV = "ELT_PIPELINE_QUALITY_BACKEND"
_QUALITY_POLICY_ENV = "ELT_PIPELINE_QUALITY_POLICY"
_QUALITY_ROW_COUNT_MIN_ENV = "ELT_PIPELINE_QUALITY_ROW_COUNT_MIN"
_QUALITY_STAGES_ENV = "ELT_PIPELINE_QUALITY_STAGES"
_QUALITY_ERROR_RECORDED_CONTEXT_KEY = "quality_error_recorded"
_SUPPORTED_QUALITY_STAGES = frozenset({"normalize", "sql"})


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


class QualityHookRequest(BaseModel):
    run_id: str
    stage: str
    job_name: str
    environment: str
    datasets: list[QualityDatasetRef] = Field(default_factory=list)
    metrics: dict[str, int | float | str] = Field(default_factory=dict)


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


class RowCountQualityHook:
    backend_type = "row_count_threshold"

    def __init__(
        self,
        *,
        row_count_min: int,
        enabled_stages: set[str] | frozenset[str] | None = None,
    ) -> None:
        self._row_count_min = _validate_row_count_min(
            row_count_min=row_count_min,
            backend_type=self.backend_type,
        )
        self._enabled_stages = _validate_enabled_stages(
            enabled_stages or {"normalize", "sql"},
            backend_type=self.backend_type,
        )

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]:
        stage_name = request.stage.strip().lower()
        if stage_name not in self._enabled_stages:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=QualityCheckStatus.skipped,
                    message=f"Stage '{request.stage}' is not enabled for row count checks",
                )
            ]

        if not request.datasets:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=QualityCheckStatus.skipped,
                    message="No datasets were emitted for quality evaluation",
                    expected_value=self._row_count_min,
                )
            ]

        results: list[QualityCheckResult] = []
        for dataset in request.datasets:
            if dataset.row_count is None:
                results.append(
                    QualityCheckResult(
                        backend_type=self.backend_type,
                        check_name="row_count_min",
                        status=QualityCheckStatus.skipped,
                        dataset_id=dataset.dataset_id,
                        dataset_name=dataset.dataset_name,
                        message="Row count metric is unavailable for this dataset",
                        expected_value=self._row_count_min,
                    )
                )
                continue

            passed = dataset.row_count >= self._row_count_min
            results.append(
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=(
                        QualityCheckStatus.pass_ if passed else QualityCheckStatus.fail
                    ),
                    dataset_id=dataset.dataset_id,
                    dataset_name=dataset.dataset_name,
                    observed_value=dataset.row_count,
                    expected_value=self._row_count_min,
                    message=(
                        None
                        if passed
                        else (
                            f"Dataset '{dataset.dataset_name}' row count "
                            f"{dataset.row_count} is below minimum {self._row_count_min}"
                        )
                    ),
                )
            )
        return results


class QualityHookAdapter:
    def __init__(
        self,
        *,
        artifact_store: LocalArtifactStore,
        backend: QualityHookBackend | None = None,
        policy: QualityHookPolicy = QualityHookPolicy.best_effort,
    ) -> None:
        self._artifact_store = artifact_store
        self._backend = backend
        self._policy = policy

    def evaluate(
        self,
        *,
        run_context: RunContext,
        environment: str,
        request: QualityHookRequest,
    ) -> QualityHookSummary | None:
        if self._backend is None:
            return None

        try:
            results = self._backend.evaluate(request=request)
        except PipelineError as exc:
            summary = self._build_backend_failure_summary(
                request=request,
                backend_type=self._backend.backend_type,
                message=str(exc),
            )
            self._record_backend_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._backend.backend_type,
                error=exc,
                summary=summary,
            )
            if self._policy is QualityHookPolicy.blocking:
                raise PipelineError(
                    message="Optional data-quality backend execution failed",
                    error_code="QUALITY_BACKEND_EXECUTION_FAILED",
                    error_category=ErrorCategory.validation_error,
                    retryable=exc.retryable,
                    context={
                        **exc.context,
                        "backend_type": self._backend.backend_type,
                        "quality_summary": summary.model_dump(mode="json"),
                        _QUALITY_ERROR_RECORDED_CONTEXT_KEY: True,
                    },
                ) from exc
            return summary
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional data-quality backend execution failed",
                error_code="QUALITY_BACKEND_EXECUTION_FAILED",
                error_category=ErrorCategory.validation_error,
                retryable=True,
                context={
                    "backend_type": self._backend.backend_type,
                    "stage": request.stage,
                    "job_name": request.job_name,
                },
            )
            summary = self._build_backend_failure_summary(
                request=request,
                backend_type=self._backend.backend_type,
                message=str(exc),
            )
            self._record_backend_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._backend.backend_type,
                error=pipeline_error,
                summary=summary,
            )
            if self._policy is QualityHookPolicy.blocking:
                raise PipelineError(
                    message="Optional data-quality backend execution failed",
                    error_code="QUALITY_BACKEND_EXECUTION_FAILED",
                    error_category=ErrorCategory.validation_error,
                    retryable=True,
                    context={
                        "backend_type": self._backend.backend_type,
                        "stage": request.stage,
                        "job_name": request.job_name,
                        "quality_summary": summary.model_dump(mode="json"),
                        _QUALITY_ERROR_RECORDED_CONTEXT_KEY: True,
                    },
                ) from exc
            return summary

        coerced_results = [
            result.model_copy(
                update={
                    "blocking": (
                        result.blocking
                        or (
                            result.status == QualityCheckStatus.fail
                            and self._policy is QualityHookPolicy.blocking
                        )
                    )
                }
            )
            for result in results
        ]
        return QualityHookSummary(
            backend_type=self._backend.backend_type,
            stage=request.stage,
            passed=all(result.status != QualityCheckStatus.fail for result in coerced_results),
            results=coerced_results,
        )

    def _build_backend_failure_summary(
        self,
        *,
        request: QualityHookRequest,
        backend_type: str,
        message: str,
    ) -> QualityHookSummary:
        status = (
            QualityCheckStatus.fail
            if self._policy is QualityHookPolicy.blocking
            else QualityCheckStatus.warn
        )
        return QualityHookSummary(
            backend_type=backend_type,
            stage=request.stage,
            passed=status != QualityCheckStatus.fail,
            results=[
                QualityCheckResult(
                    backend_type=backend_type,
                    check_name="backend_execution",
                    status=status,
                    blocking=self._policy is QualityHookPolicy.blocking,
                    message=message,
                )
            ],
        )

    def _record_backend_failure(
        self,
        *,
        run_context: RunContext,
        environment: str,
        backend_type: str,
        error: PipelineError,
        summary: QualityHookSummary,
    ) -> None:
        self._artifact_store.append_error_record(
            run_context=run_context,
            environment=environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=error.error_code,
                error_category=error.error_category,
                message=str(error),
                retryable=error.retryable,
                context=error.context,
            ),
        )
        self._artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity=(
                    "ERROR" if self._policy is QualityHookPolicy.blocking else "WARNING"
                ),
                component="quality",
                event_type="quality_hook_failed",
                message="Optional data-quality backend execution failed",
                details={
                    "backend_type": backend_type,
                    "error_code": error.error_code,
                    "blocking": self._policy is QualityHookPolicy.blocking,
                    "status_counts": summary.counts_by_status(),
                },
            ),
        )


def build_quality_hook(
    root_path: Path,
    *,
    backend: QualityHookBackend | None = None,
    policy: QualityHookPolicy | None = None,
) -> QualityHookAdapter:
    configured_backend = backend
    configured_policy = policy

    if configured_backend is None:
        backend_config = _load_row_count_backend_config_from_env()
        if backend_config is not None:
            configured_backend = RowCountQualityHook(
                row_count_min=backend_config.row_count_min,
                enabled_stages=set(backend_config.enabled_stages),
            )
            configured_policy = configured_policy or backend_config.policy

    return QualityHookAdapter(
        artifact_store=LocalArtifactStore(root_path),
        backend=configured_backend,
        policy=configured_policy or QualityHookPolicy.best_effort,
    )


def raise_for_blocking_quality_failures(summary: QualityHookSummary) -> None:
    if summary.blocking_failure_count == 0:
        return
    raise PipelineError(
        message=f"Quality checks failed for stage '{summary.stage}'",
        error_code="QUALITY_CHECK_FAILED",
        error_category=ErrorCategory.validation_error,
        retryable=False,
        context={
            "stage": summary.stage,
            "backend_type": summary.backend_type,
            "quality_summary": summary.model_dump(mode="json"),
        },
    )


def quality_error_already_recorded(error: PipelineError) -> bool:
    return bool(error.context.get(_QUALITY_ERROR_RECORDED_CONTEXT_KEY))


def _load_row_count_backend_config_from_env() -> _RowCountBackendConfig | None:
    raw_backend_type = os.getenv(_QUALITY_BACKEND_ENV)
    if raw_backend_type is None or not raw_backend_type.strip():
        return None

    backend_type = raw_backend_type.strip().lower()
    if backend_type != RowCountQualityHook.backend_type:
        raise ConfigValidationError(
            message="Unsupported data-quality backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": [RowCountQualityHook.backend_type],
            },
        )

    raw_policy = os.getenv(_QUALITY_POLICY_ENV, QualityHookPolicy.best_effort.value)
    try:
        policy = QualityHookPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message="Data-quality hook policy is invalid",
            context={
                "backend_type": backend_type,
                "policy": raw_policy,
                "supported_values": [item.value for item in QualityHookPolicy],
            },
        ) from exc

    raw_row_count_min = _require_quality_env_value(_QUALITY_ROW_COUNT_MIN_ENV)
    try:
        row_count_min = int(raw_row_count_min)
    except ValueError as exc:
        raise ConfigValidationError(
            message="Data-quality row count minimum must be an integer",
            context={"backend_type": backend_type, "row_count_min": raw_row_count_min},
        ) from exc
    row_count_min = _validate_row_count_min(
        row_count_min=row_count_min,
        backend_type=backend_type,
    )

    raw_stages = os.getenv(_QUALITY_STAGES_ENV, "normalize,sql")
    enabled_stages = _validate_enabled_stages(
        raw_stages.split(","),
        backend_type=backend_type,
    )

    return _RowCountBackendConfig(
        row_count_min=row_count_min,
        enabled_stages=enabled_stages,
        policy=policy,
    )


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

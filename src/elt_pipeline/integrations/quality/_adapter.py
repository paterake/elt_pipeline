from __future__ import annotations

import os
from typing import Any

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations.quality._hooks import (
    BuiltinQualityHook,
    RowCountQualityHook,
)
from elt_pipeline.integrations.quality._models import (
    _QUALITY_BACKEND_ENV,
    _QUALITY_CHECKS_JSON_ENV,
    _QUALITY_CHECKS_YAML_ENV,
    _QUALITY_ERROR_RECORDED_CONTEXT_KEY,
    _QUALITY_POLICY_ENV,
    _QUALITY_ROW_COUNT_MIN_ENV,
    _QUALITY_STAGES_ENV,
    BUILTIN_CHECKS_BACKEND_TYPE,
    QualityCheckResult,
    QualityCheckStatus,
    QualityHookBackend,
    QualityHookPolicy,
    QualityHookRequest,
    QualityHookSummary,
    _BuiltinChecksBackendConfig,
    _require_quality_env_value,
    _RowCountBackendConfig,
    _validate_enabled_stages,
    _validate_row_count_min,
)
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext


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

        quarantine_paths: dict[str, int] = {}
        for result in coerced_results:
            if result.status != QualityCheckStatus.fail:
                continue
            if not result.violated_records:
                continue
            wrote_path = self._artifact_store.append_quarantine_records(
                run_context=run_context,
                environment=environment,
                stage=request.stage,
                check_name=result.check_name,
                dataset_id=result.dataset_id,
                dataset_name=result.dataset_name,
                records=[dict(r) for r in result.violated_records],
                extra_metadata={
                    "backend_type": self._backend.backend_type if self._backend else "unknown",
                    "blocking": bool(result.blocking),
                    "observed_value": result.observed_value,
                    "expected_value": result.expected_value,
                    "kind": (
                        result.check_details.get("kind")
                        if isinstance(result.check_details, dict)
                        else None
                    ),
                },
            )
            quarantine_paths[wrote_path] = (
                quarantine_paths.get(wrote_path, 0) + len(result.violated_records)
            )

        summary = QualityHookSummary(
            backend_type=self._backend.backend_type,
            stage=request.stage,
            passed=all(result.status != QualityCheckStatus.fail for result in coerced_results),
            results=coerced_results,
        )
        if quarantine_paths:
            self._artifact_store.append_log_event(
                run_context=run_context,
                environment=environment,
                log_event=build_log_event(
                    run_context=run_context,
                    severity="WARNING",
                    component="quality",
                    event_type="quality_quarantine_written",
                    message=(
                        f"DQ check failures written to quarantine for stage "
                        f"{request.stage!r}: {sum(quarantine_paths.values())} rows across "
                        f"{len(quarantine_paths)} file(s)."
                    ),
                    details={
                        "stage": request.stage,
                        "backend_type": self._backend.backend_type if self._backend else "unknown",
                        "policy": self._policy.value,
                        "quarantine_paths": list(quarantine_paths.keys()),
                        "quarantine_row_counts": quarantine_paths,
                        "status_counts": summary.counts_by_status(),
                        "blocking_failure_count": summary.blocking_failure_count,
                    },
                ),
            )
        return summary

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
    root_path: str,
    *,
    backend: QualityHookBackend | None = None,
    policy: QualityHookPolicy | None = None,
) -> QualityHookAdapter:
    configured_backend = backend
    configured_policy = policy

    if configured_backend is None:
        row_cfg = _load_row_count_backend_config_from_env()
        builtin_cfg = _load_builtin_checks_backend_config_from_env()
        if row_cfg is not None and builtin_cfg is not None:
            raise ConfigValidationError(
                message=(
                    "Ambiguous data-quality env configuration: both "
                    "row_count_threshold and builtin_checks backends are configured. "
                    "Set only ONE of ELT_PIPELINE_QUALITY_BACKEND values."
                ),
                context={
                    "row_count_configured_via": _QUALITY_BACKEND_ENV
                    + f"={RowCountQualityHook.backend_type!r}",
                    "builtin_checks_configured_via": (
                        f"{_QUALITY_CHECKS_JSON_ENV} or {_QUALITY_CHECKS_YAML_ENV}"
                    ),
                },
            )
        if row_cfg is not None:
            configured_backend = RowCountQualityHook(
                row_count_min=row_cfg.row_count_min,
                enabled_stages=set(row_cfg.enabled_stages),
            )
            configured_policy = configured_policy or row_cfg.policy
        elif builtin_cfg is not None:
            configured_backend = BuiltinQualityHook(
                checks=builtin_cfg.checks,
                enabled_stages=set(builtin_cfg.enabled_stages),
            )
            configured_policy = configured_policy or builtin_cfg.policy

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
    if backend_type == BUILTIN_CHECKS_BACKEND_TYPE:
        return None
    if backend_type != RowCountQualityHook.backend_type:
        raise ConfigValidationError(
            message="Unsupported data-quality backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": [
                    RowCountQualityHook.backend_type,
                    BUILTIN_CHECKS_BACKEND_TYPE,
                ],
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


def _load_builtin_checks_backend_config_from_env() -> _BuiltinChecksBackendConfig | None:
    json_path = os.getenv(_QUALITY_CHECKS_JSON_ENV)
    yaml_path = os.getenv(_QUALITY_CHECKS_YAML_ENV)
    backend_hint = (os.getenv(_QUALITY_BACKEND_ENV) or "").strip().lower()
    if not json_path and not yaml_path and backend_hint != BUILTIN_CHECKS_BACKEND_TYPE:
        return None

    checks: list[Any]
    if json_path and yaml_path:
        raise ConfigValidationError(
            message=(
                "Ambiguous builtin DQ configuration: both checks JSON and YAML paths set."
            ),
            context={
                "backend_type": BUILTIN_CHECKS_BACKEND_TYPE,
                _QUALITY_CHECKS_JSON_ENV: json_path,
                _QUALITY_CHECKS_YAML_ENV: yaml_path,
            },
        )

    from elt_pipeline.shared.quality import (
        load_builtin_checks_from_json,
        load_builtin_checks_from_yaml,
    )

    if json_path:
        checks = load_builtin_checks_from_json(json_path.strip())
    elif yaml_path:
        checks = load_builtin_checks_from_yaml(yaml_path.strip())
    else:
        if backend_hint == BUILTIN_CHECKS_BACKEND_TYPE:
            raise ConfigValidationError(
                message=(
                    "ELT_PIPELINE_QUALITY_BACKEND=builtin_checks requires either "
                    f"{_QUALITY_CHECKS_JSON_ENV} or {_QUALITY_CHECKS_YAML_ENV} to be set."
                ),
                context={
                    "backend_type": BUILTIN_CHECKS_BACKEND_TYPE,
                },
            )
        return None

    raw_policy = os.getenv(_QUALITY_POLICY_ENV, QualityHookPolicy.best_effort.value)
    try:
        policy = QualityHookPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message="Data-quality hook policy is invalid",
            context={
                "backend_type": BUILTIN_CHECKS_BACKEND_TYPE,
                "policy": raw_policy,
                "supported_values": [item.value for item in QualityHookPolicy],
            },
        ) from exc

    raw_stages = os.getenv(_QUALITY_STAGES_ENV, "normalize,sql")
    enabled_stages = _validate_enabled_stages(
        [s for s in raw_stages.split(",") if s.strip()],
        backend_type=BUILTIN_CHECKS_BACKEND_TYPE,
    )

    return _BuiltinChecksBackendConfig(
        checks=checks,
        enabled_stages=enabled_stages,
        policy=policy,
    )

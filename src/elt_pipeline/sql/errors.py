from __future__ import annotations

from enum import Enum
from typing import Any

from elt_pipeline.shared.errors import ErrorCategory, PipelineError


class SqlRuntimeErrorCode(str, Enum):
    run_context_invalid = "SQL_RUN_CONTEXT_INVALID"
    execution_failed = "SQL_EXECUTION_FAILED"
    load_mode_unsupported = "SQL_LOAD_MODE_UNSUPPORTED"
    model_validation_failed = "SQL_MODEL_VALIDATION_FAILED"
    partition_value_missing = "SQL_PARTITION_VALUE_MISSING"
    planning_failed = "SQL_PLANNING_FAILED"
    identifier_invalid = "SQL_IDENTIFIER_INVALID"
    dependency_not_materialized = "SQL_DEPENDENCY_NOT_MATERIALIZED"
    level2_source_not_found = "SQL_LEVEL2_SOURCE_NOT_FOUND"
    staging_write_failed = "SQL_STAGING_WRITE_FAILED"
    atomic_swap_failed = "SQL_ATOMIC_SWAP_FAILED"
    staging_scheme_unsupported = "SQL_STAGING_SCHEME_UNSUPPORTED"


_SQL_RUNTIME_ERROR_CATEGORIES = {
    SqlRuntimeErrorCode.run_context_invalid: ErrorCategory.unexpected_runtime_error,
    SqlRuntimeErrorCode.execution_failed: ErrorCategory.processing_error,
    SqlRuntimeErrorCode.load_mode_unsupported: ErrorCategory.config_error,
    SqlRuntimeErrorCode.model_validation_failed: ErrorCategory.validation_error,
    SqlRuntimeErrorCode.partition_value_missing: ErrorCategory.config_error,
    SqlRuntimeErrorCode.planning_failed: ErrorCategory.processing_error,
    SqlRuntimeErrorCode.identifier_invalid: ErrorCategory.config_error,
    SqlRuntimeErrorCode.dependency_not_materialized: ErrorCategory.config_error,
    SqlRuntimeErrorCode.level2_source_not_found: ErrorCategory.input_contract_error,
    SqlRuntimeErrorCode.staging_write_failed: ErrorCategory.processing_error,
    SqlRuntimeErrorCode.atomic_swap_failed: ErrorCategory.processing_error,
    SqlRuntimeErrorCode.staging_scheme_unsupported: ErrorCategory.config_error,
}


def build_sql_runtime_error(
    *,
    code: SqlRuntimeErrorCode,
    message: str,
    retryable: bool = False,
    context: dict[str, Any] | None = None,
) -> PipelineError:
    return PipelineError(
        message=message,
        error_code=code.value,
        error_category=_SQL_RUNTIME_ERROR_CATEGORIES[code],
        retryable=retryable,
        context=context,
    )

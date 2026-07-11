from __future__ import annotations

from enum import Enum
from typing import Any

from elt_pipeline.shared.errors import ErrorCategory, PipelineError


class SparkRuntimeErrorCode(str, Enum):
    session_unavailable = "SPARK_SESSION_UNAVAILABLE"
    write_failed = "SPARK_WRITE_FAILED"
    read_failed = "SPARK_READ_FAILED"
    schema_mismatch = "SPARK_SCHEMA_MISMATCH"


_SPARK_RUNTIME_ERROR_CATEGORIES = {
    SparkRuntimeErrorCode.session_unavailable: ErrorCategory.unexpected_runtime_error,
    SparkRuntimeErrorCode.write_failed: ErrorCategory.storage_write_error,
    SparkRuntimeErrorCode.read_failed: ErrorCategory.processing_error,
    SparkRuntimeErrorCode.schema_mismatch: ErrorCategory.validation_error,
}


def build_spark_runtime_error(
    *,
    code: SparkRuntimeErrorCode,
    message: str,
    retryable: bool = False,
    context: dict[str, Any] | None = None,
) -> PipelineError:
    return PipelineError(
        message=message,
        error_code=code.value,
        error_category=_SPARK_RUNTIME_ERROR_CATEGORIES[code],
        retryable=retryable,
        context=context,
    )

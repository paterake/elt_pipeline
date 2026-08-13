from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ErrorCategory(str, Enum):
    config_error = "config_error"
    validation_error = "validation_error"
    input_contract_error = "input_contract_error"
    processing_error = "processing_error"
    storage_write_error = "storage_write_error"
    lineage_error = "lineage_error"
    unexpected_runtime_error = "unexpected_runtime_error"


class ErrorRecord(BaseModel):
    run_id: str
    error_code: str
    error_category: ErrorCategory
    message: str
    retryable: bool
    context: dict[str, Any] = Field(default_factory=dict)


class PipelineError(Exception):
    def __init__(
        self,
        *,
        message: str,
        error_code: str,
        error_category: ErrorCategory,
        retryable: bool = False,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.error_category = error_category
        self.retryable = retryable
        self.context = context or {}


class ConfigValidationError(PipelineError):
    def __init__(self, *, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(
            message=message,
            error_code="CONFIG_VALIDATION_ERROR",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context=context,
        )


def build_error_record(
    *,
    run_id: str,
    error_code: str,
    error_category: str | ErrorCategory,
    message: str,
    retryable: bool,
    context: dict[str, Any] | None = None,
) -> ErrorRecord:
    category = (
        error_category
        if isinstance(error_category, ErrorCategory)
        else ErrorCategory(error_category)
    )
    return ErrorRecord(
        run_id=run_id,
        error_code=error_code,
        error_category=category,
        message=message,
        retryable=retryable,
        context=context or {},
    )

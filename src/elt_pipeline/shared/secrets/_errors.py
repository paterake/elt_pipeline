from __future__ import annotations

from typing import Any

from elt_pipeline.shared.errors import ErrorCategory, PipelineError

from ._models import SecretScheme, redact_secret


class SecretsError(PipelineError):
    def __init__(
        self,
        *,
        message: str,
        error_code: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            error_category=ErrorCategory.config_error,
            retryable=False,
            context=context,
        )


class SecretRefSyntaxError(SecretsError):
    def __init__(self, *, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(
            message=message,
            error_code="SECRET_REF_SYNTAX_ERROR",
            context=context,
        )


class SecretNotFoundError(SecretsError):
    def __init__(
        self,
        *,
        scheme: SecretScheme,
        path: str,
        message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        msg = message or f"Secret not found for {scheme.value}://{redact_secret(path)}"
        ctx = {"scheme": scheme.value, "path_repr": redact_secret(path)}
        if context:
            ctx.update(context)
        super().__init__(
            message=msg,
            error_code="SECRET_NOT_FOUND",
            context=ctx,
        )


class SecretsNotImplementedError(SecretsError, NotImplementedError):
    def __init__(self, *, scheme: SecretScheme, message: str | None = None) -> None:
        msg = (
            message
            or (
                f"Secrets provider '{scheme.value}://' is not yet implemented in v1 "
                f"(see BACKLOG item G-5 §roadmap).  Use env:// or file:// today, or "
                f"pull forward the corresponding roadmap item to add it."
            )
        )
        super().__init__(
            message=msg,
            error_code="SECRETS_PROVIDER_NOT_IMPLEMENTED",
            context={"scheme": scheme.value},
        )

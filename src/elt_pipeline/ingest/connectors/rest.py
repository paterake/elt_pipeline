from __future__ import annotations

import base64
import json
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError
from elt_pipeline.shared.runtime import RunContext


class RestAuthStrategy(str, Enum):
    none = "none"
    api_key = "api_key"
    basic = "basic"
    bearer_token = "bearer_token"
    client_credentials = "client_credentials"


class RestPaginationMode(str, Enum):
    none = "none"
    page = "page"
    offset = "offset"
    cursor = "cursor"


class RestRequestTemplate(BaseModel):
    method: str = "GET"
    path: str
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, Any] = Field(default_factory=dict)
    body_template: Any = None
    timeout_seconds: float = Field(default=30.0, gt=0)
    retry: "RestRetryPolicy" = Field(default_factory=lambda: RestRetryPolicy())
    payload_format: str = "json"
    artifact_name: str | None = None
    response_items_path: str | None = None

    @field_validator("method")
    @classmethod
    def _normalize_method(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("Request method must not be empty")
        return normalized

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Request path must not be empty")
        return normalized


class RestAuthConfig(BaseModel):
    strategy: RestAuthStrategy = RestAuthStrategy.none
    secret_refs: dict[str, str] = Field(default_factory=dict)
    token_request: RestRequestTemplate | None = None
    token_response_path: str | None = None
    injection_location: str = "header"
    injection_name: str = "Authorization"
    injection_scheme: str = "Bearer"


class RestPaginationConfig(BaseModel):
    mode: RestPaginationMode = RestPaginationMode.none
    page_parameter_name: str = "page"
    offset_parameter_name: str = "offset"
    page_size_parameter_name: str = "page_size"
    cursor_parameter_name: str = "cursor"
    page_size: int | None = Field(default=None, ge=1)
    start_value: int | str | None = None
    max_pages: int | None = Field(default=None, ge=1)
    response_total_count_path: str | None = None
    response_next_cursor_path: str | None = None


class RestRequestWindow(BaseModel):
    start: datetime | None = None
    end: datetime | None = None
    label: str | None = None


class RestRetryPolicy(BaseModel):
    max_attempts: int = Field(default=1, ge=1)
    initial_backoff_seconds: float = Field(default=0.0, ge=0)
    backoff_multiplier: float = Field(default=2.0, ge=1)
    max_backoff_seconds: float | None = Field(default=None, gt=0)
    retryable_status_codes: list[int] = Field(
        default_factory=lambda: [408, 425, 429, 500, 502, 503, 504]
    )
    non_retryable_status_codes: list[int] = Field(default_factory=list)
    success_status_codes: list[int] | None = None

    @field_validator(
        "retryable_status_codes",
        "non_retryable_status_codes",
        "success_status_codes",
        mode="before",
    )
    @classmethod
    def _normalize_status_code_lists(cls, value: Any) -> Any:
        if value is None:
            return None
        return list(value)

    @field_validator(
        "retryable_status_codes",
        "non_retryable_status_codes",
        "success_status_codes",
    )
    @classmethod
    def _validate_status_code_lists(cls, value: list[int] | None) -> list[int] | None:
        if value is None:
            return None
        normalized: list[int] = []
        seen: set[int] = set()
        for status_code in value:
            if not 100 <= status_code <= 599:
                raise ValueError("HTTP status codes must be between 100 and 599")
            if status_code not in seen:
                normalized.append(status_code)
                seen.add(status_code)
        return normalized

    @model_validator(mode="after")
    def _validate_status_code_overrides(self) -> "RestRetryPolicy":
        conflicting_statuses = set(self.retryable_status_codes) & set(
            self.non_retryable_status_codes
        )
        if conflicting_statuses:
            raise ValueError(
                "retryable_status_codes and non_retryable_status_codes must not overlap"
            )
        return self


class RestResolvedAuth(BaseModel):
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, Any] = Field(default_factory=dict)
    body_fields: dict[str, Any] = Field(default_factory=dict)
    redacted_fields: set[str] = Field(default_factory=set)


class RestPreparedRequest(BaseModel):
    method: str
    url: str
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, Any] = Field(default_factory=dict)
    body: Any = None
    timeout_seconds: float = Field(default=30.0, gt=0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RestResponse(BaseModel):
    status_code: int
    headers: dict[str, str] = Field(default_factory=dict)
    body: bytes | str
    received_at: datetime
    content_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RestConnectorConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    execution_mode: str
    base_url: str
    request: RestRequestTemplate
    auth: RestAuthConfig = Field(default_factory=RestAuthConfig)
    pagination: RestPaginationConfig = Field(default_factory=RestPaginationConfig)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("base_url must not be empty")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return normalized.rstrip("/")

    @model_validator(mode="after")
    def _validate_token_requirements(self) -> RestConnectorConfig:
        if (
            self.auth.strategy == RestAuthStrategy.client_credentials
            and self.auth.token_request is None
        ):
            raise ValueError("client_credentials auth requires token_request configuration")
        return self

    @classmethod
    def from_resolved_entity_config(
        cls,
        resolved_config: ResolvedEntityConfig,
    ) -> RestConnectorConfig:
        if resolved_config.connector_type != "rest":
            raise ConfigValidationError(
                message="Resolved entity config is not a REST connector",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": resolved_config.connector_type,
                },
            )

        extraction = resolved_config.extraction
        request_payload = extraction.get("request") or {
            "method": extraction.get("method", "GET"),
            "path": extraction.get("path") or extraction.get("endpoint"),
            "headers": extraction.get("headers", {}),
            "query_params": extraction.get("query_params", {}),
            "body_template": extraction.get("body"),
            "timeout_seconds": extraction.get("timeout_seconds", 30.0),
            "retry": extraction.get("retry", {}),
            "payload_format": extraction.get("payload_format", "json"),
            "artifact_name": extraction.get("artifact_name"),
            "response_items_path": extraction.get("response_items_path"),
        }
        if not request_payload.get("path"):
            raise ConfigValidationError(
                message="REST extraction config must define request.path or extraction.path",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                },
            )

        base_url = extraction.get("base_url") or resolved_config.settings.get("base_url")
        pagination_payload = extraction.get("pagination") or {}
        auth_payload = resolved_config.auth or {}

        try:
            return cls(
                schema_version=resolved_config.schema_version,
                environment=resolved_config.environment,
                source_name=resolved_config.source_name,
                entity_name=resolved_config.entity_name,
                execution_mode=resolved_config.trigger_mode or "manual",
                base_url=base_url,
                request=RestRequestTemplate.model_validate(request_payload),
                auth=RestAuthConfig.model_validate(auth_payload),
                pagination=RestPaginationConfig.model_validate(pagination_payload),
                persistence=resolved_config.persistence,
                state=resolved_config.state,
                settings=resolved_config.settings,
                raw=resolved_config.raw,
            )
        except ValidationError as exc:
            raise ConfigValidationError(
                message="REST connector configuration validation failed",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc


class RestRunResult(BaseModel):
    manifests: list[Level1ArtifactManifest] = Field(default_factory=list)
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    request_count: int = 0
    response_count: int = 0


class RestConnectorBase(ABC):
    def __init__(self, *, config: RestConnectorConfig, run_context: RunContext) -> None:
        self.config = config
        self.run_context = run_context
        self._active_checkpoint_before: dict[str, Any] | None = None
        self._active_window: RestRequestWindow | None = None

    def validate_config(self) -> RestConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    def resolve_window(self) -> RestRequestWindow | None:
        return None

    def resolve_secret(self, *, secret_name: str, secret_ref: str) -> str:
        return secret_ref

    def resolve_client_credentials_authentication(
        self,
        *,
        auth_config: RestAuthConfig,
        resolved_secrets: Mapping[str, str],
    ) -> RestResolvedAuth:
        token_request_template = auth_config.token_request
        if token_request_template is None:
            raise ConfigValidationError(
                message="client_credentials auth requires token_request configuration",
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "strategy": auth_config.strategy.value,
                },
            )

        template_context = _build_template_context(
            config=self.config,
            run_context=self.run_context,
            checkpoint_before=self._active_checkpoint_before,
            window=self._active_window,
        )
        template_context["secret"] = dict(resolved_secrets)
        template_context["secrets"] = dict(resolved_secrets)

        token_request = RestPreparedRequest(
            method=token_request_template.method,
            url=_build_url(
                self.config.base_url,
                _render_string_template(
                    token_request_template.path,
                    template_context=template_context,
                    source_name=self.config.source_name,
                    entity_name=self.config.entity_name,
                ),
            ),
            headers=_render_headers(
                token_request_template.headers,
                template_context=template_context,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            ),
            query_params=_render_template_value(
                token_request_template.query_params,
                template_context=template_context,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            ),
            body=_render_template_value(
                token_request_template.body_template,
                template_context=template_context,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            ),
            timeout_seconds=token_request_template.timeout_seconds,
            metadata={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "request_kind": "auth_token",
            },
        )
        token_response = self.send_request(token_request)
        token = _extract_access_token(
            response=token_response,
            token_response_path=auth_config.token_response_path,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        scheme = auth_config.injection_scheme.strip()
        auth_value = f"{scheme} {token}".strip() if scheme else token
        return _inject_auth_value(
            location=auth_config.injection_location,
            name=auth_config.injection_name,
            value=auth_value,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )

    def resolve_authentication(self) -> RestResolvedAuth:
        auth_config = self.config.auth
        if auth_config.strategy == RestAuthStrategy.none:
            return RestResolvedAuth()

        resolved_secrets = {
            secret_name: self.resolve_secret(secret_name=secret_name, secret_ref=secret_ref)
            for secret_name, secret_ref in auth_config.secret_refs.items()
        }

        if auth_config.strategy == RestAuthStrategy.bearer_token:
            token = _require_secret(
                resolved_secrets,
                preferred_names=("token", "bearer_token"),
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
                strategy=auth_config.strategy,
                fallback_to_single_secret=True,
            )
            scheme = auth_config.injection_scheme.strip()
            auth_value = f"{scheme} {token}".strip() if scheme else token
            return _inject_auth_value(
                location=auth_config.injection_location,
                name=auth_config.injection_name,
                value=auth_value,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            )

        if auth_config.strategy == RestAuthStrategy.basic:
            username = _require_secret(
                resolved_secrets,
                preferred_names=("username", "user"),
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
                strategy=auth_config.strategy,
            )
            password = _require_secret(
                resolved_secrets,
                preferred_names=("password", "pass"),
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
                strategy=auth_config.strategy,
            )
            credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            return _inject_auth_value(
                location=auth_config.injection_location,
                name=auth_config.injection_name,
                value=f"Basic {credentials}",
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            )

        if auth_config.strategy == RestAuthStrategy.api_key:
            api_key = _require_secret(
                resolved_secrets,
                preferred_names=("api_key", "key", "token"),
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
                strategy=auth_config.strategy,
                fallback_to_single_secret=True,
            )
            return _inject_auth_value(
                location=auth_config.injection_location,
                name=auth_config.injection_name,
                value=api_key,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
            )

        if auth_config.strategy == RestAuthStrategy.client_credentials:
            return self.resolve_client_credentials_authentication(
                auth_config=auth_config,
                resolved_secrets=resolved_secrets,
            )

        raise ConfigValidationError(
            message="REST auth strategy is not supported",
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "strategy": auth_config.strategy.value,
            },
        )

    def build_request_plan(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        window: RestRequestWindow | None,
        auth: RestResolvedAuth,
    ) -> list[RestPreparedRequest]:
        request = self.config.request
        template_context = _build_template_context(
            config=self.config,
            run_context=self.run_context,
            checkpoint_before=checkpoint_before,
            window=window,
        )
        headers = _render_headers(
            {**request.headers, **auth.headers},
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        query_params = _render_template_value(
            {**request.query_params, **auth.query_params},
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        body = _render_template_value(
            _merge_body_fields(request.body_template, auth.body_fields),
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        path = _render_string_template(
            request.path,
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return [
            RestPreparedRequest(
                method=request.method,
                url=_build_url(self.config.base_url, path),
                headers=headers,
                query_params=query_params,
                body=body,
                timeout_seconds=request.timeout_seconds,
                metadata={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "window_label": window.label if window else None,
                    "checkpoint_before": checkpoint_before,
                    "pagination_mode": self.config.pagination.mode.value,
                    "template_context": template_context,
                },
            )
        ]

    def send_request(self, request: RestPreparedRequest) -> RestResponse:
        retry_policy = self.config.request.retry
        last_error: PipelineError | None = None
        for attempt in range(1, retry_policy.max_attempts + 1):
            attempt_request = request.model_copy(
                update={
                    "metadata": {
                        **request.metadata,
                        "attempt": attempt,
                        "max_attempts": retry_policy.max_attempts,
                    }
                }
            )
            try:
                response = self.execute_request(attempt_request)
            except TimeoutError as exc:
                last_error = self.classify_timeout_error(
                    request=attempt_request,
                    attempt=attempt,
                    max_attempts=retry_policy.max_attempts,
                    error=exc,
                )
                if self._should_retry(last_error, attempt, retry_policy.max_attempts):
                    self.sleep_before_retry(self._compute_backoff_seconds(retry_policy, attempt))
                    continue
                raise last_error from exc
            except PipelineError as exc:
                last_error = exc
                if self._should_retry(last_error, attempt, retry_policy.max_attempts):
                    self.sleep_before_retry(self._compute_backoff_seconds(retry_policy, attempt))
                    continue
                raise
            except Exception as exc:
                last_error = self.classify_runtime_error(
                    request=attempt_request,
                    attempt=attempt,
                    max_attempts=retry_policy.max_attempts,
                    error=exc,
                )
                if self._should_retry(last_error, attempt, retry_policy.max_attempts):
                    self.sleep_before_retry(self._compute_backoff_seconds(retry_policy, attempt))
                    continue
                raise last_error from exc

            try:
                self.raise_for_response_status(
                    request=attempt_request,
                    response=response,
                    attempt=attempt,
                    max_attempts=retry_policy.max_attempts,
                )
            except PipelineError as exc:
                last_error = exc
                if self._should_retry(last_error, attempt, retry_policy.max_attempts):
                    self.sleep_before_retry(self._compute_backoff_seconds(retry_policy, attempt))
                    continue
                raise

            response.metadata = {
                **response.metadata,
                "attempt": attempt,
                "max_attempts": retry_policy.max_attempts,
            }
            return response

        if last_error is None:
            raise PipelineError(
                message="REST request execution failed before a response was returned",
                error_code="REST_REQUEST_FAILED",
                error_category=ErrorCategory.unexpected_runtime_error,
                retryable=False,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "method": request.method,
                    "url": request.url,
                },
            )
        raise last_error

    def classify_timeout_error(
        self,
        *,
        request: RestPreparedRequest,
        attempt: int,
        max_attempts: int,
        error: TimeoutError,
    ) -> PipelineError:
        return PipelineError(
            message=f"REST request timed out after {request.timeout_seconds} seconds",
            error_code="REST_REQUEST_TIMEOUT",
            error_category=ErrorCategory.processing_error,
            retryable=True,
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "method": request.method,
                "url": request.url,
                "timeout_seconds": request.timeout_seconds,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "error_type": type(error).__name__,
            },
        )

    def classify_runtime_error(
        self,
        *,
        request: RestPreparedRequest,
        attempt: int,
        max_attempts: int,
        error: Exception,
    ) -> PipelineError:
        return PipelineError(
            message=f"REST request execution failed: {error}",
            error_code="REST_REQUEST_FAILED",
            error_category=ErrorCategory.unexpected_runtime_error,
            retryable=False,
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "method": request.method,
                "url": request.url,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "error_type": type(error).__name__,
            },
        )

    def raise_for_response_status(
        self,
        *,
        request: RestPreparedRequest,
        response: RestResponse,
        attempt: int,
        max_attempts: int,
    ) -> None:
        retry_policy = self.config.request.retry
        success_status_codes = set(retry_policy.success_status_codes or [])
        if success_status_codes:
            is_success = response.status_code in success_status_codes
        else:
            is_success = 200 <= response.status_code < 300
        if is_success:
            return

        retryable_status_codes = set(retry_policy.retryable_status_codes)
        non_retryable_status_codes = set(retry_policy.non_retryable_status_codes)
        retryable = response.status_code in retryable_status_codes or (
            response.status_code >= 500 and response.status_code not in non_retryable_status_codes
        )
        if response.status_code in non_retryable_status_codes:
            retryable = False

        raise PipelineError(
            message=f"REST request returned unexpected HTTP status {response.status_code}",
            error_code=(
                "REST_HTTP_STATUS_RETRYABLE"
                if retryable
                else "REST_HTTP_STATUS_ERROR"
            ),
            error_category=_status_error_category(response.status_code),
            retryable=retryable,
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "method": request.method,
                "url": request.url,
                "status_code": response.status_code,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "content_type": response.content_type,
            },
        )

    def sleep_before_retry(self, delay_seconds: float) -> None:
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    @staticmethod
    def _compute_backoff_seconds(retry_policy: RestRetryPolicy, attempt: int) -> float:
        if attempt <= 0 or retry_policy.initial_backoff_seconds <= 0:
            return 0.0
        delay_seconds = retry_policy.initial_backoff_seconds * (
            retry_policy.backoff_multiplier ** (attempt - 1)
        )
        if retry_policy.max_backoff_seconds is not None:
            delay_seconds = min(delay_seconds, retry_policy.max_backoff_seconds)
        return delay_seconds

    @staticmethod
    def _should_retry(error: PipelineError, attempt: int, max_attempts: int) -> bool:
        return error.retryable and attempt < max_attempts

    @abstractmethod
    def execute_request(self, request: RestPreparedRequest) -> RestResponse:
        """Execute a prepared REST request and return the raw response."""

    @abstractmethod
    def persist_response(
        self,
        *,
        request: RestPreparedRequest,
        response: RestResponse,
        checkpoint_before: dict[str, Any] | None,
        window: RestRequestWindow | None,
    ) -> list[Level1ArtifactManifest]:
        """Persist a response into level1 and return all written manifests."""

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        requests: list[RestPreparedRequest],
        responses: list[RestResponse],
        manifests: list[Level1ArtifactManifest],
        window: RestRequestWindow | None,
    ) -> dict[str, Any] | None:
        return checkpoint_before

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
        window: RestRequestWindow | None,
    ) -> None:
        return None

    def run(self) -> RestRunResult:
        self.validate_config()
        checkpoint_before = self.resolve_checkpoint_before()
        window = self.resolve_window()
        self._active_checkpoint_before = checkpoint_before
        self._active_window = window
        try:
            auth = self.resolve_authentication()
            requests = self.build_request_plan(
                checkpoint_before=checkpoint_before,
                window=window,
                auth=auth,
            )

            manifests: list[Level1ArtifactManifest] = []
            responses: list[RestResponse] = []
            for request in requests:
                response = self.send_request(request)
                responses.append(response)
                manifests.extend(
                    self.persist_response(
                        request=request,
                        response=response,
                        checkpoint_before=checkpoint_before,
                        window=window,
                    )
                )
        finally:
            self._active_checkpoint_before = None
            self._active_window = None

        checkpoint_after = self.build_checkpoint_after(
            checkpoint_before=checkpoint_before,
            requests=requests,
            responses=responses,
            manifests=manifests,
            window=window,
        )
        self.update_checkpoint(
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            manifests=manifests,
            window=window,
        )

        return RestRunResult(
            manifests=manifests,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            request_count=len(requests),
            response_count=len(responses),
        )


def _build_url(base_url: str, path: str) -> str:
    if path.startswith(("http://", "https://")):
        return path
    if path.startswith("/"):
        return f"{base_url}{path}"
    return f"{base_url}/{path}"


def _merge_body_fields(body_template: Any, auth_body_fields: dict[str, Any]) -> Any:
    if not auth_body_fields:
        return body_template
    if body_template is None:
        return dict(auth_body_fields)
    if isinstance(body_template, dict):
        return {**body_template, **auth_body_fields}
    return body_template


_TEMPLATE_PATTERN = re.compile(r"\{([a-zA-Z0-9_.]+)\}")


def _build_template_context(
    *,
    config: RestConnectorConfig,
    run_context: RunContext,
    checkpoint_before: dict[str, Any] | None,
    window: RestRequestWindow | None,
) -> dict[str, Any]:
    window_start = window.start if window else None
    window_end = window.end if window else None
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
            "base_url": config.base_url,
        },
        "window": {
            "start": _serialize_template_scalar(window_start),
            "end": _serialize_template_scalar(window_end),
            "start_date": window_start.date().isoformat() if window_start else None,
            "end_date": window_end.date().isoformat() if window_end else None,
            "label": window.label if window else None,
        },
        "checkpoint": checkpoint_before or {},
    }


def _render_headers(
    value: Mapping[str, Any],
    *,
    template_context: dict[str, Any],
    source_name: str,
    entity_name: str,
) -> dict[str, str]:
    rendered_headers: dict[str, str] = {}
    for key, raw_value in value.items():
        rendered_value = _render_template_value(
            raw_value,
            template_context=template_context,
            source_name=source_name,
            entity_name=entity_name,
        )
        if rendered_value is None:
            continue
        rendered_headers[key] = str(rendered_value)
    return rendered_headers


def _render_template_value(
    value: Any,
    *,
    template_context: dict[str, Any],
    source_name: str,
    entity_name: str,
) -> Any:
    if isinstance(value, str):
        return _render_string_template(
            value,
            template_context=template_context,
            source_name=source_name,
            entity_name=entity_name,
        )
    if isinstance(value, Mapping):
        return {
            key: _render_template_value(
                child_value,
                template_context=template_context,
                source_name=source_name,
                entity_name=entity_name,
            )
            for key, child_value in value.items()
        }
    if isinstance(value, list):
        return [
            _render_template_value(
                child_value,
                template_context=template_context,
                source_name=source_name,
                entity_name=entity_name,
            )
            for child_value in value
        ]
    return value


def _render_string_template(
    value: str,
    *,
    template_context: dict[str, Any],
    source_name: str,
    entity_name: str,
) -> Any:
    match = _TEMPLATE_PATTERN.fullmatch(value)
    if match:
        resolved = _resolve_template_token(
            match.group(1),
            template_context=template_context,
            source_name=source_name,
            entity_name=entity_name,
        )
        return _serialize_template_scalar(resolved)

    def replace_token(match: re.Match[str]) -> str:
        resolved = _resolve_template_token(
            match.group(1),
            template_context=template_context,
            source_name=source_name,
            entity_name=entity_name,
        )
        serialized = _serialize_template_scalar(resolved)
        return "" if serialized is None else str(serialized)

    return _TEMPLATE_PATTERN.sub(replace_token, value)


def _resolve_template_token(
    token: str,
    *,
    template_context: dict[str, Any],
    source_name: str,
    entity_name: str,
) -> Any:
    current: Any = template_context
    for part in token.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        raise ConfigValidationError(
            message="REST request template references an unknown token",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "token": token,
            },
        )
    return current


def _serialize_template_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _inject_auth_value(
    *,
    location: str,
    name: str,
    value: Any,
    source_name: str,
    entity_name: str,
) -> RestResolvedAuth:
    normalized_location = location.strip().lower()
    normalized_name = name.strip()
    if not normalized_name:
        raise ConfigValidationError(
            message="REST auth injection_name must not be empty",
            context={"source_name": source_name, "entity_name": entity_name},
        )

    if normalized_location == "header":
        return RestResolvedAuth(
            headers={normalized_name: str(value)},
            redacted_fields={f"headers.{normalized_name}"},
        )
    if normalized_location == "query_param":
        return RestResolvedAuth(
            query_params={normalized_name: value},
            redacted_fields={f"query_params.{normalized_name}"},
        )
    if normalized_location == "body_field":
        return RestResolvedAuth(
            body_fields={normalized_name: value},
            redacted_fields={f"body.{normalized_name}"},
        )

    raise ConfigValidationError(
        message="REST auth injection_location is invalid",
        context={
            "source_name": source_name,
            "entity_name": entity_name,
            "injection_location": location,
        },
    )


def _require_secret(
    resolved_secrets: Mapping[str, str],
    *,
    preferred_names: tuple[str, ...],
    source_name: str,
    entity_name: str,
    strategy: RestAuthStrategy,
    fallback_to_single_secret: bool = False,
) -> str:
    for secret_name in preferred_names:
        value = resolved_secrets.get(secret_name)
        if value:
            return value

    if fallback_to_single_secret and len(resolved_secrets) == 1:
        return next(iter(resolved_secrets.values()))

    raise ConfigValidationError(
        message="REST auth strategy is missing a required secret reference",
        context={
            "source_name": source_name,
            "entity_name": entity_name,
            "strategy": strategy.value,
            "required_secret_names": list(preferred_names),
            "configured_secret_names": sorted(resolved_secrets),
        },
    )


def _extract_access_token(
    *,
    response: RestResponse,
    token_response_path: str | None,
    source_name: str,
    entity_name: str,
) -> str:
    response_document = _parse_json_response_body(
        response=response,
        source_name=source_name,
        entity_name=entity_name,
    )
    token_value = _resolve_response_path(
        response_document,
        path=(token_response_path or "access_token"),
        source_name=source_name,
        entity_name=entity_name,
    )
    if token_value is None or isinstance(token_value, Mapping | list):
        raise ConfigValidationError(
            message="REST auth token response path must resolve to a scalar value",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "token_response_path": token_response_path or "access_token",
            },
        )
    return str(token_value)


def _parse_json_response_body(
    *,
    response: RestResponse,
    source_name: str,
    entity_name: str,
) -> Any:
    raw_body = response.body.decode("utf-8") if isinstance(response.body, bytes) else response.body
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message="REST auth token response body is not valid JSON",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "status_code": response.status_code,
                "content_type": response.content_type,
            },
        ) from exc


def _resolve_response_path(
    response_document: Any,
    *,
    path: str,
    source_name: str,
    entity_name: str,
) -> Any:
    normalized_path = path.strip()
    if not normalized_path:
        raise ConfigValidationError(
            message="REST auth token_response_path must not be empty",
            context={"source_name": source_name, "entity_name": entity_name},
        )

    current = response_document
    for part in normalized_path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
                continue
        raise ConfigValidationError(
            message="REST auth token response path did not resolve to a value",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "token_response_path": normalized_path,
            },
        )
    return current


def _status_error_category(status_code: int) -> ErrorCategory:
    if 400 <= status_code < 500:
        return ErrorCategory.input_contract_error
    return ErrorCategory.processing_error


__all__ = [
    "RestAuthConfig",
    "RestAuthStrategy",
    "RestConnectorBase",
    "RestConnectorConfig",
    "RestPaginationConfig",
    "RestPaginationMode",
    "RestPreparedRequest",
    "RestRetryPolicy",
    "RestRequestTemplate",
    "RestRequestWindow",
    "RestResolvedAuth",
    "RestResponse",
    "RestRunResult",
]

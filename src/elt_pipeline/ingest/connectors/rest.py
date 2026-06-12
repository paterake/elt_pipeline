from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError
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

    def validate_config(self) -> RestConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    def resolve_window(self) -> RestRequestWindow | None:
        return None

    def resolve_authentication(self) -> RestResolvedAuth:
        return RestResolvedAuth()

    def build_request_plan(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        window: RestRequestWindow | None,
        auth: RestResolvedAuth,
    ) -> list[RestPreparedRequest]:
        request = self.config.request
        return [
            RestPreparedRequest(
                method=request.method,
                url=_build_url(self.config.base_url, request.path),
                headers={**request.headers, **auth.headers},
                query_params={**request.query_params, **auth.query_params},
                body=_merge_body_fields(request.body_template, auth.body_fields),
                timeout_seconds=request.timeout_seconds,
                metadata={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "window_label": window.label if window else None,
                    "checkpoint_before": checkpoint_before,
                    "pagination_mode": self.config.pagination.mode.value,
                },
            )
        ]

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
        auth = self.resolve_authentication()
        requests = self.build_request_plan(
            checkpoint_before=checkpoint_before,
            window=window,
            auth=auth,
        )

        manifests: list[Level1ArtifactManifest] = []
        responses: list[RestResponse] = []
        for request in requests:
            response = self.execute_request(request)
            responses.append(response)
            manifests.extend(
                self.persist_response(
                    request=request,
                    response=response,
                    checkpoint_before=checkpoint_before,
                    window=window,
                )
            )

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


__all__ = [
    "RestAuthConfig",
    "RestAuthStrategy",
    "RestConnectorBase",
    "RestConnectorConfig",
    "RestPaginationConfig",
    "RestPaginationMode",
    "RestPreparedRequest",
    "RestRequestTemplate",
    "RestRequestWindow",
    "RestResolvedAuth",
    "RestResponse",
    "RestRunResult",
]

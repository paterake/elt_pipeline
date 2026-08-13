from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.lineage import LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext

_LINEAGE_BACKEND_ENV = "ELT_PIPELINE_LINEAGE_BACKEND"
_LINEAGE_URL_ENV = "ELT_PIPELINE_LINEAGE_URL"
_LINEAGE_POLICY_ENV = "ELT_PIPELINE_LINEAGE_POLICY"
_LINEAGE_TIMEOUT_ENV = "ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS"
_LINEAGE_AUTH_HEADER_ENV = "ELT_PIPELINE_LINEAGE_AUTH_HEADER"


class LineageEmissionPolicy(str, Enum):
    best_effort = "best_effort"
    blocking = "blocking"


class LineageRemoteEmitter(Protocol):
    backend_type: str

    def emit(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> None: ...


@dataclass(frozen=True)
class _OpenLineageHttpBackendConfig:
    endpoint_url: str
    emission_policy: LineageEmissionPolicy
    timeout_seconds: float
    auth_header: str | None = None


class OpenLineageHttpEmitter:
    backend_type = "openlineage_http"

    def __init__(
        self,
        *,
        endpoint_url: str,
        timeout_seconds: float = 10.0,
        auth_header: str | None = None,
    ) -> None:
        self._endpoint_url = _validate_lineage_endpoint_url(
            endpoint_url=endpoint_url,
            backend_type=self.backend_type,
        )
        self._timeout_seconds = _validate_lineage_timeout_seconds(
            timeout_seconds=timeout_seconds,
            backend_type=self.backend_type,
        )
        self._auth_header = _normalize_auth_header(
            auth_header=auth_header,
            backend_type=self.backend_type,
        )

    def emit(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "elt_pipeline/openlineage_http",
        }
        if self._auth_header is not None:
            headers["Authorization"] = self._auth_header

        payload = json.dumps(lineage_event.model_dump(mode="json")).encode("utf-8")
        request = Request(
            self._endpoint_url,
            data=payload,
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                response.read()
        except HTTPError as exc:
            raise PipelineError(
                message="OpenLineage backend request failed",
                error_code="LINEAGE_BACKEND_EMISSION_FAILED",
                error_category=ErrorCategory.lineage_error,
                retryable=500 <= exc.code < 600,
                context={
                    "backend_type": self.backend_type,
                    "endpoint_url": self._endpoint_url,
                    "status_code": exc.code,
                    "event_type": lineage_event.event_type,
                    "job_name": run_context.job_name,
                    "environment": environment,
                },
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", str(exc))
            raise PipelineError(
                message="OpenLineage backend request failed",
                error_code="LINEAGE_BACKEND_EMISSION_FAILED",
                error_category=ErrorCategory.lineage_error,
                retryable=True,
                context={
                    "backend_type": self.backend_type,
                    "endpoint_url": self._endpoint_url,
                    "reason": str(reason),
                    "event_type": lineage_event.event_type,
                    "job_name": run_context.job_name,
                    "environment": environment,
                },
            ) from exc


class LineageAdapter:
    def __init__(
        self,
        *,
        artifact_store: LocalArtifactStore,
        remote_emitter: LineageRemoteEmitter | None = None,
        emission_policy: LineageEmissionPolicy = LineageEmissionPolicy.best_effort,
    ) -> None:
        self._artifact_store = artifact_store
        self._remote_emitter = remote_emitter
        self._emission_policy = emission_policy

    def emit(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> str:
        local_path = self._artifact_store.append_lineage_event(
            run_context=run_context,
            environment=environment,
            lineage_event=lineage_event,
        )
        if self._remote_emitter is None:
            return local_path

        try:
            self._remote_emitter.emit(
                run_context=run_context,
                environment=environment,
                lineage_event=lineage_event,
            )
        except PipelineError as exc:
            self._record_remote_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._remote_emitter.backend_type,
                error=exc,
            )
            if self._emission_policy is LineageEmissionPolicy.blocking:
                raise
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional lineage backend emission failed",
                error_code="LINEAGE_BACKEND_EMISSION_FAILED",
                error_category=ErrorCategory.lineage_error,
                retryable=True,
                context={
                    "backend_type": self._remote_emitter.backend_type,
                    "event_type": lineage_event.event_type,
                    "job_name": run_context.job_name,
                },
            )
            self._record_remote_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._remote_emitter.backend_type,
                error=pipeline_error,
            )
            if self._emission_policy is LineageEmissionPolicy.blocking:
                raise pipeline_error from exc

        return local_path

    def _record_remote_failure(
        self,
        *,
        run_context: RunContext,
        environment: str,
        backend_type: str,
        error: PipelineError,
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
                    "ERROR"
                    if self._emission_policy is LineageEmissionPolicy.blocking
                    else "WARNING"
                ),
                component="lineage",
                event_type="lineage_remote_emit_failed",
                message="Optional lineage backend emission failed",
                details={
                    "backend_type": backend_type,
                    "error_code": error.error_code,
                    "error_category": error.error_category.value,
                    "blocking": self._emission_policy is LineageEmissionPolicy.blocking,
                },
            ),
        )


def build_lineage_adapter(
    root_path: str,
    *,
    remote_emitter: LineageRemoteEmitter | None = None,
    emission_policy: LineageEmissionPolicy | None = None,
) -> LineageAdapter:
    configured_remote_emitter = remote_emitter
    configured_emission_policy = emission_policy

    if configured_remote_emitter is None:
        backend_config = _load_openlineage_http_backend_config_from_env()
        if backend_config is not None:
            configured_remote_emitter = OpenLineageHttpEmitter(
                endpoint_url=backend_config.endpoint_url,
                timeout_seconds=backend_config.timeout_seconds,
                auth_header=backend_config.auth_header,
            )
            configured_emission_policy = (
                configured_emission_policy or backend_config.emission_policy
            )

    return LineageAdapter(
        artifact_store=LocalArtifactStore(root_path),
        remote_emitter=configured_remote_emitter,
        emission_policy=configured_emission_policy or LineageEmissionPolicy.best_effort,
    )


def _load_openlineage_http_backend_config_from_env() -> (
    _OpenLineageHttpBackendConfig | None
):
    raw_backend_type = os.getenv(_LINEAGE_BACKEND_ENV)
    if raw_backend_type is None or not raw_backend_type.strip():
        return None

    backend_type = raw_backend_type.strip().lower()
    if backend_type != OpenLineageHttpEmitter.backend_type:
        raise ConfigValidationError(
            message="Unsupported lineage backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": [OpenLineageHttpEmitter.backend_type],
            },
        )

    endpoint_url = _validate_lineage_endpoint_url(
        endpoint_url=_require_lineage_env_value(_LINEAGE_URL_ENV),
        backend_type=backend_type,
    )

    raw_policy = os.getenv(_LINEAGE_POLICY_ENV, LineageEmissionPolicy.best_effort.value)
    try:
        emission_policy = LineageEmissionPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message="Lineage emission policy is invalid",
            context={
                "backend_type": backend_type,
                "emission_policy": raw_policy,
                "supported_values": [policy.value for policy in LineageEmissionPolicy],
            },
        ) from exc

    raw_timeout = os.getenv(_LINEAGE_TIMEOUT_ENV, "10")
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError as exc:
        raise ConfigValidationError(
            message="Lineage backend timeout must be numeric",
            context={
                "backend_type": backend_type,
                "timeout_seconds": raw_timeout,
            },
        ) from exc
    timeout_seconds = _validate_lineage_timeout_seconds(
        timeout_seconds=timeout_seconds,
        backend_type=backend_type,
    )

    auth_header = _normalize_auth_header(
        auth_header=os.getenv(_LINEAGE_AUTH_HEADER_ENV),
        backend_type=backend_type,
    )

    return _OpenLineageHttpBackendConfig(
        endpoint_url=endpoint_url,
        emission_policy=emission_policy,
        timeout_seconds=timeout_seconds,
        auth_header=auth_header,
    )


def _require_lineage_env_value(variable_name: str) -> str:
    value = os.getenv(variable_name)
    if value is None or not value.strip():
        raise ConfigValidationError(
            message=f"{variable_name} is required when lineage backend emission is enabled",
            context={"variable_name": variable_name},
        )
    return value.strip()


def _validate_lineage_endpoint_url(*, endpoint_url: str, backend_type: str) -> str:
    normalized_url = endpoint_url.strip()
    parsed_url = urlparse(normalized_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ConfigValidationError(
            message="Lineage backend URL must be a valid http or https URL",
            context={
                "backend_type": backend_type,
                "endpoint_url": endpoint_url,
            },
        )
    return normalized_url


def _validate_lineage_timeout_seconds(*, timeout_seconds: float, backend_type: str) -> float:
    if timeout_seconds <= 0:
        raise ConfigValidationError(
            message="Lineage backend timeout must be greater than zero",
            context={
                "backend_type": backend_type,
                "timeout_seconds": timeout_seconds,
            },
        )
    return timeout_seconds


def _normalize_auth_header(*, auth_header: str | None, backend_type: str) -> str | None:
    if auth_header is None:
        return None
    normalized_header = auth_header.strip()
    if not normalized_header:
        raise ConfigValidationError(
            message="Lineage auth header must not be empty when configured",
            context={"backend_type": backend_type},
        )
    return normalized_header

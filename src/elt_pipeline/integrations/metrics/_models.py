from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Protocol
from urllib.parse import urlparse

from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.observability import (
    AlertEvent,
    MetricPoint,
    TraceSpan,
)
from elt_pipeline.shared.runtime import RunContext

_OBSERVABILITY_BACKEND_SUPPORTED = {
    "metrics": ["prometheus_remote_write"],
    "tracing": ["otlp_http"],
    "alerts": ["webhook"],
}


class ObservabilityPolicy(str, Enum):
    best_effort = "best_effort"
    blocking = "blocking"


class MetricsExporter(Protocol):
    backend_type: str

    def export_metrics(
        self,
        *,
        run_context: RunContext,
        environment: str,
        metrics: list[MetricPoint],
    ) -> None: ...


class TraceExporter(Protocol):
    backend_type: str

    def export_traces(
        self,
        *,
        run_context: RunContext,
        environment: str,
        spans: list[TraceSpan],
    ) -> None: ...


class AlertHook(Protocol):
    backend_type: str

    def trigger_alert(
        self,
        *,
        run_context: RunContext,
        environment: str,
        event: AlertEvent,
    ) -> None: ...


@dataclass(frozen=True)
class _PrometheusRemoteWriteConfig:
    endpoint_url: str
    emission_policy: ObservabilityPolicy
    timeout_seconds: float
    auth_header: str | None = None


@dataclass(frozen=True)
class _OtlpHttpTraceConfig:
    endpoint_url: str
    emission_policy: ObservabilityPolicy
    timeout_seconds: float
    auth_header: str | None = None


@dataclass(frozen=True)
class _WebhookAlertConfig:
    endpoint_url: str
    emission_policy: ObservabilityPolicy
    timeout_seconds: float
    auth_header: str | None = None


def _validate_endpoint_url(
    *, endpoint_url: str, backend_type: str, subsystem: str
) -> str:
    normalized_url = endpoint_url.strip()
    parsed_url = urlparse(normalized_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ConfigValidationError(
            message=f"Observability {subsystem} backend URL must be a valid http or https URL",
            context={
                "backend_type": backend_type,
                "endpoint_url": endpoint_url,
                "subsystem": subsystem,
            },
        )
    return normalized_url


def _validate_timeout_seconds(*, timeout_seconds: float, backend_type: str) -> float:
    if timeout_seconds <= 0:
        raise ConfigValidationError(
            message="Observability backend timeout must be greater than zero",
            context={
                "backend_type": backend_type,
                "timeout_seconds": timeout_seconds,
            },
        )
    return timeout_seconds


def _normalize_auth_header(
    *, auth_header: str | None, backend_type: str
) -> str | None:
    if auth_header is None:
        return None
    normalized_header = auth_header.strip()
    if not normalized_header:
        raise ConfigValidationError(
            message="Observability auth header must not be empty when configured",
            context={"backend_type": backend_type},
        )
    return normalized_header


def _sanitize_metric_name(name: str) -> str:
    sanitized = "".join(
        ch if ch.isalnum() or ch == "_" else "_" for ch in name
    )
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


def _derive_trace_id(run_id: str) -> str:
    import hashlib

    return hashlib.sha256(f"trace:{run_id}".encode("utf-8")).hexdigest()[:32]


def _derive_span_id(run_id: str, stage: str) -> str:
    import hashlib

    return hashlib.sha256(f"span:{run_id}:{stage}".encode("utf-8")).hexdigest()[:16]


def _require_env_value(variable_name: str, subsystem: str) -> str:
    value = os.getenv(variable_name)
    if value is None or not value.strip():
        raise ConfigValidationError(
            message=(
                f"{variable_name} is required when observability {subsystem} "
                "backend emission is enabled"
            ),
            context={"variable_name": variable_name, "subsystem": subsystem},
        )
    return value.strip()

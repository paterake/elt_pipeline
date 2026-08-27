from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from elt_pipeline.integrations.metrics._models import (
    _normalize_auth_header,
    _validate_endpoint_url,
    _validate_timeout_seconds,
)
from elt_pipeline.shared.errors import (
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.observability import (
    AlertEvent,
    MetricPoint,
    SpanStatus,
    TraceSpan,
)
from elt_pipeline.shared.runtime import RunContext


def _post_json(
    *,
    endpoint_url: str,
    payload: bytes,
    headers: dict[str, str],
    timeout_seconds: float,
    backend_type: str,
    subsystem: str,
    run_context: RunContext,
    environment: str,
) -> None:
    request = Request(
        endpoint_url,
        data=payload,
        headers=headers,
        method="POST",
    )
    error_code_map = {
        "metrics": "OBSERVABILITY_METRICS_EXPORT_FAILED",
        "tracing": "OBSERVABILITY_TRACING_EXPORT_FAILED",
        "alerts": "OBSERVABILITY_ALERT_HOOK_FAILED",
    }
    error_code = error_code_map.get(
        subsystem, "OBSERVABILITY_EXPORT_FAILED"
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            response.read()
    except HTTPError as exc:
        raise PipelineError(
            message=f"Observability {subsystem} backend request failed",
            error_code=error_code,
            error_category=ErrorCategory.observability_error,
            retryable=500 <= exc.code < 600,
            context={
                "backend_type": backend_type,
                "endpoint_url": endpoint_url,
                "status_code": exc.code,
                "job_name": run_context.job_name,
                "environment": environment,
            },
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", str(exc))
        raise PipelineError(
            message=f"Observability {subsystem} backend request failed",
            error_code=error_code,
            error_category=ErrorCategory.observability_error,
            retryable=True,
            context={
                "backend_type": backend_type,
                "endpoint_url": endpoint_url,
                "reason": str(reason),
                "job_name": run_context.job_name,
                "environment": environment,
            },
        ) from exc


class PrometheusRemoteWriteExporter:
    backend_type = "prometheus_remote_write"

    def __init__(
        self,
        *,
        endpoint_url: str,
        timeout_seconds: float = 10.0,
        auth_header: str | None = None,
    ) -> None:
        self._endpoint_url = _validate_endpoint_url(
            endpoint_url=endpoint_url,
            backend_type=self.backend_type,
            subsystem="metrics",
        )
        self._timeout_seconds = _validate_timeout_seconds(
            timeout_seconds=timeout_seconds,
            backend_type=self.backend_type,
        )
        self._auth_header = _normalize_auth_header(
            auth_header=auth_header,
            backend_type=self.backend_type,
        )

    def export_metrics(
        self,
        *,
        run_context: RunContext,
        environment: str,
        metrics: list[MetricPoint],
    ) -> None:
        if not metrics:
            return
        series_list = []
        for point in metrics:
            labels = {
                "__name__": point.metric_name,
                **point.labels,
            }
            if point.run_id:
                labels.setdefault("run_id", point.run_id)
            if point.stage:
                labels.setdefault("stage", point.stage)
            if point.job_name:
                labels.setdefault("job_name", point.job_name)
            timestamp_ms = int(point.timestamp.timestamp() * 1000)
            series_list.append(
                {
                    "labels": [
                        {"name": name, "value": str(value)}
                        for name, value in sorted(labels.items())
                    ],
                    "samples": [
                        {
                            "value": float(point.value),
                            "timestamp": timestamp_ms,
                        }
                    ],
                }
            )
        payload = json.dumps(
            {"status": "success", "data": {"resultType": "series", "result": series_list}}
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "elt_pipeline/prometheus_remote_write",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        }
        if self._auth_header is not None:
            headers["Authorization"] = self._auth_header
        _post_json(
            endpoint_url=self._endpoint_url,
            payload=payload,
            headers=headers,
            timeout_seconds=self._timeout_seconds,
            backend_type=self.backend_type,
            subsystem="metrics",
            run_context=run_context,
            environment=environment,
        )


class OtlpHttpTraceExporter:
    backend_type = "otlp_http"

    def __init__(
        self,
        *,
        endpoint_url: str,
        timeout_seconds: float = 10.0,
        auth_header: str | None = None,
    ) -> None:
        self._endpoint_url = _validate_endpoint_url(
            endpoint_url=endpoint_url,
            backend_type=self.backend_type,
            subsystem="tracing",
        )
        self._timeout_seconds = _validate_timeout_seconds(
            timeout_seconds=timeout_seconds,
            backend_type=self.backend_type,
        )
        self._auth_header = _normalize_auth_header(
            auth_header=auth_header,
            backend_type=self.backend_type,
        )

    def export_traces(
        self,
        *,
        run_context: RunContext,
        environment: str,
        spans: list[TraceSpan],
    ) -> None:
        if not spans:
            return
        scope_spans = []
        for span in spans:
            start_unix_nano = int(span.start_time.timestamp() * 1e9)
            if span.end_time is not None:
                end_unix_nano = int(span.end_time.timestamp() * 1e9)
            else:
                end_unix_nano = start_unix_nano
            otlp_status_code = {
                SpanStatus.unset: 0,
                SpanStatus.ok: 1,
                SpanStatus.error: 2,
            }[span.status]
            attributes = [
                {"key": k, "value": {"stringValue": str(v)}}
                for k, v in span.attributes.items()
            ]
            if span.run_id:
                attributes.append(
                    {"key": "run_id", "value": {"stringValue": span.run_id}}
                )
            if span.stage:
                attributes.append(
                    {"key": "stage", "value": {"stringValue": span.stage}}
                )
            if span.job_name:
                attributes.append(
                    {"key": "job_name", "value": {"stringValue": span.job_name}}
                )
            otlp_span = {
                "traceId": span.trace_id,
                "spanId": span.span_id,
                "name": span.name,
                "startTimeUnixNano": str(start_unix_nano),
                "endTimeUnixNano": str(end_unix_nano),
                "status": {"code": otlp_status_code},
                "attributes": attributes,
                "events": [
                    {
                        "name": ev.get("name", "event"),
                        "timeUnixNano": str(
                            int(
                                ev.get(
                                    "time",
                                    datetime.now(tz=UTC).timestamp() * 1e9,
                                )
                            )
                        ),
                        "attributes": [
                            {"key": k, "value": {"stringValue": str(v)}}
                            for k, v in ev.items()
                            if k not in {"name", "time"}
                        ],
                    }
                    for ev in span.events
                ],
            }
            if span.parent_span_id:
                otlp_span["parentSpanId"] = span.parent_span_id
            scope_spans.append(otlp_span)
        payload = json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": {"stringValue": "elt_pipeline"},
                                }
                            ]
                        },
                        "scopeSpans": [
                            {
                                "scope": {"name": "elt_pipeline.observability"},
                                "spans": scope_spans,
                            }
                        ],
                    }
                ]
            }
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "elt_pipeline/otlp_http",
        }
        if self._auth_header is not None:
            headers["Authorization"] = self._auth_header
        _post_json(
            endpoint_url=self._endpoint_url,
            payload=payload,
            headers=headers,
            timeout_seconds=self._timeout_seconds,
            backend_type=self.backend_type,
            subsystem="tracing",
            run_context=run_context,
            environment=environment,
        )


class WebhookAlertHook:
    backend_type = "webhook"

    def __init__(
        self,
        *,
        endpoint_url: str,
        timeout_seconds: float = 10.0,
        auth_header: str | None = None,
    ) -> None:
        self._endpoint_url = _validate_endpoint_url(
            endpoint_url=endpoint_url,
            backend_type=self.backend_type,
            subsystem="alerts",
        )
        self._timeout_seconds = _validate_timeout_seconds(
            timeout_seconds=timeout_seconds,
            backend_type=self.backend_type,
        )
        self._auth_header = _normalize_auth_header(
            auth_header=auth_header,
            backend_type=self.backend_type,
        )

    def trigger_alert(
        self,
        *,
        run_context: RunContext,
        environment: str,
        event: AlertEvent,
    ) -> None:
        payload = json.dumps(
            {
                "severity": event.severity.value,
                "message": event.message,
                "labels": event.labels,
                "timestamp": event.timestamp.isoformat(),
                "run_id": event.run_id,
                "stage": event.stage,
                "job_name": event.job_name,
            }
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "elt_pipeline/webhook",
        }
        if self._auth_header is not None:
            headers["Authorization"] = self._auth_header
        _post_json(
            endpoint_url=self._endpoint_url,
            payload=payload,
            headers=headers,
            timeout_seconds=self._timeout_seconds,
            backend_type=self.backend_type,
            subsystem="alerts",
            run_context=run_context,
            environment=environment,
        )

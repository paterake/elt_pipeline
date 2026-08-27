from __future__ import annotations

import os

from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations.metrics._exporters import (
    OtlpHttpTraceExporter,
    PrometheusRemoteWriteExporter,
    WebhookAlertHook,
)
from elt_pipeline.integrations.metrics._models import (
    _OBSERVABILITY_BACKEND_SUPPORTED,
    AlertHook,
    MetricsExporter,
    ObservabilityPolicy,
    TraceExporter,
    _derive_span_id,
    _derive_trace_id,
    _normalize_auth_header,
    _OtlpHttpTraceConfig,
    _PrometheusRemoteWriteConfig,
    _require_env_value,
    _sanitize_metric_name,
    _validate_endpoint_url,
    _validate_timeout_seconds,
    _WebhookAlertConfig,
)
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.observability import (
    AlertEvent,
    AlertSeverity,
    MetricPoint,
    MetricType,
    SpanStatus,
    TraceSpan,
)
from elt_pipeline.shared.runtime import RunContext


class ObservabilityAdapter:
    def __init__(
        self,
        *,
        artifact_store: LocalArtifactStore,
        metrics_exporter: MetricsExporter | None = None,
        trace_exporter: TraceExporter | None = None,
        alert_hook: AlertHook | None = None,
        metrics_policy: ObservabilityPolicy = ObservabilityPolicy.best_effort,
        tracing_policy: ObservabilityPolicy = ObservabilityPolicy.best_effort,
        alerts_policy: ObservabilityPolicy = ObservabilityPolicy.best_effort,
    ) -> None:
        self._artifact_store = artifact_store
        self._metrics_exporter = metrics_exporter
        self._trace_exporter = trace_exporter
        self._alert_hook = alert_hook
        self._metrics_policy = metrics_policy
        self._tracing_policy = tracing_policy
        self._alerts_policy = alerts_policy

    def record_metrics(
        self,
        *,
        run_context: RunContext,
        environment: str,
        metrics: list[MetricPoint],
    ) -> list[str]:
        local_paths: list[str] = []
        for point in metrics:
            local_paths.append(
                self._artifact_store.append_metrics_point(
                    run_context=run_context,
                    environment=environment,
                    metrics_point=point,
                )
            )
        if self._metrics_exporter is None or not metrics:
            return local_paths
        try:
            self._metrics_exporter.export_metrics(
                run_context=run_context,
                environment=environment,
                metrics=metrics,
            )
        except PipelineError as exc:
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="metrics",
                backend_type=self._metrics_exporter.backend_type,
                policy=self._metrics_policy,
                error=exc,
            )
            if self._metrics_policy is ObservabilityPolicy.blocking:
                raise
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional metrics export failed",
                error_code="OBSERVABILITY_METRICS_EXPORT_FAILED",
                error_category=ErrorCategory.observability_error,
                retryable=True,
                context={
                    "backend_type": self._metrics_exporter.backend_type,
                    "metric_count": len(metrics),
                    "job_name": run_context.job_name,
                },
            )
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="metrics",
                backend_type=self._metrics_exporter.backend_type,
                policy=self._metrics_policy,
                error=pipeline_error,
            )
            if self._metrics_policy is ObservabilityPolicy.blocking:
                raise pipeline_error from exc
        return local_paths

    def record_traces(
        self,
        *,
        run_context: RunContext,
        environment: str,
        spans: list[TraceSpan],
    ) -> list[str]:
        local_paths: list[str] = []
        for span in spans:
            local_paths.append(
                self._artifact_store.append_trace_span(
                    run_context=run_context,
                    environment=environment,
                    trace_span=span,
                )
            )
        if self._trace_exporter is None or not spans:
            return local_paths
        try:
            self._trace_exporter.export_traces(
                run_context=run_context,
                environment=environment,
                spans=spans,
            )
        except PipelineError as exc:
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="tracing",
                backend_type=self._trace_exporter.backend_type,
                policy=self._tracing_policy,
                error=exc,
            )
            if self._tracing_policy is ObservabilityPolicy.blocking:
                raise
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional trace export failed",
                error_code="OBSERVABILITY_TRACING_EXPORT_FAILED",
                error_category=ErrorCategory.observability_error,
                retryable=True,
                context={
                    "backend_type": self._trace_exporter.backend_type,
                    "span_count": len(spans),
                    "job_name": run_context.job_name,
                },
            )
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="tracing",
                backend_type=self._trace_exporter.backend_type,
                policy=self._tracing_policy,
                error=pipeline_error,
            )
            if self._tracing_policy is ObservabilityPolicy.blocking:
                raise pipeline_error from exc
        return local_paths

    def trigger_alert(
        self,
        *,
        run_context: RunContext,
        environment: str,
        event: AlertEvent,
    ) -> str:
        local_path = self._artifact_store.append_alert_event(
            run_context=run_context,
            environment=environment,
            alert_event=event,
        )
        if self._alert_hook is None:
            return local_path
        try:
            self._alert_hook.trigger_alert(
                run_context=run_context,
                environment=environment,
                event=event,
            )
        except PipelineError as exc:
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="alerts",
                backend_type=self._alert_hook.backend_type,
                policy=self._alerts_policy,
                error=exc,
            )
            if self._alerts_policy is ObservabilityPolicy.blocking:
                raise
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional alert hook failed",
                error_code="OBSERVABILITY_ALERT_HOOK_FAILED",
                error_category=ErrorCategory.observability_error,
                retryable=True,
                context={
                    "backend_type": self._alert_hook.backend_type,
                    "severity": event.severity.value,
                    "job_name": run_context.job_name,
                },
            )
            self._record_emission_failure(
                run_context=run_context,
                environment=environment,
                subsystem="alerts",
                backend_type=self._alert_hook.backend_type,
                policy=self._alerts_policy,
                error=pipeline_error,
            )
            if self._alerts_policy is ObservabilityPolicy.blocking:
                raise pipeline_error from exc
        return local_path

    def on_run_complete(
        self,
        *,
        run_context: RunContext,
        environment: str,
        audit_record: AuditRecord,
    ) -> None:
        base_labels = {
            "stage": audit_record.stage,
            "job_name": audit_record.job_name,
            "environment": environment,
            "trigger_type": audit_record.trigger_type,
            "status": audit_record.status,
        }
        metrics: list[MetricPoint] = []
        metrics_summary: MetricsSummary = audit_record.metrics_summary
        duration_seconds: float | None = None
        if (
            audit_record.completed_at is not None
            and audit_record.started_at is not None
        ):
            duration_seconds = (
                audit_record.completed_at - audit_record.started_at
            ).total_seconds()
            metrics.append(
                MetricPoint(
                    metric_name="elt_run_duration_seconds",
                    metric_type=MetricType.gauge,
                    value=duration_seconds,
                    labels=base_labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                )
            )
        if metrics_summary.records_read is not None:
            metrics.append(
                MetricPoint(
                    metric_name="elt_records_read_total",
                    metric_type=MetricType.counter,
                    value=metrics_summary.records_read,
                    labels=base_labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                )
            )
        if metrics_summary.records_written is not None:
            metrics.append(
                MetricPoint(
                    metric_name="elt_records_written_total",
                    metric_type=MetricType.counter,
                    value=metrics_summary.records_written,
                    labels=base_labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                )
            )
        if metrics_summary.files_written is not None:
            metrics.append(
                MetricPoint(
                    metric_name="elt_files_written_total",
                    metric_type=MetricType.counter,
                    value=metrics_summary.files_written,
                    labels=base_labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                )
            )
        status_value = 1 if audit_record.status == "success" else 0
        metrics.append(
            MetricPoint(
                metric_name="elt_run_status",
                metric_type=MetricType.gauge,
                value=status_value,
                labels=base_labels,
                run_id=audit_record.run_id,
                stage=audit_record.stage,
                job_name=audit_record.job_name,
            )
        )
        for key, raw_value in metrics_summary.extra.items():
            if isinstance(raw_value, (int, float)):
                metrics.append(
                    MetricPoint(
                        metric_name=f"elt_extra_{_sanitize_metric_name(key)}",
                        metric_type=MetricType.gauge,
                        value=raw_value,
                        labels=base_labels,
                        run_id=audit_record.run_id,
                        stage=audit_record.stage,
                        job_name=audit_record.job_name,
                    )
                )
        for vr in audit_record.validation_results:
            status_label = vr.get("status", "unknown")
            labels = {**base_labels, "validation_status": str(status_label)}
            check_name = str(vr.get("check_name", "unnamed"))
            labels["check_name"] = check_name
            metrics.append(
                MetricPoint(
                    metric_name="elt_validation_result",
                    metric_type=MetricType.counter,
                    value=1,
                    labels=labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                )
            )
        self.record_metrics(
            run_context=run_context,
            environment=environment,
            metrics=metrics,
        )
        span_status = (
            SpanStatus.ok
            if audit_record.status == "success"
            else SpanStatus.error
        )
        span = TraceSpan(
            trace_id=_derive_trace_id(audit_record.run_id),
            span_id=_derive_span_id(audit_record.run_id, audit_record.stage),
            name=f"{audit_record.stage}:{audit_record.job_name}",
            start_time=audit_record.started_at,
            end_time=audit_record.completed_at or audit_record.started_at,
            status=span_status,
            attributes={
                **base_labels,
                "duration_seconds": duration_seconds or 0,
                "records_read": metrics_summary.records_read or 0,
                "records_written": metrics_summary.records_written or 0,
                "files_written": metrics_summary.files_written or 0,
            },
            run_id=audit_record.run_id,
            stage=audit_record.stage,
            job_name=audit_record.job_name,
        )
        self.record_traces(
            run_context=run_context,
            environment=environment,
            spans=[span],
        )
        if audit_record.status != "success":
            severity = AlertSeverity.critical
            if audit_record.error_summary is not None:
                error_code = audit_record.error_summary.get("error_code", "")
                if error_code and (
                    "RETRY" in error_code.upper() or "TIMEOUT" in error_code.upper()
                ):
                    severity = AlertSeverity.warning
            labels = {**base_labels}
            if audit_record.error_summary is not None:
                for k, v in audit_record.error_summary.items():
                    labels[f"error_{k}"] = str(v)[:64]
            self.trigger_alert(
                run_context=run_context,
                environment=environment,
                event=AlertEvent(
                    severity=severity,
                    message=(
                        f"ELT run failed: stage={audit_record.stage} "
                        f"job={audit_record.job_name} run_id={audit_record.run_id[:8]}"
                    ),
                    labels=labels,
                    run_id=audit_record.run_id,
                    stage=audit_record.stage,
                    job_name=audit_record.job_name,
                ),
            )

    def _record_emission_failure(
        self,
        *,
        run_context: RunContext,
        environment: str,
        subsystem: str,
        backend_type: str,
        policy: ObservabilityPolicy,
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
                    "ERROR" if policy is ObservabilityPolicy.blocking else "WARNING"
                ),
                component=f"observability.{subsystem}",
                event_type=f"observability_{subsystem}_export_failed",
                message=f"Optional observability {subsystem} export failed",
                details={
                    "backend_type": backend_type,
                    "error_code": error.error_code,
                    "error_category": error.error_category.value,
                    "blocking": policy is ObservabilityPolicy.blocking,
                },
            ),
        )


def build_observability_adapter(
    root_path: str,
    *,
    metrics_exporter: MetricsExporter | None = None,
    trace_exporter: TraceExporter | None = None,
    alert_hook: AlertHook | None = None,
    metrics_policy: ObservabilityPolicy | None = None,
    tracing_policy: ObservabilityPolicy | None = None,
    alerts_policy: ObservabilityPolicy | None = None,
) -> ObservabilityAdapter:
    env = runtime_manifest.env
    configured_metrics = metrics_exporter
    configured_traces = trace_exporter
    configured_alerts = alert_hook
    configured_metrics_policy = metrics_policy
    configured_tracing_policy = tracing_policy
    configured_alerts_policy = alerts_policy

    if configured_metrics is None:
        metrics_cfg = _load_prometheus_remote_write_config_from_env(
            backend_env=env.metrics_backend,
            url_env=env.metrics_url,
            policy_env=env.metrics_policy,
            timeout_env=env.metrics_timeout_seconds,
            auth_env=env.metrics_auth_header,
            subsystem="metrics",
        )
        if metrics_cfg is not None:
            configured_metrics = PrometheusRemoteWriteExporter(
                endpoint_url=metrics_cfg.endpoint_url,
                timeout_seconds=metrics_cfg.timeout_seconds,
                auth_header=metrics_cfg.auth_header,
            )
            configured_metrics_policy = (
                configured_metrics_policy or metrics_cfg.emission_policy
            )

    if configured_traces is None:
        tracing_cfg = _load_otlp_http_trace_config_from_env(
            backend_env=env.tracing_backend,
            url_env=env.tracing_url,
            policy_env=env.tracing_policy,
            timeout_env=env.tracing_timeout_seconds,
            auth_env=env.tracing_auth_header,
        )
        if tracing_cfg is not None:
            configured_traces = OtlpHttpTraceExporter(
                endpoint_url=tracing_cfg.endpoint_url,
                timeout_seconds=tracing_cfg.timeout_seconds,
                auth_header=tracing_cfg.auth_header,
            )
            configured_tracing_policy = (
                configured_tracing_policy or tracing_cfg.emission_policy
            )

    if configured_alerts is None:
        alerts_cfg = _load_webhook_alert_config_from_env(
            backend_env=env.alerts_backend,
            url_env=env.alerts_url,
            policy_env=env.alerts_policy,
            timeout_env=env.alerts_timeout_seconds,
            auth_env=env.alerts_auth_header,
        )
        if alerts_cfg is not None:
            configured_alerts = WebhookAlertHook(
                endpoint_url=alerts_cfg.endpoint_url,
                timeout_seconds=alerts_cfg.timeout_seconds,
                auth_header=alerts_cfg.auth_header,
            )
            configured_alerts_policy = (
                configured_alerts_policy or alerts_cfg.emission_policy
            )

    return ObservabilityAdapter(
        artifact_store=LocalArtifactStore(root_path),
        metrics_exporter=configured_metrics,
        trace_exporter=configured_traces,
        alert_hook=configured_alerts,
        metrics_policy=configured_metrics_policy or ObservabilityPolicy.best_effort,
        tracing_policy=configured_tracing_policy or ObservabilityPolicy.best_effort,
        alerts_policy=configured_alerts_policy or ObservabilityPolicy.best_effort,
    )


def _load_prometheus_remote_write_config_from_env(
    *,
    backend_env: str,
    url_env: str,
    policy_env: str,
    timeout_env: str,
    auth_env: str,
    subsystem: str,
) -> _PrometheusRemoteWriteConfig | None:
    raw_backend_type = os.getenv(backend_env)
    if raw_backend_type is None or not raw_backend_type.strip():
        return None
    backend_type = raw_backend_type.strip().lower()
    supported = _OBSERVABILITY_BACKEND_SUPPORTED[subsystem]
    if backend_type not in supported:
        raise ConfigValidationError(
            message=f"Unsupported observability {subsystem} backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": supported,
                "subsystem": subsystem,
            },
        )
    endpoint_url = _validate_endpoint_url(
        endpoint_url=_require_env_value(url_env, subsystem),
        backend_type=backend_type,
        subsystem=subsystem,
    )
    raw_policy = os.getenv(policy_env, ObservabilityPolicy.best_effort.value)
    try:
        emission_policy = ObservabilityPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message=f"Observability {subsystem} emission policy is invalid",
            context={
                "backend_type": backend_type,
                "emission_policy": raw_policy,
                "supported_values": [p.value for p in ObservabilityPolicy],
                "subsystem": subsystem,
            },
        ) from exc
    raw_timeout = os.getenv(timeout_env, "10")
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError as exc:
        raise ConfigValidationError(
            message=f"Observability {subsystem} timeout must be numeric",
            context={
                "backend_type": backend_type,
                "timeout_seconds": raw_timeout,
                "subsystem": subsystem,
            },
        ) from exc
    timeout_seconds = _validate_timeout_seconds(
        timeout_seconds=timeout_seconds,
        backend_type=backend_type,
    )
    auth_header = _normalize_auth_header(
        auth_header=os.getenv(auth_env),
        backend_type=backend_type,
    )
    return _PrometheusRemoteWriteConfig(
        endpoint_url=endpoint_url,
        emission_policy=emission_policy,
        timeout_seconds=timeout_seconds,
        auth_header=auth_header,
    )


def _load_otlp_http_trace_config_from_env(
    *,
    backend_env: str,
    url_env: str,
    policy_env: str,
    timeout_env: str,
    auth_env: str,
) -> _OtlpHttpTraceConfig | None:
    raw_backend_type = os.getenv(backend_env)
    if raw_backend_type is None or not raw_backend_type.strip():
        return None
    backend_type = raw_backend_type.strip().lower()
    supported = _OBSERVABILITY_BACKEND_SUPPORTED["tracing"]
    if backend_type not in supported:
        raise ConfigValidationError(
            message="Unsupported observability tracing backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": supported,
                "subsystem": "tracing",
            },
        )
    endpoint_url = _validate_endpoint_url(
        endpoint_url=_require_env_value(url_env, "tracing"),
        backend_type=backend_type,
        subsystem="tracing",
    )
    raw_policy = os.getenv(policy_env, ObservabilityPolicy.best_effort.value)
    try:
        emission_policy = ObservabilityPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message="Observability tracing emission policy is invalid",
            context={
                "backend_type": backend_type,
                "emission_policy": raw_policy,
                "supported_values": [p.value for p in ObservabilityPolicy],
                "subsystem": "tracing",
            },
        ) from exc
    raw_timeout = os.getenv(timeout_env, "10")
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError as exc:
        raise ConfigValidationError(
            message="Observability tracing timeout must be numeric",
            context={
                "backend_type": backend_type,
                "timeout_seconds": raw_timeout,
                "subsystem": "tracing",
            },
        ) from exc
    timeout_seconds = _validate_timeout_seconds(
        timeout_seconds=timeout_seconds,
        backend_type=backend_type,
    )
    auth_header = _normalize_auth_header(
        auth_header=os.getenv(auth_env),
        backend_type=backend_type,
    )
    return _OtlpHttpTraceConfig(
        endpoint_url=endpoint_url,
        emission_policy=emission_policy,
        timeout_seconds=timeout_seconds,
        auth_header=auth_header,
    )


def _load_webhook_alert_config_from_env(
    *,
    backend_env: str,
    url_env: str,
    policy_env: str,
    timeout_env: str,
    auth_env: str,
) -> _WebhookAlertConfig | None:
    raw_backend_type = os.getenv(backend_env)
    if raw_backend_type is None or not raw_backend_type.strip():
        return None
    backend_type = raw_backend_type.strip().lower()
    supported = _OBSERVABILITY_BACKEND_SUPPORTED["alerts"]
    if backend_type not in supported:
        raise ConfigValidationError(
            message="Unsupported observability alerts backend type",
            context={
                "backend_type": backend_type,
                "supported_backend_types": supported,
                "subsystem": "alerts",
            },
        )
    endpoint_url = _validate_endpoint_url(
        endpoint_url=_require_env_value(url_env, "alerts"),
        backend_type=backend_type,
        subsystem="alerts",
    )
    raw_policy = os.getenv(policy_env, ObservabilityPolicy.best_effort.value)
    try:
        emission_policy = ObservabilityPolicy(raw_policy.strip().lower())
    except ValueError as exc:
        raise ConfigValidationError(
            message="Observability alerts emission policy is invalid",
            context={
                "backend_type": backend_type,
                "emission_policy": raw_policy,
                "supported_values": [p.value for p in ObservabilityPolicy],
                "subsystem": "alerts",
            },
        ) from exc
    raw_timeout = os.getenv(timeout_env, "10")
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError as exc:
        raise ConfigValidationError(
            message="Observability alerts timeout must be numeric",
            context={
                "backend_type": backend_type,
                "timeout_seconds": raw_timeout,
                "subsystem": "alerts",
            },
        ) from exc
    timeout_seconds = _validate_timeout_seconds(
        timeout_seconds=timeout_seconds,
        backend_type=backend_type,
    )
    auth_header = _normalize_auth_header(
        auth_header=os.getenv(auth_env),
        backend_type=backend_type,
    )
    return _WebhookAlertConfig(
        endpoint_url=endpoint_url,
        emission_policy=emission_policy,
        timeout_seconds=timeout_seconds,
        auth_header=auth_header,
    )

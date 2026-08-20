from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations import (
    ObservabilityAdapter,
    ObservabilityPolicy,
    OtlpHttpTraceExporter,
    PrometheusRemoteWriteExporter,
    WebhookAlertHook,
    build_observability_adapter,
)
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.observability import (
    AlertEvent,
    AlertSeverity,
    MetricPoint,
    MetricType,
    SpanStatus,
    TraceSpan,
)
from elt_pipeline.shared.runtime import StageName, new_run_context

env = runtime_manifest.env


class _CapturingHandler(BaseHTTPRequestHandler):
    captured: list[dict[str, Any]] = []

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        _CapturingHandler.captured.append(
            {
                "path": self.path,
                "headers": dict(self.headers),
                "body": body,
                "json": json.loads(body.decode("utf-8")),
            }
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format, *args):
        return


class _FailingHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(content_length)
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"error"}')

    def log_message(self, format, *args):
        return


@pytest.fixture()
def tmp_root() -> str:
    with tempfile.TemporaryDirectory() as tmp:
        yield tmp


@pytest.fixture()
def run_context():
    return new_run_context(
        stage=StageName.sql,
        job_name="test_job",
        trigger_type="manual",
        attributes={"environment": "test"},
    )


@pytest.fixture()
def artifact_store(tmp_root: str) -> LocalArtifactStore:
    return LocalArtifactStore(tmp_root)


@pytest.fixture()
def adapter_noop(tmp_root: str) -> ObservabilityAdapter:
    return build_observability_adapter(tmp_root)


@pytest.fixture()
def metrics_server():
    _CapturingHandler.captured = []
    server = HTTPServer(("127.0.0.1", 0), _CapturingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


@pytest.fixture()
def failing_server():
    server = HTTPServer(("127.0.0.1", 0), _FailingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _server_url(server: HTTPServer, path: str = "/") -> str:
    port = server.server_address[1]
    return f"http://127.0.0.1:{port}{path}"


# ---------------------------------------------------------------------------
# Shared data models
# ---------------------------------------------------------------------------
class TestDataModels:
    def test_metric_point_defaults(self) -> None:
        point = MetricPoint(
            metric_name="test_counter",
            metric_type=MetricType.counter,
            value=42,
        )
        assert point.metric_name == "test_counter"
        assert point.value == 42
        assert point.labels == {}
        assert point.timestamp is not None
        assert point.run_id is None

    def test_trace_span_defaults(self) -> None:
        span = TraceSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            name="test_span",
        )
        assert span.status is SpanStatus.unset
        assert span.parent_span_id is None
        assert span.attributes == {}
        assert span.events == []

    def test_alert_event_defaults(self) -> None:
        event = AlertEvent(
            severity=AlertSeverity.warning,
            message="hello",
        )
        assert event.labels == {}
        assert event.timestamp is not None
        assert event.run_id is None

    def test_enum_values_stable(self) -> None:
        assert {m.value for m in MetricType} == {
            "counter",
            "gauge",
            "histogram",
            "summary",
        }
        assert {s.value for s in SpanStatus} == {"ok", "error", "unset"}
        assert {a.value for a in AlertSeverity} == {"critical", "warning", "info"}


# ---------------------------------------------------------------------------
# Local persistence (JSONL)
# ---------------------------------------------------------------------------
class TestLocalPersistence:
    def test_record_metrics_writes_jsonl(
        self,
        tmp_root: str,
        run_context,
        adapter_noop: ObservabilityAdapter,
        artifact_store: LocalArtifactStore,
    ) -> None:
        points = [
            MetricPoint(
                metric_name="elt_run_duration_seconds",
                metric_type=MetricType.gauge,
                value=12.5,
                labels={"stage": "sql", "status": "success"},
            ),
            MetricPoint(
                metric_name="elt_records_written_total",
                metric_type=MetricType.counter,
                value=1000,
                labels={"stage": "sql"},
            ),
        ]
        adapter_noop.record_metrics(
            run_context=run_context,
            environment="test",
            metrics=points,
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        rows = _read_jsonl(os.path.join(run_dir, "metrics.jsonl"))
        assert len(rows) == 2
        assert rows[0]["metric_name"] == "elt_run_duration_seconds"
        assert rows[0]["value"] == 12.5
        assert rows[0]["labels"]["stage"] == "sql"
        assert rows[1]["metric_name"] == "elt_records_written_total"
        assert rows[1]["value"] == 1000

    def test_record_traces_writes_jsonl(
        self,
        tmp_root: str,
        run_context,
        adapter_noop: ObservabilityAdapter,
        artifact_store: LocalArtifactStore,
    ) -> None:
        spans = [
            TraceSpan(
                trace_id="a" * 32,
                span_id="b" * 16,
                name="sql:test_job",
                status=SpanStatus.ok,
            )
        ]
        adapter_noop.record_traces(
            run_context=run_context, environment="test", spans=spans
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        rows = _read_jsonl(os.path.join(run_dir, "traces.jsonl"))
        assert len(rows) == 1
        assert rows[0]["name"] == "sql:test_job"
        assert rows[0]["trace_id"] == "a" * 32
        assert rows[0]["status"] == "ok"

    def test_trigger_alert_writes_jsonl(
        self,
        tmp_root: str,
        run_context,
        adapter_noop: ObservabilityAdapter,
        artifact_store: LocalArtifactStore,
    ) -> None:
        adapter_noop.trigger_alert(
            run_context=run_context,
            environment="test",
            event=AlertEvent(
                severity=AlertSeverity.critical,
                message="run failed",
                labels={"error_code": "E1"},
            ),
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        rows = _read_jsonl(os.path.join(run_dir, "alerts.jsonl"))
        assert len(rows) == 1
        assert rows[0]["severity"] == "critical"
        assert rows[0]["message"] == "run failed"
        assert rows[0]["labels"]["error_code"] == "E1"

    def test_empty_input_no_files(
        self,
        tmp_root: str,
        run_context,
        adapter_noop: ObservabilityAdapter,
        artifact_store: LocalArtifactStore,
    ) -> None:
        adapter_noop.record_metrics(
            run_context=run_context, environment="test", metrics=[]
        )
        adapter_noop.record_traces(
            run_context=run_context, environment="test", spans=[]
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        assert not os.path.exists(os.path.join(run_dir, "metrics.jsonl"))
        assert not os.path.exists(os.path.join(run_dir, "traces.jsonl"))


# ---------------------------------------------------------------------------
# Env config validation
# ---------------------------------------------------------------------------
class TestEnvConfigValidation:
    def test_invalid_backend_type_raises(self, tmp_root: str) -> None:
        env_vars = {env.metrics_backend: "nope"}
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert "metrics" in str(exc.value).lower()
        assert "nope" in str(exc.value.context.get("backend_type", ""))

    def test_backend_without_url_raises(self, tmp_root: str) -> None:
        env_vars = {env.tracing_backend: "otlp_http"}
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert env.tracing_url in str(exc.value.message)

    def test_invalid_url_raises(self, tmp_root: str) -> None:
        env_vars = {
            env.alerts_backend: "webhook",
            env.alerts_url: "not-a-url",
        }
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert "http" in str(exc.value.message).lower()

    def test_invalid_timeout_raises(self, tmp_root: str) -> None:
        env_vars = {
            env.metrics_backend: "prometheus_remote_write",
            env.metrics_url: "http://localhost:9090/api/v1/write",
            env.metrics_timeout_seconds: "0",
        }
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert "timeout" in str(exc.value.message).lower()

    def test_invalid_policy_raises(self, tmp_root: str) -> None:
        env_vars = {
            env.tracing_backend: "otlp_http",
            env.tracing_url: "http://localhost:4318/v1/traces",
            env.tracing_policy: "aggressive",
        }
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert "policy" in str(exc.value.message).lower()

    def test_empty_auth_header_raises(self, tmp_root: str) -> None:
        env_vars = {
            env.alerts_backend: "webhook",
            env.alerts_url: "http://localhost:8080/alert",
            env.alerts_auth_header: "   ",
        }
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            with pytest.raises(ConfigValidationError) as exc:
                build_observability_adapter(tmp_root)
        assert "auth header" in str(exc.value.message).lower()

    def test_valid_env_builds_adapter(self, tmp_root: str) -> None:
        env_vars = {
            env.metrics_backend: "prometheus_remote_write",
            env.metrics_url: "http://localhost:9090/api/v1/write",
            env.tracing_backend: "otlp_http",
            env.tracing_url: "http://localhost:4318/v1/traces",
            env.alerts_backend: "webhook",
            env.alerts_url: "http://localhost:8080/alert",
        }
        with pytest.MonkeyPatch.context() as mp:
            for k, v in env_vars.items():
                mp.setenv(k, v)
            adapter = build_observability_adapter(tmp_root)
        assert isinstance(adapter, ObservabilityAdapter)


# ---------------------------------------------------------------------------
# HTTP emitters
# ---------------------------------------------------------------------------
class TestHttpEmitters:
    def test_prometheus_exporter_sends_payload(
        self, tmp_root: str, run_context, metrics_server
    ) -> None:
        exporter = PrometheusRemoteWriteExporter(
            endpoint_url=_server_url(metrics_server, "/write")
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            metrics_exporter=exporter,
        )
        adapter.record_metrics(
            run_context=run_context,
            environment="test",
            metrics=[
                MetricPoint(
                    metric_name="elt_records_written_total",
                    metric_type=MetricType.counter,
                    value=100,
                    labels={"stage": "sql"},
                    run_id=run_context.run_id,
                )
            ],
        )
        assert len(_CapturingHandler.captured) == 1
        payload = _CapturingHandler.captured[0]["json"]
        assert "data" in payload
        result = payload["data"]["result"]
        assert len(result) == 1
        labels = {item["name"]: item["value"] for item in result[0]["labels"]}
        assert labels["__name__"] == "elt_records_written_total"
        assert labels["stage"] == "sql"
        assert labels["run_id"] == run_context.run_id
        assert result[0]["samples"][0]["value"] == 100.0

    def test_otlp_exporter_sends_payload(
        self, tmp_root: str, run_context, metrics_server
    ) -> None:
        exporter = OtlpHttpTraceExporter(
            endpoint_url=_server_url(metrics_server, "/v1/traces")
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            trace_exporter=exporter,
        )
        adapter.record_traces(
            run_context=run_context,
            environment="test",
            spans=[
                TraceSpan(
                    trace_id="a" * 32,
                    span_id="b" * 16,
                    name="sql:job",
                    status=SpanStatus.ok,
                    attributes={"records_written": 100},
                )
            ],
        )
        assert len(_CapturingHandler.captured) == 1
        payload = _CapturingHandler.captured[0]["json"]
        assert "resourceSpans" in payload
        scope_spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        assert len(scope_spans) == 1
        assert scope_spans[0]["traceId"] == "a" * 32
        assert scope_spans[0]["spanId"] == "b" * 16
        assert scope_spans[0]["name"] == "sql:job"
        assert scope_spans[0]["status"]["code"] == 1

    def test_webhook_sends_alert(
        self, tmp_root: str, run_context, metrics_server
    ) -> None:
        hook = WebhookAlertHook(
            endpoint_url=_server_url(metrics_server, "/hook")
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            alert_hook=hook,
        )
        adapter.trigger_alert(
            run_context=run_context,
            environment="test",
            event=AlertEvent(
                severity=AlertSeverity.critical,
                message="boom",
                labels={"error_code": "E500"},
            ),
        )
        assert len(_CapturingHandler.captured) == 1
        payload = _CapturingHandler.captured[0]["json"]
        assert payload["severity"] == "critical"
        assert payload["message"] == "boom"
        assert payload["labels"]["error_code"] == "E500"

    def test_exporter_with_auth_header(
        self, tmp_root: str, run_context, metrics_server
    ) -> None:
        exporter = PrometheusRemoteWriteExporter(
            endpoint_url=_server_url(metrics_server, "/write"),
            auth_header="Bearer token-abc",
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            metrics_exporter=exporter,
        )
        adapter.record_metrics(
            run_context=run_context,
            environment="test",
            metrics=[
                MetricPoint(
                    metric_name="m", metric_type=MetricType.counter, value=1
                )
            ],
        )
        headers = _CapturingHandler.captured[0]["headers"]
        assert headers.get("Authorization") == "Bearer token-abc"


# ---------------------------------------------------------------------------
# Policy (best_effort vs blocking)
# ---------------------------------------------------------------------------
class TestPolicyBehavior:
    def test_best_effort_failure_does_not_raise(
        self, tmp_root: str, run_context, failing_server
    ) -> None:
        exporter = PrometheusRemoteWriteExporter(
            endpoint_url=_server_url(failing_server, "/write"),
            timeout_seconds=0.5,
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            metrics_exporter=exporter,
            metrics_policy=ObservabilityPolicy.best_effort,
        )
        adapter.record_metrics(
            run_context=run_context,
            environment="test",
            metrics=[
                MetricPoint(
                    metric_name="m", metric_type=MetricType.counter, value=1
                )
            ],
        )

    def test_blocking_failure_raises(
        self, tmp_root: str, run_context, failing_server
    ) -> None:
        exporter = OtlpHttpTraceExporter(
            endpoint_url=_server_url(failing_server, "/v1/traces"),
            timeout_seconds=0.5,
        )
        adapter = ObservabilityAdapter(
            artifact_store=LocalArtifactStore(tmp_root),
            trace_exporter=exporter,
            tracing_policy=ObservabilityPolicy.blocking,
        )
        with pytest.raises(PipelineError) as exc:
            adapter.record_traces(
                run_context=run_context,
                environment="test",
                spans=[
                    TraceSpan(
                        trace_id="a" * 32, span_id="b" * 16, name="span"
                    )
                ],
            )
        assert exc.value.error_category is ErrorCategory.observability_error
        assert "500" in str(exc.value.context.get("status_code", ""))

    def test_best_effort_records_failure_to_logs(
        self,
        tmp_root: str,
        run_context,
        failing_server,
        artifact_store: LocalArtifactStore,
    ) -> None:
        hook = WebhookAlertHook(
            endpoint_url=_server_url(failing_server, "/hook"),
            timeout_seconds=0.5,
        )
        adapter = ObservabilityAdapter(
            artifact_store=artifact_store,
            alert_hook=hook,
            alerts_policy=ObservabilityPolicy.best_effort,
        )
        adapter.trigger_alert(
            run_context=run_context,
            environment="test",
            event=AlertEvent(severity=AlertSeverity.warning, message="warn"),
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        log_rows = _read_jsonl(os.path.join(run_dir, "logs.jsonl"))
        assert any(
            "observability_alerts_export_failed" in r.get("event_type", "")
            for r in log_rows
        )
        error_rows = _read_jsonl(os.path.join(run_dir, "errors.jsonl"))
        assert any(
            "OBSERVABILITY_ALERT" in r.get("error_code", "") for r in error_rows
        )


# ---------------------------------------------------------------------------
# on_run_complete (AuditRecord auto-derivation)
# ---------------------------------------------------------------------------
class TestOnRunComplete:
    def test_success_audit_derives_metrics(
        self,
        tmp_root: str,
        run_context,
        artifact_store: LocalArtifactStore,
        adapter_noop: ObservabilityAdapter,
    ) -> None:
        started_at = run_context.started_at
        completed_at = started_at + timedelta(seconds=7.5)
        audit = AuditRecord(
            run_id=run_context.run_id,
            stage="sql",
            job_name=run_context.job_name,
            trigger_type="manual",
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            metrics_summary=MetricsSummary(
                records_read=100,
                records_written=80,
                files_written=3,
                extra={"request_count": 10, "model.main.row_count": 500},
            ),
            context={"environment": "test"},
        )
        adapter_noop.on_run_complete(
            run_context=run_context, environment="test", audit_record=audit
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        metrics = _read_jsonl(os.path.join(run_dir, "metrics.jsonl"))
        metrics_by_name = {m["metric_name"]: m for m in metrics}
        assert metrics_by_name["elt_run_duration_seconds"]["value"] == 7.5
        assert metrics_by_name["elt_run_duration_seconds"]["labels"]["status"] == "success"
        assert metrics_by_name["elt_records_read_total"]["value"] == 100
        assert metrics_by_name["elt_records_written_total"]["value"] == 80
        assert metrics_by_name["elt_files_written_total"]["value"] == 3
        assert metrics_by_name["elt_run_status"]["value"] == 1
        assert metrics_by_name["elt_extra_request_count"]["value"] == 10
        assert (
            metrics_by_name["elt_extra_model_main_row_count"]["value"]
            == 500
        )

    def test_success_audit_derives_ok_span(
        self,
        tmp_root: str,
        run_context,
        artifact_store: LocalArtifactStore,
        adapter_noop: ObservabilityAdapter,
    ) -> None:
        started_at = run_context.started_at
        completed_at = started_at + timedelta(seconds=5)
        audit = AuditRecord(
            run_id=run_context.run_id,
            stage="ingest",
            job_name=run_context.job_name,
            trigger_type="manual",
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            metrics_summary=MetricsSummary(
                records_read=10, records_written=10, files_written=1
            ),
            context={"environment": "test"},
        )
        adapter_noop.on_run_complete(
            run_context=run_context, environment="test", audit_record=audit
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        traces = _read_jsonl(os.path.join(run_dir, "traces.jsonl"))
        assert len(traces) == 1
        span = traces[0]
        assert span["name"] == f"ingest:{run_context.job_name}"
        assert span["status"] == "ok"
        assert span["attributes"]["records_written"] == 10
        assert span["attributes"]["duration_seconds"] == 5.0

    def test_failed_audit_derives_error_span_and_alert(
        self,
        tmp_root: str,
        run_context,
        artifact_store: LocalArtifactStore,
        adapter_noop: ObservabilityAdapter,
    ) -> None:
        started_at = run_context.started_at
        audit = AuditRecord(
            run_id=run_context.run_id,
            stage="sql",
            job_name=run_context.job_name,
            trigger_type="manual",
            started_at=started_at,
            completed_at=started_at + timedelta(seconds=2),
            status="failed",
            metrics_summary=MetricsSummary(
                records_read=0, records_written=0, files_written=0
            ),
            error_summary={
                "error_code": "SQL_COMPILE_ERROR",
                "message": "syntax error",
            },
            context={"environment": "test"},
        )
        adapter_noop.on_run_complete(
            run_context=run_context, environment="test", audit_record=audit
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        traces = _read_jsonl(os.path.join(run_dir, "traces.jsonl"))
        assert traces[0]["status"] == "error"
        metrics = _read_jsonl(os.path.join(run_dir, "metrics.jsonl"))
        metrics_by_name = {m["metric_name"]: m for m in metrics}
        assert metrics_by_name["elt_run_status"]["value"] == 0
        alerts = _read_jsonl(os.path.join(run_dir, "alerts.jsonl"))
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "critical"
        assert "SQL_COMPILE_ERROR" in alerts[0]["message"] or alerts[0][
            "labels"
        ].get("error_error_code") == "SQL_COMPILE_ERROR"

    def test_retry_error_code_uses_warning_severity(
        self,
        tmp_root: str,
        run_context,
        artifact_store: LocalArtifactStore,
        adapter_noop: ObservabilityAdapter,
    ) -> None:
        audit = AuditRecord(
            run_id=run_context.run_id,
            stage="sql",
            job_name=run_context.job_name,
            trigger_type="manual",
            started_at=run_context.started_at,
            completed_at=run_context.started_at + timedelta(seconds=1),
            status="failed",
            metrics_summary=MetricsSummary(),
            error_summary={"error_code": "RETRY_TIMEOUT_OCCURRED"},
        )
        adapter_noop.on_run_complete(
            run_context=run_context, environment="test", audit_record=audit
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        alerts = _read_jsonl(os.path.join(run_dir, "alerts.jsonl"))
        assert alerts[0]["severity"] == "warning"

    def test_validation_results_counted(
        self,
        tmp_root: str,
        run_context,
        artifact_store: LocalArtifactStore,
        adapter_noop: ObservabilityAdapter,
    ) -> None:
        audit = AuditRecord(
            run_id=run_context.run_id,
            stage="sql",
            job_name=run_context.job_name,
            trigger_type="manual",
            started_at=run_context.started_at,
            completed_at=run_context.started_at + timedelta(seconds=1),
            status="success",
            metrics_summary=MetricsSummary(),
            validation_results=[
                {"check_name": "not_null", "status": "pass"},
                {"check_name": "unique_id", "status": "fail"},
            ],
        )
        adapter_noop.on_run_complete(
            run_context=run_context, environment="test", audit_record=audit
        )
        run_dir = artifact_store.layout.run_dir(
            run_context=run_context, environment="test"
        )
        metrics = _read_jsonl(os.path.join(run_dir, "metrics.jsonl"))
        validation = [
            m
            for m in metrics
            if m["metric_name"] == "elt_validation_result"
        ]
        assert len(validation) == 2
        statuses = {m["labels"]["validation_status"] for m in validation}
        assert statuses == {"pass", "fail"}


# ---------------------------------------------------------------------------
# Adapter construction (build factory)
# ---------------------------------------------------------------------------
class TestBuildFactory:
    def test_no_env_no_exporters(self, tmp_root: str) -> None:
        adapter = build_observability_adapter(tmp_root)
        assert adapter._metrics_exporter is None
        assert adapter._trace_exporter is None
        assert adapter._alert_hook is None
        assert (
            adapter._metrics_policy is ObservabilityPolicy.best_effort
        )

    def test_explicit_exporter_overrides_env(
        self, tmp_root: str, monkeypatch
    ) -> None:
        class _FakeExporter:
            backend_type = "fake"

            def export_metrics(self, **_):
                raise AssertionError("should not be called")

        monkeypatch.setenv(env.metrics_backend, "prometheus_remote_write")
        monkeypatch.setenv(env.metrics_url, "http://bogus:9999/x")
        adapter = build_observability_adapter(
            tmp_root, metrics_exporter=_FakeExporter()
        )
        assert isinstance(adapter._metrics_exporter, _FakeExporter)


# ---------------------------------------------------------------------------
# Sanitization + ID derivation helpers
# ---------------------------------------------------------------------------
class TestHelpers:
    def test_sanitize_metric_name(self) -> None:
        from elt_pipeline.integrations.metrics import _sanitize_metric_name

        assert _sanitize_metric_name("model.main.row_count") == "model_main_row_count"
        assert _sanitize_metric_name("123abc") == "_123abc"
        assert _sanitize_metric_name("normal") == "normal"

    def test_trace_id_deterministic(self) -> None:
        from elt_pipeline.integrations.metrics import (
            _derive_span_id,
            _derive_trace_id,
        )

        t1 = _derive_trace_id("run-abc")
        t2 = _derive_trace_id("run-abc")
        assert t1 == t2
        assert len(t1) == 32
        s1 = _derive_span_id("run-abc", "sql")
        s2 = _derive_span_id("run-abc", "sql")
        assert s1 == s2
        assert len(s1) == 16
        s3 = _derive_span_id("run-abc", "ingest")
        assert s1 != s3

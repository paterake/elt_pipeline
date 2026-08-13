from __future__ import annotations

import json
from pathlib import Path
from urllib.error import URLError

import pytest

from elt_pipeline.integrations import (
    LineageEmissionPolicy,
    LineageRemoteEmitter,
    OpenLineageHttpEmitter,
    build_lineage_adapter,
)
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.shared.lineage import LineageEvent
from elt_pipeline.shared.runtime import StageName, new_run_context


class _FailingRemoteEmitter(LineageRemoteEmitter):
    backend_type = "test_backend"

    def emit(
        self,
        *,
        run_context,
        environment: str,
        lineage_event: LineageEvent,
    ) -> None:
        raise RuntimeError("backend unavailable")


def test_lineage_adapter_writes_local_lineage_artifact(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(str(tmp_path))

    lineage_path = Path(
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="START",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )
    )

    assert lineage_path.exists()
    events = _read_jsonl(lineage_path)
    assert events[0]["event_type"] == "START"
    assert events[0]["run_id"] == run_context.run_id


def test_lineage_adapter_records_non_blocking_remote_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(
        str(tmp_path),
        remote_emitter=_FailingRemoteEmitter(),
        emission_policy=LineageEmissionPolicy.best_effort,
    )

    lineage_path = Path(
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="COMPLETE",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )
    )

    run_dir = lineage_path.parent
    assert lineage_path.exists()
    assert _read_jsonl(run_dir / "lineage.jsonl")[0]["event_type"] == "COMPLETE"
    assert _read_jsonl(run_dir / "errors.jsonl")[0]["error_code"] == (
        "LINEAGE_BACKEND_EMISSION_FAILED"
    )
    assert _read_jsonl(run_dir / "logs.jsonl")[0]["event_type"] == "lineage_remote_emit_failed"


def test_lineage_adapter_raises_for_blocking_remote_failures(tmp_path: Path) -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(
        str(tmp_path),
        remote_emitter=_FailingRemoteEmitter(),
        emission_policy=LineageEmissionPolicy.blocking,
    )

    with pytest.raises(PipelineError) as exc_info:
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )

    run_dir = tmp_path / "runs" / "stage=shared" / "job=lineage-test"
    run_dir = run_dir / f"run_id={run_context.run_id}"
    assert "environment=" not in str(run_dir)
    assert exc_info.value.error_code == "LINEAGE_BACKEND_EMISSION_FAILED"
    assert (run_dir / "lineage.jsonl").exists()
    assert _read_jsonl(run_dir / "errors.jsonl")[0]["error_code"] == (
        "LINEAGE_BACKEND_EMISSION_FAILED"
    )


def test_build_lineage_adapter_uses_env_configured_openlineage_http_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request: dict[str, object] = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def _fake_urlopen(request, *, timeout: float):
        captured_request["url"] = request.full_url
        captured_request["timeout"] = timeout
        captured_request["authorization"] = request.get_header("Authorization")
        captured_request["content_type"] = request.get_header("Content-type")
        captured_request["payload"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse()

    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_BACKEND", "openlineage_http")
    monkeypatch.setenv(
        "ELT_PIPELINE_LINEAGE_URL",
        "https://lineage.example.test/api/v1/lineage",
    )
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_POLICY", "blocking")
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS", "3.5")
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_AUTH_HEADER", "Bearer test-token")
    monkeypatch.setattr("elt_pipeline.integrations.lineage.urlopen", _fake_urlopen)

    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(str(tmp_path))

    lineage_path = Path(
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="COMPLETE",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )
    )

    assert lineage_path.exists()
    assert captured_request == {
        "url": "https://lineage.example.test/api/v1/lineage",
        "timeout": 3.5,
        "authorization": "Bearer test-token",
        "content_type": "application/json",
        "payload": {
            "event_type": "COMPLETE",
            "event_time": captured_request["payload"]["event_time"],
            "run_id": run_context.run_id,
            "job_name": run_context.job_name,
            "producer": "elt_pipeline",
            "inputs": [],
            "outputs": [],
        },
    }


def test_build_lineage_adapter_normalizes_env_configured_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request: dict[str, object] = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def _fake_urlopen(request, *, timeout: float):
        captured_request["url"] = request.full_url
        captured_request["timeout"] = timeout
        captured_request["authorization"] = request.get_header("Authorization")
        return _FakeResponse()

    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_BACKEND", " OPENLINEAGE_HTTP ")
    monkeypatch.setenv(
        "ELT_PIPELINE_LINEAGE_URL",
        " https://lineage.example.test/api/v1/lineage ",
    )
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_POLICY", " BLOCKING ")
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS", "4")
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_AUTH_HEADER", " Bearer trimmed-token ")
    monkeypatch.setattr("elt_pipeline.integrations.lineage.urlopen", _fake_urlopen)

    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(str(tmp_path))

    lineage_path = Path(
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="COMPLETE",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )
    )

    assert lineage_path.exists()
    assert captured_request == {
        "url": "https://lineage.example.test/api/v1/lineage",
        "timeout": 4.0,
        "authorization": "Bearer trimmed-token",
    }


def test_build_lineage_adapter_rejects_invalid_env_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_BACKEND", "openlineage_http")
    monkeypatch.setenv("ELT_PIPELINE_LINEAGE_URL", "not-a-url")

    with pytest.raises(ConfigValidationError) as exc_info:
        build_lineage_adapter(str(tmp_path))

    assert exc_info.value.context["endpoint_url"] == "not-a-url"


def test_openlineage_http_emitter_rejects_invalid_constructor_arguments() -> None:
    with pytest.raises(ConfigValidationError, match="valid http or https URL"):
        OpenLineageHttpEmitter(endpoint_url="not-a-url")

    with pytest.raises(ConfigValidationError, match="greater than zero"):
        OpenLineageHttpEmitter(
            endpoint_url="https://lineage.example.test/api/v1/lineage",
            timeout_seconds=0,
        )

    with pytest.raises(ConfigValidationError, match="must not be empty"):
        OpenLineageHttpEmitter(
            endpoint_url="https://lineage.example.test/api/v1/lineage",
            auth_header="   ",
        )


def test_openlineage_http_emitter_surfaces_retryable_backend_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _failing_urlopen(request, *, timeout: float):
        raise URLError("connection refused")

    monkeypatch.setattr("elt_pipeline.integrations.lineage.urlopen", _failing_urlopen)

    run_context = new_run_context(stage=StageName.shared, job_name="lineage-test")
    adapter = build_lineage_adapter(
        str(tmp_path),
        remote_emitter=OpenLineageHttpEmitter(
            endpoint_url="https://lineage.example.test/api/v1/lineage",
            timeout_seconds=2.0,
        ),
        emission_policy=LineageEmissionPolicy.best_effort,
    )

    lineage_path = Path(
        adapter.emit(
            run_context=run_context,
            environment="default",
            lineage_event=LineageEvent(
                event_type="START",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
            ),
        )
    )

    run_dir = lineage_path.parent
    assert lineage_path.exists()
    error_record = _read_jsonl(run_dir / "errors.jsonl")[0]
    assert error_record["error_code"] == "LINEAGE_BACKEND_EMISSION_FAILED"
    assert error_record["retryable"] is True


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

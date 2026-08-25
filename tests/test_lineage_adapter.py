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
from elt_pipeline.shared.lineage import (
    DatasetRef,
    LineageEvent,
    OpenLineageRunEvent,
    convert_to_openlineage_run_event,
)
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
    payload = captured_request.pop("payload")
    assert captured_request == {
        "url": "https://lineage.example.test/api/v1/lineage",
        "timeout": 3.5,
        "authorization": "Bearer test-token",
        "content_type": "application/json",
    }
    assert payload["eventType"] == "COMPLETE"
    assert payload["eventTime"] == payload["eventTime"]
    assert payload["run"] == {"runId": run_context.run_id, "facets": payload["run"]["facets"]}
    assert "environment" in payload["run"]["facets"]
    assert payload["run"]["facets"]["environment"]["environmentName"] == "default"
    assert payload["job"] == {
        "namespace": "elt_pipeline",
        "name": run_context.job_name,
        "facets": {},
    }
    assert payload["inputs"] == []
    assert payload["outputs"] == []
    assert payload["producer"] == "https://github.com/emailrak/elt_pipeline/openlineage/1"
    assert payload["schemaURL"].startswith("https://openlineage.io/spec/")
    assert "#/definitions/RunEvent" in payload["schemaURL"]


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


def test_convert_to_openlineage_minimal_event_shape() -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="ol-test")
    event = LineageEvent(
        event_type="START",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
    )
    ol = convert_to_openlineage_run_event(event)

    assert isinstance(ol, OpenLineageRunEvent)
    assert ol.eventType == "START"
    assert ol.run.runId == run_context.run_id
    assert ol.job.namespace == "elt_pipeline"
    assert ol.job.name == run_context.job_name
    assert ol.inputs == []
    assert ol.outputs == []
    assert ol.producer == "https://github.com/emailrak/elt_pipeline/openlineage/1"
    assert ol.schemaURL.startswith("https://openlineage.io/spec/")


def test_convert_to_openlineage_with_inputs_outputs_and_facets() -> None:
    run_context = new_run_context(stage=StageName.sql, job_name="ol-io-test")
    event = LineageEvent(
        event_type="COMPLETE",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
        inputs=[
            DatasetRef(namespace="warehouse.l1", name="raw.orders", facets={"row_count": 1000}),
            DatasetRef(namespace="warehouse.l1", name="raw.customers"),
        ],
        outputs=[
            DatasetRef(namespace="warehouse.l3", name="orders.mart", facets={"schema_version": 3}),
        ],
        run_facets={"spark_version": "4.1.2"},
        job_facets={"ownership": {"data_owner": "analytics"}},
        job_namespace="analytics_team",
    )
    ol = convert_to_openlineage_run_event(event)

    assert ol.eventType == "COMPLETE"
    assert ol.job.namespace == "analytics_team"
    assert ol.run.facets["spark_version"] == "4.1.2"
    assert ol.job.facets["ownership"]["data_owner"] == "analytics"
    assert len(ol.inputs) == 2
    assert ol.inputs[0].namespace == "warehouse.l1"
    assert ol.inputs[0].name == "raw.orders"
    assert ol.inputs[0].facets == {"row_count": 1000}
    assert ol.inputs[1].namespace == "warehouse.l1"
    assert ol.inputs[1].name == "raw.customers"
    assert len(ol.outputs) == 1
    assert ol.outputs[0].namespace == "warehouse.l3"
    assert ol.outputs[0].name == "orders.mart"
    assert ol.outputs[0].facets == {"schema_version": 3}


def test_convert_to_openlineage_injects_environment_run_facet() -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="ol-env-test")
    event = LineageEvent(
        event_type="COMPLETE",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
        environment="production",
    )
    ol = convert_to_openlineage_run_event(event)

    assert "environment" in ol.run.facets
    env_facet = ol.run.facets["environment"]
    assert env_facet["environmentName"] == "production"
    assert "_producer" in env_facet
    assert "_schemaURL" in env_facet
    assert "EnvironmentRunFacet" in env_facet["_schemaURL"]


def test_convert_to_openlineage_no_env_facet_when_environment_unset() -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="ol-noenv-test")
    event = LineageEvent(
        event_type="COMPLETE",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
    )
    ol = convert_to_openlineage_run_event(event)

    assert "environment" not in ol.run.facets


def test_convert_to_openlineage_does_not_override_existing_environment_facet() -> None:
    run_context = new_run_context(stage=StageName.shared, job_name="ol-env-preserve")
    custom_env_facet = {
        "_producer": "custom",
        "_schemaURL": "custom-schema",
        "environmentName": "staging",
        "extraField": True,
    }
    event = LineageEvent(
        event_type="FAIL",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
        run_facets={"environment": custom_env_facet},
        environment="production",
    )
    ol = convert_to_openlineage_run_event(event)

    assert ol.run.facets["environment"] is custom_env_facet
    assert ol.run.facets["environment"]["environmentName"] == "staging"
    assert ol.run.facets["environment"]["extraField"] is True


def test_openlineage_http_emitter_sends_wire_compatible_payload_with_datasets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def _fake_urlopen(request, *, timeout: float):
        captured_payload["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse()

    monkeypatch.setattr("elt_pipeline.integrations.lineage.urlopen", _fake_urlopen)

    run_context = new_run_context(stage=StageName.sql, job_name="ol-e2e-datasets")
    adapter = build_lineage_adapter(
        str(tmp_path),
        remote_emitter=OpenLineageHttpEmitter(
            endpoint_url="https://marquez.example.test/api/v1/lineage",
            timeout_seconds=5.0,
        ),
        emission_policy=LineageEmissionPolicy.blocking,
    )

    lineage_event = LineageEvent(
        event_type="COMPLETE",
        run_id=run_context.run_id,
        job_name=run_context.job_name,
        inputs=[
            DatasetRef(namespace="l1", name="orders", facets={"bytes": 1024}),
        ],
        outputs=[
            DatasetRef(namespace="l3", name="mart_orders", facets={"bytes": 2048}),
        ],
    )
    adapter.emit(
        run_context=run_context,
        environment="production",
        lineage_event=lineage_event,
    )

    body = captured_payload["body"]
    assert body["eventType"] == "COMPLETE"
    assert body["run"]["runId"] == run_context.run_id
    assert body["run"]["facets"]["environment"]["environmentName"] == "production"
    assert body["job"]["name"] == run_context.job_name
    assert len(body["inputs"]) == 1
    assert body["inputs"][0] == {
        "namespace": "l1",
        "name": "orders",
        "facets": {"bytes": 1024},
        "inputFacets": {},
    }
    assert len(body["outputs"]) == 1
    assert body["outputs"][0] == {
        "namespace": "l3",
        "name": "mart_orders",
        "facets": {"bytes": 2048},
        "outputFacets": {},
    }
    assert body["producer"] == "https://github.com/emailrak/elt_pipeline/openlineage/1"
    assert body["schemaURL"].startswith("https://openlineage.io/spec/2-0-2/")
    assert "RunEvent" in body["schemaURL"]


def test_openlineage_wire_format_roundtrips_via_pydantic_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_body: dict[str, object] = {}

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def _fake_urlopen(request, *, timeout: float):
        captured_body["json"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse()

    monkeypatch.setattr("elt_pipeline.integrations.lineage.urlopen", _fake_urlopen)

    run_context = new_run_context(stage=StageName.publish, job_name="ol-validate")
    adapter = build_lineage_adapter(
        str(tmp_path),
        remote_emitter=OpenLineageHttpEmitter(
            endpoint_url="https://marquez.example.test/api/v1/lineage",
        ),
    )
    adapter.emit(
        run_context=run_context,
        environment="staging",
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
        ),
    )

    validated = OpenLineageRunEvent(**captured_body["json"])
    assert validated.eventType == "START"
    assert validated.run.runId == run_context.run_id
    assert validated.job.name == run_context.job_name
    assert validated.producer == "https://github.com/emailrak/elt_pipeline/openlineage/1"

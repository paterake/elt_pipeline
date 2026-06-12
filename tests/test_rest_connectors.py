from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    RestConnectorBase,
    RestConnectorConfig,
    RestPreparedRequest,
    RestRequestWindow,
    RestResolvedAuth,
    RestResponse,
)
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_rest_connector_config_builds_from_resolved_entity_config() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_api",
        entity_name="orders",
        connector_type="rest",
        trigger_mode="scheduled_batch",
        auth={"strategy": "bearer_token", "secret_refs": {"token": "ORDERS_API_TOKEN"}},
        extraction={
            "base_url": "https://api.example.com/",
            "request": {
                "method": "get",
                "path": "/v1/orders",
                "headers": {"Accept": "application/json"},
                "query_params": {"status": "open"},
            },
            "pagination": {
                "mode": "page",
                "page_parameter_name": "page",
                "page_size": 200,
                "max_pages": 10,
            },
        },
    )

    connector_config = RestConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.base_url == "https://api.example.com"
    assert connector_config.request.method == "GET"
    assert connector_config.request.path == "/v1/orders"
    assert connector_config.auth.strategy.value == "bearer_token"
    assert connector_config.pagination.mode.value == "page"
    assert connector_config.execution_mode == "scheduled_batch"


def test_rest_connector_config_rejects_non_rest_connector() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_db",
        entity_name="orders",
        connector_type="sql",
    )

    with pytest.raises(ConfigValidationError, match="not a REST connector"):
        RestConnectorConfig.from_resolved_entity_config(resolved_config)


def test_rest_connector_base_persists_before_checkpoint_update(tmp_path: Path) -> None:
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="orders-ingest",
        trigger_type="scheduled_batch",
    )
    connector = FakeRestConnector(tmp_path=tmp_path, run_context=run_context)

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="orders_api",
        entity_name="orders",
    )

    assert connector.call_order == [
        "resolve_checkpoint_before",
        "resolve_window",
        "resolve_authentication",
        "execute_request",
        "persist_response",
        "build_checkpoint_after",
        "update_checkpoint",
    ]
    assert result.request_count == 1
    assert result.response_count == 1
    assert len(result.manifests) == 1
    assert result.checkpoint_before == {"page": 3}
    assert result.checkpoint_after == {"page": 4}
    assert checkpoint_document.current_checkpoint == {"page": 4}
    assert checkpoint_document.history[0].manifest_paths == [result.manifests[0].manifest_path]


class FakeRestConnector(RestConnectorBase):
    def __init__(self, *, tmp_path: Path, run_context) -> None:
        self.call_order: list[str] = []
        self.writer = LocalLevel1Writer(tmp_path)
        self.checkpoint_store = LocalCheckpointStore(tmp_path)
        super().__init__(
            config=RestConnectorConfig(
                schema_version="v1",
                environment="dev",
                source_name="orders_api",
                entity_name="orders",
                execution_mode="scheduled_batch",
                base_url="https://api.example.com",
                request={
                    "method": "GET",
                    "path": "/v1/orders",
                    "artifact_name": "orders-page",
                },
            ),
            run_context=run_context,
        )

    def resolve_checkpoint_before(self) -> dict[str, int]:
        self.call_order.append("resolve_checkpoint_before")
        return {"page": 3}

    def resolve_window(self) -> RestRequestWindow:
        self.call_order.append("resolve_window")
        return RestRequestWindow(
            start=datetime(2026, 1, 3, tzinfo=UTC),
            end=datetime(2026, 1, 4, tzinfo=UTC),
            label="2026-01-03_to_2026-01-04",
        )

    def resolve_authentication(self) -> RestResolvedAuth:
        self.call_order.append("resolve_authentication")
        return RestResolvedAuth(headers={"Authorization": "Bearer redacted"})

    def execute_request(self, request: RestPreparedRequest) -> RestResponse:
        self.call_order.append("execute_request")
        assert request.headers["Authorization"] == "Bearer redacted"
        return RestResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body='{"items":[{"id": 1}]}',
            received_at=datetime(2026, 1, 4, 0, 5, tzinfo=UTC),
            content_type="application/json",
            metadata={"page": 4},
        )

    def persist_response(
        self,
        *,
        request: RestPreparedRequest,
        response: RestResponse,
        checkpoint_before: dict[str, int] | None,
        window: RestRequestWindow | None,
    ):
        self.call_order.append("persist_response")
        manifest = self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=response.body,
            payload_format=self.config.request.payload_format,
            extraction_mode=self.config.execution_mode,
            artifact_name=self.config.request.artifact_name,
            checkpoint_before=checkpoint_before,
            window_start=window.start if window else None,
            window_end=window.end if window else None,
            window_label=window.label if window else None,
            metadata={"request_url": request.url},
            ingest_completed_at=response.received_at,
        )
        return [manifest]

    def build_checkpoint_after(self, **kwargs):
        self.call_order.append("build_checkpoint_after")
        return {"page": 4}

    def update_checkpoint(
        self,
        *,
        checkpoint_before,
        checkpoint_after,
        manifests,
        window,
    ) -> None:
        self.call_order.append("update_checkpoint")
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=window.start if window else None,
            window_end=window.end if window else None,
            window_label=window.label if window else None,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
        )

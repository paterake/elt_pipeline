from __future__ import annotations

import base64
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
from elt_pipeline.shared.runtime import RunContext, StageName, new_run_context


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


def test_rest_connector_build_request_plan_renders_templates() -> None:
    run_context = RunContext(
        run_id="run-123",
        stage=StageName.ingest,
        job_name="orders-ingest",
        trigger_type="scheduled_batch",
        started_at=datetime(2026, 1, 5, 10, 30, tzinfo=UTC),
    )
    connector = TemplatedRestConnector(run_context=run_context)

    requests = connector.build_request_plan(
        checkpoint_before={"page": 3, "watermark": "2026-01-01T00:00:00+00:00"},
        window=RestRequestWindow(
            start=datetime(2026, 1, 3, tzinfo=UTC),
            end=datetime(2026, 1, 4, tzinfo=UTC),
            label="2026-01-03_to_2026-01-04",
        ),
        auth=RestResolvedAuth(
            headers={"Authorization": "Bearer {run.id}"},
            query_params={"api_key": "{entity.name}"},
            body_fields={"issued_for": "{source.name}"},
        ),
    )

    assert len(requests) == 1
    request = requests[0]
    assert request.url == "https://api.example.com/v1/orders/2026-01-03"
    assert request.headers == {
        "Accept": "application/json",
        "X-Window-Label": "2026-01-03_to_2026-01-04",
        "Authorization": "Bearer run-123",
    }
    assert request.query_params == {
        "page": 3,
        "start": "2026-01-03T00:00:00+00:00",
        "end": "2026-01-04T00:00:00+00:00",
        "api_key": "orders",
    }
    assert request.body == {
        "window": {
            "start": "2026-01-03T00:00:00+00:00",
            "end": "2026-01-04T00:00:00+00:00",
        },
        "metadata": ["run-123", "2026-01-03", "scheduled_batch"],
        "watermark": "2026-01-01T00:00:00+00:00",
        "issued_for": "orders_api",
    }
    assert request.metadata["template_context"]["run"]["id"] == "run-123"


def test_rest_connector_build_request_plan_rejects_unknown_template_token() -> None:
    connector = TemplatedRestConnector(
        run_context=RunContext(
            run_id="run-123",
            stage=StageName.ingest,
            job_name="orders-ingest",
            trigger_type="manual",
            started_at=datetime(2026, 1, 5, 10, 30, tzinfo=UTC),
        )
    )
    connector.config.request.path = "/v1/orders/{window.missing_value}"

    with pytest.raises(ConfigValidationError, match="unknown token"):
        connector.build_request_plan(
            checkpoint_before={"page": 1},
            window=RestRequestWindow(start=datetime(2026, 1, 3, tzinfo=UTC)),
            auth=RestResolvedAuth(),
        )


def test_rest_connector_resolves_bearer_token_authentication() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "bearer_token",
            "secret_refs": {"token": "ORDERS_API_TOKEN"},
        },
        secrets={"ORDERS_API_TOKEN": "super-secret-token"},
    )

    auth = connector.resolve_authentication()

    assert auth.headers == {"Authorization": "Bearer super-secret-token"}
    assert auth.redacted_fields == {"headers.Authorization"}


def test_rest_connector_resolves_basic_authentication() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "basic",
            "secret_refs": {
                "username": "ORDERS_API_USERNAME",
                "password": "ORDERS_API_PASSWORD",
            },
        },
        secrets={
            "ORDERS_API_USERNAME": "alice",
            "ORDERS_API_PASSWORD": "s3cr3t",
        },
    )

    auth = connector.resolve_authentication()

    encoded = base64.b64encode(b"alice:s3cr3t").decode("ascii")
    assert auth.headers == {"Authorization": f"Basic {encoded}"}
    assert auth.redacted_fields == {"headers.Authorization"}


def test_rest_connector_resolves_api_key_query_param_authentication() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "api_key",
            "injection_location": "query_param",
            "injection_name": "api_key",
            "secret_refs": {"api_key": "ORDERS_API_KEY"},
        },
        secrets={"ORDERS_API_KEY": "abc123"},
    )

    auth = connector.resolve_authentication()

    assert auth.query_params == {"api_key": "abc123"}
    assert auth.redacted_fields == {"query_params.api_key"}


def test_rest_connector_resolves_api_key_body_field_authentication() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "api_key",
            "injection_location": "body_field",
            "injection_name": "api_key",
            "secret_refs": {"api_key": "ORDERS_API_KEY"},
        },
        secrets={"ORDERS_API_KEY": "body-token"},
        request={
            "method": "POST",
            "path": "/v1/orders",
            "body_template": {"existing": "value"},
        },
    )

    request = connector.build_request_plan(
        checkpoint_before=None,
        window=None,
        auth=connector.resolve_authentication(),
    )[0]

    assert request.body == {"existing": "value", "api_key": "body-token"}


def test_rest_connector_resolves_client_credentials_authentication() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "client_credentials",
            "secret_refs": {
                "client_id": "ORDERS_API_CLIENT_ID",
                "client_secret": "ORDERS_API_CLIENT_SECRET",
            },
            "token_request": {
                "method": "POST",
                "path": "/oauth/token",
                "headers": {"Content-Type": "application/json"},
                "body_template": {
                    "grant_type": "client_credentials",
                    "client_id": "{secret.client_id}",
                    "client_secret": "{secrets.client_secret}",
                    "requested_by": "{run.id}",
                },
            },
            "token_response_path": "data.access_token",
        },
        secrets={
            "ORDERS_API_CLIENT_ID": "client-123",
            "ORDERS_API_CLIENT_SECRET": "secret-456",
        },
        token_response=RestResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body='{"data":{"access_token":"issued-token"}}',
            received_at=datetime(2026, 1, 5, 10, 31, tzinfo=UTC),
            content_type="application/json",
        ),
    )

    auth = connector.resolve_authentication()

    assert auth.headers == {"Authorization": "Bearer issued-token"}
    assert connector.executed_requests[0].url == "https://api.example.com/oauth/token"
    assert connector.executed_requests[0].body == {
        "grant_type": "client_credentials",
        "client_id": "client-123",
        "client_secret": "secret-456",
        "requested_by": "run-auth-123",
    }


def test_rest_connector_resolves_client_credentials_query_param_injection() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "client_credentials",
            "injection_location": "query_param",
            "injection_name": "access_token",
            "injection_scheme": "",
            "secret_refs": {"client_id": "ORDERS_API_CLIENT_ID"},
            "token_request": {
                "method": "POST",
                "path": "/oauth/token",
                "body_template": {"client_id": "{secret.client_id}"},
            },
        },
        secrets={"ORDERS_API_CLIENT_ID": "client-123"},
        token_response=RestResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body='{"access_token":"query-token"}',
            received_at=datetime(2026, 1, 5, 10, 31, tzinfo=UTC),
            content_type="application/json",
        ),
    )

    auth = connector.resolve_authentication()

    assert auth.query_params == {"access_token": "query-token"}
    assert auth.redacted_fields == {"query_params.access_token"}


def test_rest_connector_rejects_missing_client_credentials_token_path() -> None:
    connector = AuthResolvingRestConnector(
        run_context=_build_auth_run_context(),
        auth={
            "strategy": "client_credentials",
            "secret_refs": {"client_id": "ORDERS_API_CLIENT_ID"},
            "token_request": {
                "method": "POST",
                "path": "/oauth/token",
                "body_template": {"client_id": "{secret.client_id}"},
            },
            "token_response_path": "data.access_token",
        },
        secrets={"ORDERS_API_CLIENT_ID": "client-123"},
        token_response=RestResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body='{"access_token":"query-token"}',
            received_at=datetime(2026, 1, 5, 10, 31, tzinfo=UTC),
            content_type="application/json",
        ),
    )

    with pytest.raises(ConfigValidationError, match="did not resolve to a value"):
        connector.resolve_authentication()


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


class TemplatedRestConnector(RestConnectorBase):
    def __init__(self, *, run_context: RunContext) -> None:
        super().__init__(
            config=RestConnectorConfig(
                schema_version="v1",
                environment="dev",
                source_name="orders_api",
                entity_name="orders",
                execution_mode="scheduled_batch",
                base_url="https://api.example.com",
                request={
                    "method": "POST",
                    "path": "/v1/orders/{window.start_date}",
                    "headers": {
                        "Accept": "application/json",
                        "X-Window-Label": "{window.label}",
                    },
                    "query_params": {
                        "page": "{checkpoint.page}",
                        "start": "{window.start}",
                        "end": "{window.end}",
                    },
                    "body_template": {
                        "window": {
                            "start": "{window.start}",
                            "end": "{window.end}",
                        },
                        "metadata": [
                            "{run.id}",
                            "{window.start_date}",
                            "{run.trigger_type}",
                        ],
                        "watermark": "{checkpoint.watermark}",
                    },
                },
            ),
            run_context=run_context,
        )

    def execute_request(self, request: RestPreparedRequest) -> RestResponse:
        raise NotImplementedError

    def persist_response(self, **kwargs):
        raise NotImplementedError


class AuthResolvingRestConnector(RestConnectorBase):
    def __init__(
        self,
        *,
        run_context: RunContext,
        auth: dict[str, object],
        secrets: dict[str, str],
        request: dict[str, object] | None = None,
        token_response: RestResponse | None = None,
    ) -> None:
        self.executed_requests: list[RestPreparedRequest] = []
        self.secrets = secrets
        self.token_response = token_response
        super().__init__(
            config=RestConnectorConfig(
                schema_version="v1",
                environment="dev",
                source_name="orders_api",
                entity_name="orders",
                execution_mode="scheduled_batch",
                base_url="https://api.example.com",
                auth=auth,
                request=request or {"method": "GET", "path": "/v1/orders"},
            ),
            run_context=run_context,
        )

    def resolve_secret(self, *, secret_name: str, secret_ref: str) -> str:
        return self.secrets[secret_ref]

    def execute_request(self, request: RestPreparedRequest) -> RestResponse:
        self.executed_requests.append(request)
        if self.token_response is None:
            raise NotImplementedError
        return self.token_response

    def persist_response(self, **kwargs):
        raise NotImplementedError


def _build_auth_run_context() -> RunContext:
    return RunContext(
        run_id="run-auth-123",
        stage=StageName.ingest,
        job_name="orders-ingest",
        trigger_type="manual",
        started_at=datetime(2026, 1, 5, 10, 30, tzinfo=UTC),
    )

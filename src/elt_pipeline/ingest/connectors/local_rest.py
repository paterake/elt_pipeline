from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from elt_pipeline.ingest.connectors.rest import (
    RestConnectorBase,
    RestConnectorConfig,
    RestPaginationMode,
    RestPreparedRequest,
    RestRequestWindow,
    RestResponse,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.runtime import RunContext

_SAFE_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_fragment(value: Any) -> str:
    cleaned = _SAFE_FRAGMENT.sub("_", str(value).strip())
    return cleaned or "unknown"


def _merge_query_params(url: str, params: dict[str, Any]) -> str:
    if not params:
        return url
    parsed = urllib.parse.urlsplit(url)
    existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    merged = {**existing, **{key: str(value) for key, value in params.items() if value is not None}}
    query = urllib.parse.urlencode(merged, doseq=True)
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment)
    )


class LocalRestConnector(RestConnectorBase):
    def __init__(
        self,
        *,
        config: RestConnectorConfig,
        run_context: RunContext,
        root_path: str,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)

    def execute_request(self, request: RestPreparedRequest) -> RestResponse:
        url = _merge_query_params(request.url, request.query_params)
        headers = dict(request.headers)
        data: bytes | None = None
        if request.body is not None:
            if isinstance(request.body, bytes):
                data = request.body
            elif isinstance(request.body, str):
                data = request.body.encode("utf-8")
            else:
                payload_format = (self.config.request.payload_format or "").strip().lower()
                if payload_format == "json":
                    headers.setdefault("Content-Type", "application/json")
                    data = json.dumps(request.body).encode("utf-8")
                else:
                    data = str(request.body).encode("utf-8")

        request_obj = urllib.request.Request(
            url=url,
            data=data,
            headers=headers,
            method=request.method,
        )

        received_at = datetime.now(tz=UTC)
        try:
            with urllib.request.urlopen(request_obj, timeout=request.timeout_seconds) as handle:
                body = handle.read()
                response_headers = {key: value for key, value in handle.headers.items()}
                content_type = response_headers.get("Content-Type")
                return RestResponse(
                    status_code=handle.status,
                    headers=response_headers,
                    body=body,
                    received_at=received_at,
                    content_type=content_type,
                    metadata={},
                )
        except urllib.error.HTTPError as exc:
            body = exc.read()
            response_headers = {key: value for key, value in exc.headers.items()}
            content_type = response_headers.get("Content-Type")
            return RestResponse(
                status_code=exc.code,
                headers=response_headers,
                body=body,
                received_at=received_at,
                content_type=content_type,
                metadata={},
            )
        except TimeoutError:
            raise
        except Exception as exc:
            timeout_exc = getattr(exc, "reason", None)
            if isinstance(timeout_exc, TimeoutError):
                raise timeout_exc from exc
            raise

    def persist_response(
        self,
        *,
        request: RestPreparedRequest,
        response: RestResponse,
        checkpoint_before: dict[str, Any] | None,
        window: RestRequestWindow | None,
    ) -> list[Level1ArtifactManifest]:
        manifests: list[Level1ArtifactManifest] = []
        envelope_manifest = self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=response.body,
            payload_format=self.config.request.payload_format,
            extraction_mode=self.config.execution_mode,
            artifact_name=self._artifact_name_for_request(request, suffix="envelope"),
            checkpoint_before=checkpoint_before,
            window_start=window.start if window else None,
            window_end=window.end if window else None,
            window_label=window.label if window else None,
            metadata={
                "request_method": request.method,
                "request_url": request.url,
                "status_code": response.status_code,
                "response_content_type": response.content_type,
                "pagination": request.metadata.get("pagination"),
            },
            ingest_completed_at=response.received_at,
        )
        manifests.append(envelope_manifest)

        extracted = response.metadata.get("extracted_items")
        if extracted is not None:
            extracted_manifest = self.writer.write_payload(
                run_context=self.run_context,
                environment=self.config.environment,
                source_name=self.config.source_name,
                entity_name=self.config.entity_name,
                payload=json.dumps(extracted, sort_keys=True),
                payload_format="json",
                extraction_mode=self.config.execution_mode,
                artifact_name=self._artifact_name_for_request(request, suffix="items"),
                checkpoint_before=checkpoint_before,
                window_start=window.start if window else None,
                window_end=window.end if window else None,
                window_label=window.label if window else None,
                metadata={
                    "envelope_artifact_id": envelope_manifest.artifact_id,
                    "envelope_manifest_path": envelope_manifest.manifest_path,
                    "response_items_path": self.config.request.response_items_path,
                    "pagination": request.metadata.get("pagination"),
                },
                ingest_completed_at=response.received_at,
            )
            manifests.append(extracted_manifest)

        return manifests

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        requests: list[RestPreparedRequest],
        responses: list[RestResponse],
        manifests: list[Level1ArtifactManifest],
        window: RestRequestWindow | None,
    ) -> dict[str, Any] | None:
        pagination = self.config.pagination
        if not requests or pagination.mode == RestPaginationMode.none:
            return checkpoint_before
        checkpoint_after = dict(checkpoint_before or {})
        last_request = requests[-1]

        if pagination.mode == RestPaginationMode.page:
            page_value = last_request.query_params.get(pagination.page_parameter_name)
            if page_value is not None:
                checkpoint_after[pagination.page_parameter_name] = int(page_value) + 1
                return checkpoint_after
            return checkpoint_before

        if pagination.mode == RestPaginationMode.offset:
            offset_value = last_request.query_params.get(pagination.offset_parameter_name)
            page_size = pagination.page_size or last_request.query_params.get(
                pagination.page_size_parameter_name
            )
            if offset_value is not None and page_size is not None:
                checkpoint_after[pagination.offset_parameter_name] = int(offset_value) + int(
                    page_size
                )
                return checkpoint_after
            return checkpoint_before

        if pagination.mode == RestPaginationMode.cursor:
            next_cursor = responses[-1].metadata.get("next_cursor") if responses else None
            if next_cursor is not None and not isinstance(next_cursor, (dict, list)):
                checkpoint_after[pagination.cursor_parameter_name] = str(next_cursor)
                return checkpoint_after
            return checkpoint_before

        return checkpoint_before

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
        window: RestRequestWindow | None,
    ) -> None:
        if checkpoint_after is None:
            return None
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
        return None

    def _artifact_name_for_request(
        self,
        request: RestPreparedRequest,
        *,
        suffix: str,
    ) -> str | None:
        base_name = self.config.request.artifact_name
        pagination = self.config.pagination
        fragments: list[str] = []
        if pagination.mode == RestPaginationMode.page:
            value = request.query_params.get(pagination.page_parameter_name)
            if value is not None:
                fragments.append(f"page-{_safe_fragment(value)}")
        if pagination.mode == RestPaginationMode.offset:
            value = request.query_params.get(pagination.offset_parameter_name)
            if value is not None:
                fragments.append(f"offset-{_safe_fragment(value)}")
        if pagination.mode == RestPaginationMode.cursor:
            value = request.query_params.get(pagination.cursor_parameter_name)
            if value is not None:
                fragments.append(f"cursor-{_safe_fragment(value)}")
        fragments.append(suffix)
        if base_name:
            return "-".join([base_name, *fragments])
        if pagination.mode == RestPaginationMode.none:
            return None
        return "-".join([_safe_fragment(self.config.entity_name), *fragments])


__all__ = ["LocalRestConnector"]

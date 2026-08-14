from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.audit import AuditRecord
from elt_pipeline.shared.errors import ErrorCategory, ErrorRecord, PipelineError
from elt_pipeline.shared.lineage import LineageEvent
from elt_pipeline.shared.logging import ExecutionLogEvent
from elt_pipeline.shared.path_utils import (
    join_paths,
    path_basename,
    path_exists,
    path_mkdir,
    path_normalize,
    path_open_for_append,
    path_parent,
    path_relative_to,
    path_replace,
    path_with_suffix,
    path_write_bytes,
    path_write_text,
)
from elt_pipeline.shared.runtime import RunContext

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")
_FORMAT_EXTENSION_MAP = {
    "json": "json",
    "ndjson": "ndjson",
    "jsonl": "jsonl",
    "csv": "csv",
    "xml": "xml",
    "txt": "txt",
    "parquet": "parquet",
}


def _sanitize_path_fragment(value: str) -> str:
    cleaned = _SAFE_PATH_FRAGMENT.sub("_", value.strip())
    return cleaned or "unknown"


def _write_json_file(path: str, payload: BaseModel | dict[str, Any]) -> None:
    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    data = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    temp_path = path_with_suffix(path, ".json.tmp")
    path_write_text(
        temp_path,
        json.dumps(data, indent=2, sort_keys=True),
        encoding="utf-8",
        atomic=True,
    )
    path_replace(temp_path, path)


def _append_jsonl_file(path: str, payload: BaseModel | dict[str, Any]) -> None:
    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    data = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    with path_open_for_append(path, encoding="utf-8") as handle:
        handle.write(json.dumps(data, sort_keys=True))
        handle.write("\n")


_NO_ENV_IN_PATH_MESSAGE = (
    "Generated path contains 'environment=' segment; per P5 decision, environment is "
    "handled exclusively by which root path is pointed at, never as an in-path segment."
)


class LocalArtifactLayout:
    def __init__(self, root_path: str) -> None:
        self.root_path: str = path_normalize(root_path)

    def level1_run_dir(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        ingested_at: datetime,
        run_id: str,
        window_label: str | None = None,
    ) -> str:
        _ = environment
        segments = [
            "level1",
            f"source={_sanitize_path_fragment(source_name)}",
            f"entity={_sanitize_path_fragment(entity_name)}",
            f"ingest_date={ingested_at.date().isoformat()}",
        ]
        if window_label:
            segments.append(f"window={_sanitize_path_fragment(window_label)}")
        segments.append(f"run_id={run_id}")
        result = join_paths(self.root_path, *segments)
        assert "environment=" not in result, _NO_ENV_IN_PATH_MESSAGE
        return result

    def run_dir(self, *, run_context: RunContext, environment: str) -> str:
        _ = environment
        result = join_paths(
            self.root_path,
            "runs",
            f"stage={run_context.stage.value}",
            f"job={_sanitize_path_fragment(run_context.job_name)}",
            f"run_id={run_context.run_id}",
        )
        assert "environment=" not in result, _NO_ENV_IN_PATH_MESSAGE
        return result

    def state_file(self, *, environment: str, source_name: str, entity_name: str) -> str:
        _ = environment
        result = join_paths(
            self.root_path,
            "state",
            f"source={_sanitize_path_fragment(source_name)}",
            f"entity={_sanitize_path_fragment(entity_name)}.json",
        )
        assert "environment=" not in result, _NO_ENV_IN_PATH_MESSAGE
        return result


class LocalLevel1Writer:
    def __init__(self, root_path: str) -> None:
        self.layout = LocalArtifactLayout(root_path)

    def write_payload(
        self,
        *,
        run_context: RunContext,
        environment: str,
        source_name: str,
        entity_name: str,
        payload: bytes | str,
        payload_format: str,
        extraction_mode: str,
        artifact_name: str | None = None,
        file_extension: str | None = None,
        compression: str | None = None,
        checkpoint_before: dict[str, Any] | None = None,
        checkpoint_after: dict[str, Any] | None = None,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        window_label: str | None = None,
        record_count_estimate: int | None = None,
        metadata: dict[str, Any] | None = None,
        ingest_completed_at: datetime | None = None,
    ) -> Level1ArtifactManifest:
        completed_at = ingest_completed_at or datetime.now(tz=UTC)
        payload_bytes = payload if isinstance(payload, bytes) else payload.encode("utf-8")

        extension = file_extension or _FORMAT_EXTENSION_MAP.get(payload_format, payload_format)
        extension = extension.lstrip(".")
        base_name = artifact_name or (
            f"{_sanitize_path_fragment(entity_name)}-{run_context.run_id[:8]}"
        )
        file_name = (
            f"{_sanitize_path_fragment(base_name)}.{extension}" if extension else base_name
        )

        data_dir = self.layout.level1_run_dir(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
            ingested_at=run_context.started_at,
            run_id=run_context.run_id,
            window_label=window_label,
        )
        data_path = join_paths(data_dir, file_name)

        if path_exists(data_path):
            raise PipelineError(
                message=f"Refusing to overwrite existing level1 artifact: {data_path}",
                error_code="LEVEL1_ARTIFACT_EXISTS",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={"path": data_path},
            )

        path_mkdir(path_parent(data_path), parents=True, exist_ok=True)
        path_write_bytes(data_path, payload_bytes, atomic=True)

        manifest_path = join_paths(
            data_dir,
            f"{path_basename(data_path)}.manifest.json",
        )
        relative_data_path = path_relative_to(data_path, self.layout.root_path)
        relative_manifest_path = path_relative_to(manifest_path, self.layout.root_path)
        artifact_id = hashlib.sha256(
            f"{run_context.run_id}:{relative_data_path}".encode("utf-8"),
        ).hexdigest()[:24]
        manifest = Level1ArtifactManifest(
            artifact_id=artifact_id,
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            trigger_type=run_context.trigger_type,
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
            extraction_mode=extraction_mode,
            ingest_started_at=run_context.started_at,
            ingest_completed_at=completed_at,
            window_start=window_start,
            window_end=window_end,
            window_label=window_label,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            payload_format=payload_format,
            compression=compression,
            record_count_estimate=record_count_estimate,
            content_hash=hashlib.sha256(payload_bytes).hexdigest(),
            file_size_bytes=len(payload_bytes),
            data_path=relative_data_path,
            manifest_path=relative_manifest_path,
            metadata=metadata or {},
        )
        _write_json_file(manifest_path, manifest)
        return manifest


class LocalArtifactStore:
    def __init__(self, root_path: str) -> None:
        self.layout = LocalArtifactLayout(root_path)

    def write_audit_record(
        self,
        *,
        run_context: RunContext,
        environment: str,
        audit_record: AuditRecord,
    ) -> str:
        path = join_paths(
            self.layout.run_dir(run_context=run_context, environment=environment),
            "audit.json",
        )
        _write_json_file(path, audit_record)
        return path

    def append_log_event(
        self,
        *,
        run_context: RunContext,
        environment: str,
        log_event: ExecutionLogEvent,
    ) -> str:
        path = join_paths(
            self.layout.run_dir(run_context=run_context, environment=environment),
            "logs.jsonl",
        )
        _append_jsonl_file(path, log_event)
        return path

    def append_error_record(
        self,
        *,
        run_context: RunContext,
        environment: str,
        error_record: ErrorRecord,
    ) -> str:
        path = join_paths(
            self.layout.run_dir(run_context=run_context, environment=environment),
            "errors.jsonl",
        )
        _append_jsonl_file(path, error_record)
        return path

    def append_lineage_event(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> str:
        path = join_paths(
            self.layout.run_dir(run_context=run_context, environment=environment),
            "lineage.jsonl",
        )
        _append_jsonl_file(path, lineage_event)
        return path

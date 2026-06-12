from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.models import Level2TableManifest, NormalizedTable
from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.runtime import RunContext

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_path_fragment(value: str) -> str:
    cleaned = _SAFE_PATH_FRAGMENT.sub("_", value.strip())
    return cleaned or "unknown"


def _append_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


class LocalLevel2Layout:
    def __init__(self, root_path: Path) -> None:
        self.root_path = root_path

    def table_run_dir(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        mapping_version: str,
        partition: dict[str, str],
        table_name: str,
        run_id: str,
    ) -> Path:
        path = (
            self.root_path
            / "level2"
            / f"environment={_sanitize_path_fragment(environment)}"
            / f"source={_sanitize_path_fragment(source_name)}"
            / f"entity={_sanitize_path_fragment(entity_name)}"
            / f"mapping_version={_sanitize_path_fragment(mapping_version)}"
        )
        for key in sorted(partition):
            path /= f"{_sanitize_path_fragment(key)}={_sanitize_path_fragment(partition[key])}"
        path /= f"table={_sanitize_path_fragment(table_name)}"
        return path / f"run_id={_sanitize_path_fragment(run_id)}"


class LocalLevel2Writer:
    def __init__(self, root_path: Path) -> None:
        self.layout = LocalLevel2Layout(root_path)

    def write_table(
        self,
        *,
        run_context: RunContext,
        manifest: Level1ArtifactManifest,
        mapping_version: str,
        table: NormalizedTable,
        partition: dict[str, str],
        normalize_completed_at: datetime | None = None,
    ) -> Level2TableManifest:
        completed_at = normalize_completed_at or datetime.now(tz=UTC)
        data_dir = self.layout.table_run_dir(
            environment=manifest.environment,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            mapping_version=mapping_version,
            partition=partition,
            table_name=table.physical_name,
            run_id=run_context.run_id,
        )
        data_path = data_dir / "data.jsonl"
        if data_path.exists():
            raise PipelineError(
                message=f"Refusing to overwrite existing level2 artifact: {data_path}",
                error_code="LEVEL2_ARTIFACT_EXISTS",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={"path": str(data_path)},
            )

        _append_jsonl_rows(data_path, table.rows)

        relative_data_path = data_path.relative_to(self.layout.root_path).as_posix()
        manifest_path = data_path.with_suffix(f"{data_path.suffix}.manifest.json")
        relative_manifest_path = manifest_path.relative_to(self.layout.root_path).as_posix()
        artifact_id = hashlib.sha256(
            f"{run_context.run_id}:{relative_data_path}".encode("utf-8")
        ).hexdigest()[:24]
        manifest_payload = Level2TableManifest(
            artifact_id=artifact_id,
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            trigger_type=run_context.trigger_type,
            environment=manifest.environment,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            mapping_version=mapping_version,
            input_artifact_id=manifest.artifact_id,
            input_data_path=manifest.data_path,
            input_manifest_path=manifest.manifest_path,
            table_name=table.physical_name,
            partition=partition,
            normalize_started_at=run_context.started_at,
            normalize_completed_at=completed_at,
            record_count=len(table.rows),
            file_size_bytes=data_path.stat().st_size,
            data_path=relative_data_path,
            manifest_path=relative_manifest_path,
        )
        _write_json_file(manifest_path, manifest_payload.model_dump(mode="json"))
        return manifest_payload


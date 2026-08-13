from __future__ import annotations

import json
import posixpath
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from elt_pipeline.ingest.models import CheckpointHistoryEntry, CheckpointStateDocument
from elt_pipeline.ingest.storage import LocalArtifactLayout
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.path_utils import (
    path_exists,
    path_mkdir,
    path_parent,
    path_read_text,
    path_replace,
    path_with_suffix,
    path_write_text,
)


def _write_state_file(path: str, document: CheckpointStateDocument) -> None:
    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    suf = posixpath.splitext(path)[1] or ".json"
    temp_path = path_with_suffix(path, f"{suf}.tmp")
    path_write_text(
        temp_path,
        json.dumps(document.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
        atomic=True,
    )
    path_replace(temp_path, path)


class LocalCheckpointStore:
    def __init__(self, root_path: str) -> None:
        self.layout = LocalArtifactLayout(root_path)

    def load(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
    ) -> CheckpointStateDocument:
        path = self.layout.state_file(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        if not path_exists(path):
            return CheckpointStateDocument(
                environment=environment,
                source_name=source_name,
                entity_name=entity_name,
            )
        payload = path_read_text(path, encoding="utf-8")
        try:
            return CheckpointStateDocument.model_validate_json(payload)
        except ValidationError as exc:
            raise ConfigValidationError(
                message="Checkpoint state document is malformed",
                context={
                    "state_path": path,
                    "source_name": source_name,
                    "entity_name": entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc

    def commit(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        run_id: str,
        checkpoint_after: dict[str, Any],
        recorded_at: datetime,
        checkpoint_before: dict[str, Any] | None = None,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        window_label: str | None = None,
        manifest_paths: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> CheckpointHistoryEntry:
        document = self.load(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        entry = CheckpointHistoryEntry(
            run_id=run_id,
            recorded_at=recorded_at,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            window_start=window_start,
            window_end=window_end,
            window_label=window_label,
            manifest_paths=manifest_paths or [],
            metadata=metadata or {},
        )
        document.current_checkpoint = checkpoint_after
        document.updated_at = recorded_at
        document.updated_by_run_id = run_id
        document.history.append(entry)

        path = self.layout.state_file(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        _write_state_file(path, document)
        return entry

    def list_history(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
    ) -> list[CheckpointHistoryEntry]:
        document = self.load(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        return list(document.history)

    def find_replay_entries(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        run_id: str | None = None,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> list[CheckpointHistoryEntry]:
        entries = self.list_history(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        matches: list[CheckpointHistoryEntry] = []
        for entry in entries:
            if run_id and entry.run_id != run_id:
                continue
            if window_start and entry.window_end and entry.window_end < window_start:
                continue
            if window_end and entry.window_start and entry.window_start > window_end:
                continue
            matches.append(entry)
        return matches

    def resolve_backfill_seed(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        window_start: datetime,
    ) -> CheckpointHistoryEntry | None:
        entries = self.list_history(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        )
        eligible = [
            entry
            for entry in entries
            if entry.window_end is not None and entry.window_end <= window_start
        ]
        if not eligible:
            return None
        return max(eligible, key=lambda entry: entry.window_end or entry.recorded_at)

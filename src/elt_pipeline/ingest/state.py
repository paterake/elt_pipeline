from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from elt_pipeline.ingest.models import CheckpointHistoryEntry, CheckpointStateDocument
from elt_pipeline.ingest.storage import LocalArtifactLayout


def _write_state_file(path: Path, document: CheckpointStateDocument) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(document.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(path)


class LocalCheckpointStore:
    def __init__(self, root_path: Path) -> None:
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
        if not path.exists():
            return CheckpointStateDocument(
                environment=environment,
                source_name=source_name,
                entity_name=entity_name,
            )
        return CheckpointStateDocument.model_validate_json(path.read_text(encoding="utf-8"))

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

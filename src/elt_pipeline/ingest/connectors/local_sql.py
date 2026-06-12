from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from elt_pipeline.ingest.connectors.sql import (
    SqlConnectionDriver,
    SqlConnectorBase,
    SqlConnectorConfig,
    SqlPreparedQuery,
    SqlQueryResult,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import RunContext


class LocalSqlConnector(SqlConnectorBase):
    def __init__(
        self,
        *,
        config: SqlConnectorConfig,
        run_context: RunContext,
        root_path: Path,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)

    def validate_config(self) -> SqlConnectorConfig:
        config = super().validate_config()
        if config.connection.driver != SqlConnectionDriver.sqlite:
            raise ConfigValidationError(
                message="Local SQL connector only supports sqlite for v1",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                    "driver": config.connection.driver.value,
                },
            )
        return config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        checkpoint_document = self.checkpoint_store.load(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return checkpoint_document.current_checkpoint

    def execute_query(self, query: SqlPreparedQuery) -> SqlQueryResult:
        executed_at = datetime.now(tz=UTC)
        with sqlite3.connect(self.config.connection.database) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.execute(query.sql, query.parameters)
            rows = [dict(row) for row in cursor.fetchall()]
            columns = [description[0] for description in (cursor.description or [])]
        return SqlQueryResult(
            rows=rows,
            columns=columns,
            executed_at=executed_at,
            metadata=query.metadata,
        )

    def persist_query_result(
        self,
        *,
        query: SqlPreparedQuery,
        result: SqlQueryResult,
        checkpoint_before: dict[str, Any] | None,
    ) -> list[Level1ArtifactManifest]:
        payload = json.dumps(
            {
                "columns": result.columns,
                "row_count": result.row_count,
                "rows": result.rows,
            },
            sort_keys=True,
            default=_json_default,
        )
        manifest = self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=payload,
            payload_format="json",
            extraction_mode=self.config.extraction_mode.value,
            artifact_name=self.config.query.artifact_name,
            checkpoint_before=checkpoint_before,
            record_count_estimate=result.row_count,
            metadata={
                "compiled_sql": query.metadata.get("compiled_sql"),
                "compiled_parameters": query.metadata.get("compiled_parameters"),
                "columns": result.columns,
            },
            ingest_completed_at=result.executed_at,
        )
        return [manifest]

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "sql"},
        )
        return None


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


__all__ = ["LocalSqlConnector"]

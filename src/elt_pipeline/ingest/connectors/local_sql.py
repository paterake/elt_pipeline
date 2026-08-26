from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

from elt_pipeline.ingest.connectors.sql import (
    SqlConnectorBase,
    SqlConnectorConfig,
    SqlDbDriver,
    SqlPreparedQuery,
    SqlQueryResult,
    _build_db_driver,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.runtime import RunContext


class LocalSqlConnector(SqlConnectorBase):
    def __init__(
        self,
        *,
        config: SqlConnectorConfig,
        run_context: RunContext,
        root_path: str,
        driver_override: SqlDbDriver | None = None,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)
        self._driver_override = driver_override
        self._driver_cached: SqlDbDriver | None = None

    @property
    def _driver(self) -> SqlDbDriver:
        if self._driver_cached is None:
            if self._driver_override is not None:
                self._driver_cached = self._driver_override
            else:
                self._driver_cached = _build_db_driver(self.config.connection.driver)
        return self._driver_cached

    def validate_config(self) -> SqlConnectorConfig:
        return super().validate_config()

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        checkpoint_document = self.checkpoint_store.load(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return checkpoint_document.current_checkpoint

    def execute_query(self, query: SqlPreparedQuery) -> SqlQueryResult:
        executed_at = datetime.now(tz=UTC)
        fetch_size = (
            self.config.query.fetch_size if self.config.query.fetch_size >= 1 else 1000
        )
        connection = self._driver.connect(
            database=self.config.connection.database,
            options=self.config.connection.options,
        )
        try:
            cursor = connection.cursor()
            if query.parameters:
                cursor.execute(query.sql, query.parameters)
            else:
                cursor.execute(query.sql)
            rows: list[dict[str, Any]] = []
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    if hasattr(row, "keys") and callable(getattr(row, "keys", None)):
                        rows.append(dict(row))
                    else:
                        cols = [
                            description[0]
                            for description in (cursor.description or [])
                        ]
                        rows.append(
                            dict(zip(cols, row, strict=False)) if cols else dict(row)
                        )
            columns = [
                description[0] for description in (cursor.description or [])
            ]
        finally:
            connection.close()
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
                "driver": self.config.connection.driver.value,
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
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


__all__ = ["LocalSqlConnector"]

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    LocalSqlConnector,
    SqlConnectorBase,
    SqlConnectorConfig,
    SqlPreparedQuery,
    SqlQueryResult,
)
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_sql_connector_config_builds_from_resolved_entity_config(tmp_path: Path) -> None:
    database_path = tmp_path / "orders.db"
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_db",
        entity_name="orders",
        connector_type="sql",
        trigger_mode="scheduled_batch",
        extraction={
            "database": str(database_path),
            "mode": "delta",
            "query": {
                "sql": "select * from orders where updated_at > :watermark",
                "parameters": {"watermark": "{watermark.value}"},
                "artifact_name": "orders-delta",
            },
            "watermark": {
                "column_name": "updated_at",
                "checkpoint_key": "max_updated_at",
                "default_value": "2026-01-01T00:00:00+00:00",
            },
        },
    )

    connector_config = SqlConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.connection.driver.value == "sqlite"
    assert connector_config.connection.database == str(database_path)
    assert connector_config.extraction_mode.value == "delta"
    assert connector_config.query.sql == "select * from orders where updated_at > :watermark"
    assert connector_config.query.parameters == {"watermark": "{watermark.value}"}
    assert connector_config.watermark is not None
    assert connector_config.watermark.column_name == "updated_at"
    assert connector_config.watermark.checkpoint_key == "max_updated_at"


def test_sql_connector_config_rejects_non_sql_connector() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_api",
        entity_name="orders",
        connector_type="rest",
    )

    with pytest.raises(ConfigValidationError, match="not a SQL connector"):
        SqlConnectorConfig.from_resolved_entity_config(resolved_config)


def test_sql_connector_build_query_plan_renders_templates(tmp_path: Path) -> None:
    connector = InspectableSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="orders_db",
            entity_name="orders",
            execution_mode="scheduled_batch",
            extraction_mode="delta",
            connection={"database": str(tmp_path / "orders.db")},
            query={
                "sql": (
                    "select * from orders "
                    "where tenant = '{environment}' and updated_at > :min_updated_at"
                ),
                "parameters": {
                    "min_updated_at": "{watermark.value}",
                    "entity_name": "{entity.name}",
                    "run_id": "{run.id}",
                },
            },
            watermark={
                "column_name": "updated_at",
                "checkpoint_key": "max_updated_at",
                "default_value": "2026-01-01T00:00:00+00:00",
            },
        )
    )

    queries = connector.build_query_plan(
        checkpoint_before={"batch_id": 7},
        watermark_value="2026-01-04T00:00:00+00:00",
    )

    assert len(queries) == 1
    assert queries[0].sql == (
        "select * from orders where tenant = 'dev' and updated_at > :min_updated_at"
    )
    assert queries[0].parameters == {
        "min_updated_at": "2026-01-04T00:00:00+00:00",
        "entity_name": "orders",
        "run_id": connector.run_context.run_id,
    }
    assert queries[0].metadata["watermark_value"] == "2026-01-04T00:00:00+00:00"


def test_local_sql_connector_delta_run_persists_rows_and_updates_checkpoint(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "orders.db"
    _create_orders_db(
        database_path,
        [
            (1, "2026-01-02T00:00:00+00:00", "new"),
            (2, "2026-01-03T00:00:00+00:00", "paid"),
            (3, "2026-01-05T00:00:00+00:00", "shipped"),
        ],
    )
    connector = LocalSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="orders_db",
            entity_name="orders",
            execution_mode="scheduled_batch",
            extraction_mode="delta",
            connection={"database": str(database_path)},
            query={
                "sql": (
                    "select id, updated_at, status from orders "
                    "where updated_at > :watermark order by updated_at"
                ),
                "parameters": {"watermark": "{watermark.value}"},
                "artifact_name": "orders-delta",
            },
            watermark={
                "column_name": "updated_at",
                "checkpoint_key": "max_updated_at",
                "default_value": "2026-01-02T12:00:00+00:00",
            },
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="orders-sql-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=tmp_path,
    )

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="orders_db",
        entity_name="orders",
    )

    assert result.query_count == 1
    assert result.row_count == 2
    assert result.checkpoint_before is None
    assert result.checkpoint_after == {"max_updated_at": "2026-01-05T00:00:00+00:00"}
    assert len(result.manifests) == 1
    assert checkpoint_document.current_checkpoint == {"max_updated_at": "2026-01-05T00:00:00+00:00"}
    assert checkpoint_document.history[0].manifest_paths == [result.manifests[0].manifest_path]

    manifest_path = tmp_path / result.manifests[0].manifest_path
    data_path = tmp_path / result.manifests[0].data_path
    payload = json.loads(data_path.read_text(encoding="utf-8"))
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["row_count"] == 2
    assert [row["id"] for row in payload["rows"]] == [2, 3]
    assert manifest_payload["record_count_estimate"] == 2
    assert manifest_payload["metadata"]["compiled_parameters"] == {
        "watermark": "2026-01-02T12:00:00+00:00"
    }


def test_local_sql_connector_uses_saved_checkpoint_on_next_delta_run(tmp_path: Path) -> None:
    database_path = tmp_path / "orders.db"
    _create_orders_db(
        database_path,
        [
            (1, "2026-01-03T00:00:00+00:00", "new"),
            (2, "2026-01-05T00:00:00+00:00", "paid"),
            (3, "2026-01-06T00:00:00+00:00", "shipped"),
        ],
    )
    checkpoint_store = LocalCheckpointStore(tmp_path)
    checkpoint_store.commit(
        environment="dev",
        source_name="orders_db",
        entity_name="orders",
        run_id="prior-run",
        checkpoint_before={"max_updated_at": "2026-01-03T00:00:00+00:00"},
        checkpoint_after={"max_updated_at": "2026-01-05T00:00:00+00:00"},
        recorded_at=new_run_context(
            stage=StageName.ingest,
            job_name="prior-orders-sql-ingest",
            trigger_type="scheduled_batch",
        ).started_at,
    )
    connector = LocalSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="orders_db",
            entity_name="orders",
            execution_mode="scheduled_batch",
            extraction_mode="delta",
            connection={"database": str(database_path)},
            query={
                "sql": (
                    "select id, updated_at, status from orders "
                    "where updated_at > :watermark order by updated_at"
                ),
                "parameters": {"watermark": "{watermark.value}"},
            },
            watermark={
                "column_name": "updated_at",
                "checkpoint_key": "max_updated_at",
                "default_value": "2026-01-01T00:00:00+00:00",
            },
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="orders-sql-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=tmp_path,
    )

    result = connector.run()

    assert result.checkpoint_before == {"max_updated_at": "2026-01-05T00:00:00+00:00"}
    assert result.row_count == 1
    assert result.checkpoint_after == {"max_updated_at": "2026-01-06T00:00:00+00:00"}


class InspectableSqlConnector(SqlConnectorBase):
    def __init__(self, *, config: SqlConnectorConfig) -> None:
        super().__init__(
            config=config,
            run_context=new_run_context(
                stage=StageName.ingest,
                job_name="orders-sql-build-plan",
                trigger_type="manual",
            ),
        )

    def execute_query(self, query: SqlPreparedQuery) -> SqlQueryResult:
        raise NotImplementedError

    def persist_query_result(self, **kwargs):
        raise NotImplementedError


def _create_orders_db(database_path: Path, rows: list[tuple[int, str, str]]) -> None:
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table orders (
                id integer primary key,
                updated_at text not null,
                status text not null
            )
            """
        )
        connection.executemany(
            "insert into orders (id, updated_at, status) values (?, ?, ?)",
            rows,
        )
        connection.commit()

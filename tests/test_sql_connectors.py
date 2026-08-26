from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    LocalSqlConnector,
    SqlConnectionDriver,
    SqlConnectorBase,
    SqlConnectorConfig,
    SqlDbDriver,
    SqlPreparedQuery,
    SqlQueryResult,
    _build_db_driver,
)
from elt_pipeline.ingest.connectors.registry import (
    ConnectorFamily,
    get_connector_factory,
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
        root_path=str(tmp_path),
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

    manifest_path = tmp_path / Path(result.manifests[0].manifest_path)
    data_path = tmp_path / Path(result.manifests[0].data_path)
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
    checkpoint_store = LocalCheckpointStore(str(tmp_path))
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
        root_path=str(tmp_path),
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


# ---------------------------------------------------------------------------
# M-2 — SQL Multi-DB JDBC source connectors
# ---------------------------------------------------------------------------

def test_sql_connection_driver_enum_extended_to_six_built_in_values() -> None:
    expected = {
        "sqlite",
        "duckdb",
        "postgres",
        "mysql",
        "mssql",
        "jdbc_generic",
    }
    assert {d.value for d in SqlConnectionDriver} == expected


def test_build_db_driver_sqlite_returns_protocol_instance() -> None:
    driver = _build_db_driver(SqlConnectionDriver.sqlite)
    assert isinstance(driver, SqlDbDriver)


def test_build_db_driver_duckdb_sdk_missing_raises_sharp_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins
    real_import = builtins.__import__

    def _bounce(name, *rest, **kw):
        if name == "duckdb":
            raise ImportError("duckdb not installed")
        return real_import(name, *rest, **kw)

    monkeypatch.setattr(builtins, "__import__", _bounce)
    with pytest.raises(
        ConfigValidationError, match="SQL driver 'duckdb' is not installed"
    ) as excinfo:
        _build_db_driver(SqlConnectionDriver.duckdb)
    assert excinfo.value.context["error_code"] == "SQL_DRIVER_SDK_MISSING"
    assert "uv sync --extra duckdb" in excinfo.value.context["install_hint"]


def test_build_db_driver_postgres_sdk_missing_raises_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins
    real_import = builtins.__import__

    def _bounce(name, *rest, **kw):
        if name == "psycopg":
            raise ImportError("psycopg not installed")
        return real_import(name, *rest, **kw)

    monkeypatch.setattr(builtins, "__import__", _bounce)
    with pytest.raises(
        ConfigValidationError, match="SQL driver 'postgres' is not installed"
    ) as excinfo:
        _build_db_driver(SqlConnectionDriver.postgres)
    assert excinfo.value.context["error_code"] == "SQL_DRIVER_SDK_MISSING"
    assert "uv sync --extra postgres" in excinfo.value.context["install_hint"]


def test_build_db_driver_mysql_sdk_missing_raises_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins
    real_import = builtins.__import__

    def _bounce(name, *rest, **kw):
        if name == "mysql.connector":
            raise ImportError("mysql connector not installed")
        return real_import(name, *rest, **kw)

    monkeypatch.setattr(builtins, "__import__", _bounce)
    with pytest.raises(
        ConfigValidationError, match="SQL driver 'mysql' is not installed"
    ) as excinfo:
        _build_db_driver(SqlConnectionDriver.mysql)
    assert excinfo.value.context["error_code"] == "SQL_DRIVER_SDK_MISSING"
    assert "uv sync --extra mysql" in excinfo.value.context["install_hint"]


def test_build_db_driver_mssql_sdk_missing_raises_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins
    real_import = builtins.__import__

    def _bounce(name, *rest, **kw):
        if name == "pymssql":
            raise ImportError("pymssql not installed")
        return real_import(name, *rest, **kw)

    monkeypatch.setattr(builtins, "__import__", _bounce)
    with pytest.raises(
        ConfigValidationError, match="SQL driver 'mssql' is not installed"
    ) as excinfo:
        _build_db_driver(SqlConnectionDriver.mssql)
    assert excinfo.value.context["error_code"] == "SQL_DRIVER_SDK_MISSING"
    assert "uv sync --extra mssql" in excinfo.value.context["install_hint"]


def test_build_db_driver_jdbc_generic_sdk_missing_raises_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins
    real_import = builtins.__import__

    def _bounce(name, *rest, **kw):
        if name == "jaydebeapi":
            raise ImportError("jaydebeapi not installed")
        return real_import(name, *rest, **kw)

    monkeypatch.setattr(builtins, "__import__", _bounce)
    with pytest.raises(
        ConfigValidationError, match="SQL driver 'jdbc_generic' is not installed"
    ) as excinfo:
        _build_db_driver(SqlConnectionDriver.jdbc_generic)
    assert excinfo.value.context["error_code"] == "SQL_DRIVER_SDK_MISSING"
    assert "uv sync --extra jdbc" in excinfo.value.context["install_hint"]


def test_jdbc_generic_driver_requires_jclassname_option() -> None:
    import builtins
    real_import = builtins.__import__

    def _fake_jaydebeapi_import(name, *rest, **kw):
        if name == "jaydebeapi":

            class _FakeJayDeBeApi:
                @staticmethod
                def connect(*_args, **_kwargs):
                    raise AssertionError("should not reach connect without jclassname")

            return _FakeJayDeBeApi()
        return real_import(name, *rest, **kw)

    import unittest.mock as _mock
    with _mock.patch.object(builtins, "__import__", side_effect=_fake_jaydebeapi_import):
        driver = _build_db_driver(SqlConnectionDriver.jdbc_generic)
        with pytest.raises(
            ConfigValidationError,
            match="jdbc_generic SQL driver requires 'jclassname' option",
        ) as excinfo:
            driver.connect(database="jdbc:postgresql://localhost:5432/db", options={})
        assert excinfo.value.context["error_code"] == "SQL_JDBC_JCLASSNAME_REQUIRED"


class _FakeDbCursor:
    def __init__(self, rows: list[dict[str, Any]], columns: list[str]) -> None:
        self._rows = [tuple(r[c] for c in columns) for r in rows]
        self.description = [(c, None) for c in columns]
        self._idx = 0

    def execute(self, _sql: str, _params: Any = None) -> None:
        self._idx = 0

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch = self._rows[self._idx : self._idx + size]
        self._idx += len(batch)
        return batch


class _FakeDbConnection:
    def __init__(self, rows: list[dict[str, Any]], columns: list[str]) -> None:
        self._cursor = _FakeDbCursor(rows, columns)
        self.closed = False

    def cursor(self) -> _FakeDbCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "_FakeDbConnection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class _FakePostgresDriver:
    """In-memory stand-in for the real psycopg driver to exercise driver_override end-to-end."""

    def __init__(self, seed_rows: list[dict[str, Any]]) -> None:
        self.seed_rows = seed_rows
        self.connect_calls: list[tuple[str, dict[str, Any]]] = []

    def connect(self, *, database: str, options: dict[str, Any]) -> _FakeDbConnection:
        self.connect_calls.append((database, dict(options)))
        columns = list(self.seed_rows[0].keys()) if self.seed_rows else []
        return _FakeDbConnection(self.seed_rows, columns)


def test_local_sql_connector_driver_override_dispatches_postgres_style(tmp_path: Path) -> None:
    """Simulate a postgres snapshot run without a real Postgres server using driver_override="""
    seed_rows = [
        {"id": 1, "updated_at": "2026-01-03T00:00:00+00:00", "status": "new"},
        {"id": 2, "updated_at": "2026-01-05T00:00:00+00:00", "status": "paid"},
    ]
    fake_driver = _FakePostgresDriver(seed_rows)
    connector = LocalSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="pg_orders_db",
            entity_name="orders",
            execution_mode="scheduled_batch",
            extraction_mode="snapshot",
            connection={
                "driver": "postgres",
                "database": "shop_prod",
                "options": {
                    "host": "db.internal",
                    "port": 5432,
                    "user": "etl_ro",
                    "sslmode": "require",
                },
            },
            query={
                "sql": "select id, updated_at, status from orders",
                "artifact_name": "orders-snapshot",
            },
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="pg-orders-sql-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=str(tmp_path),
        driver_override=fake_driver,
    )

    result = connector.run()

    assert len(fake_driver.connect_calls) == 1
    database, options = fake_driver.connect_calls[0]
    assert database == "shop_prod"
    assert options["host"] == "db.internal"
    assert options["port"] == 5432
    assert options["sslmode"] == "require"
    assert result.query_count == 1
    assert result.row_count == 2
    assert len(result.manifests) == 1

    manifest_data = json.loads(
        (tmp_path / Path(result.manifests[0].manifest_path)).read_text(encoding="utf-8")
    )
    assert manifest_data["metadata"]["driver"] == "postgres"

    data = json.loads(
        (tmp_path / Path(result.manifests[0].data_path)).read_text(encoding="utf-8")
    )
    assert data["row_count"] == 2
    assert data["columns"] == ["id", "updated_at", "status"]
    assert [row["id"] for row in data["rows"]] == [1, 2]


def test_local_sql_connector_duckdb_real_end_to_end(tmp_path: Path) -> None:
    """REAL integration test using the duckdb SDK (in dev extras) against a temp DB file."""
    duckdb = pytest.importorskip("duckdb", reason="duckdb SDK required for this test")
    db_path = tmp_path / "shop.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            create table products (
                id integer primary key,
                sku varchar not null,
                price numeric(10,2) not null,
                created_at timestamp not null
            )
            """
        )
        conn.execute(
            "insert into products values (1, 'SKU-1', 19.99, '2026-01-01 00:00:00'), "
            "(2, 'SKU-2', 29.99, '2026-01-02 00:00:00'), (3, 'SKU-3', 39.99, '2026-01-03 00:00:00')"
        )

    connector = LocalSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="duckdb_shop",
            entity_name="products",
            execution_mode="scheduled_batch",
            extraction_mode="snapshot",
            connection={
                "driver": "duckdb",
                "database": str(db_path),
                "options": {"read_only": True},
            },
            query={
                "sql": "select id, sku, price, created_at from products where price > 20",
                "artifact_name": "products-over-20",
                "fetch_size": 2,
            },
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="duckdb-products-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=str(tmp_path),
    )

    result = connector.run()

    assert result.query_count == 1
    assert result.row_count == 2
    assert len(result.manifests) == 1
    data = json.loads(
        (tmp_path / Path(result.manifests[0].data_path)).read_text(encoding="utf-8")
    )
    assert sorted([row["sku"] for row in data["rows"]]) == ["SKU-2", "SKU-3"]


def test_connector_registry_factory_for_sql_routes_extended_driver_values(tmp_path: Path) -> None:
    """Verify M-1 registry factory dispatches SqlConnectorConfig for all 6 new drivers."""
    factory = get_connector_factory(ConnectorFamily.sql)
    assert factory.family_type == "sql"

    for driver_value in (
        "sqlite",
        "duckdb",
        "postgres",
        "mysql",
        "mssql",
        "jdbc_generic",
    ):
        resolved = ResolvedEntityConfig(
            schema_version="v1",
            environment="dev",
            source_name="multi_db",
            entity_name=f"entity_{driver_value}",
            connector_type="sql",
            trigger_mode="scheduled_batch",
            extraction={
                "connection": {
                    "driver": driver_value,
                    "database": "memdb",
                },
                "query": {"sql": "select 1 as x"},
            },
        )
        cfg = factory.build_config_from_resolved(resolved_config=resolved)
        assert isinstance(cfg, SqlConnectorConfig)
        assert cfg.connection.driver.value == driver_value


def test_sql_connector_config_default_driver_is_still_sqlite_for_backward_compat(
    tmp_path: Path,
) -> None:
    """Confirm the default driver was not accidentally changed from sqlite."""
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="legacy_orders",
        entity_name="orders",
        connector_type="sql",
        extraction={
            "database": str(tmp_path / "orders.db"),
            "query": {"sql": "select 1"},
        },
    )
    cfg = SqlConnectorConfig.from_resolved_entity_config(resolved_config)
    assert cfg.connection.driver == SqlConnectionDriver.sqlite


def test_local_sql_connector_delta_with_fake_driver_and_checkpoint_update(
    tmp_path: Path,
) -> None:
    """driver_override pattern should exercise full checkpoint update flow too."""
    seed_rows = [
        {"id": 1, "updated_at": "2026-01-03T00:00:00+00:00", "status": "new"},
        {"id": 2, "updated_at": "2026-01-06T00:00:00+00:00", "status": "paid"},
    ]
    fake_driver = _FakePostgresDriver(seed_rows)
    connector = LocalSqlConnector(
        config=SqlConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="pg_orders_db",
            entity_name="orders",
            execution_mode="scheduled_batch",
            extraction_mode="delta",
            connection={
                "driver": "postgres",
                "database": "shop_prod",
                "options": {"host": "pg.local", "port": 5432},
            },
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
            job_name="pg-delta-ingest",
            trigger_type="scheduled_batch",
        ),
        root_path=str(tmp_path),
        driver_override=fake_driver,
    )

    result = connector.run()

    assert result.row_count == 2
    assert result.checkpoint_after == {
        "max_updated_at": "2026-01-06T00:00:00+00:00"
    }
    chk = connector.checkpoint_store.load(
        environment="dev", source_name="pg_orders_db", entity_name="orders"
    )
    assert chk.current_checkpoint == {"max_updated_at": "2026-01-06T00:00:00+00:00"}

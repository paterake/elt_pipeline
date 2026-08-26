from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors.rest import (
    _render_string_template,
    _render_template_value,
    _serialize_template_scalar,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import RunContext


class SqlConnectionDriver(str, Enum):
    sqlite = "sqlite"
    duckdb = "duckdb"
    postgres = "postgres"
    mysql = "mysql"
    mssql = "mssql"
    jdbc_generic = "jdbc_generic"


_DRIVER_INSTALL_HINTS: dict[SqlConnectionDriver, str] = {
    SqlConnectionDriver.duckdb: (
        "uv sync --extra duckdb  (or pip install 'duckdb>=1.0,<2.0')"
    ),
    SqlConnectionDriver.postgres: (
        "uv sync --extra postgres  (or pip install 'psycopg[binary]>=3.2,<4.0')"
    ),
    SqlConnectionDriver.mysql: (
        "uv sync --extra mysql  (or pip install 'mysql-connector-python>=9.0,<11.0')"
    ),
    SqlConnectionDriver.mssql: (
        "uv sync --extra mssql  (or pip install 'pymssql>=2.3,<3.0')"
    ),
    SqlConnectionDriver.jdbc_generic: (
        "uv sync --extra jdbc  (or pip install 'JayDeBeApi>=1.2,<2.0' "
        "plus a JVM/JDBC driver jar on the classpath)"
    ),
}


class SqlExtractionMode(str, Enum):
    snapshot = "snapshot"
    delta = "delta"


class SqlWatermarkSource(str, Enum):
    checkpoint = "checkpoint"
    static = "static"


class SqlConnectionConfig(BaseModel):
    driver: SqlConnectionDriver = SqlConnectionDriver.sqlite
    database: str
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("database")
    @classmethod
    def _validate_database(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("database must not be empty")
        return normalized


class SqlQueryTemplate(BaseModel):
    sql: str | None = None
    sql_file: str | None = None
    catalog_table: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    filters: list[str] = Field(default_factory=list)
    fetch_size: int = Field(default=1000, ge=1)
    artifact_name: str | None = None

    @field_validator("sql", "sql_file", "catalog_table")
    @classmethod
    def _validate_optional_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        return normalized

    @field_validator("filters")
    @classmethod
    def _validate_filters(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for item in value or []:
            if not isinstance(item, str):
                raise ValueError("filters entries must be strings")
            stripped = item.strip()
            if stripped:
                cleaned.append(stripped)
        return cleaned


class SqlWatermarkConfig(BaseModel):
    column_name: str
    parameter_name: str = "watermark"
    checkpoint_key: str = "watermark"
    state_source: SqlWatermarkSource = SqlWatermarkSource.checkpoint
    default_value: Any = None

    @field_validator("column_name", "parameter_name", "checkpoint_key")
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Watermark fields must not be empty")
        return normalized

    @model_validator(mode="after")
    def _validate_static_source(self) -> "SqlWatermarkConfig":
        if self.state_source == SqlWatermarkSource.static and self.default_value is None:
            raise ValueError("static watermark state_source requires default_value")
        return self


class SqlConnectorConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    execution_mode: str
    extraction_mode: SqlExtractionMode = SqlExtractionMode.snapshot
    connection: SqlConnectionConfig
    query: SqlQueryTemplate
    watermark: SqlWatermarkConfig | None = None
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_delta_requirements(self) -> "SqlConnectorConfig":
        if self.extraction_mode == SqlExtractionMode.delta and self.watermark is None:
            raise ValueError("Delta SQL extraction requires watermark configuration")
        return self

    @classmethod
    def from_resolved_entity_config(
        cls,
        resolved_config: ResolvedEntityConfig,
    ) -> "SqlConnectorConfig":
        if resolved_config.connector_type != "sql":
            raise ConfigValidationError(
                message="Resolved entity config is not a SQL connector",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": resolved_config.connector_type,
                },
            )

        extraction = resolved_config.extraction
        query_payload = extraction.get("query", extraction.get("sql"))
        if isinstance(query_payload, str):
            query_payload = {"sql": query_payload}
        elif query_payload is None:
            query_payload = {
                "sql": extraction.get("statement") or extraction.get("base_query"),
                "sql_file": extraction.get("sql_file"),
                "catalog_table": extraction.get("catalog_table"),
                "filters": extraction.get("filters", []),
                "parameters": extraction.get("query_parameters", {}),
                "fetch_size": extraction.get("fetch_size", 1000),
                "artifact_name": extraction.get("artifact_name"),
            }
        else:
            # Top-level keys in extraction (sql_file, filters, catalog_table) are
            # accepted as short-form overrides even when query= dict is present
            for key in ("sql_file", "catalog_table", "filters"):
                if key in extraction and key not in query_payload:
                    query_payload[key] = extraction[key]
            if "query_parameters" in extraction and "parameters" not in query_payload:
                query_payload["parameters"] = extraction["query_parameters"]
            if "fetch_size" in extraction and "fetch_size" not in query_payload:
                query_payload["fetch_size"] = extraction["fetch_size"]
            if "artifact_name" in extraction and "artifact_name" not in query_payload:
                query_payload["artifact_name"] = extraction["artifact_name"]

        config_file_dir = resolved_config.settings.get("config_file_dir")
        config_file_path = resolved_config.settings.get("config_file_path")
        resolved_sql = _resolve_query_sql(
            query_payload=query_payload,
            resolved_config=resolved_config,
            config_file_dir=config_file_dir,
            config_file_path=config_file_path,
        )
        query_payload = dict(query_payload)
        query_payload["sql"] = resolved_sql

        connection_payload = extraction.get("connection") or {
            "driver": extraction.get("driver", "sqlite"),
            "database": extraction.get("database") or resolved_config.settings.get("database"),
            "options": extraction.get("connection_options", {}),
        }

        extraction_mode = extraction.get("mode") or extraction.get("extraction_mode") or "snapshot"
        watermark_payload = extraction.get("watermark")

        try:
            return cls(
                schema_version=resolved_config.schema_version,
                environment=resolved_config.environment,
                source_name=resolved_config.source_name,
                entity_name=resolved_config.entity_name,
                execution_mode=resolved_config.trigger_mode or "manual",
                extraction_mode=SqlExtractionMode(extraction_mode),
                connection=SqlConnectionConfig.model_validate(connection_payload),
                query=SqlQueryTemplate.model_validate(query_payload),
                watermark=(
                    SqlWatermarkConfig.model_validate(watermark_payload)
                    if watermark_payload is not None
                    else None
                ),
                persistence=resolved_config.persistence,
                state=resolved_config.state,
                settings=resolved_config.settings,
                raw=resolved_config.raw,
            )
        except ValidationError as exc:
            raise ConfigValidationError(
                message="SQL connector configuration validation failed",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc


def _resolve_query_sql(
    *,
    query_payload: dict[str, Any],
    resolved_config: ResolvedEntityConfig,
    config_file_dir: str | None,
    config_file_path: str | None,
) -> str:
    explicit_sql = query_payload.get("sql")
    sql_file = query_payload.get("sql_file")
    if explicit_sql:
        return str(explicit_sql).strip()
    if sql_file:
        sql_path = Path(sql_file)
        if not sql_path.is_absolute():
            if not config_file_dir:
                raise ConfigValidationError(
                    message=(
                        "sql_file is relative but no config_file_dir is available "
                        "(pass config_path to resolve_entity_config())"
                    ),
                    context={
                        "source_name": resolved_config.source_name,
                        "entity_name": resolved_config.entity_name,
                        "sql_file": sql_file,
                        "config_file_path": config_file_path,
                        "error_code": "SQL_SQLFILE_NO_BASEDIR",
                    },
                )
            sql_path = Path(config_file_dir) / sql_file
        if not sql_path.exists():
            raise ConfigValidationError(
                message=f"sql_file does not exist: {sql_path}",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "sql_file": sql_file,
                    "resolved_path": str(sql_path),
                    "error_code": "SQL_SQLFILE_NOT_FOUND",
                },
            )
        try:
            content = sql_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigValidationError(
                message=f"Failed to read sql_file {sql_path}: {exc}",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "sql_file": sql_file,
                    "resolved_path": str(sql_path),
                    "error_code": "SQL_SQLFILE_READ_ERROR",
                },
            ) from exc
        if not content:
            raise ConfigValidationError(
                message=f"sql_file is empty: {sql_path}",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "sql_file": sql_file,
                    "error_code": "SQL_SQLFILE_EMPTY",
                },
            )
        return content
    # Auto fallback: SELECT * FROM <catalog_table or entity_name>
    table_name = (query_payload.get("catalog_table") or resolved_config.entity_name).strip()
    if not table_name:
        raise ConfigValidationError(
            message=(
                "No SQL available: provide extraction.query.sql, extraction.sql_file, "
                "or ensure entity_name is a valid source table name (auto-SELECT *)"
            ),
            context={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
                "error_code": "SQL_QUERY_UNAVAILABLE",
            },
        )
    return f"SELECT * FROM {table_name}"


class SqlPreparedQuery(BaseModel):
    sql: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SqlQueryResult(BaseModel):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    executed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def row_count(self) -> int:
        return len(self.rows)


class SqlRunResult(BaseModel):
    manifests: list[Level1ArtifactManifest] = Field(default_factory=list)
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    query_count: int = 0
    row_count: int = 0


@runtime_checkable
class SqlDbDriver(Protocol):
    """Runtime-checkable Protocol for a DB-API 2.0 driver.

    A driver exposes a single ``connect`` method that returns a DB-API 2.0
    compliant Connection object with ``cursor()``, ``commit()``, ``close()``
    and ``__enter__``/``__exit__`` context-manager semantics.  Concrete
    drivers are lazily imported by ``_driver_from_config()`` to avoid
    pulling heavy SDKs into processes that only use SQLite.
    """

    def connect(self, *, database: str, options: dict[str, Any]) -> Any: ...


def _raise_driver_missing(driver: SqlConnectionDriver) -> None:
    hint = _DRIVER_INSTALL_HINTS.get(
        driver, f"install the Python DB client for driver '{driver.value}'"
    )
    raise ConfigValidationError(
        message=(
            f"SQL driver '{driver.value}' is not installed. Install it: {hint}"
        ),
        context={
            "driver": driver.value,
            "install_hint": hint,
            "error_code": "SQL_DRIVER_SDK_MISSING",
        },
    )


def _build_db_driver(driver_enum: SqlConnectionDriver) -> SqlDbDriver:
    """Lazy-importer that returns a SqlDbDriver Protocol wrapper per driver.

    Raises ConfigValidationError with a sharp install hint when the SDK is
    absent — never silently falls through to a later step.  SQLite is the
    only zero-dependency driver (stdlib).
    """
    if driver_enum == SqlConnectionDriver.sqlite:

        class _SqliteDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                import sqlite3 as _sqlite3

                timeout = float(options.get("timeout", 5.0))
                isolation_level = options.get("isolation_level", None)
                uri = bool(options.get("uri", False))
                detect_types = int(options.get("detect_types", 0))
                return _sqlite3.connect(
                    database,
                    timeout=timeout,
                    isolation_level=isolation_level,
                    uri=uri,
                    detect_types=detect_types,
                )

        return _SqliteDriver()

    if driver_enum == SqlConnectionDriver.duckdb:
        try:
            import duckdb as _duckdb  # type: ignore[import-not-found]
        except ImportError:
            _raise_driver_missing(SqlConnectionDriver.duckdb)

        class _DuckDbDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                read_only = bool(options.get("read_only", False))
                config = options.get("config")
                kwargs: dict[str, Any] = {"read_only": read_only}
                if config is not None:
                    kwargs["config"] = config
                return _duckdb.connect(str(database), **kwargs)

        return _DuckDbDriver()

    if driver_enum == SqlConnectionDriver.postgres:
        try:
            import psycopg as _psycopg  # type: ignore[import-not-found]
        except ImportError:
            _raise_driver_missing(SqlConnectionDriver.postgres)

        class _PostgresDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                kwargs: dict[str, Any] = {"dbname": database}
                host = options.get("host")
                port = options.get("port")
                user = options.get("user")
                password = options.get("password")
                sslmode = options.get("sslmode")
                application_name = options.get("application_name")
                if host is not None:
                    kwargs["host"] = host
                if port is not None:
                    kwargs["port"] = int(port)
                if user is not None:
                    kwargs["user"] = user
                if password is not None:
                    kwargs["password"] = password
                if sslmode is not None:
                    kwargs["sslmode"] = sslmode
                if application_name is not None:
                    kwargs["application_name"] = application_name
                conninfo = options.get("conninfo")
                if conninfo is not None:
                    return _psycopg.connect(conninfo)
                autocommit = options.get("autocommit", True)
                conn = _psycopg.connect(**kwargs)
                conn.autocommit = bool(autocommit)
                return conn

        return _PostgresDriver()

    if driver_enum == SqlConnectionDriver.mysql:
        try:
            import mysql.connector as _mysql_conn  # type: ignore[import-not-found]
        except ImportError:
            _raise_driver_missing(SqlConnectionDriver.mysql)

        class _MySqlDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                kwargs: dict[str, Any] = {"database": database}
                host = options.get("host", "127.0.0.1")
                port = options.get("port", 3306)
                user = options.get("user")
                password = options.get("password")
                ssl_disabled = options.get("ssl_disabled", False)
                if host is not None:
                    kwargs["host"] = host
                if port is not None:
                    kwargs["port"] = int(port)
                if user is not None:
                    kwargs["user"] = user
                if password is not None:
                    kwargs["password"] = password
                if ssl_disabled is not None:
                    kwargs["ssl_disabled"] = bool(ssl_disabled)
                for extra in ("charset", "collation", "unix_socket"):
                    if extra in options:
                        kwargs[extra] = options[extra]
                autocommit = options.get("autocommit", True)
                conn = _mysql_conn.connect(**kwargs)
                conn.autocommit = bool(autocommit)
                return conn

        return _MySqlDriver()

    if driver_enum == SqlConnectionDriver.mssql:
        try:
            import pymssql as _pymssql  # type: ignore[import-not-found]
        except ImportError:
            _raise_driver_missing(SqlConnectionDriver.mssql)

        class _MsSqlDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                kwargs: dict[str, Any] = {"database": database}
                server = options.get("server", "127.0.0.1")
                port = options.get("port", 1433)
                user = options.get("user")
                password = options.get("password")
                tds_version = options.get("tds_version")
                charset = options.get("charset", "utf8")
                kwargs["server"] = server
                kwargs["port"] = int(port)
                if user is not None:
                    kwargs["user"] = user
                if password is not None:
                    kwargs["password"] = password
                if tds_version is not None:
                    kwargs["tds_version"] = tds_version
                if charset is not None:
                    kwargs["charset"] = charset
                for extra in ("host", "conn_properties", "as_dict"):
                    if extra in options:
                        kwargs[extra] = options[extra]
                conn = _pymssql.connect(**kwargs)
                conn.autocommit(bool(options.get("autocommit", True)))
                return conn

        return _MsSqlDriver()

    if driver_enum == SqlConnectionDriver.jdbc_generic:
        try:
            import jaydebeapi as _jaydebeapi  # type: ignore[import-not-found]
        except ImportError:
            _raise_driver_missing(SqlConnectionDriver.jdbc_generic)

        class _JdbcGenericDriver:
            def connect(
                self, *, database: str, options: dict[str, Any]
            ) -> Any:
                jclassname = options.get("jclassname")
                if jclassname is None:
                    raise ConfigValidationError(
                        message=(
                            "jdbc_generic SQL driver requires 'jclassname' option "
                            "(JDBC driver FQCN, e.g. 'org.postgresql.Driver')."
                        ),
                        context={
                            "driver": "jdbc_generic",
                            "available_options": sorted(options.keys()),
                            "error_code": "SQL_JDBC_JCLASSNAME_REQUIRED",
                        },
                    )
                driver_args: list[Any] = [database]
                url_user = options.get("user")
                url_password = options.get("password")
                if url_user is not None or url_password is not None:
                    driver_args.append([url_user or "", url_password or ""])
                jars = options.get("jars")
                libs = options.get("libs")
                return _jaydebeapi.connect(
                    jclassname,
                    *driver_args,
                    jars=jars,
                    libs=libs,
                )

        return _JdbcGenericDriver()

    raise ConfigValidationError(
        message=f"Unknown SQL driver '{driver_enum.value}'.",
        context={
            "driver": driver_enum.value,
            "supported_drivers": sorted(d.value for d in SqlConnectionDriver),
            "error_code": "SQL_DRIVER_UNKNOWN",
        },
    )


@runtime_checkable
class SqlDbConnection(Protocol):
    """Protocol mirroring the DB-API 2.0 Connection methods we actually call.

    Written as an explicit Protocol so driver wrappers for jaydebeapi etc.
    that do not inherit from ``sqlite3.Connection`` still satisfy the type
    checker without ``# type: ignore`` noise on every cursor call.
    """

    def cursor(self) -> Any: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...

    def __enter__(self) -> "SqlDbConnection": ...

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...


class SqlConnectorBase(ABC):
    def __init__(self, *, config: SqlConnectorConfig, run_context: RunContext) -> None:
        self.config = config
        self.run_context = run_context

    def validate_config(self) -> SqlConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    def resolve_watermark(self, *, checkpoint_before: dict[str, Any] | None) -> Any:
        if self.config.extraction_mode == SqlExtractionMode.snapshot:
            return None
        watermark = self.config.watermark
        if watermark is None:
            raise ConfigValidationError(
                message="Delta SQL extraction requires watermark configuration",
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                },
            )
        if watermark.state_source == SqlWatermarkSource.checkpoint:
            if checkpoint_before and watermark.checkpoint_key in checkpoint_before:
                return checkpoint_before[watermark.checkpoint_key]
            if watermark.default_value is not None:
                return watermark.default_value
            raise ConfigValidationError(
                message="SQL delta extraction could not resolve a starting watermark",
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "checkpoint_key": watermark.checkpoint_key,
                },
            )
        if watermark.state_source == SqlWatermarkSource.static:
            return watermark.default_value
        raise ConfigValidationError(
            message="SQL watermark state_source is not supported",
            context={
                "source_name": self.config.source_name,
                "entity_name": self.config.entity_name,
                "state_source": watermark.state_source.value,
            },
        )

    def build_query_plan(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        watermark_value: Any,
    ) -> list[SqlPreparedQuery]:
        template_context = _build_template_context(
            config=self.config,
            run_context=self.run_context,
            checkpoint_before=checkpoint_before,
            watermark_value=watermark_value,
        )
        watermark = self.config.watermark
        base_sql = str(self.config.query.sql)
        filters = list(self.config.query.filters or [])
        parameters = dict(self.config.query.parameters)
        if (
            self.config.extraction_mode == SqlExtractionMode.delta
            and watermark is not None
            and watermark_value is not None
        ):
            param_name = watermark.parameter_name or "watermark"
            explicit_user_watermark_handling = (
                f":{param_name}" in base_sql
                or param_name in parameters
                or _sql_contains_any_parameter(base_sql)
                or _any_value_references_watermark(parameters)
            )
            if not explicit_user_watermark_handling:
                filters.append(f"{watermark.column_name} > :{param_name}")
                parameters.setdefault(param_name, watermark_value)

        final_sql = _assemble_sql_with_filters(base_sql=base_sql, filters=filters)

        compiled_sql = _render_string_template(
            final_sql,
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        compiled_parameters = _render_template_value(
            parameters,
            template_context=template_context,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return [
            SqlPreparedQuery(
                sql=str(compiled_sql),
                parameters=dict(compiled_parameters),
                metadata={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "base_sql": base_sql,
                    "final_sql": str(compiled_sql),
                    "compiled_parameters": dict(compiled_parameters),
                    "filters_applied": list(filters),
                    "watermark_value": _serialize_template_scalar(watermark_value),
                    "extraction_mode": self.config.extraction_mode.value,
                },
            )
        ]

    @abstractmethod
    def execute_query(self, query: SqlPreparedQuery) -> SqlQueryResult:
        """Execute a prepared SQL query and return its rows."""

    @abstractmethod
    def persist_query_result(
        self,
        *,
        query: SqlPreparedQuery,
        result: SqlQueryResult,
        checkpoint_before: dict[str, Any] | None,
    ) -> list[Level1ArtifactManifest]:
        """Persist query results into level1 and return written manifests."""

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        results: list[SqlQueryResult],
        manifests: list[Level1ArtifactManifest],
    ) -> dict[str, Any] | None:
        del manifests
        if self.config.extraction_mode == SqlExtractionMode.snapshot:
            return checkpoint_before
        watermark = self.config.watermark
        if watermark is None:
            return checkpoint_before
        watermark_values = [
            row.get(watermark.column_name)
            for result in results
            for row in result.rows
            if row.get(watermark.column_name) is not None
        ]
        if not watermark_values:
            return checkpoint_before
        max_watermark = _max_watermark_value(
            watermark_values,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            column_name=watermark.column_name,
        )
        checkpoint_after = dict(checkpoint_before or {})
        checkpoint_after[watermark.checkpoint_key] = max_watermark
        return checkpoint_after

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        del checkpoint_before
        del checkpoint_after
        del manifests
        return None

    def run(self) -> SqlRunResult:
        self.validate_config()
        checkpoint_before = self.resolve_checkpoint_before()
        watermark_value = self.resolve_watermark(checkpoint_before=checkpoint_before)
        queries = self.build_query_plan(
            checkpoint_before=checkpoint_before,
            watermark_value=watermark_value,
        )
        manifests: list[Level1ArtifactManifest] = []
        results: list[SqlQueryResult] = []

        for query in queries:
            result = self.execute_query(query)
            results.append(result)
            manifests.extend(
                self.persist_query_result(
                    query=query,
                    result=result,
                    checkpoint_before=checkpoint_before,
                )
            )

        checkpoint_after = self.build_checkpoint_after(
            checkpoint_before=checkpoint_before,
            results=results,
            manifests=manifests,
        )
        self.update_checkpoint(
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            manifests=manifests,
        )
        return SqlRunResult(
            manifests=manifests,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            query_count=len(queries),
            row_count=sum(result.row_count for result in results),
        )


def _build_template_context(
    *,
    config: SqlConnectorConfig,
    run_context: RunContext,
    checkpoint_before: dict[str, Any] | None,
    watermark_value: Any,
) -> dict[str, Any]:
    watermark = config.watermark
    today = run_context.started_at
    return {
        "run": {
            "id": run_context.run_id,
            "job_name": run_context.job_name,
            "trigger_type": run_context.trigger_type,
            "started_at": run_context.started_at.isoformat(),
        },
        "source": {"name": config.source_name},
        "entity": {"name": config.entity_name},
        "config": {
            "schema_version": config.schema_version,
            "environment": config.environment,
            "execution_mode": config.execution_mode,
            "driver": config.connection.driver.value,
            "database": config.connection.database,
            "extraction_mode": config.extraction_mode.value,
        },
        "checkpoint": checkpoint_before or {},
        "watermark": {
            "value": _serialize_template_scalar(watermark_value),
            "column_name": watermark.column_name if watermark else None,
            "checkpoint_key": watermark.checkpoint_key if watermark else None,
            "parameter_name": watermark.parameter_name if watermark else None,
        },
        "today": {
            "date": today.date().isoformat(),
            "yyyymmdd": today.strftime("%Y%m%d"),
            "iso": today.isoformat(),
            "datetime_iso": today.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "environment": config.environment,
    }


_TEMPLATE_PATTERN = r"\{([a-zA-Z0-9_.]+)\}"


def _sql_contains_any_parameter(sql: str) -> bool:
    stripped = sql.strip()
    if not stripped:
        return False
    for style in (":", "@", "?"):
        if style == "?":
            if "?" in stripped:
                return True
        else:
            idx = 0
            while True:
                idx = stripped.find(style, idx)
                if idx < 0:
                    break
                if idx + 1 < len(stripped) and (
                    stripped[idx + 1].isalpha() or stripped[idx + 1] == "_"
                ):
                    return True
                idx += 1
    return False


def _any_value_references_watermark(parameters: dict[str, Any]) -> bool:
    for value in parameters.values():
        if not isinstance(value, str):
            continue
        if (
            "{watermark" in value
            or "{checkpoint" in value
            or "{today" in value
        ):
            return True
    return False


def _assemble_sql_with_filters(*, base_sql: str, filters: list[str]) -> str:
    if not filters:
        return base_sql
    normalized = base_sql.rstrip().rstrip(";").rstrip()
    combined = " AND ".join(f"({f})" for f in filters)
    upper_sql = normalized.upper()
    where_pos = upper_sql.rfind(" WHERE ")
    group_pos = upper_sql.rfind(" GROUP BY ")
    order_pos = upper_sql.rfind(" ORDER BY ")
    limit_pos = upper_sql.rfind(" LIMIT ")
    insert_pos = len(normalized)
    for marker_pos in (group_pos, order_pos, limit_pos):
        if marker_pos > where_pos and marker_pos < insert_pos:
            insert_pos = marker_pos
    if where_pos >= 0 and (insert_pos == len(normalized) or where_pos < insert_pos):
        prefix = normalized[: where_pos + len(" WHERE ")]
        rest = normalized[where_pos + len(" WHERE ") : insert_pos]
        suffix = normalized[insert_pos:]
        return f"{prefix}({rest}) AND {combined}{suffix}"
    body = normalized[:insert_pos]
    suffix = normalized[insert_pos:]
    return f"{body} WHERE {combined}{suffix}"


def _max_watermark_value(
    values: list[Any],
    *,
    source_name: str,
    entity_name: str,
    column_name: str,
) -> Any:
    if not values:
        return None
    first_type = type(values[0])
    if any(type(value) is not first_type for value in values[1:]):
        raise ConfigValidationError(
            message="SQL watermark values must share a comparable type",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "column_name": column_name,
                "value_types": sorted({type(value).__name__ for value in values}),
            },
        )
    try:
        return max(values)
    except TypeError as exc:
        raise ConfigValidationError(
            message="SQL watermark values are not comparable",
            context={
                "source_name": source_name,
                "entity_name": entity_name,
                "column_name": column_name,
                "value_type": first_type.__name__,
            },
        ) from exc


__all__ = [
    "SqlConnectionConfig",
    "SqlConnectionDriver",
    "SqlConnectorBase",
    "SqlConnectorConfig",
    "SqlDbConnection",
    "SqlDbDriver",
    "SqlExtractionMode",
    "SqlPreparedQuery",
    "SqlQueryResult",
    "SqlQueryTemplate",
    "SqlRunResult",
    "SqlWatermarkConfig",
    "SqlWatermarkSource",
    "_build_db_driver",
]

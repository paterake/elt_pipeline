from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    SqlExecutionRecord,
    SqlExecutionResult,
    SqlLoadMode,
)


class LocalSqlModelExecutor:
    def __init__(
        self,
        *,
        database_path: str | Path,
        partition_values: dict[str, str] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self.partition_values = partition_values or {}

    def execute(
        self,
        models: list[CompiledSqlModel],
        execution_observer: Callable[[CompiledSqlModel, SqlExecutionRecord], None] | None = None,
    ) -> SqlExecutionResult:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        execution_result = SqlExecutionResult(database_path=self.database_path)
        with sqlite3.connect(self.database_path) as connection:
            for model in models:
                try:
                    row_count = self._execute_model(connection=connection, model=model)
                except sqlite3.DatabaseError as exc:
                    raise PipelineError(
                        message=f"Failed to execute SQL model '{model.model_id}'",
                        error_code="SQL_EXECUTION_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=False,
                        context={
                            "model_id": model.model_id,
                            "target_table_name": model.target_table_name,
                            "database_path": str(self.database_path),
                        },
                    ) from exc

                record = SqlExecutionRecord(
                    model_id=model.model_id,
                    target_table_name=model.target_table_name,
                    load_mode=model.load_mode,
                    row_count=row_count,
                )
                execution_result.executed_models.append(record)
                if execution_observer is not None:
                    execution_observer(model, record)
            connection.commit()
        return execution_result

    def _execute_model(self, *, connection: sqlite3.Connection, model: CompiledSqlModel) -> int:
        target_table = _quote_identifier(model.target_table_name)
        select_sql = model.compiled_sql.rstrip().rstrip(";")

        if model.load_mode == SqlLoadMode.full_refresh:
            connection.execute(f"drop table if exists {target_table}")
            connection.execute(f"create table {target_table} as {select_sql}")
            return _count_rows(connection=connection, table_name=model.target_table_name)

        self._ensure_target_table(connection=connection, model=model)

        if model.load_mode == SqlLoadMode.append:
            connection.execute(f"insert into {target_table} {select_sql}")
            return _count_rows(connection=connection, table_name=model.target_table_name)

        if model.load_mode == SqlLoadMode.partition_overwrite:
            self._delete_partition_rows(connection=connection, model=model)
            connection.execute(f"insert into {target_table} {select_sql}")
            return _count_rows(connection=connection, table_name=model.target_table_name)

        raise PipelineError(
            message=f"Unsupported SQL load mode '{model.load_mode.value}'",
            error_code="SQL_LOAD_MODE_UNSUPPORTED",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"model_id": model.model_id, "load_mode": model.load_mode.value},
        )

    def _ensure_target_table(
        self,
        *,
        connection: sqlite3.Connection,
        model: CompiledSqlModel,
    ) -> None:
        target_table = _quote_identifier(model.target_table_name)
        select_sql = model.compiled_sql.rstrip().rstrip(";")
        connection.execute(
            f"create table if not exists {target_table} as "
            f"select * from ({select_sql}) as source_model where 1 = 0"
        )

    def _delete_partition_rows(
        self,
        *,
        connection: sqlite3.Connection,
        model: CompiledSqlModel,
    ) -> None:
        partition_columns = model.partition_columns
        missing_columns = sorted(
            column for column in partition_columns if column not in self.partition_values
        )
        if missing_columns:
            raise PipelineError(
                message="Partition overwrite requires runtime partition values",
                error_code="SQL_PARTITION_VALUE_MISSING",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={"model_id": model.model_id, "missing_columns": missing_columns},
            )

        where_clauses: list[str] = []
        parameters: list[str] = []
        for column in partition_columns:
            where_clauses.append(f"{_quote_identifier(column)} = ?")
            parameters.append(self.partition_values[column])

        target_table = _quote_identifier(model.target_table_name)
        where_sql = " and ".join(where_clauses)
        connection.execute(f"delete from {target_table} where {where_sql}", parameters)


def _count_rows(*, connection: sqlite3.Connection, table_name: str) -> int:
    cursor = connection.execute(f"select count(*) from {_quote_identifier(table_name)}")
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _quote_identifier(identifier: str) -> str:
    if not identifier or not all(
        character.isalnum() or character == "_" for character in identifier
    ):
        raise PipelineError(
            message=f"Unsupported SQL identifier '{identifier}'",
            error_code="SQL_IDENTIFIER_INVALID",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"identifier": identifier},
        )
    return f'"{identifier}"'

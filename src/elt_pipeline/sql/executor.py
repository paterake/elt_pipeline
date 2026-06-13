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
    SqlModelValidationSummary,
    SqlValidationResult,
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
            try:
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

                    validation_summary = self._validate_model(
                        connection=connection,
                        model=model,
                        row_count=row_count,
                    )
                    execution_result.model_validations.append(validation_summary)

                    if execution_observer is not None:
                        execution_observer(model, record)
                connection.commit()
            except PipelineError as exc:
                validation_summary = exc.context.get("validation_results")
                if validation_summary is not None:
                    summary = SqlModelValidationSummary.model_validate(validation_summary)
                    if all(
                        existing.model_id != summary.model_id
                        for existing in execution_result.model_validations
                    ):
                        execution_result.model_validations.append(summary)
                if "execution_result" not in exc.context:
                    exc.context["execution_result"] = execution_result.model_dump(mode="json")
                raise
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

    def _validate_model(
        self,
        *,
        connection: sqlite3.Connection,
        model: CompiledSqlModel,
        row_count: int,
    ) -> SqlModelValidationSummary:
        validations: list[SqlValidationResult] = []
        quality = model.quality

        if quality.row_count_min is not None:
            passed = row_count >= quality.row_count_min
            validations.append(
                SqlValidationResult(
                    validation_type="row_count_min",
                    passed=passed,
                    observed_value=row_count,
                    expected_value=quality.row_count_min,
                    message=(
                        None
                        if passed
                        else (
                            f"Expected at least {quality.row_count_min} rows but observed {row_count}"
                        )
                    ),
                )
            )

        for column in quality.unique_columns:
            duplicate_count = self._count_duplicate_rows(
                connection=connection,
                table_name=model.target_table_name,
                columns=[column],
            )
            passed = duplicate_count == 0
            validations.append(
                SqlValidationResult(
                    validation_type="unique_columns",
                    passed=passed,
                    columns=[column],
                    observed_value=duplicate_count,
                    expected_value=0,
                    message=(
                        None
                        if passed
                        else f"Found {duplicate_count} duplicate rows for unique column '{column}'"
                    ),
                )
            )

        for column in quality.not_null_columns:
            null_count = self._count_null_rows(
                connection=connection,
                table_name=model.target_table_name,
                column=column,
            )
            passed = null_count == 0
            validations.append(
                SqlValidationResult(
                    validation_type="not_null_columns",
                    passed=passed,
                    columns=[column],
                    observed_value=null_count,
                    expected_value=0,
                    message=(
                        None
                        if passed
                        else f"Found {null_count} null rows for required column '{column}'"
                    ),
                )
            )

        summary = SqlModelValidationSummary(
            model_id=model.model_id,
            target_table_name=model.target_table_name,
            passed=all(result.passed for result in validations),
            validations=validations,
        )

        failing_validations = [result for result in validations if not result.passed]
        if failing_validations:
            raise PipelineError(
                message=f"SQL model validations failed for '{model.model_id}'",
                error_code="SQL_MODEL_VALIDATION_FAILED",
                error_category=ErrorCategory.validation_error,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "target_table_name": model.target_table_name,
                    "validation_results": summary.model_dump(mode="json"),
                },
            )

        return summary

    def _count_duplicate_rows(
        self,
        *,
        connection: sqlite3.Connection,
        table_name: str,
        columns: list[str],
    ) -> int:
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        cursor = connection.execute(
            "select count(*) from ("
            f"select {quoted_columns} from {_quote_identifier(table_name)} "
            f"group by {quoted_columns} having count(*) > 1"
            ")"
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def _count_null_rows(
        self,
        *,
        connection: sqlite3.Connection,
        table_name: str,
        column: str,
    ) -> int:
        cursor = connection.execute(
            f"select count(*) from {_quote_identifier(table_name)} "
            f"where {_quote_identifier(column)} is null"
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


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

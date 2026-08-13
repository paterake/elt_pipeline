from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.level2_source import Level2DatasetLocator
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    SqlExecutionRecord,
    SqlExecutionResult,
    SqlLoadMode,
    SqlModelPlan,
    SqlModelStage,
    SqlModelValidationSummary,
    SqlPlanningResult,
    SqlQueryPlanStep,
    SqlValidationResult,
)


class SparkSqlModelExecutor:
    def __init__(
        self,
        *,
        spark: SparkSession,
        warehouse_root: str | Path,
        root_path: str | Path,
        environment: str,
        partition_values: dict[str, str] | None = None,
    ) -> None:
        self.spark = spark
        self.warehouse_root = Path(warehouse_root)
        self.root_path = Path(root_path)
        self.environment = environment
        self.partition_values = partition_values or {}
        self._level2_locator = Level2DatasetLocator(root_path=self.root_path, spark=spark)

    def execute(
        self,
        models: list[CompiledSqlModel],
        execution_observer: Callable[[CompiledSqlModel, SqlExecutionRecord], None] | None = None,
    ) -> SqlExecutionResult:
        execution_result = SqlExecutionResult(warehouse_root=self.warehouse_root)
        models_by_id = {model.model_id: model for model in models}
        try:
            for model in models:
                try:
                    self._register_execute_inputs(model=model, models_by_id=models_by_id)
                    row_count = self._execute_model(model=model)
                except PySparkException as exc:
                    raise build_sql_runtime_error(
                        message=f"Failed to execute SQL model '{model.model_id}'",
                        code=SqlRuntimeErrorCode.execution_failed,
                        retryable=False,
                        context={
                            "model_id": model.model_id,
                            "target_table_name": model.target_table_name,
                            "warehouse_root": str(self.warehouse_root),
                        },
                    ) from exc

                record = SqlExecutionRecord(
                    model_id=model.model_id,
                    stage=model.stage,
                    target_table_name=model.target_table_name,
                    load_mode=model.load_mode,
                    row_count=row_count,
                )
                execution_result.executed_models.append(record)

                validation_summary = self._validate_model(model=model, row_count=row_count)
                execution_result.model_validations.append(validation_summary)

                if execution_observer is not None:
                    execution_observer(model, record)
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

    def plan(
        self,
        models: list[CompiledSqlModel],
        *,
        include_query_plan: bool = False,
    ) -> SqlPlanningResult:
        planning_result = SqlPlanningResult(warehouse_root=self.warehouse_root)
        for model in models:
            self._validate_partition_requirements(model=model)
            self._register_plan_sources(model=model)
            query_plan = self._explain_model(model=model, include_query_plan=include_query_plan)
            planning_result.planned_models.append(
                SqlModelPlan(
                    model_id=model.model_id,
                    target_table_name=model.target_table_name,
                    load_mode=model.load_mode,
                    depends_on=model.depends_on,
                    token_values=model.token_values,
                    validation_passed=True,
                    query_plan=query_plan,
                )
            )
            self._register_planned_output(model=model)
        return planning_result

    def _table_path(self, *, stage: SqlModelStage, table_name: str) -> Path:
        return self.warehouse_root / stage.value / table_name

    def _register_execute_inputs(
        self,
        *,
        model: CompiledSqlModel,
        models_by_id: dict[str, CompiledSqlModel],
    ) -> None:
        for source_ref in model.sources:
            dataframe = self._level2_locator.read(
                source_ref=source_ref,
                environment=source_ref.environment or self.environment,
            )
            dataframe.createOrReplaceTempView(source_ref.logical_name)

        for dependency_id in model.depends_on:
            dependency = models_by_id.get(dependency_id)
            if dependency is None:
                raise build_sql_runtime_error(
                    code=SqlRuntimeErrorCode.dependency_not_materialized,
                    message=(
                        f"Dependency '{dependency_id}' for model '{model.model_id}' was not "
                        "included in this run; rerun with --include-deps"
                    ),
                    context={"model_id": model.model_id, "dependency_id": dependency_id},
                )
            dependency_path = self._table_path(
                stage=dependency.stage, table_name=dependency.target_table_name
            )
            dataframe = self.spark.read.parquet(str(dependency_path))
            dataframe.createOrReplaceTempView(dependency.target_table_name)

    def _register_plan_sources(self, *, model: CompiledSqlModel) -> None:
        for source_ref in model.sources:
            dataframe = self._level2_locator.read(
                source_ref=source_ref,
                environment=source_ref.environment or self.environment,
            )
            dataframe.createOrReplaceTempView(source_ref.logical_name)

    def _register_planned_output(self, *, model: CompiledSqlModel) -> None:
        select_sql = model.compiled_sql.rstrip().rstrip(";")
        planned_df = self.spark.sql(f"select * from ({select_sql}) as planned_model limit 0")
        planned_df.createOrReplaceTempView(model.target_table_name)

    def _execute_model(self, *, model: CompiledSqlModel) -> int:
        select_sql = model.compiled_sql.rstrip().rstrip(";")
        dataframe = self.spark.sql(select_sql)
        target_path = self._table_path(stage=model.stage, table_name=model.target_table_name)
        effective_partition_columns = self._effective_partition_columns(model=model)

        if model.load_mode == SqlLoadMode.full_refresh:
            writer = dataframe.write.mode("overwrite")
            if effective_partition_columns:
                writer = writer.partitionBy(*effective_partition_columns)
            writer.parquet(str(target_path))
        elif model.load_mode == SqlLoadMode.append:
            writer = dataframe.write.mode("append")
            if effective_partition_columns:
                writer = writer.partitionBy(*effective_partition_columns)
            writer.parquet(str(target_path))
        elif model.load_mode == SqlLoadMode.partition_overwrite:
            self._validate_partition_requirements(model=model)
            writer = dataframe.write.mode("overwrite")
            if effective_partition_columns:
                writer = writer.partitionBy(*effective_partition_columns)
            writer.parquet(str(target_path))
        else:
            raise build_sql_runtime_error(
                message=f"Unsupported SQL load mode '{model.load_mode.value}'",
                code=SqlRuntimeErrorCode.load_mode_unsupported,
                retryable=False,
                context={"model_id": model.model_id, "load_mode": model.load_mode.value},
            )

        return self.spark.read.parquet(str(target_path)).count()

    def _validate_model(
        self,
        *,
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
                            "Expected at least "
                            f"{quality.row_count_min} rows but observed {row_count}"
                        )
                    ),
                )
            )

        if quality.unique_columns or quality.not_null_columns:
            target_path = self._table_path(stage=model.stage, table_name=model.target_table_name)
            target_df = self.spark.read.parquet(str(target_path))

            for column in quality.unique_columns:
                duplicate_count = (
                    target_df.groupBy(column).count().filter("count > 1").count()
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
                            else (
                                f"Found {duplicate_count} duplicate rows "
                                f"for unique column '{column}'"
                            )
                        ),
                    )
                )

            for column in quality.not_null_columns:
                null_count = target_df.filter(target_df[column].isNull()).count()
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
            raise build_sql_runtime_error(
                message=f"SQL model validations failed for '{model.model_id}'",
                code=SqlRuntimeErrorCode.model_validation_failed,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "target_table_name": model.target_table_name,
                    "validation_results": summary.model_dump(mode="json"),
                },
            )

        return summary

    def _effective_partition_columns(self, *, model: CompiledSqlModel) -> list[str]:
        if model.partition_columns:
            return list(model.partition_columns)
        if model.load_mode == SqlLoadMode.full_refresh:
            return []
        if model.stage == SqlModelStage.level3:
            return ["source_name", "business_date"]
        return ["business_date"]

    def _validate_partition_requirements(self, *, model: CompiledSqlModel) -> None:
        effective_columns = self._effective_partition_columns(model=model)
        if model.load_mode != SqlLoadMode.partition_overwrite:
            return
        missing_columns = sorted(
            column for column in effective_columns if column not in self.partition_values
        )
        if missing_columns:
            raise build_sql_runtime_error(
                message="Partition overwrite requires runtime partition values",
                code=SqlRuntimeErrorCode.partition_value_missing,
                retryable=False,
                context={"model_id": model.model_id, "missing_columns": missing_columns},
            )

    def _explain_model(
        self,
        *,
        model: CompiledSqlModel,
        include_query_plan: bool,
    ) -> list[SqlQueryPlanStep]:
        select_sql = model.compiled_sql.rstrip().rstrip(";")
        try:
            plan_rows = self.spark.sql(f"EXPLAIN FORMATTED {select_sql}").collect()
        except PySparkException as exc:
            raise build_sql_runtime_error(
                message=f"Failed to validate SQL model '{model.model_id}'",
                code=SqlRuntimeErrorCode.planning_failed,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "target_table_name": model.target_table_name,
                    "warehouse_root": str(self.warehouse_root),
                },
            ) from exc
        if not include_query_plan:
            return []
        return [
            SqlQueryPlanStep(step_index=index, plan_text=str(row[0]))
            for index, row in enumerate(plan_rows)
        ]

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from elt_pipeline.config import runtime_context
from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.governance import build_governance_table_properties
from elt_pipeline.shared.path_utils import join_paths, path_exists
from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe
from elt_pipeline.sql._contract_enforcement import (
    enforce_data_contract_at_write,
    extract_schema_fields,
    try_read_existing_iceberg_table_schema,
)
from elt_pipeline.sql._staging_swap import (
    SwapMode,
    atomic_swap,
    best_effort_delete_staging,
    build_staging_path,
    validate_swap_scheme,
)
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.level2_source import Level2DatasetLocator
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    DataContractWarningRecord,
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

_DEFAULT_ICEBERG_CATALOG_NAME = runtime_manifest.catalogs.default_catalog_name


def _is_iceberg_enabled(spark: SparkSession) -> bool:
    """Return True if Iceberg is active for the given Spark session.

    3-tier resolution (single cascade through singleton):
      1. Spark session conf (builder already applied CLI arg > ENV > YAML > manifest)
         → If ``spark.sql.extensions`` contains the Iceberg extension class, it's ON.
      2. runtime_context singleton (``spark.enable_iceberg``) — explicit boolean
         from the single materializer: ONLY explicit ``false/0/no/off`` here
         can OVERRIDE and SHORT-CIRCUIT OFF (even if extensions somehow loaded).
         Explicit ``true/1/yes/on`` cannot turn iceberg ON without extensions
         actually being present in the session builder config.
      3. Fallback → presence of extensions in Spark conf.
    """
    try:
        conf_val = spark.conf.get("spark.sql.extensions", "")
    except Exception:  # noqa: BLE001
        conf_val = ""
    has_extension = runtime_manifest.classes.iceberg_spark_extensions.split(".")[-1] in conf_val
    ctx_val = runtime_context.get("spark.enable_iceberg")
    if ctx_val is not None:
        ctx_raw = str(ctx_val).strip().lower()
        if ctx_raw in ("false", "0", "no", "off"):
            return False
    return has_extension


def _iceberg_catalog_name(spark: SparkSession | None = None) -> str:
    """Resolve the active Iceberg catalog name.

    Precedence (single cascade):
      1. ``spark.sql.defaultCatalog`` — *actually* active in the session
         (build_spark_session applies CLI arg > ENV > YAML > manifest here).
      2. runtime_context singleton (``iceberg_writer.catalog_name``, fallback
         ``iceberg_serving.catalog_name``) — single materializer path.
      3. Frozen manifest default — ``iceberg``.
    """
    if spark is not None:
        try:
            default_catalog = spark.conf.get("spark.sql.defaultCatalog", "").strip()
        except Exception:  # noqa: BLE001
            default_catalog = ""
        if default_catalog:
            return default_catalog
    writer_name = runtime_context.get("iceberg_writer.catalog_name")
    if writer_name and str(writer_name).strip():
        return str(writer_name).strip()
    serving_name = runtime_context.get("iceberg_serving.catalog_name")
    if serving_name and str(serving_name).strip():
        return str(serving_name).strip()
    return _DEFAULT_ICEBERG_CATALOG_NAME


def _iceberg_table_fq(
    *, stage: SqlModelStage, domain: str, name: str, spark: SparkSession | None = None
) -> str:
    catalog = _iceberg_catalog_name(spark)
    return f"{catalog}.{stage.value}.{domain}.{name}"


class SparkSqlModelExecutor:
    def __init__(
        self,
        *,
        spark: SparkSession,
        warehouse_root: str,
        root_path: str,
        environment: str,
        run_id: str,
        partition_values: dict[str, str] | None = None,
    ) -> None:
        self.spark = spark
        self.warehouse_root = warehouse_root
        self.root_path = root_path
        self.environment = environment
        self.run_id = run_id
        self.partition_values = partition_values or {}
        self._level2_locator = Level2DatasetLocator(root_path=self.root_path, spark=spark)
        self._contract_warnings: list[DataContractWarningRecord] = []

    def _try_read_parquet_catalog_schema(
        self,
        target_path: str,
    ) -> dict[str, dict[str, Any]] | None:
        if not path_exists(target_path):
            return None
        try:
            existing_df = self.spark.read.parquet(target_path)
            return extract_schema_fields(existing_df.schema)
        except Exception:
            return None

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
                    alias_to_input_fqn = self._register_execute_inputs(
                        model=model, models_by_id=models_by_id
                    )
                    row_count, column_lineage_map = self._execute_model(
                        model=model,
                        alias_to_input_fqn=alias_to_input_fqn,
                    )
                except PySparkException as exc:
                    raise build_sql_runtime_error(
                        message=f"Failed to execute SQL model '{model.model_id}'",
                        code=SqlRuntimeErrorCode.execution_failed,
                        retryable=False,
                        context={
                            "model_id": model.model_id,
                            "target_table_name": model.target_table_name,
                            "warehouse_root": self.warehouse_root,
                        },
                    ) from exc

                record = SqlExecutionRecord(
                    model_id=model.model_id,
                    stage=model.stage,
                    target_table_name=model.target_table_name,
                    load_mode=model.load_mode,
                    row_count=row_count,
                    column_lineage_map=column_lineage_map,
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
            execution_result.contract_warnings = list(self._contract_warnings)
            if "execution_result" not in exc.context:
                exc.context["execution_result"] = execution_result.model_dump(mode="json")
            raise
        execution_result.contract_warnings = list(self._contract_warnings)
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

    def _table_path(self, *, stage: SqlModelStage, table_name: str) -> str:
        return join_paths(self.warehouse_root, stage.value, table_name)

    def _register_execute_inputs(
        self,
        *,
        model: CompiledSqlModel,
        models_by_id: dict[str, CompiledSqlModel],
    ) -> dict[str, str]:
        alias_to_input_fqn: dict[str, str] = {}
        output_namespace = (
            "iceberg" if _is_iceberg_enabled(self.spark) else "spark_parquet"
        )
        for source_ref in model.sources:
            dataframe = self._level2_locator.read(
                source_ref=source_ref,
                environment=source_ref.environment or self.environment,
            )
            dataframe.createOrReplaceTempView(source_ref.logical_name)
            fqn = source_ref.logical_name
            alias_to_input_fqn[source_ref.logical_name] = fqn

        use_iceberg = _is_iceberg_enabled(self.spark)
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
            if use_iceberg:
                dep_fq = _iceberg_table_fq(
                    stage=dependency.stage,
                    domain=dependency.domain,
                    name=dependency.target_table_name,
                    spark=self.spark,
                )
                dataframe = self.spark.table(dep_fq)
            else:
                dependency_path = self._table_path(
                    stage=dependency.stage, table_name=dependency.target_table_name
                )
                dataframe = self.spark.read.parquet(dependency_path)
            dataframe.createOrReplaceTempView(dependency.target_table_name)
            fqn = f"{output_namespace}:{dependency.target_table_name}"
            alias_to_input_fqn[dependency.target_table_name] = fqn

        return alias_to_input_fqn

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

    def _execute_model(
        self,
        *,
        model: CompiledSqlModel,
        alias_to_input_fqn: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, list[tuple[str, str]]]]:
        select_sql = model.compiled_sql.rstrip().rstrip(";")
        dataframe = self.spark.sql(select_sql)
        if alias_to_input_fqn is None:
            alias_to_input_fqn = {}
        column_lineage_map = extract_column_lineage_from_dataframe(
            dataframe, input_datasets_by_alias=alias_to_input_fqn
        )
        use_iceberg = _is_iceberg_enabled(self.spark)
        effective_partition_columns = self._effective_partition_columns(model=model)

        if use_iceberg:
            row_count = self._execute_iceberg_write(
                model=model,
                dataframe=dataframe,
                effective_partition_columns=effective_partition_columns,
            )
            return (row_count, column_lineage_map)

        target_path = self._table_path(stage=model.stage, table_name=model.target_table_name)

        if model.contract != "off":
            catalog_columns = self._try_read_parquet_catalog_schema(target_path)
            enforce_data_contract_at_write(
                model=model,
                dataframe_schema=dataframe.schema,
                catalog_columns=catalog_columns,
                execution_result_contract_warnings=self._contract_warnings,
            )

        if model.load_mode == SqlLoadMode.append:
            writer = dataframe.write.mode("append")
            if effective_partition_columns:
                writer = writer.partitionBy(*effective_partition_columns)
            writer.parquet(target_path)
            return (self.spark.read.parquet(target_path).count(), column_lineage_map)

        if model.load_mode == SqlLoadMode.partition_overwrite:
            self._validate_partition_requirements(model=model)

        scheme = validate_swap_scheme(target_path, model.model_id)
        staging_root = model.staging_root or join_paths(self.warehouse_root, "_staging")
        staging_path = build_staging_path(
            staging_root=staging_root,
            stage=model.stage,
            target_table_name=model.target_table_name,
            run_id=self.run_id,
        )

        try:
            writer = dataframe.write.mode("overwrite")
            if effective_partition_columns:
                writer = writer.partitionBy(*effective_partition_columns)
            writer.parquet(staging_path)
        except PySparkException as exc:
            best_effort_delete_staging(staging_path, scheme)
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.staging_write_failed,
                message="Failed to write staging parquet output during SQL materialization",
                retryable=True,
                context={
                    "model_id": model.model_id,
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            ) from exc

        try:
            staging_df: DataFrame = self.spark.read.parquet(staging_path)
            row_count = staging_df.count()
        except PySparkException as exc:
            best_effort_delete_staging(staging_path, scheme)
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.staging_write_failed,
                message=(
                    "Staging write unverifiable — could not read back staging parquet output. "
                    "Input DataFrame write reported success, but post-write read failed."
                ),
                retryable=True,
                context={
                    "model_id": model.model_id,
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            ) from exc

        swap_mode: SwapMode = (
            "partition_overwrite"
            if model.load_mode == SqlLoadMode.partition_overwrite
            else "full_refresh"
        )
        try:
            atomic_swap(
                staging_path=staging_path,
                target_path=target_path,
                scheme=scheme,
                mode=swap_mode,
            )
        except PipelineError as exc:
            best_effort_delete_staging(staging_path, scheme)
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.atomic_swap_failed,
                message=(
                    "Atomic swap from staging to canonical path "
                    "failed during SQL materialization"
                ),
                retryable=exc.retryable,
                context={
                    "model_id": model.model_id,
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "swap_mode": swap_mode,
                    "operator_action": (
                        "Inspect target path for partial state. Staging contents preserved "
                        "in staging_path on a best-effort basis if the error occurred mid-copy; "
                        "staging cleanup may have been partial."
                    ),
                    "underlying_error": str(exc),
                },
            ) from exc
        except Exception as exc:
            best_effort_delete_staging(staging_path, scheme)
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.atomic_swap_failed,
                message="Unexpected error during atomic swap from staging to canonical path",
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "swap_mode": swap_mode,
                    "underlying_error": str(exc),
                },
            ) from exc

        best_effort_delete_staging(staging_path, scheme)
        return (row_count, column_lineage_map)

    def _apply_governance_table_properties(
        self,
        *,
        fq_table: str,
        model: CompiledSqlModel,
        row_count: int,
        owner_name: str | None = None,
        owner_email: str | None = None,
    ) -> int:
        props = build_governance_table_properties(
            governance=model.governance,
            domain=model.domain,
            owner_name=owner_name,
            owner_email=owner_email,
        )
        props["elt.run.last_model_id"] = model.model_id
        props["elt.run.last_row_count"] = str(row_count)
        if not props:
            return row_count

        def _escape_key(key: str) -> str:
            return "'" + key.replace("'", "''") + "'"

        def _escape_val(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        set_clauses = ", ".join(
            f"{_escape_key(k)} = {_escape_val(v)}" for k, v in props.items()
        )
        try:
            self.spark.sql(f"ALTER TABLE {fq_table} SET TBLPROPERTIES ({set_clauses})")
        except PySparkException:
            pass
        return row_count

    def _execute_iceberg_write(
        self,
        *,
        model: CompiledSqlModel,
        dataframe: DataFrame,
        effective_partition_columns: list[str],
    ) -> int:
        catalog = _iceberg_catalog_name(self.spark)
        fq_table = _iceberg_table_fq(
            stage=model.stage, domain=model.domain, name=model.target_table_name,
            spark=self.spark,
        )
        namespace = f"{catalog}.{model.stage.value}.{model.domain}"
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        except PySparkException as exc:
            raise build_sql_runtime_error(
                message=(
                    f"Failed to create Iceberg namespace {namespace!r} "
                    f"for model {model.model_id!r}"
                ),
                code=SqlRuntimeErrorCode.execution_failed,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "namespace": namespace,
                    "underlying_error": str(exc),
                },
            ) from exc

        if model.contract != "off":
            catalog_columns = try_read_existing_iceberg_table_schema(self.spark, fq_table)
            enforce_data_contract_at_write(
                model=model,
                dataframe_schema=dataframe.schema,
                catalog_columns=catalog_columns,
                execution_result_contract_warnings=self._contract_warnings,
            )

        if model.load_mode == SqlLoadMode.partition_overwrite:
            self._validate_partition_requirements(model=model)
            try:
                (
                    dataframe
                    .writeTo(fq_table)
                    .option("mergeSchema", "true")
                    .partitionedBy(*effective_partition_columns)
                    .overwritePartitions()
                )
            except PySparkException as exc:
                raise build_sql_runtime_error(
                    message=(
                        f"Iceberg partition_overwrite failed for model {model.model_id!r}"
                    ),
                    code=SqlRuntimeErrorCode.execution_failed,
                    retryable=True,
                    context={
                        "model_id": model.model_id,
                        "target_table": fq_table,
                        "partition_values": self.partition_values,
                        "underlying_error": str(exc),
                    },
                ) from exc
            row_count = self.spark.table(fq_table).count()
            return self._apply_governance_table_properties(
                fq_table=fq_table, model=model, row_count=row_count
            )

        if model.load_mode == SqlLoadMode.append:
            try:
                writer = (
                    dataframe.writeTo(fq_table)
                    .option("mergeSchema", "true")
                )
                if effective_partition_columns:
                    writer = writer.partitionedBy(*effective_partition_columns)
                writer.append()
            except PySparkException:
                # First-run append to a non-existent table: fall back to createOrReplace
                # so the target bootstraps cleanly.
                try:
                    writer = (
                        dataframe.writeTo(fq_table)
                        .using("iceberg")
                        .option("mergeSchema", "true")
                    )
                    if effective_partition_columns:
                        writer = writer.partitionedBy(*effective_partition_columns)
                    writer.create()
                except PySparkException as create_exc:
                    raise build_sql_runtime_error(
                        message=(
                            f"Iceberg append failed for model {model.model_id!r}"
                        ),
                        code=SqlRuntimeErrorCode.execution_failed,
                        retryable=True,
                        context={
                            "model_id": model.model_id,
                            "target_table": fq_table,
                            "underlying_error": str(create_exc),
                        },
                    ) from create_exc
            row_count = self.spark.table(fq_table).count()
            return self._apply_governance_table_properties(
                fq_table=fq_table, model=model, row_count=row_count
            )

        try:
            writer = (
                dataframe.writeTo(fq_table)
                .using("iceberg")
                .option("mergeSchema", "true")
            )
            if effective_partition_columns:
                writer = writer.partitionedBy(*effective_partition_columns)
            writer.createOrReplace()
        except PySparkException as exc:
            raise build_sql_runtime_error(
                message=(
                    f"Iceberg full_refresh createOrReplace failed "
                    f"for model {model.model_id!r}"
                ),
                code=SqlRuntimeErrorCode.execution_failed,
                retryable=True,
                context={
                    "model_id": model.model_id,
                    "target_table": fq_table,
                    "underlying_error": str(exc),
                },
            ) from exc
        row_count = self.spark.table(fq_table).count()
        return self._apply_governance_table_properties(
            fq_table=fq_table, model=model, row_count=row_count
        )

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
            if _is_iceberg_enabled(self.spark):
                fq_table = _iceberg_table_fq(
                    stage=model.stage,
                    domain=model.domain,
                    name=model.target_table_name,
                    spark=self.spark,
                )
                target_df = self.spark.table(fq_table)
            else:
                target_path = self._table_path(
                    stage=model.stage, table_name=model.target_table_name
                )
                target_df = self.spark.read.parquet(target_path)

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
                    "warehouse_root": self.warehouse_root,
                },
            ) from exc
        if not include_query_plan:
            return []
        return [
            SqlQueryPlanStep(step_index=index, plan_text=str(row[0]))
            for index, row in enumerate(plan_rows)
        ]

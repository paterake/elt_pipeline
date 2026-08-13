from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from elt_pipeline.integrations import (
    QualityCheckResult,
    QualityCheckStatus,
    QualityHookPolicy,
    build_quality_hook,
)
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.shared.runtime import StageName, new_run_context
from elt_pipeline.sql import (
    SparkSqlModelExecutor,
    SqlRuntimeErrorCode,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    resolve_selected_model_ids,
    run_sql_models_locally,
    topologically_sort_sql_models,
)


def _seed_level2_table(
    spark_session,
    root_path: Path,
    *,
    environment: str = "dev",
    source_name: str = "orders_source",
    entity_name: str = "orders",
    table_name: str = "raw_orders",
    rows: list[dict],
    ingest_date: str = "2026-01-15",
    run_id: str = "seed-run",
) -> None:
    _ = environment
    data_dir = (
        root_path
        / "level2"
        / f"source={source_name}"
        / f"entity={entity_name}"
        / "mapping_version=v1"
        / f"table={table_name}"
        / f"run_id={run_id}"
    )
    enriched_rows = [
        {
            **row,
            "source_name": source_name,
            "ingest_date": ingest_date,
            "_run_id": run_id,
        }
        for row in rows
    ]
    spark_session.createDataFrame(enriched_rows).write.mode("error").parquet(str(data_dir))


def test_discover_sql_models_reads_valid_package(tmp_path: Path) -> None:
    package_root = _write_basic_sql_package(tmp_path)

    models = discover_sql_models(package_root)

    assert [model.model_id for model in models] == [
        "level3.sales.base_orders",
        "level4.sales.order_summary",
    ]
    assert models[0].manifest.target.table_name == "base_orders"
    assert "select order_id" in models[0].sql_text


def test_discover_sql_models_rejects_manifest_path_mismatch(tmp_path: Path) -> None:
    package_root = tmp_path / "models"
    model_dir = package_root / "level3" / "sales" / "wrong_name"
    model_dir.mkdir(parents=True)
    (model_dir / "model.sql").write_text("select 1 as value", encoding="utf-8")
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: right_name
            stage: level3
            domain: sales
            target:
              table_name: right_name
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match="name does not match directory structure"):
        discover_sql_models(package_root)


def test_compile_sql_model_resolves_tokens(tmp_path: Path) -> None:
    models = discover_sql_models(_write_basic_sql_package(tmp_path))
    base_model = filter_sql_models(
        models,
        stage="level3",
        domain="sales",
        model_name="base_orders",
    )[0]

    compiled = compile_sql_model(
        base_model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-123",
            stage=base_model.manifest.stage.value,
            domain=base_model.manifest.domain,
            model_name=base_model.manifest.name,
            target_table_name=base_model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-31",
        ),
    )

    assert "where order_date >= '2026-01-01'" in compiled.compiled_sql
    assert compiled.token_values["window.end_date"] == "2026-01-31"
    assert compiled.sources[0].logical_name == "raw_orders"


def test_compile_sql_model_resolves_source_namespace_tokens(tmp_path: Path) -> None:
    package_root = tmp_path / "sql_models"
    model_dir = package_root / "level3" / "sales" / "source_token_test"
    model_dir.mkdir(parents=True)
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: source_token_test
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            target:
              table_name: source_token_test
            sources:
              - logical_name: t
                source_name: orders_source
                entity_name: orders
                table_name: orders_table
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (model_dir / "model.sql").write_text(
        dedent(
            """
            select
              '{{ source.name }}' as source_name,
              '{{ source.entity }}' as entity_name,
              '{{ source.table }}' as table_name
            from t
            """
        ).strip(),
        encoding="utf-8",
    )
    models = discover_sql_models(package_root)
    model = models[0]

    compiled_explicit = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-1",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-31",
            source_name="EXPLICIT_source",
            source_entity="EXPLICIT_entity",
            source_table="EXPLICIT_table",
        ),
    )
    assert "'EXPLICIT_source' as source_name" in compiled_explicit.compiled_sql
    assert "'EXPLICIT_entity' as entity_name" in compiled_explicit.compiled_sql
    assert "'EXPLICIT_table' as table_name" in compiled_explicit.compiled_sql
    assert compiled_explicit.token_values["source.name"] == "EXPLICIT_source"
    assert compiled_explicit.token_values["source.entity"] == "EXPLICIT_entity"
    assert compiled_explicit.token_values["source.table"] == "EXPLICIT_table"

    compiled_implicit = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-2",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-31",
        ),
    )
    assert "'orders_source' as source_name" in compiled_implicit.compiled_sql
    assert "'orders' as entity_name" in compiled_implicit.compiled_sql
    assert "'orders_table' as table_name" in compiled_implicit.compiled_sql
    assert compiled_implicit.token_values["source.name"] == "orders_source"
    assert compiled_implicit.token_values["source.entity"] == "orders"
    assert compiled_implicit.token_values["source.table"] == "orders_table"


def test_compile_sql_model_fails_for_missing_token(tmp_path: Path) -> None:
    package_root = _write_basic_sql_package(tmp_path)
    model = discover_sql_models(package_root)[0]

    with pytest.raises(PipelineError, match="Missing SQL compilation token"):
        compile_sql_model(model, token_context={"environment": "dev"})


def test_compile_sql_model_fails_for_non_scalar_token(tmp_path: Path) -> None:
    package_root = _write_basic_sql_package(tmp_path)
    model = discover_sql_models(package_root)[0]

    with pytest.raises(PipelineError, match="non-scalar value"):
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
                extra_values={"window": {"start_date": {"nested": "bad"}}},
            ),
        )


def test_topologically_sort_sql_models_orders_dependencies(tmp_path: Path) -> None:
    models = discover_sql_models(_write_basic_sql_package(tmp_path))

    ordered = topologically_sort_sql_models(models)
    selected = resolve_selected_model_ids(
        all_models=models,
        selected_models=filter_sql_models(
            models,
            stage="level4",
            domain="sales",
            model_name="order_summary",
        ),
        include_dependencies=True,
    )

    assert [model.model_id for model in ordered] == [
        "level3.sales.base_orders",
        "level4.sales.order_summary",
    ]
    assert selected == {"level3.sales.base_orders", "level4.sales.order_summary"}


def test_topologically_sort_sql_models_resolves_shorthand_dependencies(tmp_path: Path) -> None:
    models = discover_sql_models(_write_same_stage_dependency_sql_package(tmp_path))

    ordered = topologically_sort_sql_models(models)
    selected = resolve_selected_model_ids(
        all_models=models,
        selected_models=filter_sql_models(
            models,
            stage="level3",
            domain="sales",
            model_name="downstream_orders",
        ),
        include_dependencies=True,
    )

    assert [model.model_id for model in ordered] == [
        "level3.sales.base_orders",
        "level3.sales.downstream_orders",
    ]
    assert selected == {"level3.sales.base_orders", "level3.sales.downstream_orders"}


def test_topologically_sort_sql_models_rejects_dependency_cycles(tmp_path: Path) -> None:
    package_root = _write_cyclic_sql_package(tmp_path)

    with pytest.raises(PipelineError, match="dependency cycle detected"):
        topologically_sort_sql_models(discover_sql_models(package_root))


def test_topologically_sort_sql_models_rejects_missing_dependencies(tmp_path: Path) -> None:
    package_root = _write_missing_dependency_sql_package(tmp_path)

    with pytest.raises(PipelineError, match="could not be resolved"):
        topologically_sort_sql_models(discover_sql_models(package_root))


def test_sql_model_manifest_rejects_sources_on_level4_models(tmp_path: Path) -> None:
    package_root = tmp_path / "invalid_sources_models"
    model_dir = package_root / "level4" / "sales" / "bad_model"
    model_dir.mkdir(parents=True)
    (model_dir / "model.sql").write_text("select 1 as value", encoding="utf-8")
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: bad_model
            stage: level4
            domain: sales
            target:
              table_name: bad_model
            sources:
              - logical_name: raw_orders
                source_name: orders_source
                entity_name: orders
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match="SQL model manifest validation failed"):
        discover_sql_models(package_root)


def test_local_sql_model_executor_runs_models_in_spark(tmp_path: Path, spark_session) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
            {"order_id": 3, "amount": 30, "order_date": "2026-01-07"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]

    warehouse_root = tmp_path / "warehouse"
    result = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=tmp_path,
        environment="dev",
    ).execute(compiled)

    assert result.model_count == 2
    base_orders_count = spark_session.read.parquet(
        str(warehouse_root / "level3" / "base_orders")
    ).count()
    summary_rows = sorted(
        (
            row.asDict()
            for row in spark_session.read.parquet(
                str(warehouse_root / "level4" / "order_summary")
            ).collect()
        ),
        key=lambda row: row["order_date"],
    )

    assert base_orders_count == 3
    assert [(row["order_date"], row["total_amount"]) for row in summary_rows] == [
        ("2026-01-01", 10),
        ("2026-01-03", 20),
        ("2026-01-07", 30),
    ]


def test_local_sql_model_executor_plans_models_with_query_plan(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    models = discover_sql_models(_write_basic_sql_package(tmp_path))
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in topologically_sort_sql_models(models)
    ]

    planning_result = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=tmp_path / "warehouse",
        root_path=tmp_path,
        environment="dev",
    ).plan(
        compiled,
        include_query_plan=True,
    )

    assert planning_result.model_count == 2
    assert [model.model_id for model in planning_result.planned_models] == [
        "level3.sales.base_orders",
        "level4.sales.order_summary",
    ]
    assert planning_result.planned_models[0].query_plan
    assert planning_result.planned_models[1].depends_on == ["level3.sales.base_orders"]


def test_local_sql_model_executor_appends_rows_across_runs(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
            {"order_id": 3, "amount": 30, "order_date": "2026-01-07"},
        ],
    )

    model = discover_sql_models(_write_append_sql_package(tmp_path))[0]
    warehouse_root = tmp_path / "warehouse"
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=tmp_path,
        environment="dev",
    )

    first_run = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-123",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-03",
        ),
    )
    second_run = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-124",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
            start_date="2026-01-04",
            end_date="2026-01-31",
        ),
    )

    first_result = executor.execute([first_run])
    second_result = executor.execute([second_run])

    assert first_result.executed_models[0].load_mode.value == "append"
    assert second_result.executed_models[0].row_count == 3
    rows = sorted(
        (
            row.asDict()
            for row in spark_session.read.parquet(
                str(warehouse_root / "level3" / "appended_orders")
            ).collect()
        ),
        key=lambda row: row["order_id"],
    )
    assert [(row["order_id"], row["amount"], row["order_date"]) for row in rows] == [
        (1, 10, "2026-01-01"),
        (2, 20, "2026-01-03"),
        (3, 30, "2026-01-07"),
    ]


def test_local_sql_model_executor_partition_overwrite_replaces_selected_partition(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    existing_path = warehouse_root / "level3" / "daily_orders"
    daily_orders_schema = StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField("order_date", StringType(), nullable=False),
        ]
    )
    spark_session.createDataFrame(
        [
            {"order_id": 999, "order_date": "2026-01-01"},
            {"order_id": 200, "order_date": "2026-01-02"},
        ],
        schema=daily_orders_schema,
    ).write.mode("overwrite").partitionBy("order_date").parquet(str(existing_path))

    model = discover_sql_models(_write_partition_overwrite_sql_package(tmp_path))[0]
    compiled = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-123",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
        ),
    )

    result = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=tmp_path,
        environment="dev",
        partition_values={"order_date": "2026-01-01"},
    ).execute([compiled])

    assert result.executed_models[0].load_mode.value == "partition_overwrite"
    rows = sorted(
        (
            row.asDict()
            for row in spark_session.read.parquet(str(existing_path)).collect()
        ),
        key=lambda row: (str(row["order_date"]), row["order_id"]),
    )
    assert [(row["order_id"], str(row["order_date"])) for row in rows] == [
        (1, "2026-01-01"),
        (200, "2026-01-02"),
    ]


def test_run_sql_models_locally_writes_audit_log_and_lineage_artifacts(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]

    result = run_sql_models_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.sql, job_name="sql-run"),
        environment="dev",
        package_path=package_root,
        warehouse_root=tmp_path / "warehouse",
        spark=spark_session,
        compiled_models=compiled,
    )

    assert result.execution_result.model_count == 2
    assert result.artifacts.audit_path is not None
    assert result.artifacts.log_path is not None
    assert result.artifacts.lineage_path is not None
    assert result.artifacts.audit_path.exists()
    assert result.artifacts.log_path.exists()
    assert result.artifacts.lineage_path.exists()

    audit_payload = json.loads(result.artifacts.audit_path.read_text(encoding="utf-8"))
    log_lines = result.artifacts.log_path.read_text(encoding="utf-8").strip().splitlines()
    lineage_lines = result.artifacts.lineage_path.read_text(encoding="utf-8").strip().splitlines()

    assert audit_payload["status"] == "success"
    assert audit_payload["metrics_summary"]["extra"]["models_executed"] == 2
    assert audit_payload["context"]["environment"] == "dev"
    assert any(json.loads(line)["event_type"] == "sql_model_executed" for line in log_lines)
    assert any(
        any(
            output.get("facets", {}).get("model_id") == "level4.sales.order_summary"
            for output in json.loads(line).get("outputs", [])
        )
        for line in lineage_lines
    )


def test_run_sql_models_locally_captures_validation_results_in_audit(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(
        tmp_path,
        base_orders_quality="""
quality:
  row_count_min: 2
  unique_columns:
    - order_id
  not_null_columns:
    - order_id
""",
    )
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]

    result = run_sql_models_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.sql, job_name="sql-run"),
        environment="dev",
        package_path=package_root,
        warehouse_root=tmp_path / "warehouse",
        spark=spark_session,
        compiled_models=compiled,
    )

    assert len(result.execution_result.model_validations) == 2

    audit_payload = json.loads(result.artifacts.audit_path.read_text(encoding="utf-8"))
    base_orders_validation = next(
        summary
        for summary in audit_payload["validation_results"]
        if summary["model_id"] == "level3.sales.base_orders"
    )

    assert audit_payload["metrics_summary"]["extra"]["validation_models_evaluated"] == 2
    assert audit_payload["metrics_summary"]["extra"]["validation_failures"] == 0
    assert base_orders_validation["passed"] is True
    assert [result["validation_type"] for result in base_orders_validation["validations"]] == [
        "row_count_min",
        "unique_columns",
        "not_null_columns",
    ]


def test_run_sql_models_locally_fails_on_validation_error_and_audits_results(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 1, "amount": 20, "order_date": "2026-01-03"},
            {"order_id": None, "amount": 30, "order_date": "2026-01-04"},
        ],
    )

    package_root = _write_basic_sql_package(
        tmp_path,
        base_orders_quality="""
quality:
  unique_columns:
    - order_id
  not_null_columns:
    - order_id
""",
    )
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]
    run_context = new_run_context(stage=StageName.sql, job_name="sql-run")

    with pytest.raises(PipelineError, match="SQL model validations failed"):
        run_sql_models_locally(
            root_path=tmp_path,
            run_context=run_context,
            environment="dev",
            package_path=package_root,
            warehouse_root=tmp_path / "warehouse",
            spark=spark_session,
            compiled_models=compiled,
        )

    audit_path = (
        tmp_path
        / "runs"
        / "stage=sql"
        / "job=sql-run"
        / f"run_id={run_context.run_id}"
        / "audit.json"
    )
    error_path = audit_path.with_name("errors.jsonl")

    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert "environment=" not in str(audit_path)
    error_lines = error_path.read_text(encoding="utf-8").strip().splitlines()
    base_orders_validation = audit_payload["validation_results"][0]
    failed_checks = [
        result for result in base_orders_validation["validations"] if result["passed"] is False
    ]

    assert audit_payload["status"] == "failed"
    assert audit_payload["error_summary"]["error_code"] == "SQL_MODEL_VALIDATION_FAILED"
    assert audit_payload["metrics_summary"]["extra"]["models_executed"] == 1
    assert audit_payload["metrics_summary"]["extra"]["validation_failures"] == 2
    assert base_orders_validation["model_id"] == "level3.sales.base_orders"
    assert {result["validation_type"] for result in failed_checks} == {
        "unique_columns",
        "not_null_columns",
    }
    assert json.loads(error_lines[0])["error_code"] == SqlRuntimeErrorCode.model_validation_failed


def test_run_sql_models_locally_captures_quality_results_in_audit(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]

    result = run_sql_models_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.sql, job_name="sql-run"),
        environment="dev",
        package_path=package_root,
        warehouse_root=tmp_path / "warehouse",
        spark=spark_session,
        compiled_models=compiled,
        quality_hook=build_quality_hook(tmp_path, backend=_PassingSqlQualityBackend()),
    )

    audit_payload = json.loads(result.artifacts.audit_path.read_text(encoding="utf-8"))
    quality_summary = audit_payload["validation_results"][-1]

    assert audit_payload["status"] == "success"
    assert quality_summary["backend_type"] == "test_quality"
    assert quality_summary["results"][0]["status"] == "pass"
    assert audit_payload["metrics_summary"]["extra"]["quality.pass"] == 1


def test_run_sql_models_locally_fails_for_blocking_quality_results(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]
    run_context = new_run_context(stage=StageName.sql, job_name="sql-run")

    with pytest.raises(PipelineError, match="Quality checks failed"):
        run_sql_models_locally(
            root_path=tmp_path,
            run_context=run_context,
            environment="dev",
            package_path=package_root,
            warehouse_root=tmp_path / "warehouse",
            spark=spark_session,
            compiled_models=compiled,
            quality_hook=build_quality_hook(
                tmp_path,
                backend=_FailingSqlQualityBackend(),
                policy=QualityHookPolicy.blocking,
            ),
        )

    audit_path = (
        tmp_path
        / "runs"
        / "stage=sql"
        / "job=sql-run"
        / f"run_id={run_context.run_id}"
        / "audit.json"
    )
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert "environment=" not in str(audit_path)
    quality_summary = audit_payload["validation_results"][-1]

    assert audit_payload["status"] == "failed"
    assert audit_payload["error_summary"]["error_code"] == "QUALITY_CHECK_FAILED"
    assert quality_summary["results"][0]["status"] == "fail"
    assert audit_payload["metrics_summary"]["extra"]["quality.fail"] == 1


def test_run_sql_models_locally_logs_warning_for_non_blocking_quality_results(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]

    result = run_sql_models_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.sql, job_name="sql-run"),
        environment="dev",
        package_path=package_root,
        warehouse_root=tmp_path / "warehouse",
        spark=spark_session,
        compiled_models=compiled,
        quality_hook=build_quality_hook(
            tmp_path,
            backend=_WarningSqlQualityBackend(),
            policy=QualityHookPolicy.best_effort,
        ),
    )

    log_lines = result.artifacts.log_path.read_text(encoding="utf-8").strip().splitlines()
    parsed_log_lines = [json.loads(line) for line in log_lines]
    quality_event = next(
        event for event in parsed_log_lines if event["event_type"] == "quality_hook_complete"
    )

    assert quality_event["severity"] == "WARNING"


def test_run_sql_models_locally_records_single_error_for_blocking_quality_backend_failure(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        rows=[
            {"order_id": 1, "amount": 10, "order_date": "2026-01-01"},
            {"order_id": 2, "amount": 20, "order_date": "2026-01-03"},
        ],
    )

    package_root = _write_basic_sql_package(tmp_path)
    discovered = discover_sql_models(package_root)
    ordered = topologically_sort_sql_models(discovered)
    compiled = [
        compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id="run-123",
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-01-31",
            ),
        )
        for model in ordered
    ]
    run_context = new_run_context(stage=StageName.sql, job_name="sql-run")

    with pytest.raises(PipelineError, match="Optional data-quality backend execution failed"):
        run_sql_models_locally(
            root_path=tmp_path,
            run_context=run_context,
            environment="dev",
            package_path=package_root,
            warehouse_root=tmp_path / "warehouse",
            spark=spark_session,
            compiled_models=compiled,
            quality_hook=build_quality_hook(
                tmp_path,
                backend=_ExplodingSqlQualityBackend(),
                policy=QualityHookPolicy.blocking,
            ),
        )

    errors_path = (
        tmp_path
        / "runs"
        / "stage=sql"
        / "job=sql-run"
        / f"run_id={run_context.run_id}"
        / "errors.jsonl"
    )
    assert "environment=" not in str(errors_path)
    error_records = [
        json.loads(line) for line in errors_path.read_text(encoding="utf-8").splitlines() if line
    ]

    assert len(error_records) == 1
    assert error_records[0]["error_code"] == "QUALITY_BACKEND_EXECUTION_FAILED"


def test_local_sql_model_executor_returns_structured_planning_error_code(
    tmp_path: Path, spark_session
) -> None:
    package_root = _write_basic_sql_package(tmp_path)
    model = discover_sql_models(package_root)[0]
    compiled = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-123",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-31",
        ),
    )

    with pytest.raises(PipelineError) as exc_info:
        SparkSqlModelExecutor(
            spark=spark_session,
            warehouse_root=tmp_path / "warehouse",
            root_path=tmp_path,
            environment="dev",
        ).plan([compiled])

    assert exc_info.value.error_code == SqlRuntimeErrorCode.level2_source_not_found


def test_local_sql_model_executor_returns_structured_partition_error_code(
    tmp_path: Path, spark_session
) -> None:
    model = discover_sql_models(
        _write_partition_overwrite_sql_package(tmp_path),
    )[0]
    compiled = compile_sql_model(
        model,
        token_context=build_token_context(
            environment="dev",
            run_id="run-123",
            stage=model.manifest.stage.value,
            domain=model.manifest.domain,
            model_name=model.manifest.name,
            target_table_name=model.manifest.target.table_name,
        ),
    )

    with pytest.raises(PipelineError) as exc_info:
        SparkSqlModelExecutor(
            spark=spark_session,
            warehouse_root=tmp_path / "warehouse",
            root_path=tmp_path,
            environment="dev",
        ).execute([compiled])

    assert exc_info.value.error_code == SqlRuntimeErrorCode.partition_value_missing
    assert exc_info.value.error_category.value == "config_error"


def test_level3_model_applies_default_partitions_and_repartitions_late_arriving_data(
    tmp_path: Path, spark_session
) -> None:
    _seed_level2_table(
        spark_session,
        tmp_path,
        source_name="orders_source",
        entity_name="orders",
        table_name="raw_orders",
        ingest_date="2026-08-10",
        run_id="seed-late-run",
        rows=[
            {"order_id": 1001, "amount": 50, "business_date": "2026-07-31"},
            {"order_id": 1002, "amount": 75, "business_date": "2026-07-31"},
            {"order_id": 1003, "amount": 20, "business_date": "2026-08-10"},
            {"order_id": 1004, "amount": 40, "business_date": "2026-08-10"},
            {"order_id": 1005, "amount": 60, "business_date": "2026-08-10"},
        ],
    )

    warehouse_root = tmp_path / "warehouse"
    pre_seed_path = (
        warehouse_root
        / "level3"
        / "canonical_orders"
        / "source_name=orders_source"
        / "business_date=2026-06-01"
    )
    pre_seed_path.mkdir(parents=True, exist_ok=True)
    spark_session.createDataFrame(
        [
            {
                "order_id": 9990,
                "amount": 100,
                "business_date": "2026-06-01",
                "source_name": "orders_source",
            },
            {
                "order_id": 9991,
                "amount": 200,
                "business_date": "2026-06-01",
                "source_name": "orders_source",
            },
        ]
    ).write.mode("overwrite").parquet(str(pre_seed_path))

    package_root = _write_late_arrival_level3_sql_package(tmp_path)
    model = discover_sql_models(package_root)[0]
    assert model.manifest.target.partition_columns == [], (
        "Test fixture must declare NO manifest partition_columns to exercise the default convention"
    )

    def compile_and_run(run_id: str):
        compiled = compile_sql_model(
            model,
            token_context=build_token_context(
                environment="dev",
                run_id=run_id,
                stage=model.manifest.stage.value,
                domain=model.manifest.domain,
                model_name=model.manifest.name,
                target_table_name=model.manifest.target.table_name,
            ),
        )
        return SparkSqlModelExecutor(
            spark=spark_session,
            warehouse_root=warehouse_root,
            root_path=tmp_path,
            environment="dev",
            partition_values={"source_name": "orders_source", "business_date": "2026-08-10"},
        ).execute([compiled])

    first_result = compile_and_run("run-late-1")
    assert first_result.executed_models[0].load_mode.value == "partition_overwrite"

    canonical_root = warehouse_root / "level3" / "canonical_orders"
    source_partitions = sorted(
        p.name
        for p in canonical_root.iterdir()
        if p.is_dir() and p.name.startswith("source_name=")
    )
    assert source_partitions == ["source_name=orders_source"]

    business_partition_dirs = sorted(
        p.name
        for p in (canonical_root / "source_name=orders_source").iterdir()
        if p.is_dir() and p.name.startswith("business_date=")
    )
    assert business_partition_dirs == [
        "business_date=2026-06-01",
        "business_date=2026-07-31",
        "business_date=2026-08-10",
    ]

    pre_seed_count = spark_session.read.parquet(str(pre_seed_path)).count()
    assert pre_seed_count == 2, (
        "Unrelated pre-seed partition must survive dynamic partition overwrite"
    )

    jul31_count = spark_session.read.parquet(
        str(canonical_root / "source_name=orders_source" / "business_date=2026-07-31")
    ).count()
    aug10_count = spark_session.read.parquet(
        str(canonical_root / "source_name=orders_source" / "business_date=2026-08-10")
    ).count()
    assert jul31_count == 2, (
        "Late-arriving 2026-07-31 data must be co-located in its own business_date partition"
    )
    assert aug10_count == 3

    second_result = compile_and_run("run-late-2")
    assert second_result.executed_models[0].row_count == first_result.executed_models[0].row_count

    post_idempotency_dirs = sorted(
        p.name
        for p in (canonical_root / "source_name=orders_source").iterdir()
        if p.is_dir() and p.name.startswith("business_date=")
    )
    assert post_idempotency_dirs == business_partition_dirs

    post_jul31 = spark_session.read.parquet(
        str(canonical_root / "source_name=orders_source" / "business_date=2026-07-31")
    ).count()
    post_aug10 = spark_session.read.parquet(
        str(canonical_root / "source_name=orders_source" / "business_date=2026-08-10")
    ).count()
    assert post_jul31 == jul31_count, "Idempotent re-run must produce identical partition contents"
    assert post_aug10 == aug10_count


def _write_basic_sql_package(
    base_path: Path,
    *,
    base_orders_quality: str = "",
    order_summary_quality: str = "",
    order_summary_depends_on: str = "",
) -> Path:
    package_root = base_path / "sql_models"
    base_orders_dir = package_root / "level3" / "sales" / "base_orders"
    base_orders_dir.mkdir(parents=True, exist_ok=True)
    (base_orders_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: base_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            target:
              table_name: base_orders
            sources:
              - logical_name: raw_orders
                source_name: orders_source
                entity_name: orders
            owner:
              name: platform
            {base_orders_quality}
            """
        ).format(base_orders_quality=base_orders_quality.strip()).strip(),
        encoding="utf-8",
    )
    (base_orders_dir / "model.sql").write_text(
        dedent(
            """
            select order_id, amount, order_date
            from raw_orders
            where order_date >= '{{ window.start_date }}'
              and order_date <= '{{ window.end_date }}'
            """
        ).strip(),
        encoding="utf-8",
    )

    order_summary_dir = package_root / "level4" / "sales" / "order_summary"
    order_summary_dir.mkdir(parents=True, exist_ok=True)
    (order_summary_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: order_summary
            stage: level4
            domain: sales
            materialization: table
            load_mode: full_refresh
            {order_summary_depends_on}
            target:
              table_name: order_summary
            owner:
              name: platform
            {order_summary_quality}
            """
        )
        .format(
            order_summary_depends_on=(
                order_summary_depends_on.strip()
                or "depends_on:\n  - level3.sales.base_orders"
            ),
            order_summary_quality=order_summary_quality.strip(),
        )
        .strip(),
        encoding="utf-8",
    )
    (order_summary_dir / "model.sql").write_text(
        dedent(
            """
            select order_date, sum(amount) as total_amount
            from base_orders
            group by order_date
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root


class _PassingSqlQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request):
        return [
            QualityCheckResult(
                backend_type=self.backend_type,
                check_name="model_count",
                status=QualityCheckStatus.pass_,
                observed_value=len(request.datasets),
                expected_value=2,
            )
        ]


class _FailingSqlQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request):
        return [
            QualityCheckResult(
                backend_type=self.backend_type,
                check_name="row_count_min",
                status=QualityCheckStatus.fail,
                dataset_id=request.datasets[-1].dataset_id,
                dataset_name=request.datasets[-1].dataset_name,
                observed_value=request.datasets[-1].row_count,
                expected_value=3,
                message="Row count below threshold",
            )
        ]


class _WarningSqlQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request):
        return [
            QualityCheckResult(
                backend_type=self.backend_type,
                check_name="distribution_drift",
                status=QualityCheckStatus.warn,
                dataset_id=request.datasets[-1].dataset_id,
                dataset_name=request.datasets[-1].dataset_name,
                message="Observed row distribution drifted from prior run",
            )
        ]


class _ExplodingSqlQualityBackend:
    backend_type = "test_quality"

    def evaluate(self, *, request):
        raise RuntimeError("quality backend unavailable")


def _write_append_sql_package(base_path: Path) -> Path:
    package_root = base_path / "append_sql_models"
    model_dir = package_root / "level3" / "sales" / "appended_orders"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: appended_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: append
            target:
              table_name: appended_orders
            sources:
              - logical_name: raw_orders
                source_name: orders_source
                entity_name: orders
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (model_dir / "model.sql").write_text(
        dedent(
            """
            select order_id, amount, order_date
            from raw_orders
            where order_date >= '{{ window.start_date }}'
              and order_date <= '{{ window.end_date }}'
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root


def _write_same_stage_dependency_sql_package(base_path: Path) -> Path:
    package_root = base_path / "same_stage_sql_models"
    upstream_dir = package_root / "level3" / "sales" / "base_orders"
    upstream_dir.mkdir(parents=True, exist_ok=True)
    (upstream_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: base_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            target:
              table_name: base_orders
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (upstream_dir / "model.sql").write_text("select 1 as order_id", encoding="utf-8")

    downstream_dir = package_root / "level3" / "sales" / "downstream_orders"
    downstream_dir.mkdir(parents=True, exist_ok=True)
    (downstream_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: downstream_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            depends_on:
              - base_orders
            target:
              table_name: downstream_orders
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (downstream_dir / "model.sql").write_text(
        "select order_id from base_orders",
        encoding="utf-8",
    )
    return package_root


def _write_partition_overwrite_sql_package(base_path: Path) -> Path:
    package_root = base_path / "partition_sql_models"
    model_dir = package_root / "level3" / "sales" / "daily_orders"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: daily_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: partition_overwrite
            target:
              table_name: daily_orders
              partition_columns:
                - order_date
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (model_dir / "model.sql").write_text(
        dedent(
            """
            select 1 as order_id, '2026-01-01' as order_date
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root


def _write_cyclic_sql_package(base_path: Path) -> Path:
    package_root = base_path / "cyclic_sql_models"
    first_dir = package_root / "level3" / "sales" / "first_model"
    first_dir.mkdir(parents=True, exist_ok=True)
    (first_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: first_model
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            depends_on:
              - second_model
            target:
              table_name: first_model
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (first_dir / "model.sql").write_text("select 1 as value", encoding="utf-8")

    second_dir = package_root / "level3" / "sales" / "second_model"
    second_dir.mkdir(parents=True, exist_ok=True)
    (second_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: second_model
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            depends_on:
              - first_model
            target:
              table_name: second_model
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (second_dir / "model.sql").write_text("select 2 as value", encoding="utf-8")
    return package_root


def _write_missing_dependency_sql_package(base_path: Path) -> Path:
    package_root = base_path / "missing_dependency_sql_models"
    model_dir = package_root / "level3" / "sales" / "orphan_model"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: orphan_model
            stage: level3
            domain: sales
            materialization: table
            load_mode: full_refresh
            depends_on:
              - missing_model
            target:
              table_name: orphan_model
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (model_dir / "model.sql").write_text("select 1 as value", encoding="utf-8")
    return package_root


def _write_late_arrival_level3_sql_package(base_path: Path) -> Path:
    package_root = base_path / "late_arrival_sql_models"
    model_dir = package_root / "level3" / "sales" / "canonical_orders"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "manifest.yaml").write_text(
        dedent(
            """
            name: canonical_orders
            stage: level3
            domain: sales
            materialization: table
            load_mode: partition_overwrite
            target:
              table_name: canonical_orders
            sources:
              - logical_name: raw_orders
                source_name: orders_source
                entity_name: orders
            owner:
              name: platform
            """
        ).strip(),
        encoding="utf-8",
    )
    (model_dir / "model.sql").write_text(
        dedent(
            """
            select
              source_name,
              order_id,
              amount,
              business_date
            from raw_orders
            where ingest_date = '2026-08-10'
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root

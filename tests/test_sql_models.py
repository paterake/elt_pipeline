from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from textwrap import dedent

import pytest

from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.shared.runtime import StageName, new_run_context
from elt_pipeline.sql import (
    LocalSqlModelExecutor,
    SqlRuntimeErrorCode,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    resolve_selected_model_ids,
    run_sql_models_locally,
    topologically_sort_sql_models,
)


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


def test_local_sql_model_executor_runs_models_in_sqlite(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (2, 20, "2026-01-03"),
                (3, 30, "2026-01-07"),
            ],
        )
        connection.commit()

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

    result = LocalSqlModelExecutor(database_path=database_path).execute(compiled)

    assert result.model_count == 2
    with sqlite3.connect(database_path) as connection:
        base_orders_count = connection.execute("select count(*) from base_orders").fetchone()[0]
        summary_rows = connection.execute(
            "select order_date, total_amount from order_summary order by order_date"
        ).fetchall()

    assert base_orders_count == 3
    assert summary_rows == [("2026-01-01", 10), ("2026-01-03", 20), ("2026-01-07", 30)]


def test_local_sql_model_executor_plans_models_with_query_plan(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (2, 20, "2026-01-03"),
            ],
        )
        connection.commit()

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

    planning_result = LocalSqlModelExecutor(database_path=database_path).plan(
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


def test_local_sql_model_executor_appends_rows_across_runs(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (2, 20, "2026-01-03"),
                (3, 30, "2026-01-07"),
            ],
        )
        connection.commit()

    model = discover_sql_models(_write_append_sql_package(tmp_path))[0]
    executor = LocalSqlModelExecutor(database_path=database_path)

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
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "select order_id, amount, order_date from appended_orders order by order_id"
        ).fetchall()
    assert rows == [
        (1, 10, "2026-01-01"),
        (2, 20, "2026-01-03"),
        (3, 30, "2026-01-07"),
    ]


def test_local_sql_model_executor_partition_overwrite_replaces_selected_partition(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table daily_orders (
                order_id integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into daily_orders (order_id, order_date) values (?, ?)",
            [
                (999, "2026-01-01"),
                (200, "2026-01-02"),
            ],
        )
        connection.commit()

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

    result = LocalSqlModelExecutor(
        database_path=database_path,
        partition_values={"order_date": "2026-01-01"},
    ).execute([compiled])

    assert result.executed_models[0].load_mode.value == "partition_overwrite"
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "select order_id, order_date from daily_orders order by order_date, order_id"
        ).fetchall()
    assert rows == [
        (1, "2026-01-01"),
        (200, "2026-01-02"),
    ]


def test_run_sql_models_locally_writes_audit_log_and_lineage_artifacts(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (2, 20, "2026-01-03"),
            ],
        )
        connection.commit()

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
        database_path=database_path,
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


def test_run_sql_models_locally_captures_validation_results_in_audit(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (2, 20, "2026-01-03"),
            ],
        )
        connection.commit()

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
        database_path=database_path,
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
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "warehouse.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                amount integer,
                order_date text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, amount, order_date) values (?, ?, ?)",
            [
                (1, 10, "2026-01-01"),
                (1, 20, "2026-01-03"),
                (None, 30, "2026-01-04"),
            ],
        )
        connection.commit()

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
            database_path=database_path,
            compiled_models=compiled,
        )

    audit_path = (
        tmp_path
        / "runs"
        / "stage=sql"
        / "environment=dev"
        / "job=sql-run"
        / f"run_id={run_context.run_id}"
        / "audit.json"
    )
    error_path = audit_path.with_name("errors.jsonl")

    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
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


def test_local_sql_model_executor_returns_structured_planning_error_code(
    tmp_path: Path,
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
        LocalSqlModelExecutor(database_path=tmp_path / "warehouse.db").plan([compiled])

    assert exc_info.value.error_code == SqlRuntimeErrorCode.planning_failed
    assert exc_info.value.error_category.value == "processing_error"


def test_local_sql_model_executor_returns_structured_partition_error_code(
    tmp_path: Path,
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
        LocalSqlModelExecutor(database_path=tmp_path / "warehouse.db").execute([compiled])

    assert exc_info.value.error_code == SqlRuntimeErrorCode.partition_value_missing
    assert exc_info.value.error_category.value == "config_error"


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

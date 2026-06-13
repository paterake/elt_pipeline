from __future__ import annotations

import sqlite3
from pathlib import Path
from textwrap import dedent

import pytest

from elt_pipeline.shared.errors import ConfigValidationError, PipelineError
from elt_pipeline.sql import (
    LocalSqlModelExecutor,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    resolve_selected_model_ids,
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


def _write_basic_sql_package(base_path: Path) -> Path:
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
            """
        ).strip(),
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
            depends_on:
              - level3.sales.base_orders
            target:
              table_name: order_summary
            owner:
              name: platform
            """
        ).strip(),
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

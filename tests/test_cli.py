import json
import subprocess
import sys
from pathlib import Path
from textwrap import dedent


def write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        dedent(
            """
        schema_version: v1
        environments:
          default:
            defaults: {}
        sources:
          - name: rest_source
            connector_type: rest
            entities:
              - name: orders
            """
        ).strip(),
        encoding="utf-8",
    )
    return config_path


def test_validate_config_command(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "validate-config",
            str(config_path),
            "--source",
            "rest_source",
            "--entity",
            "orders",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["connector_type"] == "rest"
    assert payload["entity_name"] == "orders"


def test_show_run_context_command() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "show-run-context",
            "--stage",
            "ingest",
            "--job-name",
            "demo-job",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage"] == "ingest"
    assert payload["job_name"] == "demo-job"


def test_ingest_run_command(tmp_path: Path) -> None:
    config_path, output_root = write_object_storage_cli_fixture(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "ingest",
            "run",
            str(config_path),
            "--source",
            "local_files",
            "--entity",
            "orders",
            "--root-path",
            str(output_root),
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["command"] == "ingest.run"
    assert payload["result_count"] == 1
    assert payload["results"][0]["connector_type"] == "object_storage"
    manifest = payload["results"][0]["result"]["manifests"][0]
    assert Path(output_root / manifest["data_path"]).exists()
    assert Path(output_root / manifest["manifest_path"]).exists()
    assert payload["results"][0]["result"]["objects_copied"] == 1


def test_normalize_run_command(tmp_path: Path) -> None:
    config_path, output_root = write_object_storage_cli_fixture(tmp_path)
    _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--source",
            "local_files",
            "--entity",
            "orders",
            "--root-path",
            str(output_root),
        ]
    )

    result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--source",
            "local_files",
            "--entity",
            "orders",
            "--root-path",
            str(output_root),
        ]
    )
    payload = json.loads(result.stdout)
    assert payload["command"] == "normalize.run"
    assert payload["processed_count"] == 1
    assert Path(output_root / payload["results"][0]["mapping_catalog_path"]).exists()
    table_manifests = payload["results"][0]["table_manifests"]
    assert len(table_manifests) == 2
    assert all(Path(output_root / table["data_path"]).exists() for table in table_manifests)


def test_sql_compile_command(tmp_path: Path) -> None:
    package_root = write_sql_package(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "sql",
            "compile",
            str(package_root),
            "--stage",
            "level3",
            "--domain",
            "sales",
            "--model",
            "base_orders",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["model_count"] == 1
    assert payload["models"][0]["model_id"] == "level3.sales.base_orders"
    assert "2026-01-31" in payload["models"][0]["compiled_sql"]


def test_sql_run_command(tmp_path: Path) -> None:
    package_root = write_sql_package(tmp_path)
    database_path = tmp_path / "warehouse.db"
    _seed_sqlite_database(database_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "sql",
            "run",
            str(package_root),
            "--model",
            "order_summary",
            "--include-deps",
            "--database",
            str(database_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["model_count"] == 2
    assert payload["executed_models"][0]["model_id"] == "level3.sales.base_orders"
    assert payload["executed_models"][1]["model_id"] == "level4.sales.order_summary"
    assert Path(payload["artifacts"]["audit_path"]).exists()
    assert Path(payload["artifacts"]["log_path"]).exists()
    assert Path(payload["artifacts"]["lineage_path"]).exists()


def test_sql_run_validate_only_command(tmp_path: Path) -> None:
    package_root = write_sql_package(tmp_path)
    database_path = tmp_path / "warehouse.db"
    _seed_sqlite_database(database_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "sql",
            "run",
            str(package_root),
            "--model",
            "order_summary",
            "--include-deps",
            "--database",
            str(database_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--validate-only",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["mode"] == "validate_only"
    assert payload["model_count"] == 2
    assert payload["execution_order"] == [
        "level3.sales.base_orders",
        "level4.sales.order_summary",
    ]
    assert all(model["validation_passed"] for model in payload["models"])

    import sqlite3

    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "select name from sqlite_master where type = 'table' and name = 'order_summary'"
        ).fetchone()
    assert row is None


def test_sql_run_explain_command(tmp_path: Path) -> None:
    package_root = write_sql_package(tmp_path)
    database_path = tmp_path / "warehouse.db"
    _seed_sqlite_database(database_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "sql",
            "run",
            str(package_root),
            "--model",
            "order_summary",
            "--include-deps",
            "--database",
            str(database_path),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--explain",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["mode"] == "explain"
    assert payload["model_count"] == 2
    assert payload["models"][0]["query_plan"]
    assert payload["models"][1]["query_plan"]


def write_sql_package(tmp_path: Path) -> Path:
    package_root = tmp_path / "sql_models"
    base_orders_dir = package_root / "level3" / "sales" / "base_orders"
    base_orders_dir.mkdir(parents=True)
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
    order_summary_dir.mkdir(parents=True)
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


def _seed_sqlite_database(database_path: Path) -> None:
    import sqlite3

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


def write_object_storage_cli_fixture(tmp_path: Path) -> tuple[Path, Path]:
    bucket_path = tmp_path / "bucket"
    bucket_path.mkdir()
    (bucket_path / "orders.json").write_text(
        json.dumps(
            [
                {
                    "order_id": "A-100",
                    "customer": {"name": "Alice"},
                    "items": [{"sku": "SKU-1", "quantity": 2}],
                }
            ]
        ),
        encoding="utf-8",
    )

    output_root = tmp_path / "runtime"
    config_path = tmp_path / "object-storage-pipeline.yaml"
    config_path.write_text(
        dedent(
            f"""
            schema_version: v1
            environments:
              default:
                defaults: {{}}
            sources:
              - name: local_files
                connector_type: object_storage
                entities:
                  - name: orders
                    extraction:
                      bucket_path: {bucket_path.as_posix()}
                      payload_format: json
                      sync_mode: full
            """
        ).strip(),
        encoding="utf-8",
    )
    return config_path, output_root


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )

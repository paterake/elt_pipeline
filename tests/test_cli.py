import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from textwrap import dedent

from elt_pipeline.ingest import LocalCheckpointStore, LocalLevel1Writer
from elt_pipeline.shared.runtime import RunContext, StageName


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


def test_normalize_run_command_filters_by_window(tmp_path: Path) -> None:
    config_path, output_root, selected_manifest = write_normalize_window_fixture(tmp_path)

    result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--source",
            "windowed_source",
            "--entity",
            "orders",
            "--root-path",
            str(output_root),
            "--window-start",
            "2026-01-03T00:00:00+00:00",
            "--window-end",
            "2026-01-03T23:59:59+00:00",
            "--backfill",
        ]
    )

    payload = json.loads(result.stdout)
    assert payload["selection"]["backfill"] is True
    assert payload["processed_count"] == 1
    assert payload["results"][0]["trigger_type"] == "backfill"
    assert payload["results"][0]["input_artifact_id"] == selected_manifest.artifact_id


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
    assert payload["selection"]["stage"] == "level3"
    assert payload["selection"]["domain"] == "sales"
    assert payload["selection"]["model"] == "base_orders"
    assert payload["selection"]["include_dependencies"] is False
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


def test_ingest_run_command_supports_backfill_checkpoint_seed(tmp_path: Path) -> None:
    config_path, output_root = write_sql_ingest_cli_fixture(tmp_path)
    checkpoint_store = LocalCheckpointStore(output_root)
    checkpoint_store.commit(
        environment="default",
        source_name="orders_db",
        entity_name="orders",
        run_id="run-1",
        checkpoint_before=None,
        checkpoint_after={"max_updated_at": "2026-01-02T00:00:00+00:00"},
        recorded_at=datetime(2026, 1, 2, tzinfo=UTC),
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
    checkpoint_store.commit(
        environment="default",
        source_name="orders_db",
        entity_name="orders",
        run_id="run-2",
        checkpoint_before={"max_updated_at": "2026-01-02T00:00:00+00:00"},
        checkpoint_after={"max_updated_at": "2026-01-03T00:00:00+00:00"},
        recorded_at=datetime(2026, 1, 3, tzinfo=UTC),
        window_start=datetime(2026, 1, 2, tzinfo=UTC),
        window_end=datetime(2026, 1, 3, tzinfo=UTC),
    )

    result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--source",
            "orders_db",
            "--entity",
            "orders",
            "--root-path",
            str(output_root),
            "--window-start",
            "2026-01-03T00:00:00+00:00",
            "--window-end",
            "2026-01-04T00:00:00+00:00",
            "--backfill",
        ]
    )

    payload = json.loads(result.stdout)
    assert payload["selection"]["backfill"] is True
    assert payload["result_count"] == 1
    assert payload["results"][0]["trigger_type"] == "backfill"
    assert payload["results"][0]["result"]["checkpoint_before"] == {
        "max_updated_at": "2026-01-03T00:00:00+00:00"
    }


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


def write_sql_ingest_cli_fixture(tmp_path: Path) -> tuple[Path, Path]:
    database_path = tmp_path / "source.db"
    output_root = tmp_path / "runtime"
    config_path = tmp_path / "sql-ingest-pipeline.yaml"

    import sqlite3

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table raw_orders (
                order_id integer,
                updated_at text
            )
            """
        )
        connection.executemany(
            "insert into raw_orders (order_id, updated_at) values (?, ?)",
            [
                (1, "2026-01-03T00:00:00+00:00"),
                (2, "2026-01-05T00:00:00+00:00"),
            ],
        )
        connection.commit()

    config_path.write_text(
        dedent(
            f"""
            schema_version: v1
            environments:
              default:
                defaults: {{}}
            sources:
              - name: orders_db
                connector_type: sql
                entities:
                  - name: orders
                    extraction:
                      mode: delta
                      database: {database_path.as_posix()}
                      query:
                        sql: select order_id, updated_at from raw_orders where updated_at >= :watermark
                        parameters:
                          watermark: "{{watermark.value}}"
                      watermark:
                        column_name: updated_at
                        checkpoint_key: max_updated_at
                        parameter_name: watermark
                        default_value: "2026-01-01T00:00:00+00:00"
            """
        ).strip(),
        encoding="utf-8",
    )
    return config_path, output_root


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


def write_normalize_window_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, object]:
    output_root = tmp_path / "runtime"
    writer = LocalLevel1Writer(output_root)
    config_path = tmp_path / "normalize-window-pipeline.yaml"
    config_path.write_text(
        dedent(
            """
            schema_version: v1
            environments:
              default:
                defaults: {}
            sources:
              - name: windowed_source
                connector_type: rest
                entities:
                  - name: orders
            """
        ).strip(),
        encoding="utf-8",
    )

    first_run = RunContext(
        run_id="run-window-1",
        stage=StageName.ingest,
        job_name="ingest-run",
        trigger_type="scheduled_batch",
        started_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
    )
    writer.write_payload(
        run_context=first_run,
        environment="default",
        source_name="windowed_source",
        entity_name="orders",
        payload='[{"order_id":"A-100","customer":{"name":"Alice"}}]',
        payload_format="json",
        extraction_mode="scheduled_batch",
        artifact_name="orders-day-1",
        window_start=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 1, 1, 23, 59, tzinfo=UTC),
    )

    second_run = RunContext(
        run_id="run-window-2",
        stage=StageName.ingest,
        job_name="ingest-run",
        trigger_type="scheduled_batch",
        started_at=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
    )
    selected_manifest = writer.write_payload(
        run_context=second_run,
        environment="default",
        source_name="windowed_source",
        entity_name="orders",
        payload='[{"order_id":"A-200","customer":{"name":"Bob"}}]',
        payload_format="json",
        extraction_mode="scheduled_batch",
        artifact_name="orders-day-3",
        window_start=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 1, 3, 23, 59, tzinfo=UTC),
    )

    return config_path, output_root, selected_manifest


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )

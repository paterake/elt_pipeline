from __future__ import annotations

import http.server
import json
import sqlite3
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Iterator

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIGS = {
    "examples/configs/local_object_storage_orders.yaml": ("local_files", "orders"),
    "examples/configs/local_object_storage_orders_csv_bypass.yaml": (
        "local_files",
        "orders",
    ),
    "examples/configs/local_sqlite_orders_delta.yaml": ("orders_db", "orders"),
    "examples/configs/local_kafka_orders_replay.yaml": ("events", "orders"),
    "examples/configs/local_rest_orders.yaml": ("local_api", "orders"),
}


def test_example_configs_load_and_resolve() -> None:
    for relative_path, (source_name, entity_name) in EXAMPLE_CONFIGS.items():
        config = load_pipeline_config(REPO_ROOT / relative_path)
        resolved = resolve_entity_config(
            config,
            environment="default",
            source_name=source_name,
            entity_name=entity_name,
        )
        assert resolved.environment == "default"
        assert resolved.source_name == source_name
        assert resolved.entity_name == entity_name


def test_object_storage_example_runs_ingest_and_normalize(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_object_storage_orders.yaml"

    ingest_result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    ingest_payload = json.loads(ingest_result.stdout)
    assert ingest_payload["result_count"] == 1
    assert ingest_payload["results"][0]["connector_type"] == "object_storage"

    normalize_result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    normalize_payload = json.loads(normalize_result.stdout)
    assert normalize_payload["processed_count"] == 1
    assert normalize_payload["results"][0]["bypassed"] is False
    assert normalize_payload["results"][0]["table_manifests"]


def test_object_storage_csv_bypass_example_runs(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_object_storage_orders_csv_bypass.yaml"

    _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    normalize_result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    payload = json.loads(normalize_result.stdout)
    assert payload["results"][0]["bypassed"] is True
    assert payload["results"][0]["level2_mode"] == "bypass_level2"


def test_kafka_example_runs_ingest(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_kafka_orders_replay.yaml"

    result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    payload = json.loads(result.stdout)
    assert payload["result_count"] == 1
    assert payload["results"][0]["connector_type"] == "kafka"
    assert payload["results"][0]["result"]["message_count"] == 2


def test_sqlite_example_runs_ingest(tmp_path: Path) -> None:
    database_path = tmp_path / "source.db"
    _seed_example_source_database(database_path)
    config_path = _write_modified_example_config(
        tmp_path,
        "examples/configs/local_sqlite_orders_delta.yaml",
        {"examples/data/sql/source.db": database_path.as_posix()},
    )

    result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    payload = json.loads(result.stdout)
    assert payload["result_count"] == 1
    assert payload["results"][0]["connector_type"] == "sql"
    assert payload["results"][0]["result"]["query_count"] == 1
    assert payload["results"][0]["result"]["row_count"] == 3


def test_rest_example_runs_ingest(tmp_path: Path) -> None:
    with _serve_rest_example_api() as base_url:
        config_path = _write_modified_example_config(
            tmp_path,
            "examples/configs/local_rest_orders.yaml",
            {"http://127.0.0.1:8000": base_url},
        )
        result = _run_cli(
            [
                "ingest",
                "run",
                str(config_path),
                "--root-path",
                str(tmp_path),
            ]
        )

    payload = json.loads(result.stdout)
    assert payload["result_count"] == 1
    assert payload["results"][0]["connector_type"] == "rest"
    assert payload["results"][0]["result"]["request_count"] == 1
    assert payload["results"][0]["result"]["response_count"] == 1


def test_sql_example_package_compile_and_run(tmp_path: Path, spark_session) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_example_level2_orders(tmp_path)

    compile_result = _run_cli(
        [
            "sql",
            "compile",
            str(REPO_ROOT / "examples/sql/local_demo"),
            "--environment",
            "default",
            "--include-deps",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
        ]
    )
    compile_payload = json.loads(compile_result.stdout)
    assert compile_payload["model_count"] == 2
    assert [model["model_id"] for model in compile_payload["models"]] == [
        "level3.sales.base_orders",
        "level4.sales.order_summary",
    ]

    run_result = _run_cli(
        [
            "sql",
            "run",
            str(REPO_ROOT / "examples/sql/local_demo"),
            "--root-path",
            str(tmp_path),
            "--warehouse-root",
            str(warehouse_root),
            "--environment",
            "default",
            "--include-deps",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
        ]
    )
    run_payload = json.loads(run_result.stdout)
    assert run_payload["model_count"] == 2
    assert Path(run_payload["artifacts"]["audit_path"]).exists()

    rows = sorted(
        (
            row.asDict()
            for row in spark_session.read.parquet(
                str(warehouse_root / "level4" / "order_summary")
            ).collect()
        ),
        key=lambda row: row["order_date"],
    )
    assert [(row["order_date"], row["total_amount"]) for row in rows] == [
        ("2026-01-01", 10),
        ("2026-01-02", 25),
    ]


def test_publish_example_package_validate_explain_and_run(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    runtime_root = tmp_path / "runtime"
    _seed_example_level2_orders(tmp_path)

    _run_cli(
        [
            "sql",
            "run",
            str(REPO_ROOT / "examples/sql/local_demo"),
            "--root-path",
            str(tmp_path),
            "--warehouse-root",
            str(warehouse_root),
            "--environment",
            "default",
            "--include-deps",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
        ]
    )

    validate_result = _run_cli(
        [
            "publish",
            "validate",
            str(REPO_ROOT / "examples/publish/local_demo"),
        ]
    )
    validate_payload = json.loads(validate_result.stdout)
    assert validate_payload["publish_count"] == 4
    assert {
        definition["publish_id"]: definition["output_format"]
        for definition in validate_payload["definitions"]
    } == {
        "sales.daily_order_export_bundle": "csv",
        "sales.daily_order_export": "csv",
        "sales.daily_order_export_tsv": "tsv",
        "sales.daily_order_export_windowed": "jsonl",
    }

    explain_result = _run_cli(
        [
            "publish",
            "explain",
            str(REPO_ROOT / "examples/publish/local_demo"),
            "--root-path",
            str(runtime_root),
            "--window-label",
            "2026-01",
        ]
    )
    explain_payload = json.loads(explain_result.stdout)
    assert explain_payload["publish_count"] == 4
    assert all("run_id=" in plan["run_scoped_path"] for plan in explain_payload["plans"])
    bundle_plan = next(
        plan
        for plan in explain_payload["plans"]
        if plan["publish_id"] == "sales.daily_order_export_bundle"
    )
    assert bundle_plan["archive_run_scoped_path"].endswith(".zip")

    run_result = _run_cli(
        [
            "publish",
            "run",
            str(REPO_ROOT / "examples/publish/local_demo"),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export",
            "--window-label",
            "2026-01",
        ]
    )
    run_payload = json.loads(run_result.stdout)
    assert run_payload["publish_count"] == 1
    assert run_payload["results"][0]["publish_id"] == "sales.daily_order_export"
    assert Path(run_payload["artifacts"]["audit_path"]).exists()
    assert Path(run_payload["artifacts"]["export_manifest_path"]).exists()
    assert Path(run_payload["results"][0]["artifacts"][0]["run_scoped_path"]).exists()

    jsonl_run_result = _run_cli(
        [
            "publish",
            "run",
            str(REPO_ROOT / "examples/publish/local_demo"),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export_windowed",
            "--window-label",
            "2026-01",
        ]
    )
    jsonl_run_payload = json.loads(jsonl_run_result.stdout)
    assert jsonl_run_payload["publish_count"] == 1
    assert jsonl_run_payload["results"][0]["artifacts"][0]["output_format"] == "jsonl"
    assert jsonl_run_payload["results"][0]["artifacts"][0]["stable_delivery_path"] is None

    tsv_run_result = _run_cli(
        [
            "publish",
            "run",
            str(REPO_ROOT / "examples/publish/local_demo"),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export_tsv",
            "--window-label",
            "2026-01",
        ]
    )
    tsv_run_payload = json.loads(tsv_run_result.stdout)
    assert tsv_run_payload["publish_count"] == 1
    tsv_artifact = tsv_run_payload["results"][0]["artifacts"][0]
    assert tsv_artifact["output_format"] == "tsv"
    assert "run_id=" in Path(tsv_artifact["stable_delivery_path"]).name

    bundle_run_result = _run_cli(
        [
            "publish",
            "run",
            str(REPO_ROOT / "examples/publish/local_demo"),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export_bundle",
            "--window-label",
            "2026-01",
        ]
    )
    bundle_run_payload = json.loads(bundle_run_result.stdout)
    assert bundle_run_payload["publish_count"] == 1
    bundle_results = bundle_run_payload["results"][0]["artifacts"]
    bundle_artifacts = {artifact["output_format"]: artifact for artifact in bundle_results}
    assert set(bundle_artifacts) == {"csv", "zip"}
    assert Path(bundle_artifacts["csv"]["stable_delivery_path"]).exists()
    assert Path(bundle_artifacts["zip"]["stable_delivery_path"]).exists()


def test_schedule_example_runs_after_placeholder_resolution(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"

    schedule_path = _write_modified_example_schedule(
        tmp_path,
        replacements={
            "/absolute/path/to/pipeline.yaml": (
                REPO_ROOT / "examples/configs/local_object_storage_orders.yaml"
            ).as_posix(),
            "/absolute/path/to/runtime": runtime_root.as_posix(),
            "my_source": "local_files",
            "my_entity": "orders",
            "/absolute/path/to/sql-package": (
                REPO_ROOT / "examples/sql/local_demo"
            ).as_posix(),
            "/absolute/path/to/warehouse": warehouse_root.as_posix(),
        },
    )

    result = _run_cli(["schedule", "run", str(schedule_path)])
    payload = json.loads(result.stdout)

    assert payload["success"] is True
    assert payload["executed_count"] == 5
    assert [job["status"] for job in payload["jobs"]] == [
        "success",
        "success",
        "success",
        "success",
        "success",
    ]


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def _seed_example_source_database(database_path: Path) -> None:
    script = (REPO_ROOT / "examples/data/sql/source_init.sql").read_text(encoding="utf-8")
    with sqlite3.connect(database_path) as connection:
        connection.executescript(script)
        connection.commit()


def _seed_example_level2_orders(root_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_object_storage_orders.yaml"
    _run_cli(["ingest", "run", str(config_path), "--root-path", str(root_path)])
    _run_cli(["normalize", "run", str(config_path), "--root-path", str(root_path)])


def _write_modified_example_config(
    tmp_path: Path,
    relative_path: str,
    replacements: dict[str, str],
) -> Path:
    template = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
    resolved = template
    for source, target in replacements.items():
        resolved = resolved.replace(source, target)
    output_path = tmp_path / Path(relative_path).name
    output_path.write_text(resolved, encoding="utf-8")
    return output_path


def _write_modified_example_schedule(tmp_path: Path, replacements: dict[str, str]) -> Path:
    template = (REPO_ROOT / "examples/schedules/local_demo.yaml").read_text(encoding="utf-8")
    resolved = dedent(template)
    for source, target in replacements.items():
        resolved = resolved.replace(source, target)
    output_path = tmp_path / "local_demo_schedule.yaml"
    output_path.write_text(resolved, encoding="utf-8")
    return output_path


@contextmanager
def _serve_rest_example_api() -> Iterator[str]:
    directory = REPO_ROOT / "examples/data/rest_api"

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        lambda *args, **kwargs: QuietHandler(*args, directory=str(directory), **kwargs),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

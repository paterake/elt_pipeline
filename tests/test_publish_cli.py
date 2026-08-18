from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def test_publish_validate_command(tmp_path: Path) -> None:
    package_root = _write_publish_package(tmp_path)

    result = _run_cli(["publish", "validate", str(package_root)])
    payload = json.loads(result.stdout)

    assert payload["command"] == "publish.validate"
    assert payload["publish_count"] == 1
    assert payload["definitions"][0]["publish_id"] == "sales.daily_order_export"
    assert payload["definitions"][0]["output_format"] == "csv"


def test_publish_explain_command(tmp_path: Path) -> None:
    package_root = _write_publish_package(tmp_path)
    runtime_root = tmp_path / "runtime"

    result = _run_cli(
        [
            "publish",
            "explain",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--window-label",
            "2026-01",
        ]
    )
    payload = json.loads(result.stdout)

    assert payload["command"] == "publish.explain"
    assert payload["publish_count"] == 1
    assert payload["plans"][0]["publish_id"] == "sales.daily_order_export"
    assert "run_id=" in payload["plans"][0]["run_scoped_path"]


def test_publish_run_command(tmp_path: Path, spark_session) -> None:
    package_root = _write_publish_package(tmp_path, replacement_mode="overwrite_in_place")
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
        ]
    )
    payload = json.loads(result.stdout)

    assert payload["command"] == "publish.run"
    assert payload["publish_count"] == 1
    assert payload["results"][0]["publish_id"] == "sales.daily_order_export"
    assert payload["results"][0]["row_count"] == 2
    assert Path(payload["artifacts"]["audit_path"]).exists()
    assert Path(payload["artifacts"]["log_path"]).exists()
    assert Path(payload["artifacts"]["lineage_path"]).exists()
    assert Path(payload["artifacts"]["export_manifest_path"]).exists()

    artifact = payload["results"][0]["artifacts"][0]
    assert Path(artifact["run_scoped_path"]).exists()
    assert Path(artifact["stable_delivery_path"]).exists()


def test_publish_run_command_writes_jsonl(tmp_path: Path, spark_session) -> None:
    package_root = _write_publish_package(tmp_path, output_format="jsonl")
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
        ]
    )
    payload = json.loads(result.stdout)

    artifact = payload["results"][0]["artifacts"][0]
    lines = Path(artifact["run_scoped_path"]).read_text(encoding="utf-8").strip().splitlines()

    assert artifact["output_format"] == "jsonl"
    assert artifact["stable_delivery_path"] is None
    assert len(lines) == 2
    # The publish definition declares no order_by, so JSONL row order is not a contract
    # (Spark read order is unspecified). Assert on row content, order-independently.
    assert sorted((json.loads(line) for line in lines), key=lambda row: row["order_date"]) == [
        {"order_date": "2026-01-01", "total_amount": 10},
        {"order_date": "2026-01-02", "total_amount": 35},
    ]


def test_publish_run_command_appends_new_delivery_artifact(tmp_path: Path, spark_session) -> None:
    package_root = _write_publish_package(tmp_path, replacement_mode="append_new_artifact")
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
        ]
    )
    payload = json.loads(result.stdout)

    artifact = payload["results"][0]["artifacts"][0]
    assert Path(artifact["run_scoped_path"]).exists()
    assert Path(artifact["stable_delivery_path"]).exists()
    assert "run_id=" in Path(artifact["stable_delivery_path"]).name


def test_publish_run_command_reuses_prior_audit_selection(tmp_path: Path, spark_session) -> None:
    package_root = _write_publish_package(tmp_path, replacement_mode="overwrite_in_place")
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    first_result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export",
            "--window-start",
            "2026-01-01T00:00:00+00:00",
            "--window-end",
            "2026-01-31T23:59:59+00:00",
            "--window-label",
            "jan-2026",
        ]
    )
    first_payload = json.loads(first_result.stdout)

    second_result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--rerun-run-id",
            first_payload["run_id"],
        ]
    )
    second_payload = json.loads(second_result.stdout)

    assert second_payload["selection"]["publish_name"] is None
    assert second_payload["selection"]["window_label"] == "jan-2026"
    assert second_payload["selection"]["rerun_run_id"] == first_payload["run_id"]
    assert second_payload["publish_count"] == 1

    rerun_audit = json.loads(
        Path(second_payload["artifacts"]["audit_path"]).read_text(encoding="utf-8")
    )
    assert rerun_audit["context"]["rerun_of_run_id"] == first_payload["run_id"]
    assert rerun_audit["context"]["selected_publish_ids"] == "sales.daily_order_export"

    export_manifest_path = Path(second_payload["artifacts"]["export_manifest_path"])
    export_manifest = json.loads(export_manifest_path.read_text(encoding="utf-8"))
    assert export_manifest["rerun_of_run_id"] == first_payload["run_id"]


def test_publish_run_command_supports_backfill_trigger(tmp_path: Path, spark_session) -> None:
    package_root = _write_publish_package(tmp_path)
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--window-start",
            "2026-01-01T00:00:00+00:00",
            "--window-end",
            "2026-01-31T23:59:59+00:00",
            "--window-label",
            "jan-2026",
            "--backfill",
        ]
    )
    payload = json.loads(result.stdout)
    audit = json.loads(Path(payload["artifacts"]["audit_path"]).read_text(encoding="utf-8"))

    assert payload["selection"]["backfill"] is True
    assert audit["trigger_type"] == "backfill"
    assert audit["context"]["checkpoint_mode"] == "backfill"


def test_publish_run_command_rejects_conflicting_rerun_selection(
    tmp_path: Path, spark_session
) -> None:
    package_root = _write_publish_package(tmp_path)
    runtime_root = tmp_path / "runtime"
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--warehouse-root",
            str(warehouse_root),
            "--publish",
            "daily_order_export",
            "--rerun-run-id",
            "prior-run-id",
        ],
        check=False,
    )

    assert result.returncode == 2
    error_payload = json.loads(result.stderr)
    assert "publish reruns must not specify an explicit selection" in error_payload["message"]


def _run_cli(
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    # Publish here reads a plain-parquet L4 table seeded at warehouse/level4/<table>.
    # The CLI subprocess runs in its own process (no access to the in-process
    # ELT_PIPELINE_TEST_SPARK_ICEBERG conftest knob), so select the parquet-parity path
    # via the CLI's env override; otherwise it uses the Iceberg-on default and looks in a
    # catalog instead of the seeded parquet.
    env = {**os.environ, "ELT_PIPELINE_ICEBERG_ENABLED": "false"}
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=Path(__file__).resolve().parents[1],
        check=check,
        capture_output=True,
        text=True,
        env=env,
    )


def _write_publish_package(
    base_path: Path,
    *,
    replacement_mode: str = "versioned_delivery",
    output_format: str = "csv",
) -> Path:
    package_root = base_path / "publish_defs"
    publish_dir = package_root / "sales" / "daily_order_export"
    publish_dir.mkdir(parents=True, exist_ok=True)
    manifest_lines = [
        "name: daily_order_export",
        "stage: level5",
        "domain: sales",
        "version: v1",
        "source:",
        "  stage: level4",
        "  dataset: order_summary",
        "  selection_mode: direct",
        "delivery:",
        "  target_type: local_filesystem",
        f"  output_format: {output_format}",
        "  path_template: exports/{domain}/{publish_name}/daily_order_export."
        "{output_extension}",
        f"  replacement_mode: {replacement_mode}",
        "owner:",
        "  owning_domain: sales",
        "  owner_team: analytics_platform",
        "consumer_label: finance_consumer",
        "delivery_purpose: daily_summary",
        "columns:",
        "  - order_date",
        "  - total_amount",
        "validation:",
        "  required_columns:",
        "    - order_date",
        "    - total_amount",
    ]
    (publish_dir / "manifest.yaml").write_text("\n".join(manifest_lines), encoding="utf-8")
    return package_root


def _seed_order_summary_table(spark_session, warehouse_root: Path) -> None:
    schema = StructType(
        [
            StructField("order_date", StringType(), nullable=False),
            StructField("total_amount", IntegerType(), nullable=False),
        ]
    )
    dataset_path = warehouse_root / "level4" / "order_summary"
    spark_session.createDataFrame(
        [
            {"order_date": "2026-01-01", "total_amount": 10},
            {"order_date": "2026-01-02", "total_amount": 35},
        ],
        schema=schema,
    ).write.mode("error").parquet(str(dataset_path))

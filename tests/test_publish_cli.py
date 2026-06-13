from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from textwrap import dedent


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


def test_publish_run_command(tmp_path: Path) -> None:
    package_root = _write_publish_package(tmp_path, replacement_mode="overwrite_in_place")
    runtime_root = tmp_path / "runtime"
    database_path = tmp_path / "warehouse.db"
    _seed_order_summary_table(database_path)

    result = _run_cli(
        [
            "publish",
            "run",
            str(package_root),
            "--root-path",
            str(runtime_root),
            "--database",
            str(database_path),
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


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )


def _write_publish_package(
    base_path: Path,
    *,
    replacement_mode: str = "versioned_delivery",
) -> Path:
    package_root = base_path / "publish_defs"
    publish_dir = package_root / "sales" / "daily_order_export"
    publish_dir.mkdir(parents=True, exist_ok=True)
    (publish_dir / "manifest.yaml").write_text(
        dedent(
            f"""
            name: daily_order_export
            stage: level5
            domain: sales
            version: v1
            source:
              stage: level4
              dataset: order_summary
              selection_mode: direct
            delivery:
              target_type: local_filesystem
              output_format: csv
              path_template: exports/{{domain}}/{{publish_name}}/daily_order_export.{{output_extension}}
              replacement_mode: {replacement_mode}
            owner:
              owning_domain: sales
              owner_team: analytics_platform
            consumer_label: finance_consumer
            delivery_purpose: daily_summary
            columns:
              - order_date
              - total_amount
            validation:
              required_columns:
                - order_date
                - total_amount
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root


def _seed_order_summary_table(database_path: Path) -> None:
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            create table order_summary (
                order_date text,
                total_amount integer
            )
            """
        )
        connection.executemany(
            "insert into order_summary (order_date, total_amount) values (?, ?)",
            [
                ("2026-01-01", 10),
                ("2026-01-02", 35),
            ],
        )
        connection.commit()

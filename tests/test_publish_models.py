from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from textwrap import dedent

import pytest

from elt_pipeline.publish import discover_publish_definitions, run_publish_definitions_locally
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_discover_publish_definitions_reads_valid_package(tmp_path: Path) -> None:
    package_root = _write_publish_package(tmp_path)

    definitions = discover_publish_definitions(package_root)

    assert [definition.publish_id for definition in definitions] == ["sales.daily_order_export"]
    assert definitions[0].manifest.delivery.output_format.value == "csv"
    assert definitions[0].manifest.source.dataset == "order_summary"


def test_discover_publish_definitions_rejects_missing_query_sql(tmp_path: Path) -> None:
    package_root = _write_publish_package(
        tmp_path,
        selection_mode="query",
        include_query=False,
    )

    with pytest.raises(ConfigValidationError, match="must contain query.sql"):
        discover_publish_definitions(package_root)


def test_run_publish_definitions_locally_writes_csv_manifest_and_artifacts(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    _seed_order_summary_table(database_path)
    package_root = _write_publish_package(
        tmp_path,
        replacement_mode="overwrite_in_place",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        database_path=database_path,
        definitions=definitions,
    )

    assert len(result.results) == 1
    assert result.artifacts.audit_path is not None
    assert result.artifacts.log_path is not None
    assert result.artifacts.lineage_path is not None
    assert result.artifacts.audit_path.exists()
    assert result.artifacts.log_path.exists()
    assert result.artifacts.lineage_path.exists()

    artifact = result.results[0].artifacts[0]
    manifest_path = artifact.run_scoped_path.parent / "manifest.json"
    assert artifact.row_count == 2
    assert artifact.run_scoped_path.exists()
    assert artifact.stable_delivery_path is not None
    assert artifact.stable_delivery_path.exists()
    assert manifest_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["publish_name"] == "daily_order_export"
    assert manifest_payload["source_dataset"] == "order_summary"
    assert manifest_payload["artifacts"][0]["row_count"] == 2


def test_run_publish_definitions_locally_writes_jsonl_manifest_and_artifacts(tmp_path: Path) -> None:
    database_path = tmp_path / "warehouse.db"
    _seed_order_summary_table(database_path)
    package_root = _write_publish_package(
        tmp_path,
        selection_mode="query",
        output_format="jsonl",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=tmp_path,
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        database_path=database_path,
        definitions=definitions,
    )

    artifact = result.results[0].artifacts[0]
    manifest_path = artifact.run_scoped_path.parent / "manifest.json"
    lines = artifact.run_scoped_path.read_text(encoding="utf-8").strip().splitlines()

    assert artifact.row_count == 2
    assert artifact.run_scoped_path.suffix == ".jsonl"
    assert artifact.stable_delivery_path is None
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"order_date": "2026-01-01", "total_amount": 10}
    assert manifest_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_format"] == "jsonl"
    assert manifest_payload["artifacts"][0]["stable_delivery_path"] is None


def _write_publish_package(
    base_path: Path,
    *,
    selection_mode: str = "direct",
    include_query: bool = True,
    replacement_mode: str = "versioned_delivery",
    output_format: str = "csv",
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
              selection_mode: {selection_mode}
            delivery:
              target_type: local_filesystem
              output_format: {output_format}
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
    if selection_mode == "query" and include_query:
        (publish_dir / "query.sql").write_text(
            dedent(
                """
                select order_date, total_amount
                from order_summary
                where total_amount >= 10
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

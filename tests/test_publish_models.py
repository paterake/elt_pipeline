from __future__ import annotations

import json
import zipfile
from pathlib import Path
from textwrap import dedent

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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


def test_discover_publish_definitions_rejects_unknown_path_template_placeholders(
    tmp_path: Path,
) -> None:
    package_root = _write_publish_package(
        tmp_path,
        path_template="exports/{domain}/{missing_field}/daily_order_export.{output_extension}",
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        discover_publish_definitions(package_root)
    assert (
        "path_template may only use"
        in exc_info.value.context["errors"][0]["msg"]
    )


def test_run_publish_definitions_locally_writes_csv_manifest_and_artifacts(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)
    package_root = _write_publish_package(
        tmp_path,
        replacement_mode="overwrite_in_place",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )

    assert len(result.results) == 1
    assert result.artifacts.audit_path is not None
    assert result.artifacts.log_path is not None
    assert result.artifacts.lineage_path is not None
    assert Path(result.artifacts.audit_path).exists()
    assert Path(result.artifacts.log_path).exists()
    assert Path(result.artifacts.lineage_path).exists()

    artifact = result.results[0].artifacts[0]
    run_scoped_path = Path(artifact.run_scoped_path)
    manifest_path = run_scoped_path.parent / "manifest.json"
    assert artifact.row_count == 2
    assert run_scoped_path.exists()
    assert artifact.stable_delivery_path is not None
    assert Path(artifact.stable_delivery_path).exists()
    assert manifest_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["publish_name"] == "daily_order_export"
    assert manifest_payload["source_dataset"] == "order_summary"
    assert manifest_payload["artifacts"][0]["row_count"] == 2


def test_run_publish_definitions_locally_writes_jsonl_manifest_and_artifacts(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)
    package_root = _write_publish_package(
        tmp_path,
        selection_mode="query",
        output_format="jsonl",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )

    artifact = result.results[0].artifacts[0]
    run_scoped_path = Path(artifact.run_scoped_path)
    manifest_path = run_scoped_path.parent / "manifest.json"
    lines = run_scoped_path.read_text(encoding="utf-8").strip().splitlines()

    assert artifact.row_count == 2
    assert run_scoped_path.suffix == ".jsonl"
    assert artifact.stable_delivery_path is None
    assert len(lines) == 2
    # The publish definition declares no order_by, so JSONL row order is not a contract
    # (Spark read order is unspecified). Assert on row content, order-independently.
    assert sorted((json.loads(line) for line in lines), key=lambda row: row["order_date"]) == [
        {"order_date": "2026-01-01", "total_amount": 10},
        {"order_date": "2026-01-02", "total_amount": 35},
    ]
    assert manifest_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["output_format"] == "jsonl"
    assert manifest_payload["artifacts"][0]["stable_delivery_path"] is None


def test_run_publish_definitions_locally_writes_tsv_manifest_and_artifacts(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)
    package_root = _write_publish_package(
        tmp_path,
        output_format="tsv",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )

    artifact = result.results[0].artifacts[0]
    run_scoped_path = Path(artifact.run_scoped_path)
    manifest_path = run_scoped_path.parent / "manifest.json"
    header = run_scoped_path.read_text(encoding="utf-8").splitlines()[0]

    assert artifact.output_format.value == "tsv"
    assert run_scoped_path.suffix == ".tsv"
    assert "\t" in header
    assert manifest_path.exists()


def test_run_publish_definitions_locally_writes_zip_bundle_when_configured(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)
    package_root = _write_publish_package(
        tmp_path,
        replacement_mode="overwrite_in_place",
        packaging_archive_format="zip",
    )
    definitions = discover_publish_definitions(package_root)

    result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=new_run_context(stage=StageName.publish, job_name="publish-run"),
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )

    artifacts_by_format = {
        artifact.output_format.value: artifact for artifact in result.results[0].artifacts
    }
    assert set(artifacts_by_format) == {"csv", "zip"}

    zip_artifact = artifacts_by_format["zip"]
    zip_run_scoped_path = Path(zip_artifact.run_scoped_path)
    assert zip_run_scoped_path.suffix == ".zip"
    assert zip_run_scoped_path.exists()
    assert zip_artifact.stable_delivery_path is not None
    assert Path(zip_artifact.stable_delivery_path).exists()

    with zipfile.ZipFile(zip_run_scoped_path, "r") as handle:
        assert any(member.endswith(".csv") for member in handle.namelist())

    manifest_path = zip_run_scoped_path.parent / "manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sorted(
        artifact["output_format"] for artifact in manifest_payload["artifacts"]
    ) == ["csv", "zip"]


def test_run_publish_definitions_locally_appends_new_delivery_artifacts(
    tmp_path: Path, spark_session
) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_order_summary_table(spark_session, warehouse_root)
    package_root = _write_publish_package(
        tmp_path,
        replacement_mode="append_new_artifact",
    )
    definitions = discover_publish_definitions(package_root)
    first_context = new_run_context(stage=StageName.publish, job_name="publish-run")
    second_context = new_run_context(stage=StageName.publish, job_name="publish-run")

    first_result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=first_context,
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )
    second_result = run_publish_definitions_locally(
        root_path=str(tmp_path),
        run_context=second_context,
        environment="default",
        package_path=package_root,
        warehouse_root=str(warehouse_root),
        spark=spark_session,
        definitions=definitions,
    )

    first_artifact = first_result.results[0].artifacts[0]
    second_artifact = second_result.results[0].artifacts[0]

    assert first_artifact.stable_delivery_path is not None
    assert second_artifact.stable_delivery_path is not None
    first_stable_path = Path(first_artifact.stable_delivery_path)
    second_stable_path = Path(second_artifact.stable_delivery_path)
    assert first_stable_path.exists()
    assert second_stable_path.exists()
    assert first_stable_path != second_stable_path
    assert f"run_id={first_context.run_id}" in first_stable_path.name
    assert f"run_id={second_context.run_id}" in second_stable_path.name


def _write_publish_package(
    base_path: Path,
    *,
    selection_mode: str = "direct",
    include_query: bool = True,
    replacement_mode: str = "versioned_delivery",
    output_format: str = "csv",
    packaging_archive_format: str | None = None,
    path_template: str = "exports/{domain}/{publish_name}/daily_order_export.{output_extension}",
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
        f"  selection_mode: {selection_mode}",
        "delivery:",
        "  target_type: local_filesystem",
        f"  output_format: {output_format}",
        f"  path_template: {path_template}",
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
    if packaging_archive_format is not None:
        insert_at = manifest_lines.index(f"  replacement_mode: {replacement_mode}") + 1
        manifest_lines[insert_at:insert_at] = [
            "  packaging:",
            f"    archive_format: {packaging_archive_format}",
        ]
    (publish_dir / "manifest.yaml").write_text("\n".join(manifest_lines), encoding="utf-8")
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

from __future__ import annotations

import argparse
from pathlib import Path

from elt_pipeline._cli_helpers import (
    _DEFAULT_ROOT_PATH,
    _DEFAULT_ROOT_PATH_EVAL,
    _DEFAULT_WAREHOUSE_ROOT,
    _DEFAULT_WAREHOUSE_ROOT_EVAL,
)
from elt_pipeline.normalize.partitioning import PartitionMode
from elt_pipeline.shared.runtime import StageName
from elt_pipeline.sql.models import SqlModelStage


def _add_sql_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("package_path", type=Path)
    parser.add_argument(
        "--stage",
        choices=[stage.value for stage in SqlModelStage],
    )
    parser.add_argument("--domain")
    parser.add_argument("--model")
    parser.add_argument("--environment", default="default")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--vars-json")
    parser.add_argument(
        "--partition",
        action="append",
        default=[],
        help="Partition override in key=value form. May be passed multiple times.",
    )


def _add_publish_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("package_path", type=Path)
    parser.add_argument("--domain")
    parser.add_argument("--publish", dest="publish_name")
    parser.add_argument("--environment", default="default")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="elt-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate-config",
        help="Validate a YAML configuration file and optionally resolve one source/entity.",
    )
    validate_parser.add_argument("config_path", type=Path)
    validate_parser.add_argument("--environment", default="default")
    validate_parser.add_argument("--source")
    validate_parser.add_argument("--entity")

    run_context_parser = subparsers.add_parser(
        "show-run-context",
        help="Create and display a runtime context object.",
    )
    run_context_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in StageName],
        required=True,
    )
    run_context_parser.add_argument("--job-name", required=True)
    run_context_parser.add_argument("--trigger-type", default="manual")

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Run configured ingestion sources and entities in local mode.",
    )
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", required=True)
    ingest_run_parser = ingest_subparsers.add_parser(
        "run",
        help="Run one or more configured source entities and persist raw level1 artifacts.",
    )
    ingest_run_parser.add_argument("config_path", type=Path)
    ingest_run_parser.add_argument("--environment", default="default")
    ingest_run_parser.add_argument("--source")
    ingest_run_parser.add_argument("--entity")
    ingest_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    ingest_run_parser.add_argument("--job-name", default="ingest-run")
    ingest_run_parser.add_argument("--trigger-type", default="manual")
    ingest_run_parser.add_argument(
        "--window-start",
        help="Optional ISO-8601 window start for bounded or backfill ingest runs.",
    )
    ingest_run_parser.add_argument(
        "--window-end",
        help="Optional ISO-8601 window end for bounded or backfill ingest runs.",
    )
    ingest_run_parser.add_argument(
        "--window-label",
        help="Optional stable label for the requested ingest window.",
    )
    ingest_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Seed checkpoint state from prior history for the requested window.",
    )
    ingest_run_parser.add_argument(
        "--kafka-log-path",
        type=str,
        help="Optional override for local Kafka replay log input.",
    )

    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Run local level1 to level2 normalization.",
    )
    normalize_subparsers = normalize_parser.add_subparsers(
        dest="normalize_command",
        required=True,
    )
    normalize_run_parser = normalize_subparsers.add_parser(
        "run",
        help="Normalize level1 manifests into local level2 tables.",
    )
    normalize_run_parser.add_argument("config_path", type=Path)
    normalize_run_parser.add_argument("--environment", default="default")
    normalize_run_parser.add_argument("--source")
    normalize_run_parser.add_argument("--entity")
    normalize_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    normalize_run_parser.add_argument("--job-name", default="normalize-run")
    normalize_run_parser.add_argument("--trigger-type", default="manual")
    normalize_run_parser.add_argument(
        "--window-start",
        help="Optional ISO-8601 lower bound used to select level1 manifests for reruns.",
    )
    normalize_run_parser.add_argument(
        "--window-end",
        help="Optional ISO-8601 upper bound used to select level1 manifests for reruns.",
    )
    normalize_run_parser.add_argument(
        "--window-label",
        help="Optional stable label for the requested normalization window.",
    )
    normalize_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the normalization selection as a targeted historical rerun.",
    )
    normalize_run_parser.add_argument(
        "--manifest-path",
        action="append",
        default=[],
        type=str,
        help="Explicit level1 manifest path to normalize. May be passed multiple times.",
    )
    normalize_run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the exact level1 artifact selected by a prior normalize run.",
    )
    normalize_run_parser.add_argument(
        "--partition-mode",
        choices=[mode.value for mode in PartitionMode],
        default=PartitionMode.ingest_date.value,
    )
    normalize_run_parser.add_argument("--partition-key")
    normalize_run_parser.add_argument("--metadata-key")

    sql_parser = subparsers.add_parser(
        "sql",
        help="Discover, compile, and run local SQL model packages.",
    )
    sql_subparsers = sql_parser.add_subparsers(dest="sql_command", required=True)

    compile_parser = sql_subparsers.add_parser(
        "compile",
        help="Compile SQL models with runtime tokens resolved.",
    )
    _add_sql_selection_arguments(compile_parser)
    compile_parser.add_argument("--include-deps", action="store_true")

    run_parser = sql_subparsers.add_parser(
        "run",
        help="Run SQL models against a Spark-backed local parquet or Iceberg warehouse.",
    )
    _add_sql_selection_arguments(run_parser)
    run_parser.add_argument("--include-deps", action="store_true")
    run_parser.add_argument(
        "--root-path",
        type=str,
        default=_DEFAULT_ROOT_PATH_EVAL,
        help=(
            "Pipeline runtime root containing level1/level2 data and run artifacts. "
            "Defaults to ELT_PIPELINE_REPO_RUN_DIR/runtime if the project-wide repo_run "
            "directory is available, otherwise .ignore/runtime."
        ),
    )
    run_parser.add_argument(
        "--warehouse-root",
        type=str,
        default=_DEFAULT_WAREHOUSE_ROOT_EVAL,
        help=(
            "SQL warehouse root for level3/level4 output. Defaults to "
            "ELT_PIPELINE_REPO_RUN_DIR/warehouse if repo_run is available, else "
            ".ignore/warehouse."
        ),
    )
    run_parser.add_argument("--job-name", default="sql-run")
    run_parser.add_argument("--trigger-type", default="manual")
    run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the model/window/partition selection from a prior sql run.",
    )
    run_parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate compiled SQL against the target database without executing writes.",
    )
    run_parser.add_argument(
        "--explain",
        action="store_true",
        help="Include sqlite query plan details; implies a validate-only planning run.",
    )
    run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
        help=(
            "Enable Iceberg table format for level3/level4 writes (overrides env "
            "ELT_PIPELINE_ICEBERG_ENABLED). When set, writes go to the configured Iceberg "
            "catalog instead of plain parquet files and atomic staging-swap is bypassed "
            "for Iceberg-managed commits."
        ),
    )
    run_parser.add_argument(
        "--no-iceberg-enabled",
        dest="iceberg_enabled",
        action="store_false",
        default=None,
        help=(
            "Explicitly DISABLE Iceberg table format (post OD-I1 step(a) opt-out default). "
            "Takes precedence over env ELT_PIPELINE_ICEBERG_ENABLED=true or YAML "
            "spark.enable_iceberg=true. Forces legacy parquet + staging-swap path."
        ),
    )
    run_parser.add_argument(
        "--iceberg-catalog-name",
        default=None,
        help="Override env ELT_PIPELINE_ICEBERG_CATALOG_NAME (default: iceberg).",
    )
    run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "hive_metastore", "jdbc", "nessie", "rest", "glue"],
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_TYPE (default: hadoop). "
            "hadoop=filesystem (local zero-infra); "
            "hive_metastore=Hive Metastore Thrift (requires --iceberg-hive-metastore-uri); "
            "jdbc=H2/Postgres-backed (requires URI); "
            "nessie=Apache Nessie REST server alias (dispatches identical to rest, requires URI); "
            "rest=Polaris/Nessie/Lakekeeper/Tabular (requires URI); "
            "glue=AWS Glue Data Catalog (requires region or default SDK region)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-catalog-uri",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_URI. Required when "
            "--iceberg-catalog-type=jdbc (JDBC connection string) or "
            "--iceberg-catalog-type=rest (REST server endpoint, e.g. http://localhost:8181/api/v1)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-rest-token",
        default=None,
        dest="iceberg_rest_token",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_TOKEN. Bearer / API token for "
            "--iceberg-catalog-type=rest (Polaris/Nessie/Lakekeeper/Tabular auth)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-rest-warehouse",
        default=None,
        dest="iceberg_rest_warehouse",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_WAREHOUSE. Warehouse name/ID for "
            "--iceberg-catalog-type=rest when the REST server hosts multiple warehouses."
        ),
    )
    run_parser.add_argument(
        "--iceberg-glue-region",
        default=None,
        dest="iceberg_glue_region",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_GLUE_REGION. AWS region for "
            "--iceberg-catalog-type=glue (falls back to standard AWS SDK region chain)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-hive-metastore-uri",
        default=None,
        dest="iceberg_hive_metastore_uri",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_HIVE_METASTORE_URI. Required when "
            "--iceberg-writer-catalog-type=hive_metastore. Format: thrift://<host>:9083 "
            "(standard Hive Metastore Thrift endpoint)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-warehouse-dir",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR. If omitted and Iceberg is "
            "enabled, automatically falls back to <warehouse-root>/iceberg."
        ),
    )
    run_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section with layering: CLI args > ENV > YAML > "
            "frozen manifest defaults. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH`` when not explicitly passed."
        ),
    )

    publish_parser = subparsers.add_parser(
        "publish",
        help="Discover, validate, explain, and run local level5 publish definitions.",
    )
    publish_subparsers = publish_parser.add_subparsers(
        dest="publish_command",
        required=True,
    )

    publish_validate_parser = publish_subparsers.add_parser(
        "validate",
        help="Validate a publish definition package without writing outputs.",
    )
    _add_publish_selection_arguments(publish_validate_parser)

    publish_explain_parser = publish_subparsers.add_parser(
        "explain",
        help="Preview the artifacts a publish run would produce.",
    )
    _add_publish_selection_arguments(publish_explain_parser)
    publish_explain_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    publish_explain_parser.add_argument("--job-name", default="publish-explain")
    publish_explain_parser.add_argument("--trigger-type", default="manual")
    publish_explain_parser.add_argument("--window-start")
    publish_explain_parser.add_argument("--window-end")
    publish_explain_parser.add_argument("--window-label")
    publish_explain_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the publish selection as a targeted historical backfill.",
    )
    publish_explain_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH``."
        ),
    )

    publish_run_parser = publish_subparsers.add_parser(
        "run",
        help="Run publish definitions against a Spark-backed parquet or Iceberg warehouse.",
    )
    _add_publish_selection_arguments(publish_run_parser)
    publish_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    publish_run_parser.add_argument("--warehouse-root", type=str, default=_DEFAULT_WAREHOUSE_ROOT)
    publish_run_parser.add_argument("--job-name", default="publish-run")
    publish_run_parser.add_argument("--trigger-type", default="manual")
    publish_run_parser.add_argument("--window-start")
    publish_run_parser.add_argument("--window-end")
    publish_run_parser.add_argument("--window-label")
    publish_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the publish selection as a targeted historical backfill.",
    )
    publish_run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the publish/window selection from a prior publish run.",
    )
    publish_run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
        help=(
            "Enable Iceberg table format for level3/level4 source reads (overrides env "
            "ELT_PIPELINE_ICEBERG_ENABLED). When set, reads from the configured Iceberg "
            "catalog instead of plain parquet files."
        ),
    )
    publish_run_parser.add_argument(
        "--no-iceberg-enabled",
        dest="iceberg_enabled",
        action="store_false",
        default=None,
        help=(
            "Explicitly DISABLE Iceberg table format for publish reads (post OD-I1 "
            "step(a) opt-out default). Takes precedence over env "
            "ELT_PIPELINE_ICEBERG_ENABLED=true or YAML spark.enable_iceberg=true; "
            "forces legacy parquet path."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-name",
        default=None,
        help="Override env ELT_PIPELINE_ICEBERG_CATALOG_NAME (default: iceberg).",
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "hive_metastore", "jdbc", "nessie", "rest", "glue"],
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_TYPE (default: hadoop). "
            "hadoop=filesystem (local zero-infra); "
            "hive_metastore=Hive Metastore Thrift (requires --iceberg-hive-metastore-uri); "
            "jdbc=H2/Postgres-backed (requires URI); "
            "nessie=Apache Nessie REST server alias (dispatches identical to rest, requires URI); "
            "rest=Polaris/Nessie/Lakekeeper/Tabular (requires URI); "
            "glue=AWS Glue Data Catalog (requires region or default SDK region)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-uri",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_URI. Required when "
            "--iceberg-catalog-type=jdbc (JDBC connection string) or "
            "--iceberg-catalog-type=rest (REST server endpoint, e.g. http://localhost:8181/api/v1)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-rest-token",
        default=None,
        dest="iceberg_rest_token",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_TOKEN. Bearer / API token for "
            "--iceberg-catalog-type=rest (Polaris/Nessie/Lakekeeper/Tabular auth)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-rest-warehouse",
        default=None,
        dest="iceberg_rest_warehouse",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_WAREHOUSE. Warehouse name/ID for "
            "--iceberg-catalog-type=rest when the REST server hosts multiple warehouses."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-glue-region",
        default=None,
        dest="iceberg_glue_region",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_GLUE_REGION. AWS region for "
            "--iceberg-catalog-type=glue (falls back to standard AWS SDK region chain)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-hive-metastore-uri",
        default=None,
        dest="iceberg_hive_metastore_uri",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_HIVE_METASTORE_URI. Required when "
            "--iceberg-writer-catalog-type=hive_metastore. Format: thrift://<host>:9083 "
            "(standard Hive Metastore Thrift endpoint)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-warehouse-dir",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR. If omitted and Iceberg is "
            "enabled, automatically falls back to <warehouse-root>/iceberg."
        ),
    )
    publish_run_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH``."
        ),
    )

    maintain_parser = subparsers.add_parser(
        "maintain",
        help="Run Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup).",
    )
    maintain_subparsers = maintain_parser.add_subparsers(
        dest="maintain_command",
        required=True,
    )
    maintain_run_parser = maintain_subparsers.add_parser(
        "run",
        help="Run Iceberg table maintenance on the selected L3/L4 table set.",
    )
    maintain_run_parser.add_argument(
        "--table",
        dest="maintain_tables",
        action="append",
        default=[],
        help=(
            "Fully-qualified Iceberg table name (catalog.stage.name) to maintain. "
            "May be passed multiple times. Combined with --all-level3 / --all-level4."
        ),
    )
    maintain_run_parser.add_argument(
        "--all-level3",
        dest="maintain_all_level3",
        action="store_true",
        help="Include every Iceberg table under the level3 namespace.",
    )
    maintain_run_parser.add_argument(
        "--all-level4",
        dest="maintain_all_level4",
        action="store_true",
        help="Include every Iceberg table under the level4 namespace.",
    )
    maintain_run_parser.add_argument(
        "--only",
        dest="maintain_only",
        default=None,
        help=(
            "Comma-separated list of operations to run (compact, expire_snapshots, "
            "remove_orphans, rewrite_manifests). Default: compact+expire_snapshots+remove_orphans."
        ),
    )
    maintain_run_parser.add_argument(
        "--compact",
        dest="maintain_do_compact",
        action="store_true",
        default=None,
        help="Enable file compaction (rewrite_data_files). Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--no-compact",
        dest="maintain_do_compact",
        action="store_false",
        default=None,
        help="Disable file compaction. Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--expire-snapshots",
        dest="maintain_do_expire",
        action="store_true",
        default=None,
        help="Enable snapshot expiry. Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--no-expire-snapshots",
        dest="maintain_do_expire",
        action="store_false",
        default=None,
        help="Disable snapshot expiry. Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--remove-orphans",
        dest="maintain_do_orphans",
        action="store_true",
        default=None,
        help="Enable orphan file removal. Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--no-remove-orphans",
        dest="maintain_do_orphans",
        action="store_false",
        default=None,
        help="Disable orphan file removal. Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--rewrite-manifests",
        dest="maintain_do_manifests",
        action="store_true",
        default=None,
        help="Enable manifest rewrites (off by default). Overrides --only.",
    )
    maintain_run_parser.add_argument(
        "--snapshot-retain-days",
        type=int,
        default=None,
        help="Expire snapshots older than N days. Default 7.",
    )
    maintain_run_parser.add_argument(
        "--snapshot-retain-last",
        type=int,
        default=None,
        help="Always keep at least N most recent snapshots. Default 1.",
    )
    maintain_run_parser.add_argument(
        "--orphan-older-than-days",
        type=int,
        default=None,
        help="Remove orphan files older than N days. Default 3 (safety buffer).",
    )
    maintain_run_parser.add_argument(
        "--compact-strategy",
        choices=["binpack", "sort"],
        default=None,
        help="rewrite_data_files strategy. Default binpack.",
    )
    maintain_run_parser.add_argument(
        "--compact-min-input-files",
        type=int,
        default=None,
        help="Minimum number of input files before compaction runs. Default 5.",
    )
    maintain_run_parser.add_argument(
        "--compact-target-file-size-mb",
        type=int,
        default=None,
        help="Target output file size in MiB. Defaults to the Iceberg catalog default.",
    )
    maintain_run_parser.add_argument(
        "--dry-run",
        dest="maintain_dry_run",
        action="store_true",
        help="List selected tables and requested operations without executing CALLs.",
    )
    maintain_run_parser.add_argument(
        "--root-path",
        type=str,
        default=_DEFAULT_ROOT_PATH_EVAL,
        help=(
            "Pipeline runtime root (for consistency with sql runs; not used by "
            "maintenance procedures directly, but included in context resolution)."
        ),
    )
    maintain_run_parser.add_argument(
        "--warehouse-root",
        type=str,
        default=_DEFAULT_WAREHOUSE_ROOT_EVAL,
        help=(
            "SQL warehouse root for level3/level4 output. Defaults to "
            "ELT_PIPELINE_REPO_RUN_DIR/warehouse if repo_run is available, else "
            ".ignore/warehouse. Used to auto-derive --iceberg-warehouse-dir when omitted."
        ),
    )
    maintain_run_parser.add_argument(
        "--environment", default="default", dest="maintain_environment",
    )
    maintain_run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
        help="Enable Iceberg (required for maintenance; normally the default).",
    )
    maintain_run_parser.add_argument(
        "--no-iceberg-enabled",
        dest="iceberg_enabled",
        action="store_false",
        default=None,
        help="Explicitly disable Iceberg (maintenance has no effect).",
    )
    maintain_run_parser.add_argument("--iceberg-catalog-name", default=None)
    maintain_run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "hive_metastore", "jdbc", "nessie", "rest", "glue"],
    )
    maintain_run_parser.add_argument("--iceberg-catalog-uri", default=None)
    maintain_run_parser.add_argument(
        "--iceberg-rest-token", default=None, dest="iceberg_rest_token",
    )
    maintain_run_parser.add_argument(
        "--iceberg-rest-warehouse", default=None, dest="iceberg_rest_warehouse",
    )
    maintain_run_parser.add_argument(
        "--iceberg-glue-region", default=None, dest="iceberg_glue_region",
    )
    maintain_run_parser.add_argument(
        "--iceberg-hive-metastore-uri",
        default=None,
        dest="iceberg_hive_metastore_uri",
    )
    maintain_run_parser.add_argument("--iceberg-warehouse-dir", default=None)
    maintain_run_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg). Auto-resolves from env ELT_PIPELINE_CONFIG_PATH."
        ),
    )

    schedule_parser = subparsers.add_parser(
        "schedule",
        help="Execute ordered local schedule plans by calling existing CLI commands.",
    )
    schedule_subparsers = schedule_parser.add_subparsers(
        dest="schedule_command",
        required=True,
    )
    schedule_run_parser = schedule_subparsers.add_parser(
        "run",
        help="Run a validated local schedule plan in deterministic job order.",
    )
    schedule_run_parser.add_argument("plan_path", type=Path)
    schedule_run_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running remaining jobs after a job failure.",
    )
    schedule_run_parser.add_argument(
        "--audit-root",
        type=Path,
        default=None,
        help=(
            "Directory root where schedule_execution_audit.json is written. "
            "Defaults to <plan-file-dir>/runs/schedule_<sha>/ for workstation use."
        ),
    )

    lineage_parser = subparsers.add_parser(
        "lineage",
        help="Inspect lineage artifacts emitted by prior runs.",
    )
    lineage_subparsers = lineage_parser.add_subparsers(
        dest="lineage_command",
        required=True,
    )
    impact_parser = lineage_subparsers.add_parser(
        "impact-analysis",
        help="Bidirectional column-level impact analysis over collected lineage.jsonl.",
    )
    impact_parser.add_argument(
        "--column",
        dest="impact_column",
        required=True,
        help=(
            "Target column in the form '<dataset>.<column_name>'. Dataset is the "
            "output dataset FQN recorded in lineage.jsonl — "
            "typically the target table name or 'namespace:table_name'."
        ),
    )
    impact_parser.add_argument(
        "--depth",
        type=int,
        default=5,
        help="Maximum graph-walk depth in both upstream and downstream directions (default: 5).",
    )
    impact_parser.add_argument(
        "--format",
        dest="impact_format",
        choices=["table", "json"],
        default="table",
        help="Output format. 'table' produces a human-readable summary, 'json' "
             "emits raw machine-readable JSON.",
    )
    impact_parser.add_argument(
        "--root-path",
        type=str,
        default=_DEFAULT_ROOT_PATH_EVAL,
        help=(
            "Pipeline runtime root containing runs/.../lineage.jsonl files "
            "defaults to ELT_PIPELINE_REPO_RUN_DIR/runtime if available, "
            "otherwise .ignore/runtime."
        ),
    )

    metric_parser = subparsers.add_parser(
        "metric",
        help="Discover, compile, and run semantic metric definitions.",
    )
    metric_subparsers = metric_parser.add_subparsers(
        dest="metric_command",
        required=True,
    )

    metric_compile_parser = metric_subparsers.add_parser(
        "compile",
        help="Compile metric manifests and validate query_refs against SQL models.",
    )
    metric_compile_parser.add_argument("package_path", type=Path)
    metric_compile_parser.add_argument("--domain")
    metric_compile_parser.add_argument("--metric")
    metric_compile_parser.add_argument(
        "--with-sql-refs",
        dest="with_sql_refs",
        action="store_true",
        default=False,
        help=(
            "Also discover SQL models from the same package and validate that "
            "query_ref model/column exist in SqlColumnSpec governance. "
            "Without this flag, compile performs structural YAML validation only."
        ),
    )
    metric_compile_parser.add_argument(
        "--format",
        dest="compile_format",
        choices=["summary", "json"],
        default="summary",
    )

    metric_run_parser = metric_subparsers.add_parser(
        "run",
        help="Run compiled metrics in one or more resolution modes.",
    )
    metric_run_parser.add_argument("package_path", type=Path)
    metric_run_parser.add_argument("--domain")
    metric_run_parser.add_argument("--metric")
    metric_run_parser.add_argument(
        "--mode",
        dest="run_modes",
        action="append",
        choices=["materialize", "view", "prometheus"],
        required=True,
        help=(
            "Resolution mode(s). Repeatable to run multiple modes. "
            "materialize=Iceberg table, view=Trino SECURITY DEFINER VIEW, "
            "prometheus=Prometheus gauge via existing metrics adapter."
        ),
    )
    metric_run_parser.add_argument(
        "--root-path",
        type=str,
        default=_DEFAULT_ROOT_PATH_EVAL,
    )
    metric_run_parser.add_argument(
        "--warehouse-root",
        type=str,
        default=_DEFAULT_WAREHOUSE_ROOT_EVAL,
    )
    metric_run_parser.add_argument("--job-name", default="metric-run")
    metric_run_parser.add_argument("--trigger-type", default="manual")
    metric_run_parser.add_argument(
        "--target-catalog",
        default="spark_catalog",
        help="Target catalog name for materialize mode (default: spark_catalog).",
    )
    metric_run_parser.add_argument(
        "--target-namespace",
        default="metrics",
        help="Target schema/namespace for materialize and view modes (default: metrics).",
    )
    metric_run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
    )
    metric_run_parser.add_argument(
        "--no-iceberg-enabled",
        dest="iceberg_enabled",
        action="store_false",
        default=None,
    )
    metric_run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "hive_metastore", "jdbc", "nessie", "rest", "glue"],
    )
    metric_run_parser.add_argument("--iceberg-catalog-name", default=None)
    metric_run_parser.add_argument("--iceberg-catalog-uri", default=None)
    metric_run_parser.add_argument("--iceberg-rest-token", default=None)
    metric_run_parser.add_argument("--iceberg-rest-warehouse", default=None)
    metric_run_parser.add_argument("--iceberg-glue-region", default=None)
    metric_run_parser.add_argument("--iceberg-hive-metastore-uri", default=None)

    return parser

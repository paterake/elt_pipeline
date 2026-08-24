from __future__ import annotations

from pathlib import Path

from elt_pipeline.integrations import DagsterCliWrapper

try:
    from dagster import (
        AssetExecutionContext,
        AssetIn,
        Config,
        Definitions,
        EnvVar,
        asset,
        define_asset_job,
    )
except ImportError:  # pragma: no cover - example file for optional dependency users
    AssetExecutionContext = None
    AssetIn = None
    Config = None
    Definitions = None
    EnvVar = None
    asset = None
    define_asset_job = None


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "examples" / "configs" / "local_object_storage_orders.yaml"
SQL_PACKAGE = REPO_ROOT / "examples" / "sql" / "local_demo"
PUBLISH_PACKAGE = REPO_ROOT / "examples" / "publish" / "local_demo"
ROOT_PATH = REPO_ROOT
WAREHOUSE_ROOT = REPO_ROOT / ".ignore" / "warehouse"
WRAPPER = DagsterCliWrapper(repo_root=REPO_ROOT)


class PipelineConfig(Config):
    environment: str = "default"
    source: str = "orders_object_storage"
    entity: str = "orders"
    start_date: str = "2026-01-01"
    end_date: str = "2026-01-31"


if asset is not None and AssetExecutionContext is not None and Config is not None:

    @asset(
        name="ingest_orders_l1",
        group_name="elt_pipeline",
        description="Ingest raw orders from object storage into L1 artifacts.",
    )
    def ingest_orders_l1(
        context: AssetExecutionContext, config: PipelineConfig
    ) -> None:
        WRAPPER.invoke(
            subcommand=("ingest", "run"),
            arguments=(
                "--config-path",
                str(CONFIG_PATH),
                "--environment",
                config.environment,
                "--root-path",
                str(ROOT_PATH),
                "--source",
                config.source,
                "--entity",
                config.entity,
                "--job-name",
                "dagster-ingest-orders",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name,
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=600.0,
        )

    @asset(
        name="normalize_orders_l2",
        group_name="elt_pipeline",
        ins={"upstream": AssetIn("ingest_orders_l1")},
        description="Normalize L1 orders manifest into L2 parquet.",
    )
    def normalize_orders_l2(
        context: AssetExecutionContext,
        config: PipelineConfig,
        upstream: None,
    ) -> None:
        WRAPPER.invoke(
            subcommand=("normalize", "run"),
            arguments=(
                "--config-path",
                str(CONFIG_PATH),
                "--environment",
                config.environment,
                "--root-path",
                str(ROOT_PATH),
                "--source",
                config.source,
                "--entity",
                config.entity,
                "--job-name",
                "dagster-normalize-orders",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name,
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=900.0,
        )

    @asset(
        name="sql_orders_l3_l4",
        group_name="elt_pipeline",
        ins={"upstream": AssetIn("normalize_orders_l2")},
        description="Compile and run L3 (canonical) and L4 (datamart) SQL models.",
    )
    def sql_orders_l3_l4(
        context: AssetExecutionContext,
        config: PipelineConfig,
        upstream: None,
    ) -> None:
        WRAPPER.invoke(
            subcommand=("sql", "compile"),
            arguments=(
                str(SQL_PACKAGE),
                "--environment",
                config.environment,
                "--include-deps",
                "--start-date",
                config.start_date,
                "--end-date",
                config.end_date,
                "--job-name",
                "dagster-sql-compile",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name + "_compile",
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=300.0,
        )
        WRAPPER.invoke(
            subcommand=("sql", "run"),
            arguments=(
                str(SQL_PACKAGE),
                "--root-path",
                str(ROOT_PATH),
                "--warehouse-root",
                str(WAREHOUSE_ROOT),
                "--environment",
                config.environment,
                "--include-deps",
                "--start-date",
                config.start_date,
                "--end-date",
                config.end_date,
                "--job-name",
                "dagster-sql-run",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name + "_run",
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=1800.0,
        )

    @asset(
        name="publish_orders_l5",
        group_name="elt_pipeline",
        ins={"upstream": AssetIn("sql_orders_l3_l4")},
        description="Run L5 publish definitions to export artifacts (CSV/JSONL/TSV/zip).",
    )
    def publish_orders_l5(
        context: AssetExecutionContext,
        config: PipelineConfig,
        upstream: None,
    ) -> None:
        WRAPPER.invoke(
            subcommand=("publish", "validate"),
            arguments=(
                str(PUBLISH_PACKAGE),
                "--environment",
                config.environment,
                "--job-name",
                "dagster-publish-validate",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name + "_validate",
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=120.0,
        )
        WRAPPER.invoke(
            subcommand=("publish", "run"),
            arguments=(
                str(PUBLISH_PACKAGE),
                "--root-path",
                str(ROOT_PATH),
                "--warehouse-root",
                str(WAREHOUSE_ROOT),
                "--environment",
                config.environment,
                "--job-name",
                "dagster-publish-run",
            ),
            dagster_context={
                "job": context.job_def,
                "run_id": context.run_id,
                "op_name": context.op_def.name + "_run",
                "retry_number": context.retry_number + 1 if hasattr(context, "retry_number") else 1,
                "tags": context.run_tags,
                "partition_key": (
                    context.partition_key
                    if hasattr(context, "has_partition_key") and context.has_partition_key
                    else None
                ),
            },
            timeout_seconds=1200.0,
        )

    elt_pipeline_daily_job = define_asset_job(
        name="elt_pipeline_daily",
        selection=[
            ingest_orders_l1,
            normalize_orders_l2,
            sql_orders_l3_l4,
            publish_orders_l5,
        ],
        tags={"dagster/max_retries": "2"},
    )

    defs = Definitions(
        assets=[
            ingest_orders_l1,
            normalize_orders_l2,
            sql_orders_l3_l4,
            publish_orders_l5,
        ],
        jobs=[elt_pipeline_daily_job],
        resources={},
    )

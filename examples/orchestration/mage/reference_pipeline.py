from __future__ import annotations

from pathlib import Path

from elt_pipeline.integrations import MageCliWrapper

try:
    from mage_ai.data_preparation.decorators import data_loader, transformer
except ImportError:  # pragma: no cover - example file for optional dependency users
    data_loader = None
    transformer = None


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "examples" / "configs" / "local_object_storage_orders.yaml"
SQL_PACKAGE = REPO_ROOT / "examples" / "sql" / "local_demo"
PUBLISH_PACKAGE = REPO_ROOT / "examples" / "publish" / "local_demo"
ROOT_PATH = REPO_ROOT
WAREHOUSE_ROOT = REPO_ROOT / ".ignore" / "warehouse"
WRAPPER = MageCliWrapper(repo_root=REPO_ROOT)

ENVIRONMENT = "default"
SOURCE = "orders_object_storage"
ENTITY = "orders"
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"


def run_ingest(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("ingest", "run"),
        arguments=(
            "--config-path",
            str(CONFIG_PATH),
            "--environment",
            ENVIRONMENT,
            "--root-path",
            str(ROOT_PATH),
            "--source",
            SOURCE,
            "--entity",
            ENTITY,
            "--job-name",
            "mage-ingest-orders",
        ),
        mage_context=mage_context,
        timeout_seconds=600.0,
    )


def run_normalize(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("normalize", "run"),
        arguments=(
            "--config-path",
            str(CONFIG_PATH),
            "--environment",
            ENVIRONMENT,
            "--root-path",
            str(ROOT_PATH),
            "--source",
            SOURCE,
            "--entity",
            ENTITY,
            "--job-name",
            "mage-normalize-orders",
        ),
        mage_context=mage_context,
        timeout_seconds=900.0,
    )


def run_sql_compile(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("sql", "compile"),
        arguments=(
            str(SQL_PACKAGE),
            "--environment",
            ENVIRONMENT,
            "--include-deps",
            "--start-date",
            START_DATE,
            "--end-date",
            END_DATE,
            "--job-name",
            "mage-sql-compile",
        ),
        mage_context=mage_context,
        timeout_seconds=300.0,
    )


def run_sql_run(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("sql", "run"),
        arguments=(
            str(SQL_PACKAGE),
            "--root-path",
            str(ROOT_PATH),
            "--warehouse-root",
            str(WAREHOUSE_ROOT),
            "--environment",
            ENVIRONMENT,
            "--include-deps",
            "--start-date",
            START_DATE,
            "--end-date",
            END_DATE,
            "--job-name",
            "mage-sql-run",
        ),
        mage_context=mage_context,
        timeout_seconds=1800.0,
    )


def run_publish_validate(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("publish", "validate"),
        arguments=(
            str(PUBLISH_PACKAGE),
            "--environment",
            ENVIRONMENT,
            "--job-name",
            "mage-publish-validate",
        ),
        mage_context=mage_context,
        timeout_seconds=120.0,
    )


def run_publish_run(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("publish", "run"),
        arguments=(
            str(PUBLISH_PACKAGE),
            "--root-path",
            str(ROOT_PATH),
            "--warehouse-root",
            str(WAREHOUSE_ROOT),
            "--environment",
            ENVIRONMENT,
            "--job-name",
            "mage-publish-run",
        ),
        mage_context=mage_context,
        timeout_seconds=1200.0,
    )


def run_maintenance(mage_context: dict | None = None) -> None:
    WRAPPER.invoke(
        subcommand=("maintain", "run"),
        arguments=(
            "--root-path",
            str(ROOT_PATH),
            "--warehouse-root",
            str(WAREHOUSE_ROOT),
            "--environment",
            ENVIRONMENT,
            "--all-level3",
            "--all-level4",
            "--rewrite-manifests",
            "--job-name",
            "mage-maintain",
        ),
        mage_context=mage_context,
        timeout_seconds=3600.0,
    )


if data_loader is not None and transformer is not None:

    @data_loader
    def ingest_orders_l1(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_ingest(mage_context=mage_context)

    @transformer
    def normalize_orders_l2(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_normalize(mage_context=mage_context)

    @transformer
    def sql_compile_models(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_sql_compile(mage_context=mage_context)

    @transformer
    def sql_run_models(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_sql_run(mage_context=mage_context)

    @transformer
    def publish_validate(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_publish_validate(mage_context=mage_context)

    @transformer
    def publish_run_l5(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_publish_run(mage_context=mage_context)

    @transformer
    def maintain_iceberg_tables(*args, **kwargs) -> None:
        context = kwargs.get("context", {})
        pipeline = context.get("pipeline") if isinstance(context, dict) else None
        mage_context = {
            "pipeline_name": getattr(pipeline, "name", None) or context.get("pipeline_name"),
            "run_id": context.get("run_id"),
            "block_uuid": context.get("block_uuid"),
            "block_attempt": (context.get("block_attempt") or 1),
            "tags": getattr(pipeline, "tags", None) or context.get("tags"),
            "execution_date": context.get("execution_date"),
        }
        run_maintenance(mage_context=mage_context)

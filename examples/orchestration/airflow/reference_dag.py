from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from elt_pipeline.integrations import AirflowCliWrapper

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - example file for optional dependency users
    DAG = None
    PythonOperator = None


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "examples" / "configs" / "local_object_storage_orders.yaml"
SQL_PACKAGE = REPO_ROOT / "examples" / "sql" / "local_demo"
PUBLISH_PACKAGE = REPO_ROOT / "examples" / "publish" / "local_demo"
ROOT_PATH = REPO_ROOT
WAREHOUSE_ROOT = REPO_ROOT / ".ignore" / "warehouse"
WRAPPER = AirflowCliWrapper(repo_root=REPO_ROOT)

ENVIRONMENT = "default"
SOURCE = "orders_object_storage"
ENTITY = "orders"
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"


def run_ingest(**context) -> None:
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
            "airflow-ingest-orders",
        ),
        airflow_context=context,
        timeout_seconds=600.0,
    )


def run_normalize(**context) -> None:
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
            "airflow-normalize-orders",
        ),
        airflow_context=context,
        timeout_seconds=900.0,
    )


def run_sql_compile(**context) -> None:
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
            "airflow-sql-compile",
        ),
        airflow_context=context,
        timeout_seconds=300.0,
    )


def run_sql_run(**context) -> None:
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
            "airflow-sql-run",
        ),
        airflow_context=context,
        timeout_seconds=1800.0,
    )


def run_publish_validate(**context) -> None:
    WRAPPER.invoke(
        subcommand=("publish", "validate"),
        arguments=(
            str(PUBLISH_PACKAGE),
            "--environment",
            ENVIRONMENT,
            "--job-name",
            "airflow-publish-validate",
        ),
        airflow_context=context,
        timeout_seconds=120.0,
    )


def run_publish_run(**context) -> None:
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
            "airflow-publish-run",
        ),
        airflow_context=context,
        timeout_seconds=1200.0,
    )


def run_maintenance(**context) -> None:
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
            "airflow-maintain",
        ),
        airflow_context=context,
        timeout_seconds=3600.0,
    )


if DAG is not None and PythonOperator is not None:
    default_args = {
        "owner": "elt-pipeline",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    }

    with DAG(
        dag_id="elt_pipeline_reference_full",
        default_args=default_args,
        description="Full 4-phase elt_pipeline DAG: ingest → normalize → sql → publish + maintain",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["elt-pipeline", "reference", "full-pipeline"],
    ) as dag:
        ingest_task = PythonOperator(
            task_id="ingest_orders_l1",
            python_callable=run_ingest,
        )

        normalize_task = PythonOperator(
            task_id="normalize_orders_l2",
            python_callable=run_normalize,
        )

        sql_compile_task = PythonOperator(
            task_id="sql_compile_models",
            python_callable=run_sql_compile,
        )

        sql_run_task = PythonOperator(
            task_id="sql_run_models",
            python_callable=run_sql_run,
        )

        publish_validate_task = PythonOperator(
            task_id="publish_validate",
            python_callable=run_publish_validate,
        )

        publish_run_task = PythonOperator(
            task_id="publish_run_l5",
            python_callable=run_publish_run,
        )

        maintenance_task = PythonOperator(
            task_id="maintain_iceberg_tables",
            python_callable=run_maintenance,
        )

        (
            ingest_task
            >> normalize_task
            >> sql_compile_task
            >> sql_run_task
            >> publish_validate_task
            >> publish_run_task
            >> maintenance_task
        )

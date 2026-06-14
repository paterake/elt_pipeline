from __future__ import annotations

from datetime import datetime
from pathlib import Path

from elt_pipeline.integrations import AirflowCliWrapper

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - example file for optional dependency users
    DAG = None
    PythonOperator = None


REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_PATH = REPO_ROOT / "examples" / "publish" / "local_demo"
DATABASE_PATH = REPO_ROOT / "examples" / "data" / "sql" / "source.db"
ROOT_PATH = REPO_ROOT
WRAPPER = AirflowCliWrapper(repo_root=REPO_ROOT)


def run_publish(**context) -> None:
    WRAPPER.invoke(
        subcommand=("publish", "run"),
        arguments=(
            str(PACKAGE_PATH),
            "--database",
            str(DATABASE_PATH),
            "--root-path",
            str(ROOT_PATH),
            "--environment",
            "default",
            "--job-name",
            "airflow-publish-run",
            "--publish",
            "daily_order_export",
        ),
        airflow_context=context,
        timeout_seconds=300.0,
    )


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="elt_pipeline_reference_publish",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["elt-pipeline", "reference"],
    ) as dag:
        PythonOperator(
            task_id="publish_level5_orders",
            python_callable=run_publish,
        )

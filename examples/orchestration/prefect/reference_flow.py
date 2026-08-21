from __future__ import annotations

from pathlib import Path

from elt_pipeline.integrations import PrefectCliWrapper

try:
    from prefect import flow, get_run_logger, task
    from prefect.tasks import task_input_hash
except ImportError:  # pragma: no cover - example file for optional dependency users
    flow = None
    get_run_logger = None
    task = None
    task_input_hash = None


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "examples" / "configs" / "local_object_storage_orders.yaml"
SQL_PACKAGE = REPO_ROOT / "examples" / "sql" / "local_demo"
PUBLISH_PACKAGE = REPO_ROOT / "examples" / "publish" / "local_demo"
ROOT_PATH = REPO_ROOT
WAREHOUSE_ROOT = REPO_ROOT / ".ignore" / "warehouse"
WRAPPER = PrefectCliWrapper(repo_root=REPO_ROOT)


def _prefect_context_from_run(flow_run, task_run=None, tags=None, scheduled_start_time=None):
    ctx = {
        "flow_run": flow_run,
        "task_run": task_run,
        "tags": tags or [],
    }
    if scheduled_start_time is not None:
        ctx["scheduled_start_time"] = scheduled_start_time
    return ctx


if task is not None and flow is not None:

    @task(
        name="ingest_orders_l1",
        retries=2,
        retry_delay_seconds=30,
        cache_key_fn=task_input_hash,
    )
    def ingest_orders_l1(
        *,
        environment: str = "default",
        source: str = "orders_object_storage",
        entity: str = "orders",
    ) -> None:
        from prefect.context import get_run_context as _get_prefect_context

        prefect_ctx = _get_prefect_context()
        flow_run = getattr(prefect_ctx, "flow_run", None)
        task_run = getattr(prefect_ctx, "task_run", None)
        tags = getattr(flow_run, "tags", None) if flow_run is not None else None
        scheduled_start_time = (
            getattr(flow_run, "scheduled_start_time", None) if flow_run is not None else None
        )

        WRAPPER.invoke(
            subcommand=("ingest", "run"),
            arguments=(
                "--config-path",
                str(CONFIG_PATH),
                "--environment",
                environment,
                "--root-path",
                str(ROOT_PATH),
                "--source",
                source,
                "--entity",
                entity,
                "--job-name",
                "prefect-ingest-orders",
            ),
            prefect_context=_prefect_context_from_run(
                flow_run=flow_run,
                task_run=task_run,
                tags=tags,
                scheduled_start_time=scheduled_start_time,
            ),
            timeout_seconds=600.0,
        )

    @task(
        name="normalize_orders_l2",
        retries=2,
        retry_delay_seconds=30,
        cache_key_fn=task_input_hash,
    )
    def normalize_orders_l2(
        *,
        environment: str = "default",
        source: str = "orders_object_storage",
        entity: str = "orders",
        _ingest_done: None = None,
    ) -> None:
        from prefect.context import get_run_context as _get_prefect_context

        prefect_ctx = _get_prefect_context()
        flow_run = getattr(prefect_ctx, "flow_run", None)
        task_run = getattr(prefect_ctx, "task_run", None)
        tags = getattr(flow_run, "tags", None) if flow_run is not None else None
        scheduled_start_time = (
            getattr(flow_run, "scheduled_start_time", None) if flow_run is not None else None
        )

        WRAPPER.invoke(
            subcommand=("normalize", "run"),
            arguments=(
                "--config-path",
                str(CONFIG_PATH),
                "--environment",
                environment,
                "--root-path",
                str(ROOT_PATH),
                "--source",
                source,
                "--entity",
                entity,
                "--job-name",
                "prefect-normalize-orders",
            ),
            prefect_context=_prefect_context_from_run(
                flow_run=flow_run,
                task_run=task_run,
                tags=tags,
                scheduled_start_time=scheduled_start_time,
            ),
            timeout_seconds=900.0,
        )

    @task(
        name="sql_compile_and_run",
        retries=1,
        retry_delay_seconds=60,
        cache_key_fn=task_input_hash,
    )
    def sql_compile_and_run(
        *,
        environment: str = "default",
        start_date: str = "2026-01-01",
        end_date: str = "2026-01-31",
        _normalize_done: None = None,
    ) -> None:
        from prefect.context import get_run_context as _get_prefect_context

        prefect_ctx = _get_prefect_context()
        flow_run = getattr(prefect_ctx, "flow_run", None)
        task_run = getattr(prefect_ctx, "task_run", None)
        tags = getattr(flow_run, "tags", None) if flow_run is not None else None
        scheduled_start_time = (
            getattr(flow_run, "scheduled_start_time", None) if flow_run is not None else None
        )
        compile_ctx = _prefect_context_from_run(
            flow_run=flow_run,
            task_run=task_run,
            tags=tags,
            scheduled_start_time=scheduled_start_time,
        )
        if task_run is not None and hasattr(task_run, "task_key"):
            compile_ctx["task_name"] = task_run.task_key + "_compile"

        WRAPPER.invoke(
            subcommand=("sql", "compile"),
            arguments=(
                str(SQL_PACKAGE),
                "--environment",
                environment,
                "--include-deps",
                "--start-date",
                start_date,
                "--end-date",
                end_date,
                "--job-name",
                "prefect-sql-compile",
            ),
            prefect_context=compile_ctx,
            timeout_seconds=300.0,
        )

        run_ctx = _prefect_context_from_run(
            flow_run=flow_run,
            task_run=task_run,
            tags=tags,
            scheduled_start_time=scheduled_start_time,
        )
        if task_run is not None and hasattr(task_run, "task_key"):
            run_ctx["task_name"] = task_run.task_key + "_run"

        WRAPPER.invoke(
            subcommand=("sql", "run"),
            arguments=(
                str(SQL_PACKAGE),
                "--root-path",
                str(ROOT_PATH),
                "--warehouse-root",
                str(WAREHOUSE_ROOT),
                "--environment",
                environment,
                "--include-deps",
                "--start-date",
                start_date,
                "--end-date",
                end_date,
                "--job-name",
                "prefect-sql-run",
            ),
            prefect_context=run_ctx,
            timeout_seconds=1800.0,
        )

    @task(
        name="publish_orders_l5",
        retries=1,
        retry_delay_seconds=60,
        cache_key_fn=task_input_hash,
    )
    def publish_orders_l5(
        *,
        environment: str = "default",
        _sql_done: None = None,
    ) -> None:
        from prefect.context import get_run_context as _get_prefect_context

        prefect_ctx = _get_prefect_context()
        flow_run = getattr(prefect_ctx, "flow_run", None)
        task_run = getattr(prefect_ctx, "task_run", None)
        tags = getattr(flow_run, "tags", None) if flow_run is not None else None
        scheduled_start_time = (
            getattr(flow_run, "scheduled_start_time", None) if flow_run is not None else None
        )
        validate_ctx = _prefect_context_from_run(
            flow_run=flow_run,
            task_run=task_run,
            tags=tags,
            scheduled_start_time=scheduled_start_time,
        )
        if task_run is not None and hasattr(task_run, "task_key"):
            validate_ctx["task_name"] = task_run.task_key + "_validate"

        WRAPPER.invoke(
            subcommand=("publish", "validate"),
            arguments=(
                str(PUBLISH_PACKAGE),
                "--environment",
                environment,
                "--job-name",
                "prefect-publish-validate",
            ),
            prefect_context=validate_ctx,
            timeout_seconds=120.0,
        )

        run_ctx = _prefect_context_from_run(
            flow_run=flow_run,
            task_run=task_run,
            tags=tags,
            scheduled_start_time=scheduled_start_time,
        )
        if task_run is not None and hasattr(task_run, "task_key"):
            run_ctx["task_name"] = task_run.task_key + "_run"

        WRAPPER.invoke(
            subcommand=("publish", "run"),
            arguments=(
                str(PUBLISH_PACKAGE),
                "--root-path",
                str(ROOT_PATH),
                "--warehouse-root",
                str(WAREHOUSE_ROOT),
                "--environment",
                environment,
                "--job-name",
                "prefect-publish-run",
            ),
            prefect_context=run_ctx,
            timeout_seconds=1200.0,
        )

    @flow(
        name="elt_pipeline_daily",
        retries=0,
        persist_result=True,
        result_storage=None,
        tags=["elt-pipeline", "daily", "reference"],
    )
    def elt_pipeline_daily_flow(
        environment: str = "default",
        source: str = "orders_object_storage",
        entity: str = "orders",
        start_date: str = "2026-01-01",
        end_date: str = "2026-01-31",
    ) -> None:
        logger = get_run_logger()
        logger.info(
            "Starting elt_pipeline daily flow: %s %s window=%s→%s",
            source,
            entity,
            start_date,
            end_date,
        )

        ingest_done = ingest_orders_l1(
            environment=environment,
            source=source,
            entity=entity,
        )

        normalize_done = normalize_orders_l2(
            environment=environment,
            source=source,
            entity=entity,
            _ingest_done=ingest_done,
        )

        sql_done = sql_compile_and_run(
            environment=environment,
            start_date=start_date,
            end_date=end_date,
            _normalize_done=normalize_done,
        )

        publish_orders_l5(
            environment=environment,
            _sql_done=sql_done,
        )

        logger.info("elt_pipeline daily flow complete.")

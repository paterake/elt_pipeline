from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations import LineageAdapter, build_lineage_adapter
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import PipelineError, build_error_record
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext, StageName
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.executor import LocalSqlModelExecutor
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    SqlExecutionRecord,
    SqlExecutionResult,
    SqlRunArtifacts,
    SqlStageRunResult,
)


def run_sql_models_locally(
    *,
    root_path: Path,
    run_context: RunContext,
    environment: str,
    package_path: Path,
    database_path: Path,
    compiled_models: list[CompiledSqlModel],
    partition_values: dict[str, str] | None = None,
    extra_values: dict[str, object] | None = None,
    selection_stage: str | None = None,
    selection_domain: str | None = None,
    selection_model: str | None = None,
    include_dependencies: bool = False,
) -> SqlStageRunResult:
    if run_context.stage != StageName.sql:
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.run_context_invalid,
            message="run_sql_models_locally requires a sql stage RunContext",
            context={"stage": run_context.stage.value},
        )

    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
    artifacts = SqlRunArtifacts(
        artifact_root=root_path,
        run_dir=artifact_store.layout.run_dir(run_context=run_context, environment=environment),
    )
    compiled_by_id = {model.model_id: model for model in compiled_models}
    completed_at: datetime | None = None
    status = "success"
    error_summary: dict[str, str] | None = None
    execution_result = SqlExecutionResult(database_path=database_path)
    failure: PipelineError | None = None

    artifacts.log_path = artifact_store.append_log_event(
        run_context=run_context,
        environment=environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="sql",
            event_type="sql_run_start",
            message="SQL run started",
            details={
                "environment": environment,
                "package_path": str(package_path),
                "database_path": str(database_path),
                "model_count": len(compiled_models),
            },
        ),
    )
    artifacts.lineage_path = lineage_adapter.emit(
        run_context=run_context,
        environment=environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="file",
                    name=model.sql_path.as_posix(),
                    facets={
                        "model_id": model.model_id,
                        "manifest_path": model.manifest_path.as_posix(),
                        "stage": model.stage.value,
                        "domain": model.domain,
                        "target_table_name": model.target_table_name,
                    },
                )
                for model in compiled_models
            ],
        ),
    )

    try:
        executor = LocalSqlModelExecutor(
            database_path=database_path,
            partition_values=partition_values,
        )
        execution_result = executor.execute(
            compiled_models,
            execution_observer=_build_observer(
                artifact_store=artifact_store,
                lineage_adapter=lineage_adapter,
                artifacts=artifacts,
                compiled_by_id=compiled_by_id,
                database_path=database_path,
                environment=environment,
                run_context=run_context,
            ),
        )
        completed_at = datetime.now(tz=UTC)
    except PipelineError as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = exc
        partial_execution_result = exc.context.get("execution_result")
        if partial_execution_result is not None:
            execution_result = SqlExecutionResult.model_validate(partial_execution_result)
        error_summary = {
            "error_code": exc.error_code,
            "error_category": exc.error_category.value,
            "message": str(exc),
        }
        artifacts.error_path = artifact_store.append_error_record(
            run_context=run_context,
            environment=environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=exc.error_code,
                error_category=exc.error_category,
                message=str(exc),
                retryable=exc.retryable,
                context=exc.context,
            ),
        )
    finally:
        metrics = MetricsSummary(
            records_written=sum(record.row_count for record in execution_result.executed_models),
            extra={
                "models_executed": execution_result.model_count,
                "validation_models_evaluated": len(execution_result.model_validations),
                "validation_failures": sum(
                    1
                    for summary in execution_result.model_validations
                    for result in summary.validations
                    if not result.passed
                ),
                "database_path": str(database_path),
            },
        )
        for record in execution_result.executed_models:
            metrics.extra[f"model.{record.model_id}.row_count"] = record.row_count

        artifacts.audit_path = artifact_store.write_audit_record(
            run_context=run_context,
            environment=environment,
            audit_record=AuditRecord(
                run_id=run_context.run_id,
                stage=run_context.stage.value,
                job_name=run_context.job_name,
                trigger_type=run_context.trigger_type,
                started_at=run_context.started_at,
                completed_at=completed_at,
                status=status,
                config_version=None,
                metrics_summary=metrics,
                error_summary=error_summary,
                validation_results=[
                    summary.model_dump(mode="json")
                    for summary in execution_result.model_validations
                ],
                context=_build_audit_context(
                    environment=environment,
                    package_path=package_path,
                    database_path=database_path,
                    root_path=root_path,
                    compiled_models=compiled_models,
                    partition_values=partition_values,
                    extra_values=extra_values,
                    selection_stage=selection_stage,
                    selection_domain=selection_domain,
                    selection_model=selection_model,
                    include_dependencies=include_dependencies,
                    run_context=run_context,
                ),
            ),
        )
        artifacts.lineage_path = lineage_adapter.emit(
            run_context=run_context,
            environment=environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE" if status == "success" else "FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
                inputs=[
                    DatasetRef(
                        namespace="file",
                        name=model.sql_path.as_posix(),
                        facets={"model_id": model.model_id},
                    )
                    for model in compiled_models
                ],
                outputs=[
                    DatasetRef(
                        namespace="sqlite",
                        name=record.target_table_name,
                        facets={
                            "model_id": record.model_id,
                            "database_path": str(database_path),
                            "load_mode": record.load_mode.value,
                            "row_count": record.row_count,
                        },
                    )
                    for record in execution_result.executed_models
                ],
            ),
        )
        artifacts.log_path = artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO" if status == "success" else "ERROR",
                component="sql",
                event_type="sql_run_complete",
                message="SQL run completed",
                details={"status": status, "model_count": execution_result.model_count},
            ),
        )

    if failure is not None:
        raise failure

    return SqlStageRunResult(execution_result=execution_result, artifacts=artifacts)


def _build_observer(
    *,
    artifact_store: LocalArtifactStore,
    lineage_adapter: LineageAdapter,
    artifacts: SqlRunArtifacts,
    compiled_by_id: dict[str, CompiledSqlModel],
    database_path: Path,
    environment: str,
    run_context: RunContext,
) -> Callable[[CompiledSqlModel, SqlExecutionRecord], None]:
    def _observe(model: CompiledSqlModel, record: SqlExecutionRecord) -> None:
        artifacts.log_path = artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO",
                component="sql",
                event_type="sql_model_executed",
                message="SQL model executed",
                details={
                    "model_id": model.model_id,
                    "target_table_name": model.target_table_name,
                    "load_mode": model.load_mode.value,
                    "row_count": record.row_count,
                },
            ),
        )
        artifacts.lineage_path = lineage_adapter.emit(
            run_context=run_context,
            environment=environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
                inputs=[
                    DatasetRef(
                        namespace="sqlite",
                        name=compiled_by_id[dependency_id].target_table_name,
                        facets={"model_id": dependency_id},
                    )
                    if dependency_id in compiled_by_id
                    else DatasetRef(
                        namespace="sql_model",
                        name=dependency_id,
                        facets={"model_id": dependency_id},
                    )
                    for dependency_id in model.depends_on
                ],
                outputs=[
                    DatasetRef(
                        namespace="sqlite",
                        name=record.target_table_name,
                        facets={
                            "model_id": model.model_id,
                            "database_path": str(database_path),
                            "stage": model.stage.value,
                            "domain": model.domain,
                            "load_mode": model.load_mode.value,
                            "row_count": record.row_count,
                        },
                    )
                ],
            ),
        )

    return _observe


def _build_audit_context(
    *,
    environment: str,
    package_path: Path,
    database_path: Path,
    root_path: Path,
    compiled_models: list[CompiledSqlModel],
    partition_values: dict[str, str] | None,
    extra_values: dict[str, object] | None,
    selection_stage: str | None,
    selection_domain: str | None,
    selection_model: str | None,
    include_dependencies: bool,
    run_context: RunContext,
) -> dict[str, str]:
    context = {
        "environment": environment,
        "package_path": str(package_path),
        "database_path": str(database_path),
        "artifact_root": str(root_path),
        "model_count": str(len(compiled_models)),
        "selected_models": ",".join(model.model_id for model in compiled_models),
        "selection_stage": selection_stage or "",
        "selection_domain": selection_domain or "",
        "selection_model": selection_model or "",
        "include_dependencies": json.dumps(include_dependencies),
    }
    if partition_values:
        context["partition_values"] = json.dumps(partition_values, sort_keys=True)
    if extra_values:
        context["extra_values"] = json.dumps(extra_values, sort_keys=True)
    start_date = next(
        (
            model.token_values["window.start_date"]
            for model in compiled_models
            if model.token_values.get("window.start_date")
        ),
        None,
    )
    end_date = next(
        (
            model.token_values["window.end_date"]
            for model in compiled_models
            if model.token_values.get("window.end_date")
        ),
        None,
    )
    if start_date is not None:
        context["window_start"] = start_date
    if end_date is not None:
        context["window_end"] = end_date
    rerun_of_run_id = run_context.attributes.get("rerun_of_run_id")
    if isinstance(rerun_of_run_id, str) and rerun_of_run_id:
        context["rerun_of_run_id"] = rerun_of_run_id
    return context

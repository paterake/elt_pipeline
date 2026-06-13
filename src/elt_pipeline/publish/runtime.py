from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.publish.models import (
    DiscoveredPublishDefinition,
    PublishArtifactRecord,
    PublishOutputFormat,
    PublishOutputManifest,
    PublishReplacementMode,
    PublishRunArtifacts,
    PublishRunResult,
    PublishSelectionMode,
    PublishStageRunResult,
    PublishValidationResult,
)
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext, StageName


def explain_publish_definitions(
    *,
    root_path: Path,
    run_context: RunContext,
    definitions: list[DiscoveredPublishDefinition],
) -> list[dict[str, object]]:
    return [
        {
            "publish_id": definition.publish_id,
            "manifest_path": str(definition.manifest_path),
            "query_path": str(definition.query_path) if definition.query_path is not None else None,
            "source_dataset": definition.manifest.source.dataset,
            "selection_mode": definition.manifest.source.selection_mode.value,
            "output_format": definition.manifest.delivery.output_format.value,
            "replacement_mode": definition.manifest.delivery.replacement_mode.value,
            "run_scoped_path": str(_resolve_run_scoped_output_path(root_path, run_context, definition)),
            "stable_delivery_path": (
                str(_resolve_stable_delivery_path(root_path, definition))
                if _supports_stable_delivery(definition.manifest.delivery.replacement_mode)
                else None
            ),
        }
        for definition in definitions
    ]


def run_publish_definitions_locally(
    *,
    root_path: Path,
    run_context: RunContext,
    environment: str,
    package_path: Path,
    database_path: Path,
    definitions: list[DiscoveredPublishDefinition],
) -> PublishStageRunResult:
    if run_context.stage != StageName.publish:
        raise PipelineError(
            message="run_publish_definitions_locally requires a publish stage RunContext",
            error_code="PUBLISH_RUN_CONTEXT_INVALID",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"stage": run_context.stage.value},
        )

    artifact_store = LocalArtifactStore(root_path)
    artifacts = PublishRunArtifacts(
        artifact_root=root_path,
        run_dir=artifact_store.layout.run_dir(run_context=run_context, environment=environment),
    )
    results: list[PublishRunResult] = []
    completed_at: datetime | None = None
    status = "success"
    error_summary: dict[str, str] | None = None
    failure: PipelineError | None = None

    artifacts.log_path = artifact_store.append_log_event(
        run_context=run_context,
        environment=environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="publish",
            event_type="publish_run_start",
            message="Publish run started",
            details={
                "environment": environment,
                "package_path": str(package_path),
                "database_path": str(database_path),
                "publish_count": len(definitions),
            },
        ),
    )
    artifacts.lineage_path = artifact_store.append_lineage_event(
        run_context=run_context,
        environment=environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="sqlite",
                    name=definition.manifest.source.dataset,
                    facets={
                        "publish_id": definition.publish_id,
                        "manifest_path": str(definition.manifest_path),
                    },
                )
                for definition in definitions
            ],
        ),
    )

    try:
        with sqlite3.connect(database_path) as connection:
            connection.row_factory = sqlite3.Row
            for definition in definitions:
                result = _run_single_publish_definition(
                    connection=connection,
                    root_path=root_path,
                    run_context=run_context,
                    environment=environment,
                    artifact_store=artifact_store,
                    definition=definition,
                )
                results.append(result)
                artifacts.export_manifest_path = result.artifacts[0].run_scoped_path.parent / "manifest.json"
        completed_at = datetime.now(tz=UTC)
    except PipelineError as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = exc
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
            records_written=sum(result.row_count for result in results),
            files_written=sum(len(result.artifacts) for result in results),
            extra={
                "publish_count": len(results),
                "database_path": str(database_path),
            },
        )
        for result in results:
            metrics.extra[f"publish.{result.publish_id}.row_count"] = result.row_count

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
                    {
                        "publish_id": result.publish_id,
                        "validations": [
                            validation.model_dump(mode="json") for validation in result.validations
                        ],
                    }
                    for result in results
                ],
                context={
                    "environment": environment,
                    "package_path": str(package_path),
                    "database_path": str(database_path),
                    "artifact_root": str(root_path),
                    "publish_count": str(len(definitions)),
                    "selected_publish_ids": ",".join(
                        definition.publish_id for definition in definitions
                    ),
                },
            ),
        )
        artifacts.lineage_path = artifact_store.append_lineage_event(
            run_context=run_context,
            environment=environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE" if status == "success" else "FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
                inputs=[
                    DatasetRef(
                        namespace="sqlite",
                        name=definition.manifest.source.dataset,
                        facets={"publish_id": definition.publish_id},
                    )
                    for definition in definitions
                ],
                outputs=[
                    DatasetRef(
                        namespace="file",
                        name=artifact.run_scoped_path.as_posix(),
                        facets={
                            "publish_id": result.publish_id,
                            "output_format": artifact.output_format.value,
                            "row_count": artifact.row_count,
                            "stable_delivery_path": (
                                artifact.stable_delivery_path.as_posix()
                                if artifact.stable_delivery_path is not None
                                else ""
                            ),
                        },
                    )
                    for result in results
                    for artifact in result.artifacts
                ],
            ),
        )
        artifacts.log_path = artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO" if status == "success" else "ERROR",
                component="publish",
                event_type="publish_run_complete",
                message="Publish run completed",
                details={"status": status, "publish_count": len(results)},
            ),
        )

    if failure is not None:
        raise failure

    return PublishStageRunResult(results=results, artifacts=artifacts)


def _run_single_publish_definition(
    *,
    connection: sqlite3.Connection,
    root_path: Path,
    run_context: RunContext,
    environment: str,
    artifact_store: LocalArtifactStore,
    definition: DiscoveredPublishDefinition,
) -> PublishRunResult:
    if definition.manifest.delivery.output_format != PublishOutputFormat.csv:
        raise ConfigValidationError(
            message="The first publish runtime slice only supports CSV output",
            context={
                "publish_id": definition.publish_id,
                "output_format": definition.manifest.delivery.output_format.value,
            },
        )
    if definition.manifest.delivery.replacement_mode not in {
        PublishReplacementMode.versioned_delivery,
        PublishReplacementMode.overwrite_in_place,
    }:
        raise ConfigValidationError(
            message=(
                "The first publish runtime slice only supports versioned_delivery "
                "and overwrite_in_place replacement modes"
            ),
            context={
                "publish_id": definition.publish_id,
                "replacement_mode": definition.manifest.delivery.replacement_mode.value,
            },
        )

    sql_text = _build_publish_sql(definition)
    cursor = connection.execute(sql_text)
    column_names = [str(description[0]) for description in cursor.description or []]
    rows = cursor.fetchall()
    validations = _validate_publish_output(definition=definition, column_names=column_names, rows=rows)

    run_scoped_path = _resolve_run_scoped_output_path(root_path, run_context, definition)
    stable_delivery_path = (
        _resolve_stable_delivery_path(root_path, definition)
        if _supports_stable_delivery(definition.manifest.delivery.replacement_mode)
        else None
    )
    run_scoped_path.parent.mkdir(parents=True, exist_ok=True)

    with run_scoped_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=column_names)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    checksum_sha256 = hashlib.sha256(run_scoped_path.read_bytes()).hexdigest()
    file_size_bytes = run_scoped_path.stat().st_size

    if stable_delivery_path is not None:
        stable_delivery_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(run_scoped_path, stable_delivery_path)

    artifact = PublishArtifactRecord(
        output_format=definition.manifest.delivery.output_format,
        run_scoped_path=run_scoped_path,
        stable_delivery_path=stable_delivery_path,
        file_size_bytes=file_size_bytes,
        row_count=len(rows),
        checksum_sha256=checksum_sha256,
    )
    manifest_path = run_scoped_path.parent / "manifest.json"
    output_manifest = PublishOutputManifest(
        run_id=run_context.run_id,
        publish_name=definition.manifest.name,
        publish_version=definition.manifest.version,
        source_stage=definition.manifest.source.stage,
        source_dataset=definition.manifest.source.dataset,
        execution_window={
            "start": _string_or_none(run_context.attributes.get("window_start")),
            "end": _string_or_none(run_context.attributes.get("window_end")),
            "label": _string_or_none(run_context.attributes.get("window_label")),
        },
        output_format=definition.manifest.delivery.output_format,
        replacement_mode=definition.manifest.delivery.replacement_mode,
        produced_at=datetime.now(tz=UTC).isoformat(),
        owning_domain=definition.manifest.owner.owning_domain,
        owner_team=definition.manifest.owner.owner_team,
        consumer_label=definition.manifest.consumer_label,
        delivery_purpose=definition.manifest.delivery_purpose,
        validation_results=validations,
        artifacts=[artifact],
    )
    manifest_path.write_text(
        json.dumps(output_manifest.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    artifact_store.append_log_event(
        run_context=run_context,
        environment=environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="publish",
            event_type="publish_definition_completed",
            message="Publish definition completed",
            details={
                "publish_id": definition.publish_id,
                "row_count": len(rows),
                "run_scoped_path": str(run_scoped_path),
                "stable_delivery_path": (
                    str(stable_delivery_path) if stable_delivery_path is not None else ""
                ),
            },
        ),
    )
    artifact_store.append_lineage_event(
        run_context=run_context,
        environment=environment,
        lineage_event=LineageEvent(
            event_type="COMPLETE",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="sqlite",
                    name=definition.manifest.source.dataset,
                    facets={"publish_id": definition.publish_id},
                )
            ],
            outputs=[
                DatasetRef(
                    namespace="file",
                    name=run_scoped_path.as_posix(),
                    facets={
                        "publish_id": definition.publish_id,
                        "manifest_path": manifest_path.as_posix(),
                        "output_format": definition.manifest.delivery.output_format.value,
                        "row_count": len(rows),
                    },
                )
            ],
        ),
    )
    return PublishRunResult(
        publish_id=definition.publish_id,
        row_count=len(rows),
        artifacts=[artifact],
        validations=validations,
    )


def _validate_publish_output(
    *,
    definition: DiscoveredPublishDefinition,
    column_names: list[str],
    rows: list[sqlite3.Row],
) -> list[PublishValidationResult]:
    validations: list[PublishValidationResult] = []
    missing_required_columns = [
        column
        for column in definition.manifest.validation.required_columns
        if column not in column_names
    ]
    validations.append(
        PublishValidationResult(
            validation_type="required_columns",
            passed=not missing_required_columns,
            message=(
                None
                if not missing_required_columns
                else f"Missing required columns: {', '.join(missing_required_columns)}"
            ),
        )
    )
    validations.append(
        PublishValidationResult(
            validation_type="non_empty",
            passed=(len(rows) > 0) or (not definition.manifest.validation.require_non_empty),
            message=(
                None
                if len(rows) > 0 or not definition.manifest.validation.require_non_empty
                else "Publish result set must not be empty"
            ),
        )
    )

    failed = [validation for validation in validations if not validation.passed]
    if failed:
        raise PipelineError(
            message="Publish output validation failed",
            error_code="PUBLISH_VALIDATION_FAILED",
            error_category=ErrorCategory.validation_error,
            retryable=False,
            context={
                "publish_id": definition.publish_id,
                "validation_errors": [validation.model_dump(mode="json") for validation in failed],
            },
        )
    return validations


def _build_publish_sql(definition: DiscoveredPublishDefinition) -> str:
    if definition.manifest.source.selection_mode == PublishSelectionMode.direct:
        selected_columns = ", ".join(definition.manifest.columns) if definition.manifest.columns else "*"
        return f"select {selected_columns} from {definition.manifest.source.dataset}"
    if not definition.query_text:
        raise ConfigValidationError(
            message="Query-based publish definitions require query.sql",
            context={"publish_id": definition.publish_id},
        )
    return definition.query_text


def _resolve_run_scoped_output_path(
    root_path: Path,
    run_context: RunContext,
    definition: DiscoveredPublishDefinition,
) -> Path:
    rendered_relative_path = _render_output_path_template(run_context, definition)
    return (
        root_path
        / "artifacts"
        / "level5"
        / rendered_relative_path.parent
        / f"run_id={run_context.run_id}"
        / rendered_relative_path.name
    )


def _resolve_stable_delivery_path(
    root_path: Path,
    definition: DiscoveredPublishDefinition,
) -> Path:
    return root_path / "artifacts" / "level5" / _render_output_path_template(None, definition)


def _render_output_path_template(
    run_context: RunContext | None,
    definition: DiscoveredPublishDefinition,
) -> Path:
    window_label = (
        _string_or_none(run_context.attributes.get("window_label"))
        if run_context is not None
        else None
    ) or "open_window"
    rendered = definition.manifest.delivery.path_template.format(
        domain=definition.manifest.domain,
        publish_name=definition.manifest.name,
        run_id=run_context.run_id if run_context is not None else "stable",
        window_label=window_label,
        output_extension=definition.manifest.delivery.output_format.value,
    )
    return Path(rendered)


def _supports_stable_delivery(replacement_mode: PublishReplacementMode) -> bool:
    return replacement_mode == PublishReplacementMode.overwrite_in_place


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)

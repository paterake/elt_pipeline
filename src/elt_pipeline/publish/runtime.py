from __future__ import annotations

import csv
import hashlib
import json
import os
import posixpath
import shutil
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import Row, SparkSession

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations import LineageAdapter, build_lineage_adapter
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
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_basename,
    path_mkdir,
    path_parent,
    path_read_bytes,
    path_with_suffix,
    path_write_bytes,
    path_write_text,
    strip_file_scheme,
)
from elt_pipeline.shared.runtime import RunContext, StageName
from elt_pipeline.sql.models import SqlModelStage
from elt_pipeline.sql.spark_executor import _iceberg_table_fq, _is_iceberg_enabled

_PUBLISH_MAX_ROWS_ENV_VAR = "ELT_PIPELINE_PUBLISH_MAX_ROWS"
_PUBLISH_MAX_ROWS_DEFAULT = 1_000_000


def _resolve_publish_max_rows() -> int:
    raw = os.environ.get(_PUBLISH_MAX_ROWS_ENV_VAR)
    if raw is None:
        return _PUBLISH_MAX_ROWS_DEFAULT
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigValidationError(
            message=f"{_PUBLISH_MAX_ROWS_ENV_VAR} must be a positive integer",
            context={"value": raw},
        ) from exc
    if value <= 0:
        raise ConfigValidationError(
            message=f"{_PUBLISH_MAX_ROWS_ENV_VAR} must be a positive integer",
            context={"value": raw},
        )
    return value


def _enforce_publish_row_ceiling(
    *,
    publish_id: str,
    row_count: int,
    max_rows: int,
) -> None:
    if row_count > max_rows:
        raise PipelineError(
            message=(
                "Publish result set exceeds configured max rows ceiling "
                f"({row_count} > {max_rows}). Increase {_PUBLISH_MAX_ROWS_ENV_VAR} "
                "or narrow the publish query."
            ),
            error_code="PUBLISH_ROWS_EXCEED_CEILING",
            error_category=ErrorCategory.validation_error,
            retryable=False,
            context={
                "publish_id": publish_id,
                "row_count": row_count,
                "max_rows": max_rows,
                "env_var": _PUBLISH_MAX_ROWS_ENV_VAR,
            },
        )


def explain_publish_definitions(
    *,
    root_path: str,
    run_context: RunContext,
    definitions: list[DiscoveredPublishDefinition],
) -> list[dict[str, object]]:
    plans: list[dict[str, object]] = []
    for definition in definitions:
        run_scoped_path = _resolve_run_scoped_output_path(root_path, run_context, definition)
        stable_delivery_path = (
            _resolve_stable_delivery_path(root_path, run_context, definition)
            if _supports_stable_delivery_copy(definition.manifest.delivery.replacement_mode)
            else None
        )
        payload: dict[str, object] = {
            "publish_id": definition.publish_id,
            "manifest_path": str(definition.manifest_path),
            "query_path": str(definition.query_path) if definition.query_path is not None else None,
            "source_dataset": definition.manifest.source.dataset,
            "selection_mode": definition.manifest.source.selection_mode.value,
            "output_format": definition.manifest.delivery.output_format.value,
            "replacement_mode": definition.manifest.delivery.replacement_mode.value,
            "run_scoped_path": run_scoped_path,
            "stable_delivery_path": (
                stable_delivery_path if stable_delivery_path is not None else None
            ),
        }
        packaging = definition.manifest.delivery.packaging
        if packaging is not None and packaging.archive_format is not None:
            archive_extension = packaging.archive_format.value
            archive_run_scoped_path = path_with_suffix(
                run_scoped_path, f".{archive_extension}"
            )
            payload["archive_run_scoped_path"] = archive_run_scoped_path
            payload["archive_stable_delivery_path"] = (
                path_with_suffix(stable_delivery_path, f".{archive_extension}")
                if stable_delivery_path is not None
                else None
            )
        plans.append(payload)
    return plans


def run_publish_definitions_locally(
    *,
    root_path: str,
    run_context: RunContext,
    environment: str,
    package_path: Path,
    warehouse_root: str,
    spark: SparkSession,
    definitions: list[DiscoveredPublishDefinition],
    serving_endpoint: dict[str, object] | None = None,
) -> PublishStageRunResult:
    if run_context.stage != StageName.publish:
        raise PipelineError(
            message="run_publish_definitions_locally requires a publish stage RunContext",
            error_code="PUBLISH_RUN_CONTEXT_INVALID",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"stage": run_context.stage.value},
        )

    use_iceberg = _is_iceberg_enabled(spark)
    source_namespace = "iceberg" if use_iceberg else "spark_parquet"

    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
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
                "warehouse_root": warehouse_root,
                "publish_count": len(definitions),
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
                    namespace=source_namespace,
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
        for definition in definitions:
            result = _run_single_publish_definition(
                spark=spark,
                warehouse_root=warehouse_root,
                root_path=root_path,
                run_context=run_context,
                environment=environment,
                artifact_store=artifact_store,
                lineage_adapter=lineage_adapter,
                definition=definition,
            )
            results.append(result)
            artifacts.export_manifest_path = join_paths(
                path_parent(result.artifacts[0].run_scoped_path), "manifest.json"
            )
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
                "warehouse_root": warehouse_root,
            },
        )
        for result in results:
            metrics.extra[f"publish.{result.publish_id}.row_count"] = result.row_count

        publish_audit_context: dict[str, str] = {
            "environment": environment,
            "package_path": str(package_path),
            "warehouse_root": warehouse_root,
            "artifact_root": root_path,
            "publish_count": str(len(definitions)),
            "selected_publish_ids": ",".join(
                definition.publish_id for definition in definitions
            ),
            "window_start": (
                _string_or_none(run_context.attributes.get("window_start")) or ""
            ),
            "window_end": (
                _string_or_none(run_context.attributes.get("window_end")) or ""
            ),
            "window_label": (
                _string_or_none(run_context.attributes.get("window_label")) or ""
            ),
            "checkpoint_mode": _string_or_none(
                run_context.attributes.get("checkpoint_mode")
            )
            or "",
            "rerun_of_run_id": _string_or_none(
                run_context.attributes.get("rerun_of_run_id")
            )
            or "",
            "export_manifest_paths": json.dumps(
                [
                    join_paths(
                        path_parent(result.artifacts[0].run_scoped_path),
                        "manifest.json",
                    )
                    for result in results
                ]
            ),
            "run_scoped_artifact_paths": json.dumps(
                [
                    artifact.run_scoped_path
                    for result in results
                    for artifact in result.artifacts
                ]
            ),
        }
        if serving_endpoint is not None:
            publish_audit_context["serving_endpoint"] = json.dumps(
                serving_endpoint, sort_keys=True
            )
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
                context=publish_audit_context,
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
                        namespace=source_namespace,
                        name=definition.manifest.source.dataset,
                        facets={"publish_id": definition.publish_id},
                    )
                    for definition in definitions
                ],
                outputs=[
                    DatasetRef(
                        namespace="file",
                        name=artifact.run_scoped_path,
                        facets={
                            "publish_id": result.publish_id,
                            "output_format": artifact.output_format.value,
                            "row_count": artifact.row_count,
                            "stable_delivery_path": (
                                artifact.stable_delivery_path
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


def _register_level4_source(
    *,
    spark: SparkSession,
    warehouse_root: str,
    definition: DiscoveredPublishDefinition,
) -> None:
    use_iceberg = _is_iceberg_enabled(spark)
    dataset = definition.manifest.source.dataset
    if use_iceberg:
        stage_str = definition.manifest.source.stage
        stage = (
            SqlModelStage(stage_str)
            if stage_str in {s.value for s in SqlModelStage}
            else SqlModelStage.level4
        )
        domain = definition.manifest.domain
        fq = _iceberg_table_fq(stage=stage, domain=domain, name=dataset)
        dataframe = spark.table(fq)
    else:
        stage = definition.manifest.source.stage
        dataset_path = join_paths(warehouse_root, stage, dataset)
        dataframe = spark.read.parquet(dataset_path)
    dataframe.createOrReplaceTempView(dataset)


def _run_single_publish_definition(
    *,
    spark: SparkSession,
    warehouse_root: str,
    root_path: str,
    run_context: RunContext,
    environment: str,
    artifact_store: LocalArtifactStore,
    lineage_adapter: LineageAdapter,
    definition: DiscoveredPublishDefinition,
) -> PublishRunResult:
    use_iceberg = _is_iceberg_enabled(spark)
    source_namespace = "iceberg" if use_iceberg else "spark_parquet"
    if definition.manifest.delivery.output_format not in {
        PublishOutputFormat.csv,
        PublishOutputFormat.jsonl,
        PublishOutputFormat.tsv,
    }:
        raise ConfigValidationError(
            message="The current publish runtime only supports csv, jsonl, and tsv outputs",
            context={
                "publish_id": definition.publish_id,
                "output_format": definition.manifest.delivery.output_format.value,
            },
        )
    if definition.manifest.delivery.replacement_mode not in {
        PublishReplacementMode.versioned_delivery,
        PublishReplacementMode.overwrite_in_place,
        PublishReplacementMode.append_new_artifact,
    }:
        raise ConfigValidationError(
            message=(
                "The current publish runtime only supports versioned_delivery, "
                "overwrite_in_place, and append_new_artifact replacement modes"
            ),
            context={
                "publish_id": definition.publish_id,
                "replacement_mode": definition.manifest.delivery.replacement_mode.value,
            },
        )

    _register_level4_source(spark=spark, warehouse_root=warehouse_root, definition=definition)
    sql_text = _build_publish_sql(definition)
    result_df = spark.sql(sql_text)
    column_names = result_df.columns

    row_count = result_df.count()
    max_rows = _resolve_publish_max_rows()
    _enforce_publish_row_ceiling(
        publish_id=definition.publish_id,
        row_count=row_count,
        max_rows=max_rows,
    )

    validations = _validate_publish_output(
        definition=definition,
        column_names=column_names,
        row_count=row_count,
    )

    run_scoped_path = _resolve_run_scoped_output_path(root_path, run_context, definition)
    stable_delivery_path = (
        _resolve_stable_delivery_path(root_path, run_context, definition)
        if _supports_stable_delivery_copy(definition.manifest.delivery.replacement_mode)
        else None
    )
    path_mkdir(path_parent(run_scoped_path), parents=True, exist_ok=True)

    # -- Publish sink boundary (accepted per OD-P1 2026-08-15) --
    # This is an intentional driver-side step, not a Spark-executor-parallel step.
    # Delivery contract for publish artifacts is a SINGLE file (.csv/.tsv/.jsonl)
    # with a sha256 checksum over the whole file, plus optional archive packaging.
    # Spark's df.write.<fmt>() produces part-* directories, which is the wrong
    # output shape for an external-consumer delivery artifact. We mitigate the
    # driver-heap spike from the old result_df.collect() by streaming partition
    # by partition via toLocalIterator() (vs. full materialization), and we
    # enforce a row-count ceiling via _enforce_publish_row_ceiling() above to
    # fail fast rather than silently OOM on an oversized publish result set.
    row_iterator: Iterator[Row] = result_df.toLocalIterator()
    rows_written = _write_publish_output(
        output_path=run_scoped_path,
        output_format=definition.manifest.delivery.output_format,
        column_names=column_names,
        rows=row_iterator,
    )
    if rows_written != row_count:
        raise PipelineError(
            message=(
                f"Publish row count mismatch: Spark reported {row_count} rows "
                f"but write pass wrote {rows_written} rows"
            ),
            error_code="PUBLISH_ROW_COUNT_MISMATCH",
            error_category=ErrorCategory.processing_error,
            retryable=True,
            context={
                "publish_id": definition.publish_id,
                "spark_count": row_count,
                "written_count": rows_written,
            },
        )

    checksum_sha256 = hashlib.sha256(path_read_bytes(run_scoped_path)).hexdigest()
    file_size_bytes = _file_size_bytes(run_scoped_path)

    if stable_delivery_path is not None:
        path_mkdir(path_parent(stable_delivery_path), parents=True, exist_ok=True)
        _copy_file(run_scoped_path, stable_delivery_path)

    artifacts: list[PublishArtifactRecord] = [
        PublishArtifactRecord(
            output_format=definition.manifest.delivery.output_format,
            run_scoped_path=run_scoped_path,
            stable_delivery_path=stable_delivery_path,
            file_size_bytes=file_size_bytes,
            row_count=row_count,
            checksum_sha256=checksum_sha256,
        )
    ]

    if (
        definition.manifest.delivery.packaging is not None
        and definition.manifest.delivery.packaging.archive_format is not None
    ):
        archive_extension = definition.manifest.delivery.packaging.archive_format.value
        archive_run_scoped_path = path_with_suffix(
            run_scoped_path, f".{archive_extension}"
        )
        archive_stable_delivery_path = (
            path_with_suffix(stable_delivery_path, f".{archive_extension}")
            if stable_delivery_path is not None
            else None
        )
        with zipfile.ZipFile(
            _local_path_or_exception(archive_run_scoped_path),
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive_handle:
            archive_handle.write(
                _local_path_or_exception(run_scoped_path),
                arcname=path_basename(run_scoped_path),
            )

        archive_checksum_sha256 = hashlib.sha256(
            path_read_bytes(archive_run_scoped_path)
        ).hexdigest()
        archive_file_size_bytes = _file_size_bytes(archive_run_scoped_path)

        if archive_stable_delivery_path is not None:
            path_mkdir(
                path_parent(archive_stable_delivery_path), parents=True, exist_ok=True
            )
            _copy_file(archive_run_scoped_path, archive_stable_delivery_path)

        artifacts.append(
            PublishArtifactRecord(
                output_format=PublishOutputFormat.zip,
                run_scoped_path=archive_run_scoped_path,
                stable_delivery_path=archive_stable_delivery_path,
                file_size_bytes=archive_file_size_bytes,
                row_count=row_count,
                checksum_sha256=archive_checksum_sha256,
            )
        )
    manifest_path = join_paths(path_parent(run_scoped_path), "manifest.json")
    output_manifest = PublishOutputManifest(
        run_id=run_context.run_id,
        rerun_of_run_id=_string_or_none(run_context.attributes.get("rerun_of_run_id")),
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
        artifacts=artifacts,
    )
    path_write_text(
        manifest_path,
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
                "row_count": row_count,
                "run_scoped_path": run_scoped_path,
                "stable_delivery_path": (
                    stable_delivery_path if stable_delivery_path is not None else ""
                ),
                "packaged_artifact_count": len(artifacts),
            },
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=environment,
        lineage_event=LineageEvent(
            event_type="COMPLETE",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace=source_namespace,
                    name=definition.manifest.source.dataset,
                    facets={"publish_id": definition.publish_id},
                )
            ],
            outputs=[
                DatasetRef(
                    namespace="file",
                    name=run_scoped_path,
                    facets={
                        "publish_id": definition.publish_id,
                        "manifest_path": manifest_path,
                        "output_format": definition.manifest.delivery.output_format.value,
                        "row_count": row_count,
                    },
                )
            ]
            + [
                DatasetRef(
                    namespace="file",
                    name=artifact.run_scoped_path,
                    facets={
                        "publish_id": definition.publish_id,
                        "manifest_path": manifest_path,
                        "output_format": artifact.output_format.value,
                        "row_count": artifact.row_count,
                        "stable_delivery_path": (
                            artifact.stable_delivery_path
                            if artifact.stable_delivery_path is not None
                            else ""
                        ),
                    },
                )
                for artifact in artifacts
                if artifact.run_scoped_path != run_scoped_path
            ],
        ),
    )
    return PublishRunResult(
        publish_id=definition.publish_id,
        row_count=row_count,
        artifacts=artifacts,
        validations=validations,
    )


def _file_size_bytes(path_str: str) -> int:
    scheme = detect_scheme(path_str)
    if scheme in (_StorageScheme.file, _StorageScheme.local_unschemed):
        try:
            return os.stat(strip_file_scheme(path_str)).st_size
        except OSError:
            return 0
    try:
        return len(path_read_bytes(path_str))
    except PipelineError:
        return 0


def _local_path_or_exception(path_str: str) -> str:
    scheme = detect_scheme(path_str)
    if scheme not in (_StorageScheme.file, _StorageScheme.local_unschemed):
        raise ConfigValidationError(
            message="Publish runtime (zip/csv write via local Python tools) requires "
            "POSIX/local file:// or unschemed paths. Configure a POSIX root_path for publish "
            "or defer packaging to a delivery layer.",
            context={
                "path": path_str,
                "scheme": scheme.value if scheme else "unknown",
            },
        )
    return strip_file_scheme(path_str)


def _copy_file(src: str, dst: str) -> None:
    src_scheme = detect_scheme(src)
    dst_scheme = detect_scheme(dst)
    if (
        src_scheme in (_StorageScheme.file, _StorageScheme.local_unschemed)
        and dst_scheme in (_StorageScheme.file, _StorageScheme.local_unschemed)
    ):
        shutil.copyfile(strip_file_scheme(src), strip_file_scheme(dst))
        return
    data = path_read_bytes(src)
    path_write_bytes(dst, data, atomic=True)


def _validate_publish_output(
    *,
    definition: DiscoveredPublishDefinition,
    column_names: list[str],
    row_count: int,
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
            passed=(row_count > 0) or (not definition.manifest.validation.require_non_empty),
            message=(
                None
                if row_count > 0 or not definition.manifest.validation.require_non_empty
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


def _write_publish_output(
    *,
    output_path: str,
    output_format: PublishOutputFormat,
    column_names: list[str],
    rows: Iterator[Row],
) -> int:
    local_output_path = _local_path_or_exception(output_path)
    rows_written = 0

    if output_format == PublishOutputFormat.csv:
        with open(local_output_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=column_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(_row_to_serializable_mapping(row=row, column_names=column_names))
                rows_written += 1
        return rows_written

    if output_format == PublishOutputFormat.tsv:
        with open(local_output_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=column_names, delimiter="\t")
            writer.writeheader()
            for row in rows:
                writer.writerow(_row_to_serializable_mapping(row=row, column_names=column_names))
                rows_written += 1
        return rows_written

    if output_format == PublishOutputFormat.jsonl:
        with open(local_output_path, "w", encoding="utf-8", newline="") as handle:
            for row in rows:
                handle.write(
                    json.dumps(
                        _row_to_serializable_mapping(row=row, column_names=column_names),
                        default=str,
                    )
                )
                handle.write("\n")
                rows_written += 1
        return rows_written

    raise ConfigValidationError(
        message="Unsupported publish output format",
        context={"output_format": output_format.value},
    )


def _row_to_serializable_mapping(
    *,
    row: Row,
    column_names: list[str],
) -> dict[str, object]:
    return {column_name: row[column_name] for column_name in column_names}


def _build_publish_sql(definition: DiscoveredPublishDefinition) -> str:
    if definition.manifest.source.selection_mode == PublishSelectionMode.direct:
        selected_columns = (
            ", ".join(definition.manifest.columns) if definition.manifest.columns else "*"
        )
        return f"select {selected_columns} from {definition.manifest.source.dataset}"
    if not definition.query_text:
        raise ConfigValidationError(
            message="Query-based publish definitions require query.sql",
            context={"publish_id": definition.publish_id},
        )
    return definition.query_text


def _resolve_run_scoped_output_path(
    root_path: str,
    run_context: RunContext,
    definition: DiscoveredPublishDefinition,
) -> str:
    rendered_relative_parent, rendered_relative_name = _render_output_path_template_parts(
        run_context, definition
    )
    return join_paths(
        root_path,
        "artifacts",
        "level5",
        rendered_relative_parent,
        f"run_id={run_context.run_id}",
        rendered_relative_name,
    )


def _resolve_stable_delivery_path(
    root_path: str,
    run_context: RunContext,
    definition: DiscoveredPublishDefinition,
) -> str:
    parent, name = _render_output_path_template_parts(run_context, definition)
    if definition.manifest.delivery.replacement_mode == PublishReplacementMode.append_new_artifact:
        stem, ext = posixpath.splitext(name)
        delivery_name = f"{stem}.run_id={run_context.run_id}{ext}"
    else:
        delivery_name = name
    return join_paths(root_path, "artifacts", "level5", parent, delivery_name)


def _render_output_path_template_parts(
    run_context: RunContext,
    definition: DiscoveredPublishDefinition,
) -> tuple[str, str]:
    window_label = (
        _string_or_none(run_context.attributes.get("window_label")) or "open_window"
    )
    rendered = definition.manifest.delivery.path_template.format(
        domain=definition.manifest.domain,
        publish_name=definition.manifest.name,
        run_id=run_context.run_id,
        window_label=window_label,
        output_extension=definition.manifest.delivery.output_format.value,
    )
    normalized = rendered.replace("\\", "/")
    parent = posixpath.dirname(normalized) or ""
    name = posixpath.basename(normalized) or "output"
    return parent, name


def _supports_stable_delivery_copy(replacement_mode: PublishReplacementMode) -> bool:
    return replacement_mode in {
        PublishReplacementMode.overwrite_in_place,
        PublishReplacementMode.append_new_artifact,
    }


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.integrations import (
    QualityDatasetRef,
    QualityHookAdapter,
    QualityHookRequest,
    QualityHookSummary,
    build_lineage_adapter,
    build_quality_hook,
    quality_error_already_recorded,
    raise_for_blocking_quality_failures,
)
from elt_pipeline.normalize.level2_storage import SparkLevel2Writer
from elt_pipeline.normalize.models import Level2WriteSummary
from elt_pipeline.normalize.partitioning import PartitionStrategy
from elt_pipeline.normalize.runner import NormalizationRunner
from elt_pipeline.normalize.storage import LocalMappingCatalogStore
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import PipelineError, build_error_record
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.path_utils import path_relative_to
from elt_pipeline.shared.runtime import RunContext, StageName


def _load_payload(payload: Any) -> Any:
    if isinstance(payload, (bytes, bytearray, memoryview, str, dict, list)):
        return payload
    if hasattr(payload, "read_bytes"):
        return payload.read_bytes()
    return payload


def normalize_level1_to_local_level2(
    *,
    root_path: str,
    run_context: RunContext,
    manifest: Level1ArtifactManifest,
    payload: Any,
    spark: SparkSession,
    partition_strategy: PartitionStrategy | None = None,
    normalization_runner: NormalizationRunner | None = None,
    quality_hook: QualityHookAdapter | None = None,
) -> Level2WriteSummary:
    if run_context.stage != StageName.normalize:
        raise ValueError("normalize_level1_to_local_level2 requires a normalize stage RunContext")

    runner = normalization_runner or NormalizationRunner()
    partition_strategy = partition_strategy or PartitionStrategy()
    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
    quality_adapter = quality_hook or build_quality_hook(root_path)
    mapping_store = LocalMappingCatalogStore(root_path)
    level2_writer = SparkLevel2Writer(root_path, spark)

    started_at = run_context.started_at
    environment = manifest.environment
    job_name = run_context.job_name
    completed_at: datetime | None = None
    mapping_version: str | None = None
    table_manifests = []
    mapping_catalog_path: str | None = None
    quality_summary: QualityHookSummary | None = None
    failure: PipelineError | None = None

    artifact_store.append_log_event(
        run_context=run_context,
        environment=environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="normalize",
            event_type="normalize_start",
            message="Normalization run started",
            details={"source_name": manifest.source_name, "entity_name": manifest.entity_name},
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=job_name,
            inputs=[
                DatasetRef(
                    namespace="local",
                    name=manifest.data_path,
                    facets={
                        "artifact_id": manifest.artifact_id,
                        "content_hash": manifest.content_hash,
                    },
                )
            ],
        ),
    )

    status = "success"
    error_summary: dict[str, str] | None = None

    try:
        normalized = runner.normalize_level1(manifest=manifest, payload=_load_payload(payload))
        mapping_version = normalized.mapping_version
        mapping_catalog_path = mapping_store.write_catalog(normalized.mapping_catalog)
        artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO",
                component="normalize",
                event_type="mapping_catalog_written",
                message="Mapping catalog persisted",
                details={
                    "mapping_version": mapping_version,
                    "path": path_relative_to(mapping_catalog_path, root_path),
                },
            ),
        )

        partition = partition_strategy.resolve(manifest=manifest)
        for table in normalized.tables:
            table_manifest = level2_writer.write_table(
                run_context=run_context,
                manifest=manifest,
                mapping_version=mapping_version,
                table=table,
                partition=partition,
            )
            table_manifests.append(table_manifest)
            artifact_store.append_log_event(
                run_context=run_context,
                environment=environment,
                log_event=build_log_event(
                    run_context=run_context,
                    severity="INFO",
                    component="normalize",
                    event_type="table_written",
                    message="Level2 table written",
                    details={
                        "table_name": table_manifest.table_name,
                        "record_count": table_manifest.record_count,
                        "data_path": table_manifest.data_path,
                    },
                ),
            )

        quality_summary = quality_adapter.evaluate(
            run_context=run_context,
            environment=environment,
            request=_build_quality_request(
                run_context=run_context,
                environment=environment,
                table_manifests=table_manifests,
            ),
        )
        if quality_summary is not None:
            status_counts = quality_summary.counts_by_status()
            artifact_store.append_log_event(
                run_context=run_context,
                environment=environment,
                log_event=build_log_event(
                    run_context=run_context,
                    severity=quality_summary.log_severity,
                    component="quality",
                    event_type="quality_hook_complete",
                    message="Normalization quality hook evaluated",
                    details={
                        "backend_type": quality_summary.backend_type,
                        "status_counts": status_counts,
                    },
                ),
            )
            raise_for_blocking_quality_failures(quality_summary)

        completed_at = datetime.now(tz=UTC)
    except PipelineError as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = exc
        raw_quality_summary = exc.context.get("quality_summary")
        if raw_quality_summary is not None:
            quality_summary = QualityHookSummary.model_validate(raw_quality_summary)
        error_record = build_error_record(
            run_id=run_context.run_id,
            error_code=exc.error_code,
            error_category=exc.error_category.value,
            message=str(exc),
            retryable=exc.retryable,
            context=exc.context,
        )
        if not quality_error_already_recorded(exc):
            artifact_store.append_error_record(
                run_context=run_context,
                environment=environment,
                error_record=error_record,
            )
        error_summary = {
            "error_code": exc.error_code,
            "error_category": exc.error_category.value,
            "message": str(exc),
        }
    finally:
        records_read = manifest.record_count_estimate
        if records_read is None:
            root_table = next(
                (
                    manifest_table
                    for manifest_table in table_manifests
                    if manifest_table.table_name
                ),
                None,
            )
            records_read = root_table.record_count if root_table else None

        records_written = sum(
            table_manifest.record_count for table_manifest in table_manifests
        )
        files_written = len(table_manifests)
        metrics = MetricsSummary(
            records_read=records_read,
            records_written=records_written,
            files_written=files_written,
            extra={
                "tables_written": files_written,
                "mapping_version": mapping_version or "unknown",
            },
        )
        if quality_summary is not None:
            for status_name, count in quality_summary.counts_by_status().items():
                metrics.extra[f"quality.{status_name}"] = count
        for table_manifest in table_manifests:
            metrics.extra[f"table.{table_manifest.table_name}.records_written"] = (
                table_manifest.record_count
            )

        audit = AuditRecord(
            run_id=run_context.run_id,
            stage=run_context.stage.value,
            job_name=run_context.job_name,
            trigger_type=run_context.trigger_type,
            started_at=started_at,
            completed_at=completed_at,
            status=status,
            config_version=None,
            metrics_summary=metrics,
            error_summary=error_summary,
            validation_results=(
                [quality_summary.model_dump(mode="json")] if quality_summary is not None else []
            ),
            context={
                "environment": environment,
                "source_name": manifest.source_name,
                "entity_name": manifest.entity_name,
                "input_artifact_id": manifest.artifact_id,
                "input_manifest_path": manifest.manifest_path,
                "mapping_version": mapping_version or "unknown",
                "quality_backend_type": (
                    quality_summary.backend_type if quality_summary else ""
                ),
            },
        )
        rerun_of_run_id = run_context.attributes.get("rerun_of_run_id")
        if isinstance(rerun_of_run_id, str) and rerun_of_run_id:
            audit.context["rerun_of_run_id"] = rerun_of_run_id
        artifact_store.write_audit_record(
            run_context=run_context,
            environment=environment,
            audit_record=audit,
        )

        lineage_adapter.emit(
            run_context=run_context,
            environment=environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE" if status == "success" else "FAIL",
                run_id=run_context.run_id,
                job_name=job_name,
                inputs=[
                    DatasetRef(
                        namespace="local",
                        name=manifest.data_path,
                        facets={
                            "artifact_id": manifest.artifact_id,
                            "content_hash": manifest.content_hash,
                        },
                    )
                ],
                outputs=[
                    DatasetRef(
                        namespace="local",
                        name=table_manifest.data_path,
                        facets={
                            "artifact_id": table_manifest.artifact_id,
                            "table_name": table_manifest.table_name,
                            "mapping_version": table_manifest.mapping_version,
                            "partition": table_manifest.partition,
                        },
                    )
                    for table_manifest in table_manifests
                ],
            ),
        )
        artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO" if status == "success" else "ERROR",
                component="normalize",
                event_type="normalize_complete",
                message="Normalization run completed",
                details={"status": status},
            ),
        )

    if failure is not None:
        raise failure

    if mapping_catalog_path is None:
        mapping_catalog_path = mapping_store.catalog_path(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            mapping_version=mapping_version or "unknown",
        )

    return Level2WriteSummary(
        mapping_catalog_path=path_relative_to(mapping_catalog_path, root_path),
        table_manifests=table_manifests,
    )


def _build_quality_request(
    *,
    run_context: RunContext,
    environment: str,
    table_manifests: list,
) -> QualityHookRequest:
    records_written = sum(table_manifest.record_count for table_manifest in table_manifests)
    return QualityHookRequest(
        run_id=run_context.run_id,
        stage=run_context.stage.value,
        job_name=run_context.job_name,
        environment=environment,
        datasets=[
            QualityDatasetRef(
                dataset_id=table_manifest.artifact_id,
                dataset_name=table_manifest.table_name,
                materialization_type="spark_parquet",
                target_name=table_manifest.table_name,
                output_path=table_manifest.data_path,
                row_count=table_manifest.record_count,
                metrics={
                    "file_count": table_manifest.file_count,
                    "total_file_size_bytes": table_manifest.total_file_size_bytes,
                    "mapping_version": table_manifest.mapping_version,
                },
            )
            for table_manifest in table_manifests
        ],
        metrics={
            "tables_written": len(table_manifests),
            "records_written": records_written,
        },
    )

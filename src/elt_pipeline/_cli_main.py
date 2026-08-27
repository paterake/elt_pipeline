from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pyspark.sql import SparkSession

from elt_pipeline._cli_connectors import (
    _CliBrokerKafkaConnector,
    _CliLocalKafkaConnector,
    _CliLocalObjectStorageConnector,
    _CliLocalRestConnector,
    _CliLocalSqlConnector,
)
from elt_pipeline._cli_helpers import (
    _build_serving_endpoint,
    _iceberg_effective_enabled,
    _load_runtime_overrides_from_env_or_args,
    _resolve_iceberg_session_kwargs,
    _run_catalog_preflight_from_env,
    _validate_iceberg_catalog_binding,
)
from elt_pipeline._cli_models import (
    _CheckpointOverride,
    _PublishRerunSelection,
    _SqlRerunSelection,
)
from elt_pipeline._cli_parser import build_parser
from elt_pipeline.config import runtime_context
from elt_pipeline.config.loader import (
    load_pipeline_config,
    resolve_entity_config,
)
from elt_pipeline.config.models import Level2Mode, PipelineConfig, ResolvedEntityConfig
from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.ingest import (
    ConnectorFamily,
    ConnectorRegistryError,
    LocalArtifactStore,
    apply_connector_preset_defaults,
    get_connector_factory,
    load_connector_manifest_from_json,
    load_connector_manifest_from_yaml,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.integrations import (
    build_lineage_adapter,
    build_observability_adapter,
    load_orchestration_metadata_from_env,
)
from elt_pipeline.maintenance import (
    DEFAULT_OPERATIONS,
    MaintenanceConfig,
    MaintenanceOperation,
    run_maintenance,
)
from elt_pipeline.normalize.partitioning import PartitionStrategy
from elt_pipeline.normalize.pipeline import normalize_level1_to_local_level2
from elt_pipeline.publish import (
    discover_publish_definitions,
    explain_publish_definitions,
    filter_publish_definitions,
    run_publish_definitions_locally,
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
from elt_pipeline.shared.observability import (
    AlertEvent,
    AlertSeverity,
    MetricPoint,
    MetricType,
)
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_exists,
    path_glob,
    path_mkdir,
    path_normalize,
    path_read_text,
    path_rglob,
    path_write_bytes,
)
from elt_pipeline.shared.runtime import (
    ExecutionWindow,
    StageName,
    build_job_runtime,
    new_run_context,
)
from elt_pipeline.shared.scheduler import (
    SchedulePlan,
    WaitForSpec,
    load_schedule_plan,
    parse_schedule_payload,
    topological_sort_schedule_jobs,
)
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql import (
    SparkSqlModelExecutor,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    resolve_selected_model_ids,
    run_sql_models_locally,
    topologically_sort_sql_models,
)


def main(argv: list[str] | None = None) -> int:
    import glob as _glob
    import os as _os
    import shutil as _shutil
    import subprocess as _sp
    import sys as _sys

    if not _os.environ.get("PYSPARK_PYTHON", "").strip():
        _os.environ["PYSPARK_PYTHON"] = _sys.executable
    if not _os.environ.get("PYSPARK_DRIVER_PYTHON", "").strip():
        _os.environ["PYSPARK_DRIVER_PYTHON"] = _os.environ["PYSPARK_PYTHON"]
    _java_on_path = _shutil.which("java")
    _java_home = _os.environ.get("JAVA_HOME", "").strip()
    _jdk_home_ok = bool(
        _java_home
        and Path(_java_home).is_dir()
        and (Path(_java_home) / "bin" / "java").exists()
    )
    if (not _jdk_home_ok) or (not _java_on_path) or (
        _java_on_path and "/usr/bin/java" in _java_on_path
    ):
        for _probe in (("mise", ["which", "java"]),):
            try:
                _probe_bin = _shutil.which(_probe[0])
                if _probe_bin is None:
                    continue
                _out = _sp.run(
                    [_probe_bin, *_probe[1]],
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip()
                if not _out:
                    continue
                _p = Path(_out).resolve()
                if _probe[0] == "mise" and _p.name == "java" and _p.parent.name == "bin":
                    _candidate_home = str(_p.parents[1])
                else:
                    continue
                _bin_path = str(Path(_candidate_home) / "bin")
                if not Path(_candidate_home).is_dir() or not Path(
                    _bin_path / "java"
                ).exists():
                    continue
                _os.environ["JAVA_HOME"] = _candidate_home
                if Path(_bin_path).is_dir():
                    _os.environ["PATH"] = _bin_path + _os.pathsep + _os.environ.get(
                        "PATH", ""
                    )
                break
            except Exception:
                continue
        if not _os.environ.get("JAVA_HOME", "").strip():
            for _home_glob in (
                str(Path.home() / ".local/share/mise/installs/java/*/bin/java"),
                str(Path.home() / ".sdkman/candidates/java/current/bin/java"),
                str(Path.home() / ".asdf/installs/java/*/bin/java"),
                str(Path.home() / ".jabba/jdk/*/Contents/Home/bin/java"),
            ):
                try:
                    _matches = sorted(_glob.glob(_home_glob))
                except Exception:
                    _matches = []
                for _m_str in _matches:
                    _m = Path(_m_str)
                    try:
                        _candidate_home = str(_m.parents[1])
                    except Exception:
                        continue
                    if Path(_candidate_home).is_dir() and (
                        Path(_candidate_home) / "bin" / "java"
                    ).exists():
                        _os.environ["JAVA_HOME"] = _candidate_home
                        _bin_path = str(Path(_candidate_home) / "bin")
                        if Path(_bin_path).is_dir():
                            _os.environ["PATH"] = (
                                _bin_path
                                + _os.pathsep
                                + _os.environ.get("PATH", "")
                            )
                        break
                if _os.environ.get("JAVA_HOME", "").strip():
                    break

    parser = build_parser()
    args = parser.parse_args(argv)

    _config_path_arg = getattr(args, "config_path", None)
    _config_path_str = str(_config_path_arg) if _config_path_arg is not None else None
    _environment_arg = getattr(args, "environment", None)

    try:
        runtime_context.initialize(
            config_path_arg=_config_path_str,
            environment_arg=_environment_arg,
        )

        if args.command == "validate-config":
            explicit_cp: str | Path | None = None
            if args.config_path is None:
                explicit_cp = runtime_context.config_path()
                if explicit_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "Cannot validate-config: no pipeline.yaml found at "
                            "repo root and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(explicit_cp))
            else:
                config = load_pipeline_config(args.config_path)
            if args.source and args.entity:
                resolved = resolve_entity_config(
                    config,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
                    config_path=explicit_cp or args.config_path,
                )
                print(json.dumps(resolved.model_dump(mode="json"), indent=2))
            else:
                print(json.dumps(config.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "show-run-context":
            context = new_run_context(
                stage=StageName(args.stage),
                job_name=args.job_name,
                trigger_type=args.trigger_type,
                attributes=_with_orchestration_attributes(),
            )
            print(json.dumps(context.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "ingest":
            if args.config_path is None:
                ingest_cp = runtime_context.config_path()
                if ingest_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "ingest: no pipeline.yaml found at repo root "
                            "and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(ingest_cp))
                ingest_config_path = str(ingest_cp)
            else:
                config = load_pipeline_config(args.config_path)
                ingest_config_path = args.config_path
            cli_window = _build_cli_window_selection(
                window_start=args.window_start,
                window_end=args.window_end,
                window_label=args.window_label,
                backfill=args.backfill,
            )
            selected_entities = _resolve_entity_selections(
                config,
                environment=args.environment,
                source_name=args.source,
                entity_name=args.entity,
                config_path=ingest_config_path,
            )
            results = [
                _run_ingest_entity(
                    resolved_config=resolved_config,
                    root_path=args.root_path,
                    job_name=args.job_name,
                    trigger_type=args.trigger_type,
                    kafka_log_path=args.kafka_log_path,
                    cli_window=cli_window,
                    backfill=args.backfill,
                )
                for resolved_config in selected_entities
            ]
            payload = {
                "command": "ingest.run",
                "environment": args.environment,
                "selection": {
                    "source": args.source,
                    "entity": args.entity,
                    "window_start": _serialize_datetime(cli_window.start),
                    "window_end": _serialize_datetime(cli_window.end),
                    "window_label": cli_window.label,
                    "backfill": args.backfill,
                },
                "result_count": len(results),
                "results": results,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "normalize":
            if args.config_path is None:
                normalize_cp = runtime_context.config_path()
                if normalize_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "normalize: no pipeline.yaml found at repo root "
                            "and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(normalize_cp))
                normalize_config_path = str(normalize_cp)
            else:
                config = load_pipeline_config(args.config_path)
                normalize_config_path = args.config_path
            cli_window = _build_cli_window_selection(
                window_start=args.window_start,
                window_end=args.window_end,
                window_label=args.window_label,
                backfill=args.backfill,
            )
            if args.rerun_run_id:
                _validate_normalize_rerun_request(args)
                manifests = [
                    _resolve_normalize_rerun_manifest(
                        root_path=args.root_path,
                        rerun_run_id=args.rerun_run_id,
                    )
                ]
                selected_environment = manifests[0].environment
            else:
                manifests = _select_level1_manifests(
                    root_path=args.root_path,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
                    explicit_manifest_paths=args.manifest_path,
                    window_start=cli_window.start,
                    window_end=cli_window.end,
                )
                selected_environment = args.environment
            selected_source = manifests[0].source_name if args.rerun_run_id else args.source
            selected_entity = manifests[0].entity_name if args.rerun_run_id else args.entity
            if (
                args.rerun_run_id
                and selected_environment != runtime_context.selected_environment()
            ):
                rerun_override_ro = _load_runtime_overrides_from_env_or_args(
                    config_path=str(args.config_path)
                    if getattr(args, "config_path", None)
                    else None,
                    environment=selected_environment,
                )
            else:
                rerun_override_ro = runtime_context.as_runtime_overrides()
            normalize_spark = build_spark_session(
                app_name=f"elt-pipeline-normalize-{args.job_name}",
                runtime_overrides=rerun_override_ro,
            )
            try:
                summaries = [
                    _run_normalize_manifest(
                        manifest=manifest,
                        resolved_config=resolve_entity_config(
                            config,
                            environment=manifest.environment,
                            source_name=manifest.source_name,
                            entity_name=manifest.entity_name,
                            config_path=normalize_config_path,
                        ),
                        root_path=args.root_path,
                        job_name=args.job_name,
                        trigger_type=args.trigger_type,
                        backfill=args.backfill,
                        rerun_run_id=args.rerun_run_id,
                        partition_strategy=PartitionStrategy(
                            mode=args.partition_mode,
                            partition_key=args.partition_key,
                            metadata_key=args.metadata_key,
                        ),
                        spark=normalize_spark,
                    )
                    for manifest in manifests
                ]
            finally:
                normalize_spark.stop()
            payload = {
                "command": "normalize.run",
                "environment": selected_environment,
                "selection": {
                    "source": selected_source,
                    "entity": selected_entity,
                    "manifest_paths": (
                        [manifest.manifest_path for manifest in manifests]
                        if args.rerun_run_id
                        else [str(path) for path in args.manifest_path]
                    ),
                    "window_start": _serialize_datetime(cli_window.start),
                    "window_end": _serialize_datetime(cli_window.end),
                    "window_label": cli_window.label,
                    "backfill": args.backfill,
                    "rerun_run_id": args.rerun_run_id,
                },
                "processed_count": len(summaries),
                "results": summaries,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "sql":
            rerun_run_id = getattr(args, "rerun_run_id", None)
            discovered_models = discover_sql_models(args.package_path)
            ordered_models = topologically_sort_sql_models(discovered_models)
            sql_environment = args.environment
            selection_stage = args.stage
            selection_domain = args.domain
            selection_model = args.model
            selection_include_dependencies = args.include_deps
            selection_start_date = args.start_date
            selection_end_date = args.end_date
            rerun_selection: _SqlRerunSelection | None = None

            if args.sql_command == "run" and rerun_run_id:
                _validate_sql_rerun_request(args)
                rerun_selection = _resolve_sql_rerun_selection(
                    root_path=args.root_path,
                    rerun_run_id=rerun_run_id,
                )
                selected_ids = set(rerun_selection.model_ids)
                models_to_process = [
                    model for model in ordered_models if model.model_id in selected_ids
                ]
                missing_model_ids = sorted(
                    model_id
                    for model_id in selected_ids
                    if all(model.model_id != model_id for model in ordered_models)
                )
                if missing_model_ids:
                    raise ConfigValidationError(
                        message="Rerun selection references SQL models that are not present",
                        context={
                            "package_path": str(args.package_path),
                            "rerun_run_id": args.rerun_run_id,
                            "missing_model_ids": missing_model_ids,
                        },
                    )
                sql_environment = rerun_selection.environment
                selection_stage = None
                selection_domain = None
                selection_model = None
                selection_include_dependencies = False
                selection_start_date = rerun_selection.start_date
                selection_end_date = rerun_selection.end_date
            else:
                selected_models = filter_sql_models(
                    discovered_models,
                    stage=args.stage,
                    domain=args.domain,
                    model_name=args.model,
                )
                if not selected_models:
                    raise ConfigValidationError(
                        message="No SQL models matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "stage": args.stage,
                            "domain": args.domain,
                            "model": args.model,
                        },
                    )

                selected_ids = resolve_selected_model_ids(
                    all_models=discovered_models,
                    selected_models=selected_models,
                    include_dependencies=args.include_deps,
                )
                models_to_process = [
                    model for model in ordered_models if model.model_id in selected_ids
                ]

            if (
                rerun_run_id
                and sql_environment != runtime_context.selected_environment()
            ):
                _sql_runtime_overrides = _load_runtime_overrides_from_env_or_args(
                    config_path=(
                        str(args.config_path)
                        if getattr(args, "config_path", None)
                        else None
                    ),
                    environment=sql_environment,
                )
            else:
                _sql_runtime_overrides = runtime_context.as_runtime_overrides()

            run_context = new_run_context(
                stage=StageName.sql,
                job_name=getattr(args, "job_name", "sql-compile"),
                trigger_type=getattr(args, "trigger_type", "manual"),
                attributes=_with_orchestration_attributes(
                    {
                        "environment": sql_environment,
                        "package_path": str(args.package_path),
                        "stage_selection": selection_stage or "",
                        "domain_selection": selection_domain or "",
                        "model_selection": selection_model or "",
                        "rerun_of_run_id": rerun_run_id or "",
                    }
                ),
            )
            extra_values = (
                rerun_selection.extra_values
                if rerun_selection is not None
                else _parse_vars_json(args.vars_json)
            )
            partition_values = (
                rerun_selection.partition_values
                if rerun_selection is not None
                else _parse_partition_values(args.partition)
            )
            compiled_models = [
                compile_sql_model(
                    model,
                    token_context=build_token_context(
                        environment=sql_environment,
                        run_id=run_context.run_id,
                        stage=model.manifest.stage.value,
                        domain=model.manifest.domain,
                        model_name=model.manifest.name,
                        target_table_name=model.manifest.target.table_name,
                        start_date=selection_start_date,
                        end_date=selection_end_date,
                        partition_values=partition_values,
                        source_name=(
                            model.manifest.sources[0].source_name
                            if model.manifest.sources
                            else None
                        ),
                        source_entity=(
                            model.manifest.sources[0].entity_name
                            if model.manifest.sources
                            else None
                        ),
                        source_table=(
                            model.manifest.sources[0].table_name
                            if model.manifest.sources and model.manifest.sources[0].table_name
                            else None
                        ),
                        extra_values=extra_values,
                    ),
                )
                for model in models_to_process
            ]

            if args.sql_command == "compile":
                payload = {
                    "run_id": run_context.run_id,
                    "selection": {
                        "stage": selection_stage,
                        "domain": selection_domain,
                        "model": selection_model,
                        "include_dependencies": selection_include_dependencies,
                        "start_date": selection_start_date,
                        "end_date": selection_end_date,
                        "partitions": partition_values,
                        "rerun_run_id": rerun_run_id,
                    },
                    "model_count": len(compiled_models),
                    "models": [
                        {
                            "model_id": model.model_id,
                            "target_table_name": model.target_table_name,
                            "load_mode": model.load_mode.value,
                            "compiled_sql": model.compiled_sql,
                            "token_values": model.token_values,
                        }
                        for model in compiled_models
                    ],
                }
                print(json.dumps(payload, indent=2))
                return 0

            if args.sql_command == "run":
                _validate_iceberg_catalog_binding(args, runtime_overrides=_sql_runtime_overrides)
                _run_catalog_preflight_from_env(
                    args,
                    runtime_overrides=_sql_runtime_overrides,
                    stage_label="sql",
                )
                if args.validate_only or args.explain:
                    sql_spark = build_spark_session(
                        **_resolve_iceberg_session_kwargs(
                            args=args,
                            app_name=f"elt-pipeline-sql-{getattr(args, 'job_name', 'sql-run')}",
                            runtime_overrides=_sql_runtime_overrides,
                        )
                    )
                    try:
                        planning_result = SparkSqlModelExecutor(
                            spark=sql_spark,
                            warehouse_root=args.warehouse_root,
                            root_path=args.root_path,
                            environment=sql_environment,
                            run_id=run_context.run_id,
                            partition_values=partition_values,
                        ).plan(
                            compiled_models,
                            include_query_plan=args.explain,
                        )
                    finally:
                        sql_spark.stop()
                    payload = {
                        "run_id": run_context.run_id,
                        "mode": "explain" if args.explain else "validate_only",
                        "selection": {
                            "stage": selection_stage,
                            "domain": selection_domain,
                            "model": selection_model,
                            "include_dependencies": selection_include_dependencies,
                            "start_date": selection_start_date,
                            "end_date": selection_end_date,
                            "partitions": partition_values,
                            "rerun_run_id": rerun_run_id,
                        },
                        "warehouse_root": str(planning_result.warehouse_root),
                        "model_count": planning_result.model_count,
                        "execution_order": [
                            model_plan.model_id for model_plan in planning_result.planned_models
                        ],
                        "models": [
                            {
                                "model_id": model_plan.model_id,
                                "target_table_name": model_plan.target_table_name,
                                "load_mode": model_plan.load_mode.value,
                                "depends_on": model_plan.depends_on,
                                "token_values": model_plan.token_values,
                                "validation_passed": model_plan.validation_passed,
                                "validation_message": model_plan.validation_message,
                                "query_plan": [
                                    plan_step.model_dump(mode="json")
                                    for plan_step in model_plan.query_plan
                                ],
                            }
                            for model_plan in planning_result.planned_models
                        ],
                    }
                    print(json.dumps(payload, indent=2))
                    return 0

                sql_spark = build_spark_session(
                    **_resolve_iceberg_session_kwargs(
                        args=args,
                        app_name=f"elt-pipeline-sql-{getattr(args, 'job_name', 'sql-run')}",
                        runtime_overrides=_sql_runtime_overrides,
                    )
                )
                serving_endpoint = _build_serving_endpoint(
                    args, runtime_overrides=_sql_runtime_overrides
                )
                try:
                    result = run_sql_models_locally(
                        root_path=args.root_path,
                        run_context=run_context,
                        environment=sql_environment,
                        package_path=args.package_path,
                        warehouse_root=args.warehouse_root,
                        spark=sql_spark,
                        compiled_models=compiled_models,
                        partition_values=partition_values,
                        extra_values=extra_values,
                        selection_stage=selection_stage,
                        selection_domain=selection_domain,
                        selection_model=selection_model,
                        include_dependencies=selection_include_dependencies,
                        serving_endpoint=serving_endpoint,
                    )
                finally:
                    sql_spark.stop()
                payload = {
                    "run_id": run_context.run_id,
                    "selection": {
                        "stage": selection_stage,
                        "domain": selection_domain,
                        "model": selection_model,
                        "include_dependencies": selection_include_dependencies,
                        "start_date": selection_start_date,
                        "end_date": selection_end_date,
                        "partitions": partition_values,
                        "rerun_run_id": rerun_run_id,
                    },
                    "warehouse_root": str(result.execution_result.warehouse_root),
                    "model_count": result.execution_result.model_count,
                    "executed_models": [
                        record.model_dump(mode="json")
                        for record in result.execution_result.executed_models
                    ],
                    "validation_results": [
                        summary.model_dump(mode="json")
                        for summary in result.execution_result.model_validations
                    ],
                    "artifacts": {
                        "artifact_root": str(result.artifacts.artifact_root),
                        "run_dir": str(result.artifacts.run_dir),
                        "audit_path": str(result.artifacts.audit_path),
                        "log_path": str(result.artifacts.log_path),
                        "lineage_path": str(result.artifacts.lineage_path),
                        "error_path": (
                            str(result.artifacts.error_path)
                            if result.artifacts.error_path is not None
                            else None
                        ),
                    },
                    "serving_endpoint": serving_endpoint,
                }
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "publish":
            discovered_definitions = discover_publish_definitions(args.package_path)
            if args.publish_command == "validate":
                selected_definitions = filter_publish_definitions(
                    discovered_definitions,
                    domain=args.domain,
                    publish_name=args.publish_name,
                )
                if not selected_definitions:
                    raise ConfigValidationError(
                        message="No publish definitions matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "domain": args.domain,
                            "publish_name": args.publish_name,
                        },
                    )
                payload = {
                    "command": "publish.validate",
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": args.domain,
                        "publish_name": args.publish_name,
                    },
                    "publish_count": len(selected_definitions),
                    "definitions": [
                        {
                            "publish_id": definition.publish_id,
                            "manifest_path": str(definition.manifest_path),
                            "query_path": (
                                str(definition.query_path)
                                if definition.query_path is not None
                                else None
                            ),
                            "source_dataset": definition.manifest.source.dataset,
                            "selection_mode": definition.manifest.source.selection_mode.value,
                            "output_format": definition.manifest.delivery.output_format.value,
                            "replacement_mode": definition.manifest.delivery.replacement_mode.value,
                        }
                        for definition in selected_definitions
                    ],
                }
                print(json.dumps(payload, indent=2))
                return 0

            publish_environment = args.environment
            selection_domain = args.domain
            selection_publish = args.publish_name
            selection_window_start = args.window_start
            selection_window_end = args.window_end
            selection_window_label = args.window_label
            publish_backfill = getattr(args, "backfill", False)
            rerun_run_id = getattr(args, "rerun_run_id", None)

            if args.publish_command == "run" and rerun_run_id:
                _validate_publish_rerun_request(args)
                rerun_selection = _resolve_publish_rerun_selection(
                    root_path=path_normalize(args.root_path),
                    rerun_run_id=rerun_run_id,
                )
                publish_environment = rerun_selection.environment
                selection_domain = None
                selection_publish = None
                selection_window_start = rerun_selection.window_start
                selection_window_end = rerun_selection.window_end
                selection_window_label = rerun_selection.window_label
                publish_backfill = rerun_selection.backfill
                selected_ids = set(rerun_selection.publish_ids)
                selected_definitions = [
                    definition
                    for definition in discovered_definitions
                    if definition.publish_id in selected_ids
                ]
                missing_publish_ids = sorted(
                    publish_id
                    for publish_id in selected_ids
                    if all(
                        definition.publish_id != publish_id
                        for definition in discovered_definitions
                    )
                )
                if missing_publish_ids:
                    raise ConfigValidationError(
                        message=(
                            "Rerun selection references publish definitions "
                            "that are not present"
                        ),
                        context={
                            "package_path": str(args.package_path),
                            "rerun_run_id": rerun_run_id,
                            "missing_publish_ids": missing_publish_ids,
                        },
                    )
            else:
                selected_definitions = filter_publish_definitions(
                    discovered_definitions,
                    domain=args.domain,
                    publish_name=args.publish_name,
                )
                if not selected_definitions:
                    raise ConfigValidationError(
                        message="No publish definitions matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "domain": args.domain,
                            "publish_name": args.publish_name,
                        },
                    )

            if (
                rerun_run_id
                and publish_environment != runtime_context.selected_environment()
            ):
                _publish_runtime_overrides = _load_runtime_overrides_from_env_or_args(
                    config_path=(
                        str(args.config_path)
                        if getattr(args, "config_path", None)
                        else None
                    ),
                    environment=publish_environment,
                )
            else:
                _publish_runtime_overrides = runtime_context.as_runtime_overrides()

            cli_window = _build_cli_window_selection(
                window_start=selection_window_start,
                window_end=selection_window_end,
                window_label=selection_window_label,
                backfill=publish_backfill,
            )
            run_context = build_job_runtime(
                stage=StageName.publish,
                job_name=getattr(args, "job_name", "publish-explain"),
                environment=publish_environment,
                trigger_type=getattr(args, "trigger_type", "manual"),
                window=cli_window,
                backfill=publish_backfill,
                attributes=_with_orchestration_attributes(
                    {
                        "package_path": str(args.package_path),
                        "domain_selection": selection_domain or "",
                        "publish_selection": selection_publish or "",
                        "rerun_of_run_id": rerun_run_id or "",
                    }
                ),
            ).to_run_context()

            if args.publish_command == "explain":
                payload = {
                    "command": "publish.explain",
                    "run_id": run_context.run_id,
                    "environment": publish_environment,
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": selection_domain,
                        "publish_name": selection_publish,
                        "window_start": _serialize_datetime(cli_window.start),
                        "window_end": _serialize_datetime(cli_window.end),
                        "window_label": cli_window.label,
                        "backfill": publish_backfill,
                    },
                    "publish_count": len(selected_definitions),
                    "plans": explain_publish_definitions(
                        root_path=path_normalize(args.root_path),
                        run_context=run_context,
                        definitions=selected_definitions,
                    ),
                }
                print(json.dumps(payload, indent=2))
                return 0

            if args.publish_command == "run":
                _validate_iceberg_catalog_binding(
                    args, runtime_overrides=_publish_runtime_overrides
                )
                _run_catalog_preflight_from_env(
                    args,
                    runtime_overrides=_publish_runtime_overrides,
                    stage_label="publish",
                )
                publish_spark = build_spark_session(
                    **_resolve_iceberg_session_kwargs(
                        args=args,
                        app_name=f"elt-pipeline-publish-{getattr(args, 'job_name', 'publish-run')}",
                        runtime_overrides=_publish_runtime_overrides,
                    )
                )
                serving_endpoint = _build_serving_endpoint(
                    args, runtime_overrides=_publish_runtime_overrides
                )
                try:
                    result = run_publish_definitions_locally(
                        root_path=path_normalize(args.root_path),
                        run_context=run_context,
                        environment=publish_environment,
                        package_path=args.package_path,
                        warehouse_root=args.warehouse_root,
                        spark=publish_spark,
                        definitions=selected_definitions,
                        serving_endpoint=serving_endpoint,
                    )
                finally:
                    publish_spark.stop()
                payload = {
                    "command": "publish.run",
                    "run_id": run_context.run_id,
                    "environment": publish_environment,
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": selection_domain,
                        "publish_name": selection_publish,
                        "window_start": _serialize_datetime(cli_window.start),
                        "window_end": _serialize_datetime(cli_window.end),
                        "window_label": cli_window.label,
                        "backfill": publish_backfill,
                        "rerun_run_id": rerun_run_id,
                    },
                    "warehouse_root": str(args.warehouse_root),
                    "publish_count": len(result.results),
                    "results": [
                        {
                            "publish_id": publish_result.publish_id,
                            "row_count": publish_result.row_count,
                            "artifacts": [
                                artifact.model_dump(mode="json")
                                for artifact in publish_result.artifacts
                            ],
                            "validations": [
                                validation.model_dump(mode="json")
                                for validation in publish_result.validations
                            ],
                        }
                        for publish_result in result.results
                    ],
                    "artifacts": {
                        "artifact_root": str(result.artifacts.artifact_root),
                        "run_dir": str(result.artifacts.run_dir),
                        "export_manifest_path": (
                            str(result.artifacts.export_manifest_path)
                            if result.artifacts.export_manifest_path is not None
                            else None
                        ),
                        "audit_path": (
                            str(result.artifacts.audit_path)
                            if result.artifacts.audit_path is not None
                            else None
                        ),
                        "log_path": (
                            str(result.artifacts.log_path)
                            if result.artifacts.log_path is not None
                            else None
                        ),
                        "lineage_path": (
                            str(result.artifacts.lineage_path)
                            if result.artifacts.lineage_path is not None
                            else None
                        ),
                        "error_path": (
                            str(result.artifacts.error_path)
                            if result.artifacts.error_path is not None
                            else None
                        ),
                    },
                    "serving_endpoint": serving_endpoint,
                }
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "maintain":
            if not _iceberg_effective_enabled(args):
                raise ConfigValidationError(
                    message="maintain: Iceberg must be enabled (it is OFF). "
                            "Remove --no-iceberg-enabled or set YAML/ENV iceberg enabled."
                )
            maintenance_cfg = _build_maintenance_config(args)
            has_tables = (
                bool(maintenance_cfg.table_fqns)
                or maintenance_cfg.all_level3
                or maintenance_cfg.all_level4
            )
            if not has_tables:
                raise ConfigValidationError(
                    message="maintain: no tables selected. Pass --all-level3, --all-level4, "
                            "and/or one or more --table flags."
                )
            session_kwargs = _resolve_iceberg_session_kwargs(
                args=args, app_name="elt_pipeline_maintain",
            )
            spark = build_spark_session(**session_kwargs)
            try:
                report = run_maintenance(spark=spark, config=maintenance_cfg)
            finally:
                spark.stop()
            print(json.dumps(
                {"command": "maintain.run", **report.to_dict()},
                indent=2,
                default=str,
            ))
            return 0

        if args.command == "schedule":
            plan = load_schedule_plan(args.plan_path)
            continue_on_error = args.continue_on_error or plan.continue_on_error
            if args.audit_root is not None:
                audit_root = path_normalize(str(args.audit_root))
            else:
                plan_hash = hashlib.sha1(
                    str(args.plan_path.resolve()).encode("utf-8")
                ).hexdigest()[:12]
                audit_root = join_paths(
                    path_normalize(str(args.plan_path.resolve().parent)),
                    "runs",
                    f"schedule_{plan_hash}",
                )
            path_mkdir(audit_root, exist_ok=True)
            run_id, start_iso = _new_schedule_run_id_and_start()
            payload, exit_code = _run_schedule_plan(
                plan=plan,
                plan_path=path_normalize(str(args.plan_path)),
                continue_on_error=continue_on_error,
                run_id=run_id,
                started_at_iso=start_iso,
            )
            audit_path = join_paths(audit_root, "schedule_execution_audit.json")
            audit_bytes = (
                json.dumps(payload, indent=2, default=str, sort_keys=True) + "\n"
            ).encode("utf-8")
            path_write_bytes(audit_path, audit_bytes)
            payload["audit_path"] = audit_path
            payload["run_id"] = run_id
            print(json.dumps(payload, indent=2))
            return exit_code

        if args.command == "lineage":
            from elt_pipeline.shared.lineage_impact import run_lineage_impact_analysis

            if args.lineage_command == "impact-analysis":
                if args.depth < 1:
                    print(
                        json.dumps(
                            {
                                "error": "depth must be >= 1",
                                "provided": args.depth,
                            },
                            indent=2,
                        ),
                        file=sys.stderr,
                    )
                    return 2
                try:
                    result = run_lineage_impact_analysis(
                        root_path=path_normalize(args.root_path),
                        column=args.impact_column,
                        depth=args.depth,
                        output_format=args.impact_format,
                    )
                except ValueError as exc:
                    print(
                        json.dumps(
                            {"error": "lineage_impact_invalid_args", "message": str(exc)},
                            indent=2,
                        ),
                        file=sys.stderr,
                    )
                    return 2
                if args.impact_format == "json":
                    clean = {k: v for k, v in result.items() if not k.startswith("_")}
                    print(json.dumps(clean, indent=2, default=str, sort_keys=True))
                else:
                    lines = result.get("_display_lines") or []
                    for line in lines:
                        print(line)
                return 0
            parser.error(f"Unhandled lineage subcommand: {args.lineage_command}")
            return 1
    except (ConfigValidationError, ValidationError) as exc:
        error_record = build_error_record(
            run_id="unassigned",
            error_code="CONFIG_VALIDATION_FAILED",
            error_category="config_error",
            message=str(exc),
            retryable=False,
        )
        print(json.dumps(error_record.model_dump(mode="json"), indent=2), file=sys.stderr)
        return 2
    except PipelineError as exc:
        error_record = build_error_record(
            run_id="unassigned",
            error_code=exc.error_code,
            error_category=exc.error_category.value,
            message=str(exc),
            retryable=exc.retryable,
            context=exc.context,
        )
        print(json.dumps(error_record.model_dump(mode="json"), indent=2), file=sys.stderr)
        return 1

    parser.error(f"Unhandled command: {args.command}")
    return 1


def _new_schedule_run_id_and_start() -> tuple[str, str]:
    now = datetime.now(UTC)
    stamp = now.strftime("%Y%m%dT%H%M%S.%fZ")
    salt = hashlib.sha1(os.urandom(16)).hexdigest()[:8]
    return (f"schedule_run_{stamp}_{salt}", now.isoformat())


def _build_schedule_run_context(run_id: str, started_at_iso: str):
    from elt_pipeline.shared.runtime import RunContext, StageName

    try:
        start_dt = datetime.fromisoformat(started_at_iso)
    except ValueError:
        start_dt = datetime.now(UTC)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=UTC)
    return RunContext(
        run_id=run_id,
        stage=StageName.ingest,
        job_name="schedule_run",
        trigger_type="scheduled",
        started_at=start_dt,
        attributes={},
    )


def _emit_sensor_poll_log(
    *,
    run_id: str,
    started_at_iso: str,
    job_name: str,
    poll_index: int,
    state: str,
    wait_kind: str,
    wait_target: str,
    elapsed_seconds: float,
    detail: str = "",
) -> dict[str, Any]:
    event = {
        "run_id": run_id,
        "severity": "INFO" if state != "error" else "WARNING",
        "component": "schedule.sensor",
        "event_type": "sensor_poll",
        "message": (
            f"sensor poll job={job_name} index={poll_index} state={state} "
            f"kind={wait_kind} elapsed={elapsed_seconds:.2f}s"
        ),
        "timestamp": started_at_iso,
        "details": {
            "job": job_name,
            "poll_index": poll_index,
            "state": state,
            "wait_kind": wait_kind,
            "wait_target": wait_target,
            "elapsed_seconds": round(elapsed_seconds, 3),
        },
    }
    if detail:
        event["details"]["detail"] = detail
    print(json.dumps(event, sort_keys=True, default=str), file=sys.stderr)
    return event


def _wait_for_path_exists(
    *,
    path: str,
    poll_sec: float,
    timeout_sec: float,
    run_id: str,
    started_at_iso: str,
    job_name: str,
    sensor_events: list[dict[str, Any]],
    poll_gauge: dict[tuple[str, str], int],
) -> tuple[bool, str]:
    start = time.monotonic()
    poll_index = 0
    while True:
        poll_index += 1
        elapsed = time.monotonic() - start
        try:
            satisfied = path_exists(path)
        except Exception as exc:
            state = "error"
            detail = f"path_exists raised: {type(exc).__name__}: {exc}"
            poll_gauge[(job_name, "error")] = poll_gauge.get((job_name, "error"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="path_exists",
                wait_target=path,
                elapsed_seconds=elapsed,
                detail=detail,
            )
            sensor_events.append(evt)
        else:
            if satisfied:
                state = "satisfied"
                poll_gauge[(job_name, "satisfied")] = (
                    poll_gauge.get((job_name, "satisfied"), 0) + 1
                )
                evt = _emit_sensor_poll_log(
                    run_id=run_id,
                    started_at_iso=started_at_iso,
                    job_name=job_name,
                    poll_index=poll_index,
                    state=state,
                    wait_kind="path_exists",
                    wait_target=path,
                    elapsed_seconds=elapsed,
                    detail="path found",
                )
                sensor_events.append(evt)
                return True, "path_exists satisfied"
            state = "polling"
            poll_gauge[(job_name, "polling")] = poll_gauge.get((job_name, "polling"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="path_exists",
                wait_target=path,
                elapsed_seconds=elapsed,
                detail="path not yet present",
            )
            sensor_events.append(evt)
        if elapsed >= timeout_sec:
            poll_gauge[(job_name, "timeout")] = poll_gauge.get((job_name, "timeout"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state="timeout",
                wait_kind="path_exists",
                wait_target=path,
                elapsed_seconds=elapsed,
                detail=f"timeout after {timeout_sec}s",
            )
            sensor_events.append(evt)
            return False, f"path_exists timeout after {timeout_sec}s"
        time.sleep(poll_sec)


def _wait_for_path_glob(
    *,
    base: str,
    pattern: str,
    poll_sec: float,
    timeout_sec: float,
    run_id: str,
    started_at_iso: str,
    job_name: str,
    sensor_events: list[dict[str, Any]],
    poll_gauge: dict[tuple[str, str], int],
) -> tuple[bool, str]:
    start = time.monotonic()
    poll_index = 0
    target_repr = f"base={base} pattern={pattern}"
    while True:
        poll_index += 1
        elapsed = time.monotonic() - start
        try:
            matches = path_glob(base, pattern)
        except Exception as exc:
            state = "error"
            detail = f"path_glob raised: {type(exc).__name__}: {exc}"
            poll_gauge[(job_name, "error")] = poll_gauge.get((job_name, "error"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="path_glob",
                wait_target=target_repr,
                elapsed_seconds=elapsed,
                detail=detail,
            )
            sensor_events.append(evt)
        else:
            if matches:
                state = "satisfied"
                poll_gauge[(job_name, "satisfied")] = (
                    poll_gauge.get((job_name, "satisfied"), 0) + 1
                )
                evt = _emit_sensor_poll_log(
                    run_id=run_id,
                    started_at_iso=started_at_iso,
                    job_name=job_name,
                    poll_index=poll_index,
                    state=state,
                    wait_kind="path_glob",
                    wait_target=target_repr,
                    elapsed_seconds=elapsed,
                    detail=f"{len(matches)} matches found",
                )
                sensor_events.append(evt)
                return True, f"path_glob satisfied ({len(matches)} matches)"
            state = "polling"
            poll_gauge[(job_name, "polling")] = poll_gauge.get((job_name, "polling"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="path_glob",
                wait_target=target_repr,
                elapsed_seconds=elapsed,
                detail="no matches yet",
            )
            sensor_events.append(evt)
        if elapsed >= timeout_sec:
            poll_gauge[(job_name, "timeout")] = poll_gauge.get((job_name, "timeout"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state="timeout",
                wait_kind="path_glob",
                wait_target=target_repr,
                elapsed_seconds=elapsed,
                detail=f"timeout after {timeout_sec}s",
            )
            sensor_events.append(evt)
            return False, f"path_glob timeout after {timeout_sec}s"
        time.sleep(poll_sec)


def _http_get_status(url: str, timeout: float) -> int:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def _wait_for_http_2xx(
    *,
    url: str,
    poll_sec: float,
    timeout_sec: float,
    run_id: str,
    started_at_iso: str,
    job_name: str,
    sensor_events: list[dict[str, Any]],
    poll_gauge: dict[tuple[str, str], int],
) -> tuple[bool, str]:
    start = time.monotonic()
    poll_index = 0
    backoff_base = poll_sec
    attempt_for_backoff = 0
    while True:
        poll_index += 1
        attempt_for_backoff += 1
        elapsed = time.monotonic() - start
        status = -1
        try:
            req_timeout = min(30.0, max(5.0, poll_sec * 2))
            status = _http_get_status(url, req_timeout)
        except urllib.error.HTTPError as exc:
            status = exc.code
        except Exception as exc:
            state = "error"
            detail = f"HTTP raised: {type(exc).__name__}: {exc}"
            poll_gauge[(job_name, "error")] = poll_gauge.get((job_name, "error"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="http_url",
                wait_target=url,
                elapsed_seconds=elapsed,
                detail=detail,
            )
            sensor_events.append(evt)
        if status != -1:
            if 200 <= status < 300:
                state = "satisfied"
                poll_gauge[(job_name, "satisfied")] = (
                    poll_gauge.get((job_name, "satisfied"), 0) + 1
                )
                evt = _emit_sensor_poll_log(
                    run_id=run_id,
                    started_at_iso=started_at_iso,
                    job_name=job_name,
                    poll_index=poll_index,
                    state=state,
                    wait_kind="http_url",
                    wait_target=url,
                    elapsed_seconds=elapsed,
                    detail=f"HTTP {status}",
                )
                sensor_events.append(evt)
                return True, f"HTTP 2xx satisfied (status={status})"
            state = "polling"
            poll_gauge[(job_name, "polling")] = poll_gauge.get((job_name, "polling"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state=state,
                wait_kind="http_url",
                wait_target=url,
                elapsed_seconds=elapsed,
                detail=f"HTTP {status} (not 2xx)",
            )
            sensor_events.append(evt)
        if elapsed >= timeout_sec:
            poll_gauge[(job_name, "timeout")] = poll_gauge.get((job_name, "timeout"), 0) + 1
            evt = _emit_sensor_poll_log(
                run_id=run_id,
                started_at_iso=started_at_iso,
                job_name=job_name,
                poll_index=poll_index,
                state="timeout",
                wait_kind="http_url",
                wait_target=url,
                elapsed_seconds=elapsed,
                detail=f"timeout after {timeout_sec}s",
            )
            sensor_events.append(evt)
            return False, f"http_url timeout after {timeout_sec}s"
        jitter = random.uniform(0.0, 0.5 * backoff_base)
        sleep_s = min(poll_sec * 30.0, backoff_base * (2 ** (attempt_for_backoff - 1))) + jitter
        sleep_s = min(sleep_s, max(0.1, timeout_sec - elapsed - 0.001))
        if sleep_s > 0:
            time.sleep(sleep_s)


def _run_schedule_sensor_wait(
    *,
    job_name: str,
    wait_for: WaitForSpec,
    run_id: str,
    started_at_iso: str,
    sensor_events: list[dict[str, Any]],
    poll_gauge: dict[tuple[str, str], int],
) -> tuple[bool, str, str, str]:
    if wait_for.path_exists is not None:
        ok, reason = _wait_for_path_exists(
            path=wait_for.path_exists,
            poll_sec=wait_for.poll_sec,
            timeout_sec=wait_for.timeout_sec,
            run_id=run_id,
            started_at_iso=started_at_iso,
            job_name=job_name,
            sensor_events=sensor_events,
            poll_gauge=poll_gauge,
        )
        return ok, "path_exists", wait_for.path_exists, reason
    if wait_for.path_glob is not None:
        ok, reason = _wait_for_path_glob(
            base=wait_for.path_glob["base"],
            pattern=wait_for.path_glob["pattern"],
            poll_sec=wait_for.poll_sec,
            timeout_sec=wait_for.timeout_sec,
            run_id=run_id,
            started_at_iso=started_at_iso,
            job_name=job_name,
            sensor_events=sensor_events,
            poll_gauge=poll_gauge,
        )
        target_repr = f"base={wait_for.path_glob['base']} pattern={wait_for.path_glob['pattern']}"
        return ok, "path_glob", target_repr, reason
    if wait_for.http_url is not None:
        ok, reason = _wait_for_http_2xx(
            url=wait_for.http_url,
            poll_sec=wait_for.poll_sec,
            timeout_sec=wait_for.timeout_sec,
            run_id=run_id,
            started_at_iso=started_at_iso,
            job_name=job_name,
            sensor_events=sensor_events,
            poll_gauge=poll_gauge,
        )
        return ok, "http_url", wait_for.http_url, reason
    return False, "unknown", "", "wait_for kind not set"


def _build_sensor_metric_points(
    *,
    poll_gauge: dict[tuple[str, str], int],
    run_id: str,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for (job, state), count in poll_gauge.items():
        if count <= 0:
            continue
        mp = MetricPoint(
            metric_name="elt_sensor_poll_count",
            metric_type=MetricType.gauge,
            value=count,
            labels={"job": job, "state": state},
            run_id=run_id,
            stage="schedule",
            job_name=job,
        )
        points.append(mp.model_dump(mode="json"))
    return points


def _run_schedule_plan(
    *,
    plan: SchedulePlan,
    plan_path: str,
    continue_on_error: bool,
    run_id: str,
    started_at_iso: str,
) -> tuple[dict[str, Any], int]:
    ordered_names = topological_sort_schedule_jobs(plan.jobs)
    by_name = {job.name: job for job in plan.jobs}
    completed: set[str] = set()
    failed: set[str] = set()
    position_by_name = {job.name: i for i, job in enumerate(plan.jobs, start=1)}

    job_results: list[dict[str, Any]] = []
    skipped_jobs: list[dict[str, Any]] = []
    overall_exit_code = 0
    stop_after_this_job: str | None = None

    sensor_events: list[dict[str, Any]] = []
    poll_gauge: dict[tuple[str, str], int] = {}
    sla_alerts: list[dict[str, Any]] = []
    plan_start_dt = datetime.fromisoformat(started_at_iso)
    if plan_start_dt.tzinfo is None:
        plan_start_dt = plan_start_dt.replace(tzinfo=UTC)

    for name in ordered_names:
        job = by_name[name]
        position = position_by_name[name]
        unmet_deps = [dep for dep in job.depends_on if dep not in completed]
        failed_deps = [dep for dep in job.depends_on if dep in failed]
        if stop_after_this_job is not None:
            skipped_jobs.append(
                {
                    "name": job.name,
                    "position": position,
                    "argv": job.argv,
                    "status": "skipped_stop_on_error",
                    "depends_on": list(job.depends_on),
                    "stopped_after_job_failed": stop_after_this_job,
                }
            )
            continue
        if failed_deps and not continue_on_error:
            skipped_jobs.append(
                {
                    "name": job.name,
                    "position": position,
                    "argv": job.argv,
                    "status": "skipped_upstream_failure",
                    "depends_on": list(job.depends_on),
                    "skipped_because_failed_dependencies": failed_deps,
                }
            )
            continue
        if unmet_deps:
            skipped_jobs.append(
                {
                    "name": job.name,
                    "position": position,
                    "argv": job.argv,
                    "status": "skipped_unmet_dependencies",
                    "depends_on": list(job.depends_on),
                    "skipped_because_missing_dependencies": unmet_deps,
                }
            )
            continue

        wait_ok = True
        wait_kind = ""
        wait_target = ""
        wait_reason = ""
        if job.wait_for is not None:
            wait_ok, wait_kind, wait_target, wait_reason = _run_schedule_sensor_wait(
                job_name=job.name,
                wait_for=job.wait_for,
                run_id=run_id,
                started_at_iso=started_at_iso,
                sensor_events=sensor_events,
                poll_gauge=poll_gauge,
            )
            if not wait_ok:
                failed.add(job.name)
                status = "failed_sensor"
                if overall_exit_code == 0:
                    overall_exit_code = 5
                if not continue_on_error:
                    stop_after_this_job = job.name
                job_results.append(
                    {
                        "name": job.name,
                        "position": position,
                        "argv": job.argv,
                        "status": status,
                        "exit_code": 5,
                        "attempts": [],
                        "attempt_count": 0,
                        "retries_requested": job.retries,
                        "retry_delay_seconds": job.retry_delay_seconds,
                        "depends_on": list(job.depends_on),
                        "wait_for": {
                            "kind": wait_kind,
                            "target": wait_target,
                            "poll_sec": job.wait_for.poll_sec,
                            "timeout_sec": job.wait_for.timeout_sec,
                            "satisfied": False,
                            "reason": wait_reason,
                        },
                        "output": None,
                        "error": {"sensor_failure": wait_reason},
                    }
                )
                continue

        attempts: list[dict[str, Any]] = []
        final_exit_code: int = 1
        final_stdout_text = ""
        final_stderr_text = ""
        max_attempts = 1 + max(0, job.retries)

        import elt_pipeline.cli as _cli_facade

        job_start_mono = time.monotonic()
        job_start_wall = datetime.now(UTC)

        for attempt in range(1, max_attempts + 1):
            if attempt > 1 and job.retry_delay_seconds > 0:
                time.sleep(job.retry_delay_seconds)
            exit_code, stdout_text, stderr_text = _cli_facade._invoke_cli_job(job.argv)
            attempts.append(
                {
                    "attempt": attempt,
                    "exit_code": exit_code,
                    "output": parse_schedule_payload(stdout_text),
                    "error": parse_schedule_payload(stderr_text),
                }
            )
            final_exit_code = exit_code
            final_stdout_text = stdout_text
            final_stderr_text = stderr_text
            if exit_code == 0:
                break

        job_end_mono = time.monotonic()
        job_elapsed = job_end_mono - job_start_mono

        sla_breached = False
        if job.sla_seconds is not None and job_elapsed > float(job.sla_seconds):
            sla_breached = True
            labels = {
                "job": job.name,
                "sla_seconds": str(job.sla_seconds),
                "elapsed_seconds": f"{job_elapsed:.2f}",
                "stage": "schedule",
            }
            alert = AlertEvent(
                severity=AlertSeverity.warning,
                message=(
                    f"Schedule job SLA breached: job={job.name} "
                    f"elapsed={job_elapsed:.2f}s > sla={job.sla_seconds}s"
                ),
                labels=labels,
                run_id=run_id,
                stage="schedule",
                job_name=job.name,
            )
            sla_alerts.append(alert.model_dump(mode="json"))
            alert_log = {
                "run_id": run_id,
                "severity": "WARNING",
                "component": "schedule.sla",
                "event_type": "sla_breached",
                "message": (
                    f"SLA breached job={job.name} elapsed={job_elapsed:.2f}s "
                    f"> sla={job.sla_seconds}s"
                ),
                "timestamp": datetime.now(UTC).isoformat(),
                "details": {
                    "job": job.name,
                    "elapsed_seconds": round(job_elapsed, 3),
                    "sla_seconds": job.sla_seconds,
                    "sla_breached": True,
                },
            }
            print(json.dumps(alert_log, sort_keys=True, default=str), file=sys.stderr)

        if final_exit_code == 0:
            completed.add(job.name)
            status = "success"
        else:
            failed.add(job.name)
            status = "failed"
            if overall_exit_code == 0:
                overall_exit_code = final_exit_code
            if not continue_on_error:
                stop_after_this_job = job.name

        result_row: dict[str, Any] = {
            "name": job.name,
            "position": position,
            "argv": job.argv,
            "status": status,
            "exit_code": final_exit_code,
            "attempts": attempts,
            "attempt_count": len(attempts),
            "retries_requested": job.retries,
            "retry_delay_seconds": job.retry_delay_seconds,
            "depends_on": list(job.depends_on),
            "elapsed_seconds": round(job_elapsed, 3),
            "started_at_iso": job_start_wall.isoformat(),
            "finished_at_iso": datetime.now(UTC).isoformat(),
            "output": parse_schedule_payload(final_stdout_text),
            "error": parse_schedule_payload(final_stderr_text),
        }
        if job.sla_seconds is not None:
            result_row["sla_seconds"] = job.sla_seconds
            result_row["sla_breached"] = sla_breached
        if job.wait_for is not None:
            result_row["wait_for"] = {
                "kind": wait_kind,
                "target": wait_target,
                "poll_sec": job.wait_for.poll_sec,
                "timeout_sec": job.wait_for.timeout_sec,
                "satisfied": wait_ok,
                "reason": wait_reason,
            }
        job_results.append(result_row)

    sensor_metric_points = _build_sensor_metric_points(
        poll_gauge=poll_gauge,
        run_id=run_id,
    )

    finished_at_iso = datetime.now(UTC).isoformat()
    success_count = sum(1 for jr in job_results if jr["status"] == "success")
    failed_count = sum(
        1 for jr in job_results if jr["status"] in {"failed", "failed_sensor"}
    )
    skipped_count = len(skipped_jobs)
    return (
        {
            "command": "schedule.run",
            "plan_path": str(plan_path),
            "run_id": run_id,
            "started_at_iso": started_at_iso,
            "finished_at_iso": finished_at_iso,
            "execution_order": ordered_names,
            "job_count": len(plan.jobs),
            "executed_count": len(job_results),
            "skipped_count": skipped_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "continue_on_error": continue_on_error,
            "success": overall_exit_code == 0,
            "jobs": job_results,
            "skipped_jobs": skipped_jobs,
            "sensor_events": sensor_events,
            "sensor_metric_points": sensor_metric_points,
            "sla_alerts": sla_alerts,
        },
        overall_exit_code,
    )


def _invoke_cli_job(argv: list[str]) -> tuple[int, str, str]:
    import subprocess as _sp

    result = _sp.run(
        [sys.executable, "-m", "elt_pipeline", *argv],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def _build_maintenance_config(args: Any) -> MaintenanceConfig:
    explicit: dict[str, bool] = {}
    if getattr(args, "maintain_do_compact", None) is not None:
        explicit[MaintenanceOperation.compact.value] = bool(args.maintain_do_compact)
    if getattr(args, "maintain_do_expire", None) is not None:
        explicit[MaintenanceOperation.expire_snapshots.value] = bool(args.maintain_do_expire)
    if getattr(args, "maintain_do_orphans", None) is not None:
        explicit[MaintenanceOperation.remove_orphans.value] = bool(args.maintain_do_orphans)
    if getattr(args, "maintain_do_manifests", None) is not None:
        explicit[MaintenanceOperation.rewrite_manifests.value] = bool(args.maintain_do_manifests)

    only_raw = getattr(args, "maintain_only", None)
    if only_raw and only_raw.strip():
        names = [part.strip() for part in only_raw.split(",") if part.strip()]
        valid = {op.value for op in MaintenanceOperation}
        invalid = [n for n in names if n not in valid]
        if invalid:
            raise ConfigValidationError(
                message=(
                    "maintain --only: unknown operation(s): "
                    + ", ".join(invalid)
                    + ". Valid: "
                    + ", ".join(sorted(valid))
                ),
                context={"--only": only_raw, "invalid": invalid},
            )
        chosen_ops: list[MaintenanceOperation] = [MaintenanceOperation(n) for n in names]
    else:
        chosen_ops = list(DEFAULT_OPERATIONS)
    for name, enabled in explicit.items():
        op = MaintenanceOperation(name)
        if enabled and op not in chosen_ops:
            chosen_ops.append(op)
        elif not enabled and op in chosen_ops:
            chosen_ops.remove(op)

    if not chosen_ops:
        raise ConfigValidationError(
            message="maintain: zero operations selected. Use --only or the --<op> flags."
        )

    target_mb = getattr(args, "compact_target_file_size_mb", None)
    target_bytes: int | None = None
    if target_mb is not None:
        mb_int = int(target_mb)
        if mb_int <= 0:
            raise ConfigValidationError(
                message="--compact-target-file-size-mb must be a positive integer",
                context={"compact_target_file_size_mb": target_mb},
            )
        target_bytes = mb_int * 1024 * 1024

    return MaintenanceConfig(
        table_fqns=list(getattr(args, "maintain_tables", None) or []),
        all_level3=bool(getattr(args, "maintain_all_level3", False)),
        all_level4=bool(getattr(args, "maintain_all_level4", False)),
        operations=tuple(chosen_ops),
        snapshot_retain_days=(
            int(args.snapshot_retain_days)
            if getattr(args, "snapshot_retain_days", None) is not None
            else 7
        ),
        snapshot_retain_last=(
            max(1, int(args.snapshot_retain_last))
            if getattr(args, "snapshot_retain_last", None) is not None
            else 1
        ),
        orphan_older_than_days=(
            max(0, int(args.orphan_older_than_days))
            if getattr(args, "orphan_older_than_days", None) is not None
            else 3
        ),
        compact_strategy=(
            str(args.compact_strategy)
            if getattr(args, "compact_strategy", None)
            else "binpack"
        ),
        compact_min_input_files=(
            max(1, int(args.compact_min_input_files))
            if getattr(args, "compact_min_input_files", None) is not None
            else 5
        ),
        compact_target_file_size_bytes=target_bytes,
        dry_run=bool(getattr(args, "maintain_dry_run", False)),
    )


def _parse_vars_json(raw_value: str | None) -> dict[str, Any]:
    if raw_value is None:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse --vars-json payload: {exc}",
            context={"vars_json": raw_value},
        ) from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError(
            message="--vars-json must decode to a JSON object",
            context={"vars_json": raw_value},
        )
    return payload


def _parse_partition_values(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        key, separator, raw_partition_value = value.partition("=")
        if not separator or not key.strip():
            raise ConfigValidationError(
                message="Partition values must use key=value format",
                context={"partition": value},
            )
        parsed[key.strip()] = raw_partition_value
    return parsed


def _parse_datetime_argument(*, field_name: str, raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized:
        raise ConfigValidationError(
            message=f"{field_name} must not be empty",
            context={field_name: raw_value},
        )
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfigValidationError(
            message=f"{field_name} must be an ISO-8601 date or datetime",
            context={field_name: raw_value},
        ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _build_cli_window_selection(
    *,
    window_start: str | None,
    window_end: str | None,
    window_label: str | None,
    backfill: bool,
) -> ExecutionWindow:
    start = _parse_datetime_argument(field_name="window_start", raw_value=window_start)
    end = _parse_datetime_argument(field_name="window_end", raw_value=window_end)
    if backfill and start is None:
        raise ConfigValidationError(
            message="--backfill requires --window-start",
            context={"window_start": window_start},
        )
    label = window_label.strip() if window_label and window_label.strip() else None
    try:
        return ExecutionWindow(start=start, end=end, label=label)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="window_end must be greater than or equal to window_start",
            context={
                "window_start": window_start,
                "window_end": window_end,
            },
        ) from exc


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _validate_normalize_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.source:
        conflicting_args.append("--source")
    if args.entity:
        conflicting_args.append("--entity")
    if args.manifest_path:
        conflicting_args.append("--manifest-path")
    if args.window_start:
        conflicting_args.append("--window-start")
    if args.window_end:
        conflicting_args.append("--window-end")
    if args.window_label:
        conflicting_args.append("--window-label")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message=(
                "normalize reruns must not specify an explicit selection "
                "alongside --rerun-run-id"
            ),
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _validate_sql_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.stage:
        conflicting_args.append("--stage")
    if args.domain:
        conflicting_args.append("--domain")
    if args.model:
        conflicting_args.append("--model")
    if args.include_deps:
        conflicting_args.append("--include-deps")
    if args.start_date:
        conflicting_args.append("--start-date")
    if args.end_date:
        conflicting_args.append("--end-date")
    if args.vars_json:
        conflicting_args.append("--vars-json")
    if args.partition:
        conflicting_args.append("--partition")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message="sql reruns must not specify an explicit selection alongside --rerun-run-id",
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _validate_publish_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.domain:
        conflicting_args.append("--domain")
    if args.publish_name:
        conflicting_args.append("--publish")
    if args.window_start:
        conflicting_args.append("--window-start")
    if args.window_end:
        conflicting_args.append("--window-end")
    if args.window_label:
        conflicting_args.append("--window-label")
    if args.backfill:
        conflicting_args.append("--backfill")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message=(
                "publish reruns must not specify an explicit selection "
                "alongside --rerun-run-id"
            ),
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _resolve_entity_selections(
    config: PipelineConfig,
    *,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
    config_path: str | None = None,
) -> list[ResolvedEntityConfig]:
    if entity_name and not source_name:
        raise ConfigValidationError(
            message="--entity requires --source",
            context={"entity_name": entity_name},
        )

    if source_name:
        try:
            source = config.get_source(source_name)
        except LookupError as exc:
            raise ConfigValidationError(
                message=str(exc),
                context={"source_name": source_name},
            ) from exc
        entities = [entity.name for entity in source.entities]
        if entity_name is not None:
            entities = [entity_name]
        return [
            resolve_entity_config(
                config,
                environment=environment,
                source_name=source_name,
                entity_name=selected_entity_name,
                config_path=config_path,
            )
            for selected_entity_name in entities
        ]

    return [
        resolve_entity_config(
            config,
            environment=environment,
            source_name=source.name,
            entity_name=entity.name,
            config_path=config_path,
        )
        for source in config.sources
        for entity in source.entities
    ]


def _run_ingest_entity(
    *,
    resolved_config: ResolvedEntityConfig,
    root_path: str,
    job_name: str,
    trigger_type: str,
    kafka_log_path: str | None,
    cli_window: ExecutionWindow,
    backfill: bool,
) -> dict[str, Any]:
    connector_type = resolved_config.connector_type
    root_path = path_normalize(root_path)
    checkpoint_override = _resolve_checkpoint_override(
        root_path=root_path,
        resolved_config=resolved_config,
        window=cli_window,
        backfill=backfill,
    )
    try:
        run_context = build_job_runtime(
            stage=StageName.ingest,
            job_name=job_name,
            environment=resolved_config.environment,
            trigger_type=trigger_type,
            source_name=resolved_config.source_name,
            entity_name=resolved_config.entity_name,
            window=cli_window,
            backfill=backfill,
            checkpoint_seed=checkpoint_override.value,
            attributes=_with_orchestration_attributes(
                {
                    "connector_type": connector_type,
                    "backfill": backfill,
                }
            ),
        ).to_run_context()
    except ValueError as exc:
        raise ConfigValidationError(
            message=str(exc),
            context={
                "job_name": job_name,
                "trigger_type": trigger_type,
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
            },
        ) from exc

    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
    observability_adapter = build_observability_adapter(root_path)
    artifact_store.append_log_event(
        run_context=run_context,
        environment=resolved_config.environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="ingest",
            event_type="ingest_run_start",
            message="Ingest run started",
            details={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
                "connector_type": connector_type,
                "window_start": _serialize_datetime(cli_window.start),
                "window_end": _serialize_datetime(cli_window.end),
                "window_label": cli_window.label,
                "backfill": backfill,
            },
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=resolved_config.environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="source",
                    name=f"{resolved_config.source_name}/{resolved_config.entity_name}",
                    facets={"connector_type": connector_type},
                )
            ],
        ),
    )

    completed_at: datetime | None = None
    status = "success"
    error_summary: dict[str, str] | None = None
    failure: PipelineError | None = None
    result = None

    connector_manifest = _load_connector_manifest_from_env()
    if connector_manifest is not None:
        resolved_config = apply_connector_preset_defaults(resolved_config, connector_manifest)

    try:
        if connector_type == "rest":
            factory = get_connector_factory("rest")
            validated_config = factory.build_config_from_resolved(resolved_config=resolved_config)
            result = _CliLocalRestConnector(
                config=validated_config,
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "sql":
            factory = get_connector_factory("sql")
            validated_config = factory.build_config_from_resolved(resolved_config=resolved_config)
            result = _CliLocalSqlConnector(
                config=validated_config,
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "object_storage":
            factory = get_connector_factory("object_storage")
            validated_config = factory.build_config_from_resolved(resolved_config=resolved_config)
            result = _CliLocalObjectStorageConnector(
                config=validated_config,
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "kafka":
            factory = get_connector_factory("kafka")
            validated_config = factory.build_config_from_resolved(resolved_config=resolved_config)
            if validated_config.bootstrap_servers is not None:
                result = _CliBrokerKafkaConnector(
                    config=validated_config,
                    run_context=run_context,
                    root_path=root_path,
                    checkpoint_override=checkpoint_override,
                    window=cli_window,
                ).run()
            else:
                result = _CliLocalKafkaConnector(
                    config=validated_config,
                    run_context=run_context,
                    root_path=root_path,
                    log_path=_resolve_kafka_log_path(
                        resolved_config=resolved_config,
                        explicit_log_path=kafka_log_path,
                    ),
                    checkpoint_override=checkpoint_override,
                    window=cli_window,
                ).run()
        else:
            raise ConfigValidationError(
                message=(
                    "Unsupported connector type for local ingest CLI. "
                    "Use one of the built-in families (rest/sql/kafka/object_storage) "
                    "or register a ConnectorFactory for a new family via "
                    "ingest.connectors.register_connector_factory()."
                ),
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": connector_type,
                    "builtin_families": sorted({f.value for f in ConnectorFamily}),
                },
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
        artifact_store.append_error_record(
            run_context=run_context,
            environment=resolved_config.environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=exc.error_code,
                error_category=exc.error_category,
                message=str(exc),
                retryable=exc.retryable,
                context=exc.context,
            ),
        )
    except Exception as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = PipelineError(
            message="Unexpected ingest failure",
            error_code="INGEST_UNEXPECTED_ERROR",
            error_category=ErrorCategory.unexpected_runtime_error,
            retryable=True,
            context={
                "exception_type": type(exc).__name__,
                "message": str(exc),
            },
        )
        error_summary = {
            "error_code": failure.error_code,
            "error_category": failure.error_category.value,
            "message": str(failure),
        }
        artifact_store.append_error_record(
            run_context=run_context,
            environment=resolved_config.environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=failure.error_code,
                error_category=failure.error_category,
                message=str(failure),
                retryable=failure.retryable,
                context=failure.context,
            ),
        )
    finally:
        manifests: list[Level1ArtifactManifest] = []
        if result is not None:
            manifests = list(getattr(result, "manifests", []) or [])

        total_record_estimates = [
            manifest.record_count_estimate
            for manifest in manifests
            if manifest.record_count_estimate is not None
        ]
        records_written = (
            sum(total_record_estimates) if total_record_estimates else None
        )
        metrics_extra: dict[str, int | float | str] = {
            "connector_type": connector_type,
            "backfill": str(backfill).lower(),
        }
        for field_name in (
            "request_count",
            "response_count",
            "query_count",
            "row_count",
            "message_count",
            "objects_discovered",
            "objects_copied",
            "bytes_copied",
        ):
            value = getattr(result, field_name, None) if result is not None else None
            if isinstance(value, (int, float)):
                metrics_extra[field_name] = value

        audit = AuditRecord(
            run_id=run_context.run_id,
            stage=run_context.stage.value,
            job_name=run_context.job_name,
            trigger_type=run_context.trigger_type,
            started_at=run_context.started_at,
            completed_at=completed_at,
            status=status,
            config_version=None,
            metrics_summary=MetricsSummary(
                records_read=records_written,
                records_written=records_written,
                files_written=len(manifests),
                extra=metrics_extra,
            ),
            error_summary=error_summary,
            context={
                "environment": resolved_config.environment,
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
                "connector_type": connector_type,
                "root_path": str(root_path),
                "window_start": _serialize_datetime(cli_window.start) or "",
                "window_end": _serialize_datetime(cli_window.end) or "",
                "window_label": cli_window.label or "",
                "checkpoint_seeded": str(checkpoint_override.active).lower(),
            },
        )
        artifact_store.write_audit_record(
            run_context=run_context,
            environment=resolved_config.environment,
            audit_record=audit,
        )
        observability_adapter.on_run_complete(
            run_context=run_context,
            environment=resolved_config.environment,
            audit_record=audit,
        )
        lineage_adapter.emit(
            run_context=run_context,
            environment=resolved_config.environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE" if status == "success" else "FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
                inputs=[
                    DatasetRef(
                        namespace="source",
                        name=f"{resolved_config.source_name}/{resolved_config.entity_name}",
                        facets={"connector_type": connector_type},
                    )
                ],
                outputs=[
                    DatasetRef(
                        namespace="local",
                        name=manifest.data_path,
                        facets={
                            "artifact_id": manifest.artifact_id,
                            "content_hash": manifest.content_hash,
                            "payload_format": manifest.payload_format,
                            "manifest_path": manifest.manifest_path,
                        },
                    )
                    for manifest in manifests
                ],
            ),
        )
        artifact_store.append_log_event(
            run_context=run_context,
            environment=resolved_config.environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO" if status == "success" else "ERROR",
                component="ingest",
                event_type="ingest_run_complete",
                message="Ingest run completed",
                details={
                    "status": status,
                    "artifact_count": len(manifests),
                },
            ),
        )

    if failure is not None:
        raise failure

    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": resolved_config.source_name,
        "entity_name": resolved_config.entity_name,
        "connector_type": connector_type,
        "window_start": _serialize_datetime(cli_window.start),
        "window_end": _serialize_datetime(cli_window.end),
        "window_label": cli_window.label,
        "backfill": backfill,
        "result": result.model_dump(mode="json"),
    }


def _resolve_kafka_log_path(
    *,
    resolved_config: ResolvedEntityConfig,
    explicit_log_path: str | None,
) -> str:
    if explicit_log_path is not None:
        return path_normalize(explicit_log_path)
    candidate = (
        resolved_config.extraction.get("log_path")
        or resolved_config.settings.get("log_path")
        or resolved_config.state.get("log_path")
    )
    if candidate:
        return path_normalize(str(candidate))
    if (
        resolved_config.extraction.get("bootstrap_servers") is not None
        or resolved_config.settings.get("bootstrap_servers") is not None
    ):
        return ""
    raise ConfigValidationError(
        message=(
            "Kafka local ingest requires log_path in config or --kafka-log-path. "
            "Set bootstrap_servers= in extraction config to use the real broker connector."
        ),
        context={
            "source_name": resolved_config.source_name,
            "entity_name": resolved_config.entity_name,
        },
    )


def _select_level1_manifests(
    *,
    root_path: str,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
    explicit_manifest_paths: list[str],
    window_start: datetime | None,
    window_end: datetime | None,
) -> list[Level1ArtifactManifest]:
    if entity_name and not source_name:
        raise ConfigValidationError(
            message="--entity requires --source",
            context={"entity_name": entity_name},
        )

    root_path = path_normalize(root_path)
    manifests = [
        _read_level1_manifest(path=manifest_path, root_path=root_path)
        for manifest_path in (
            explicit_manifest_paths
            if explicit_manifest_paths
            else sorted(path_rglob(join_paths(root_path, "level1"), "*.manifest.json"))
        )
    ]
    filtered_manifests = [
        manifest
        for manifest in manifests
        if manifest.environment == environment
        and (source_name is None or manifest.source_name == source_name)
        and (entity_name is None or manifest.entity_name == entity_name)
        and _manifest_matches_window(
            manifest=manifest,
            window_start=window_start,
            window_end=window_end,
        )
    ]
    if not filtered_manifests:
        raise ConfigValidationError(
            message="No level1 manifests matched the requested selection",
            context={
                "root_path": root_path,
                "environment": environment,
                "source": source_name,
                "entity": entity_name,
                "manifest_paths": explicit_manifest_paths,
                "window_start": _serialize_datetime(window_start),
                "window_end": _serialize_datetime(window_end),
            },
        )
    return filtered_manifests


def _read_level1_manifest(*, path: str, root_path: str) -> Level1ArtifactManifest:
    manifest_path = (
        path if detect_scheme(path) != _StorageScheme.local_unschemed or path.startswith("/")
        else join_paths(root_path, path)
    )
    if not path_exists(manifest_path):
        raise ConfigValidationError(
            message="Level1 manifest path does not exist",
            context={"manifest_path": manifest_path},
        )
    try:
        payload = json.loads(path_read_text(manifest_path, encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse level1 manifest JSON: {exc}",
            context={"manifest_path": manifest_path},
        ) from exc
    try:
        return Level1ArtifactManifest.model_validate(payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Level1 manifest validation failed",
            context={
                "manifest_path": manifest_path,
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def _resolve_normalize_rerun_manifest(
    *,
    root_path: str,
    rerun_run_id: str,
) -> Level1ArtifactManifest:
    root_path = path_normalize(root_path)
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.normalize,
        rerun_run_id=rerun_run_id,
    )
    manifest_path = audit.context.get("input_manifest_path")
    if manifest_path:
        return _read_level1_manifest(path=str(manifest_path), root_path=root_path)

    artifact_id = audit.context.get("input_artifact_id")
    if not artifact_id:
        raise ConfigValidationError(
            message="Normalize rerun audit is missing input_artifact_id",
            context={"rerun_run_id": rerun_run_id},
        )

    for candidate_path in sorted(
        path_rglob(join_paths(root_path, "level1"), "*.manifest.json"),
    ):
        manifest = _read_level1_manifest(path=candidate_path, root_path=root_path)
        if manifest.artifact_id == artifact_id:
            return manifest

    raise ConfigValidationError(
        message="Normalize rerun could not resolve the prior level1 manifest",
        context={"rerun_run_id": rerun_run_id, "input_artifact_id": artifact_id},
    )


def _resolve_sql_rerun_selection(
    *,
    root_path: str,
    rerun_run_id: str,
) -> _SqlRerunSelection:
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.sql,
        rerun_run_id=rerun_run_id,
    )
    environment = audit.context.get("environment")
    selected_models = [
        model_id
        for model_id in audit.context.get("selected_models", "").split(",")
        if model_id
    ]
    if not environment or not selected_models:
        raise ConfigValidationError(
            message="SQL rerun audit is missing the required selection context",
            context={"rerun_run_id": rerun_run_id},
        )
    return _SqlRerunSelection(
        environment=environment,
        model_ids=tuple(selected_models),
        start_date=audit.context.get("window_start") or None,
        end_date=audit.context.get("window_end") or None,
        partition_values=_load_json_string_dict(
            raw_value=audit.context.get("partition_values"),
            field_name="partition_values",
            rerun_run_id=rerun_run_id,
        ),
        extra_values=_load_json_object_dict(
            raw_value=audit.context.get("extra_values"),
            field_name="extra_values",
            rerun_run_id=rerun_run_id,
        ),
    )


def _resolve_publish_rerun_selection(
    *,
    root_path: str,
    rerun_run_id: str,
) -> _PublishRerunSelection:
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.publish,
        rerun_run_id=rerun_run_id,
    )
    environment = audit.context.get("environment")
    selected_publishes = [
        publish_id
        for publish_id in audit.context.get("selected_publish_ids", "").split(",")
        if publish_id
    ]
    if not environment or not selected_publishes:
        raise ConfigValidationError(
            message="Publish rerun audit is missing the required selection context",
            context={"rerun_run_id": rerun_run_id},
        )
    return _PublishRerunSelection(
        environment=environment,
        publish_ids=tuple(selected_publishes),
        window_start=audit.context.get("window_start") or None,
        window_end=audit.context.get("window_end") or None,
        window_label=audit.context.get("window_label") or None,
        backfill=(
            audit.trigger_type == "backfill"
            or audit.context.get("checkpoint_mode") == "backfill"
        ),
    )


def _load_stage_audit_record(
    *,
    root_path: str,
    stage: StageName,
    rerun_run_id: str,
) -> AuditRecord:
    stage_root = join_paths(path_normalize(root_path), "runs", f"stage={stage.value}")
    if not path_exists(stage_root):
        raise ConfigValidationError(
            message="No run artifacts exist for the requested stage",
            context={
                "root_path": path_normalize(root_path),
                "stage": stage.value,
                "rerun_run_id": rerun_run_id,
            },
        )
    audit_paths = sorted(
        path_rglob(
            stage_root,
            f"run_id={rerun_run_id}/audit.json",
        ),
    )
    if not audit_paths:
        raise ConfigValidationError(
            message="No prior run artifacts matched --rerun-run-id",
            context={
                "root_path": path_normalize(root_path),
                "stage": stage.value,
                "rerun_run_id": rerun_run_id,
            },
        )
    audit_path = audit_paths[0]
    try:
        payload = json.loads(path_read_text(audit_path, encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse audit artifact JSON: {exc}",
            context={"audit_path": audit_path, "rerun_run_id": rerun_run_id},
        ) from exc
    try:
        return AuditRecord.model_validate(payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Audit artifact validation failed",
            context={
                "audit_path": audit_path,
                "rerun_run_id": rerun_run_id,
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def _load_json_string_dict(
    *,
    raw_value: str | None,
    field_name: str,
    rerun_run_id: str,
) -> dict[str, str]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must contain valid JSON",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        ) from exc
    if not isinstance(payload, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must decode to a string map",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        )
    return payload


def _with_orchestration_attributes(
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    combined_attributes = dict(attributes or {})
    orchestration_metadata = load_orchestration_metadata_from_env()
    if orchestration_metadata is not None:
        combined_attributes.update(orchestration_metadata.to_run_attributes())
    return combined_attributes


def _load_json_object_dict(
    *,
    raw_value: str | None,
    field_name: str,
    rerun_run_id: str,
) -> dict[str, Any]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must contain valid JSON",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        ) from exc
    if not isinstance(payload, dict) or any(not isinstance(key, str) for key in payload):
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must decode to an object",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        )
    return payload


def _run_normalize_manifest(
    *,
    manifest: Level1ArtifactManifest,
    resolved_config: ResolvedEntityConfig,
    root_path: str,
    job_name: str,
    trigger_type: str,
    backfill: bool,
    rerun_run_id: str | None,
    partition_strategy: PartitionStrategy,
    spark: SparkSession,
) -> dict[str, Any]:
    try:
        run_context = build_job_runtime(
            stage=StageName.normalize,
            job_name=job_name,
            environment=manifest.environment,
            trigger_type=trigger_type,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            window=ExecutionWindow(
                start=manifest.window_start,
                end=manifest.window_end,
                label=manifest.window_label,
            ),
            backfill=backfill,
            attributes=_with_orchestration_attributes(
                {
                    "input_artifact_id": manifest.artifact_id,
                    "input_manifest_path": manifest.manifest_path,
                    "rerun_of_run_id": rerun_run_id or "",
                }
            ),
        ).to_run_context()
    except ValueError as exc:
        raise ConfigValidationError(
            message=str(exc),
            context={
                "job_name": job_name,
                "trigger_type": trigger_type,
                "source_name": manifest.source_name,
                "entity_name": manifest.entity_name,
                "input_artifact_id": manifest.artifact_id,
            },
        ) from exc
    level2_mode = resolved_config.level2_mode
    if level2_mode == "bypass_level2":
        return _bypass_normalize_manifest(
            manifest=manifest,
            root_path=path_normalize(root_path),
            run_context=run_context,
            rerun_run_id=rerun_run_id,
            level2_mode=level2_mode,
        )

    summary = normalize_level1_to_local_level2(
        root_path=path_normalize(root_path),
        run_context=run_context,
        manifest=manifest,
        payload=join_paths(path_normalize(root_path), manifest.data_path),
        spark=spark,
        partition_strategy=partition_strategy,
    )
    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": manifest.source_name,
        "entity_name": manifest.entity_name,
        "level2_mode": level2_mode,
        "bypassed": False,
        "input_artifact_id": manifest.artifact_id,
        "mapping_catalog_path": summary.mapping_catalog_path,
        "table_manifests": [
            table_manifest.model_dump(mode="json") for table_manifest in summary.table_manifests
        ],
    }


def _bypass_normalize_manifest(
    *,
    manifest: Level1ArtifactManifest,
    root_path: str,
    run_context,
    rerun_run_id: str | None,
    level2_mode: Level2Mode,
) -> dict[str, Any]:
    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
    observability_adapter = build_observability_adapter(root_path)

    artifact_store.append_log_event(
        run_context=run_context,
        environment=manifest.environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="normalize",
            event_type="normalize_bypassed",
            message="Physical level2 normalization was bypassed by configuration",
            details={
                "source_name": manifest.source_name,
                "entity_name": manifest.entity_name,
                "level2_mode": level2_mode,
                "input_artifact_id": manifest.artifact_id,
            },
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=manifest.environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
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

    audit = AuditRecord(
        run_id=run_context.run_id,
        stage=run_context.stage.value,
        job_name=run_context.job_name,
        trigger_type=run_context.trigger_type,
        started_at=run_context.started_at,
        completed_at=run_context.started_at,
        status="success",
        config_version=None,
        metrics_summary=MetricsSummary(
            records_read=manifest.record_count_estimate,
            records_written=manifest.record_count_estimate,
            files_written=0,
            extra={
                "level2_mode": level2_mode,
                "bypassed_level2": "true",
            },
        ),
        context={
            "environment": manifest.environment,
            "source_name": manifest.source_name,
            "entity_name": manifest.entity_name,
            "input_artifact_id": manifest.artifact_id,
            "input_manifest_path": manifest.manifest_path,
            "level2_mode": level2_mode,
            "bypassed": "true",
            "source_data_path": manifest.data_path,
        },
    )
    if rerun_run_id:
        audit.context["rerun_of_run_id"] = rerun_run_id
    artifact_store.write_audit_record(
        run_context=run_context,
        environment=manifest.environment,
        audit_record=audit,
    )
    observability_adapter.on_run_complete(
        run_context=run_context,
        environment=manifest.environment,
        audit_record=audit,
    )

    lineage_adapter.emit(
        run_context=run_context,
        environment=manifest.environment,
        lineage_event=LineageEvent(
            event_type="COMPLETE",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
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
                    name=manifest.data_path,
                    facets={
                        "artifact_id": manifest.artifact_id,
                        "level2_mode": level2_mode,
                        "bypassed": True,
                    },
                )
            ],
        ),
    )

    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": manifest.source_name,
        "entity_name": manifest.entity_name,
        "level2_mode": level2_mode,
        "bypassed": True,
        "input_artifact_id": manifest.artifact_id,
        "mapping_catalog_path": None,
        "table_manifests": [],
        "source_data_path": manifest.data_path,
        "source_manifest_path": manifest.manifest_path,
    }


def _manifest_matches_window(
    *,
    manifest: Level1ArtifactManifest,
    window_start: datetime | None,
    window_end: datetime | None,
) -> bool:
    if window_start is None and window_end is None:
        return True

    manifest_start = manifest.window_start or manifest.ingest_started_at
    manifest_end = manifest.window_end or manifest.ingest_completed_at
    if window_start is not None and manifest_end < window_start:
        return False
    if window_end is not None and manifest_start > window_end:
        return False
    return True


def _load_connector_manifest_from_env() -> object | None:
    env_ref = runtime_manifest.env
    manifest_path = os.getenv(env_ref.connector_registry_manifest)
    strict_raw = os.getenv(env_ref.connector_registry_strict, "").strip().lower()
    strict = strict_raw in {"1", "true", "yes", "on"}
    if manifest_path is None or not manifest_path.strip():
        return None
    manifest_path = manifest_path.strip()
    path_lower = manifest_path.lower()
    try:
        if path_lower.endswith((".yaml", ".yml")):
            return load_connector_manifest_from_yaml(manifest_path, cache=True)
        if path_lower.endswith(".json"):
            return load_connector_manifest_from_json(manifest_path, cache=True)
        try:
            return load_connector_manifest_from_yaml(manifest_path, cache=True)
        except Exception:
            return load_connector_manifest_from_json(manifest_path, cache=True)
    except ConnectorRegistryError:
        if strict:
            raise
        return None
    except Exception as exc:
        if strict:
            raise ConfigValidationError(
                message=f"Failed to load connector registry manifest from {manifest_path}: {exc}",
                context={"manifest_path": manifest_path, "strict": str(strict)},
            ) from exc
        return None


def _resolve_checkpoint_override(
    *,
    root_path: str,
    resolved_config: ResolvedEntityConfig,
    window: ExecutionWindow,
    backfill: bool,
) -> _CheckpointOverride:
    if not backfill:
        return _CheckpointOverride(active=False, value=None)

    checkpoint_store = LocalCheckpointStore(root_path)
    seed_entry = checkpoint_store.resolve_backfill_seed(
        environment=resolved_config.environment,
        source_name=resolved_config.source_name,
        entity_name=resolved_config.entity_name,
        window_start=window.start,
    )
    return _CheckpointOverride(
        active=True,
        value=seed_entry.checkpoint_after if seed_entry is not None else None,
    )

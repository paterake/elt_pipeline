from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config
from elt_pipeline.config.models import PipelineConfig, ResolvedEntityConfig
from elt_pipeline.ingest import (
    KafkaConnectorConfig,
    LocalKafkaConnector,
    LocalObjectStorageConnector,
    LocalSqlConnector,
    LocalRestConnector,
    ObjectStorageConnectorConfig,
    SqlConnectorConfig,
    RestConnectorConfig,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.partitioning import PartitionMode, PartitionStrategy
from elt_pipeline.normalize.pipeline import normalize_level1_to_local_level2
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError, build_error_record
from elt_pipeline.shared.runtime import StageName, new_run_context
from elt_pipeline.sql import (
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    LocalSqlModelExecutor,
    run_sql_models_locally,
    resolve_selected_model_ids,
    topologically_sort_sql_models,
)
from elt_pipeline.sql.models import SqlModelStage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="elt-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate-config",
        help="Validate a YAML configuration file and optionally resolve one source/entity.",
    )
    validate_parser.add_argument("config_path", type=Path)
    validate_parser.add_argument("--environment", default="default")
    validate_parser.add_argument("--source")
    validate_parser.add_argument("--entity")

    run_context_parser = subparsers.add_parser(
        "show-run-context",
        help="Create and display a runtime context object.",
    )
    run_context_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in StageName],
        required=True,
    )
    run_context_parser.add_argument("--job-name", required=True)
    run_context_parser.add_argument("--trigger-type", default="manual")

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Run configured ingestion sources and entities in local mode.",
    )
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", required=True)
    ingest_run_parser = ingest_subparsers.add_parser(
        "run",
        help="Run one or more configured source entities and persist raw level1 artifacts.",
    )
    ingest_run_parser.add_argument("config_path", type=Path)
    ingest_run_parser.add_argument("--environment", default="default")
    ingest_run_parser.add_argument("--source")
    ingest_run_parser.add_argument("--entity")
    ingest_run_parser.add_argument("--root-path", type=Path, default=Path.cwd())
    ingest_run_parser.add_argument("--job-name", default="ingest-run")
    ingest_run_parser.add_argument("--trigger-type", default="manual")
    ingest_run_parser.add_argument(
        "--kafka-log-path",
        type=Path,
        help="Optional override for local Kafka replay log input.",
    )

    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Run local level1 to level2 normalization.",
    )
    normalize_subparsers = normalize_parser.add_subparsers(
        dest="normalize_command",
        required=True,
    )
    normalize_run_parser = normalize_subparsers.add_parser(
        "run",
        help="Normalize level1 manifests into local level2 tables.",
    )
    normalize_run_parser.add_argument("config_path", type=Path)
    normalize_run_parser.add_argument("--environment", default="default")
    normalize_run_parser.add_argument("--source")
    normalize_run_parser.add_argument("--entity")
    normalize_run_parser.add_argument("--root-path", type=Path, default=Path.cwd())
    normalize_run_parser.add_argument("--job-name", default="normalize-run")
    normalize_run_parser.add_argument("--trigger-type", default="manual")
    normalize_run_parser.add_argument(
        "--manifest-path",
        action="append",
        default=[],
        type=Path,
        help="Explicit level1 manifest path to normalize. May be passed multiple times.",
    )
    normalize_run_parser.add_argument(
        "--partition-mode",
        choices=[mode.value for mode in PartitionMode],
        default=PartitionMode.ingest_date.value,
    )
    normalize_run_parser.add_argument("--partition-key")
    normalize_run_parser.add_argument("--metadata-key")

    sql_parser = subparsers.add_parser(
        "sql",
        help="Discover, compile, and run local SQL model packages.",
    )
    sql_subparsers = sql_parser.add_subparsers(dest="sql_command", required=True)

    compile_parser = sql_subparsers.add_parser(
        "compile",
        help="Compile SQL models with runtime tokens resolved.",
    )
    _add_sql_selection_arguments(compile_parser)
    compile_parser.add_argument("--include-deps", action="store_true")

    run_parser = sql_subparsers.add_parser(
        "run",
        help="Run SQL models against a local sqlite database.",
    )
    _add_sql_selection_arguments(run_parser)
    run_parser.add_argument("--include-deps", action="store_true")
    run_parser.add_argument("--database", type=Path, required=True)
    run_parser.add_argument("--job-name", default="sql-run")
    run_parser.add_argument("--trigger-type", default="manual")
    run_parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate compiled SQL against the target database without executing writes.",
    )
    run_parser.add_argument(
        "--explain",
        action="store_true",
        help="Include sqlite query plan details; implies a validate-only planning run.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "validate-config":
            config = load_pipeline_config(args.config_path)
            if args.source and args.entity:
                resolved = resolve_entity_config(
                    config,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
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
            )
            print(json.dumps(context.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "ingest":
            config = load_pipeline_config(args.config_path)
            selected_entities = _resolve_entity_selections(
                config,
                environment=args.environment,
                source_name=args.source,
                entity_name=args.entity,
            )
            results = [
                _run_ingest_entity(
                    resolved_config=resolved_config,
                    root_path=args.root_path,
                    job_name=args.job_name,
                    trigger_type=args.trigger_type,
                    kafka_log_path=args.kafka_log_path,
                )
                for resolved_config in selected_entities
            ]
            payload = {
                "command": "ingest.run",
                "environment": args.environment,
                "selection": {
                    "source": args.source,
                    "entity": args.entity,
                },
                "result_count": len(results),
                "results": results,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "normalize":
            load_pipeline_config(args.config_path)
            summaries = [
                _run_normalize_manifest(
                    manifest=manifest,
                    root_path=args.root_path,
                    job_name=args.job_name,
                    trigger_type=args.trigger_type,
                    partition_strategy=PartitionStrategy(
                        mode=PartitionMode(args.partition_mode),
                        partition_key=args.partition_key,
                        metadata_key=args.metadata_key,
                    ),
                )
                for manifest in _select_level1_manifests(
                    root_path=args.root_path,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
                    explicit_manifest_paths=args.manifest_path,
                )
            ]
            payload = {
                "command": "normalize.run",
                "environment": args.environment,
                "selection": {
                    "source": args.source,
                    "entity": args.entity,
                    "manifest_paths": [str(path) for path in args.manifest_path],
                },
                "processed_count": len(summaries),
                "results": summaries,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "sql":
            discovered_models = discover_sql_models(args.package_path)
            ordered_models = topologically_sort_sql_models(discovered_models)
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

            run_context = new_run_context(
                stage=StageName.sql,
                job_name=getattr(args, "job_name", "sql-compile"),
                trigger_type=getattr(args, "trigger_type", "manual"),
                attributes={
                    "environment": args.environment,
                    "package_path": str(args.package_path),
                    "stage_selection": args.stage or "",
                    "domain_selection": args.domain or "",
                    "model_selection": args.model or "",
                },
            )
            extra_values = _parse_vars_json(args.vars_json)
            partition_values = _parse_partition_values(args.partition)
            compiled_models = [
                compile_sql_model(
                    model,
                    token_context=build_token_context(
                        environment=args.environment,
                        run_id=run_context.run_id,
                        stage=model.manifest.stage.value,
                        domain=model.manifest.domain,
                        model_name=model.manifest.name,
                        target_table_name=model.manifest.target.table_name,
                        start_date=args.start_date,
                        end_date=args.end_date,
                        partition_values=partition_values,
                        extra_values=extra_values,
                    ),
                )
                for model in models_to_process
            ]

            if args.sql_command == "compile":
                payload = {
                    "run_id": run_context.run_id,
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
                if args.validate_only or args.explain:
                    planning_result = LocalSqlModelExecutor(
                        database_path=args.database,
                        partition_values=partition_values,
                    ).plan(
                        compiled_models,
                        include_query_plan=args.explain,
                    )
                    payload = {
                        "run_id": run_context.run_id,
                        "mode": "explain" if args.explain else "validate_only",
                        "database_path": str(planning_result.database_path),
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

                result = run_sql_models_locally(
                    root_path=_resolve_sql_artifact_root(
                        package_path=args.package_path,
                        database_path=args.database,
                    ),
                    run_context=run_context,
                    environment=args.environment,
                    package_path=args.package_path,
                    database_path=args.database,
                    compiled_models=compiled_models,
                    partition_values=partition_values,
                )
                payload = {
                    "run_id": run_context.run_id,
                    "database_path": str(result.execution_result.database_path),
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
                }
                print(json.dumps(payload, indent=2))
                return 0
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


def _add_sql_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("package_path", type=Path)
    parser.add_argument(
        "--stage",
        choices=[stage.value for stage in SqlModelStage],
    )
    parser.add_argument("--domain")
    parser.add_argument("--model")
    parser.add_argument("--environment", default="default")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--vars-json")
    parser.add_argument(
        "--partition",
        action="append",
        default=[],
        help="Partition override in key=value form. May be passed multiple times.",
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


def _resolve_sql_artifact_root(*, package_path: Path, database_path: Path) -> Path:
    try:
        common_root = os.path.commonpath([package_path.resolve(), database_path.resolve()])
    except ValueError:
        return Path.cwd()
    return Path(common_root)


def _resolve_entity_selections(
    config: PipelineConfig,
    *,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
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
            )
            for selected_entity_name in entities
        ]

    return [
        resolve_entity_config(
            config,
            environment=environment,
            source_name=source.name,
            entity_name=entity.name,
        )
        for source in config.sources
        for entity in source.entities
    ]


def _run_ingest_entity(
    *,
    resolved_config: ResolvedEntityConfig,
    root_path: Path,
    job_name: str,
    trigger_type: str,
    kafka_log_path: Path | None,
) -> dict[str, Any]:
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name=job_name,
        trigger_type=trigger_type,
        attributes={
            "environment": resolved_config.environment,
            "source_name": resolved_config.source_name,
            "entity_name": resolved_config.entity_name,
            "connector_type": resolved_config.connector_type,
        },
    )
    connector_type = resolved_config.connector_type
    root_path = root_path.resolve()

    if connector_type == "rest":
        result = LocalRestConnector(
            config=RestConnectorConfig.from_resolved_entity_config(resolved_config),
            run_context=run_context,
            root_path=root_path,
        ).run()
    elif connector_type == "sql":
        result = LocalSqlConnector(
            config=SqlConnectorConfig.from_resolved_entity_config(resolved_config),
            run_context=run_context,
            root_path=root_path,
        ).run()
    elif connector_type == "object_storage":
        result = LocalObjectStorageConnector(
            config=ObjectStorageConnectorConfig.from_resolved_entity_config(resolved_config),
            run_context=run_context,
            root_path=root_path,
        ).run()
    elif connector_type == "kafka":
        result = LocalKafkaConnector(
            config=KafkaConnectorConfig.from_resolved_entity_config(resolved_config),
            run_context=run_context,
            root_path=root_path,
            log_path=_resolve_kafka_log_path(
                resolved_config=resolved_config,
                explicit_log_path=kafka_log_path,
            ),
        ).run()
    else:
        raise ConfigValidationError(
            message="Unsupported connector type for local ingest CLI",
            context={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
                "connector_type": connector_type,
            },
        )

    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": resolved_config.source_name,
        "entity_name": resolved_config.entity_name,
        "connector_type": connector_type,
        "result": result.model_dump(mode="json"),
    }


def _resolve_kafka_log_path(
    *,
    resolved_config: ResolvedEntityConfig,
    explicit_log_path: Path | None,
) -> Path:
    if explicit_log_path is not None:
        return explicit_log_path.resolve()
    candidate = (
        resolved_config.extraction.get("log_path")
        or resolved_config.settings.get("log_path")
        or resolved_config.state.get("log_path")
    )
    if not candidate:
        raise ConfigValidationError(
            message="Kafka local ingest requires log_path in config or --kafka-log-path",
            context={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
            },
        )
    return Path(str(candidate)).resolve()


def _select_level1_manifests(
    *,
    root_path: Path,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
    explicit_manifest_paths: list[Path],
) -> list[Level1ArtifactManifest]:
    if entity_name and not source_name:
        raise ConfigValidationError(
            message="--entity requires --source",
            context={"entity_name": entity_name},
        )

    manifests = [
        _read_level1_manifest(path=manifest_path, root_path=root_path)
        for manifest_path in (
            explicit_manifest_paths
            if explicit_manifest_paths
            else sorted((root_path / "level1").rglob("*.manifest.json"))
        )
    ]
    filtered_manifests = [
        manifest
        for manifest in manifests
        if manifest.environment == environment
        and (source_name is None or manifest.source_name == source_name)
        and (entity_name is None or manifest.entity_name == entity_name)
    ]
    if not filtered_manifests:
        raise ConfigValidationError(
            message="No level1 manifests matched the requested selection",
            context={
                "root_path": str(root_path),
                "environment": environment,
                "source": source_name,
                "entity": entity_name,
                "manifest_paths": [str(path) for path in explicit_manifest_paths],
            },
        )
    return filtered_manifests


def _read_level1_manifest(*, path: Path, root_path: Path) -> Level1ArtifactManifest:
    manifest_path = path if path.is_absolute() else root_path / path
    if not manifest_path.exists():
        raise ConfigValidationError(
            message="Level1 manifest path does not exist",
            context={"manifest_path": str(manifest_path)},
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse level1 manifest JSON: {exc}",
            context={"manifest_path": str(manifest_path)},
        ) from exc
    try:
        return Level1ArtifactManifest.model_validate(payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Level1 manifest validation failed",
            context={
                "manifest_path": str(manifest_path),
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def _run_normalize_manifest(
    *,
    manifest: Level1ArtifactManifest,
    root_path: Path,
    job_name: str,
    trigger_type: str,
    partition_strategy: PartitionStrategy,
) -> dict[str, Any]:
    run_context = new_run_context(
        stage=StageName.normalize,
        job_name=job_name,
        trigger_type=trigger_type,
        attributes={
            "environment": manifest.environment,
            "source_name": manifest.source_name,
            "entity_name": manifest.entity_name,
            "input_artifact_id": manifest.artifact_id,
        },
    )
    summary = normalize_level1_to_local_level2(
        root_path=root_path.resolve(),
        run_context=run_context,
        manifest=manifest,
        payload=(root_path / manifest.data_path).resolve(),
        partition_strategy=partition_strategy,
    )
    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": manifest.source_name,
        "entity_name": manifest.entity_name,
        "input_artifact_id": manifest.artifact_id,
        "mapping_catalog_path": summary.mapping_catalog_path,
        "table_manifests": [
            table_manifest.model_dump(mode="json") for table_manifest in summary.table_manifests
        ],
    }

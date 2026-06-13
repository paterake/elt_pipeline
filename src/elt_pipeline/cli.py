from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError, build_error_record
from elt_pipeline.shared.runtime import StageName, new_run_context
from elt_pipeline.sql import (
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
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

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError, build_error_record
from elt_pipeline.shared.runtime import StageName, new_run_context


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

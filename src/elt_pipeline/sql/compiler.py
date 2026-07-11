from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.sql.models import CompiledSqlModel, DiscoveredSqlModel

_TOKEN_PATTERN = re.compile(r"\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}")


def compile_sql_model(
    model: DiscoveredSqlModel,
    *,
    token_context: Mapping[str, Any],
) -> CompiledSqlModel:
    token_values: dict[str, str] = {}

    def replace_token(match: re.Match[str]) -> str:
        token_name = match.group(1)
        resolved = _resolve_token_value(token_context, token_name, model=model)
        token_values[token_name] = resolved
        return resolved

    compiled_sql = _TOKEN_PATTERN.sub(replace_token, model.sql_text)
    return CompiledSqlModel(
        model_id=model.model_id,
        stage=model.manifest.stage,
        domain=model.manifest.domain,
        name=model.manifest.name,
        target_table_name=model.manifest.target.table_name,
        partition_columns=model.manifest.target.partition_columns,
        load_mode=model.manifest.load_mode,
        materialization=model.manifest.materialization,
        manifest_path=model.manifest_path,
        sql_path=model.sql_path,
        compiled_sql=compiled_sql,
        token_values=token_values,
        depends_on=model.manifest.depends_on,
        sources=model.manifest.sources,
        quality=model.manifest.quality,
    )


def build_token_context(
    *,
    environment: str,
    run_id: str,
    stage: str | None = None,
    domain: str | None = None,
    model_name: str | None = None,
    target_table_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    partition_values: dict[str, str] | None = None,
    extra_values: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "environment": environment,
        "run_id": run_id,
        "model": {
            "stage": stage or "",
            "domain": domain or "",
            "name": model_name or "",
            "target_table_name": target_table_name or "",
        },
        "window": {
            "start_date": start_date or "",
            "end_date": end_date or "",
        },
        "partition": partition_values or {},
    }
    if extra_values:
        context = _deep_merge(context, dict(extra_values))
    return context


def _resolve_token_value(
    token_context: Mapping[str, Any],
    token_name: str,
    *,
    model: DiscoveredSqlModel,
) -> str:
    current: Any = token_context
    for segment in token_name.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            raise PipelineError(
                message=f"Missing SQL compilation token '{token_name}'",
                error_code="SQL_COMPILATION_TOKEN_MISSING",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "sql_path": str(model.sql_path),
                    "token_name": token_name,
                },
            )
        current = current[segment]

    if isinstance(current, (dict, list)):
        raise PipelineError(
            message=f"SQL compilation token '{token_name}' resolved to a non-scalar value",
            error_code="SQL_COMPILATION_TOKEN_INVALID",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={
                "model_id": model.model_id,
                "sql_path": str(model.sql_path),
                "token_name": token_name,
            },
        )
    return "" if current is None else str(current)


def _deep_merge(left: dict[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged

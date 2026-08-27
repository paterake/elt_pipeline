from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from elt_pipeline.integrations.orchestration._models import (
    _ORCHESTRATION_FLOW_NAME_ENV,
    _ORCHESTRATION_FLOW_RUN_ID_ENV,
    _ORCHESTRATION_PLATFORM_ENV,
    _ORCHESTRATION_TAGS_ENV,
    _ORCHESTRATION_TASK_ATTEMPT_ENV,
    _ORCHESTRATION_TASK_NAME_ENV,
    OrchestrationMetadata,
    _coerce_optional_int,
    _coerce_optional_string,
    _coerce_tag_sequence,
    _normalize_env_value,
    _parse_tags,
    _parse_task_attempt,
    _value_or_attribute,
)
from elt_pipeline.shared.errors import ConfigValidationError


def load_orchestration_metadata_from_env(
    env: Mapping[str, str] | None = None,
) -> OrchestrationMetadata | None:
    source = env if env is not None else os.environ
    raw_values = {
        _ORCHESTRATION_PLATFORM_ENV: _normalize_env_value(
            source.get(_ORCHESTRATION_PLATFORM_ENV)
        ),
        _ORCHESTRATION_FLOW_NAME_ENV: _normalize_env_value(
            source.get(_ORCHESTRATION_FLOW_NAME_ENV)
        ),
        _ORCHESTRATION_FLOW_RUN_ID_ENV: _normalize_env_value(
            source.get(_ORCHESTRATION_FLOW_RUN_ID_ENV)
        ),
        _ORCHESTRATION_TASK_NAME_ENV: _normalize_env_value(
            source.get(_ORCHESTRATION_TASK_NAME_ENV)
        ),
        _ORCHESTRATION_TASK_ATTEMPT_ENV: _normalize_env_value(
            source.get(_ORCHESTRATION_TASK_ATTEMPT_ENV)
        ),
        _ORCHESTRATION_TAGS_ENV: _normalize_env_value(source.get(_ORCHESTRATION_TAGS_ENV)),
    }
    configured_fields = sorted(
        field_name for field_name, field_value in raw_values.items() if field_value is not None
    )
    if not configured_fields:
        return None

    platform = raw_values[_ORCHESTRATION_PLATFORM_ENV]
    if platform is None:
        raise ConfigValidationError(
            message=(
                "Orchestration metadata must define "
                f"'{_ORCHESTRATION_PLATFORM_ENV}' when any orchestration metadata is set"
            ),
            context={"configured_fields": configured_fields},
        )

    task_attempt = _parse_task_attempt(
        raw_value=raw_values[_ORCHESTRATION_TASK_ATTEMPT_ENV],
        configured_fields=configured_fields,
    )
    tags = _parse_tags(
        raw_value=raw_values[_ORCHESTRATION_TAGS_ENV],
        configured_fields=configured_fields,
    )
    return OrchestrationMetadata(
        platform=platform,
        flow_name=raw_values[_ORCHESTRATION_FLOW_NAME_ENV],
        flow_run_id=raw_values[_ORCHESTRATION_FLOW_RUN_ID_ENV],
        task_name=raw_values[_ORCHESTRATION_TASK_NAME_ENV],
        task_attempt=task_attempt,
        tags=tags,
    )


def build_airflow_orchestration_metadata(
    context: Mapping[str, Any] | None = None,
) -> OrchestrationMetadata:
    source = context or {}
    dag = source.get("dag")
    dag_run = source.get("dag_run")
    task = source.get("task")
    task_instance = source.get("task_instance") or source.get("ti")

    dag_id = _coerce_optional_string(
        _value_or_attribute(source.get("dag_id"), dag, "dag_id")
    )
    flow_run_id = _coerce_optional_string(
        _value_or_attribute(source.get("run_id"), dag_run, "run_id")
    )
    task_id = _coerce_optional_string(
        _value_or_attribute(source.get("task_id"), task, "task_id")
        or _value_or_attribute(None, task_instance, "task_id")
    )
    try_number = _coerce_optional_int(
        _value_or_attribute(source.get("try_number"), task_instance, "try_number")
    )

    tags: dict[str, str] = {}
    dag_tags = _coerce_tag_sequence(_value_or_attribute(None, dag, "tags"))
    if dag_tags:
        tags["dag_tags"] = ",".join(dag_tags)
    logical_date = _coerce_optional_string(source.get("logical_date"))
    if logical_date is not None:
        tags["logical_date"] = logical_date

    return OrchestrationMetadata(
        platform="airflow",
        flow_name=dag_id,
        flow_run_id=flow_run_id,
        task_name=task_id,
        task_attempt=try_number,
        tags=tags,
    )


def build_dagster_orchestration_metadata(
    context: Mapping[str, Any] | None = None,
) -> OrchestrationMetadata:
    source = context or {}
    job = source.get("job")
    job_name = _coerce_optional_string(
        _value_or_attribute(source.get("job_name"), job, "name")
    )
    run_id = _coerce_optional_string(source.get("run_id"))
    op = source.get("op")
    op_name = _coerce_optional_string(
        _value_or_attribute(source.get("op_name"), op, "name")
    )
    retry_number = _coerce_optional_int(source.get("retry_number"))

    tags: dict[str, str] = {}
    dag_tags = _coerce_tag_sequence(source.get("tags"))
    if dag_tags:
        tags["run_tags"] = ",".join(dag_tags)
    partition_key = _coerce_optional_string(source.get("partition_key"))
    if partition_key is not None:
        tags["partition_key"] = partition_key

    return OrchestrationMetadata(
        platform="dagster",
        flow_name=job_name,
        flow_run_id=run_id,
        task_name=op_name,
        task_attempt=retry_number,
        tags=tags,
    )


def build_prefect_orchestration_metadata(
    context: Mapping[str, Any] | None = None,
) -> OrchestrationMetadata:
    source = context or {}
    flow = source.get("flow")
    flow_run = source.get("flow_run")
    task_run = source.get("task_run")

    flow_name = _coerce_optional_string(
        _value_or_attribute(source.get("flow_name"), flow, "name")
    )
    flow_run_id = _coerce_optional_string(
        _value_or_attribute(source.get("flow_run_id"), flow_run, "id")
        or _value_or_attribute(source.get("flow_run_id"), flow_run, "flow_run_id")
    )
    task_name = _coerce_optional_string(
        _value_or_attribute(source.get("task_name"), task_run, "task_key")
        or _value_or_attribute(source.get("task_name"), task_run, "name")
    )
    task_run_id = _coerce_optional_string(
        _value_or_attribute(source.get("task_run_id"), task_run, "id")
        or _value_or_attribute(source.get("task_run_id"), task_run, "task_run_id")
    )
    run_count = _coerce_optional_int(source.get("run_count"))
    task_run_count = _coerce_optional_int(source.get("task_run_count"))
    attempt_number = task_run_count if task_run_count is not None else run_count

    tags: dict[str, str] = {}
    flow_tags = _coerce_tag_sequence(
        _value_or_attribute(source.get("tags"), flow, "tags")
        or _value_or_attribute(source.get("tags"), flow_run, "tags")
    )
    if flow_tags:
        tags["flow_tags"] = ",".join(flow_tags)
    if task_run_id is not None:
        tags["task_run_id"] = task_run_id
    scheduled_start = _coerce_optional_string(source.get("scheduled_start_time"))
    if scheduled_start is not None:
        tags["scheduled_start_time"] = scheduled_start

    return OrchestrationMetadata(
        platform="prefect",
        flow_name=flow_name,
        flow_run_id=flow_run_id,
        task_name=task_name,
        task_attempt=attempt_number,
        tags=tags,
    )


def build_mage_orchestration_metadata(
    context: Mapping[str, Any] | None = None,
) -> OrchestrationMetadata:
    source = context or {}
    pipeline_name = _coerce_optional_string(source.get("pipeline_name"))
    run_id = _coerce_optional_string(source.get("run_id"))
    block_uuid = _coerce_optional_string(source.get("block_uuid"))
    raw_block_attempt = _coerce_optional_int(source.get("block_attempt"))

    tags: dict[str, str] = {}
    mage_tags = _coerce_tag_sequence(source.get("tags"))
    if mage_tags:
        tags["mage_pipeline_tags"] = ",".join(mage_tags)
    execution_date = _coerce_optional_string(source.get("execution_date"))
    if execution_date is not None:
        tags["execution_date"] = execution_date

    return OrchestrationMetadata(
        platform="mage",
        flow_name=pipeline_name,
        flow_run_id=run_id,
        task_name=block_uuid,
        task_attempt=raw_block_attempt,
        tags=tags,
    )

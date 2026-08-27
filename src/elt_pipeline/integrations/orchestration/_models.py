from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError

_ORCHESTRATION_PLATFORM_ENV = "ELT_PIPELINE_ORCHESTRATION_PLATFORM"
_ORCHESTRATION_FLOW_NAME_ENV = "ELT_PIPELINE_ORCHESTRATION_FLOW_NAME"
_ORCHESTRATION_FLOW_RUN_ID_ENV = "ELT_PIPELINE_ORCHESTRATION_FLOW_RUN_ID"
_ORCHESTRATION_TASK_NAME_ENV = "ELT_PIPELINE_ORCHESTRATION_TASK_NAME"
_ORCHESTRATION_TASK_ATTEMPT_ENV = "ELT_PIPELINE_ORCHESTRATION_TASK_ATTEMPT"
_ORCHESTRATION_TAGS_ENV = "ELT_PIPELINE_ORCHESTRATION_TAGS_JSON"


@dataclass(frozen=True)
class OrchestrationMetadata:
    platform: str
    flow_name: str | None = None
    flow_run_id: str | None = None
    task_name: str | None = None
    task_attempt: int | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "platform",
            _validate_orchestration_platform(self.platform),
        )
        object.__setattr__(
            self,
            "flow_name",
            _coerce_optional_string(self.flow_name),
        )
        object.__setattr__(
            self,
            "flow_run_id",
            _coerce_optional_string(self.flow_run_id),
        )
        object.__setattr__(
            self,
            "task_name",
            _coerce_optional_string(self.task_name),
        )
        object.__setattr__(
            self,
            "task_attempt",
            _validate_task_attempt_value(self.task_attempt),
        )
        object.__setattr__(
            self,
            "tags",
            _normalize_orchestration_tags(self.tags),
        )

    def to_env(self) -> dict[str, str]:
        payload = {_ORCHESTRATION_PLATFORM_ENV: self.platform}
        if self.flow_name is not None:
            payload[_ORCHESTRATION_FLOW_NAME_ENV] = self.flow_name
        if self.flow_run_id is not None:
            payload[_ORCHESTRATION_FLOW_RUN_ID_ENV] = self.flow_run_id
        if self.task_name is not None:
            payload[_ORCHESTRATION_TASK_NAME_ENV] = self.task_name
        if self.task_attempt is not None:
            payload[_ORCHESTRATION_TASK_ATTEMPT_ENV] = str(self.task_attempt)
        if self.tags:
            payload[_ORCHESTRATION_TAGS_ENV] = json.dumps(self.tags, sort_keys=True)
        return payload

    def to_run_attributes(self) -> dict[str, Any]:
        attributes: dict[str, Any] = {
            "orchestration_platform": self.platform,
        }
        if self.flow_name is not None:
            attributes["orchestration_flow_name"] = self.flow_name
        if self.flow_run_id is not None:
            attributes["orchestration_flow_run_id"] = self.flow_run_id
        if self.task_name is not None:
            attributes["orchestration_task_name"] = self.task_name
        if self.task_attempt is not None:
            attributes["orchestration_task_attempt"] = self.task_attempt
        if self.tags:
            attributes["orchestration_tags"] = dict(self.tags)
        return attributes


@dataclass(frozen=True)
class CliInvocationRequest:
    subcommand: tuple[str, ...]
    arguments: tuple[str, ...] = ()
    cwd: Path | None = None
    environment_overrides: dict[str, str] = field(default_factory=dict)
    orchestration_metadata: OrchestrationMetadata | None = None

    def argv(self) -> tuple[str, ...]:
        return (sys.executable, "-m", "elt_pipeline", *self.subcommand, *self.arguments)

    def build_env(self, base_env: Mapping[str, str] | None = None) -> dict[str, str]:
        effective_env = dict(base_env if base_env is not None else os.environ)
        if self.orchestration_metadata is not None:
            effective_env.update(self.orchestration_metadata.to_env())
        effective_env.update(self.environment_overrides)
        return effective_env


@dataclass(frozen=True)
class CliInvocationResult:
    argv: tuple[str, ...]
    cwd: Path | None
    exit_code: int
    stdout: str
    stderr: str

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0

    def raise_for_exit_code(self) -> None:
        if self.succeeded:
            return
        raise PipelineError(
            message="Orchestration wrapper CLI invocation failed",
            error_code="ORCHESTRATION_WRAPPER_INVOCATION_FAILED",
            error_category=ErrorCategory.processing_error,
            context={
                "argv": list(self.argv),
                "cwd": None if self.cwd is None else str(self.cwd),
                "exit_code": self.exit_code,
                "stderr": self.stderr,
            },
        )


class OrchestrationCliInvoker(Protocol):
    def invoke(
        self,
        request: CliInvocationRequest,
        *,
        timeout_seconds: float | None = None,
    ) -> CliInvocationResult: ...


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_orchestration_platform(platform: str) -> str:
    normalized = _coerce_optional_string(platform)
    if normalized is None:
        raise ConfigValidationError(
            message="Orchestration platform must not be empty",
            context={"platform": platform},
        )
    return normalized


def _parse_task_attempt(
    *,
    raw_value: str | None,
    configured_fields: Sequence[str],
) -> int | None:
    if raw_value is None:
        return None
    try:
        task_attempt = int(raw_value)
    except ValueError as exc:
        raise ConfigValidationError(
            message="Orchestration task attempt must be an integer",
            context={
                "configured_fields": list(configured_fields),
                _ORCHESTRATION_TASK_ATTEMPT_ENV: raw_value,
            },
        ) from exc
    return _validate_task_attempt_value(
        task_attempt,
        context={
            "configured_fields": list(configured_fields),
            _ORCHESTRATION_TASK_ATTEMPT_ENV: raw_value,
        },
    )


def _parse_tags(*, raw_value: str | None, configured_fields: Sequence[str]) -> dict[str, str]:
    if raw_value is None:
        return {}
    try:
        decoded = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message="Orchestration tags metadata must be valid JSON",
            context={
                "configured_fields": list(configured_fields),
                _ORCHESTRATION_TAGS_ENV: raw_value,
            },
        ) from exc
    if not isinstance(decoded, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in decoded.items()
    ):
        raise ConfigValidationError(
            message=(
                "Orchestration tags metadata must decode to an object "
                "of string keys and values"
            ),
            context={
                "configured_fields": list(configured_fields),
                _ORCHESTRATION_TAGS_ENV: raw_value,
            },
        )
    return _normalize_orchestration_tags(decoded)


def _validate_task_attempt_value(
    task_attempt: int | None,
    *,
    context: dict[str, Any] | None = None,
) -> int | None:
    if task_attempt is None:
        return None
    if isinstance(task_attempt, bool) or not isinstance(task_attempt, int) or task_attempt < 1:
        raise ConfigValidationError(
            message="Orchestration task attempt must be greater than or equal to 1",
            context=context or {"task_attempt": task_attempt},
        )
    return task_attempt


def _normalize_orchestration_tags(tags: Mapping[str, str]) -> dict[str, str]:
    normalized_tags: dict[str, str] = {}
    for key, value in tags.items():
        normalized_key = _coerce_optional_string(key)
        normalized_value = _coerce_optional_string(value)
        if normalized_key is None or normalized_value is None:
            raise ConfigValidationError(
                message="Orchestration tags metadata must contain non-empty string keys and values",
                context={"tags": dict(tags)},
            )
        normalized_tags[normalized_key] = normalized_value
    return normalized_tags


def _value_or_attribute(
    explicit_value: Any,
    obj: Any,
    attribute_name: str,
) -> Any:
    if explicit_value is not None:
        return explicit_value
    if obj is None:
        return None
    return getattr(obj, attribute_name, None)


def _coerce_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    normalized = str(value).strip()
    return normalized or None


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 1 else None


def _coerce_tag_sequence(value: Any) -> list[str]:
    if value is None or isinstance(value, str):
        return []
    if not isinstance(value, Sequence):
        return []
    return [normalized for item in value if (normalized := _coerce_optional_string(item))]


def _coerce_strings(args: Sequence[str] | str) -> Sequence[str]:
    if isinstance(args, str):
        return [args]
    return [str(argument) for argument in args]

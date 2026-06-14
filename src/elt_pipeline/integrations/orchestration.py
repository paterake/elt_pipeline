from __future__ import annotations

import json
import os
import subprocess
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


class SubprocessCliInvoker:
    def invoke(
        self,
        request: CliInvocationRequest,
        *,
        timeout_seconds: float | None = None,
    ) -> CliInvocationResult:
        completed = subprocess.run(
            list(request.argv()),
            cwd=request.cwd,
            env=request.build_env(),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        return CliInvocationResult(
            argv=tuple(_coerce_strings(completed.args)),
            cwd=request.cwd,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


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
    if task_attempt < 1:
        raise ConfigValidationError(
            message="Orchestration task attempt must be greater than or equal to 1",
            context={
                "configured_fields": list(configured_fields),
                _ORCHESTRATION_TASK_ATTEMPT_ENV: raw_value,
            },
        )
    return task_attempt


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
    return decoded


def _coerce_strings(args: Sequence[str] | str) -> Sequence[str]:
    if isinstance(args, str):
        return [args]
    return [str(argument) for argument in args]

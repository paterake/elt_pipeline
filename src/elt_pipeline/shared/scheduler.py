from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from elt_pipeline.shared.errors import ConfigValidationError


class ScheduledCliJob(BaseModel):
    name: str = Field(min_length=1)
    argv: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_argv(self) -> "ScheduledCliJob":
        normalized_argv = [argument.strip() for argument in self.argv]
        if any(not argument for argument in normalized_argv):
            raise ValueError("schedule job argv entries must not be empty")
        if normalized_argv[0] == "schedule":
            raise ValueError("nested schedule jobs are not supported")
        if normalized_argv[0] in {"elt-pipeline", "python", "python3"}:
            raise ValueError("schedule job argv must omit the program name")
        self.argv = normalized_argv
        self.name = self.name.strip()
        if not self.name:
            raise ValueError("schedule job name must not be empty")
        return self


class SchedulePlan(BaseModel):
    jobs: list[ScheduledCliJob] = Field(min_length=1)
    continue_on_error: bool = False


def load_schedule_plan(path: Path) -> SchedulePlan:
    resolved_path = path.resolve()
    if not resolved_path.exists():
        raise ConfigValidationError(
            message="Schedule plan file does not exist",
            context={"plan_path": str(resolved_path)},
        )

    try:
        raw_payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse schedule plan YAML: {exc}",
            context={"plan_path": str(resolved_path)},
        ) from exc

    if not isinstance(raw_payload, dict):
        raise ConfigValidationError(
            message="Schedule plan must decode to a YAML object",
            context={"plan_path": str(resolved_path)},
        )

    try:
        return SchedulePlan.model_validate(raw_payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Schedule plan validation failed",
            context={
                "plan_path": str(resolved_path),
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def parse_schedule_payload(raw_text: str) -> Any | None:
    payload = raw_text.strip()
    if not payload:
        return None
    try:
        return yaml.safe_load(payload)
    except yaml.YAMLError:
        return payload

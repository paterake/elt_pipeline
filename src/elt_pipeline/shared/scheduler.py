from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from elt_pipeline.shared.errors import ConfigValidationError


class ScheduledCliJob(BaseModel):
    name: str = Field(min_length=1)
    argv: list[str] = Field(min_length=1)
    retries: int = Field(default=0, ge=0, le=100)
    retry_delay_seconds: float = Field(default=0.0, ge=0.0, le=3600.0)
    depends_on: list[str] = Field(default_factory=list)

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
        normalized_deps = [dep.strip() for dep in self.depends_on]
        if any(not dep for dep in normalized_deps):
            raise ValueError("schedule job depends_on entries must not be empty")
        self.depends_on = normalized_deps
        return self


class SchedulePlan(BaseModel):
    jobs: list[ScheduledCliJob] = Field(min_length=1)
    continue_on_error: bool = False

    @model_validator(mode="after")
    def validate_dependencies(self) -> "SchedulePlan":
        job_names = {job.name for job in self.jobs}
        for job in self.jobs:
            unknown_deps = [dep for dep in job.depends_on if dep not in job_names]
            if unknown_deps:
                raise ValueError(
                    f"schedule job '{job.name}' has unknown depends_on references: "
                    + ", ".join(unknown_deps)
                )
            if job.name in job.depends_on:
                raise ValueError(
                    f"schedule job '{job.name}' cannot depend on itself"
                )
        order = topological_sort_schedule_jobs(self.jobs)
        if len(order) != len(self.jobs):
            remaining = {j.name for j in self.jobs} - set(order)
            raise ValueError(
                "schedule plan contains a cyclic dependency; remaining jobs in cycle: "
                + ", ".join(sorted(remaining))
            )
        return self


def topological_sort_schedule_jobs(
    jobs: list[ScheduledCliJob],
) -> list[str]:
    order_index = {job.name: i for i, job in enumerate(jobs)}
    indegree: dict[str, int] = {job.name: 0 for job in jobs}
    for job in jobs:
        for _dep in job.depends_on:
            indegree[job.name] = indegree.get(job.name, 0) + 1
    ready: list[tuple[int, str]] = [
        (order_index[name], name)
        for name, degree in indegree.items()
        if degree == 0
    ]
    ready.sort(key=lambda pair: pair[0])
    queue: deque[str] = deque(name for _, name in ready)
    ordered: list[str] = []
    successors_of: dict[str, list[str]] = {job.name: [] for job in jobs}
    for job in jobs:
        for dep in job.depends_on:
            successors_of[dep].append(job.name)
    for name in successors_of:
        successors_of[name].sort(key=lambda n: order_index[n])
    while queue:
        name = queue.popleft()
        ordered.append(name)
        for successor in successors_of[name]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                queue.append(successor)
    return ordered


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

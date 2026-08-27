from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class StageName(str, Enum):
    ingest = "ingest"
    normalize = "normalize"
    sql = "sql"
    publish = "publish"
    shared = "shared"
    semantic_metrics = "semantic_metrics"


class TriggerType(str, Enum):
    manual = "manual"
    scheduled_batch = "scheduled_batch"
    backfill = "backfill"
    event_driven = "event_driven"
    on_demand = "on_demand"


class CheckpointMode(str, Enum):
    resume = "resume"
    backfill = "backfill"
    reset = "reset"


class ExecutionWindow(BaseModel):
    start: datetime | None = None
    end: datetime | None = None
    label: str | None = None

    @field_validator("label")
    @classmethod
    def normalize_label(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @model_validator(mode="after")
    def validate_bounds(self) -> "ExecutionWindow":
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("window end must be greater than or equal to window start")
        if self.label is None and (self.start is not None or self.end is not None):
            start_fragment = self.start.date().isoformat() if self.start is not None else "open"
            end_fragment = self.end.date().isoformat() if self.end is not None else "open"
            self.label = f"{start_fragment}_to_{end_fragment}"
        return self


class JobTarget(BaseModel):
    environment: str
    source_name: str | None = None
    entity_name: str | None = None

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        return _validate_required_runtime_text(value=value, field_name="environment")

    @field_validator("source_name", "entity_name")
    @classmethod
    def normalize_optional_names(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class CheckpointDirective(BaseModel):
    mode: CheckpointMode = CheckpointMode.resume
    seed: dict[str, Any] | None = None


class RunContext(BaseModel):
    run_id: str
    stage: StageName
    job_name: str
    trigger_type: str
    started_at: datetime
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("run_id", "job_name", "trigger_type")
    @classmethod
    def validate_required_text(cls, value: str, info) -> str:
        return _validate_required_runtime_text(value=value, field_name=info.field_name)


class JobRuntime(BaseModel):
    stage: StageName
    job_name: str
    target: JobTarget
    trigger_type: TriggerType = TriggerType.manual
    window: ExecutionWindow = Field(default_factory=ExecutionWindow)
    checkpoint: CheckpointDirective = Field(default_factory=CheckpointDirective)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, value: str) -> str:
        return _validate_required_runtime_text(value=value, field_name="job_name")

    def checkpoint_seed(self) -> dict[str, Any] | None:
        if self.checkpoint.mode is CheckpointMode.backfill:
            return self.checkpoint.seed
        return None

    def to_run_context(self) -> RunContext:
        runtime_attributes = {
            "environment": self.target.environment,
            "checkpoint_mode": self.checkpoint.mode.value,
            **self.attributes,
        }
        if self.target.source_name is not None:
            runtime_attributes.setdefault("source_name", self.target.source_name)
        if self.target.entity_name is not None:
            runtime_attributes.setdefault("entity_name", self.target.entity_name)
        if self.window.start is not None:
            runtime_attributes.setdefault("window_start", self.window.start.isoformat())
        if self.window.end is not None:
            runtime_attributes.setdefault("window_end", self.window.end.isoformat())
        if self.window.label is not None:
            runtime_attributes.setdefault("window_label", self.window.label)
        if self.checkpoint.seed is not None:
            runtime_attributes.setdefault("checkpoint_seed", self.checkpoint.seed)

        return new_run_context(
            stage=self.stage,
            job_name=self.job_name,
            trigger_type=self.trigger_type.value,
            attributes=runtime_attributes,
        )


def build_job_runtime(
    *,
    stage: StageName,
    job_name: str,
    environment: str,
    trigger_type: str = TriggerType.manual.value,
    source_name: str | None = None,
    entity_name: str | None = None,
    window: ExecutionWindow | None = None,
    backfill: bool = False,
    checkpoint_seed: dict[str, Any] | None = None,
    attributes: dict[str, Any] | None = None,
) -> JobRuntime:
    resolved_window = window or ExecutionWindow()
    if backfill and resolved_window.start is None:
        raise ValueError("backfill requires a window start")

    resolved_trigger = TriggerType(trigger_type)
    if backfill and resolved_trigger is TriggerType.manual:
        resolved_trigger = TriggerType.backfill

    checkpoint_mode = CheckpointMode.backfill if backfill else CheckpointMode.resume
    return JobRuntime(
        stage=stage,
        job_name=job_name,
        target=JobTarget(
            environment=environment,
            source_name=source_name,
            entity_name=entity_name,
        ),
        trigger_type=resolved_trigger,
        window=resolved_window,
        checkpoint=CheckpointDirective(mode=checkpoint_mode, seed=checkpoint_seed),
        attributes=attributes or {},
    )


def new_run_context(
    *,
    stage: StageName,
    job_name: str,
    trigger_type: str = "manual",
    attributes: dict[str, Any] | None = None,
) -> RunContext:
    return RunContext(
        run_id=str(uuid4()),
        stage=stage,
        job_name=job_name,
        trigger_type=trigger_type,
        started_at=datetime.now(tz=UTC),
        attributes=attributes or {},
    )


def _validate_required_runtime_text(*, value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty")
    return cleaned

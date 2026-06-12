from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class MetricsSummary(BaseModel):
    records_read: int | None = None
    records_written: int | None = None
    files_written: int | None = None
    extra: dict[str, int | float | str] = Field(default_factory=dict)


class AuditRecord(BaseModel):
    run_id: str
    stage: str
    job_name: str
    trigger_type: str
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    config_version: str | None = None
    metrics_summary: MetricsSummary = Field(default_factory=MetricsSummary)
    error_summary: dict[str, str] | None = None
    context: dict[str, str] = Field(default_factory=dict)

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class MetricType(str, Enum):
    counter = "counter"
    gauge = "gauge"
    histogram = "histogram"
    summary = "summary"


class SpanStatus(str, Enum):
    ok = "ok"
    error = "error"
    unset = "unset"


class AlertSeverity(str, Enum):
    critical = "critical"
    warning = "warning"
    info = "info"


class MetricPoint(BaseModel):
    metric_name: str
    metric_type: MetricType
    value: int | float
    labels: dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str | None = None
    stage: str | None = None
    job_name: str | None = None


class TraceSpan(BaseModel):
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    name: str
    start_time: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    end_time: datetime | None = None
    status: SpanStatus = SpanStatus.unset
    attributes: dict[str, Any] = Field(default_factory=dict)
    events: list[dict[str, Any]] = Field(default_factory=list)
    run_id: str | None = None
    stage: str | None = None
    job_name: str | None = None


class AlertEvent(BaseModel):
    severity: AlertSeverity
    message: str
    labels: dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str | None = None
    stage: str | None = None
    job_name: str | None = None

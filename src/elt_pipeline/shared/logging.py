from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel, Field

from elt_pipeline.shared.runtime import RunContext


class ExecutionLogEvent(BaseModel):
    run_id: str
    severity: str
    component: str
    event_type: str
    message: str
    timestamp: str
    details: dict[str, Any] = Field(default_factory=dict)


def build_log_event(
    *,
    run_context: RunContext,
    severity: str,
    component: str,
    event_type: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> ExecutionLogEvent:
    return ExecutionLogEvent(
        run_id=run_context.run_id,
        severity=severity,
        component=component,
        event_type=event_type,
        message=message,
        timestamp=run_context.started_at.isoformat(),
        details=details or {},
    )


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "severity": record.levelname,
            "component": getattr(record, "component", record.name),
            "run_id": getattr(record, "run_id", None),
            "event_type": getattr(record, "event_type", record.funcName),
            "message": record.getMessage(),
        }
        return json.dumps(payload, sort_keys=True)

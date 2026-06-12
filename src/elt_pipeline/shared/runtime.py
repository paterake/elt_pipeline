from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class StageName(str, Enum):
    ingest = "ingest"
    normalize = "normalize"
    sql = "sql"
    shared = "shared"


class RunContext(BaseModel):
    run_id: str
    stage: StageName
    job_name: str
    trigger_type: str
    started_at: datetime
    attributes: dict[str, Any] = Field(default_factory=dict)


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

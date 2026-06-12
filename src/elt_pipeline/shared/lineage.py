from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class DatasetRef(BaseModel):
    namespace: str
    name: str
    facets: dict[str, object] = Field(default_factory=dict)


class LineageEvent(BaseModel):
    event_type: str
    event_time: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str
    job_name: str
    producer: str = "elt_pipeline"
    inputs: list[DatasetRef] = Field(default_factory=list)
    outputs: list[DatasetRef] = Field(default_factory=list)

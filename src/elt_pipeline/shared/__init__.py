"""Shared runtime contracts for all pipeline stages."""

from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import ErrorRecord, PipelineError
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.runtime import (
    CheckpointDirective,
    CheckpointMode,
    ExecutionWindow,
    JobRuntime,
    JobTarget,
    RunContext,
    StageName,
    TriggerType,
    build_job_runtime,
    new_run_context,
)

__all__ = [
    "AuditRecord",
    "CheckpointDirective",
    "CheckpointMode",
    "DatasetRef",
    "ErrorRecord",
    "ExecutionWindow",
    "JobRuntime",
    "JobTarget",
    "LineageEvent",
    "MetricsSummary",
    "PipelineError",
    "RunContext",
    "StageName",
    "TriggerType",
    "build_job_runtime",
    "new_run_context",
]

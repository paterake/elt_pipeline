from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Protocol

from elt_pipeline.ingest.storage import LocalArtifactStore
from elt_pipeline.shared.errors import ErrorCategory, PipelineError, build_error_record
from elt_pipeline.shared.lineage import LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.runtime import RunContext


class LineageEmissionPolicy(str, Enum):
    best_effort = "best_effort"
    blocking = "blocking"


class LineageRemoteEmitter(Protocol):
    backend_type: str

    def emit(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> None: ...


class LineageAdapter:
    def __init__(
        self,
        *,
        artifact_store: LocalArtifactStore,
        remote_emitter: LineageRemoteEmitter | None = None,
        emission_policy: LineageEmissionPolicy = LineageEmissionPolicy.best_effort,
    ) -> None:
        self._artifact_store = artifact_store
        self._remote_emitter = remote_emitter
        self._emission_policy = emission_policy

    def emit(
        self,
        *,
        run_context: RunContext,
        environment: str,
        lineage_event: LineageEvent,
    ) -> Path:
        local_path = self._artifact_store.append_lineage_event(
            run_context=run_context,
            environment=environment,
            lineage_event=lineage_event,
        )
        if self._remote_emitter is None:
            return local_path

        try:
            self._remote_emitter.emit(
                run_context=run_context,
                environment=environment,
                lineage_event=lineage_event,
            )
        except PipelineError as exc:
            self._record_remote_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._remote_emitter.backend_type,
                error=exc,
            )
            if self._emission_policy is LineageEmissionPolicy.blocking:
                raise
        except Exception as exc:
            pipeline_error = PipelineError(
                message="Optional lineage backend emission failed",
                error_code="LINEAGE_BACKEND_EMISSION_FAILED",
                error_category=ErrorCategory.lineage_error,
                retryable=True,
                context={
                    "backend_type": self._remote_emitter.backend_type,
                    "event_type": lineage_event.event_type,
                    "job_name": run_context.job_name,
                },
            )
            self._record_remote_failure(
                run_context=run_context,
                environment=environment,
                backend_type=self._remote_emitter.backend_type,
                error=pipeline_error,
            )
            if self._emission_policy is LineageEmissionPolicy.blocking:
                raise pipeline_error from exc

        return local_path

    def _record_remote_failure(
        self,
        *,
        run_context: RunContext,
        environment: str,
        backend_type: str,
        error: PipelineError,
    ) -> None:
        self._artifact_store.append_error_record(
            run_context=run_context,
            environment=environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=error.error_code,
                error_category=error.error_category,
                message=str(error),
                retryable=error.retryable,
                context=error.context,
            ),
        )
        self._artifact_store.append_log_event(
            run_context=run_context,
            environment=environment,
            log_event=build_log_event(
                run_context=run_context,
                severity=(
                    "ERROR"
                    if self._emission_policy is LineageEmissionPolicy.blocking
                    else "WARNING"
                ),
                component="lineage",
                event_type="lineage_remote_emit_failed",
                message="Optional lineage backend emission failed",
                details={
                    "backend_type": backend_type,
                    "error_code": error.error_code,
                    "error_category": error.error_category.value,
                    "blocking": self._emission_policy is LineageEmissionPolicy.blocking,
                },
            ),
        )


def build_lineage_adapter(
    root_path: Path,
    *,
    remote_emitter: LineageRemoteEmitter | None = None,
    emission_policy: LineageEmissionPolicy = LineageEmissionPolicy.best_effort,
) -> LineageAdapter:
    return LineageAdapter(
        artifact_store=LocalArtifactStore(root_path),
        remote_emitter=remote_emitter,
        emission_policy=emission_policy,
    )

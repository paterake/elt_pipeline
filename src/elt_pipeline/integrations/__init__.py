"""Optional integration boundaries for external platform tooling."""

from elt_pipeline.integrations.lineage import (
    LineageAdapter,
    LineageEmissionPolicy,
    LineageRemoteEmitter,
    OpenLineageHttpEmitter,
    build_lineage_adapter,
)
from elt_pipeline.integrations.orchestration import (
    AirflowCliWrapper,
    CliInvocationRequest,
    CliInvocationResult,
    OrchestrationCliInvoker,
    OrchestrationMetadata,
    SubprocessCliInvoker,
    build_airflow_orchestration_metadata,
    load_orchestration_metadata_from_env,
)

__all__ = [
    "LineageAdapter",
    "LineageEmissionPolicy",
    "LineageRemoteEmitter",
    "OpenLineageHttpEmitter",
    "build_lineage_adapter",
    "AirflowCliWrapper",
    "CliInvocationRequest",
    "CliInvocationResult",
    "OrchestrationCliInvoker",
    "OrchestrationMetadata",
    "SubprocessCliInvoker",
    "build_airflow_orchestration_metadata",
    "load_orchestration_metadata_from_env",
]

"""Optional integration boundaries for external platform tooling."""

from elt_pipeline.integrations.lineage import (
    LineageAdapter,
    LineageEmissionPolicy,
    LineageRemoteEmitter,
    OpenLineageHttpEmitter,
    build_lineage_adapter,
)
from elt_pipeline.integrations.orchestration import (
    CliInvocationRequest,
    CliInvocationResult,
    OrchestrationCliInvoker,
    OrchestrationMetadata,
    SubprocessCliInvoker,
    load_orchestration_metadata_from_env,
)

__all__ = [
    "LineageAdapter",
    "LineageEmissionPolicy",
    "LineageRemoteEmitter",
    "OpenLineageHttpEmitter",
    "build_lineage_adapter",
    "CliInvocationRequest",
    "CliInvocationResult",
    "OrchestrationCliInvoker",
    "OrchestrationMetadata",
    "SubprocessCliInvoker",
    "load_orchestration_metadata_from_env",
]

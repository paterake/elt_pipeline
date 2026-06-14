"""Optional integration boundaries for external platform tooling."""

from elt_pipeline.integrations.lineage import (
    LineageAdapter,
    LineageEmissionPolicy,
    LineageRemoteEmitter,
    build_lineage_adapter,
)

__all__ = [
    "LineageAdapter",
    "LineageEmissionPolicy",
    "LineageRemoteEmitter",
    "build_lineage_adapter",
]

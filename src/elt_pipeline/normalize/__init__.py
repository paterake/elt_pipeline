"""Normalization stage package."""

from elt_pipeline.normalize.level2_storage import LocalLevel2Writer
from elt_pipeline.normalize.models import MappingCatalog, NormalizationResult, NormalizedTable
from elt_pipeline.normalize.partitioning import PartitionMode, PartitionStrategy
from elt_pipeline.normalize.pipeline import normalize_level1_to_local_level2
from elt_pipeline.normalize.runner import NormalizationRunner
from elt_pipeline.normalize.storage import LocalMappingCatalogStore

__all__ = [
    "LocalLevel2Writer",
    "LocalMappingCatalogStore",
    "MappingCatalog",
    "NormalizationResult",
    "NormalizedTable",
    "NormalizationRunner",
    "PartitionMode",
    "PartitionStrategy",
    "normalize_level1_to_local_level2",
]

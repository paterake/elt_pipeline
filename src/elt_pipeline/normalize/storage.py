from __future__ import annotations

import json
import posixpath
import re

from elt_pipeline.normalize.models import MappingCatalog
from elt_pipeline.shared.path_utils import (
    join_paths,
    path_mkdir,
    path_parent,
    path_replace,
    path_with_suffix,
    path_write_text,
)

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_path_fragment(value: str) -> str:
    cleaned = _SAFE_PATH_FRAGMENT.sub("_", value.strip())
    return cleaned or "unknown"


class LocalMappingCatalogStore:
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path

    def catalog_path(
        self,
        *,
        source_name: str,
        entity_name: str,
        mapping_version: str,
    ) -> str:
        return join_paths(
            self.root_path,
            "level2",
            "_mapping_catalogs",
            f"source={_sanitize_path_fragment(source_name)}",
            f"entity={_sanitize_path_fragment(entity_name)}",
            f"mapping_version={_sanitize_path_fragment(mapping_version)}",
            "catalog.json",
        )

    def write_catalog(self, catalog: MappingCatalog) -> str:
        path = self.catalog_path(
            source_name=catalog.source_name,
            entity_name=catalog.entity_name,
            mapping_version=catalog.mapping_version,
        )
        path_mkdir(path_parent(path), parents=True, exist_ok=True)
        temp_path = path_with_suffix(path, f"{posixpath.splitext(path)[1]}.tmp")
        path_write_text(
            temp_path,
            json.dumps(catalog.model_dump(mode="json"), indent=2, sort_keys=True),
            encoding="utf-8",
            atomic=True,
        )
        path_replace(temp_path, path)
        return path

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from elt_pipeline.normalize.models import TableMappingEntry
from elt_pipeline.shared.errors import ErrorCategory, PipelineError

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9]+")


def sanitize_identifier(value: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", value.strip().lower()).strip("_")
    return cleaned or "value"


def join_path(parent_path: str, segment: str) -> str:
    if parent_path == "$":
        return f"$.{segment}"
    return f"{parent_path}.{segment}"


@dataclass
class IdentifierPolicy:
    max_identifier_length: int = 63
    separator: str = "__"
    value_column_name: str = "value"

    _logical_path_by_physical_name: dict[str, str] = field(default_factory=dict)

    def reset(self) -> None:
        self._logical_path_by_physical_name.clear()

    def make_table_name(self, *, name_segments: list[str], logical_path: str) -> str:
        base_name = self.separator.join(
            sanitize_identifier(segment) for segment in name_segments
        )
        used_path = self._logical_path_by_physical_name.get(base_name)
        if len(base_name) <= self.max_identifier_length and (
            used_path in (None, logical_path)
        ):
            self._logical_path_by_physical_name[base_name] = logical_path
            return base_name
        return self._build_hashed_identifier(
            base_name=base_name, logical_path=logical_path
        )

    def make_column_name(self, field_segments: list[str]) -> str:
        return self.separator.join(
            sanitize_identifier(segment) for segment in field_segments
        )

    def _build_hashed_identifier(self, *, base_name: str, logical_path: str) -> str:
        digest = hashlib.sha256(logical_path.encode("utf-8")).hexdigest()[:8]
        suffix = f"{self.separator}{digest}"
        allowed_prefix_length = self.max_identifier_length - len(suffix)
        if allowed_prefix_length <= 0:
            raise PipelineError(
                message="Maximum identifier length is too small for hashed table names",
                error_code="NORMALIZE_IDENTIFIER_LENGTH_INVALID",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={"max_identifier_length": self.max_identifier_length},
            )
        prefix = base_name[:allowed_prefix_length].rstrip("_")
        candidate_name = f"{prefix}{suffix}"
        used_path = self._logical_path_by_physical_name.get(candidate_name)
        if used_path not in (None, logical_path):
            raise PipelineError(
                message="Hashed table name collided with an existing derived table",
                error_code="NORMALIZE_TABLE_NAME_COLLISION",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "logical_path": logical_path,
                    "candidate_name": candidate_name,
                },
            )
        self._logical_path_by_physical_name[candidate_name] = logical_path
        return candidate_name


def build_mapping_version(entries: list[TableMappingEntry]) -> str:
    canonical_payload = [
        {
            "logical_path": entry.logical_path,
            "physical_table_name": entry.physical_table_name,
            "parent_table_name": entry.parent_table_name,
            "join_key_columns": entry.join_key_columns,
            "column_mappings": [
                {
                    "logical_path": mapping.logical_path,
                    "physical_name": mapping.physical_name,
                }
                for mapping in entry.column_mappings
            ],
        }
        for entry in entries
    ]
    raw = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def build_mapping_catalog(
    *,
    source_name: str,
    entity_name: str,
    entries: list[TableMappingEntry],
) -> tuple[str, str]:
    mapping_version = build_mapping_version(entries)
    root_table_name = (
        entries[0].physical_table_name
        if entries
        else sanitize_identifier(entity_name)
    )
    return mapping_version, root_table_name

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass, field
from io import StringIO
from typing import Any
from uuid import uuid4

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.models import (
    MappingCatalog,
    NormalizationResult,
    NormalizedTable,
    TableColumnMapping,
    TableMappingEntry,
)
from elt_pipeline.shared.errors import ErrorCategory, PipelineError

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9]+")


def _sanitize_identifier(value: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", value.strip().lower()).strip("_")
    return cleaned or "value"


def _join_path(parent_path: str, segment: str) -> str:
    if parent_path == "$":
        return f"$.{segment}"
    return f"{parent_path}.{segment}"


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list))


@dataclass
class _TableState:
    logical_path: str
    physical_name: str
    parent_table_name: str | None
    join_key_columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    column_mappings: dict[str, str] = field(default_factory=dict)


class NormalizationRunner:
    def __init__(
        self,
        *,
        max_identifier_length: int = 63,
        separator: str = "__",
        value_column_name: str = "value",
    ) -> None:
        self.max_identifier_length = max_identifier_length
        self.separator = separator
        self.value_column_name = value_column_name

    def normalize_level1_json(
        self,
        *,
        manifest: Level1ArtifactManifest,
        payload: str | bytes | dict[str, Any] | list[Any],
    ) -> NormalizationResult:
        document = self._load_json_payload(payload=payload, manifest=manifest)
        state = _RunnerState(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            max_identifier_length=self.max_identifier_length,
            separator=self.separator,
            value_column_name=self.value_column_name,
        )
        root_table = state.get_or_create_table(
            logical_path="$",
            name_segments=[manifest.entity_name],
            parent_table_name=None,
        )
        self._normalize_root_value(
            value=document,
            table_state=root_table,
            runner_state=state,
        )
        mapping_catalog = state.build_mapping_catalog()
        return NormalizationResult(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            artifact_id=manifest.artifact_id,
            data_path=manifest.data_path,
            mapping_version=mapping_catalog.mapping_version,
            tables=state.build_tables(),
            mapping_catalog=mapping_catalog,
        )

    def normalize_level1(
        self,
        *,
        manifest: Level1ArtifactManifest,
        payload: str | bytes | dict[str, Any] | list[Any],
    ) -> NormalizationResult:
        payload_format = manifest.payload_format.strip().lower()
        if payload_format == "csv":
            return self.normalize_level1_csv(manifest=manifest, payload=payload)
        return self.normalize_level1_json(manifest=manifest, payload=payload)

    def normalize_level1_csv(
        self,
        *,
        manifest: Level1ArtifactManifest,
        payload: str | bytes,
    ) -> NormalizationResult:
        csv_text = self._load_text_payload(payload=payload, manifest=manifest)
        try:
            reader = csv.DictReader(StringIO(csv_text))
        except csv.Error as exc:
            raise PipelineError(
                message=f"Failed to decode CSV payload for artifact {manifest.artifact_id}",
                error_code="NORMALIZE_INVALID_CSV",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={"artifact_id": manifest.artifact_id, "data_path": manifest.data_path},
            ) from exc

        if not reader.fieldnames:
            raise PipelineError(
                message="Normalization expects a CSV payload with a header row",
                error_code="NORMALIZE_CSV_HEADER_REQUIRED",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={"artifact_id": manifest.artifact_id, "data_path": manifest.data_path},
            )

        state = _RunnerState(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            max_identifier_length=self.max_identifier_length,
            separator=self.separator,
            value_column_name=self.value_column_name,
        )
        root_table = state.get_or_create_table(
            logical_path="$",
            name_segments=[manifest.entity_name],
            parent_table_name=None,
        )
        fieldnames = [fieldname or "" for fieldname in reader.fieldnames]
        for fieldname in fieldnames:
            physical_name = state.make_column_name([fieldname])
            self._register_scalar_column(
                table_state=root_table,
                logical_path=f"$.{fieldname}",
                physical_name=physical_name,
            )

        for source_row in reader:
            row_id = str(uuid4())
            row: dict[str, Any] = {"_row_id": row_id}
            for fieldname in fieldnames:
                physical_name = root_table.column_mappings[f"$.{fieldname}"]
                row[physical_name] = source_row.get(fieldname)
            root_table.rows.append(row)

        mapping_catalog = state.build_mapping_catalog()
        return NormalizationResult(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            artifact_id=manifest.artifact_id,
            data_path=manifest.data_path,
            mapping_version=mapping_catalog.mapping_version,
            tables=state.build_tables(),
            mapping_catalog=mapping_catalog,
        )

    def _load_text_payload(
        self,
        *,
        payload: str | bytes,
        manifest: Level1ArtifactManifest,
    ) -> str:
        if isinstance(payload, bytes):
            try:
                return payload.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise PipelineError(
                    message=f"Failed to decode text payload for artifact {manifest.artifact_id}",
                    error_code="NORMALIZE_TEXT_DECODE_FAILED",
                    error_category=ErrorCategory.input_contract_error,
                    retryable=False,
                    context={"artifact_id": manifest.artifact_id, "data_path": manifest.data_path},
                ) from exc
        return payload

    def _load_json_payload(
        self,
        *,
        payload: str | bytes | dict[str, Any] | list[Any],
        manifest: Level1ArtifactManifest,
    ) -> dict[str, Any] | list[Any]:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            try:
                document = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise PipelineError(
                    message=f"Failed to decode JSON payload for artifact {manifest.artifact_id}",
                    error_code="NORMALIZE_INVALID_JSON",
                    error_category=ErrorCategory.input_contract_error,
                    retryable=False,
                    context={"artifact_id": manifest.artifact_id, "data_path": manifest.data_path},
                ) from exc
        else:
            document = payload

        if not isinstance(document, (dict, list)):
            raise PipelineError(
                message="Normalization expects a JSON object or array payload",
                error_code="NORMALIZE_UNSUPPORTED_ROOT",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={"artifact_id": manifest.artifact_id, "data_path": manifest.data_path},
            )
        return document

    def _normalize_root_value(
        self,
        *,
        value: dict[str, Any] | list[Any],
        table_state: _TableState,
        runner_state: "_RunnerState",
    ) -> None:
        if isinstance(value, list):
            for item in value:
                self._append_row(
                    value=item,
                    table_state=table_state,
                    runner_state=runner_state,
                    parent_row_id=None,
                    array_index=None,
                    field_segments=[],
                    table_path="$",
                )
            return

        self._append_row(
            value=value,
            table_state=table_state,
            runner_state=runner_state,
            parent_row_id=None,
            array_index=None,
            field_segments=[],
            table_path="$",
        )

    def _append_row(
        self,
        *,
        value: Any,
        table_state: _TableState,
        runner_state: "_RunnerState",
        parent_row_id: str | None,
        array_index: int | None,
        field_segments: list[str],
        table_path: str,
    ) -> None:
        row_id = str(uuid4())
        row: dict[str, Any] = {"_row_id": row_id}
        if parent_row_id is not None:
            row["_parent_row_id"] = parent_row_id
        if array_index is not None:
            row["_array_index"] = array_index

        if isinstance(value, dict):
            self._populate_from_object(
                row=row,
                value=value,
                table_state=table_state,
                runner_state=runner_state,
                row_id=row_id,
                field_segments=field_segments,
                table_path=table_path,
            )
        elif isinstance(value, list):
            self._register_scalar_column(
                table_state=table_state,
                logical_path=f"{table_path}.{self.value_column_name}",
                physical_name=self.value_column_name,
            )
            row[self.value_column_name] = json.dumps(value, sort_keys=True)
        else:
            self._register_scalar_column(
                table_state=table_state,
                logical_path=f"{table_path}.{self.value_column_name}",
                physical_name=self.value_column_name,
            )
            row[self.value_column_name] = value

        table_state.rows.append(row)

    def _populate_from_object(
        self,
        *,
        row: dict[str, Any],
        value: dict[str, Any],
        table_state: _TableState,
        runner_state: "_RunnerState",
        row_id: str,
        field_segments: list[str],
        table_path: str,
    ) -> None:
        for key, nested_value in value.items():
            current_segments = [*field_segments, key]
            logical_path = _join_path(table_path, key)
            if isinstance(nested_value, dict):
                self._populate_from_object(
                    row=row,
                    value=nested_value,
                    table_state=table_state,
                    runner_state=runner_state,
                    row_id=row_id,
                    field_segments=current_segments,
                    table_path=logical_path,
                )
                continue
            if isinstance(nested_value, list):
                child_table = runner_state.get_or_create_table(
                    logical_path=logical_path,
                    name_segments=[runner_state.entity_name, *current_segments],
                    parent_table_name=table_state.physical_name,
                )
                for index, item in enumerate(nested_value):
                    self._append_row(
                        value=item,
                        table_state=child_table,
                        runner_state=runner_state,
                        parent_row_id=row_id,
                        array_index=index,
                        field_segments=[],
                        table_path=logical_path,
                    )
                continue

            physical_name = runner_state.make_column_name(current_segments)
            self._register_scalar_column(
                table_state=table_state,
                logical_path=logical_path,
                physical_name=physical_name,
            )
            row[physical_name] = nested_value

    def _register_scalar_column(
        self,
        *,
        table_state: _TableState,
        logical_path: str,
        physical_name: str,
    ) -> None:
        existing_name = table_state.column_mappings.get(logical_path)
        if existing_name is None:
            table_state.column_mappings[logical_path] = physical_name
            return
        if existing_name != physical_name:
            raise PipelineError(
                message="Conflicting column mapping detected during normalization",
                error_code="NORMALIZE_CONFLICTING_COLUMN_MAPPING",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "logical_path": logical_path,
                    "existing_name": existing_name,
                    "new_name": physical_name,
                    "table_name": table_state.physical_name,
                },
            )


class _RunnerState:
    def __init__(
        self,
        *,
        source_name: str,
        entity_name: str,
        max_identifier_length: int,
        separator: str,
        value_column_name: str,
    ) -> None:
        self.source_name = source_name
        self.entity_name = entity_name
        self.max_identifier_length = max_identifier_length
        self.separator = separator
        self.value_column_name = value_column_name
        self._tables_by_logical_path: dict[str, _TableState] = {}
        self._logical_path_by_physical_name: dict[str, str] = {}
        self._table_order: list[str] = []

    def get_or_create_table(
        self,
        *,
        logical_path: str,
        name_segments: list[str],
        parent_table_name: str | None,
    ) -> _TableState:
        existing = self._tables_by_logical_path.get(logical_path)
        if existing is not None:
            return existing

        candidate_name = self.make_table_name(
            name_segments=name_segments,
            logical_path=logical_path,
        )
        join_key_columns = ["_parent_row_id"] if parent_table_name else []
        table_state = _TableState(
            logical_path=logical_path,
            physical_name=candidate_name,
            parent_table_name=parent_table_name,
            join_key_columns=join_key_columns,
        )
        self._tables_by_logical_path[logical_path] = table_state
        self._logical_path_by_physical_name[candidate_name] = logical_path
        self._table_order.append(logical_path)
        return table_state

    def make_table_name(self, *, name_segments: list[str], logical_path: str) -> str:
        base_name = self.separator.join(_sanitize_identifier(segment) for segment in name_segments)
        used_path = self._logical_path_by_physical_name.get(base_name)
        if len(base_name) <= self.max_identifier_length and (used_path in (None, logical_path)):
            return base_name
        return self._build_hashed_identifier(base_name=base_name, logical_path=logical_path)

    def make_column_name(self, field_segments: list[str]) -> str:
        return self.separator.join(_sanitize_identifier(segment) for segment in field_segments)

    def build_tables(self) -> list[NormalizedTable]:
        tables: list[NormalizedTable] = []
        for logical_path in self._table_order:
            table_state = self._tables_by_logical_path[logical_path]
            tables.append(
                NormalizedTable(
                    logical_path=table_state.logical_path,
                    physical_name=table_state.physical_name,
                    parent_table_name=table_state.parent_table_name,
                    rows=table_state.rows,
                )
            )
        return tables

    def build_mapping_catalog(self) -> MappingCatalog:
        entries: list[TableMappingEntry] = []
        for logical_path in self._table_order:
            table_state = self._tables_by_logical_path[logical_path]
            column_mappings = [
                TableColumnMapping(logical_path=path, physical_name=name)
                for path, name in sorted(table_state.column_mappings.items())
            ]
            entries.append(
                TableMappingEntry(
                    logical_path=table_state.logical_path,
                    physical_table_name=table_state.physical_name,
                    parent_table_name=table_state.parent_table_name,
                    join_key_columns=table_state.join_key_columns,
                    column_mappings=column_mappings,
                )
            )

        mapping_version = self._build_mapping_version(entries)
        root_table_name = entries[0].physical_table_name if entries else _sanitize_identifier(
            self.entity_name
        )
        return MappingCatalog(
            source_name=self.source_name,
            entity_name=self.entity_name,
            mapping_version=mapping_version,
            root_table_name=root_table_name,
            entries=entries,
        )

    def _build_mapping_version(self, entries: list[TableMappingEntry]) -> str:
        canonical_payload = [
            {
                "logical_path": entry.logical_path,
                "physical_table_name": entry.physical_table_name,
                "parent_table_name": entry.parent_table_name,
                "join_key_columns": entry.join_key_columns,
                "column_mappings": [
                    {"logical_path": mapping.logical_path, "physical_name": mapping.physical_name}
                    for mapping in entry.column_mappings
                ],
            }
            for entry in entries
        ]
        raw = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

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
                context={"logical_path": logical_path, "candidate_name": candidate_name},
            )
        return candidate_name

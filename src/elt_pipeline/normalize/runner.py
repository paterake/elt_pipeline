from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from io import StringIO
from typing import Any
from uuid import uuid4

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize._policy import (
    IdentifierPolicy,
    build_mapping_catalog,
    join_path,
)
from elt_pipeline.normalize.models import (
    MappingCatalog,
    NormalizationResult,
    NormalizedTable,
    TableColumnMapping,
    TableMappingEntry,
)
from elt_pipeline.shared.errors import ErrorCategory, PipelineError


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
            ingest_date=manifest.ingest_started_at.date().isoformat(),
            run_id=manifest.run_id,
            policy=IdentifierPolicy(
                max_identifier_length=self.max_identifier_length,
                separator=self.separator,
                value_column_name=self.value_column_name,
            ),
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
                context={
                    "artifact_id": manifest.artifact_id,
                    "data_path": manifest.data_path,
                },
            ) from exc

        if not reader.fieldnames:
            raise PipelineError(
                message="Normalization expects a CSV payload with a header row",
                error_code="NORMALIZE_CSV_HEADER_REQUIRED",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={
                    "artifact_id": manifest.artifact_id,
                    "data_path": manifest.data_path,
                },
            )

        state = _RunnerState(
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            ingest_date=manifest.ingest_started_at.date().isoformat(),
            run_id=manifest.run_id,
            policy=IdentifierPolicy(
                max_identifier_length=self.max_identifier_length,
                separator=self.separator,
                value_column_name=self.value_column_name,
            ),
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
                    context={
                        "artifact_id": manifest.artifact_id,
                        "data_path": manifest.data_path,
                    },
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
                    context={
                        "artifact_id": manifest.artifact_id,
                        "data_path": manifest.data_path,
                    },
                ) from exc
        else:
            document = payload

        if not isinstance(document, (dict, list)):
            raise PipelineError(
                message="Normalization expects a JSON object or array payload",
                error_code="NORMALIZE_UNSUPPORTED_ROOT",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={
                    "artifact_id": manifest.artifact_id,
                    "data_path": manifest.data_path,
                },
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
                logical_path=f"{table_path}.{runner_state.policy.value_column_name}",
                physical_name=runner_state.policy.value_column_name,
            )
            row[runner_state.policy.value_column_name] = json.dumps(
                value, sort_keys=True
            )
        else:
            self._register_scalar_column(
                table_state=table_state,
                logical_path=f"{table_path}.{runner_state.policy.value_column_name}",
                physical_name=runner_state.policy.value_column_name,
            )
            row[runner_state.policy.value_column_name] = value

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
            logical_path = join_path(table_path, key)
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
        ingest_date: str,
        run_id: str,
        policy: IdentifierPolicy,
    ) -> None:
        self.source_name = source_name
        self.entity_name = entity_name
        self.ingest_date = ingest_date
        self.run_id = run_id
        self.policy = policy
        self.policy.reset()
        self._tables_by_logical_path: dict[str, _TableState] = {}
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

        candidate_name = self.policy.make_table_name(
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
        self._table_order.append(logical_path)
        return table_state

    def make_column_name(self, field_segments: list[str]) -> str:
        return self.policy.make_column_name(field_segments)

    def build_tables(self) -> list[NormalizedTable]:
        tables: list[NormalizedTable] = []
        for logical_path in self._table_order:
            table_state = self._tables_by_logical_path[logical_path]
            for row in table_state.rows:
                row.setdefault("source_name", self.source_name)
                row.setdefault("ingest_date", self.ingest_date)
                row.setdefault("_run_id", self.run_id)
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

        mapping_version, root_table_name = build_mapping_catalog(
            source_name=self.source_name,
            entity_name=self.entity_name,
            entries=entries,
        )
        return MappingCatalog(
            source_name=self.source_name,
            entity_name=self.entity_name,
            mapping_version=mapping_version,
            root_table_name=root_table_name,
            entries=entries,
        )

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field

from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StructField,
    StructType,
)

from elt_pipeline.normalize._policy import (
    IdentifierPolicy,
    build_mapping_catalog,
    join_path,
)
from elt_pipeline.normalize.models import (
    MappingCatalog,
    TableColumnMapping,
    TableMappingEntry,
)
from elt_pipeline.shared.errors import ErrorCategory, PipelineError


@dataclass
class PlannedArrayExplosion:
    array_accessor: str
    child_table_logical_path: str


@dataclass
class PlannedTable:
    logical_path: str
    physical_table_name: str
    parent_table_name: str | None
    join_key_columns: list[str]
    column_mappings: list[TableColumnMapping]
    scalar_accessors: list[tuple[str, str]] = dc_field(default_factory=list)
    child_arrays: list[PlannedArrayExplosion] = dc_field(default_factory=list)

    def to_mapping_entry(self) -> TableMappingEntry:
        return TableMappingEntry(
            logical_path=self.logical_path,
            physical_table_name=self.physical_table_name,
            parent_table_name=self.parent_table_name,
            join_key_columns=self.join_key_columns,
            column_mappings=self.column_mappings,
        )


@dataclass
class NormalizationPlan:
    entity_name: str
    source_name: str
    tables: list[PlannedTable] = dc_field(default_factory=list)
    _tables_by_logical_path: dict[str, PlannedTable] = dc_field(default_factory=dict)

    def mapping_entries(self) -> list[TableMappingEntry]:
        return [t.to_mapping_entry() for t in self.tables]

    def build_mapping_catalog(self) -> tuple[MappingCatalog, str]:
        entries = self.mapping_entries()
        mapping_version, root_table_name = build_mapping_catalog(
            source_name=self.source_name,
            entity_name=self.entity_name,
            entries=entries,
        )
        catalog = MappingCatalog(
            source_name=self.source_name,
            entity_name=self.entity_name,
            mapping_version=mapping_version,
            root_table_name=root_table_name,
            entries=entries,
        )
        return catalog, mapping_version


@dataclass
class _WalkFrame:
    planned: PlannedTable
    is_array_item: bool


class _PlanBuilder:
    def __init__(
        self,
        *,
        source_name: str,
        entity_name: str,
        policy: IdentifierPolicy,
    ) -> None:
        self.entity_name = entity_name
        self.source_name = source_name
        self.policy = policy
        self.plan = NormalizationPlan(
            entity_name=entity_name,
            source_name=source_name,
        )

    def build(self, schema: StructType | ArrayType) -> NormalizationPlan:
        if isinstance(schema, ArrayType):
            self._walk_root_array(schema)
        else:
            self._walk_root_struct(schema)
        return self.plan

    def _walk_root_struct(self, schema: StructType) -> None:
        root_table = self._create_table(
            logical_path="$",
            name_segments=[self.entity_name],
            parent_table_name=None,
        )
        for field in schema.fields:
            self._walk_field(
                field=field,
                frame=_WalkFrame(planned=root_table, is_array_item=False),
                logical_path=join_path("$", field.name),
                field_segments=[field.name],
            )

    def _walk_root_array(self, schema: ArrayType) -> None:
        root_table = self._create_table(
            logical_path="$",
            name_segments=[self.entity_name],
            parent_table_name=None,
        )
        element_type = schema.elementType
        if isinstance(element_type, StructType):
            for field in element_type.fields:
                self._walk_field(
                    field=field,
                    frame=_WalkFrame(planned=root_table, is_array_item=True),
                    logical_path=join_path("$", field.name),
                    field_segments=[field.name],
                )
        else:
            self._register_scalar(
                frame=_WalkFrame(planned=root_table, is_array_item=True),
                logical_path=f"$.{self.policy.value_column_name}",
                field_segments=[],
                physical_name=self.policy.value_column_name,
                field_accessor="value",
            )

    def _create_table(
        self,
        *,
        logical_path: str,
        name_segments: list[str],
        parent_table_name: str | None,
    ) -> PlannedTable:
        physical_name = self.policy.make_table_name(
            name_segments=name_segments, logical_path=logical_path
        )
        join_key_columns = ["_parent_row_id"] if parent_table_name else []
        planned = PlannedTable(
            logical_path=logical_path,
            physical_table_name=physical_name,
            parent_table_name=parent_table_name,
            join_key_columns=join_key_columns,
            column_mappings=[],
            scalar_accessors=[],
            child_arrays=[],
        )
        self.plan.tables.append(planned)
        self.plan._tables_by_logical_path[logical_path] = planned
        return planned

    def _walk_field(
        self,
        *,
        field: StructField,
        frame: _WalkFrame,
        logical_path: str,
        field_segments: list[str],
    ) -> None:
        dtype = field.dataType
        if isinstance(dtype, StructType):
            for inner_field in dtype.fields:
                self._walk_field(
                    field=inner_field,
                    frame=frame,
                    logical_path=join_path(logical_path, inner_field.name),
                    field_segments=[*field_segments, inner_field.name],
                )
            return
        if isinstance(dtype, ArrayType):
            child_table_logical_path = logical_path
            child_name_segments = [self.entity_name, *field_segments]
            child_table = self._create_table(
                logical_path=child_table_logical_path,
                name_segments=child_name_segments,
                parent_table_name=frame.planned.physical_table_name,
            )
            array_accessor = ".".join(field_segments)
            frame.planned.child_arrays.append(
                PlannedArrayExplosion(
                    array_accessor=array_accessor,
                    child_table_logical_path=child_table_logical_path,
                )
            )
            element_type = dtype.elementType
            if isinstance(element_type, StructType):
                for inner_field in element_type.fields:
                    self._walk_field(
                        field=inner_field,
                        frame=_WalkFrame(planned=child_table, is_array_item=True),
                        logical_path=join_path(child_table_logical_path, inner_field.name),
                        field_segments=[inner_field.name],
                    )
            else:
                self._register_scalar(
                    frame=_WalkFrame(planned=child_table, is_array_item=True),
                    logical_path=f"{child_table_logical_path}.{self.policy.value_column_name}",
                    field_segments=[],
                    physical_name=self.policy.value_column_name,
                    field_accessor="value",
                )
            return
        if isinstance(dtype, MapType):
            physical_name = self.policy.make_column_name(field_segments)
            field_accessor = ".".join(field_segments)
            self._register_scalar(
                frame=frame,
                logical_path=logical_path,
                field_segments=field_segments,
                physical_name=physical_name,
                field_accessor=field_accessor,
            )
            return
        if frame.is_array_item and len(field_segments) == 0:
            physical_name = self.policy.value_column_name
            self._register_scalar(
                frame=frame,
                logical_path=f"{frame.planned.logical_path}.{self.policy.value_column_name}",
                field_segments=[],
                physical_name=physical_name,
                field_accessor="value",
            )
            return
        physical_name = self.policy.make_column_name(field_segments)
        field_accessor = ".".join(field_segments)
        self._register_scalar(
            frame=frame,
            logical_path=logical_path,
            field_segments=field_segments,
            physical_name=physical_name,
            field_accessor=field_accessor,
        )

    def _register_scalar(
        self,
        *,
        frame: _WalkFrame,
        logical_path: str,
        field_segments: list[str],
        physical_name: str,
        field_accessor: str,
    ) -> None:
        _ = field_segments
        existing = dict(frame.planned.column_mappings)
        existing_name = existing.get(logical_path)
        if existing_name is not None:
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
                        "table_name": frame.planned.physical_table_name,
                    },
                )
            return
        mapping = TableColumnMapping(
            logical_path=logical_path,
            physical_name=physical_name,
        )
        frame.planned.column_mappings.append(mapping)
        frame.planned.scalar_accessors.append((physical_name, field_accessor))


class NormalizationPlanner:
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

    def plan_from_schema(
        self,
        *,
        source_name: str,
        entity_name: str,
        schema: StructType | ArrayType,
    ) -> NormalizationPlan:
        policy = IdentifierPolicy(
            max_identifier_length=self.max_identifier_length,
            separator=self.separator,
            value_column_name=self.value_column_name,
        )
        builder = _PlanBuilder(
            source_name=source_name,
            entity_name=entity_name,
            policy=policy,
        )
        return builder.build(schema)

    def plan_from_csv_header(
        self,
        *,
        source_name: str,
        entity_name: str,
        fieldnames: list[str],
    ) -> NormalizationPlan:
        policy = IdentifierPolicy(
            max_identifier_length=self.max_identifier_length,
            separator=self.separator,
            value_column_name=self.value_column_name,
        )
        builder = _PlanBuilder(
            source_name=source_name,
            entity_name=entity_name,
            policy=policy,
        )
        schema_fields = [
            StructField(str(f or ""), _placeholder_scalar_type(), True)
            for f in fieldnames
        ]
        schema = StructType(schema_fields)
        return builder.build(schema)


def _placeholder_scalar_type() -> DataType:
    from pyspark.sql.types import StringType

    return StringType()

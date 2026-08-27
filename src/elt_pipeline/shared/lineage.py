from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from elt_pipeline.shared.governance import SqlColumnSpec

OPENLINEAGE_PRODUCER_URI: str = (
    "https://github.com/emailrak/elt_pipeline/openlineage/1"
)
OPENLINEAGE_SCHEMA_URL: str = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
)
OPENLINEAGE_DEFAULT_NAMESPACE: str = "elt_pipeline"
OPENLINEAGE_SCHEMA_FACET_SCHEMA_URL: str = (
    "https://openlineage.io/spec/facets/1-1-1/"
    "SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"
)
OPENLINEAGE_COLUMN_LINEAGE_FACET_SCHEMA_URL: str = (
    "https://openlineage.io/spec/facets/1-0-0/"
    "ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"
)


class DatasetRef(BaseModel):
    namespace: str
    name: str
    facets: dict[str, object] = Field(default_factory=dict)


class LineageEvent(BaseModel):
    event_type: str
    event_time: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    run_id: str
    job_name: str
    producer: str = "elt_pipeline"
    inputs: list[DatasetRef] = Field(default_factory=list)
    outputs: list[DatasetRef] = Field(default_factory=list)
    run_facets: dict[str, object] = Field(default_factory=dict)
    job_facets: dict[str, object] = Field(default_factory=dict)
    job_namespace: str | None = None
    environment: str | None = None


class _OLRun(BaseModel):
    runId: str
    facets: dict[str, object] = Field(default_factory=dict)


class _OLJob(BaseModel):
    namespace: str
    name: str
    facets: dict[str, object] = Field(default_factory=dict)


class _OLDataset(BaseModel):
    namespace: str
    name: str
    facets: dict[str, object] = Field(default_factory=dict)


class _OLInputDataset(_OLDataset):
    inputFacets: dict[str, object] = Field(default_factory=dict)


class _OLOutputDataset(_OLDataset):
    outputFacets: dict[str, object] = Field(default_factory=dict)


class OpenLineageRunEvent(BaseModel):
    eventType: str
    eventTime: str
    run: _OLRun
    job: _OLJob
    inputs: list[_OLInputDataset] = Field(default_factory=list)
    outputs: list[_OLOutputDataset] = Field(default_factory=list)
    producer: str
    schemaURL: str = OPENLINEAGE_SCHEMA_URL


def convert_to_openlineage_run_event(
    event: LineageEvent,
    *,
    producer_uri: str = OPENLINEAGE_PRODUCER_URI,
    schema_url: str = OPENLINEAGE_SCHEMA_URL,
    default_namespace: str = OPENLINEAGE_DEFAULT_NAMESPACE,
) -> OpenLineageRunEvent:
    job_namespace = event.job_namespace or default_namespace
    run_facets = dict(event.run_facets)
    if event.environment:
        run_facets.setdefault(
            "environment",
            {
                "_producer": producer_uri,
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-0/"
                    "EnvironmentRunFacet.json#/$defs/EnvironmentRunFacet"
                ),
                "environmentName": event.environment,
            },
        )
    return OpenLineageRunEvent(
        eventType=event.event_type,
        eventTime=event.event_time.isoformat(),
        run=_OLRun(runId=event.run_id, facets=run_facets),
        job=_OLJob(namespace=job_namespace, name=event.job_name, facets=event.job_facets),
        inputs=[
            _OLInputDataset(
                namespace=d.namespace or default_namespace,
                name=d.name,
                facets=d.facets,
            )
            for d in event.inputs
        ],
        outputs=[
            _OLOutputDataset(
                namespace=d.namespace or default_namespace,
                name=d.name,
                facets=d.facets,
            )
            for d in event.outputs
        ],
        producer=producer_uri,
        schemaURL=schema_url,
    )


def _normalise_facet_type(raw: str | None) -> str:
    if raw is None:
        return "UNKNOWN"
    cleaned = raw.strip().upper().replace(" ", "")
    if not cleaned:
        return "UNKNOWN"
    return cleaned


def build_openlineage_schema_dataset_facet(
    columns: list[SqlColumnSpec],
    *,
    producer_uri: str = OPENLINEAGE_PRODUCER_URI,
    _fallback_type_token: str = "UNKNOWN",
) -> dict[str, object]:
    """Build an OpenLineage ``SchemaDatasetFacet`` from SqlColumnSpec entries.

    The facet is a wire-format dict compatible with the 1-1-1 JSON schema. It
    is attached as ``outputs[i].facets["schema"]`` when emitting a run event
    so that consumers (Marquez, DataHub, Amundsen etc.) can render column-level
    metadata, descriptions, classification tags, and nullability without
    crawling the warehouse catalog separately.

    All unknown / unset fields are filled with stable sentinel tokens rather
    than omitted, so OL parsers do not have to accept sparse records.
    """
    fields: list[dict[str, object]] = []
    for col in columns:
        facet_type = _normalise_facet_type(col.type)
        if facet_type == "UNKNOWN":
            facet_type = _fallback_type_token
        field: dict[str, object] = {
            "name": col.name,
            "type": facet_type,
        }
        if col.description:
            field["description"] = col.description
        if col.nullable is not None:
            field["nullable"] = bool(col.nullable)
        if col.classification is not None or col.custom_tags:
            tags: list[str] = []
            if col.classification is not None:
                tags.append(f"classification.{col.classification.value}")
            for t_key, t_val in col.custom_tags.items():
                if t_val:
                    tags.append(f"{t_key}={t_val}")
                else:
                    tags.append(t_key)
            if tags:
                field["tags"] = tags
        fields.append(field)
    return {
        "_producer": producer_uri,
        "_schemaURL": OPENLINEAGE_SCHEMA_FACET_SCHEMA_URL,
        "fields": fields,
    }


def build_openlineage_column_lineage_facet(
    *,
    column_lineage_map: dict[str, list[tuple[str, str]]],
    producer_uri: str = OPENLINEAGE_PRODUCER_URI,
) -> dict[str, object]:
    """Build an OpenLineage ``ColumnLineageDatasetFacet``.

    ``column_lineage_map`` is a mapping of *output column name* → list of
    ``(input_dataset_fqn, input_column_name)`` pairs identifying which input
    dataset columns flow into the named output column.  The list may be empty
    for literal / identity columns that have no upstream source (for example
    ``current_timestamp()``); this is preserved so OL UIs surface it
    explicitly.
    """
    facets: dict[str, object] = {}
    for out_col in sorted(column_lineage_map.keys()):
        lineage_entries = []
        for (in_ds_fqn, in_col) in column_lineage_map[out_col]:
            lineage_entries.append(
                {
                    "namespace": "elt_pipeline",
                    "name": in_ds_fqn,
                    "field": in_col,
                }
            )
        facets[out_col] = {
            "inputFields": lineage_entries,
            "transformationType": "DIRECT" if lineage_entries else "LITERAL",
            "transformationDescription": (
                "Derived from upstream columns" if lineage_entries
                else "Literal or built-in expression with no upstream source"
            ),
        }
    return {
        "_producer": producer_uri,
        "_schemaURL": OPENLINEAGE_COLUMN_LINEAGE_FACET_SCHEMA_URL,
        "fields": facets,
    }

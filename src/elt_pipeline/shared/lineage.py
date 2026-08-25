from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

OPENLINEAGE_PRODUCER_URI: str = (
    "https://github.com/emailrak/elt_pipeline/openlineage/1"
)
OPENLINEAGE_SCHEMA_URL: str = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
)
OPENLINEAGE_DEFAULT_NAMESPACE: str = "elt_pipeline"


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

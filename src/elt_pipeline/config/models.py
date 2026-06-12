from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ConfigLayer(BaseModel):
    model_config = ConfigDict(extra="allow")

    trigger_mode: str | None = None
    auth: dict[str, Any] = Field(default_factory=dict)
    extraction: dict[str, Any] = Field(default_factory=dict)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    defaults: dict[str, Any] = Field(default_factory=dict)

    def to_payload(self, *, exclude: set[str] | None = None) -> dict[str, Any]:
        excluded = {"defaults"}
        if exclude:
            excluded.update(exclude)
        return self.model_dump(exclude=excluded, exclude_none=True)


class EntityConfig(ConfigLayer):
    name: str


class SourceConfig(ConfigLayer):
    name: str
    connector_type: str
    entities: list[EntityConfig] = Field(default_factory=list)


class EnvironmentOverlay(BaseModel):
    defaults: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    schema_version: str
    defaults: dict[str, Any] = Field(default_factory=dict)
    environments: dict[str, EnvironmentOverlay] = Field(default_factory=dict)
    sources: list[SourceConfig] = Field(default_factory=list)

    def get_source(self, source_name: str) -> SourceConfig:
        for source in self.sources:
            if source.name == source_name:
                return source
        raise LookupError(f"Unknown source: {source_name}")


class ResolvedEntityConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    connector_type: str
    trigger_mode: str | None = None
    auth: dict[str, Any] = Field(default_factory=dict)
    extraction: dict[str, Any] = Field(default_factory=dict)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    resolved_defaults: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

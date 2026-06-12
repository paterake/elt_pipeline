from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from elt_pipeline.config.models import PipelineConfig, ResolvedEntityConfig
from elt_pipeline.shared.errors import ConfigValidationError


def load_pipeline_config(config_path: str | Path) -> PipelineConfig:
    path = Path(config_path)
    if not path.exists():
        raise ConfigValidationError(
            message=f"Configuration file does not exist: {path}",
            context={"config_path": str(path)},
        )

    try:
        raw_config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse YAML configuration: {exc}",
            context={"config_path": str(path)},
        ) from exc

    try:
        return PipelineConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Configuration schema validation failed",
            context={"config_path": str(path), "errors": exc.errors(include_url=False)},
        ) from exc


def resolve_entity_config(
    config: PipelineConfig,
    *,
    environment: str,
    source_name: str,
    entity_name: str,
) -> ResolvedEntityConfig:
    source = _get_source(config, source_name)
    entity = _get_entity(source, entity_name)
    default_environment = config.environments.get("default")
    environment_overlay = config.environments.get(environment)
    if environment_overlay is None:
        raise ConfigValidationError(
            message=f"Unknown environment overlay: {environment}",
            context={"environment": environment},
        )

    merged_defaults = _deep_merge(
        config.defaults,
        default_environment.defaults if default_environment else {},
        environment_overlay.defaults,
        source.defaults,
        entity.defaults,
    )
    merged_payload = _deep_merge(
        source.to_payload(exclude={"name", "connector_type", "entities"}),
        entity.to_payload(exclude={"name"}),
    )

    trigger_mode = merged_payload.pop("trigger_mode", None)
    auth = merged_payload.pop("auth", {})
    extraction = merged_payload.pop("extraction", {})
    persistence = merged_payload.pop("persistence", {})
    state = merged_payload.pop("state", {})
    settings = _deep_merge(merged_defaults, merged_payload.pop("settings", {}), merged_payload)

    return ResolvedEntityConfig(
        schema_version=config.schema_version,
        environment=environment,
        source_name=source.name,
        entity_name=entity.name,
        connector_type=source.connector_type,
        trigger_mode=trigger_mode,
        auth=auth,
        extraction=extraction,
        persistence=persistence,
        state=state,
        settings=settings,
        resolved_defaults=merged_defaults,
        raw={
            "source": source.model_dump(mode="python"),
            "entity": entity.model_dump(mode="python"),
        },
    )


def _get_source(config: PipelineConfig, source_name: str):
    try:
        return config.get_source(source_name)
    except LookupError as exc:
        raise ConfigValidationError(
            message=str(exc),
            context={"source_name": source_name},
        ) from exc


def _get_entity(source, entity_name: str):
    for entity in source.entities:
        if entity.name == entity_name:
            return entity
    raise ConfigValidationError(
        message=f"Unknown entity '{entity_name}' for source '{source.name}'",
        context={"source_name": source.name, "entity_name": entity_name},
    )


def _deep_merge(*layers: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for layer in layers:
        merged = _merge_two_dicts(merged, layer)
    return merged


def _merge_two_dicts(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(left)
    for key, value in right.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _merge_two_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged

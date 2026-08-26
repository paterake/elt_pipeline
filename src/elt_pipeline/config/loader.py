from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from elt_pipeline.config.models import PipelineConfig, ResolvedEntityConfig, RuntimeConfig
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    _validate_root_is_string,
    detect_scheme,
)


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


def load_runtime_overrides(
    config_path: str | Path | None,
    *,
    environment: str | None = None,
) -> dict[str, Any]:
    """Load infrastructure-wide runtime defaults from the pipeline YAML.

    Returns a deep-merged dict shaped like the RuntimeConfig Pydantic model
    (with None values for fields not set by the user).  If no config_path is
    provided, return empty dict so callers fall through to ENV + manifest
    frozen defaults.

    Merge order (later layers win):
      1. PipelineConfig.runtime (lowest precedence)
      2. environments["default"].runtime (if present)
      3. environments[environment].runtime (highest precedence runtime layer,
         if `environment` is set and the overlay is defined)

    Design contract — portable, no-code friendly:
      * Users set infrastructure config ONCE in the pipeline YAML runtime
        section and NEVER need to export ELT_PIPELINE_* env vars on the shell.
      * ENV vars remain available as the highest-precedence override at call
        time (useful for CI/secret injection), but are optional for the
        workstation happy path.
    """
    if config_path is None:
        return {}
    config = load_pipeline_config(config_path)
    layers: list[Mapping[str, Any]] = [
        config.runtime.model_dump(mode="python", exclude_none=True),
    ]
    default_env = config.environments.get("default")
    if default_env is not None:
        layers.append(
            default_env.runtime.model_dump(mode="python", exclude_none=True)
        )
    if environment:
        selected = config.environments.get(environment)
        if selected is not None:
            layers.append(
                selected.runtime.model_dump(mode="python", exclude_none=True)
            )
    merged = _deep_merge(*layers)
    # Round-trip through RuntimeConfig to guarantee returned dict only contains
    # RuntimeConfig fields — arbitrary extra keys from the deep merge are
    # rejected by Pydantic strictness.
    sanitized = RuntimeConfig.model_validate(merged)
    return sanitized.model_dump(mode="python", exclude_none=True)


def resolve_entity_config(
    config: PipelineConfig,
    *,
    environment: str,
    source_name: str,
    entity_name: str,
    config_path: str | Path | None = None,
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
    source_payload = source.to_payload(exclude={"name", "connector_type", "entities"})
    entity_payload = entity.to_payload(exclude={"name"})
    # Cascade source-level extraction defaults (extraction inside the
    # `source.defaults` dict) into the entity extraction block BEFORE the
    # entity extraction overrides win.  This is the "list 50 simple tables
    # under one source with a single shared extraction spec" ergonomic.
    source_defaults_extraction = source.defaults.get("extraction") or {}
    source_top_level_extraction = source_payload.pop("extraction", {}) or {}
    entity_top_level_extraction = entity_payload.pop("extraction", {}) or {}
    extraction = _deep_merge(
        source_defaults_extraction,
        source_top_level_extraction,
        entity_top_level_extraction,
    )

    trigger_mode = source_payload.pop("trigger_mode", None)
    trigger_mode = entity_payload.pop("trigger_mode", trigger_mode)
    level2_mode = source_payload.pop("level2_mode", "required_level2")
    level2_mode = entity_payload.pop("level2_mode", level2_mode)
    auth = _deep_merge(
        source_payload.pop("auth", {}) or {},
        entity_payload.pop("auth", {}) or {},
    )
    persistence = _deep_merge(
        source_payload.pop("persistence", {}) or {},
        entity_payload.pop("persistence", {}) or {},
    )
    state = _deep_merge(
        source_payload.pop("state", {}) or {},
        entity_payload.pop("state", {}) or {},
    )
    settings = _deep_merge(
        merged_defaults,
        source_payload.pop("settings", {}) or {},
        entity_payload.pop("settings", {}) or {},
        source_payload,
        entity_payload,
    )
    if config_path is not None:
        settings.setdefault(
            "config_file_dir", str(Path(config_path).resolve().parent)
        )
        settings.setdefault(
            "config_file_path", str(Path(config_path).resolve())
        )

    return ResolvedEntityConfig(
        schema_version=config.schema_version,
        environment=environment,
        source_name=source.name,
        entity_name=entity.name,
        connector_type=source.connector_type,
        trigger_mode=trigger_mode,
        level2_mode=level2_mode,
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


def _iter_configured_bucket_paths(config: PipelineConfig) -> list[str]:
    buckets: list[str] = []
    for source in config.sources:
        payload = source.to_payload(exclude={"name", "connector_type", "entities"})
        for candidate_key in ("bucket_path", "root_path", "data_root", "kafka_log_path"):
            candidate = payload.get(candidate_key)
            if isinstance(candidate, str) and candidate:
                buckets.append(candidate)
        for entity in source.entities:
            entity_payload = entity.to_payload(exclude={"name"})
            for candidate_key in ("bucket_path", "root_path", "data_root"):
                candidate = entity_payload.get(candidate_key)
                if isinstance(candidate, str) and candidate:
                    buckets.append(candidate)
    return buckets


def validate_config_root_schemes(
    *,
    root_path: str,
    warehouse_root: str,
    config: PipelineConfig | None = None,
    extra_paths: list[str] | None = None,
) -> None:
    paths_to_check = [root_path, warehouse_root]
    if config is not None:
        paths_to_check.extend(_iter_configured_bucket_paths(config))
    if extra_paths:
        paths_to_check.extend(extra_paths)

    invalid: list[tuple[str, str]] = []
    for path_str in paths_to_check:
        if not isinstance(path_str, str) or not path_str:
            continue
        try:
            _validate_root_is_string(path_str)
        except (ConfigValidationError, TypeError, ValueError):
            invalid.append((path_str, f"root-not-string:{type(path_str).__name__}"))
            continue
        # detect_scheme() raises ConfigValidationError itself for unsupported :// prefixes
        try:
            scheme = detect_scheme(path_str)
        except ConfigValidationError as exc:
            detail = exc.context.get("message") if exc.context else str(exc.message)
            invalid.append((path_str, detail or "unsupported-scheme-in-path"))
            continue
        # Suffix / prefix hygiene checks (scheme is already valid enum member at this point)
        if scheme == _StorageScheme.file:
            # Triple-slash variant file:/// is valid (matches RFC); we permit both
            # file:// and file:///. Error only for literal file:/ single slash:
            single_slash = path_str.startswith("file:/") and not path_str.startswith("file://")
            if single_slash:
                invalid.append(
                    (
                        path_str,
                        "unsupported-file-scheme-single-slash:file:/... (expected 'file://')",
                    )
                )
                continue
        if scheme == _StorageScheme.s3:
            cleaned = path_str[len("s3://"):]
            if not cleaned or cleaned == "/":
                invalid.append((path_str, "s3-bucket-empty:expected s3://<bucket>/<prefix>"))
                continue

    if invalid:
        raise ConfigValidationError(
            message=(
                "One or more storage roots use unsupported schemes. "
                "Allowed schemes are: s3:// (object storage), file:// (absolute "
                "local), or an un-prefixed absolute/relative POSIX path. "
                "Unsupported: s3a://, hdfs://, gs://, abfss://, wasb(s)://, etc."
            ),
            context={
                "valid_schemes": ["s3://", "file://", "(unprefixed POSIX path)"],
                "invalid_paths": [
                    {"path": path, "detail": detail} for path, detail in invalid
                ],
            },
        )

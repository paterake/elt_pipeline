"""Connector family registry (BACKLOG item M-1).

Design contract (matches B-6 storage_backends, G-5 secrets pattern exactly):

1. ConnectorFamily enum (explicit boundary; 4 built-in families = rest/sql/kafka/object_storage)
2. ConnectorFactory @runtime_checkable Protocol with:
   - family_type: str
   - build_config_from_resolved(resolved_config) -> BaseModel (XxxConfig validation)
   - build_connector(config, run_context, root_path, **kwargs) -> runnable connector
3. _CONNECTOR_REGISTRY: dict[ConnectorFamily, ConnectorFactory] singleton
4. register_connector_factory() / get_connector_factory() public API (no dynamic auto-discovery)
5. Lazy init via _ensure_default_connectors_registered() (4 built-in families at first use)
6. Env var centralization via EnvVarNames (registry manifest path + strict mode)
7. Connector manifest (YAML/JSON): named presets within existing 4 families, loaded at
   factory-init time.  No-code authoring for pipeline-specific source presets.

Zero-env lockdown: manifest-path env vars are read ONLY through the runtime_context
cascade (or explicit loader callers); no direct os.environ reads here.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field, ValidationError

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors.kafka import (
    KafkaConnectorConfig,
)
from elt_pipeline.ingest.connectors.local_kafka import LocalKafkaConnector
from elt_pipeline.ingest.connectors.local_object_storage import (
    LocalObjectStorageConnector,
)
from elt_pipeline.ingest.connectors.local_rest import LocalRestConnector
from elt_pipeline.ingest.connectors.local_sql import LocalSqlConnector
from elt_pipeline.ingest.connectors.object_storage import (
    ObjectStorageConnectorConfig,
)
from elt_pipeline.ingest.connectors.rest import (
    RestConnectorConfig,
)
from elt_pipeline.ingest.connectors.sql import (
    SqlConnectorConfig,
)
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import path_read_text
from elt_pipeline.shared.runtime import RunContext

# ---------------------------------------------------------------------------
# Family enum + errors
# ---------------------------------------------------------------------------


class ConnectorFamily(str, Enum):
    rest = "rest"
    sql = "sql"
    kafka = "kafka"
    object_storage = "object_storage"


_SUPPORTED_FAMILIES: set[str] = {f.value for f in ConnectorFamily}


class ConnectorRegistryError(PipelineError):
    def __init__(
        self,
        *,
        message: str,
        error_code: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            error_category=ErrorCategory.config_error,
            retryable=False,
            context=context,
        )


class ConnectorFamilyUnsupportedError(ConnectorRegistryError):
    def __init__(self, *, family: str, context: dict[str, Any] | None = None) -> None:
        ctx: dict[str, Any] = {
            "family": family,
            "supported_families": sorted(_SUPPORTED_FAMILIES),
        }
        if context:
            ctx.update(context)
        super().__init__(
            message=(
                f"Unsupported connector family '{family}'. "
                f"Supported families: {sorted(_SUPPORTED_FAMILIES)}. "
                f"Register a new family via connectors.register_connector_factory() "
                f"or use an existing built-in family."
            ),
            error_code="CONNECTOR_FAMILY_UNSUPPORTED",
            context=ctx,
        )


# ---------------------------------------------------------------------------
# ConnectorFactory Protocol + registry singleton
# ---------------------------------------------------------------------------


@runtime_checkable
class ConnectorFactory(Protocol):
    family_type: str

    def build_config_from_resolved(
        self,
        *,
        resolved_config: ResolvedEntityConfig,
    ) -> BaseModel: ...

    def build_connector(
        self,
        *,
        config: BaseModel,
        run_context: RunContext,
        root_path: str,
        **kwargs: Any,
    ) -> Any: ...


_CONNECTOR_REGISTRY: dict[ConnectorFamily, ConnectorFactory] = {}


def register_connector_factory(
    family: ConnectorFamily | str,
    factory: ConnectorFactory,
) -> None:
    family_key = (
        ConnectorFamily(family) if isinstance(family, str) else family
    )
    if family_key in _CONNECTOR_REGISTRY:
        raise ConnectorRegistryError(
            message=(
                f"Connector factory already registered for family "
                f"'{family_key.value}'.  Use register_connector_factory() only once "
                f"per family per process."
            ),
            error_code="CONNECTOR_FACTORY_ALREADY_REGISTERED",
            context={"family": family_key.value},
        )
    if not isinstance(factory, ConnectorFactory):
        raise ConnectorRegistryError(
            message=(
                f"register_connector_factory expected ConnectorFactory Protocol "
                f"implementor, got {type(factory).__name__}."
            ),
            error_code="CONNECTOR_FACTORY_INVALID",
            context={"family": family_key.value, "type": type(factory).__name__},
        )
    _CONNECTOR_REGISTRY[family_key] = factory


def get_connector_factory(
    family: ConnectorFamily | str,
) -> ConnectorFactory:
    _ensure_default_connectors_registered()
    try:
        family_key = (
            ConnectorFamily(family) if isinstance(family, str) else family
        )
    except ValueError as exc:
        raise ConnectorFamilyUnsupportedError(family=str(family)) from exc
    if family_key not in _CONNECTOR_REGISTRY:
        raise ConnectorRegistryError(
            message=(
                f"No ConnectorFactory registered for family '{family_key.value}'. "
                f"Register one via connectors.register_connector_factory()."
            ),
            error_code="CONNECTOR_NO_FACTORY",
            context={"family": family_key.value},
        )
    return _CONNECTOR_REGISTRY[family_key]


def is_connector_factory_registered(
    family: ConnectorFamily | str,
) -> bool:
    _ensure_default_connectors_registered()
    try:
        family_key = (
            ConnectorFamily(family) if isinstance(family, str) else family
        )
    except ValueError:
        return False
    return family_key in _CONNECTOR_REGISTRY


# ---------------------------------------------------------------------------
# 4 built-in concrete factories
# ---------------------------------------------------------------------------


class _RestConnectorFactory:
    family_type = "rest"

    def build_config_from_resolved(
        self,
        *,
        resolved_config: ResolvedEntityConfig,
    ) -> RestConnectorConfig:
        return RestConnectorConfig.from_resolved_entity_config(resolved_config)

    def build_connector(
        self,
        *,
        config: BaseModel,
        run_context: RunContext,
        root_path: str,
        **kwargs: Any,
    ) -> LocalRestConnector:
        if not isinstance(config, RestConnectorConfig):
            raise ConfigValidationError(
                message="Rest connector factory requires RestConnectorConfig",
                context={"config_type": type(config).__name__},
            )
        return LocalRestConnector(
            config=config,
            run_context=run_context,
            root_path=root_path,
        )


class _SqlConnectorFactory:
    family_type = "sql"

    def build_config_from_resolved(
        self,
        *,
        resolved_config: ResolvedEntityConfig,
    ) -> SqlConnectorConfig:
        return SqlConnectorConfig.from_resolved_entity_config(resolved_config)

    def build_connector(
        self,
        *,
        config: BaseModel,
        run_context: RunContext,
        root_path: str,
        **kwargs: Any,
    ) -> LocalSqlConnector:
        if not isinstance(config, SqlConnectorConfig):
            raise ConfigValidationError(
                message="Sql connector factory requires SqlConnectorConfig",
                context={"config_type": type(config).__name__},
            )
        return LocalSqlConnector(
            config=config,
            run_context=run_context,
            root_path=root_path,
        )


class _ObjectStorageConnectorFactory:
    family_type = "object_storage"

    def build_config_from_resolved(
        self,
        *,
        resolved_config: ResolvedEntityConfig,
    ) -> ObjectStorageConnectorConfig:
        return ObjectStorageConnectorConfig.from_resolved_entity_config(resolved_config)

    def build_connector(
        self,
        *,
        config: BaseModel,
        run_context: RunContext,
        root_path: str,
        **kwargs: Any,
    ) -> LocalObjectStorageConnector:
        if not isinstance(config, ObjectStorageConnectorConfig):
            raise ConfigValidationError(
                message="ObjectStorage connector factory requires ObjectStorageConnectorConfig",
                context={"config_type": type(config).__name__},
            )
        return LocalObjectStorageConnector(
            config=config,
            run_context=run_context,
            root_path=root_path,
        )


class _KafkaConnectorFactory:
    family_type = "kafka"

    def build_config_from_resolved(
        self,
        *,
        resolved_config: ResolvedEntityConfig,
    ) -> KafkaConnectorConfig:
        return KafkaConnectorConfig.from_resolved_entity_config(resolved_config)

    def build_connector(
        self,
        *,
        config: BaseModel,
        run_context: RunContext,
        root_path: str,
        **kwargs: Any,
    ) -> LocalKafkaConnector:
        if not isinstance(config, KafkaConnectorConfig):
            raise ConfigValidationError(
                message="Kafka connector factory requires KafkaConnectorConfig",
                context={"config_type": type(config).__name__},
            )
        log_path = kwargs.get("log_path")
        if log_path is None:
            raise ConfigValidationError(
                message="Kafka connector factory requires log_path= kwarg",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                },
            )
        return LocalKafkaConnector(
            config=config,
            run_context=run_context,
            root_path=root_path,
            log_path=log_path,
        )


# ---------------------------------------------------------------------------
# Lazy default registration
# ---------------------------------------------------------------------------


def _ensure_default_connectors_registered() -> None:
    if _CONNECTOR_REGISTRY:
        return
    register_connector_factory(ConnectorFamily.rest, _RestConnectorFactory())
    register_connector_factory(ConnectorFamily.sql, _SqlConnectorFactory())
    register_connector_factory(
        ConnectorFamily.object_storage, _ObjectStorageConnectorFactory()
    )
    register_connector_factory(ConnectorFamily.kafka, _KafkaConnectorFactory())


# ---------------------------------------------------------------------------
# Connector manifest (no-code presets within the 4 built-in families)
# ---------------------------------------------------------------------------


class ConnectorPreset(BaseModel):
    name: str
    family: ConnectorFamily
    description: str | None = None
    extraction_defaults: dict[str, Any] = Field(default_factory=dict)
    auth_defaults: dict[str, Any] | None = None
    settings_defaults: dict[str, Any] = Field(default_factory=dict)
    persistence_defaults: dict[str, Any] = Field(default_factory=dict)


class ConnectorManifest(BaseModel):
    schema_version: str = "1.0"
    presets: list[ConnectorPreset] = Field(default_factory=list)

    def preset_by_name(self, name: str) -> ConnectorPreset | None:
        for preset in self.presets:
            if preset.name == name:
                return preset
        return None


_MANIFEST_CACHE: dict[str, ConnectorManifest] = {}


def _parse_manifest_from_text(text: str, *, source_path: str) -> ConnectorManifest:
    candidates: list[tuple[str, Any]] = []
    try:
        import json
        candidates.append(("json", json.loads(text)))
    except Exception:
        pass
    try:
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError:
            yaml = None
        if yaml is not None:
            candidates.append(("yaml", yaml.safe_load(text)))
    except Exception:
        pass
    if not candidates:
        raise ConfigValidationError(
            message=(
                f"Connector manifest at {source_path!r} could not be parsed as "
                f"JSON or YAML."
            ),
            context={"source_path": source_path},
        )
    last_errors: list[str] = []
    for fmt, payload in candidates:
        try:
            return ConnectorManifest.model_validate(payload)
        except ValidationError as exc:
            last_errors.append(
                f"{fmt}: {'; '.join(e['msg'] for e in exc.errors(include_url=False))}"
            )
    raise ConfigValidationError(
        message=(
            f"Connector manifest at {source_path!r} failed validation against "
            f"ConnectorManifest schema."
        ),
        context={
            "source_path": source_path,
            "parse_errors": last_errors,
        },
    )


def load_connector_manifest_from_yaml(
    yaml_path: str,
    *,
    cache: bool = True,
) -> ConnectorManifest:
    cache_key = f"yaml:{yaml_path}"
    if cache and cache_key in _MANIFEST_CACHE:
        return _MANIFEST_CACHE[cache_key]
    text = path_read_text(yaml_path)
    manifest = _parse_manifest_from_text(text, source_path=yaml_path)
    if cache:
        _MANIFEST_CACHE[cache_key] = manifest
    return manifest


def load_connector_manifest_from_json(
    json_path: str,
    *,
    cache: bool = True,
) -> ConnectorManifest:
    cache_key = f"json:{json_path}"
    if cache and cache_key in _MANIFEST_CACHE:
        return _MANIFEST_CACHE[cache_key]
    text = path_read_text(json_path)
    manifest = _parse_manifest_from_text(text, source_path=json_path)
    if cache:
        _MANIFEST_CACHE[cache_key] = manifest
    return manifest


def apply_connector_preset_defaults(
    resolved_config: ResolvedEntityConfig,
    manifest: ConnectorManifest,
    *,
    preset_name_override: str | None = None,
) -> ResolvedEntityConfig:
    preset_name = preset_name_override or resolved_config.settings.get(
        "connector_preset"
    )
    if not preset_name:
        return resolved_config
    preset = manifest.preset_by_name(preset_name)
    if preset is None:
        raise ConfigValidationError(
            message=(
                f"Connector preset {preset_name!r} not found in manifest.  "
                f"Available presets: "
                f"{sorted(p.name for p in manifest.presets)}."
            ),
            context={
                "preset_name": preset_name,
                "available_presets": sorted(p.name for p in manifest.presets),
            },
        )
    if preset.family.value != resolved_config.connector_type:
        raise ConfigValidationError(
            message=(
                f"Connector preset {preset_name!r} is for family "
                f"{preset.family.value!r} but entity uses connector_type "
                f"{resolved_config.connector_type!r}."
            ),
            context={
                "preset_name": preset_name,
                "preset_family": preset.family.value,
                "entity_connector_type": resolved_config.connector_type,
            },
        )
    new_extraction = dict(preset.extraction_defaults)
    new_extraction.update(resolved_config.extraction)
    new_auth: dict[str, Any] | None
    if preset.auth_defaults is not None:
        new_auth = dict(preset.auth_defaults)
        new_auth.update(resolved_config.auth or {})
    else:
        new_auth = resolved_config.auth
    new_settings = dict(preset.settings_defaults)
    new_settings.update(resolved_config.settings)
    new_persistence = dict(preset.persistence_defaults)
    new_persistence.update(resolved_config.persistence)
    return ResolvedEntityConfig(
        schema_version=resolved_config.schema_version,
        environment=resolved_config.environment,
        source_name=resolved_config.source_name,
        entity_name=resolved_config.entity_name,
        connector_type=resolved_config.connector_type,
        trigger_mode=resolved_config.trigger_mode,
        extraction=new_extraction,
        auth=new_auth,
        settings=new_settings,
        persistence=new_persistence,
        state=resolved_config.state,
        raw=resolved_config.raw,
    )

"""Tests for BACKLOG item M-1: Connector family registry.

Mirroring the G-5 secrets test_secrets.py pattern (registry contract, Protocol
duck-typing, 4 built-in factories, manifest loading, preset application).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    ConnectorFactory,
    ConnectorFamily,
    ConnectorFamilyUnsupportedError,
    ConnectorManifest,
    ConnectorPreset,
    ConnectorRegistryError,
    apply_connector_preset_defaults,
    get_connector_factory,
    is_connector_factory_registered,
    load_connector_manifest_from_json,
    load_connector_manifest_from_yaml,
    register_connector_factory,
)
from elt_pipeline.ingest.connectors.registry import (
    _CONNECTOR_REGISTRY,
    _ensure_default_connectors_registered,
)
from elt_pipeline.ingest.connectors.kafka import KafkaConnectorConfig
from elt_pipeline.ingest.connectors.local_kafka import LocalKafkaConnector
from elt_pipeline.ingest.connectors.local_object_storage import (
    LocalObjectStorageConnector,
)
from elt_pipeline.ingest.connectors.local_rest import LocalRestConnector
from elt_pipeline.ingest.connectors.local_sql import LocalSqlConnector
from elt_pipeline.ingest.connectors.object_storage import (
    ObjectStorageConnectorConfig,
)
from elt_pipeline.ingest.connectors.rest import RestConnectorConfig
from elt_pipeline.ingest.connectors.sql import SqlConnectorConfig
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_resolved_rest() -> ResolvedEntityConfig:
    return ResolvedEntityConfig(
        schema_version="1.0",
        environment="test",
        source_name="orders_api",
        entity_name="orders",
        connector_type="rest",
        trigger_mode="scheduled",
        auth={"type": "bearer", "token": "tok-42"},
        extraction={
            "url": "https://api.example.com/orders",
            "method": "GET",
            "headers": {"Accept": "application/json"},
            "response_path": "data",
            "pagination": {"mode": "none"},
        },
        settings={"request_timeout_seconds": 30},
        persistence={},
        state={},
        raw={},
    )


@pytest.fixture
def sample_resolved_sql() -> ResolvedEntityConfig:
    return ResolvedEntityConfig(
        schema_version="1.0",
        environment="test",
        source_name="analytics_db",
        entity_name="transactions",
        connector_type="sql",
        trigger_mode="scheduled",
        auth={
            "driver": "sqlite",
            "username": "sa",
            "password": "",
        },
        extraction={
            "connection_string": "sqlite:///:memory:",
            "extraction_mode": "full",
            "query_template": "SELECT 1",
            "watermark": {"mode": "none"},
        },
        settings={},
        persistence={},
        state={},
        raw={},
    )


@pytest.fixture
def sample_resolved_kafka() -> ResolvedEntityConfig:
    return ResolvedEntityConfig(
        schema_version="1.0",
        environment="test",
        source_name="events_bus",
        entity_name="click_events",
        connector_type="kafka",
        trigger_mode="scheduled",
        auth={
            "security_protocol": "PLAINTEXT",
        },
        extraction={
            "bootstrap_servers": "localhost:9092",
            "topic": "clicks",
            "consumer_group_id": "elt-test",
            "starting_position": {"type": "latest"},
            "poll_timeout_seconds": 5,
            "max_records_per_batch": 100,
        },
        settings={},
        persistence={},
        state={},
        raw={},
    )


@pytest.fixture
def sample_resolved_objstore() -> ResolvedEntityConfig:
    return ResolvedEntityConfig(
        schema_version="1.0",
        environment="test",
        source_name="data_lake",
        entity_name="raw_logs",
        connector_type="object_storage",
        trigger_mode="scheduled",
        auth={
            "type": "anonymous",
        },
        extraction={
            "bucket": "my-bucket",
            "prefix": "raw/",
            "storage_scheme": "s3",
            "format": "parquet",
            "sync_mode": "full",
        },
        settings={},
        persistence={},
        state={},
        raw={},
    )


@pytest.fixture
def registry_clean() -> None:
    """Ensure the registry is in a consistent state before each test."""
    _ensure_default_connectors_registered()
    yield


# ---------------------------------------------------------------------------
# ConnectorFamily enum (explicit boundary)
# ---------------------------------------------------------------------------


class TestConnectorFamilyEnum:
    def test_explicit_boundary_four_families(self) -> None:
        members = list(ConnectorFamily)
        assert {m.value for m in members} == {
            "rest",
            "sql",
            "kafka",
            "object_storage",
        }
        assert len(members) == 4

    def test_str_enum_compatible(self) -> None:
        assert ConnectorFamily.rest == "rest"
        assert ConnectorFamily.sql.value == "sql"
        assert f"{ConnectorFamily.kafka.value}" == "kafka"

    def test_unknown_value_raises(self) -> None:
        with pytest.raises(ValueError):
            ConnectorFamily("sftp")
        with pytest.raises(ValueError):
            ConnectorFamily("webhook")


# ---------------------------------------------------------------------------
# Protocol duck-typing
# ---------------------------------------------------------------------------


class TestConnectorFactoryProtocol:
    def test_runtime_checkable_valid_impl(self) -> None:
        class ValidFactory:
            family_type = "custom"

            def build_config_from_resolved(
                self, *, resolved_config: ResolvedEntityConfig
            ) -> BaseModel:
                raise NotImplementedError

            def build_connector(
                self,
                *,
                config: BaseModel,
                run_context: Any,
                root_path: str,
                **kwargs: Any,
            ) -> Any:
                raise NotImplementedError

        assert isinstance(ValidFactory(), ConnectorFactory)

    def test_missing_family_type_rejected(self) -> None:
        class NoFamilyType:
            def build_config_from_resolved(
                self, *, resolved_config: ResolvedEntityConfig
            ) -> BaseModel:
                raise NotImplementedError

            def build_connector(
                self,
                *,
                config: BaseModel,
                run_context: Any,
                root_path: str,
                **kwargs: Any,
            ) -> Any:
                raise NotImplementedError

        assert not isinstance(NoFamilyType(), ConnectorFactory)

    def test_missing_build_config_rejected(self) -> None:
        class NoBuildConfig:
            family_type = "x"

            def build_connector(
                self,
                *,
                config: BaseModel,
                run_context: Any,
                root_path: str,
                **kwargs: Any,
            ) -> Any:
                raise NotImplementedError

        assert not isinstance(NoBuildConfig(), ConnectorFactory)

    def test_missing_build_connector_rejected(self) -> None:
        class NoBuildConnector:
            family_type = "x"

            def build_config_from_resolved(
                self, *, resolved_config: ResolvedEntityConfig
            ) -> BaseModel:
                raise NotImplementedError

        assert not isinstance(NoBuildConnector(), ConnectorFactory)


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


class TestErrorHierarchy:
    def test_registry_error_is_pipeline_error(self) -> None:
        err = ConnectorRegistryError(message="x", error_code="E1")
        assert isinstance(err, PipelineError)
        assert err.error_code == "E1"

    def test_family_unsupported_is_registry_error(self) -> None:
        err = ConnectorFamilyUnsupportedError(family="sftp")
        assert isinstance(err, ConnectorRegistryError)
        assert "sftp" in err.message
        assert "supported_families" in (err.context or {})


# ---------------------------------------------------------------------------
# Registry: lazy init + default factories
# ---------------------------------------------------------------------------


class TestDefaultRegistryRegistration:
    def test_lazy_init_registers_four_families(self, registry_clean) -> None:
        assert is_connector_factory_registered("rest")
        assert is_connector_factory_registered("sql")
        assert is_connector_factory_registered("kafka")
        assert is_connector_factory_registered("object_storage")
        assert len(_CONNECTOR_REGISTRY) == 4

    def test_lazy_init_is_idempotent(self, registry_clean) -> None:
        # Run twice, registry size stays at 4 (no duplicates, no errors)
        _ensure_default_connectors_registered()
        _ensure_default_connectors_registered()
        assert len(_CONNECTOR_REGISTRY) == 4

    def test_get_factory_returns_callable_with_family_type_attr(
        self, registry_clean
    ) -> None:
        rest_factory = get_connector_factory("rest")
        assert isinstance(rest_factory, ConnectorFactory)
        assert rest_factory.family_type == "rest"
        sql_factory = get_connector_factory(ConnectorFamily.sql)
        assert sql_factory.family_type == "sql"

    def test_get_factory_unknown_string_raises(self, registry_clean) -> None:
        with pytest.raises(ConnectorFamilyUnsupportedError, match="sftp"):
            get_connector_factory("sftp")

    def test_is_registered_negative(self, registry_clean) -> None:
        assert not is_connector_factory_registered("sftp")
        assert not is_connector_factory_registered("")
        assert not is_connector_factory_registered("REST")  # case-sensitive


# ---------------------------------------------------------------------------
# Registry: register + duplicate guard + Protocol validation
# ---------------------------------------------------------------------------


class TestRegisterAndDuplicates:
    def test_duplicate_register_raises(self, registry_clean) -> None:
        from elt_pipeline.ingest.connectors.registry import (
            _RestConnectorFactory,
        )

        with pytest.raises(ConnectorRegistryError, match="already registered"):
            register_connector_factory("rest", _RestConnectorFactory())

    def test_invalid_protocol_rejected(self, registry_clean) -> None:
        class NotAFactory:
            pass

        # Pop rest temporarily so the scheme passes
        original = _CONNECTOR_REGISTRY.pop(ConnectorFamily.rest, None)
        try:
            with pytest.raises(
                ConnectorRegistryError,
                match="expected ConnectorFactory Protocol",
            ):
                register_connector_factory(
                    "rest", NotAFactory()  # type: ignore[arg-type]
                )
        finally:
            if original is not None:
                _CONNECTOR_REGISTRY[ConnectorFamily.rest] = original

    def test_register_with_enum_key(self, registry_clean) -> None:
        from elt_pipeline.ingest.connectors.registry import (
            _KafkaConnectorFactory,
        )

        original = _CONNECTOR_REGISTRY.pop(ConnectorFamily.kafka, None)
        try:
            register_connector_factory(
                ConnectorFamily.kafka, _KafkaConnectorFactory()
            )
            assert ConnectorFamily.kafka in _CONNECTOR_REGISTRY
        finally:
            if original is not None:
                _CONNECTOR_REGISTRY[ConnectorFamily.kafka] = original


# ---------------------------------------------------------------------------
# 4 built-in factories: build_config_from_resolved + build_connector
# ---------------------------------------------------------------------------


class TestRestFactory:
    def test_build_config_delegates(self, sample_resolved_rest, registry_clean) -> None:
        from unittest.mock import patch

        factory = get_connector_factory("rest")
        sentinel_cfg = RestConnectorConfig.model_construct(
            source_name="orders_api",
            entity_name="orders",
        )
        with patch.object(
            RestConnectorConfig,
            "from_resolved_entity_config",
            return_value=sentinel_cfg,
        ) as mock_method:
            cfg = factory.build_config_from_resolved(
                resolved_config=sample_resolved_rest
            )
        mock_method.assert_called_once_with(sample_resolved_rest)
        assert cfg is sentinel_cfg

    def test_build_connector_type(self, tmp_path, registry_clean) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("rest")
        cfg = RestConnectorConfig.model_construct(
            source_name="orders_api",
            entity_name="orders",
        )
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="orders_api",
            entity_name="orders",
        )
        conn = factory.build_connector(
            config=cfg, run_context=rc, root_path=str(tmp_path)
        )
        assert isinstance(conn, LocalRestConnector)

    def test_wrong_config_type_rejected(self, tmp_path, registry_clean) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("rest")
        sql_cfg = SqlConnectorConfig.model_construct()
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="x",
            entity_name="y",
        )
        with pytest.raises(ConfigValidationError, match="RestConnectorConfig"):
            factory.build_connector(
                config=sql_cfg, run_context=rc, root_path=str(tmp_path)
            )


class TestSqlFactory:
    def test_build_config_delegates(self, sample_resolved_sql, registry_clean) -> None:
        from unittest.mock import patch

        factory = get_connector_factory("sql")
        sentinel_cfg = SqlConnectorConfig.model_construct()
        with patch.object(
            SqlConnectorConfig,
            "from_resolved_entity_config",
            return_value=sentinel_cfg,
        ) as mock_method:
            cfg = factory.build_config_from_resolved(
                resolved_config=sample_resolved_sql
            )
        mock_method.assert_called_once_with(sample_resolved_sql)
        assert cfg is sentinel_cfg
        assert isinstance(cfg, SqlConnectorConfig)

    def test_build_connector_type(self, tmp_path, registry_clean) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("sql")
        cfg = SqlConnectorConfig.model_construct()
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="analytics_db",
            entity_name="transactions",
        )
        conn = factory.build_connector(
            config=cfg, run_context=rc, root_path=str(tmp_path)
        )
        assert isinstance(conn, LocalSqlConnector)


class TestObjectStorageFactory:
    def test_build_config_delegates(
        self, sample_resolved_objstore, registry_clean
    ) -> None:
        from unittest.mock import patch

        factory = get_connector_factory("object_storage")
        sentinel_cfg = ObjectStorageConnectorConfig.model_construct()
        with patch.object(
            ObjectStorageConnectorConfig,
            "from_resolved_entity_config",
            return_value=sentinel_cfg,
        ) as mock_method:
            cfg = factory.build_config_from_resolved(
                resolved_config=sample_resolved_objstore
            )
        mock_method.assert_called_once_with(sample_resolved_objstore)
        assert isinstance(cfg, ObjectStorageConnectorConfig)

    def test_build_connector_type(self, tmp_path, registry_clean) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("object_storage")
        cfg = ObjectStorageConnectorConfig.model_construct()
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="data_lake",
            entity_name="raw_logs",
        )
        conn = factory.build_connector(
            config=cfg, run_context=rc, root_path=str(tmp_path)
        )
        assert isinstance(conn, LocalObjectStorageConnector)


class TestKafkaFactory:
    def test_build_config_delegates(
        self, sample_resolved_kafka, registry_clean
    ) -> None:
        from unittest.mock import patch

        factory = get_connector_factory("kafka")
        sentinel_cfg = KafkaConnectorConfig.model_construct(
            source_name="events_bus",
            entity_name="click_events",
        )
        with patch.object(
            KafkaConnectorConfig,
            "from_resolved_entity_config",
            return_value=sentinel_cfg,
        ) as mock_method:
            cfg = factory.build_config_from_resolved(
                resolved_config=sample_resolved_kafka
            )
        mock_method.assert_called_once_with(sample_resolved_kafka)
        assert isinstance(cfg, KafkaConnectorConfig)

    def test_log_path_required(self, tmp_path, registry_clean) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("kafka")
        cfg = KafkaConnectorConfig.model_construct(
            source_name="events_bus",
            entity_name="click_events",
        )
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="events_bus",
            entity_name="click_events",
        )
        with pytest.raises(ConfigValidationError, match="log_path"):
            factory.build_connector(
                config=cfg, run_context=rc, root_path=str(tmp_path)
            )

    def test_build_connector_with_log_path(
        self, tmp_path, registry_clean
    ) -> None:
        from elt_pipeline.shared.runtime import new_run_context

        factory = get_connector_factory("kafka")
        cfg = KafkaConnectorConfig.model_construct(
            source_name="events_bus",
            entity_name="click_events",
        )
        rc = new_run_context(
            stage="ingest",
            job_name="test",
            environment="test",
            source_name="events_bus",
            entity_name="click_events",
        )
        log_path = tmp_path / "kafka.log"
        log_path.touch()
        conn = factory.build_connector(
            config=cfg,
            run_context=rc,
            root_path=str(tmp_path),
            log_path=str(log_path),
        )
        assert isinstance(conn, LocalKafkaConnector)


# ---------------------------------------------------------------------------
# ConnectorManifest + ConnectorPreset models
# ---------------------------------------------------------------------------


class TestManifestModels:
    def test_preset_model_defaults(self) -> None:
        p = ConnectorPreset(
            name="github_api_v3",
            family=ConnectorFamily.rest,
        )
        assert p.description is None
        assert p.extraction_defaults == {}
        assert p.auth_defaults is None
        assert p.settings_defaults == {}
        assert p.persistence_defaults == {}

    def test_manifest_model_preset_lookup(self) -> None:
        m = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="p1",
                    family=ConnectorFamily.rest,
                    extraction_defaults={"url": "https://a.com"},
                ),
                ConnectorPreset(
                    name="p2",
                    family=ConnectorFamily.sql,
                    extraction_defaults={
                        "connection_string": "sqlite:///:memory:"
                    },
                ),
            ]
        )
        p1 = m.preset_by_name("p1")
        assert p1 is not None and p1.name == "p1"
        p2 = m.preset_by_name("p2")
        assert p2 is not None and p2.family is ConnectorFamily.sql
        assert m.preset_by_name("missing") is None

    def test_manifest_default_schema_version(self) -> None:
        m = ConnectorManifest(presets=[])
        assert m.schema_version == "1.0"

    def test_invalid_family_in_preset_fails(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ConnectorPreset(
                name="x", family="not-a-family"  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# Manifest loading: YAML / JSON + caching
# ---------------------------------------------------------------------------


SAMPLE_MANIFEST_YAML = """
schema_version: "1.0"
presets:
  - name: github_rest_v3
    family: rest
    description: GitHub REST API v3 preset
    extraction_defaults:
      url: "https://api.github.com"
      method: GET
      headers:
        Accept: "application/vnd.github+json"
        X-GitHub-Api-Version: "2022-11-28"
      response_path: "$"
      pagination:
        mode: page_number
        page_param: page
        per_page_param: per_page
        per_page: 100
    auth_defaults:
      type: bearer
      token_env_var: GITHUB_TOKEN
    settings_defaults:
      request_timeout_seconds: 45
      max_retries: 5
  - name: snowflake_oltp
    family: sql
    extraction_defaults:
      connection_string: "snowflake://account.snowflakecomputing.com"
    auth_defaults:
      driver: snowflake
  - name: s3_raw_bucket
    family: object_storage
    extraction_defaults:
      storage_scheme: s3
      format: parquet
      sync_mode: full
"""

SAMPLE_MANIFEST_JSON = json.dumps(
    {
        "schema_version": "1.0",
        "presets": [
            {
                "name": "shopify_rest",
                "family": "rest",
                "extraction_defaults": {
                    "url": "https://shop.myshopify.com/admin/api/2024-01",
                    "method": "GET",
                    "pagination": {"mode": "cursor"},
                },
            }
        ],
    }
)


class TestManifestLoading:
    def test_load_from_yaml(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "manifest.yaml"
        yaml_path.write_text(SAMPLE_MANIFEST_YAML)
        manifest = load_connector_manifest_from_yaml(str(yaml_path), cache=False)
        assert isinstance(manifest, ConnectorManifest)
        assert len(manifest.presets) == 3
        p = manifest.preset_by_name("github_rest_v3")
        assert p is not None
        assert p.family is ConnectorFamily.rest
        assert p.extraction_defaults["url"] == "https://api.github.com"

    def test_load_from_json(self, tmp_path: Path) -> None:
        json_path = tmp_path / "manifest.json"
        json_path.write_text(SAMPLE_MANIFEST_JSON)
        manifest = load_connector_manifest_from_json(str(json_path), cache=False)
        assert isinstance(manifest, ConnectorManifest)
        assert len(manifest.presets) == 1
        assert manifest.presets[0].name == "shopify_rest"

    def test_yaml_caching(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "cached.yaml"
        yaml_path.write_text(SAMPLE_MANIFEST_YAML)
        m1 = load_connector_manifest_from_yaml(str(yaml_path), cache=True)
        m2 = load_connector_manifest_from_yaml(str(yaml_path), cache=True)
        # Same instance returned (cache hit)
        assert m1 is m2
        # cache=False returns new parse each time
        m3 = load_connector_manifest_from_yaml(str(yaml_path), cache=False)
        assert m3 is not m1

    def test_json_caching(self, tmp_path: Path) -> None:
        json_path = tmp_path / "cached.json"
        json_path.write_text(SAMPLE_MANIFEST_JSON)
        m1 = load_connector_manifest_from_json(str(json_path), cache=True)
        m2 = load_connector_manifest_from_json(str(json_path), cache=True)
        assert m1 is m2

    def test_invalid_manifest_fails(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad_content = (
            "schema_version: 1.0\n"
            "presets:\n"
            "  - name: broken_preset\n"
            "    family: rest\n"
            "    extraction_defaults:\n"
            "      this_key: [unclosed_list\n"  # SYNTACTICALLY INVALID yaml: unclosed bracket
        )
        bad.write_text(bad_content)
        with pytest.raises(ConfigValidationError):
            load_connector_manifest_from_yaml(str(bad), cache=False)


# ---------------------------------------------------------------------------
# apply_connector_preset_defaults: layering + validation
# ---------------------------------------------------------------------------


class TestApplyPresetDefaults:
    def test_no_preset_passthrough(self, sample_resolved_rest) -> None:
        manifest = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="p1",
                    family=ConnectorFamily.rest,
                    extraction_defaults={"url": "https://example.com"},
                )
            ]
        )
        # No connector_preset in settings and no override → no change
        result = apply_connector_preset_defaults(sample_resolved_rest, manifest)
        assert result is sample_resolved_rest

    def test_preset_merges_under_entity(self, sample_resolved_rest) -> None:
        manifest = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="github_rest_v3",
                    family=ConnectorFamily.rest,
                    description="",
                    extraction_defaults={
                        "url": "https://preset-url.com",
                        "new_field": "from_preset",
                        "preset_only_key": "preset_value",
                    },
                    auth_defaults={
                        "type": "bearer",
                        "preset_token": "pk-xxx",
                    },
                    settings_defaults={
                        "new_setting": 99,
                        "request_timeout_seconds": 60,
                    },
                    persistence_defaults={"target_prefix": "raw/"},
                )
            ]
        )
        sample_resolved_rest.settings["connector_preset"] = "github_rest_v3"
        result = apply_connector_preset_defaults(sample_resolved_rest, manifest)

        # Top-level extraction keys: entity wins over preset
        assert result.extraction["url"] == "https://api.example.com/orders"
        # Preset values fill in missing top-level keys only (shallow merge, NOT deep)
        assert result.extraction["new_field"] == "from_preset"
        assert result.extraction["preset_only_key"] == "preset_value"
        # Entity extraction headers completely replace preset headers if entity had headers
        assert result.extraction["headers"]["Accept"] == "application/json"
        # Shallow merge means: entity headers are the whole dict (no key-level merge)
        assert "X-Preset" not in result.extraction["headers"]

        # Auth: entity token wins, preset auth keys fill in missing ones
        assert result.auth is not None
        assert result.auth["token"] == "tok-42"
        assert "preset_token" in result.auth

        # Settings: entity request_timeout wins, preset new_setting present
        assert result.settings["request_timeout_seconds"] == 30
        assert result.settings["new_setting"] == 99

        # Persistence: preset target_prefix present
        assert result.persistence["target_prefix"] == "raw/"

    def test_preset_name_override(self, sample_resolved_rest) -> None:
        manifest = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="override_me",
                    family=ConnectorFamily.rest,
                    extraction_defaults={"url": "https://from-override.com"},
                ),
                ConnectorPreset(
                    name="ignored_preset",
                    family=ConnectorFamily.rest,
                    extraction_defaults={"url": "https://ignored.com"},
                ),
            ]
        )
        sample_resolved_rest.settings["connector_preset"] = "ignored_preset"
        result = apply_connector_preset_defaults(
            sample_resolved_rest, manifest, preset_name_override="override_me"
        )
        # Override takes precedence
        assert result.extraction["url"] == "https://api.example.com/orders"
        # Preset name in settings was "ignored_preset" but override=override_me was used:
        # the entity extraction["url"] already overrode both, so check by absence
        # of "ignored" defaults; override_me has no extra fields, so extraction
        # is identical after layering. More importantly, no error was raised.
        assert "connector_preset" in result.settings

    def test_unknown_preset_raises(self, sample_resolved_rest) -> None:
        manifest = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="exists", family=ConnectorFamily.rest,
                    extraction_defaults={},
                )
            ]
        )
        sample_resolved_rest.settings["connector_preset"] = "does_not_exist"
        with pytest.raises(ConfigValidationError, match="not found"):
            apply_connector_preset_defaults(sample_resolved_rest, manifest)

    def test_family_mismatch_raises(self, sample_resolved_rest) -> None:
        manifest = ConnectorManifest(
            presets=[
                ConnectorPreset(
                    name="sql_preset",
                    family=ConnectorFamily.sql,  # wrong family!
                    extraction_defaults={},
                )
            ]
        )
        sample_resolved_rest.settings["connector_preset"] = "sql_preset"
        with pytest.raises(ConfigValidationError, match="family"):
            apply_connector_preset_defaults(sample_resolved_rest, manifest)


# ---------------------------------------------------------------------------
# Env var name centralization (M-1 requirement)
# ---------------------------------------------------------------------------


class TestEnvVarNames:
    def test_centralized_in_runtime_manifest(self) -> None:
        from elt_pipeline.config.runtime_manifest import runtime_manifest

        env = runtime_manifest.env
        assert hasattr(env, "connector_registry_manifest")
        assert hasattr(env, "connector_registry_strict")
        # Values follow the ELT_PIPELINE_* naming convention
        assert env.connector_registry_manifest.startswith("ELT_PIPELINE_")
        assert env.connector_registry_strict.startswith("ELT_PIPELINE_")


# ---------------------------------------------------------------------------
# Smoke: package-level re-export chain (connectors → ingest → top-level)
# ---------------------------------------------------------------------------


class TestPackageExports:
    def test_connectors_init_exports(self) -> None:
        import elt_pipeline.ingest.connectors as connectors

        expected = [
            "ConnectorFamily",
            "ConnectorFactory",
            "ConnectorManifest",
            "ConnectorPreset",
            "ConnectorRegistryError",
            "ConnectorFamilyUnsupportedError",
            "apply_connector_preset_defaults",
            "get_connector_factory",
            "is_connector_factory_registered",
            "load_connector_manifest_from_json",
            "load_connector_manifest_from_yaml",
            "register_connector_factory",
        ]
        for name in expected:
            assert hasattr(connectors, name), (
                f"connectors package missing export: {name}"
            )

    def test_ingest_init_exports(self) -> None:
        import elt_pipeline.ingest as ingest

        expected = [
            "ConnectorFamily",
            "ConnectorFactory",
            "ConnectorManifest",
            "ConnectorPreset",
            "apply_connector_preset_defaults",
            "get_connector_factory",
            "register_connector_factory",
        ]
        for name in expected:
            assert hasattr(ingest, name), (
                f"ingest package missing export: {name}"
            )

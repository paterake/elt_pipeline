from pathlib import Path
from textwrap import dedent

import pytest

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config
from elt_pipeline.shared.errors import ConfigValidationError


def write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        dedent(
            """
        schema_version: v1
        defaults:
          region: global
          extraction:
            page_size: 50
        environments:
          default:
            defaults:
              runtime_owner: data-platform
          dev:
            defaults:
              region: local
        sources:
          - name: rest_source
            connector_type: rest
            trigger_mode: scheduled
            defaults:
              extraction:
                page_size: 100
            auth:
              strategy: api_key
            entities:
              - name: orders
                extraction:
                  endpoint: /orders
                settings:
                  batch_size: 500
            """
        ).strip(),
        encoding="utf-8",
    )
    return config_path


def test_load_pipeline_config(tmp_path: Path) -> None:
    config = load_pipeline_config(write_config(tmp_path))
    assert config.schema_version == "v1"
    assert config.sources[0].connector_type == "rest"


def test_resolve_entity_config_applies_layering(tmp_path: Path) -> None:
    config = load_pipeline_config(write_config(tmp_path))
    resolved = resolve_entity_config(
        config,
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
    )
    assert resolved.environment == "dev"
    assert resolved.connector_type == "rest"
    assert resolved.auth["strategy"] == "api_key"
    assert resolved.extraction["endpoint"] == "/orders"
    assert resolved.level2_mode == "required_level2"
    assert resolved.resolved_defaults["region"] == "local"
    assert resolved.resolved_defaults["runtime_owner"] == "data-platform"
    assert resolved.settings["batch_size"] == 500


def test_resolve_entity_config_supports_explicit_level2_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        dedent(
            """
        schema_version: v1
        environments:
          default:
            defaults: {}
          dev:
            defaults: {}
        sources:
          - name: rest_source
            connector_type: rest
            level2_mode: lightweight_level2
            entities:
              - name: orders
                level2_mode: bypass_level2
            """
        ).strip(),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)
    resolved = resolve_entity_config(
        config,
        environment="dev",
        source_name="rest_source",
        entity_name="orders",
    )

    assert resolved.level2_mode == "bypass_level2"


def test_resolve_entity_config_rejects_unknown_environment(tmp_path: Path) -> None:
    config = load_pipeline_config(write_config(tmp_path))
    with pytest.raises(ConfigValidationError):
        resolve_entity_config(
            config,
            environment="qa",
            source_name="rest_source",
            entity_name="orders",
        )

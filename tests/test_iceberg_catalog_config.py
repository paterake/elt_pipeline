from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from elt_pipeline.cli import (
    _build_serving_endpoint,
    _resolve_iceberg_session_kwargs,
    _validate_iceberg_catalog_binding,
    build_parser,
)
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.spark.session import build_spark_session


class TestSessionBuilderCatalogValidation:
    """Validation paths raise ValueError BEFORE getOrCreate(), so no JVM needed."""

    def test_unknown_catalog_type_raises_before_jvm(self):
        with pytest.raises(ValueError, match="Unsupported ELT_PIPELINE_ICEBERG_CATALOG_TYPE=bogus"):
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="bogus",
                )

    def test_rest_requires_catalog_uri(self):
        rest_match = "iceberg_catalog_type=rest requires iceberg_catalog_uri"
        with pytest.raises(ValueError, match=rest_match):
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="rest",
                )

    def test_jdbc_requires_catalog_uri(self):
        jdbc_match = "iceberg_catalog_type=jdbc requires iceberg_catalog_uri"
        with pytest.raises(ValueError, match=jdbc_match):
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="jdbc",
                )

    def test_hadoop_passes_validation_no_uri_required(self):
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="hadoop",
                )
        err_text = str(exc_info.value).lower()
        assert "valueerror" not in str(type(exc_info.value)).lower() or "uri" not in err_text
        jvm_tokens = ("java", "jvm", "spark", "getorcreate")
        assert any(tok in err_text for tok in jvm_tokens)

    def test_glue_passes_validation_no_uri_required(self):
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="glue",
                )
        err_text = str(exc_info.value).lower()
        assert "valueerror" not in str(type(exc_info.value)).lower() or "uri" not in err_text

    def test_rest_with_uri_passes_validation(self):
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="rest",
                    iceberg_catalog_uri="http://localhost:8181/api/v1",
                )
        err_text = str(exc_info.value).lower()
        assert "valueerror" not in str(type(exc_info.value)).lower() or "uri" not in err_text

    def test_jdbc_with_uri_passes_validation(self):
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="jdbc",
                    iceberg_catalog_uri="jdbc:h2:file:/tmp/meta",
                )
        err_text = str(exc_info.value).lower()
        assert "valueerror" not in str(type(exc_info.value)).lower() or "uri" not in err_text


class TestCliCatalogValidation:
    """CLI pre-build validation (no Spark/PySpark import required at all)."""

    @staticmethod
    def _args(**overrides):
        base = {
            "iceberg_catalog_type": "hadoop",
            "iceberg_catalog_uri": None,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_validate_rejects_unknown_type(self):
        with pytest.raises(
            PipelineError,
            match="Unsupported Iceberg catalog binding type",
        ):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type="bogus")
                )

    def test_validate_accepts_all_four_types_when_prereqs_met(self):
        with patch.dict(os.environ, {}, clear=True):
            for catalog_type, uri in [
                ("hadoop", None),
                ("jdbc", "jdbc:h2:file:/tmp/meta"),
                ("rest", "http://nessie:19120/api/v1"),
                ("glue", None),
            ]:
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type=catalog_type, iceberg_catalog_uri=uri)
                )

    def test_validate_requires_uri_for_jdbc(self):
        with pytest.raises(PipelineError, match="requires --iceberg-catalog-uri"):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type="jdbc", iceberg_catalog_uri=None)
                )

    def test_validate_requires_uri_for_rest(self):
        with pytest.raises(PipelineError, match="requires --iceberg-catalog-uri"):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type="rest", iceberg_catalog_uri=None)
                )

    def test_hadoop_and_glue_ok_without_uri(self):
        with patch.dict(os.environ, {}, clear=True):
            for ct in ("hadoop", "glue"):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type=ct, iceberg_catalog_uri=None)
                )


class TestCliSessionKwargsResolver:
    """Threading of CLI args → session kwargs dict (env + argparse)."""

    @staticmethod
    def _args(**overrides):
        base = {
            "iceberg_enabled": None,
            "iceberg_catalog_name": None,
            "iceberg_catalog_type": None,
            "iceberg_catalog_uri": None,
            "iceberg_rest_token": None,
            "iceberg_rest_warehouse": None,
            "iceberg_glue_region": None,
            "iceberg_warehouse_dir": None,
            "warehouse_root": None,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_rest_token_resolved_from_arg(self):
        args = self._args(
            iceberg_enabled=True,
            iceberg_catalog_type="rest",
            iceberg_catalog_uri="http://p:8181",
            iceberg_rest_token="tkn-arg",
        )
        kwargs = _resolve_iceberg_session_kwargs(args=args, app_name="x")
        assert kwargs["iceberg_rest_token"] == "tkn-arg"

    def test_rest_warehouse_resolved_from_arg(self):
        args = self._args(
            iceberg_enabled=True,
            iceberg_catalog_type="rest",
            iceberg_catalog_uri="http://p:8181",
            iceberg_rest_warehouse="analytics",
        )
        kwargs = _resolve_iceberg_session_kwargs(args=args, app_name="x")
        assert kwargs["iceberg_rest_warehouse"] == "analytics"

    def test_glue_region_resolved_from_arg(self):
        args = self._args(
            iceberg_enabled=True,
            iceberg_catalog_type="glue",
            iceberg_glue_region="us-west-2",
        )
        kwargs = _resolve_iceberg_session_kwargs(args=args, app_name="x")
        assert kwargs["iceberg_glue_region"] == "us-west-2"

    def test_env_vars_as_fallback_for_new_flags(self, monkeypatch):
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_REST_TOKEN", "tkn-env")
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_REST_WAREHOUSE", "wh-env")
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_GLUE_REGION", "eu-central-1")
        args = self._args(
            iceberg_enabled=True,
            iceberg_catalog_type="rest",
            iceberg_catalog_uri="http://p:8181",
        )
        kwargs = _resolve_iceberg_session_kwargs(args=args, app_name="x")
        assert kwargs["iceberg_rest_token"] == "tkn-env"
        assert kwargs["iceberg_rest_warehouse"] == "wh-env"
        glue_kwargs = _resolve_iceberg_session_kwargs(
            args=self._args(iceberg_enabled=True, iceberg_catalog_type="glue"),
            app_name="y",
        )
        assert glue_kwargs["iceberg_glue_region"] == "eu-central-1"

    def test_arg_precedence_over_env(self, monkeypatch):
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_REST_TOKEN", "tkn-env")
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_REST_WAREHOUSE", "wh-env")
        monkeypatch.setenv("ELT_PIPELINE_ICEBERG_GLUE_REGION", "region-env")
        args = self._args(
            iceberg_enabled=True,
            iceberg_catalog_type="rest",
            iceberg_catalog_uri="http://p:8181",
            iceberg_rest_token="tkn-arg",
            iceberg_rest_warehouse="wh-arg",
        )
        kwargs = _resolve_iceberg_session_kwargs(args=args, app_name="x")
        assert kwargs["iceberg_rest_token"] == "tkn-arg"
        assert kwargs["iceberg_rest_warehouse"] == "wh-arg"
        glue_kwargs = _resolve_iceberg_session_kwargs(
            args=self._args(
                iceberg_enabled=True,
                iceberg_catalog_type="glue",
                iceberg_glue_region="region-arg",
            ),
            app_name="y",
        )
        assert glue_kwargs["iceberg_glue_region"] == "region-arg"


class TestCliArgparseChoices:
    """Ensure argparse exposes the 4-way catalog type choice set."""

    def test_sql_run_catalog_type_choices_includes_rest_glue(self):
        parser = build_parser()
        ns = parser.parse_args(
            [
                "sql",
                "run",
                "pkg",
                "--iceberg-enabled",
                "--iceberg-catalog-type",
                "rest",
            ]
        )
        assert ns.iceberg_catalog_type == "rest"
        ns = parser.parse_args(
            [
                "sql",
                "run",
                "pkg",
                "--iceberg-enabled",
                "--iceberg-catalog-type",
                "glue",
            ]
        )
        assert ns.iceberg_catalog_type == "glue"

    def test_new_cli_flags_exist_and_roundtrip(self):
        parser = build_parser()
        ns = parser.parse_args(
            [
                "sql",
                "run",
                "pkg",
                "--iceberg-enabled",
                "--iceberg-catalog-type",
                "rest",
                "--iceberg-catalog-uri",
                "http://p:8181",
                "--iceberg-rest-token",
                "mytoken",
                "--iceberg-rest-warehouse",
                "my-wh",
                "--iceberg-glue-region",
                "us-east-1",
            ]
        )
        assert ns.iceberg_rest_token == "mytoken"
        assert ns.iceberg_rest_warehouse == "my-wh"
        assert ns.iceberg_glue_region == "us-east-1"
        assert ns.iceberg_catalog_uri == "http://p:8181"


class TestServingEndpointShape:
    """Ensure serving_endpoint reports the new catalog-type fields."""

    @staticmethod
    def _args(**overrides):
        base = {
            "iceberg_enabled": True,
            "iceberg_catalog_name": "iceberg",
            "iceberg_catalog_type": "hadoop",
            "iceberg_catalog_uri": None,
            "iceberg_rest_token": None,
            "iceberg_rest_warehouse": None,
            "iceberg_glue_region": None,
            "iceberg_warehouse_dir": None,
            "warehouse_root": None,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_hadoop_shape(self):
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="hadoop",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert endpoint["catalog_type_note"].startswith("Filesystem-based catalog")
        assert endpoint["catalog_uri_provided"] is False
        assert endpoint["glue_region_provided"] is False
        assert "trino_iceberg_catalog_note" in endpoint["engines"]["trino"]
        assert (
            endpoint["engines"]["trino"]["sample_query"].startswith(
                "SELECT * FROM iceberg.level3."
            )
        )

    def test_jdbc_shape(self):
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="jdbc",
                iceberg_catalog_uri="jdbc:h2:file:/tmp/m",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert "JDBC-backed" in endpoint["catalog_type_note"]
        assert endpoint["catalog_uri_provided"] is True

    def test_rest_shape(self):
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="rest",
                iceberg_catalog_uri="http://p:8181/api/v1",
                iceberg_rest_token="tkn",
                iceberg_rest_warehouse="analytics",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert "REST catalog" in endpoint["catalog_type_note"]
        assert endpoint["catalog_uri_provided"] is True
        assert "jdbc:trino:" in endpoint["engines"]["trino"]["jdbc_url"]

    def test_glue_shape(self):
        with patch.dict(
            os.environ,
            {"ELT_PIPELINE_TRINO_PORT": "8080", "ELT_PIPELINE_ICEBERG_GLUE_REGION": ""},
            clear=False,
        ):
            args = self._args(
                iceberg_catalog_type="glue",
                iceberg_glue_region="ap-south-1",
                iceberg_warehouse_dir="s3://b/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert "AWS Glue" in endpoint["catalog_type_note"]
        assert endpoint["glue_region_provided"] is True

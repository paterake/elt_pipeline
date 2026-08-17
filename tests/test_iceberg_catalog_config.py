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
from elt_pipeline.config import runtime_context
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.spark.session import build_spark_session


class TestSessionBuilderCatalogValidation:
    """Validation paths raise ValueError BEFORE getOrCreate(), so no JVM needed."""

    def test_unknown_catalog_type_raises_before_jvm(self):
        with pytest.raises(ValueError, match=r"Unsupported iceberg_writer\.catalog_type=bogus"):
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="bogus",
                )

    def test_rest_requires_catalog_uri(self):
        rest_match = r"catalog_type=rest requires iceberg_catalog_uri"
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
        jdbc_match = r"catalog_type=jdbc requires iceberg_catalog_uri"
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

    def test_hive_metastore_rejects_when_uri_missing(self):
        hive_match = "catalog_type=hive_metastore requires iceberg_hive_metastore_uri"
        with pytest.raises(ValueError, match=hive_match):
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="hive_metastore",
                )

    def test_hive_metastore_accepts_when_uri_provided(self):
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                build_spark_session(
                    app_name="test",
                    iceberg_enabled=True,
                    iceberg_warehouse_dir="/tmp/wh",
                    iceberg_catalog_name="iceberg",
                    iceberg_catalog_type="hive_metastore",
                    iceberg_hive_metastore_uri="thrift://localhost:9083",
                )
        err_text = str(exc_info.value).lower()
        assert "valueerror" not in str(type(exc_info.value)).lower() or (
            "uri" not in err_text and "hive_metastore" not in err_text
        )
        jvm_tokens = ("java", "jvm", "spark", "getorcreate")
        assert any(tok in err_text for tok in jvm_tokens)


class TestCliCatalogValidation:
    """CLI pre-build validation (no Spark/PySpark import required at all)."""

    @staticmethod
    def _args(**overrides):
        base = {
            "iceberg_catalog_type": "hadoop",
            "iceberg_catalog_uri": None,
            "iceberg_hive_metastore_uri": None,
            "iceberg_catalog_impl_override": None,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_validate_rejects_unknown_type(self):
        with pytest.raises(
            PipelineError,
            match="Unsupported Iceberg WRITER catalog binding type",
        ):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type="bogus")
                )

    def test_validate_accepts_all_six_writer_types_when_prereqs_met(self):
        with patch.dict(os.environ, {}, clear=True):
            for catalog_type, uri, hive_uri in [
                ("hadoop", None, None),
                ("jdbc", "jdbc:h2:file:/tmp/meta", None),
                ("rest", "http://nessie:19120/api/v1", None),
                ("nessie", "http://nessie:19120/api/v1", None),
                ("glue", None, None),
                ("hive_metastore", None, "thrift://metastore:9083"),
            ]:
                _validate_iceberg_catalog_binding(
                    self._args(
                        iceberg_catalog_type=catalog_type,
                        iceberg_catalog_uri=uri,
                        iceberg_hive_metastore_uri=hive_uri,
                    )
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

    def test_validate_requires_uri_for_nessie_alias_same_as_rest(self):
        with pytest.raises(PipelineError, match="requires --iceberg-catalog-uri"):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(
                        iceberg_catalog_type="nessie", iceberg_catalog_uri=None
                    )
                )

    def test_hadoop_and_glue_ok_without_uri(self):
        with patch.dict(os.environ, {}, clear=True):
            for ct in ("hadoop", "glue"):
                _validate_iceberg_catalog_binding(
                    self._args(iceberg_catalog_type=ct, iceberg_catalog_uri=None)
                )

    def test_hive_metastore_serving_accepts_or_equivalent_alias(self):
        with patch.dict(os.environ, {}, clear=True):
            _validate_iceberg_catalog_binding(
                self._args(
                    iceberg_catalog_type="hive_metastore",
                    iceberg_hive_metastore_uri="thrift://metastore:9083",
                )
            )
        with pytest.raises(
            PipelineError,
            match="requires --iceberg-hive-metastore-uri",
        ):
            with patch.dict(os.environ, {}, clear=True):
                _validate_iceberg_catalog_binding(
                    self._args(
                        iceberg_catalog_type="hive_metastore",
                        iceberg_hive_metastore_uri=None,
                    )
                )
        parser = build_parser()
        ns = parser.parse_args([
            "sql", "run", "pkg",
            "--iceberg-enabled",
            "--iceberg-catalog-type", "hive_metastore",
            "--iceberg-hive-metastore-uri", "thrift://localhost:9083",
        ])
        assert ns.iceberg_catalog_type == "hive_metastore"
        assert ns.iceberg_hive_metastore_uri == "thrift://localhost:9083"


class TestCliSessionKwargsResolver:
    """Threading of CLI args → session kwargs dict (env + argparse)."""

    @staticmethod
    def setup_method(method):
        runtime_context._reset_for_tests()

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
            "iceberg_hive_metastore_uri": None,
            "iceberg_catalog_impl_override": None,
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
        runtime_context._reset_for_tests()
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
        runtime_context._reset_for_tests()
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
    """Ensure argparse exposes the 6-way catalog type choice set."""

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

    def test_sql_run_catalog_type_choices_includes_nessie_alias(self):
        parser = build_parser()
        ns_sql = parser.parse_args(
            [
                "sql",
                "run",
                "pkg",
                "--iceberg-enabled",
                "--iceberg-catalog-type",
                "nessie",
            ]
        )
        assert ns_sql.iceberg_catalog_type == "nessie"
        ns_publish = parser.parse_args(
            [
                "publish",
                "run",
                "pkg",
                "--iceberg-enabled",
                "--iceberg-catalog-type",
                "nessie",
            ]
        )
        assert ns_publish.iceberg_catalog_type == "nessie"

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
    def setup_method(method):
        runtime_context._reset_for_tests()

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
            "iceberg_hive_metastore_uri": None,
            "iceberg_catalog_impl_override": None,
            "iceberg_warehouse_dir": None,
            "warehouse_root": None,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_impl_override_shape_provided(self):
        runtime_context._reset_for_tests()
        ro = {
            "iceberg_writer": {
                "catalog_impl_override": "org.apache.gravitino.iceberg.spark.SparkCatalog",
            }
        }
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="rest",
                iceberg_catalog_uri="http://p:8181/api/v1",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args, runtime_overrides=ro)
        assert endpoint is not None
        assert endpoint["catalog_impl_override_provided"] is True
        assert (
            endpoint["catalog_impl_override_class"]
            == "org.apache.gravitino.iceberg.spark.SparkCatalog"
        )
        assert "Gravitino example" in endpoint["catalog_impl_override_note"]
        assert "Custom Iceberg SparkCatalog" in endpoint["catalog_impl_override_note"]
        assert "class override" in endpoint["catalog_impl_override_note"]

    def test_impl_override_shape_default(self):
        runtime_context._reset_for_tests()
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="hadoop",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert endpoint["catalog_impl_override_provided"] is False
        assert endpoint["catalog_impl_override_class"] == ""
        assert "No catalog_impl_override in effect" in endpoint["catalog_impl_override_note"]
        assert "org.apache.iceberg.spark" in endpoint["catalog_impl_override_note"]

    def test_hadoop_shape(self):
        with patch.dict(os.environ, {"ELT_PIPELINE_TRINO_PORT": "8080"}, clear=False):
            args = self._args(
                iceberg_catalog_type="hadoop",
                iceberg_warehouse_dir="/tmp/wh",
            )
            endpoint = _build_serving_endpoint(args)
        assert endpoint is not None
        assert endpoint["writer_catalog_type"] == "hadoop"
        assert endpoint["serving_catalog_type"] == "jdbc"
        assert "JDBC-backed" in endpoint["catalog_type_note"]
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
        assert endpoint["writer_catalog_type"] == "jdbc"
        assert endpoint["serving_catalog_type"] == "jdbc"
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
        assert endpoint["writer_catalog_type"] == "rest"
        assert endpoint["serving_catalog_type"] == "jdbc"
        assert "JDBC-backed" in endpoint["catalog_type_note"]
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
        assert endpoint["writer_catalog_type"] == "glue"
        assert endpoint["serving_catalog_type"] == "jdbc"
        assert "JDBC-backed" in endpoint["catalog_type_note"]
        assert endpoint["glue_region_provided"] is True


class TestCatalogImplOverrideSession:
    """Verify catalog_impl_override replaces BOTH catalog class strings in session builder."""

    @staticmethod
    def _capture_config_calls(monkeypatch, build_fn):
        calls: list[tuple[str, str]] = []
        from pyspark.sql import SparkSession as _SS

        original_config = _SS.Builder.config

        def fake_config(self, key, value=None, *args, **kwargs):
            if value is None and isinstance(key, dict):
                for k, v in key.items():
                    calls.append((k, str(v)))
            elif value is not None:
                calls.append((key, str(value)))
            return original_config(self, key, value, *args, **kwargs)

        monkeypatch.setattr(_SS.Builder, "config", fake_config)
        try:
            build_fn()
        except Exception:
            pass
        return dict(calls)

    def test_catalog_impl_override_applied_to_both_catalogs(self, monkeypatch):
        runtime_context._reset_for_tests()
        custom_class = "org.apache.gravitino.iceberg.spark.SparkCatalog"
        configs = self._capture_config_calls(
            monkeypatch,
            lambda: build_spark_session(
                app_name="test",
                iceberg_enabled=True,
                iceberg_warehouse_dir="/tmp/wh",
                iceberg_catalog_name="iceberg",
                iceberg_catalog_type="hadoop",
                iceberg_catalog_impl_override=custom_class,
            ),
        )
        assert configs["spark.sql.catalog.spark_catalog"] == custom_class
        assert configs["spark.sql.catalog.iceberg"] == custom_class
        assert (
            configs.get("spark.sql.catalog.spark_catalog.type", "") == "hadoop"
            or configs.get("spark.sql.catalog.iceberg.type", "") == "hadoop"
        )

    def test_catalog_impl_override_default_unchanged(self, monkeypatch):
        runtime_context._reset_for_tests()
        configs = self._capture_config_calls(
            monkeypatch,
            lambda: build_spark_session(
                app_name="test",
                iceberg_enabled=True,
                iceberg_warehouse_dir="/tmp/wh",
                iceberg_catalog_name="iceberg",
                iceberg_catalog_type="hadoop",
            ),
        )
        default_session = "org.apache.iceberg.spark.SparkSessionCatalog"
        default_leaf = "org.apache.iceberg.spark.SparkCatalog"
        custom_class = "org.apache.gravitino.iceberg.spark.SparkCatalog"
        assert configs["spark.sql.catalog.spark_catalog"] == default_session
        assert configs["spark.sql.catalog.iceberg"] == default_leaf
        assert custom_class not in configs.get("spark.sql.catalog.spark_catalog", "")
        assert custom_class not in configs.get("spark.sql.catalog.iceberg", "")


class TestNessieWriterAlias:
    """Verify catalog_type=nessie WRITER alias dispatches identically to rest."""

    @staticmethod
    def _capture_config_calls(monkeypatch, build_fn):
        calls: list[tuple[str, str]] = []
        from pyspark.sql import SparkSession as _SS

        original_config = _SS.Builder.config

        def fake_config(self, key, value=None, *args, **kwargs):
            if value is None and isinstance(key, dict):
                for k, v in key.items():
                    calls.append((k, str(v)))
            elif value is not None:
                calls.append((key, str(value)))
            return original_config(self, key, value, *args, **kwargs)

        monkeypatch.setattr(_SS.Builder, "config", fake_config)
        try:
            build_fn()
        except Exception:
            pass
        return dict(calls)

    def test_nessie_writer_alias_dispatched_as_rest_with_uri(self, monkeypatch):
        runtime_context._reset_for_tests()
        nessie_uri = "http://nessie-server.local:19120/api/v1"
        configs = self._capture_config_calls(
            monkeypatch,
            lambda: build_spark_session(
                app_name="test",
                iceberg_enabled=True,
                iceberg_warehouse_dir="/tmp/wh",
                iceberg_catalog_name="iceberg",
                iceberg_catalog_type="nessie",
                iceberg_catalog_uri=nessie_uri,
            ),
        )
        assert (
            configs.get("spark.sql.catalog.spark_catalog.type", "") == "rest"
            or configs.get("spark.sql.catalog.iceberg.type", "") == "rest"
        )
        assert (
            configs.get("spark.sql.catalog.spark_catalog.uri", "") == nessie_uri
            or configs.get("spark.sql.catalog.iceberg.uri", "") == nessie_uri
        )
        default_session = "org.apache.iceberg.spark.SparkSessionCatalog"
        default_leaf = "org.apache.iceberg.spark.SparkCatalog"
        assert configs.get("spark.sql.catalog.spark_catalog", "") == default_session
        assert configs.get("spark.sql.catalog.iceberg", "") == default_leaf

    def test_nessie_writer_alias_without_uri_raises_like_rest(self, monkeypatch):
        runtime_context._reset_for_tests()
        import pytest

        with pytest.raises(ValueError, match="nessie requires|rest requires"):
            build_spark_session(
                app_name="test",
                iceberg_enabled=True,
                iceberg_warehouse_dir="/tmp/wh",
                iceberg_catalog_name="iceberg",
                iceberg_catalog_type="nessie",
            )

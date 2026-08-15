from __future__ import annotations

import inspect
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from elt_pipeline.shared.runtime import RunContext, StageName, TriggerType
from elt_pipeline.sql.models import SqlModelStage
from elt_pipeline.sql.parity_check import (
    ModelParity,
    _warehouse_path_for_stage,
    compare_parity_reports,
    load_parity_report,
    write_parity_report,
)
from elt_pipeline.sql.runtime import _build_audit_context


class TestParityPathLayout:
    def test_warehouse_path_no_domain_segment_p1_fix(self):
        path = _warehouse_path_for_stage(
            warehouse_root="/tmp/wh",
            stage=SqlModelStage.level3,
            table_name="orders",
        )
        assert str(path) == "/tmp/wh/level3/orders"

    def test_warehouse_path_stage_values_all_consistent(self):
        for stage in (SqlModelStage.level3, SqlModelStage.level4):
            p = _warehouse_path_for_stage(
                warehouse_root="/wh", stage=stage, table_name="t"
            )
            assert f"/{stage.value}/" in str(p)
            assert str(p).endswith("/t")

    def test_warehouse_path_matches_spark_executor_table_path(self):
        import inspect

        from elt_pipeline.sql.spark_executor import SparkSqlModelExecutor
        src = inspect.getsource(SparkSqlModelExecutor._table_path)
        assert "join_paths" in src
        assert "stage.value" in src
        assert "table_name" in src
        assert "domain" not in src


class TestCompareParityReports:
    def test_match_with_column_reorder_tolerance(self):
        left = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="sales",
                name="orders",
                row_count=2,
                md5_of_sorted_row_hashes="abc123",
                columns=["order_id", "amount"],
            ),
            ModelParity(
                model_id="m2",
                stage="level4",
                domain="sales",
                name="summary",
                row_count=1,
                md5_of_sorted_row_hashes="def456",
                columns=["total"],
            ),
        ]
        right = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="sales",
                name="orders",
                row_count=2,
                md5_of_sorted_row_hashes="abc123",
                columns=["amount", "order_id"],
            ),
            ModelParity(
                model_id="m2",
                stage="level4",
                domain="sales",
                name="summary",
                row_count=1,
                md5_of_sorted_row_hashes="def456",
                columns=["total"],
            ),
        ]
        result = compare_parity_reports(left, right)
        assert result["parity"] is True
        assert result["match_count"] == 2
        assert result["mismatch_count"] == 0
        assert result["missing_left"] == []
        assert result["missing_right"] == []

    def test_mismatch_detects_row_count_md5_and_missing_models(self):
        left = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="sales",
                name="orders",
                row_count=3,
                md5_of_sorted_row_hashes="abc123",
                columns=["order_id"],
            ),
        ]
        right = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="sales",
                name="orders",
                row_count=2,
                md5_of_sorted_row_hashes="xyz789",
                columns=["order_id"],
            ),
            ModelParity(
                model_id="m3",
                stage="level3",
                domain="sales",
                name="extra",
                row_count=5,
                md5_of_sorted_row_hashes="000",
                columns=[],
            ),
        ]
        result = compare_parity_reports(left, right)
        assert result["parity"] is False
        assert result["mismatch_count"] == 1
        assert result["missing_left"] == ["m3"]
        assert result["missing_right"] == []
        m = result["mismatches"][0]
        assert m["model_id"] == "m1"
        assert m["row_count_match"] is False
        assert m["md5_match"] is False
        assert m["columns_match"] is True

    def test_mismatch_detects_column_order_only_does_not_flag(self):
        left = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="s",
                name="t",
                row_count=1,
                md5_of_sorted_row_hashes="x",
                columns=["z", "a", "m"],
            ),
        ]
        right = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="s",
                name="t",
                row_count=1,
                md5_of_sorted_row_hashes="x",
                columns=["a", "m", "z"],
            ),
        ]
        result = compare_parity_reports(left, right)
        assert result["parity"] is True

    def test_missing_both_sides_reports_correctly(self):
        left = [
            ModelParity(
                model_id="only_left",
                stage="level3",
                domain="d",
                name="n",
                row_count=0,
                md5_of_sorted_row_hashes="",
                columns=[],
            ),
        ]
        right = [
            ModelParity(
                model_id="only_right",
                stage="level3",
                domain="d",
                name="n",
                row_count=0,
                md5_of_sorted_row_hashes="",
                columns=[],
            ),
        ]
        result = compare_parity_reports(left, right)
        assert result["parity"] is False
        assert result["missing_left"] == ["only_right"]
        assert result["missing_right"] == ["only_left"]
        assert result["mismatch_count"] == 0

    def test_empty_inputs_parity_true(self):
        result = compare_parity_reports([], [])
        assert result["parity"] is True
        assert result["match_count"] == 0
        assert result["mismatch_count"] == 0
        assert result["total_models"] == 0


class TestParityJsonRoundtrip:
    def test_write_load_roundtrip_preserves_parity(self):
        models = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="sales",
                name="orders",
                row_count=10,
                md5_of_sorted_row_hashes="aaa",
                columns=["a", "b"],
            ),
            ModelParity(
                model_id="m2",
                stage="level4",
                domain="inv",
                name="ship",
                row_count=0,
                md5_of_sorted_row_hashes="",
                columns=[],
            ),
        ]
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "report.json")
            write_parity_report(path, models)
            with open(path) as f:
                raw_text = f.read()
            parsed = json.loads(raw_text)
            assert "models" in parsed
            assert len(parsed["models"]) == 2
            assert raw_text.startswith("{\n")
            assert "\n  \"models\"" in raw_text
            loaded = load_parity_report(path)
            result = compare_parity_reports(models, loaded)
            assert result["parity"] is True, f"Roundtrip failed: {result}"

    def test_write_uses_sort_keys_for_diffability(self):
        models = [
            ModelParity(
                model_id="m1",
                stage="level3",
                domain="d",
                name="n",
                row_count=1,
                md5_of_sorted_row_hashes="hash",
                columns=["z_col", "a_col"],
            ),
        ]
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "r.json")
            write_parity_report(path, models)
            text = Path(path).read_text()
        first_model_obj = json.loads(text)["models"][0]
        keys_in_order = list(first_model_obj.keys())
        expected_sorted = sorted(keys_in_order)
        assert keys_in_order == expected_sorted, (
            f"sort_keys=True should sort keys within each model object: "
            f"got {keys_in_order}, expected sorted {expected_sorted}"
        )
        assert text.startswith("{\n  \"models\":")


class TestSqlAuditContextServingEndpoint:
    @staticmethod
    def _run_context():
        return RunContext(
            run_id="r1",
            stage=StageName.sql,
            job_name="j1",
            trigger_type=TriggerType.manual,
            started_at=datetime.now(tz=UTC),
            attributes={},
        )

    def test_key_omitted_when_serving_endpoint_none_backward_compat(self):
        ctx = _build_audit_context(
            environment="env1",
            package_path=Path("/tmp/pkg"),
            warehouse_root="/tmp/wh",
            root_path="/tmp/rt",
            compiled_models=[],
            partition_values=None,
            extra_values=None,
            selection_stage=None,
            selection_domain=None,
            selection_model=None,
            include_dependencies=False,
            run_context=self._run_context(),
            quality_summary=None,
            serving_endpoint=None,
        )
        assert "serving_endpoint" not in ctx

    def test_key_present_and_serialized_json_sort_keys(self):
        endpoint = {
            "table_format": "iceberg",
            "catalog_name": "iceberg",
            "catalog_type": "hadoop",
            "engines": {"trino": {"jdbc_url": "jdbc:trino://h:p/c"}},
        }
        ctx = _build_audit_context(
            environment="env1",
            package_path=Path("/tmp/pkg"),
            warehouse_root="/tmp/wh",
            root_path="/tmp/rt",
            compiled_models=[],
            partition_values=None,
            extra_values=None,
            selection_stage=None,
            selection_domain=None,
            selection_model=None,
            include_dependencies=False,
            run_context=self._run_context(),
            quality_summary=None,
            serving_endpoint=endpoint,
        )
        assert "serving_endpoint" in ctx
        val = ctx["serving_endpoint"]
        assert isinstance(val, str)
        decoded = json.loads(val)
        assert decoded["table_format"] == "iceberg"
        assert decoded["catalog_type"] == "hadoop"
        assert decoded["engines"]["trino"]["jdbc_url"] == "jdbc:trino://h:p/c"
        re_encoded = json.dumps(endpoint, sort_keys=True)
        assert val == re_encoded, "Should be exactly json.dumps(sort_keys=True)"

    def test_partition_values_and_serving_endpoint_use_same_convention(self):
        endpoint = {"a": 1}
        partitions = {"business_date": "2026-01-01"}
        ctx = _build_audit_context(
            environment="env1",
            package_path=Path("/tmp/pkg"),
            warehouse_root="/tmp/wh",
            root_path="/tmp/rt",
            compiled_models=[],
            partition_values=partitions,
            extra_values=None,
            selection_stage=None,
            selection_domain=None,
            selection_model=None,
            include_dependencies=False,
            run_context=self._run_context(),
            quality_summary=None,
            serving_endpoint=endpoint,
        )
        pv = json.loads(ctx["partition_values"])
        assert pv == partitions
        se = json.loads(ctx["serving_endpoint"])
        assert se == endpoint


class TestBuildServingEndpointDisabled:
    def test_returns_none_when_iceberg_disabled(self):
        from elt_pipeline.cli import _build_serving_endpoint
        args = SimpleNamespace(
            iceberg_enabled=False,
            iceberg_catalog_name=None,
            iceberg_catalog_type=None,
            iceberg_catalog_uri=None,
            iceberg_warehouse_dir=None,
            iceberg_glue_region=None,
            warehouse_root=None,
        )
        with patch.dict(os.environ, {}, clear=True):
            result = _build_serving_endpoint(args)
        assert result is None


class TestBuildServingEndpointEnabledShape:
    @pytest.mark.parametrize(
        "catalog_type,uri_required,extra_kwargs",
        [
            ("hadoop", False, {}),
            ("glue", False, {"iceberg_glue_region": "us-east-1"}),
        ],
    )
    def test_shape_matches_test_serving_endpoint_shape_suite(
        self, catalog_type, uri_required, extra_kwargs
    ):
        from elt_pipeline.cli import _build_serving_endpoint
        args = SimpleNamespace(
            iceberg_enabled=True,
            iceberg_catalog_name="iceberg",
            iceberg_catalog_type=catalog_type,
            iceberg_catalog_uri=None,
            iceberg_warehouse_dir="/tmp/wh/iceberg",
            warehouse_root="/tmp/wh",
            **extra_kwargs,
        )
        with patch.dict(os.environ, {}, clear=True):
            ep = _build_serving_endpoint(args)
        assert ep is not None
        assert ep["table_format"] == "iceberg"
        assert ep["catalog_name"] == "iceberg"
        assert ep["catalog_type"] == catalog_type
        assert "catalog_type_note" in ep
        assert "warehouse_dir" in ep
        assert "engines" in ep
        engines = ep["engines"]
        assert "trino" in engines
        assert "jdbc_url" in engines["trino"]
        assert engines["trino"]["driver_class"] == "io.trino.jdbc.TrinoDriver"
        assert "sample_query" in engines["trino"]
        assert "spark_thrift" in engines
        assert "athena" in engines
        assert "duckdb" in engines


class TestPublishServingEndpointKwarg:
    def test_run_publish_definitions_has_serving_endpoint_default_none(self):
        from elt_pipeline.publish.runtime import run_publish_definitions_locally
        sig = inspect.signature(run_publish_definitions_locally)
        params = sig.parameters
        assert "serving_endpoint" in params
        assert params["serving_endpoint"].default is None

    def test_run_sql_models_locally_has_serving_endpoint_default_none(self):
        from elt_pipeline.sql.runtime import run_sql_models_locally
        sig = inspect.signature(run_sql_models_locally)
        params = sig.parameters
        assert "serving_endpoint" in params
        assert params["serving_endpoint"].default is None

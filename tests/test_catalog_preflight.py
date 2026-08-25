from __future__ import annotations

import urllib.error
from unittest import mock

import pytest

from elt_pipeline.shared.catalog_preflight import (
    CatalogPreflightCheckName,
    CatalogPreflightMode,
    CatalogPreflightResult,
    load_catalog_preflight_config_from_env,
    run_catalog_preflight,
)
from elt_pipeline.shared.errors import ConfigValidationError


class TestCatalogPreflightMode:
    def test_mode_enum_values(self) -> None:
        assert CatalogPreflightMode.off.value == "off"
        assert CatalogPreflightMode.best_effort.value == "best_effort"
        assert CatalogPreflightMode.strict.value == "strict"


class TestEnvConfigLoader:
    def test_default_mode_is_best_effort(self) -> None:
        conf = load_catalog_preflight_config_from_env(environ={})
        assert conf["mode"] == CatalogPreflightMode.best_effort
        assert conf["timeout_seconds"] == 5

    def test_explicit_best_effort(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_MODE": "best_effort"}
        )
        assert conf["mode"] == CatalogPreflightMode.best_effort

    def test_off_mode(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_MODE": "off"}
        )
        assert conf["mode"] == CatalogPreflightMode.off

    def test_strict_mode(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_MODE": "strict"}
        )
        assert conf["mode"] == CatalogPreflightMode.strict

    def test_mode_case_insensitive(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_MODE": "  BEST_EFFORT  "}
        )
        assert conf["mode"] == CatalogPreflightMode.best_effort

    def test_invalid_mode_raises(self) -> None:
        with pytest.raises(ConfigValidationError) as exc:
            load_catalog_preflight_config_from_env(
                environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_MODE": "bogus"}
            )
        assert "bogus" in exc.value.context["provided_mode"]

    def test_timeout_override_int(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS": "10"}
        )
        assert conf["timeout_seconds"] == 10

    def test_timeout_empty_defaults(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS": ""}
        )
        assert conf["timeout_seconds"] == 5

    def test_timeout_invalid_ignored(self) -> None:
        conf = load_catalog_preflight_config_from_env(
            environ={"ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS": "notanumber"}
        )
        assert conf["timeout_seconds"] == 5


class TestJdbcChecks:
    def test_jdbc_uri_valid_pass(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:sqlite:/tmp/foo.db"},
            serving_catalog_type="jdbc",
            serving_config={"catalog_uri": "jdbc:sqlite:/tmp/srv.db"},
            mode="best_effort",
            timeout_seconds=1,
        )
        uri_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_uri_valid
        ]
        assert len(uri_results) == 2
        for r in uri_results:
            assert r.passed, r.message
        assert uri_results[0].context["subprotocol"] == "sqlite"

    def test_jdbc_uri_empty_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        uri_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_uri_valid
        ]
        assert len(uri_results) == 1
        assert not uri_results[0].passed
        assert "empty" in uri_results[0].message.lower()

    def test_jdbc_uri_missing_prefix_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "notjdbc:sqlite:x"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        uri_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_uri_valid
        ]
        assert not uri_results[0].passed
        assert "jdbc:" in uri_results[0].message

    def test_jdbc_uri_missing_subprotocol_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        uri_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_uri_valid
        ]
        assert not uri_results[0].passed

    def test_jdbc_sqlite_in_memory_skipped(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:sqlite::memory:"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        parent_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_sqlite_parent_dir
        ]
        assert len(parent_results) == 1
        assert parent_results[0].passed
        assert "in-memory" in parent_results[0].message.lower()

    def test_jdbc_sqlite_parent_dir_created(self, tmp_path) -> None:
        db_path = tmp_path / "nested" / "newdir" / "test.db"
        uri = f"jdbc:sqlite:{db_path}"
        assert not (tmp_path / "nested" / "newdir").exists()
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": uri},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        parent_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.jdbc_sqlite_parent_dir
        ]
        assert len(parent_results) == 1
        assert parent_results[0].passed
        assert parent_results[0].context.get("created") is True
        assert (tmp_path / "nested" / "newdir").exists()


class TestRestCatalogChecks:
    def test_rest_bad_scheme_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="rest",
            writer_config={"catalog_uri": "ftp://example.com"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert len(rest_results) == 1
        assert not rest_results[0].passed
        assert "http/https" in rest_results[0].message

    def test_rest_empty_uri_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="rest",
            writer_config={"catalog_uri": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert not rest_results[0].passed
        assert "empty" in rest_results[0].message.lower()

    def test_rest_connectivity_success_via_mock(self) -> None:
        fake_resp = mock.MagicMock()
        fake_resp.status = 200
        fake_resp.__enter__ = mock.MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = mock.MagicMock(return_value=False)
        with mock.patch(
            "urllib.request.urlopen", return_value=fake_resp
        ) as mock_urlopen:
            results = run_catalog_preflight(
                writer_catalog_type="rest",
                writer_config={
                    "catalog_uri": "https://rest.example.com:8181",
                    "rest_token": "tok",
                },
                serving_catalog_type="hadoop",
                serving_config={},
                mode="best_effort",
                timeout_seconds=2,
            )
            assert mock_urlopen.called
            req = mock_urlopen.call_args[0][0]
            assert req.get_header("Authorization") == "Bearer tok"
            assert "/v1/config" in req.full_url
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert rest_results[0].passed
        assert "HTTP 200" in rest_results[0].message

    def test_rest_http_404_client_error_pass(self) -> None:
        http_err = urllib.error.HTTPError(
            url="https://example.com/v1/config",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=None,
        )
        with mock.patch("urllib.request.urlopen", side_effect=http_err):
            results = run_catalog_preflight(
                writer_catalog_type="nessie",
                writer_config={"catalog_uri": "https://nessie.example.com"},
                serving_catalog_type="hadoop",
                serving_config={},
                mode="best_effort",
                timeout_seconds=2,
            )
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert rest_results[0].passed
        assert "HTTP 404" in rest_results[0].message

    def test_rest_unreachable_fail(self) -> None:
        err = urllib.error.URLError("connection refused")
        with mock.patch("urllib.request.urlopen", side_effect=err):
            results = run_catalog_preflight(
                writer_catalog_type="rest",
                writer_config={"catalog_uri": "http://127.0.0.1:1"},
                serving_catalog_type="hadoop",
                serving_config={},
                mode="best_effort",
                timeout_seconds=1,
            )
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert not rest_results[0].passed
        assert "unreachable" in rest_results[0].message.lower()

    def test_nessie_mapped_to_rest_branch(self) -> None:
        fake_resp = mock.MagicMock()
        fake_resp.status = 200
        fake_resp.__enter__ = mock.MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = mock.MagicMock(return_value=False)
        with mock.patch("urllib.request.urlopen", return_value=fake_resp):
            results = run_catalog_preflight(
                writer_catalog_type="nessie",
                writer_config={"catalog_uri": "https://nessie.example.com"},
                serving_catalog_type="hadoop",
                serving_config={},
                mode="best_effort",
                timeout_seconds=2,
            )
        rest_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.rest_catalog_connectivity
        ]
        assert len(rest_results) == 1
        assert "writer_nessie" in rest_results[0].binding


class TestHiveMetastoreChecks:
    def test_hive_uri_format_pass(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": "thrift://hm.example.com:9083"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        format_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_uri_format
        ]
        assert len(format_results) == 1
        assert format_results[0].passed
        assert format_results[0].context["host"] == "hm.example.com"
        assert format_results[0].context["port"] == 9083

    def test_hive_uri_empty_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        format_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_uri_format
        ]
        assert not format_results[0].passed
        assert "empty" in format_results[0].message.lower()

    def test_hive_uri_no_prefix_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": "http://hm:9083"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        format_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_uri_format
        ]
        assert not format_results[0].passed
        assert "thrift://" in format_results[0].message

    def test_hive_uri_no_port_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": "thrift://hm.example.com"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        format_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_uri_format
        ]
        assert not format_results[0].passed
        assert "port" in format_results[0].message.lower()

    def test_hive_uri_bad_port_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": "thrift://hm:99999"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        format_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_uri_format
        ]
        assert not format_results[0].passed
        assert "port" in format_results[0].message.lower()

    def test_hive_tcp_connect_skip_on_bad_format(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        tcp_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_tcp_connect
        ]
        assert len(tcp_results) == 0

    def test_hive_tcp_connect_fail_on_unreachable(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hive_metastore",
            writer_config={"hive_metastore_uri": "thrift://127.0.0.1:1"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        tcp_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hive_metastore_tcp_connect
        ]
        assert len(tcp_results) == 1
        assert not tcp_results[0].passed
        assert "unreachable" in tcp_results[0].message.lower()


class TestGlueChecks:
    def test_glue_skip_without_boto3(self) -> None:
        with mock.patch.dict("sys.modules", {"boto3": None, "botocore": None}):
            with mock.patch("builtins.__import__", side_effect=ImportError("no boto3")):
                results = run_catalog_preflight(
                    writer_catalog_type="glue",
                    writer_config={"glue_region": "us-east-1"},
                    serving_catalog_type="hadoop",
                    serving_config={},
                    mode="best_effort",
                    timeout_seconds=1,
                )
        glue_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.glue_identity_available
        ]
        assert glue_results[0].passed
        assert "boto3 not installed" in glue_results[0].message


class TestHadoopChecks:
    def test_hadoop_warehouse_exists_pass(self, tmp_path) -> None:
        wh = tmp_path / "warehouse"
        wh.mkdir()
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={"warehouse_dir": str(wh)},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        hadoop_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hadoop_warehouse_dir
        ]
        assert hadoop_results[0].passed
        assert "exists" in hadoop_results[0].message.lower()

    def test_hadoop_warehouse_parent_exists_pass(self, tmp_path) -> None:
        parent = tmp_path / "parent"
        parent.mkdir()
        wh = parent / "sub_warehouse"
        assert not wh.exists()
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={"warehouse_dir": str(wh)},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        hadoop_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hadoop_warehouse_dir
        ]
        assert hadoop_results[0].passed
        assert "parent exists" in hadoop_results[0].message.lower()
        assert not wh.exists()

    def test_hadoop_warehouse_creates_dir(self, tmp_path) -> None:
        wh = tmp_path / "a" / "b" / "newwh"
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={"warehouse_dir": str(wh)},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        hadoop_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hadoop_warehouse_dir
        ]
        assert hadoop_results[0].passed
        assert hadoop_results[0].context.get("created") is True
        assert wh.exists()

    def test_hadoop_warehouse_empty_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={"warehouse_dir": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        hadoop_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.hadoop_warehouse_dir
        ]
        assert not hadoop_results[0].passed
        assert "empty" in hadoop_results[0].message.lower()


class TestSnowflakeChecks:
    def test_snowflake_params_valid_pass(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={},
            serving_catalog_type="snowflake",
            serving_config={"catalog_uri": "https://acct.snowflakecomputing.com"},
            mode="best_effort",
            timeout_seconds=1,
        )
        sf_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.snowflake_serving_params
        ]
        assert len(sf_results) == 1
        assert sf_results[0].passed
        assert sf_results[0].context["uri_scheme"] == "https"

    def test_snowflake_missing_uri_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={},
            serving_catalog_type="snowflake",
            serving_config={"catalog_uri": ""},
            mode="best_effort",
            timeout_seconds=1,
        )
        sf_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.snowflake_serving_params
        ]
        assert not sf_results[0].passed
        assert "missing required" in sf_results[0].message.lower()

    def test_snowflake_scheme_snowflake_accepts(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={},
            serving_catalog_type="snowflake",
            serving_config={"catalog_uri": "snowflake://acct/db/schema"},
            mode="best_effort",
            timeout_seconds=1,
        )
        sf_results = [
            r for r in results
            if r.check_name == CatalogPreflightCheckName.snowflake_serving_params
        ]
        assert sf_results[0].passed
        assert sf_results[0].context["uri_scheme"] == "snowflake"


class TestPreflightDispatcher:
    def test_mode_off_returns_empty(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:sqlite:/x.db"},
            serving_catalog_type="jdbc",
            serving_config={"catalog_uri": "jdbc:sqlite:/y.db"},
            mode="off",
            timeout_seconds=1,
        )
        assert results == []

    def test_mode_off_enum(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:sqlite:/x.db"},
            serving_catalog_type="jdbc",
            serving_config={"catalog_uri": "jdbc:sqlite:/y.db"},
            mode=CatalogPreflightMode.off,
            timeout_seconds=1,
        )
        assert results == []

    def test_invalid_mode_string_raises(self) -> None:
        with pytest.raises(ConfigValidationError) as exc:
            run_catalog_preflight(
                writer_catalog_type="hadoop",
                writer_config={},
                serving_catalog_type="hadoop",
                serving_config={},
                mode="not_a_mode",
                timeout_seconds=1,
            )
        assert "not_a_mode" in exc.value.context["provided_mode"]

    def test_best_effort_does_not_raise_on_fail(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": ""},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        assert isinstance(results, list)
        assert len(results) >= 1
        assert any(not r.passed for r in results)

    def test_strict_mode_raises_on_fail(self) -> None:
        with pytest.raises(ConfigValidationError) as exc:
            run_catalog_preflight(
                writer_catalog_type="jdbc",
                writer_config={"catalog_uri": ""},
                serving_catalog_type="hadoop",
                serving_config={},
                mode="strict",
                timeout_seconds=1,
            )
        ctx = exc.value.context
        assert ctx["failed_count"] >= 1
        assert "catalog preflight" in exc.value.message.lower()
        assert "strict" in exc.value.message.lower()

    def test_strict_mode_passes_when_all_green(self, tmp_path) -> None:
        wh = tmp_path / "wh"
        wh.mkdir()
        results = run_catalog_preflight(
            writer_catalog_type="hadoop",
            writer_config={"warehouse_dir": str(wh)},
            serving_catalog_type="jdbc",
            serving_config={"catalog_uri": f"jdbc:sqlite:{tmp_path / 'sv.db'}"},
            mode="strict",
            timeout_seconds=1,
        )
        assert all(r.passed for r in results)

    def test_result_shape(self) -> None:
        results = run_catalog_preflight(
            writer_catalog_type="jdbc",
            writer_config={"catalog_uri": "jdbc:postgresql://host/db"},
            serving_catalog_type="hadoop",
            serving_config={},
            mode="best_effort",
            timeout_seconds=1,
        )
        r = results[0]
        assert isinstance(r, CatalogPreflightResult)
        assert isinstance(r.check_name, CatalogPreflightCheckName)
        assert r.duration_ms >= 0
        assert r.passed in (True, False)
        assert r.binding == "writer_jdbc"


@pytest.mark.parametrize(
    "wtype,stype",
    [
        ("hadoop", "hadoop"),
        ("jdbc", "jdbc"),
        ("rest", "rest"),
        ("nessie", "jdbc"),
        ("hive_metastore", "snowflake"),
        ("glue", "nessie"),
    ],
)
def test_dispatcher_covers_all_valid_types(wtype: str, stype: str) -> None:
    fake_resp = mock.MagicMock()
    fake_resp.status = 200
    fake_resp.__enter__ = mock.MagicMock(return_value=fake_resp)
    fake_resp.__exit__ = mock.MagicMock(return_value=False)
    fake_cfg = {
        "catalog_uri": f"jdbc:sqlite:/tmp/x-{wtype}.db" if wtype in {"jdbc"} else "",
        "hive_metastore_uri": "thrift://127.0.0.1:1" if wtype == "hive_metastore" else "",
        "glue_region": "us-east-1" if wtype == "glue" else "",
        "warehouse_dir": "/tmp/wh_dummy",
    }
    if wtype in {"rest", "nessie"}:
        fake_cfg["catalog_uri"] = "https://rest.example.com"
    fake_s_cfg = {
        "catalog_uri": f"jdbc:sqlite:/tmp/sy-{stype}.db" if stype in {"jdbc"} else "",
    }
    if stype in {"rest", "nessie"}:
        fake_s_cfg["catalog_uri"] = "https://rest-srv.example.com"
    if stype == "snowflake":
        fake_s_cfg["catalog_uri"] = "https://acct.snowflakecomputing.com"
    with mock.patch("urllib.request.urlopen", return_value=fake_resp):
        results = run_catalog_preflight(
            writer_catalog_type=wtype,
            writer_config=fake_cfg,
            serving_catalog_type=stype,
            serving_config=fake_s_cfg,
            mode="best_effort",
            timeout_seconds=1,
        )
    assert isinstance(results, list)
    assert len(results) >= 1

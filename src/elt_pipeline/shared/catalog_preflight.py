from __future__ import annotations

import os
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlparse

from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.path_utils import (
    path_exists,
    path_mkdir,
    path_normalize,
    path_parent,
)

__all__ = [
    "CatalogPreflightCheckName",
    "CatalogPreflightMode",
    "CatalogPreflightResult",
    "load_catalog_preflight_config_from_env",
    "run_catalog_preflight",
]


class CatalogPreflightMode(str, Enum):
    off = "off"
    best_effort = "best_effort"
    strict = "strict"


class CatalogPreflightCheckName(str, Enum):
    jdbc_uri_valid = "jdbc_uri_valid"
    jdbc_sqlite_parent_dir = "jdbc_sqlite_parent_dir"
    rest_catalog_connectivity = "rest_catalog_connectivity"
    hive_metastore_uri_format = "hive_metastore_uri_format"
    hive_metastore_tcp_connect = "hive_metastore_tcp_connect"
    glue_identity_available = "glue_identity_available"
    hadoop_warehouse_dir = "hadoop_warehouse_dir"
    snowflake_serving_params = "snowflake_serving_params"


@dataclass
class CatalogPreflightResult:
    check_name: CatalogPreflightCheckName
    binding: str
    status: str
    message: str
    duration_ms: int
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == "pass"


def _parse_bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off", ""}:
        return False
    return default


def _parse_int_env(value: str | None, default: int) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return default


def load_catalog_preflight_config_from_env(
    *,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    env = environ if environ is not None else os.environ
    mode_raw = env.get(runtime_manifest.env.catalog_preflight_mode, "")
    mode_raw = mode_raw.strip().lower() if mode_raw else ""
    if mode_raw in {"", "best_effort"}:
        mode = CatalogPreflightMode.best_effort
    elif mode_raw == "off":
        mode = CatalogPreflightMode.off
    elif mode_raw == "strict":
        mode = CatalogPreflightMode.strict
    else:
        raise ConfigValidationError(
            message=(
                "Invalid catalog_preflight_mode. "
                "Supported: off, best_effort, strict."
            ),
            context={"provided_mode": mode_raw},
        )
    timeout = _parse_int_env(
        env.get(runtime_manifest.env.catalog_preflight_timeout_seconds),
        5,
    )
    return {"mode": mode, "timeout_seconds": timeout}


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


def _result(
    *,
    check_name: CatalogPreflightCheckName,
    binding: str,
    passed: bool,
    message: str,
    start_ms: int,
    context: dict[str, Any] | None = None,
) -> CatalogPreflightResult:
    return CatalogPreflightResult(
        check_name=check_name,
        binding=binding,
        status="pass" if passed else "fail",
        message=message,
        duration_ms=_now_ms() - start_ms,
        context=context or {},
    )


def _check_jdbc_uri_valid(
    uri: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    if not uri or not isinstance(uri, str):
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_uri_valid,
            binding=binding,
            passed=False,
            message="JDBC catalog URI is empty",
            start_ms=start,
        )
    if not uri.startswith("jdbc:"):
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_uri_valid,
            binding=binding,
            passed=False,
            message="JDBC URI must start with 'jdbc:'",
            start_ms=start,
            context={"uri_prefix": uri[:20]},
        )
    after_jdbc = uri[5:]
    if ":" not in after_jdbc:
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_uri_valid,
            binding=binding,
            passed=False,
            message="JDBC URI missing sub-protocol separator ':' after 'jdbc:'",
            start_ms=start,
        )
    subprotocol = after_jdbc.split(":", 1)[0]
    if not subprotocol:
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_uri_valid,
            binding=binding,
            passed=False,
            message="JDBC URI has empty sub-protocol",
            start_ms=start,
        )
    return _result(
        check_name=CatalogPreflightCheckName.jdbc_uri_valid,
        binding=binding,
        passed=True,
        message=f"JDBC URI well-formed (sub-protocol: {subprotocol})",
        start_ms=start,
        context={"subprotocol": subprotocol},
    )


def _check_jdbc_sqlite_parent_dir(
    uri: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    if not uri.startswith("jdbc:sqlite:"):
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_sqlite_parent_dir,
            binding=binding,
            passed=True,
            message="Skipped (not a sqlite JDBC URI)",
            start_ms=start,
        )
    sqlite_path = uri[len("jdbc:sqlite:") :]
    if (
        not sqlite_path
        or sqlite_path.startswith(":memory:")
        or sqlite_path.startswith("file::memory:")
    ):
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_sqlite_parent_dir,
            binding=binding,
            passed=True,
            message="Skipped (in-memory sqlite database)",
            start_ms=start,
        )
    if sqlite_path.startswith("file:"):
        sqlite_path = sqlite_path[5:].split("?", 1)[0].split("&", 1)[0]
    try:
        parent = path_parent(sqlite_path)
        if not parent:
            parent = "."
        if not path_exists(parent):
            path_mkdir(parent, exist_ok=True)
            return _result(
                check_name=CatalogPreflightCheckName.jdbc_sqlite_parent_dir,
                binding=binding,
                passed=True,
                message=f"SQLite parent directory did not exist, created: {parent}",
                start_ms=start,
                context={"parent_dir": parent, "created": True},
            )
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_sqlite_parent_dir,
            binding=binding,
            passed=True,
            message=f"SQLite parent directory exists: {parent}",
            start_ms=start,
            context={"parent_dir": parent, "created": False},
        )
    except Exception as exc:
        return _result(
            check_name=CatalogPreflightCheckName.jdbc_sqlite_parent_dir,
            binding=binding,
            passed=False,
            message=f"Failed to check/create SQLite parent directory: {exc}",
            start_ms=start,
            context={"parent_dir": sqlite_path, "error": str(exc)},
        )


def _check_rest_catalog_connectivity(
    uri: str,
    *,
    token: str | None,
    binding: str,
    timeout_seconds: int,
) -> CatalogPreflightResult:
    start = _now_ms()
    if not uri:
        return _result(
            check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
            binding=binding,
            passed=False,
            message="REST catalog URI is empty",
            start_ms=start,
        )
    parsed = urlparse(uri)
    if parsed.scheme not in {"http", "https"}:
        return _result(
            check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
            binding=binding,
            passed=False,
            message=f"REST catalog URI must use http/https scheme, got: {parsed.scheme!r}",
            start_ms=start,
            context={"scheme": parsed.scheme},
        )
    probe_uri = uri.rstrip("/") + "/v1/config"
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(probe_uri, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            code = getattr(resp, "status", 200)
            if 200 <= code < 500:
                return _result(
                    check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
                    binding=binding,
                    passed=True,
                    message=f"REST catalog reachable (HTTP {code})",
                    start_ms=start,
                    context={"http_status": code, "probe_uri": probe_uri},
                )
            return _result(
                check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
                binding=binding,
                passed=False,
                message=f"REST catalog returned server error (HTTP {code})",
                start_ms=start,
                context={"http_status": code, "probe_uri": probe_uri},
            )
    except urllib.error.HTTPError as exc:
        code = getattr(exc, "code", 0)
        if 400 <= code < 500:
            return _result(
                check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
                binding=binding,
                passed=True,
                message=(
                    f"REST catalog reachable (HTTP {code}, "
                    "client-side response acceptable for connectivity probe)"
                ),
                start_ms=start,
                context={"http_status": code, "probe_uri": probe_uri},
            )
        return _result(
            check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
            binding=binding,
            passed=False,
            message=f"REST catalog HTTP error: {exc}",
            start_ms=start,
            context={"http_status": code, "probe_uri": probe_uri, "error": str(exc)},
        )
    except (urllib.error.URLError, OSError, socket.timeout) as exc:
        return _result(
            check_name=CatalogPreflightCheckName.rest_catalog_connectivity,
            binding=binding,
            passed=False,
            message=f"REST catalog unreachable: {exc}",
            start_ms=start,
            context={"probe_uri": probe_uri, "error": str(exc)},
        )


def _check_hive_metastore_uri_format(
    uri: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    if not uri:
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
            binding=binding,
            passed=False,
            message="Hive metastore URI is empty",
            start_ms=start,
        )
    if not uri.startswith("thrift://"):
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
            binding=binding,
            passed=False,
            message=f"Hive metastore URI must start with 'thrift://', got: {uri[:30]!r}",
            start_ms=start,
        )
    authority = uri[len("thrift://") :].split("/", 1)[0]
    if ":" not in authority:
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
            binding=binding,
            passed=False,
            message=f"Hive metastore URI missing port in authority: {authority!r}",
            start_ms=start,
        )
    host, port_str = authority.rsplit(":", 1)
    if not host:
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
            binding=binding,
            passed=False,
            message="Hive metastore URI has empty host",
            start_ms=start,
        )
    try:
        port = int(port_str)
        if port <= 0 or port > 65535:
            raise ValueError("port out of range")
    except (TypeError, ValueError):
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
            binding=binding,
            passed=False,
            message=f"Hive metastore URI has invalid port: {port_str!r}",
            start_ms=start,
        )
    return _result(
        check_name=CatalogPreflightCheckName.hive_metastore_uri_format,
        binding=binding,
        passed=True,
        message=f"Hive metastore URI well-formed (host={host}, port={port})",
        start_ms=start,
        context={"host": host, "port": port},
    )


def _check_hive_metastore_tcp_connect(
    uri: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    if not uri.startswith("thrift://"):
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_tcp_connect,
            binding=binding,
            passed=True,
            message="Skipped (not a thrift:// URI)",
            start_ms=start,
        )
    authority = uri[len("thrift://") :].split("/", 1)[0]
    if ":" not in authority:
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_tcp_connect,
            binding=binding,
            passed=False,
            message="Cannot TCP-check: no port in URI authority",
            start_ms=start,
        )
    host, port_str = authority.rsplit(":", 1)
    try:
        port = int(port_str)
    except (TypeError, ValueError):
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_tcp_connect,
            binding=binding,
            passed=False,
            message="Cannot TCP-check: invalid port in URI",
            start_ms=start,
        )
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            pass
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_tcp_connect,
            binding=binding,
            passed=True,
            message=f"Hive metastore TCP reachable (host={host}, port={port})",
            start_ms=start,
            context={"host": host, "port": port},
        )
    except (OSError, socket.timeout) as exc:
        return _result(
            check_name=CatalogPreflightCheckName.hive_metastore_tcp_connect,
            binding=binding,
            passed=False,
            message=f"Hive metastore TCP unreachable: {exc}",
            start_ms=start,
            context={"host": host, "port": port, "error": str(exc)},
        )


def _check_glue_identity_available(
    region: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    try:
        import boto3  # type: ignore[import-not-found]
        from botocore.config import Config as BotoConfig  # type: ignore[import-not-found]
    except ImportError:
        return _result(
            check_name=CatalogPreflightCheckName.glue_identity_available,
            binding=binding,
            passed=True,
            message=(
                "Skipped (boto3 not installed; Glue catalog assumed valid "
                "for deployments with SDK present)"
            ),
            start_ms=start,
        )
    try:
        boto_cfg = BotoConfig(connect_timeout=timeout_seconds, read_timeout=timeout_seconds)
        sts = boto3.client("sts", region_name=region or None, config=boto_cfg)
        identity = sts.get_caller_identity()
        account = identity.get("Account", "unknown")
        return _result(
            check_name=CatalogPreflightCheckName.glue_identity_available,
            binding=binding,
            passed=True,
            message=f"AWS identity available (account={account})",
            start_ms=start,
            context={"account": account, "region": region or "default"},
        )
    except Exception as exc:
        return _result(
            check_name=CatalogPreflightCheckName.glue_identity_available,
            binding=binding,
            passed=False,
            message=f"AWS Glue identity check failed: {exc}",
            start_ms=start,
            context={"region": region or "default", "error": str(exc)},
        )


def _check_hadoop_warehouse_dir(
    warehouse_dir: str, *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    if not warehouse_dir:
        return _result(
            check_name=CatalogPreflightCheckName.hadoop_warehouse_dir,
            binding=binding,
            passed=False,
            message="Hadoop catalog warehouse_dir is empty",
            start_ms=start,
        )
    try:
        normalized = path_normalize(warehouse_dir)
        if path_exists(normalized):
            return _result(
                check_name=CatalogPreflightCheckName.hadoop_warehouse_dir,
                binding=binding,
                passed=True,
                message=f"Hadoop warehouse directory exists: {normalized}",
                start_ms=start,
                context={"warehouse_dir": normalized},
            )
        parent = path_parent(normalized)
        if parent and parent != normalized:
            if path_exists(parent):
                return _result(
                    check_name=CatalogPreflightCheckName.hadoop_warehouse_dir,
                    binding=binding,
                    passed=True,
                    message=(
                        "Hadoop warehouse parent exists "
                        f"(warehouse will be created by Spark): {parent}"
                    ),
                    start_ms=start,
                    context={"parent_dir": parent, "warehouse_dir": normalized},
                )
        path_mkdir(normalized, exist_ok=True)
        return _result(
            check_name=CatalogPreflightCheckName.hadoop_warehouse_dir,
            binding=binding,
            passed=True,
            message=f"Hadoop warehouse directory did not exist, created: {normalized}",
            start_ms=start,
            context={"warehouse_dir": normalized, "created": True},
        )
    except Exception as exc:
        return _result(
            check_name=CatalogPreflightCheckName.hadoop_warehouse_dir,
            binding=binding,
            passed=False,
            message=f"Failed to check/create Hadoop warehouse directory: {exc}",
            start_ms=start,
            context={"warehouse_dir": warehouse_dir, "error": str(exc)},
        )


def _check_snowflake_serving_params(
    config: dict[str, Any], *, binding: str, timeout_seconds: int
) -> CatalogPreflightResult:
    start = _now_ms()
    catalog_uri = config.get("catalog_uri", "") or ""
    required_keys = ["catalog_uri"]
    missing = [k for k in required_keys if not config.get(k)]
    if missing:
        return _result(
            check_name=CatalogPreflightCheckName.snowflake_serving_params,
            binding=binding,
            passed=False,
            message=f"Snowflake serving config missing required keys: {', '.join(missing)}",
            start_ms=start,
            context={"missing": missing},
        )
    parsed = urlparse(catalog_uri)
    if parsed.scheme not in {"https", "http", "snowflake"}:
        return _result(
            check_name=CatalogPreflightCheckName.snowflake_serving_params,
            binding=binding,
            passed=False,
            message=f"Snowflake catalog URI scheme not recognized: {parsed.scheme!r}",
            start_ms=start,
            context={"uri_prefix": catalog_uri[:40]},
        )
    return _result(
        check_name=CatalogPreflightCheckName.snowflake_serving_params,
        binding=binding,
        passed=True,
        message="Snowflake serving params well-formed",
        start_ms=start,
        context={"uri_scheme": parsed.scheme},
    )


def run_catalog_preflight(
    *,
    writer_catalog_type: str,
    writer_config: dict[str, Any],
    serving_catalog_type: str,
    serving_config: dict[str, Any],
    mode: CatalogPreflightMode | str = CatalogPreflightMode.best_effort,
    timeout_seconds: int = 5,
) -> list[CatalogPreflightResult]:
    if isinstance(mode, str):
        try:
            mode = CatalogPreflightMode(mode.strip().lower())
        except (ValueError, AttributeError) as exc:
            raise ConfigValidationError(
                message=(
                    "Invalid catalog_preflight mode. "
                    "Supported: off, best_effort, strict."
                ),
                context={"provided_mode": str(mode)},
            ) from exc
    if mode == CatalogPreflightMode.off:
        return []
    results: list[CatalogPreflightResult] = []
    wtype = (writer_catalog_type or "").strip().lower()
    stype = (serving_catalog_type or "").strip().lower()
    wconf = writer_config or {}
    sconf = serving_config or {}
    if wtype == "jdbc":
        uri = wconf.get("catalog_uri", "") or ""
        results.append(
            _check_jdbc_uri_valid(uri, binding="writer_jdbc", timeout_seconds=timeout_seconds)
        )
        if uri.startswith("jdbc:sqlite:"):
            results.append(
                _check_jdbc_sqlite_parent_dir(
                    uri, binding="writer_jdbc", timeout_seconds=timeout_seconds
                )
            )
    elif wtype in {"rest", "nessie"}:
        uri = wconf.get("catalog_uri", "") or ""
        token = wconf.get("rest_token") or wconf.get("catalog_token") or None
        results.append(
            _check_rest_catalog_connectivity(
                uri, token=token, binding=f"writer_{wtype}", timeout_seconds=timeout_seconds
            )
        )
    elif wtype == "hive_metastore":
        uri = wconf.get("hive_metastore_uri", "") or ""
        results.append(
            _check_hive_metastore_uri_format(
                uri, binding="writer_hive_metastore", timeout_seconds=timeout_seconds
            )
        )
        if results[-1].passed:
            results.append(
                _check_hive_metastore_tcp_connect(
                    uri, binding="writer_hive_metastore", timeout_seconds=timeout_seconds
                )
            )
    elif wtype == "glue":
        region = wconf.get("glue_region", "") or ""
        results.append(
            _check_glue_identity_available(
                region, binding="writer_glue", timeout_seconds=timeout_seconds
            )
        )
    elif wtype == "hadoop":
        warehouse = wconf.get("warehouse_dir", "") or ""
        results.append(
            _check_hadoop_warehouse_dir(
                warehouse, binding="writer_hadoop", timeout_seconds=timeout_seconds
            )
        )
    if stype == "jdbc":
        uri = sconf.get("catalog_uri", "") or ""
        results.append(
            _check_jdbc_uri_valid(uri, binding="serving_jdbc", timeout_seconds=timeout_seconds)
        )
        if uri.startswith("jdbc:sqlite:"):
            results.append(
                _check_jdbc_sqlite_parent_dir(
                    uri, binding="serving_jdbc", timeout_seconds=timeout_seconds
                )
            )
    elif stype in {"rest", "nessie"}:
        uri = sconf.get("catalog_uri", "") or ""
        token = sconf.get("rest_token") or sconf.get("catalog_token") or None
        results.append(
            _check_rest_catalog_connectivity(
                uri, token=token, binding=f"serving_{stype}", timeout_seconds=timeout_seconds
            )
        )
    elif stype == "snowflake":
        results.append(
            _check_snowflake_serving_params(
                sconf, binding="serving_snowflake", timeout_seconds=timeout_seconds
            )
        )
    failures = [r for r in results if not r.passed]
    if mode == CatalogPreflightMode.strict and failures:
        fail_lines = [
            f"  - [{f.binding}] {f.check_name.value}: {f.message}" for f in failures
        ]
        raise ConfigValidationError(
            message=(
                "Catalog preflight (strict mode) failed — "
                f"{len(failures)} check(s) failed before SparkSession boot:\n"
                + "\n".join(fail_lines)
            ),
            context={
                "failed_checks": [
                    {
                        "binding": f.binding,
                        "check_name": f.check_name.value,
                        "message": f.message,
                        "context": f.context,
                    }
                    for f in failures
                ],
                "total_checks": len(results),
                "failed_count": len(failures),
            },
        )
    return results

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from elt_pipeline._cli_models import (
    _MODULE_MANIFEST_ROOT_PATH_DEFAULT,
    _MODULE_MANIFEST_WH_PATH_DEFAULT,
    _RuntimeContext,
)
from elt_pipeline.config import runtime_context
from elt_pipeline.config.loader import load_runtime_overrides
from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.shared.catalog_preflight import (
    load_catalog_preflight_config_from_env,
    run_catalog_preflight,
)
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.path_utils import (
    path_normalize,
)
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error


def _load_runtime_overrides_from_env_or_args(
    *,
    config_path: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    import os as _os

    resolved_path: str | None = config_path
    if not resolved_path:
        cp_key = "ELT_PIPELINE_CONFIG_PATH"
        resolved_path = _os.environ.get(cp_key, "").strip() or None
    if resolved_path is None:
        repo_root_candidate = Path(__file__).resolve().parents[2] / "pipeline.yaml"
        if repo_root_candidate.is_file():
            resolved_path = str(repo_root_candidate)
    if resolved_path is None:
        return {}
    return load_runtime_overrides(resolved_path, environment=environment)


def _repo_run_dir(
    runtime_overrides: dict[str, Any] | None = None,
) -> Path | None:
    import os as _os

    explicit = _os.environ.get(runtime_manifest.env.repo_run_dir, "").strip()
    if explicit:
        return Path(explicit).expanduser()
    ro = (
        runtime_overrides
        if isinstance(runtime_overrides, dict)
        else _load_runtime_overrides_from_env_or_args()
    )
    yaml_dir = (ro.get("repo_run_dir") if isinstance(ro, dict) else None) or None
    if yaml_dir:
        return Path(str(yaml_dir)).expanduser()
    home = Path(_os.path.expanduser("~"))
    fallback_root = home / runtime_manifest.paths.default_user_repo_run_home
    canonical = fallback_root / runtime_manifest.paths.repo_run_results_elt_relpath
    if fallback_root.exists():
        return canonical
    return None


def _compose_runtime_context(
    *,
    config_path_arg: Path | str | None = None,
    environment_arg: str | None = None,
) -> _RuntimeContext:
    import os as _os

    repo_root = Path(__file__).resolve().parents[2]

    cp_source: str
    config_path_resolved: Path | None
    if config_path_arg:
        config_path_resolved = Path(str(config_path_arg)).resolve()
        cp_source = "arg"
    else:
        env_cp = _os.environ.get("ELT_PIPELINE_CONFIG_PATH", "").strip()
        if env_cp:
            config_path_resolved = Path(env_cp).resolve()
            cp_source = "env"
        else:
            auto_candidate = repo_root / "pipeline.yaml"
            if auto_candidate.is_file():
                config_path_resolved = auto_candidate
                cp_source = "repo_root_auto"
            else:
                config_path_resolved = None
                cp_source = "manifest_fallback"

    if config_path_resolved is not None:
        ro = load_runtime_overrides(str(config_path_resolved), environment=environment_arg)
    else:
        ro = {}

    rrd = _repo_run_dir(runtime_overrides=ro)
    if rrd is not None:
        root_default = str((rrd / "runtime").as_posix())
        wh_default = str((rrd / "warehouse").as_posix())
    else:
        yaml_root = (
            ro.get("cli_default_root_path") if isinstance(ro, dict) else None
        ) or None
        yaml_wh = (
            ro.get("cli_default_warehouse_root") if isinstance(ro, dict) else None
        ) or None
        root_default = (
            str(yaml_root) if yaml_root else _MODULE_MANIFEST_ROOT_PATH_DEFAULT
        )
        wh_default = str(yaml_wh) if yaml_wh else _MODULE_MANIFEST_WH_PATH_DEFAULT

    return _RuntimeContext(
        repo_root=repo_root,
        config_path_resolved=config_path_resolved,
        config_path_source=cp_source,
        environment=environment_arg,
        runtime_overrides=ro,
        cli_default_root_path=root_default,
        cli_default_warehouse_root=wh_default,
    )


def _resolve_defaults_for_repo_run() -> tuple[str, str]:
    target = _repo_run_dir()
    if target is not None:
        root = str((target / "runtime").as_posix())
        warehouse = str((target / "warehouse").as_posix())
        return root, warehouse
    ro = _load_runtime_overrides_from_env_or_args(config_path=None, environment=None)
    yaml_root = (
        ro.get("cli_default_root_path") if isinstance(ro, dict) else None
    ) or None
    yaml_wh = (
        ro.get("cli_default_warehouse_root") if isinstance(ro, dict) else None
    ) or None
    root = str(yaml_root) if yaml_root else _MODULE_MANIFEST_ROOT_PATH_DEFAULT
    wh = str(yaml_wh) if yaml_wh else _MODULE_MANIFEST_WH_PATH_DEFAULT
    return root, wh


_DEFAULT_ROOT_PATH_EVAL, _DEFAULT_WAREHOUSE_ROOT_EVAL = _resolve_defaults_for_repo_run()
_DEFAULT_ROOT_PATH: str = _DEFAULT_ROOT_PATH_EVAL
_DEFAULT_WAREHOUSE_ROOT: str = _DEFAULT_WAREHOUSE_ROOT_EVAL


def _iceberg_effective_enabled(
    args: Any, *, runtime_overrides: dict[str, Any] | None = None
) -> bool | None:
    explicit = getattr(args, "iceberg_enabled", None)
    if explicit is True:
        return True
    if explicit is False:
        return False

    if runtime_context.is_initialized():
        final_val = runtime_context.get("spark.enable_iceberg")
        if final_val is True:
            return True
        if final_val is False:
            return False

    if isinstance(runtime_overrides, dict):
        spark_conf = runtime_overrides.get("spark")
        yaml_value = (
            spark_conf.get("enable_iceberg") if isinstance(spark_conf, dict) else None
        )
        if yaml_value is True:
            return True
        if yaml_value is False:
            return False

    return True


def _get_from_runtime_overrides(
    runtime_overrides: dict[str, Any] | None,
    *override_path: str,
) -> Any:
    node: Any = runtime_overrides
    if not isinstance(node, dict):
        return None
    for key in override_path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _validate_iceberg_catalog_binding(
    args: Any,
    *,
    runtime_overrides: dict[str, Any] | None = None,
) -> None:
    ro: dict[str, Any] = (
        runtime_overrides if isinstance(runtime_overrides, dict) else {}
    )
    has_singleton = runtime_context.is_initialized()

    def _singleton_or(key: str, ro_path: tuple[str, ...], manifest_default: Any) -> Any:
        if has_singleton:
            v = runtime_context.get(key)
            if v not in (None, ""):
                return v
        from_ro = _get_from_runtime_overrides(ro, *ro_path)
        if from_ro not in (None, ""):
            return from_ro
        return manifest_default

    def _cli_or(key_attrs: list[str]) -> Any:
        for a in key_attrs:
            v = getattr(args, a, None)
            if v not in (None, ""):
                return v
        return None

    writer_catalog_type = (
        _cli_or(["iceberg_writer_catalog_type", "iceberg_catalog_type"])
        or _singleton_or(
            "iceberg_writer.catalog_type",
            ("iceberg_writer", "catalog_type"),
            runtime_manifest.catalogs.workstation_default_writer_catalog,
        )
        or runtime_manifest.catalogs.workstation_default_writer_catalog
    )
    if isinstance(writer_catalog_type, str):
        writer_catalog_type = writer_catalog_type.strip().lower()
    serving_catalog_type = (
        _singleton_or(
            "iceberg_serving.catalog_type",
            ("iceberg_serving", "catalog_type"),
            runtime_manifest.catalogs.workstation_default_serving_catalog,
        )
        or runtime_manifest.catalogs.workstation_default_serving_catalog
    )
    if isinstance(serving_catalog_type, str):
        serving_catalog_type = serving_catalog_type.strip().lower()
    catalog_uri = (
        _cli_or(["iceberg_catalog_uri"])
        or _singleton_or(
            "iceberg_serving.catalog_uri",
            ("iceberg_serving", "catalog_uri"),
            "",
        )
        or ""
    )
    hive_metastore_uri = (
        _cli_or(["iceberg_hive_metastore_uri"])
        or _singleton_or(
            "iceberg_writer.hive_metastore_uri",
            ("iceberg_writer", "hive_metastore_uri"),
            "",
        )
        or ""
    )
    writer_valid = set(runtime_manifest.catalogs.writer_catalog_type_valid_values)
    serving_valid = set(runtime_manifest.catalogs.serving_catalog_type_valid_values)
    if writer_catalog_type not in writer_valid:
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.config_invalid,
            message=(
                "Unsupported Iceberg WRITER catalog binding type. "
                f"Supported: {', '.join(sorted(writer_valid))}."
            ),
            retryable=False,
            context={"requested_writer_catalog_type": writer_catalog_type},
        )
    if serving_catalog_type not in serving_valid:
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.config_invalid,
            message=(
                "Unsupported Iceberg SERVING catalog binding type. "
                f"Supported: {', '.join(sorted(serving_valid))}."
            ),
            retryable=False,
            context={"requested_serving_catalog_type": serving_catalog_type},
        )
    if writer_catalog_type in {"jdbc", "rest", "nessie"} and not catalog_uri:
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.config_invalid,
            message=(
                f"Iceberg writer catalog binding requires --iceberg-catalog-uri (or "
                f"`runtime_context.get('iceberg_serving.catalog_uri')`) when "
                f"--iceberg-writer-catalog-type={writer_catalog_type}."
            ),
            retryable=False,
            context={
                "iceberg_writer_catalog_type": writer_catalog_type,
                "provided_uri": bool(catalog_uri),
            },
        )
    if writer_catalog_type == "hive_metastore" and not hive_metastore_uri:
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.config_invalid,
            message=(
                "Iceberg writer catalog binding requires --iceberg-hive-metastore-uri (or "
                "`runtime_context.get('iceberg_writer.hive_metastore_uri')`) when "
                "--iceberg-writer-catalog-type=hive_metastore. Format: thrift://<host>:9083."
            ),
            retryable=False,
            context={
                "iceberg_writer_catalog_type": writer_catalog_type,
                "provided_hive_metastore_uri": bool(hive_metastore_uri),
            },
        )
    if (
        serving_catalog_type in {"jdbc", "rest", "nessie", "snowflake"}
        and not catalog_uri
        and serving_catalog_type != runtime_manifest.catalogs.workstation_default_serving_catalog
    ):
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.config_invalid,
            message=(
                "Iceberg serving catalog binding requires "
                f"`iceberg_serving.catalog_uri` when "
                f"`iceberg_serving.catalog_type`={serving_catalog_type}. "
                "Omit SERVING_CATALOG_TYPE (defaults to jdbc+sqlite workstation) "
                "or provide a catalog URI."
            ),
            retryable=False,
            context={
                "iceberg_serving_catalog_type": serving_catalog_type,
                "provided_uri": bool(catalog_uri),
            },
        )


def _run_catalog_preflight_from_env(
    args: Any,
    *,
    runtime_overrides: dict[str, Any] | None = None,
    stage_label: str = "sql",
) -> None:

    ro: dict[str, Any] = (
        runtime_overrides if isinstance(runtime_overrides, dict) else {}
    )
    has_singleton = runtime_context.is_initialized()

    def _cli(*attrs: str) -> Any:
        for a in attrs:
            v = getattr(args, a, None)
            if v not in (None, ""):
                return v
        return None

    def _final(
        singleton_key: str,
        ro_path: tuple[str, ...],
        manifest_default: Any,
    ) -> Any:
        if has_singleton:
            v = runtime_context.get(singleton_key)
            if v not in (None, ""):
                return v
        from_ro = _get_from_runtime_overrides(ro, *ro_path)
        if from_ro not in (None, ""):
            return from_ro
        return manifest_default

    writer_catalog_type = (
        _cli("iceberg_writer_catalog_type", "iceberg_catalog_type")
        or _final(
            "iceberg_writer.catalog_type",
            ("iceberg_writer", "catalog_type"),
            runtime_manifest.catalogs.workstation_default_writer_catalog,
        )
    )
    if isinstance(writer_catalog_type, str):
        writer_catalog_type = writer_catalog_type.strip().lower()
    serving_catalog_type = _final(
        "iceberg_serving.catalog_type",
        ("iceberg_serving", "catalog_type"),
        runtime_manifest.catalogs.workstation_default_serving_catalog,
    )
    if isinstance(serving_catalog_type, str):
        serving_catalog_type = serving_catalog_type.strip().lower()
    catalog_uri = (
        _cli("iceberg_catalog_uri")
        or _final(
            "iceberg_serving.catalog_uri",
            ("iceberg_serving", "catalog_uri"),
            "",
        )
        or ""
    )
    writer_catalog_uri = (
        _cli("iceberg_catalog_uri")
        or _final(
            "iceberg_writer.catalog_uri",
            ("iceberg_writer", "catalog_uri"),
            "",
        )
        or ""
    )
    rest_token = _final(
        "iceberg_writer.rest_token",
        ("iceberg_writer", "rest_token"),
        "",
    ) or _final(
        "iceberg_serving.rest_token",
        ("iceberg_serving", "rest_token"),
        "",
    )
    hive_metastore_uri = (
        _cli("iceberg_hive_metastore_uri")
        or _final(
            "iceberg_writer.hive_metastore_uri",
            ("iceberg_writer", "hive_metastore_uri"),
            "",
        )
        or ""
    )
    glue_region = (
        _cli("iceberg_glue_region")
        or _final(
            "iceberg_writer.glue_region",
            ("iceberg_writer", "glue_region"),
            "",
        )
        or ""
    )
    warehouse_dir = (
        _cli("iceberg_warehouse_dir")
        or _final(
            "iceberg_writer.warehouse_dir",
            ("iceberg_writer", "warehouse_dir"),
            "",
        )
        or _final(
            "iceberg_serving.warehouse_dir",
            ("iceberg_serving", "warehouse_dir"),
            "",
        )
        or ""
    )
    writer_config: dict[str, Any] = {
        "catalog_uri": writer_catalog_uri or catalog_uri,
        "rest_token": rest_token,
        "hive_metastore_uri": hive_metastore_uri,
        "glue_region": glue_region,
        "warehouse_dir": warehouse_dir,
    }
    serving_config: dict[str, Any] = {
        "catalog_uri": catalog_uri,
        "rest_token": rest_token,
    }
    preflight_conf = load_catalog_preflight_config_from_env()
    mode = preflight_conf["mode"]
    timeout_seconds = preflight_conf["timeout_seconds"]
    if mode == "off" or mode.value == "off":
        return
    try:
        results = run_catalog_preflight(
            writer_catalog_type=writer_catalog_type,
            writer_config=writer_config,
            serving_catalog_type=serving_catalog_type,
            serving_config=serving_config,
            mode=mode,
            timeout_seconds=timeout_seconds,
        )
    except ConfigValidationError:
        raise
    failures = [r for r in results if not r.passed]
    if failures:
        lines = [
            f"[{stage_label}] catalog_preflight WARNING: "
            f"{len(failures)}/{len(results)} check(s) failed "
            f"(mode={mode}, best_effort — continuing before SparkSession boot):"
        ]
        for f in failures:
            lines.append(
                f"  • [{f.binding}] {f.check_name.value}: {f.message}"
            )
        sys.stderr.write("\n".join(lines) + "\n\n")
        sys.stderr.flush()


def _build_serving_endpoint(
    args: Any, *, runtime_overrides: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    ro: dict[str, Any] = (
        runtime_overrides if isinstance(runtime_overrides, dict) else {}
    )
    has_singleton = runtime_context.is_initialized()

    def _cli(*attrs: str) -> Any:
        for a in attrs:
            v = getattr(args, a, None)
            if v not in (None, ""):
                return v
        return None

    def _final(
        singleton_key: str,
        ro_path: tuple[str, ...],
        manifest_default: Any,
    ) -> Any:
        if has_singleton:
            v = runtime_context.get(singleton_key)
            if v not in (None, ""):
                return v
        from_ro = _get_from_runtime_overrides(ro, *ro_path)
        if from_ro not in (None, ""):
            return from_ro
        return manifest_default

    enabled = _iceberg_effective_enabled(args, runtime_overrides=ro)
    if not enabled:
        return None
    catalog_name = (
        _cli("iceberg_catalog_name")
        or _final(
            "iceberg_writer.catalog_name",
            ("iceberg_writer", "catalog_name"),
            runtime_manifest.catalogs.default_catalog_name,
        )
        or _final(
            "iceberg_serving.catalog_name",
            ("iceberg_serving", "catalog_name"),
            runtime_manifest.catalogs.default_catalog_name,
        )
    )
    writer_catalog_type = (
        _cli("iceberg_writer_catalog_type", "iceberg_catalog_type")
        or _final(
            "iceberg_writer.catalog_type",
            ("iceberg_writer", "catalog_type"),
            runtime_manifest.catalogs.workstation_default_writer_catalog,
        )
    )
    if isinstance(writer_catalog_type, str):
        writer_catalog_type = writer_catalog_type.strip().lower()
    serving_catalog_type = (
        _final(
            "iceberg_serving.catalog_type",
            ("iceberg_serving", "catalog_type"),
            runtime_manifest.catalogs.workstation_default_serving_catalog,
        )
    )
    if isinstance(serving_catalog_type, str):
        serving_catalog_type = serving_catalog_type.strip().lower()
    catalog_uri = (
        _cli("iceberg_catalog_uri")
        or _final(
            "iceberg_serving.catalog_uri",
            ("iceberg_serving", "catalog_uri"),
            "",
        )
        or ""
    )
    warehouse_dir = (
        _cli("iceberg_warehouse_dir")
        or _final(
            "iceberg_writer.warehouse_dir",
            ("iceberg_writer", "warehouse_dir"),
            "",
        )
        or ""
    )
    if not warehouse_dir:
        warehouse_root = getattr(args, "warehouse_root", None)
        if warehouse_root:
            warehouse_dir = str(Path(path_normalize(warehouse_root)) / "iceberg")
    trino_version = _final(
        "trino_serving.version",
        ("trino_serving", "version"),
        runtime_manifest.versions.trino_server,
    )
    trino_port = str(
        _final(
            "trino_serving.port",
            ("trino_serving", "port"),
            runtime_manifest.serving.default_trino_port,
        )
    )
    trino_host = _final(
        "trino_serving.host",
        ("trino_serving", "host"),
        runtime_manifest.serving.default_trino_host,
    )
    glue_region = (
        _cli("iceberg_glue_region")
        or _final(
            "iceberg_writer.glue_region",
            ("iceberg_writer", "glue_region"),
            "",
        )
        or ""
    )
    catalog_impl_override = (
        _cli("iceberg_catalog_impl_override")
        or _final(
            "iceberg_writer.catalog_impl_override",
            ("iceberg_writer", "catalog_impl_override"),
            None,
        )
        or _final(
            "iceberg_serving.catalog_impl_override",
            ("iceberg_serving", "catalog_impl_override"),
            None,
        )
    )
    jdbc = f"jdbc:trino://{trino_host}:{trino_port}/{catalog_name}"
    _env_keys_ref = runtime_manifest.env
    catalog_notes = {
        "hadoop": (
            "Filesystem-based writer catalog; local-first, zero-infra dev binding. "
            "Warehouse dir is the local filesystem root. Trino SERVING side bridges "
            "this to jdbc+sqlite (cache-only; data-lake files remain source of truth)."
        ),
        "jdbc": (
            "JDBC-backed catalog (SQLite workstation default, Postgres, MySQL, etc.). "
            f"Requires {_env_keys_ref.iceberg_catalog_uri} (JDBC connection string) "
            "on writer side; serving side auto-generates SQLite URI when omitted."
        ),
        "rest": (
            "REST catalog server (Polaris, Nessie, Lakekeeper, Tabular). "
            f"Requires {_env_keys_ref.iceberg_catalog_uri} (REST endpoint). "
            f"Token via {_env_keys_ref.iceberg_rest_token}."
        ),
        "glue": (
            "AWS Glue Data Catalog (AWS-managed binding). "
            f"Region via --iceberg-glue-region or {_env_keys_ref.iceberg_glue_region}; "
            "credentials from standard AWS SDK chain."
        ),
        "hive_metastore": (
            "Apache Hive Metastore ICEBERG writer catalog (Thrift RPC). "
            f"Requires --iceberg-hive-metastore-uri or {_env_keys_ref.iceberg_hive_metastore_uri} "
            "(format: thrift://<metastore-host>:9083). Writer-only binding; "
            "serving continues via jdbc/rest/nessie/snowflake valid types."
        ),
        "nessie": (
            "Apache Nessie catalog (Git-like versioned branch semantics). "
            f"Configure via {_env_keys_ref.iceberg_serving_catalog_type}=nessie + URI."
        ),
        "snowflake": (
            "Snowflake Iceberg catalog (Snowflake Polaris-backed). "
            f"Configure via {_env_keys_ref.iceberg_serving_catalog_type}=snowflake + URI "
            "and appropriate Snowflake credential env vars."
        ),
    }
    return {
        "table_format": "iceberg",
        "catalog_name": catalog_name,
        "writer_catalog_type": writer_catalog_type,
        "serving_catalog_type": serving_catalog_type,
        "catalog_type_note": catalog_notes.get(serving_catalog_type, ""),
        "writer_catalog_type_note": catalog_notes.get(writer_catalog_type, ""),
        "catalog_uri_provided": bool(catalog_uri),
        "glue_region_provided": bool(glue_region),
        "catalog_impl_override_provided": bool(catalog_impl_override),
        "catalog_impl_override_class": catalog_impl_override or "",
        "catalog_impl_override_note": (
            f"Custom Iceberg SparkCatalog class override in effect: "
            f"{catalog_impl_override}. BOTH spark_catalog (SparkSessionCatalog) "
            "and named iceberg catalog (SparkCatalog) use this class. "
            "Gravitino example: catalog_type=rest + "
            "catalog_impl_override=org.apache.gravitino.iceberg.spark.SparkCatalog + URI."
            if catalog_impl_override
            else (
                "No catalog_impl_override in effect; default "
                + runtime_manifest.classes.iceberg_spark_session_catalog
                + " / "
                + runtime_manifest.classes.iceberg_spark_leaf_catalog
                + " classes used (Apache Iceberg built-in)."
            )
        ),
        "warehouse_dir": warehouse_dir or "",
        "engines": {
            "trino": {
                "version": trino_version,
                "host": trino_host,
                "port": trino_port,
                "jdbc_url": jdbc,
                "driver_class": runtime_manifest.classes.trino_jdbc_driver,
                "script_path": "ops/trino_serving/run_trino.sh",
                "sample_query": (
                    f"SELECT * FROM {catalog_name}.level3.<domain>.<table_name> LIMIT 10"
                ),
                "trino_iceberg_catalog_note": (
                    f"Trino {trino_version} Iceberg connector: fs.hadoop.enabled=true is "
                    "auto-injected (see run_trino.sh) when using file:// scheme (local "
                    "warehouse). See docs/operator/LOCAL_OPERATOR_RUNBOOK.md."
                ),
            },
            "spark_thrift": {
                "note": (
                    "Use spark.sql.catalog." + catalog_name + " with Spark Thrift "
                    "server sharing the warehouse_dir."
                ),
            },
            "athena": {
                "binding_doc": (
                    "docs/operator/LOCAL_OPERATOR_RUNBOOK.md (AWS Athena binding)"
                ),
                "note": (
                    "Managed Trino-compatible engine; register catalog + "
                    "point warehouse_dir at same S3 prefix."
                ),
            },
            "duckdb": {
                "note": (
                    "Attach via iceberg extension and the same catalog/warehouse_dir."
                ),
            },
        },
    }


def _resolve_iceberg_session_kwargs(
    *, args: Any, app_name: str, runtime_overrides: dict[str, Any] | None = None
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"app_name": app_name}
    if runtime_overrides:
        kwargs["runtime_overrides"] = runtime_overrides
    enabled = _iceberg_effective_enabled(args)
    if enabled is None:
        return kwargs
    kwargs["iceberg_enabled"] = enabled

    ro = runtime_overrides if isinstance(runtime_overrides, dict) else {}
    writer_conf = (
        ro.get("iceberg_writer", {}) if isinstance(ro.get("iceberg_writer"), dict) else {}
    )
    serving_conf = (
        ro.get("iceberg_serving", {}) if isinstance(ro.get("iceberg_serving"), dict) else {}
    )

    def _pick(
        *,
        argname: str,
        singleton_keys: tuple[str, ...],
        runtime_subkey: str | None,
        runtime_conf: dict | None,
    ):
        val = getattr(args, argname, None)
        if val:
            return val
        for skey in singleton_keys:
            sv = runtime_context.get(skey)
            if sv is not None and sv != "":
                return sv
        if runtime_subkey and runtime_conf:
            rval = runtime_conf.get(runtime_subkey)
            if rval not in (None, ""):
                return rval
        return None

    catalog_name = _pick(
        argname="iceberg_catalog_name",
        singleton_keys=("iceberg_writer.catalog_name", "iceberg_serving.catalog_name"),
        runtime_subkey="catalog_name",
        runtime_conf=writer_conf or serving_conf,
    )
    catalog_type_arg = getattr(args, "iceberg_writer_catalog_type", None) or getattr(
        args, "iceberg_catalog_type", None
    )
    _ct_singleton = runtime_context.get("iceberg_writer.catalog_type")
    if _ct_singleton in (None, ""):
        _ct_singleton = runtime_context.get("iceberg_serving.catalog_type")
    catalog_type = (
        catalog_type_arg
        or (_ct_singleton if _ct_singleton not in (None, "") else None)
        or writer_conf.get("catalog_type")
        or serving_conf.get("catalog_type")
        or None
    )
    catalog_uri = _pick(
        argname="iceberg_catalog_uri",
        singleton_keys=("iceberg_writer.catalog_uri", "iceberg_serving.catalog_uri"),
        runtime_subkey="catalog_uri",
        runtime_conf=serving_conf,
    )
    warehouse_dir = _pick(
        argname="iceberg_warehouse_dir",
        singleton_keys=("iceberg_writer.warehouse_dir", "iceberg_serving.warehouse_dir"),
        runtime_subkey="warehouse_dir",
        runtime_conf=writer_conf,
    )
    rest_token = _pick(
        argname="iceberg_rest_token",
        singleton_keys=("iceberg_writer.rest_token", "iceberg_serving.rest_token"),
        runtime_subkey="rest_token",
        runtime_conf=serving_conf,
    )
    rest_warehouse = _pick(
        argname="iceberg_rest_warehouse",
        singleton_keys=("iceberg_writer.rest_warehouse", "iceberg_serving.rest_warehouse"),
        runtime_subkey="rest_warehouse",
        runtime_conf=serving_conf,
    )
    glue_region = _pick(
        argname="iceberg_glue_region",
        singleton_keys=("iceberg_writer.glue_region", "iceberg_serving.glue_region"),
        runtime_subkey="glue_region",
        runtime_conf=serving_conf,
    )
    hive_metastore_uri = _pick(
        argname="iceberg_hive_metastore_uri",
        singleton_keys=("iceberg_writer.hive_metastore_uri",),
        runtime_subkey="hive_metastore_uri",
        runtime_conf=writer_conf,
    )
    catalog_impl_override = _pick(
        argname="iceberg_catalog_impl_override",
        singleton_keys=(
            "iceberg_writer.catalog_impl_override",
            "iceberg_serving.catalog_impl_override",
        ),
        runtime_subkey="catalog_impl_override",
        runtime_conf=writer_conf,
    )
    if not warehouse_dir and enabled:
        warehouse_root = getattr(args, "warehouse_root", None)
        if warehouse_root:
            warehouse_dir = str(Path(path_normalize(warehouse_root)) / "iceberg")
    if catalog_name:
        kwargs["iceberg_catalog_name"] = catalog_name
    if catalog_type:
        kwargs["iceberg_catalog_type"] = catalog_type
    if catalog_uri:
        kwargs["iceberg_catalog_uri"] = catalog_uri
    if warehouse_dir:
        kwargs["iceberg_warehouse_dir"] = warehouse_dir
    if rest_token:
        kwargs["iceberg_rest_token"] = rest_token
    if rest_warehouse:
        kwargs["iceberg_rest_warehouse"] = rest_warehouse
    if glue_region:
        kwargs["iceberg_glue_region"] = glue_region
    if hive_metastore_uri:
        kwargs["iceberg_hive_metastore_uri"] = hive_metastore_uri
    if catalog_impl_override:
        kwargs["iceberg_catalog_impl_override"] = catalog_impl_override
    return kwargs

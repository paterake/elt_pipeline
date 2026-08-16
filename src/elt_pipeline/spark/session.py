from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from elt_pipeline.config import runtime_context
from elt_pipeline.config.runtime_manifest import runtime_manifest

_MASTER_ENV_VAR = runtime_manifest.env.spark_master
_DEFAULT_MASTER = runtime_manifest.spark.default_master

_ICEBERG_ENABLED_ENV_VAR = runtime_manifest.env.iceberg_enabled
_ICEBERG_CATALOG_NAME_ENV_VAR = runtime_manifest.env.iceberg_catalog_name
_DEFAULT_ICEBERG_CATALOG_NAME = runtime_manifest.catalogs.default_catalog_name
_ICEBERG_WAREHOUSE_ENV_VAR = runtime_manifest.env.iceberg_warehouse_dir
_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR = runtime_manifest.env.iceberg_writer_catalog_type
_ICEBERG_CATALOG_TYPE_ENV_VAR = runtime_manifest.env.iceberg_catalog_type_legacy
_DEFAULT_ICEBERG_WRITER_CATALOG_TYPE = runtime_manifest.catalogs.workstation_default_writer_catalog
_ICEBERG_CATALOG_URI_ENV_VAR = runtime_manifest.env.iceberg_catalog_uri
_ICEBERG_REST_TOKEN_ENV_VAR = runtime_manifest.env.iceberg_rest_token
_ICEBERG_REST_WAREHOUSE_ENV_VAR = runtime_manifest.env.iceberg_rest_warehouse
_ICEBERG_GLUE_REGION_ENV_VAR = runtime_manifest.env.iceberg_glue_region
_IVY_HOME_ENV_VAR = runtime_manifest.env.ivy_home
_DEFAULT_IVY_HOME_RELPATH = runtime_manifest.paths.spark_ivy_relpath
_JDBC_JARS_EXTRA_ENV_VAR = runtime_manifest.env.iceberg_jdbc_jars_extra
_JDBC_DRIVER_ENV_VAR = runtime_manifest.env.iceberg_jdbc_driver
_JDBC_SCHEMA_VERSION_ENV_VAR = runtime_manifest.env.iceberg_jdbc_schema_version


def _resolve_final(*, singleton_key: str, env_var: str, manifest_default: Any) -> Any:
    """Return final value: singleton (Mercell/Camellos) > os.environ > manifest_floor.

    The singleton is the primary (materialized once at entry point); os.environ
    only kicks in for direct API callers that haven't gone through main()
    (back-compat); manifest always the floor.

    **Note:** Callers should still prefer explicit params as the absolute
    highest tier; this function covers the remaining tiers (2-4).
    """
    if runtime_context.is_initialized():
        v = runtime_context.get(singleton_key)
        if v not in (None, ""):
            return v
    if env_var:
        env_val = os.environ.get(env_var)
        if env_val not in (None, ""):
            return env_val
    return manifest_default


def _iceberg_enabled() -> bool:
    final = _resolve_final(
        singleton_key="spark.enable_iceberg",
        env_var=_ICEBERG_ENABLED_ENV_VAR,
        manifest_default="true",
    )
    raw = str(final).strip().lower()
    if raw in {"", "1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    msg = (
        f"Unrecognized value for spark.enable_iceberg: {raw!r}. "
        "Use 'true' (default) or 'false'."
    )
    raise ValueError(msg)


def _resolve_ivy_home() -> str:
    configured = os.environ.get(_IVY_HOME_ENV_VAR, "").strip()
    if configured:
        return configured
    cwd = Path.cwd()
    default = cwd / _DEFAULT_IVY_HOME_RELPATH
    return str(default.resolve())


def build_spark_session(
    app_name: str | None = None,
    master: str | None = None,
    iceberg_enabled: bool | None = None,
    iceberg_catalog_name: str | None = None,
    iceberg_catalog_type: str | None = None,
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_dir: str | None = None,
    iceberg_rest_token: str | None = None,
    iceberg_rest_warehouse: str | None = None,
    iceberg_glue_region: str | None = None,
    runtime_overrides: dict[str, Any] | None = None,
) -> SparkSession:
    """Build a SparkSession, wiring the Iceberg V2 DataSource when iceberg_enabled.

    Mercell/Camellos 4-tier precedence (highest to lowest):
      1. Explicit function parameters (caller-injected).
      2. runtime_context singleton — materialized once at main() entry point.
      3. ``runtime_overrides`` dict (transitionary: explicit-pass back-compat).
      4. ``os.environ`` ELT_PIPELINE_* — back-compat for direct API callers.
      5. Frozen defaults from ``runtime_manifest`` (single floor of truth).

    The singleton (tier 2) is the primary path for normal framework runs;
    tiers 3+ only kick in for direct API callers that haven't gone through
    the main() bootstrap (back-compat during transition).
    """
    ro = (runtime_overrides or {}).copy()
    writer_conf = (
        ro.get("iceberg_writer", {}) if isinstance(ro.get("iceberg_writer"), dict) else {}
    )
    serving_conf = (
        ro.get("iceberg_serving", {}) if isinstance(ro.get("iceberg_serving"), dict) else {}
    )
    _ = serving_conf  # Spark WRITER builder uses iceberg_writer; serving uses Trino.

    def _resolve(
        param: Any,
        *,
        singleton_key: str,
        env_var: str = "",
        override_path: tuple[str, ...] | None = None,
    ):
        """Mercell/Camellos precedence:
        param > SINGLETON (final) > ro dict > os.environ > manifest_default.
        """
        if param is not None and not (isinstance(param, str) and param == ""):
            return param
        # Singleton (materialized once at entry) = the canonical single-source tier
        if runtime_context.is_initialized():
            sv = runtime_context.get(singleton_key)
            if sv is not None and sv != "":
                return sv
        # Explicit-pass runtime_overrides dict (legacy transition)
        if override_path:
            node: Any = ro
            for key in override_path:
                if isinstance(node, dict):
                    node = node.get(key)
                else:
                    node = None
            if node is not None and node != "":
                return node
        # os.environ back-compat for direct API callers (not through main())
        if env_var:
            env_val = os.environ.get(env_var)
            if env_val is not None and env_val != "":
                return env_val
        return None

    resolved_app_name = (
        _resolve(app_name, singleton_key="spark.app_name", override_path=("spark", "app_name"))
        or runtime_manifest.spark.default_app_name
    )
    resolved_master = (
        _resolve(
            master,
            singleton_key="spark.master",
            env_var=_MASTER_ENV_VAR,
            override_path=("spark", "master"),
        )
        or _DEFAULT_MASTER
    )

    driver_host = _resolve(
        None,
        singleton_key="spark.driver_host",
        override_path=("spark", "driver_host"),
    )
    if driver_host is None:
        driver_host = runtime_manifest.spark.default_driver_host
    driver_bind = _resolve(
        None,
        singleton_key="spark.driver_bind_address",
        override_path=("spark", "driver_bind_address"),
    )
    if driver_bind is None:
        driver_bind = runtime_manifest.spark.default_driver_bind_address
    shuffle_partitions = _resolve(
        None,
        singleton_key="spark.shuffle_partitions",
        override_path=("spark", "shuffle_partitions"),
    )
    if shuffle_partitions is None:
        shuffle_partitions = runtime_manifest.spark.default_shuffle_partitions
    default_parallelism = _resolve(
        None,
        singleton_key="spark.default_parallelism",
        override_path=("spark", "default_parallelism"),
    )
    if default_parallelism is None:
        default_parallelism = runtime_manifest.spark.default_parallelism
    aqe_enabled = _resolve(
        None,
        singleton_key="spark.adaptive_query_execution",
        override_path=("spark", "adaptive_query_execution"),
    )
    if aqe_enabled is None:
        aqe_enabled = runtime_manifest.spark.default_adaptive_enabled

    builder = (
        SparkSession.builder.appName(resolved_app_name)
        .master(resolved_master)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", driver_bind)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config(
            "spark.sql.adaptive.enabled",
            str(bool(aqe_enabled)).lower(),
        )
        .config("spark.default.parallelism", default_parallelism)
    )

    use_iceberg = iceberg_enabled
    if use_iceberg is None:
        from_conf = _resolve(
            None,
            singleton_key="spark.enable_iceberg",
            env_var=_ICEBERG_ENABLED_ENV_VAR,
            override_path=("spark", "enable_iceberg"),
        )
        if from_conf is None:
            use_iceberg = _iceberg_enabled()
        else:
            use_iceberg = str(from_conf).strip().lower() in {"1", "true", "yes", "on"}
    if use_iceberg:
        os.environ[_ICEBERG_ENABLED_ENV_VAR] = "true"
        w_warehouse = (
            _resolve(
                iceberg_warehouse_dir,
                singleton_key="iceberg_writer.warehouse_dir",
                env_var=_ICEBERG_WAREHOUSE_ENV_VAR,
                override_path=("iceberg_writer", "warehouse_dir"),
            )
            or _resolve(
                iceberg_warehouse_dir,
                singleton_key="iceberg_serving.warehouse_dir",
                env_var=_ICEBERG_WAREHOUSE_ENV_VAR,
                override_path=("iceberg_serving", "warehouse_dir"),
            )
            or None
        )
        if w_warehouse:
            os.environ[_ICEBERG_WAREHOUSE_ENV_VAR] = str(w_warehouse)
        cname = (
            _resolve(
                iceberg_catalog_name,
                singleton_key="iceberg_writer.catalog_name",
                env_var=_ICEBERG_CATALOG_NAME_ENV_VAR,
                override_path=("iceberg_writer", "catalog_name"),
            )
            or _resolve(
                iceberg_catalog_name,
                singleton_key="iceberg_serving.catalog_name",
                env_var=_ICEBERG_CATALOG_NAME_ENV_VAR,
                override_path=("iceberg_serving", "catalog_name"),
            )
            or None
        )
        if cname:
            os.environ[_ICEBERG_CATALOG_NAME_ENV_VAR] = str(cname)
        ctype_param = _resolve(
            iceberg_catalog_type,
            singleton_key="iceberg_writer.catalog_type",
            env_var=_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR,
            override_path=("iceberg_writer", "catalog_type"),
        )
        if ctype_param is None:
            ctype_param = _resolve(
                None,
                singleton_key="iceberg_writer.catalog_type",
                env_var=_ICEBERG_CATALOG_TYPE_ENV_VAR,
                override_path=("iceberg_writer", "catalog_type"),
            )
        if ctype_param is not None and str(ctype_param).strip():
            os.environ[_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR] = str(ctype_param).lower()
        ivy_home = Path(_resolve_ivy_home())
        (ivy_home / "cache").mkdir(parents=True, exist_ok=True)
        (ivy_home / "jars").mkdir(parents=True, exist_ok=True)
        os.environ["IVY_HOME"] = str(ivy_home)
        builder = builder.config("spark.jars.ivy", str(ivy_home))
        builder = builder.config(
            "spark.sql.extensions",
            runtime_manifest.classes.iceberg_spark_extensions,
        )
        catalog_name = (
            _resolve(
                iceberg_catalog_name,
                singleton_key="iceberg_writer.catalog_name",
                env_var=_ICEBERG_CATALOG_NAME_ENV_VAR,
                override_path=("iceberg_writer", "catalog_name"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_name",
                env_var="",
                override_path=("iceberg_serving", "catalog_name"),
            )
            or _DEFAULT_ICEBERG_CATALOG_NAME
        )
        ctype_a = _resolve(
            iceberg_catalog_type,
            singleton_key="iceberg_writer.catalog_type",
            env_var=_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR,
            override_path=("iceberg_writer", "catalog_type"),
        )
        ctype_b = _resolve(
            None,
            singleton_key="iceberg_writer.catalog_type",
            env_var=_ICEBERG_CATALOG_TYPE_ENV_VAR,
            override_path=("iceberg_writer", "catalog_type"),
        )
        ctype_default = writer_conf.get("catalog_type") or _DEFAULT_ICEBERG_WRITER_CATALOG_TYPE
        catalog_type = (ctype_a or ctype_b or ctype_default).lower()
        if catalog_type not in runtime_manifest.catalogs.writer_catalog_type_valid_values:
            valid = ", ".join(runtime_manifest.catalogs.writer_catalog_type_valid_values)
            raise ValueError(
                f"Unsupported iceberg_writer.catalog_type={catalog_type}. "
                f"Supported: {valid}"
            )
        catalog_uri = (
            _resolve(
                iceberg_catalog_uri,
                singleton_key="iceberg_writer.catalog_uri",
                env_var=_ICEBERG_CATALOG_URI_ENV_VAR,
                override_path=("iceberg_serving", "catalog_uri"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_uri",
                env_var="",
                override_path=("iceberg_serving", "catalog_uri"),
            )
        )
        resolved_warehouse = (
            _resolve(
                iceberg_warehouse_dir,
                singleton_key="iceberg_writer.warehouse_dir",
                env_var=_ICEBERG_WAREHOUSE_ENV_VAR,
                override_path=("iceberg_writer", "warehouse_dir"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.warehouse_dir",
                env_var="",
                override_path=("iceberg_serving", "warehouse_dir"),
            )
        )
        rest_token = (
            _resolve(
                iceberg_rest_token,
                singleton_key="iceberg_writer.rest_token",
                env_var=_ICEBERG_REST_TOKEN_ENV_VAR,
                override_path=("iceberg_serving", "rest_token"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_uri",
                env_var="",
                override_path=("iceberg_serving", "rest_token"),
            )
        )
        rest_warehouse = _resolve(
            iceberg_rest_warehouse,
            singleton_key="iceberg_writer.rest_warehouse",
            env_var=_ICEBERG_REST_WAREHOUSE_ENV_VAR,
            override_path=("iceberg_serving", "rest_warehouse"),
        )
        glue_region = _resolve(
            iceberg_glue_region,
            singleton_key="iceberg_writer.glue_region",
            env_var=_ICEBERG_GLUE_REGION_ENV_VAR,
            override_path=("iceberg_serving", "glue_region"),
        )

        spark_catalog_class = runtime_manifest.classes.iceberg_spark_session_catalog
        leaf_catalog_class = runtime_manifest.classes.iceberg_spark_leaf_catalog

        base_packages = [runtime_manifest.versions.iceberg_spark_runtime_maven_coord]
        if catalog_type == "jdbc":
            extra = _resolve(
                None,
                singleton_key="iceberg_serving.jdbc_jars_extra",
                env_var=_JDBC_JARS_EXTRA_ENV_VAR,
                override_path=None,
            )
            extra = str(extra).strip() if extra else ""
            if extra:
                base_packages.extend([p for p in extra.split(",") if p.strip()])
        builder = builder.config(
            "spark.jars.packages",
            ",".join(base_packages),
        )

        if catalog_type == "hadoop":
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_catalog_class,
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.type",
                "hadoop",
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                leaf_catalog_class,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.type",
                "hadoop",
            )
            if resolved_warehouse:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.warehouse",
                    resolved_warehouse,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    resolved_warehouse,
                )
        elif catalog_type == "jdbc":
            if not catalog_uri:
                raise ValueError(
                    f"{_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR}=jdbc requires "
                    f"iceberg_catalog_uri (env var {_ICEBERG_CATALOG_URI_ENV_VAR})"
                )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_catalog_class,
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.type",
                "jdbc",
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.uri",
                catalog_uri,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                leaf_catalog_class,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.type",
                "jdbc",
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.uri",
                catalog_uri,
            )
            jdbc_driver = _resolve(
                None,
                singleton_key="iceberg_serving.jdbc_driver",
                env_var=_JDBC_DRIVER_ENV_VAR,
                override_path=None,
            )
            schema_version = _resolve(
                None,
                singleton_key="iceberg_serving.jdbc_schema_version",
                env_var=_JDBC_SCHEMA_VERSION_ENV_VAR,
                override_path=None,
            )
            schema_version = schema_version or "V1"
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.jdbc.schema-version",
                schema_version,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.jdbc.schema-version",
                schema_version,
            )
            if jdbc_driver:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.jdbc.driver",
                    jdbc_driver,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.jdbc.driver",
                    jdbc_driver,
                )
            if resolved_warehouse:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.warehouse",
                    resolved_warehouse,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    resolved_warehouse,
                )
        elif catalog_type == "rest":
            if not catalog_uri:
                raise ValueError(
                    f"{_ICEBERG_WRITER_CATALOG_TYPE_ENV_VAR}=rest requires "
                    f"iceberg_catalog_uri (env var {_ICEBERG_CATALOG_URI_ENV_VAR})"
                )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_catalog_class,
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.type",
                "rest",
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.uri",
                catalog_uri,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                leaf_catalog_class,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.type",
                "rest",
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.uri",
                catalog_uri,
            )
            if rest_token:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.token",
                    rest_token,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.token",
                    rest_token,
                )
            rest_wh = rest_warehouse or resolved_warehouse
            if rest_wh:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.warehouse",
                    rest_wh,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    rest_wh,
                )
        elif catalog_type == "glue":
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_catalog_class,
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.type",
                "glue",
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                leaf_catalog_class,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.type",
                "glue",
            )
            if glue_region:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.glue.region",
                    glue_region,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.glue.region",
                    glue_region,
                )
            if resolved_warehouse:
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog.warehouse",
                    resolved_warehouse,
                )
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    resolved_warehouse,
                )

    return builder.getOrCreate()

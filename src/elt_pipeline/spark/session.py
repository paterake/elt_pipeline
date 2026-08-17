from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from elt_pipeline.config import runtime_context
from elt_pipeline.config.runtime_manifest import runtime_manifest

_DEFAULT_MASTER = runtime_manifest.spark.default_master

_DEFAULT_ICEBERG_CATALOG_NAME = runtime_manifest.catalogs.default_catalog_name
_DEFAULT_ICEBERG_WRITER_CATALOG_TYPE = (
    runtime_manifest.catalogs.workstation_default_writer_catalog
)


def _iceberg_enabled() -> bool:
    """Return True if Iceberg is enabled: singleton > manifest floor.

    The singleton (materialized once at entry point via runtime_context) is
    the ONLY config source. Direct API callers who skip main() still get
    env-var resolution through the singleton's lazy _ensure() bootstrap —
    the same single materializer, never a scattered os.environ read.
    """
    final = runtime_context.get("spark.enable_iceberg")
    if final is None or final == "":
        final = "true"
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
    """Resolve ivy_home strictly through the runtime_context singleton.

    The singleton materializer handles all tiers (ENV > YAML > cwd/.cache/ivy2)
    in exactly ONE place. No direct os.environ reads here.
    """
    configured = runtime_context.get("spark.ivy_home")
    if configured and str(configured).strip():
        return str(configured).strip()
    cwd = Path.cwd()
    default = cwd / runtime_manifest.paths.spark_ivy_relpath
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
    iceberg_hive_metastore_uri: str | None = None,
    iceberg_catalog_impl_override: str | None = None,
    runtime_overrides: dict[str, Any] | None = None,
) -> SparkSession:
    """Build a SparkSession, wiring the Iceberg V2 DataSource when iceberg_enabled.

    4-tier precedence (highest to lowest, single cascade):
      1. Explicit function parameters (caller-injected).
      2. runtime_context singleton — materialized ONCE at main() entry point.
         This singleton materializer is the ONLY place env/YAML/manifest
         cascade is applied; direct API callers who skip main() still get
         the full cascade through the singleton's lazy _ensure() bootstrap.
      3. ``runtime_overrides`` dict (transitionary explicit-pass back-compat).
      4. Frozen defaults from ``runtime_manifest`` (single floor of truth).

    **Zero os.environ reads here.** All environmental resolution flows through
    the singleton materializer — one writer, many readers, zero drift.
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
        override_path: tuple[str, ...] | None = None,
    ):
        """Single-cascade precedence:
        param > SINGLETON (final via runtime_context) > ro dict > None.

        The singleton's lazy bootstrap (``_ensure()``) handles the full
        4-tier cascade (arg > ENV > YAML > manifest) ONCE; callers that
        skip ``main()`` still get env-var resolution through this single
        materializer path — no duplicated os.environ reads here.
        """
        if param is not None and not (isinstance(param, str) and param == ""):
            return param
        sv = runtime_context.get(singleton_key)
        if sv is not None and sv != "":
            return sv
        if override_path:
            node: Any = ro
            for key in override_path:
                if isinstance(node, dict):
                    node = node.get(key)
                else:
                    node = None
            if node is not None and node != "":
                return node
        return None

    resolved_app_name = (
        _resolve(app_name, singleton_key="spark.app_name", override_path=("spark", "app_name"))
        or runtime_manifest.spark.default_app_name
    )
    resolved_master = (
        _resolve(
            master,
            singleton_key="spark.master",
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

    jdk23_sm_flags = "-Djava.security.manager=allow -Djdk.security.allowAllPermissions=true"
    builder = (
        SparkSession.builder.appName(resolved_app_name)
        .master(resolved_master)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", driver_bind)
        .config("spark.driver.extraJavaOptions", jdk23_sm_flags)
        .config("spark.executor.extraJavaOptions", jdk23_sm_flags)
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
            override_path=("spark", "enable_iceberg"),
        )
        if from_conf is None:
            use_iceberg = _iceberg_enabled()
        else:
            use_iceberg = str(from_conf).strip().lower() in {"1", "true", "yes", "on"}
    if use_iceberg:
        ctype_param = _resolve(
            iceberg_catalog_type,
            singleton_key="iceberg_writer.catalog_type",
            override_path=("iceberg_writer", "catalog_type"),
        )
        if ctype_param is None:
            ctype_param = _resolve(
                None,
                singleton_key="iceberg_writer.catalog_type",
                override_path=("iceberg_writer", "catalog_type"),
            )
        ivy_home = Path(_resolve_ivy_home())
        (ivy_home / "cache").mkdir(parents=True, exist_ok=True)
        (ivy_home / "jars").mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.jars.ivy", str(ivy_home))
        builder = builder.config(
            "spark.sql.extensions",
            runtime_manifest.classes.iceberg_spark_extensions,
        )
        catalog_name = (
            _resolve(
                iceberg_catalog_name,
                singleton_key="iceberg_writer.catalog_name",
                override_path=("iceberg_writer", "catalog_name"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_name",
                override_path=("iceberg_serving", "catalog_name"),
            )
            or _DEFAULT_ICEBERG_CATALOG_NAME
        )
        ctype_a = _resolve(
            iceberg_catalog_type,
            singleton_key="iceberg_writer.catalog_type",
            override_path=("iceberg_writer", "catalog_type"),
        )
        ctype_b = _resolve(
            None,
            singleton_key="iceberg_writer.catalog_type",
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
                override_path=("iceberg_serving", "catalog_uri"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_uri",
                override_path=("iceberg_serving", "catalog_uri"),
            )
        )
        resolved_warehouse = (
            _resolve(
                iceberg_warehouse_dir,
                singleton_key="iceberg_writer.warehouse_dir",
                override_path=("iceberg_writer", "warehouse_dir"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.warehouse_dir",
                override_path=("iceberg_serving", "warehouse_dir"),
            )
        )
        rest_token = (
            _resolve(
                iceberg_rest_token,
                singleton_key="iceberg_writer.rest_token",
                override_path=("iceberg_serving", "rest_token"),
            )
            or _resolve(
                None,
                singleton_key="iceberg_serving.catalog_uri",
                override_path=("iceberg_serving", "rest_token"),
            )
        )
        rest_warehouse = _resolve(
            iceberg_rest_warehouse,
            singleton_key="iceberg_writer.rest_warehouse",
            override_path=("iceberg_serving", "rest_warehouse"),
        )
        glue_region = _resolve(
            iceberg_glue_region,
            singleton_key="iceberg_writer.glue_region",
            override_path=("iceberg_serving", "glue_region"),
        )
        hive_metastore_uri = _resolve(
            iceberg_hive_metastore_uri,
            singleton_key="iceberg_writer.hive_metastore_uri",
            override_path=("iceberg_serving", "catalog_uri"),
        )
        catalog_impl_override = _resolve(
            iceberg_catalog_impl_override,
            singleton_key="iceberg_writer.catalog_impl_override",
            override_path=("iceberg_serving", "catalog_impl_override"),
        )
        #
        # Gravitino example: catalog_type=rest +
        #   catalog_impl_override=org.apache.gravitino.iceberg.spark.SparkCatalog + URI.
        # Generic override — applies to BOTH the SparkSessionCatalog (spark_catalog)
        # and the leaf SparkCatalog (named <catalog_name>). No vendor branches.
        spark_catalog_class = (
            catalog_impl_override
            or runtime_manifest.classes.iceberg_spark_session_catalog
        )
        leaf_catalog_class = (
            catalog_impl_override or runtime_manifest.classes.iceberg_spark_leaf_catalog
        )
        if catalog_type == "nessie":
            catalog_type = "rest"

        base_packages = [runtime_manifest.versions.iceberg_spark_runtime_maven_coord]
        if catalog_type == "jdbc":
            extra = _resolve(
                None,
                singleton_key="iceberg_serving.jdbc_jars_extra",
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
                    "iceberg_writer.catalog_type=jdbc requires "
                    "iceberg_catalog_uri (config key iceberg_writer.catalog_uri "
                    "or env var ELT_PIPELINE_ICEBERG_CATALOG_URI)"
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
                override_path=None,
            )
            schema_version = _resolve(
                None,
                singleton_key="iceberg_serving.jdbc_schema_version",
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
                    "iceberg_writer.catalog_type=rest requires "
                    "iceberg_catalog_uri (config key iceberg_writer.catalog_uri "
                    "or env var ELT_PIPELINE_ICEBERG_CATALOG_URI)"
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
        elif catalog_type == "hive_metastore":
            if not hive_metastore_uri:
                raise ValueError(
                    "iceberg_writer.catalog_type=hive_metastore requires "
                    "iceberg_hive_metastore_uri (config key iceberg_writer.hive_metastore_uri "
                    "or env var ELT_PIPELINE_ICEBERG_HIVE_METASTORE_URI). "
                    "Format: thrift://<metastore-host>:9083"
                )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog",
                spark_catalog_class,
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.type",
                "hive_metastore",
            )
            builder = builder.config(
                "spark.sql.catalog.spark_catalog.uri",
                hive_metastore_uri,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                leaf_catalog_class,
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.type",
                "hive_metastore",
            )
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.uri",
                hive_metastore_uri,
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

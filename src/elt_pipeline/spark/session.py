from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

_MASTER_ENV_VAR = "ELT_PIPELINE_SPARK_MASTER"
_DEFAULT_MASTER = "local[*]"

_ICEBERG_ENABLED_ENV_VAR = "ELT_PIPELINE_ICEBERG_ENABLED"
_ICEBERG_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
_ICEBERG_CATALOG_NAME_ENV_VAR = "ELT_PIPELINE_ICEBERG_CATALOG_NAME"
_DEFAULT_ICEBERG_CATALOG_NAME = "iceberg"
_ICEBERG_WAREHOUSE_ENV_VAR = "ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR"
_ICEBERG_CATALOG_TYPE_ENV_VAR = "ELT_PIPELINE_ICEBERG_CATALOG_TYPE"
_DEFAULT_ICEBERG_CATALOG_TYPE = "hadoop"
_ICEBERG_CATALOG_URI_ENV_VAR = "ELT_PIPELINE_ICEBERG_CATALOG_URI"
_ICEBERG_REST_TOKEN_ENV_VAR = "ELT_PIPELINE_ICEBERG_REST_TOKEN"
_ICEBERG_REST_WAREHOUSE_ENV_VAR = "ELT_PIPELINE_ICEBERG_REST_WAREHOUSE"
_ICEBERG_GLUE_REGION_ENV_VAR = "ELT_PIPELINE_ICEBERG_GLUE_REGION"
_IVY_HOME_ENV_VAR = "ELT_PIPELINE_IVY_HOME"
_DEFAULT_IVY_HOME_RELPATH = ".cache/ivy2"


def _iceberg_enabled() -> bool:
    return os.environ.get(_ICEBERG_ENABLED_ENV_VAR, "false").lower() in (
        "true",
        "1",
        "yes",
        "on",
    )


def _resolve_ivy_home() -> str:
    explicit = os.environ.get(_IVY_HOME_ENV_VAR)
    if explicit:
        return explicit
    cwd = Path.cwd()
    default = cwd / _DEFAULT_IVY_HOME_RELPATH
    return str(default.resolve())


def build_spark_session(
    *,
    app_name: str,
    master: str | None = None,
    iceberg_enabled: bool | None = None,
    iceberg_warehouse_dir: str | None = None,
    iceberg_catalog_name: str | None = None,
    iceberg_catalog_type: str | None = None,
    iceberg_catalog_uri: str | None = None,
    iceberg_rest_token: str | None = None,
    iceberg_rest_warehouse: str | None = None,
    iceberg_glue_region: str | None = None,
) -> SparkSession:
    resolved_master = master or os.environ.get(_MASTER_ENV_VAR) or _DEFAULT_MASTER

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    use_iceberg = iceberg_enabled if iceberg_enabled is not None else _iceberg_enabled()
    if use_iceberg:
        os.environ[_ICEBERG_ENABLED_ENV_VAR] = "true"
        if iceberg_warehouse_dir:
            os.environ[_ICEBERG_WAREHOUSE_ENV_VAR] = iceberg_warehouse_dir
        if iceberg_catalog_name:
            os.environ[_ICEBERG_CATALOG_NAME_ENV_VAR] = iceberg_catalog_name
        if iceberg_catalog_type:
            os.environ[_ICEBERG_CATALOG_TYPE_ENV_VAR] = iceberg_catalog_type.lower()
        ivy_home = Path(_resolve_ivy_home())
        (ivy_home / "cache").mkdir(parents=True, exist_ok=True)
        (ivy_home / "jars").mkdir(parents=True, exist_ok=True)
        os.environ["IVY_HOME"] = str(ivy_home)
        builder = builder.config("spark.jars.ivy", str(ivy_home))
        builder = builder.config(
            "spark.jars.packages",
            _ICEBERG_RUNTIME_PACKAGE,
        )
        builder = builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        catalog_name = (
            iceberg_catalog_name
            or os.environ.get(_ICEBERG_CATALOG_NAME_ENV_VAR)
            or _DEFAULT_ICEBERG_CATALOG_NAME
        )
        catalog_type = (
            iceberg_catalog_type
            or os.environ.get(_ICEBERG_CATALOG_TYPE_ENV_VAR)
            or _DEFAULT_ICEBERG_CATALOG_TYPE
        ).lower()
        catalog_uri = iceberg_catalog_uri or os.environ.get(_ICEBERG_CATALOG_URI_ENV_VAR)
        resolved_warehouse = (
            iceberg_warehouse_dir
            or os.environ.get(_ICEBERG_WAREHOUSE_ENV_VAR)
        )
        rest_token = iceberg_rest_token or os.environ.get(_ICEBERG_REST_TOKEN_ENV_VAR)
        rest_warehouse = iceberg_rest_warehouse or os.environ.get(_ICEBERG_REST_WAREHOUSE_ENV_VAR)
        glue_region = iceberg_glue_region or os.environ.get(_ICEBERG_GLUE_REGION_ENV_VAR)

        spark_catalog_class = "org.apache.iceberg.spark.SparkSessionCatalog"
        leaf_catalog_class = "org.apache.iceberg.spark.SparkCatalog"

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
                    f"iceberg_catalog_type=jdbc requires iceberg_catalog_uri"
                    f" (env var {_ICEBERG_CATALOG_URI_ENV_VAR})"
                )
            jdbc_jars_extra = os.environ.get(
                "ELT_PIPELINE_ICEBERG_JDBC_JARS_EXTRA", ""
            ).strip()
            packages_str = _ICEBERG_RUNTIME_PACKAGE
            if jdbc_jars_extra:
                packages_str = f"{_ICEBERG_RUNTIME_PACKAGE},{jdbc_jars_extra}"
            builder = builder.config(
                "spark.jars.packages",
                packages_str,
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
            jdbc_driver = os.environ.get("ELT_PIPELINE_ICEBERG_JDBC_DRIVER")
            schema_version = os.environ.get(
                "ELT_PIPELINE_ICEBERG_JDBC_SCHEMA_VERSION", "V1"
            )
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
                    f"iceberg_catalog_type=rest requires iceberg_catalog_uri"
                    f" (env var {_ICEBERG_CATALOG_URI_ENV_VAR})"
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
        else:
            raise ValueError(
                f"Unsupported {_ICEBERG_CATALOG_TYPE_ENV_VAR}={catalog_type}."
                f" Supported: hadoop, jdbc, rest, glue"
            )

        builder = builder.config(
            "spark.sql.defaultCatalog",
            "spark_catalog",
        )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session

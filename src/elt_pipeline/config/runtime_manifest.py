"""Single source of truth for every default value, env-var name, path, version,
and Java class FQCN used by the elt_pipeline framework.

Goal: a user cloning from git opens ONLY this file + `pyproject.toml` to see every
tunable, their defaults, and what env vars override them. No hunting through src/.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

__all__ = [
    "EnvVarNames",
    "RuntimeVersions",
    "RuntimeClasses",
    "Paths",
    "SparkRuntime",
    "ServingDefaults",
    "CatalogBindings",
    "JdbcDrivers",
    "runtime_manifest",
]


# --- 1. ENVIRONMENT VARIABLE NAMES -------------------------------------------------
# Centralize env var string literals here so Python + shell + docs stay in sync.
# Bash reads the same values via `python3 -c "from ... import manifest; print(...)"`
@dataclass(frozen=True)
class EnvVarNames:
    repo_run_dir: str = "ELT_PIPELINE_REPO_RUN_DIR"

    # Spark / ELT compute
    spark_master: str = "ELT_PIPELINE_SPARK_MASTER"
    spark_app_name: str = "ELT_PIPELINE_SPARK_APP_NAME"
    spark_driver_host: str = "ELT_PIPELINE_SPARK_DRIVER_HOST"
    spark_driver_bind_address: str = "ELT_PIPELINE_SPARK_DRIVER_BIND_ADDRESS"
    spark_shuffle_partitions: str = "ELT_PIPELINE_SPARK_SHUFFLE_PARTITIONS"
    spark_default_parallelism: str = "ELT_PIPELINE_SPARK_DEFAULT_PARALLELISM"
    spark_aqe: str = "ELT_PIPELINE_SPARK_AQE"
    ivy_home: str = "ELT_PIPELINE_IVY_HOME"
    root_path: str = "ELT_PIPELINE_ROOT_PATH"
    warehouse_root: str = "ELT_PIPELINE_WAREHOUSE_ROOT"

    # Iceberg (writer = Spark side; serving = Trino side)
    iceberg_enabled: str = "ELT_PIPELINE_ICEBERG_ENABLED"
    iceberg_catalog_name: str = "ELT_PIPELINE_ICEBERG_CATALOG_NAME"
    iceberg_warehouse_dir: str = "ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR"
    iceberg_writer_catalog_type: str = "ELT_PIPELINE_ICEBERG_WRITER_CATALOG_TYPE"
    iceberg_serving_catalog_type: str = "ELT_PIPELINE_ICEBERG_SERVING_CATALOG_TYPE"
    iceberg_catalog_type_legacy: str = "ELT_PIPELINE_ICEBERG_CATALOG_TYPE"  # backward compat only
    iceberg_catalog_uri: str = "ELT_PIPELINE_ICEBERG_CATALOG_URI"
    iceberg_rest_token: str = "ELT_PIPELINE_ICEBERG_REST_TOKEN"
    iceberg_rest_warehouse: str = "ELT_PIPELINE_ICEBERG_REST_WAREHOUSE"
    iceberg_glue_region: str = "ELT_PIPELINE_ICEBERG_GLUE_REGION"
    iceberg_jdbc_jars_extra: str = "ELT_PIPELINE_ICEBERG_JDBC_JARS_EXTRA"
    iceberg_jdbc_driver: str = "ELT_PIPELINE_ICEBERG_JDBC_DRIVER"
    iceberg_jdbc_schema_version: str = "ELT_PIPELINE_ICEBERG_JDBC_SCHEMA_VERSION"
    iceberg_hive_metastore_uri: str = "ELT_PIPELINE_ICEBERG_HIVE_METASTORE_URI"

    # Trino serving
    trino_port: str = "ELT_PIPELINE_TRINO_PORT"
    trino_host: str = "ELT_PIPELINE_TRINO_HOST"
    trino_version: str = "ELT_PIPELINE_TRINO_VERSION"
    trino_jvm_xms_mb: str = "ELT_PIPELINE_TRINO_JVM_XMS_MB"
    trino_jvm_xmx_mb: str = "ELT_PIPELINE_TRINO_JVM_XMX_MB"

    # Publish runtime
    publish_max_rows: str = "ELT_PIPELINE_PUBLISH_MAX_ROWS"

    # Toolchain (documented only; user manages via mise/uv)
    java_home: str = "JAVA_HOME"


# --- 2. VERSIONS + MAVEN COORDINATES ----------------------------------------------
@dataclass(frozen=True)
class RuntimeVersions:
    pyspark: str = "4.1.2"
    scala_binary: str = "2.13"
    spark_major_minor: str = "4.1"
    iceberg_core: str = "1.11.0"
    iceberg_spark_runtime_maven_coord: str = (
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
    )
    jdbc_extra_maven_coords: Mapping[str, str] = field(
        default_factory=lambda: {
            "sqlite": "org.xerial:sqlite-jdbc:3.46.0.0",
            "postgresql": "org.postgresql:postgresql:42.7.3",
            "mysql": "com.mysql:mysql-connector-j:8.4.0",
        }
    )
    trino_server: str = "468"
    trino_jdbc: str = "468"
    min_java_runtime_major: int = 17
    recommended_java_runtime_major: int = 23


# --- 3. JAVA CLASS FULLY-QUALIFIED NAMES ------------------------------------------
@dataclass(frozen=True)
class RuntimeClasses:
    # Iceberg + Spark
    iceberg_spark_extensions: str = (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    iceberg_spark_session_catalog: str = "org.apache.iceberg.spark.SparkSessionCatalog"
    iceberg_spark_leaf_catalog: str = "org.apache.iceberg.spark.SparkCatalog"
    # Trino JDBC driver (for BI/consumers)
    trino_jdbc_driver: str = "io.trino.jdbc.TrinoDriver"


# --- 4. PATHS (relative to either repo_run_dir or cwd) ----------------------------
@dataclass(frozen=True)
class Paths:
    # Under $repo_run_dir/results/elt_pipeline :
    repo_run_results_elt_relpath: str = "results/elt_pipeline"
    # Fallback used when repo_run_dir is not set (user home default):
    default_user_repo_run_home: str = "Documents/__data/repo_run"
    # Caches + artifacts under $results_elt :
    trino_cache_relpath: str = ".cache/trino"
    trino_artifacts_relpath: str = ".artifacts/trino"
    trino_install_relpath: str = "trino"
    warehouse_relpath: str = "warehouse"
    iceberg_warehouse_relpath: str = "warehouse/iceberg"
    spark_ivy_relpath: str = ".cache/ivy2"
    # ELT cli defaults (under $cwd when running uv elt_pipeline):
    cli_default_root_path: str = ".ignore/runtime"
    cli_default_warehouse_root: str = ".ignore/warehouse"
    # AUTO-generated jdbc serving metastore for workstation zero-service mode:
    serving_jdbc_metastore_relpath: str = ".artifacts/trino/iceberg_jdbc_metastore.db"
    # Trino server tarball cache:
    trino_server_tarball_relpath_template: str = ".cache/trino-server-{version}.tar.gz"


# --- 5. SPARK RUNTIME DEFAULTS ----------------------------------------------------
@dataclass(frozen=True)
class SparkRuntime:
    default_master: str = "local[*]"
    default_driver_host: str = "127.0.0.1"
    default_driver_bind_address: str = "127.0.0.1"
    default_app_name: str = "elt_pipeline"
    default_shuffle_partitions: int = 4
    default_adaptive_enabled: bool = True
    default_parallelism: int = 4


# --- 6. TRINO SERVING DEFAULTS ----------------------------------------------------
@dataclass(frozen=True)
class ServingDefaults:
    default_trino_port: int = 8080
    default_trino_host: str = "127.0.0.1"
    default_trino_xms_mb: int = 512
    default_trino_xmx_mb: int = 1024
    default_coordinator: bool = True
    default_include_coordinator: bool = True
    default_http_server_authentication_type: str = "none"
    default_node_environment: str = "elt_pipeline_iceberg"
    # fs.hadoop.enabled=true registers the Hadoop-backed file:// scheme factory
    # inside Trino 468 SwitchingFileSystem; required for local POSIX file loads.
    always_emit_fs_hadoop_enabled: bool = True
    always_enable_register_table_procedure: bool = True


# --- 7. CATALOG BINDING CONSTANTS -------------------------------------------------
@dataclass(frozen=True)
class CatalogBindings:
    serving_catalog_type_valid_values: tuple[str, ...] = (
        "hadoop",
        "jdbc",
        "rest",
        "glue",
        "nessie",
        "snowflake",
    )
    writer_catalog_type_valid_values: tuple[str, ...] = (
        "glue",
        "hadoop",
        "hive_metastore",
        "jdbc",
        "nessie",
        "rest",
    )
    default_catalog_name: str = "iceberg"
    # Workstation defaults:
    #   writer  = hadoop   (file-native, zero-service, source of truth)
    #   serving = jdbc     (auto-SQLite metastore; disposable, zero-service,
    #                       correct for WORKSTATION P0/P1 proof.  sqlite-jdbc
    #                       jar is auto-downloaded + injected into
    #                       plugin/<catalog>/ dir by run_trino.sh write-configs)
    workstation_default_writer_catalog: str = "hadoop"
    workstation_default_serving_catalog: str = "jdbc"
    workstation_default_serving_jdbc_driver: str = "sqlite"
    # Auto-derived sqlite jdbc URI template when YAML+env leave catalog_uri="".
    # Placeholder {repo_run_elt_dir} is resolved at runtime_context materialization
    # time to <repo_run_dir>/results/elt_pipeline
    workstation_default_serving_jdbc_sqlite_uri_template: str = (
        "jdbc:sqlite:{repo_run_elt_dir}/.artifacts/trino/iceberg_jdbc_metastore.db"
    )


# --- 8. JDBC DRIVER CONSTANTS -----------------------------------------------------
@dataclass(frozen=True)
class JdbcDrivers:
    sqlite_class: str = "org.sqlite.JDBC"
    postgresql_class: str = "org.postgresql.Driver"
    mysql_class: str = "com.mysql.cj.jdbc.Driver"
    sqlite_uri_scheme: str = "sqlite"


# --- AGGREGATE MANIFEST -----------------------------------------------------------
@dataclass(frozen=True)
class RuntimeManifest:
    env: EnvVarNames = field(default_factory=EnvVarNames)
    versions: RuntimeVersions = field(default_factory=RuntimeVersions)
    classes: RuntimeClasses = field(default_factory=RuntimeClasses)
    paths: Paths = field(default_factory=Paths)
    spark: SparkRuntime = field(default_factory=SparkRuntime)
    serving: ServingDefaults = field(default_factory=ServingDefaults)
    catalogs: CatalogBindings = field(default_factory=CatalogBindings)
    jdbc: JdbcDrivers = field(default_factory=JdbcDrivers)


runtime_manifest = RuntimeManifest()

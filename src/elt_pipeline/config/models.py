from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Level2Mode = Literal["required_level2", "lightweight_level2", "bypass_level2"]


class RuntimeSparkConfig(BaseModel):
    """Spark runtime infrastructure settings (portable: config YAML → no env vars needed).

    Maps to runtime_manifest.SparkRuntime defaults + builder overrides.
    Users configure these fields in the pipeline YAML `runtime.spark` section
    instead of setting ELT_PIPELINE_SPARK_* env vars.
    """

    model_config = ConfigDict(extra="forbid")

    master: str | None = None
    app_name: str | None = None
    driver_host: str | None = None
    driver_bind_address: str | None = None
    shuffle_partitions: int | None = None
    default_parallelism: int | None = None
    aqe_enabled: bool | None = None
    enable_iceberg: bool | None = None


class RuntimeIcebergWriterConfig(BaseModel):
    """Iceberg Spark WRITER catalog settings (map to writer_catalog_type defaults)."""

    model_config = ConfigDict(extra="forbid")

    catalog_type: str | None = None
    catalog_name: str | None = None
    warehouse_dir: str | None = None
    hive_metastore_uri: str | None = None
    catalog_impl_override: str | None = None


class RuntimeIcebergServingConfig(BaseModel):
    """Iceberg Trino SERVING catalog settings (map to serving_catalog defaults)."""

    model_config = ConfigDict(extra="forbid")

    catalog_type: str | None = None
    catalog_uri: str | None = None
    jdbc_driver: str | None = None
    rest_token: str | None = None
    rest_warehouse: str | None = None
    glue_region: str | None = None
    catalog_impl_override: str | None = None


class RuntimeTrinoServingConfig(BaseModel):
    """Trino serving engine runtime config (map to Trino port/host/version defaults)."""

    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    port: int | None = None
    version: str | None = None
    http_authentication_type: str | None = None
    https_enabled: bool | None = None
    https_port: int | None = None
    ssl_keystore_path: str | None = None
    ssl_keystore_password: str | None = None
    ssl_truststore_path: str | None = None
    ssl_truststore_password: str | None = None
    password_file_path: str | None = None
    krb5_conf: str | None = None
    kerberos_principal: str | None = None
    kerberos_keytab: str | None = None
    coordinator: bool | None = None
    include_coordinator: bool | None = None
    node_environment: str | None = None
    fs_hadoop_enabled: bool | None = None
    register_table_procedure_enabled: bool | None = None


class RuntimeConfig(BaseModel):
    """Infrastructure-wide runtime defaults (source of truth: pipeline YAML).

    Users cloning the repo set these fields in the YAML pipeline config and
    never need to export ELT_PIPELINE_* env vars.  Env var overrides are still
    supported on top (highest precedence: CLI > ENV > YAML.runtime > manifest
    frozen defaults) — but the happy path is zero env variables.
    """

    model_config = ConfigDict(extra="forbid")

    repo_run_dir: str | None = None
    cli_default_root_path: str | None = None
    cli_default_warehouse_root: str | None = None

    spark: RuntimeSparkConfig = Field(default_factory=RuntimeSparkConfig)

    iceberg_writer: RuntimeIcebergWriterConfig = Field(
        default_factory=RuntimeIcebergWriterConfig
    )
    iceberg_serving: RuntimeIcebergServingConfig = Field(
        default_factory=RuntimeIcebergServingConfig
    )
    trino_serving: RuntimeTrinoServingConfig = Field(
        default_factory=RuntimeTrinoServingConfig
    )


class ConfigLayer(BaseModel):
    model_config = ConfigDict(extra="allow")

    trigger_mode: str | None = None
    level2_mode: Level2Mode = "required_level2"
    auth: dict[str, Any] = Field(default_factory=dict)
    extraction: dict[str, Any] = Field(default_factory=dict)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    defaults: dict[str, Any] = Field(default_factory=dict)

    def to_payload(self, *, exclude: set[str] | None = None) -> dict[str, Any]:
        excluded = {"defaults"}
        if exclude:
            excluded.update(exclude)
        return self.model_dump(exclude=excluded, exclude_none=True)


class EntityConfig(ConfigLayer):
    name: str


class SourceConfig(ConfigLayer):
    name: str
    connector_type: str
    entities: list[EntityConfig] = Field(default_factory=list)


class EnvironmentOverlay(BaseModel):
    defaults: dict[str, Any] = Field(default_factory=dict)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)


class PipelineConfig(BaseModel):
    schema_version: str
    defaults: dict[str, Any] = Field(default_factory=dict)
    environments: dict[str, EnvironmentOverlay] = Field(default_factory=dict)
    sources: list[SourceConfig] = Field(default_factory=list)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)

    def get_source(self, source_name: str) -> SourceConfig:
        for source in self.sources:
            if source.name == source_name:
                return source
        raise LookupError(f"Unknown source: {source_name}")


class ResolvedEntityConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    connector_type: str
    trigger_mode: str | None = None
    level2_mode: Level2Mode = "required_level2"
    auth: dict[str, Any] = Field(default_factory=dict)
    extraction: dict[str, Any] = Field(default_factory=dict)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    resolved_defaults: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

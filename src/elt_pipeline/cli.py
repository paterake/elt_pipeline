from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pyspark.sql import SparkSession

from elt_pipeline.config import runtime_context
from elt_pipeline.config.loader import (
    load_pipeline_config,
    load_runtime_overrides,
    resolve_entity_config,
)
from elt_pipeline.config.models import Level2Mode, PipelineConfig, ResolvedEntityConfig
from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.ingest import (
    KafkaConnectorConfig,
    LocalArtifactStore,
    LocalKafkaConnector,
    LocalObjectStorageConnector,
    LocalRestConnector,
    LocalSqlConnector,
    ObjectStorageConnectorConfig,
    RestConnectorConfig,
    RestRequestWindow,
    SqlConnectorConfig,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.integrations import (
    build_lineage_adapter,
    load_orchestration_metadata_from_env,
)
from elt_pipeline.normalize.partitioning import PartitionMode, PartitionStrategy
from elt_pipeline.normalize.pipeline import normalize_level1_to_local_level2
from elt_pipeline.publish import (
    discover_publish_definitions,
    explain_publish_definitions,
    filter_publish_definitions,
    run_publish_definitions_locally,
)
from elt_pipeline.shared.audit import AuditRecord, MetricsSummary
from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
    build_error_record,
)
from elt_pipeline.shared.lineage import DatasetRef, LineageEvent
from elt_pipeline.shared.logging import build_log_event
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_exists,
    path_normalize,
    path_read_text,
    path_rglob,
)
from elt_pipeline.shared.runtime import (
    ExecutionWindow,
    StageName,
    build_job_runtime,
    new_run_context,
)
from elt_pipeline.shared.scheduler import SchedulePlan, load_schedule_plan, parse_schedule_payload
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql import (
    SparkSqlModelExecutor,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    filter_sql_models,
    resolve_selected_model_ids,
    run_sql_models_locally,
    topologically_sort_sql_models,
)
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.models import SqlModelStage

# Default local scratch locations for runtime output. Both live under a single gitignored
# `.ignore/` directory so bare commands run from the repo do not pollute the working tree.
# Override with --root-path / --warehouse-root for real runs.
# PORTABILITY: Final defaults for argparse are computed via _cli_default_root_paths()
# below using the same 4-tier cascade as every other consumer:
#   explicit arg > ENV ELT_PIPELINE_CONFIG_PATH > pipeline.yaml runtime.cli_default_* > manifest
# No single env var is ever required to be set.
_MODULE_MANIFEST_ROOT_PATH_DEFAULT: str = runtime_manifest.paths.cli_default_root_path
_MODULE_MANIFEST_WH_PATH_DEFAULT: str = runtime_manifest.paths.cli_default_warehouse_root


@dataclass(frozen=True)
class _RuntimeContext:
    """Composed once at the CLI entry point (Mercell/Camellos runner pattern).

    Captures every runtime decision *explicitly* so downstream helpers never
    have to re-discover repo roots, re-read config YAML, or rely on ENV-only
    fallbacks — the single source of truth for a given invocation.

    Single-writer principle: only main() calls _compose_runtime_context();
    helpers take ``runtime_overrides`` / ``config_path`` explicitly.
    """

    repo_root: Path
    """Absolute path to the repo root (used as relative anchor)."""

    config_path_resolved: Path | None
    """Absolute path to the pipeline YAML actually used, or None if fallback to manifest-only."""

    config_path_source: str
    """Human-readable origin of config_path.

    Values: "arg" | "env" | "repo_root_auto" | "manifest_fallback".
    """

    environment: str | None
    """Selected environment overlay name (e.g. "default", "staging", None)"""

    runtime_overrides: dict[str, Any]
    """4-tier composed overrides: already passed through load_runtime_overrides()."""

    cli_default_root_path: str
    """Final --root-path default value for argparse defaults (after YAML cascade)."""

    cli_default_warehouse_root: str
    """Final --warehouse-root default value for argparse defaults (after YAML cascade)."""


@dataclass(frozen=True)
class _CheckpointOverride:
    active: bool
    value: dict[str, Any] | None


@dataclass(frozen=True)
class _SqlRerunSelection:
    environment: str
    model_ids: tuple[str, ...]
    start_date: str | None
    end_date: str | None
    partition_values: dict[str, str]
    extra_values: dict[str, Any]


@dataclass(frozen=True)
class _PublishRerunSelection:
    environment: str
    publish_ids: tuple[str, ...]
    window_start: str | None
    window_end: str | None
    window_label: str | None
    backfill: bool


def _load_runtime_overrides_from_env_or_args(
    *,
    config_path: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    """Resolve runtime overrides (YAML → frozen manifest defaults fallback).

    Order used to locate a config path (first hit wins — portability contract:
    NO single env var is ever required; root pipeline.yaml is auto-discovered):
      1. Explicit ``config_path`` function argument (highest — CLI --config-path).
      2. ``ELT_PIPELINE_CONFIG_PATH`` environment variable (optional override
         for multi-config setups, CI, or running outside the repo tree).
      3. ``<repo-root>/pipeline.yaml`` — the user-edited clone-n-run root
         config file. Present on every clone; this is the default no-code
         path.
      4. No config found → return empty dict; caller falls back to ENV +
         manifest frozen defaults (absolute floor — still zero crashes).
    """
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
    """Return the repo_run root directory.

    Cascade: ENV ELT_PIPELINE_REPO_RUN_DIR > YAML runtime.repo_run_dir > frozen manifest default.
    """
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
    """Entry-point runner (Mercell/Camellos pattern) — compose once, pass everywhere.

    Composes the entire runtime decision matrix *explicitly* at the top of
    ``main()`` before any subcommand branch runs.  Resulting ``_RuntimeContext``
    is the single immutable source of truth for the invocation.  **No downstream
    helper ever re-reads the YAML or re-discovers repo roots via side-channel
    heuristics** (closes the drift gap identified vs Mercell/Camellos pattern).

    Returns:
        Frozen _RuntimeContext populated with every decision needed downstream:
          ``repo_root``                    absolute anchor, never re-computed
          ``config_path_resolved``         the YAML actually loaded, or None
          ``config_path_source``           "arg" | "env" | "repo_root_auto" | "manifest_fallback"
          ``environment``                  selected environment overlay name
          ``runtime_overrides``            dict result of load_runtime_overrides()
          ``cli_default_root_path``       argparse default (after YAML cascade)
          ``cli_default_warehouse_root``   argparse default (after YAML cascade)
    """
    import os as _os

    # 1) Repo root — explicit anchor. cli.py lives at src/elt_pipeline/cli.py so
    #    parents[2] = <repo_root>.  We compute ONCE and cache as the anchor.
    repo_root = Path(__file__).resolve().parents[2]

    # 2) Config-path cascade — arg > ENV > repo_root/pipeline.yaml > None.
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

    # 3) Runtime overrides — loaded ONCE here, with environment overlay applied.
    if config_path_resolved is not None:
        ro = load_runtime_overrides(str(config_path_resolved), environment=environment_arg)
    else:
        ro = {}

    # 4) CLI default paths for argparse — repo_run_dir (ENV > YAML) trumps manifest.
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
    """Return True if Iceberg is enabled, None if no tier matched.

    3-tier precedence (Mercell/Camellos):
      1. CLI/args (explicit --iceberg-enabled flag)            — highest
      2. runtime_context singleton (final materialized value)  — primary
      3. runtime_overrides explicit-pass (legacy)              — transition
      4. manifest frozen default = None → caller decides

    **Zero os.environ reads.**  The singleton materializer at main() is the
    only place that reads ``ELT_PIPELINE_*`` env vars.
    """
    explicit = getattr(args, "iceberg_enabled", None)
    if explicit is True:
        return True

    # Singleton — single source of truth (Mercell/Camellos)
    if runtime_context.is_initialized():
        final_val = runtime_context.get("spark.enable_iceberg")
        if final_val is True:
            return True
        if final_val is False:
            return False

    # Legacy explicit-pass runtime_overrides dict (transitionary)
    if isinstance(runtime_overrides, dict):
        spark_conf = runtime_overrides.get("spark")
        yaml_value = (
            spark_conf.get("enable_iceberg") if isinstance(spark_conf, dict) else None
        )
        if yaml_value is True:
            return True
        if yaml_value is False:
            return False

    return None


def _get_from_runtime_overrides(
    runtime_overrides: dict[str, Any] | None,
    *override_path: str,
) -> Any:
    """Traverse ``runtime_overrides`` along the dotted path, tolerant of any None/missing.

    Used to implement the 3rd tier of the cascade (YAML overrides).
    Returns the leaf value, or ``None`` if any intermediate node is missing or not a dict.
    """
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
    """Validate Iceberg writer/serving catalog binding.

    Uses the Mercell/Camellos runtime_context singleton as the single source
    of truth for FINAL values — zero ``os.environ`` reads here.  The
    materializer in :mod:`runtime_context` is the only place that reads
    ``ELT_PIPELINE_*``.
    """
    ro: dict[str, Any] = (
        runtime_overrides if isinstance(runtime_overrides, dict) else {}
    )
    has_singleton = runtime_context.is_initialized()

    def _singleton_or(key: str, ro_path: tuple[str, ...], manifest_default: Any) -> Any:
        """Tiered read: singleton (final materialized) > explicit ro > manifest."""
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
    if writer_catalog_type in {"jdbc", "rest"} and not catalog_uri:
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


def _build_serving_endpoint(
    args: Any, *, runtime_overrides: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Build the Iceberg serving endpoint descriptor.

    Mercell/Camellos pattern — zero ``os.environ`` reads; all final values
    come from the runtime_context singleton (materialized once at entry).

    Explicit fallback: explicit CLI args > singleton final > ro dict (legacy) > manifest.
    """
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
        """Final value: singleton > explicit ro > manifest."""
        if has_singleton:
            v = runtime_context.get(singleton_key)
            if v not in (None, ""):
                return v
        from_ro = _get_from_runtime_overrides(ro, *ro_path)
        if from_ro not in (None, ""):
            return from_ro
        return manifest_default

    enabled = _iceberg_effective_enabled(args, runtime_overrides=ro)
    if enabled is None:
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
    import os as _os

    ro = runtime_overrides if isinstance(runtime_overrides, dict) else {}
    writer_conf = (
        ro.get("iceberg_writer", {}) if isinstance(ro.get("iceberg_writer"), dict) else {}
    )
    serving_conf = (
        ro.get("iceberg_serving", {}) if isinstance(ro.get("iceberg_serving"), dict) else {}
    )

    def _pick(*, argname: str, envkey: str, runtime_subkey: str | None, runtime_conf: dict | None):
        val = getattr(args, argname, None)
        if val:
            return val
        env_val = _os.environ.get(envkey, "").strip()
        if env_val:
            return env_val
        if runtime_subkey and runtime_conf:
            rval = runtime_conf.get(runtime_subkey)
            if rval not in (None, ""):
                return rval
        return None

    env = runtime_manifest.env
    catalog_name = _pick(
        argname="iceberg_catalog_name",
        envkey=env.iceberg_catalog_name,
        runtime_subkey="catalog_name",
        runtime_conf=writer_conf or serving_conf,
    )
    catalog_type = (
        getattr(args, "iceberg_writer_catalog_type", None)
        or getattr(args, "iceberg_catalog_type", None)
        or _os.environ.get(env.iceberg_writer_catalog_type, "").strip()
        or _os.environ.get(env.iceberg_catalog_type_legacy, "").strip()
        or writer_conf.get("catalog_type")
        or serving_conf.get("catalog_type")
        or None
    )
    catalog_uri = _pick(
        argname="iceberg_catalog_uri",
        envkey=env.iceberg_catalog_uri,
        runtime_subkey="catalog_uri",
        runtime_conf=serving_conf,
    )
    warehouse_dir = _pick(
        argname="iceberg_warehouse_dir",
        envkey=env.iceberg_warehouse_dir,
        runtime_subkey="warehouse_dir",
        runtime_conf=writer_conf,
    )
    rest_token = _pick(
        argname="iceberg_rest_token",
        envkey=env.iceberg_rest_token,
        runtime_subkey="rest_token",
        runtime_conf=serving_conf,
    )
    rest_warehouse = _pick(
        argname="iceberg_rest_warehouse",
        envkey=env.iceberg_rest_warehouse,
        runtime_subkey="rest_warehouse",
        runtime_conf=serving_conf,
    )
    glue_region = _pick(
        argname="iceberg_glue_region",
        envkey=env.iceberg_glue_region,
        runtime_subkey="glue_region",
        runtime_conf=serving_conf,
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
    return kwargs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="elt-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate-config",
        help="Validate a YAML configuration file and optionally resolve one source/entity.",
    )
    validate_parser.add_argument("config_path", type=Path)
    validate_parser.add_argument("--environment", default="default")
    validate_parser.add_argument("--source")
    validate_parser.add_argument("--entity")

    run_context_parser = subparsers.add_parser(
        "show-run-context",
        help="Create and display a runtime context object.",
    )
    run_context_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in StageName],
        required=True,
    )
    run_context_parser.add_argument("--job-name", required=True)
    run_context_parser.add_argument("--trigger-type", default="manual")

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Run configured ingestion sources and entities in local mode.",
    )
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", required=True)
    ingest_run_parser = ingest_subparsers.add_parser(
        "run",
        help="Run one or more configured source entities and persist raw level1 artifacts.",
    )
    ingest_run_parser.add_argument("config_path", type=Path)
    ingest_run_parser.add_argument("--environment", default="default")
    ingest_run_parser.add_argument("--source")
    ingest_run_parser.add_argument("--entity")
    ingest_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    ingest_run_parser.add_argument("--job-name", default="ingest-run")
    ingest_run_parser.add_argument("--trigger-type", default="manual")
    ingest_run_parser.add_argument(
        "--window-start",
        help="Optional ISO-8601 window start for bounded or backfill ingest runs.",
    )
    ingest_run_parser.add_argument(
        "--window-end",
        help="Optional ISO-8601 window end for bounded or backfill ingest runs.",
    )
    ingest_run_parser.add_argument(
        "--window-label",
        help="Optional stable label for the requested ingest window.",
    )
    ingest_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Seed checkpoint state from prior history for the requested window.",
    )
    ingest_run_parser.add_argument(
        "--kafka-log-path",
        type=str,
        help="Optional override for local Kafka replay log input.",
    )

    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Run local level1 to level2 normalization.",
    )
    normalize_subparsers = normalize_parser.add_subparsers(
        dest="normalize_command",
        required=True,
    )
    normalize_run_parser = normalize_subparsers.add_parser(
        "run",
        help="Normalize level1 manifests into local level2 tables.",
    )
    normalize_run_parser.add_argument("config_path", type=Path)
    normalize_run_parser.add_argument("--environment", default="default")
    normalize_run_parser.add_argument("--source")
    normalize_run_parser.add_argument("--entity")
    normalize_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    normalize_run_parser.add_argument("--job-name", default="normalize-run")
    normalize_run_parser.add_argument("--trigger-type", default="manual")
    normalize_run_parser.add_argument(
        "--window-start",
        help="Optional ISO-8601 lower bound used to select level1 manifests for reruns.",
    )
    normalize_run_parser.add_argument(
        "--window-end",
        help="Optional ISO-8601 upper bound used to select level1 manifests for reruns.",
    )
    normalize_run_parser.add_argument(
        "--window-label",
        help="Optional stable label for the requested normalization window.",
    )
    normalize_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the normalization selection as a targeted historical rerun.",
    )
    normalize_run_parser.add_argument(
        "--manifest-path",
        action="append",
        default=[],
        type=str,
        help="Explicit level1 manifest path to normalize. May be passed multiple times.",
    )
    normalize_run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the exact level1 artifact selected by a prior normalize run.",
    )
    normalize_run_parser.add_argument(
        "--partition-mode",
        choices=[mode.value for mode in PartitionMode],
        default=PartitionMode.ingest_date.value,
    )
    normalize_run_parser.add_argument("--partition-key")
    normalize_run_parser.add_argument("--metadata-key")

    sql_parser = subparsers.add_parser(
        "sql",
        help="Discover, compile, and run local SQL model packages.",
    )
    sql_subparsers = sql_parser.add_subparsers(dest="sql_command", required=True)

    compile_parser = sql_subparsers.add_parser(
        "compile",
        help="Compile SQL models with runtime tokens resolved.",
    )
    _add_sql_selection_arguments(compile_parser)
    compile_parser.add_argument("--include-deps", action="store_true")

    run_parser = sql_subparsers.add_parser(
        "run",
        help="Run SQL models against a Spark-backed local parquet or Iceberg warehouse.",
    )
    _add_sql_selection_arguments(run_parser)
    run_parser.add_argument("--include-deps", action="store_true")
    run_parser.add_argument(
        "--root-path",
        type=str,
        default=_DEFAULT_ROOT_PATH_EVAL,
        help=(
            "Pipeline runtime root containing level1/level2 data and run artifacts. "
            "Defaults to ELT_PIPELINE_REPO_RUN_DIR/runtime if the project-wide repo_run "
            "directory is available, otherwise .ignore/runtime."
        ),
    )
    run_parser.add_argument(
        "--warehouse-root",
        type=str,
        default=_DEFAULT_WAREHOUSE_ROOT_EVAL,
        help=(
            "SQL warehouse root for level3/level4 output. Defaults to "
            "ELT_PIPELINE_REPO_RUN_DIR/warehouse if repo_run is available, else "
            ".ignore/warehouse."
        ),
    )
    run_parser.add_argument("--job-name", default="sql-run")
    run_parser.add_argument("--trigger-type", default="manual")
    run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the model/window/partition selection from a prior sql run.",
    )
    run_parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate compiled SQL against the target database without executing writes.",
    )
    run_parser.add_argument(
        "--explain",
        action="store_true",
        help="Include sqlite query plan details; implies a validate-only planning run.",
    )
    run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
        help=(
            "Enable Iceberg table format for level3/level4 writes (overrides env "
            "ELT_PIPELINE_ICEBERG_ENABLED). When set, writes go to the configured Iceberg "
            "catalog instead of plain parquet files and atomic staging-swap is bypassed "
            "for Iceberg-managed commits."
        ),
    )
    run_parser.add_argument(
        "--iceberg-catalog-name",
        default=None,
        help="Override env ELT_PIPELINE_ICEBERG_CATALOG_NAME (default: iceberg).",
    )
    run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "jdbc", "rest", "glue"],
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_TYPE (default: hadoop). "
            "hadoop=filesystem (local zero-infra); jdbc=H2/Postgres-backed; "
            "rest=Polaris/Nessie/Lakekeeper/Tabular (requires URI); "
            "glue=AWS Glue Data Catalog (requires region or default SDK region)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-catalog-uri",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_URI. Required when "
            "--iceberg-catalog-type=jdbc (JDBC connection string) or "
            "--iceberg-catalog-type=rest (REST server endpoint, e.g. http://localhost:8181/api/v1)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-rest-token",
        default=None,
        dest="iceberg_rest_token",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_TOKEN. Bearer / API token for "
            "--iceberg-catalog-type=rest (Polaris/Nessie/Lakekeeper/Tabular auth)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-rest-warehouse",
        default=None,
        dest="iceberg_rest_warehouse",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_WAREHOUSE. Warehouse name/ID for "
            "--iceberg-catalog-type=rest when the REST server hosts multiple warehouses."
        ),
    )
    run_parser.add_argument(
        "--iceberg-glue-region",
        default=None,
        dest="iceberg_glue_region",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_GLUE_REGION. AWS region for "
            "--iceberg-catalog-type=glue (falls back to standard AWS SDK region chain)."
        ),
    )
    run_parser.add_argument(
        "--iceberg-warehouse-dir",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR. If omitted and Iceberg is "
            "enabled, automatically falls back to <warehouse-root>/iceberg."
        ),
    )
    run_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section with layering: CLI args > ENV > YAML > "
            "frozen manifest defaults. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH`` when not explicitly passed."
        ),
    )

    publish_parser = subparsers.add_parser(
        "publish",
        help="Discover, validate, explain, and run local level5 publish definitions.",
    )
    publish_subparsers = publish_parser.add_subparsers(
        dest="publish_command",
        required=True,
    )

    publish_validate_parser = publish_subparsers.add_parser(
        "validate",
        help="Validate a publish definition package without writing outputs.",
    )
    _add_publish_selection_arguments(publish_validate_parser)

    publish_explain_parser = publish_subparsers.add_parser(
        "explain",
        help="Preview the artifacts a publish run would produce.",
    )
    _add_publish_selection_arguments(publish_explain_parser)
    publish_explain_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    publish_explain_parser.add_argument("--job-name", default="publish-explain")
    publish_explain_parser.add_argument("--trigger-type", default="manual")
    publish_explain_parser.add_argument("--window-start")
    publish_explain_parser.add_argument("--window-end")
    publish_explain_parser.add_argument("--window-label")
    publish_explain_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the publish selection as a targeted historical backfill.",
    )
    publish_explain_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH``."
        ),
    )

    publish_run_parser = publish_subparsers.add_parser(
        "run",
        help="Run publish definitions against a Spark-backed parquet or Iceberg warehouse.",
    )
    _add_publish_selection_arguments(publish_run_parser)
    publish_run_parser.add_argument("--root-path", type=str, default=_DEFAULT_ROOT_PATH)
    publish_run_parser.add_argument("--warehouse-root", type=str, default=_DEFAULT_WAREHOUSE_ROOT)
    publish_run_parser.add_argument("--job-name", default="publish-run")
    publish_run_parser.add_argument("--trigger-type", default="manual")
    publish_run_parser.add_argument("--window-start")
    publish_run_parser.add_argument("--window-end")
    publish_run_parser.add_argument("--window-label")
    publish_run_parser.add_argument(
        "--backfill",
        action="store_true",
        help="Treat the publish selection as a targeted historical backfill.",
    )
    publish_run_parser.add_argument(
        "--rerun-run-id",
        help="Reuse the publish/window selection from a prior publish run.",
    )
    publish_run_parser.add_argument(
        "--iceberg-enabled",
        dest="iceberg_enabled",
        action="store_true",
        default=None,
        help=(
            "Enable Iceberg table format for level3/level4 source reads (overrides env "
            "ELT_PIPELINE_ICEBERG_ENABLED). When set, reads from the configured Iceberg "
            "catalog instead of plain parquet files."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-name",
        default=None,
        help="Override env ELT_PIPELINE_ICEBERG_CATALOG_NAME (default: iceberg).",
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-type",
        default=None,
        choices=["hadoop", "jdbc", "rest", "glue"],
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_TYPE (default: hadoop). "
            "hadoop=filesystem (local zero-infra); jdbc=H2/Postgres-backed; "
            "rest=Polaris/Nessie/Lakekeeper/Tabular (requires URI); "
            "glue=AWS Glue Data Catalog (requires region or default SDK region)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-catalog-uri",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_CATALOG_URI. Required when "
            "--iceberg-catalog-type=jdbc (JDBC connection string) or "
            "--iceberg-catalog-type=rest (REST server endpoint, e.g. http://localhost:8181/api/v1)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-rest-token",
        default=None,
        dest="iceberg_rest_token",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_TOKEN. Bearer / API token for "
            "--iceberg-catalog-type=rest (Polaris/Nessie/Lakekeeper/Tabular auth)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-rest-warehouse",
        default=None,
        dest="iceberg_rest_warehouse",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_REST_WAREHOUSE. Warehouse name/ID for "
            "--iceberg-catalog-type=rest when the REST server hosts multiple warehouses."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-glue-region",
        default=None,
        dest="iceberg_glue_region",
        help=(
            "Override env ELT_PIPELINE_ICEBERG_GLUE_REGION. AWS region for "
            "--iceberg-catalog-type=glue (falls back to standard AWS SDK region chain)."
        ),
    )
    publish_run_parser.add_argument(
        "--iceberg-warehouse-dir",
        default=None,
        help=(
            "Override env ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR. If omitted and Iceberg is "
            "enabled, automatically falls back to <warehouse-root>/iceberg."
        ),
    )
    publish_run_parser.add_argument(
        "--config-path",
        type=Path,
        default=None,
        help=(
            "Optional pipeline YAML config path for runtime infrastructure overrides "
            "(Spark, Iceberg, Trino). When provided, runtime defaults are loaded from "
            "the YAML ``runtime:`` section. Also auto-resolves from env "
            "``ELT_PIPELINE_CONFIG_PATH``."
        ),
    )

    schedule_parser = subparsers.add_parser(
        "schedule",
        help="Execute ordered local schedule plans by calling existing CLI commands.",
    )
    schedule_subparsers = schedule_parser.add_subparsers(
        dest="schedule_command",
        required=True,
    )
    schedule_run_parser = schedule_subparsers.add_parser(
        "run",
        help="Run a validated local schedule plan in deterministic job order.",
    )
    schedule_run_parser.add_argument("plan_path", type=Path)
    schedule_run_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running remaining jobs after a job failure.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # ================================================================
    # Entry-point runner pattern (Mercell/Camellos):
    #  - FIRST → runtime_context SINGLETON.initialize() — materializes
    #    EVERY final config value via the 4-tier cascade.
    #  - From this point on, NO framework component reads os.environ
    #    or re-reads pipeline.yaml on its own.  Everything reads via
    #    runtime_context.get(dotted_key) — simple key-value lookup.
    #  - ALSO compose the legacy _RuntimeContext frozen dataclass into
    #    a LOCAL VARIABLE renamed to `composed_ctx` (to avoid name
    #    collision with the runtime_context singleton module).  This
    #    feeds callers still waiting to be migrated from the explicit
    #    runtime_overrides dict pattern; eventually all migrate to
    #    runtime_context.get().
    # ================================================================
    _config_path_arg = getattr(args, "config_path", None)
    _config_path_str = str(_config_path_arg) if _config_path_arg is not None else None
    _environment_arg = getattr(args, "environment", None)

    # (1) New singleton (Mercell/Camellos) — ONE source of truth for framework.
    runtime_context.initialize(
        config_path_arg=_config_path_str,
        environment_arg=_environment_arg,
    )

    try:
        if args.command == "validate-config":
            if args.config_path is None:
                explicit_cp = runtime_context.config_path()
                if explicit_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "Cannot validate-config: no pipeline.yaml found at "
                            "repo root and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(explicit_cp))
            else:
                config = load_pipeline_config(args.config_path)
            if args.source and args.entity:
                resolved = resolve_entity_config(
                    config,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
                )
                print(json.dumps(resolved.model_dump(mode="json"), indent=2))
            else:
                print(json.dumps(config.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "show-run-context":
            context = new_run_context(
                stage=StageName(args.stage),
                job_name=args.job_name,
                trigger_type=args.trigger_type,
                attributes=_with_orchestration_attributes(),
            )
            print(json.dumps(context.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "ingest":
            if args.config_path is None:
                ingest_cp = runtime_context.config_path()
                if ingest_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "ingest: no pipeline.yaml found at repo root "
                            "and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(ingest_cp))
            else:
                config = load_pipeline_config(args.config_path)
            cli_window = _build_cli_window_selection(
                window_start=args.window_start,
                window_end=args.window_end,
                window_label=args.window_label,
                backfill=args.backfill,
            )
            selected_entities = _resolve_entity_selections(
                config,
                environment=args.environment,
                source_name=args.source,
                entity_name=args.entity,
            )
            results = [
                _run_ingest_entity(
                    resolved_config=resolved_config,
                    root_path=args.root_path,
                    job_name=args.job_name,
                    trigger_type=args.trigger_type,
                    kafka_log_path=args.kafka_log_path,
                    cli_window=cli_window,
                    backfill=args.backfill,
                )
                for resolved_config in selected_entities
            ]
            payload = {
                "command": "ingest.run",
                "environment": args.environment,
                "selection": {
                    "source": args.source,
                    "entity": args.entity,
                    "window_start": _serialize_datetime(cli_window.start),
                    "window_end": _serialize_datetime(cli_window.end),
                    "window_label": cli_window.label,
                    "backfill": args.backfill,
                },
                "result_count": len(results),
                "results": results,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "normalize":
            if args.config_path is None:
                normalize_cp = runtime_context.config_path()
                if normalize_cp is None:
                    raise ConfigValidationError(
                        message=(
                            "normalize: no pipeline.yaml found at repo root "
                            "and --config-path omitted"
                        ),
                    )
                config = load_pipeline_config(str(normalize_cp))
            else:
                config = load_pipeline_config(args.config_path)
            cli_window = _build_cli_window_selection(
                window_start=args.window_start,
                window_end=args.window_end,
                window_label=args.window_label,
                backfill=args.backfill,
            )
            if args.rerun_run_id:
                _validate_normalize_rerun_request(args)
                manifests = [
                    _resolve_normalize_rerun_manifest(
                        root_path=args.root_path,
                        rerun_run_id=args.rerun_run_id,
                    )
                ]
                selected_environment = manifests[0].environment
            else:
                manifests = _select_level1_manifests(
                    root_path=args.root_path,
                    environment=args.environment,
                    source_name=args.source,
                    entity_name=args.entity,
                    explicit_manifest_paths=args.manifest_path,
                    window_start=cli_window.start,
                    window_end=cli_window.end,
                )
                selected_environment = args.environment
            selected_source = manifests[0].source_name if args.rerun_run_id else args.source
            selected_entity = manifests[0].entity_name if args.rerun_run_id else args.entity
            if (
                args.rerun_run_id
                and selected_environment != runtime_context.selected_environment()
            ):
                rerun_override_ro = _load_runtime_overrides_from_env_or_args(
                    config_path=str(args.config_path)
                    if getattr(args, "config_path", None)
                    else None,
                    environment=selected_environment,
                )
            else:
                rerun_override_ro = runtime_context.as_runtime_overrides()
            normalize_spark = build_spark_session(
                app_name=f"elt-pipeline-normalize-{args.job_name}",
                runtime_overrides=rerun_override_ro,
            )
            try:
                summaries = [
                    _run_normalize_manifest(
                        manifest=manifest,
                        resolved_config=resolve_entity_config(
                            config,
                            environment=manifest.environment,
                            source_name=manifest.source_name,
                            entity_name=manifest.entity_name,
                        ),
                        root_path=args.root_path,
                        job_name=args.job_name,
                        trigger_type=args.trigger_type,
                        backfill=args.backfill,
                        rerun_run_id=args.rerun_run_id,
                        partition_strategy=PartitionStrategy(
                            mode=PartitionMode(args.partition_mode),
                            partition_key=args.partition_key,
                            metadata_key=args.metadata_key,
                        ),
                        spark=normalize_spark,
                    )
                    for manifest in manifests
                ]
            finally:
                normalize_spark.stop()
            payload = {
                "command": "normalize.run",
                "environment": selected_environment,
                "selection": {
                    "source": selected_source,
                    "entity": selected_entity,
                    "manifest_paths": (
                        [manifest.manifest_path for manifest in manifests]
                        if args.rerun_run_id
                        else [str(path) for path in args.manifest_path]
                    ),
                    "window_start": _serialize_datetime(cli_window.start),
                    "window_end": _serialize_datetime(cli_window.end),
                    "window_label": cli_window.label,
                    "backfill": args.backfill,
                    "rerun_run_id": args.rerun_run_id,
                },
                "processed_count": len(summaries),
                "results": summaries,
            }
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "sql":
            rerun_run_id = getattr(args, "rerun_run_id", None)
            discovered_models = discover_sql_models(args.package_path)
            ordered_models = topologically_sort_sql_models(discovered_models)
            sql_environment = args.environment
            selection_stage = args.stage
            selection_domain = args.domain
            selection_model = args.model
            selection_include_dependencies = args.include_deps
            selection_start_date = args.start_date
            selection_end_date = args.end_date
            rerun_selection: _SqlRerunSelection | None = None

            if args.sql_command == "run" and rerun_run_id:
                _validate_sql_rerun_request(args)
                rerun_selection = _resolve_sql_rerun_selection(
                    root_path=args.root_path,
                    rerun_run_id=rerun_run_id,
                )
                selected_ids = set(rerun_selection.model_ids)
                models_to_process = [
                    model for model in ordered_models if model.model_id in selected_ids
                ]
                missing_model_ids = sorted(
                    model_id
                    for model_id in selected_ids
                    if all(model.model_id != model_id for model in ordered_models)
                )
                if missing_model_ids:
                    raise ConfigValidationError(
                        message="Rerun selection references SQL models that are not present",
                        context={
                            "package_path": str(args.package_path),
                            "rerun_run_id": args.rerun_run_id,
                            "missing_model_ids": missing_model_ids,
                        },
                    )
                sql_environment = rerun_selection.environment
                selection_stage = None
                selection_domain = None
                selection_model = None
                selection_include_dependencies = False
                selection_start_date = rerun_selection.start_date
                selection_end_date = rerun_selection.end_date
            else:
                selected_models = filter_sql_models(
                    discovered_models,
                    stage=args.stage,
                    domain=args.domain,
                    model_name=args.model,
                )
                if not selected_models:
                    raise ConfigValidationError(
                        message="No SQL models matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "stage": args.stage,
                            "domain": args.domain,
                            "model": args.model,
                        },
                    )

                selected_ids = resolve_selected_model_ids(
                    all_models=discovered_models,
                    selected_models=selected_models,
                    include_dependencies=args.include_deps,
                )
                models_to_process = [
                    model for model in ordered_models if model.model_id in selected_ids
                ]

            if (
                rerun_run_id
                and sql_environment != runtime_context.selected_environment()
            ):
                _sql_runtime_overrides = _load_runtime_overrides_from_env_or_args(
                    config_path=(
                        str(args.config_path)
                        if getattr(args, "config_path", None)
                        else None
                    ),
                    environment=sql_environment,
                )
            else:
                _sql_runtime_overrides = runtime_context.as_runtime_overrides()

            run_context = new_run_context(
                stage=StageName.sql,
                job_name=getattr(args, "job_name", "sql-compile"),
                trigger_type=getattr(args, "trigger_type", "manual"),
                attributes=_with_orchestration_attributes(
                    {
                        "environment": sql_environment,
                        "package_path": str(args.package_path),
                        "stage_selection": selection_stage or "",
                        "domain_selection": selection_domain or "",
                        "model_selection": selection_model or "",
                        "rerun_of_run_id": rerun_run_id or "",
                    }
                ),
            )
            extra_values = (
                rerun_selection.extra_values
                if rerun_selection is not None
                else _parse_vars_json(args.vars_json)
            )
            partition_values = (
                rerun_selection.partition_values
                if rerun_selection is not None
                else _parse_partition_values(args.partition)
            )
            compiled_models = [
                compile_sql_model(
                    model,
                    token_context=build_token_context(
                        environment=sql_environment,
                        run_id=run_context.run_id,
                        stage=model.manifest.stage.value,
                        domain=model.manifest.domain,
                        model_name=model.manifest.name,
                        target_table_name=model.manifest.target.table_name,
                        start_date=selection_start_date,
                        end_date=selection_end_date,
                        partition_values=partition_values,
                        source_name=(
                            model.manifest.sources[0].source_name
                            if model.manifest.sources
                            else None
                        ),
                        source_entity=(
                            model.manifest.sources[0].entity_name
                            if model.manifest.sources
                            else None
                        ),
                        source_table=(
                            model.manifest.sources[0].table_name
                            if model.manifest.sources and model.manifest.sources[0].table_name
                            else None
                        ),
                        extra_values=extra_values,
                    ),
                )
                for model in models_to_process
            ]

            if args.sql_command == "compile":
                payload = {
                    "run_id": run_context.run_id,
                    "selection": {
                        "stage": selection_stage,
                        "domain": selection_domain,
                        "model": selection_model,
                        "include_dependencies": selection_include_dependencies,
                        "start_date": selection_start_date,
                        "end_date": selection_end_date,
                        "partitions": partition_values,
                        "rerun_run_id": rerun_run_id,
                    },
                    "model_count": len(compiled_models),
                    "models": [
                        {
                            "model_id": model.model_id,
                            "target_table_name": model.target_table_name,
                            "load_mode": model.load_mode.value,
                            "compiled_sql": model.compiled_sql,
                            "token_values": model.token_values,
                        }
                        for model in compiled_models
                    ],
                }
                print(json.dumps(payload, indent=2))
                return 0

            if args.sql_command == "run":
                _validate_iceberg_catalog_binding(args, runtime_overrides=_sql_runtime_overrides)
                if args.validate_only or args.explain:
                    sql_spark = build_spark_session(
                        **_resolve_iceberg_session_kwargs(
                            args=args,
                            app_name=f"elt-pipeline-sql-{getattr(args, 'job_name', 'sql-run')}",
                            runtime_overrides=_sql_runtime_overrides,
                        )
                    )
                    try:
                        planning_result = SparkSqlModelExecutor(
                            spark=sql_spark,
                            warehouse_root=args.warehouse_root,
                            root_path=args.root_path,
                            environment=sql_environment,
                            run_id=run_context.run_id,
                            partition_values=partition_values,
                        ).plan(
                            compiled_models,
                            include_query_plan=args.explain,
                        )
                    finally:
                        sql_spark.stop()
                    payload = {
                        "run_id": run_context.run_id,
                        "mode": "explain" if args.explain else "validate_only",
                        "selection": {
                            "stage": selection_stage,
                            "domain": selection_domain,
                            "model": selection_model,
                            "include_dependencies": selection_include_dependencies,
                            "start_date": selection_start_date,
                            "end_date": selection_end_date,
                            "partitions": partition_values,
                            "rerun_run_id": rerun_run_id,
                        },
                        "warehouse_root": str(planning_result.warehouse_root),
                        "model_count": planning_result.model_count,
                        "execution_order": [
                            model_plan.model_id for model_plan in planning_result.planned_models
                        ],
                        "models": [
                            {
                                "model_id": model_plan.model_id,
                                "target_table_name": model_plan.target_table_name,
                                "load_mode": model_plan.load_mode.value,
                                "depends_on": model_plan.depends_on,
                                "token_values": model_plan.token_values,
                                "validation_passed": model_plan.validation_passed,
                                "validation_message": model_plan.validation_message,
                                "query_plan": [
                                    plan_step.model_dump(mode="json")
                                    for plan_step in model_plan.query_plan
                                ],
                            }
                            for model_plan in planning_result.planned_models
                        ],
                    }
                    print(json.dumps(payload, indent=2))
                    return 0

                sql_spark = build_spark_session(
                    **_resolve_iceberg_session_kwargs(
                        args=args,
                        app_name=f"elt-pipeline-sql-{getattr(args, 'job_name', 'sql-run')}",
                        runtime_overrides=_sql_runtime_overrides,
                    )
                )
                serving_endpoint = _build_serving_endpoint(
                    args, runtime_overrides=_sql_runtime_overrides
                )
                try:
                    result = run_sql_models_locally(
                        root_path=args.root_path,
                        run_context=run_context,
                        environment=sql_environment,
                        package_path=args.package_path,
                        warehouse_root=args.warehouse_root,
                        spark=sql_spark,
                        compiled_models=compiled_models,
                        partition_values=partition_values,
                        extra_values=extra_values,
                        selection_stage=selection_stage,
                        selection_domain=selection_domain,
                        selection_model=selection_model,
                        include_dependencies=selection_include_dependencies,
                        serving_endpoint=serving_endpoint,
                    )
                finally:
                    sql_spark.stop()
                payload = {
                    "run_id": run_context.run_id,
                    "selection": {
                        "stage": selection_stage,
                        "domain": selection_domain,
                        "model": selection_model,
                        "include_dependencies": selection_include_dependencies,
                        "start_date": selection_start_date,
                        "end_date": selection_end_date,
                        "partitions": partition_values,
                        "rerun_run_id": rerun_run_id,
                    },
                    "warehouse_root": str(result.execution_result.warehouse_root),
                    "model_count": result.execution_result.model_count,
                    "executed_models": [
                        record.model_dump(mode="json")
                        for record in result.execution_result.executed_models
                    ],
                    "validation_results": [
                        summary.model_dump(mode="json")
                        for summary in result.execution_result.model_validations
                    ],
                    "artifacts": {
                        "artifact_root": str(result.artifacts.artifact_root),
                        "run_dir": str(result.artifacts.run_dir),
                        "audit_path": str(result.artifacts.audit_path),
                        "log_path": str(result.artifacts.log_path),
                        "lineage_path": str(result.artifacts.lineage_path),
                        "error_path": (
                            str(result.artifacts.error_path)
                            if result.artifacts.error_path is not None
                            else None
                        ),
                    },
                    "serving_endpoint": serving_endpoint,
                }
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "publish":
            discovered_definitions = discover_publish_definitions(args.package_path)
            if args.publish_command == "validate":
                selected_definitions = filter_publish_definitions(
                    discovered_definitions,
                    domain=args.domain,
                    publish_name=args.publish_name,
                )
                if not selected_definitions:
                    raise ConfigValidationError(
                        message="No publish definitions matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "domain": args.domain,
                            "publish_name": args.publish_name,
                        },
                    )
                payload = {
                    "command": "publish.validate",
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": args.domain,
                        "publish_name": args.publish_name,
                    },
                    "publish_count": len(selected_definitions),
                    "definitions": [
                        {
                            "publish_id": definition.publish_id,
                            "manifest_path": str(definition.manifest_path),
                            "query_path": (
                                str(definition.query_path)
                                if definition.query_path is not None
                                else None
                            ),
                            "source_dataset": definition.manifest.source.dataset,
                            "selection_mode": definition.manifest.source.selection_mode.value,
                            "output_format": definition.manifest.delivery.output_format.value,
                            "replacement_mode": definition.manifest.delivery.replacement_mode.value,
                        }
                        for definition in selected_definitions
                    ],
                }
                print(json.dumps(payload, indent=2))
                return 0

            publish_environment = args.environment
            selection_domain = args.domain
            selection_publish = args.publish_name
            selection_window_start = args.window_start
            selection_window_end = args.window_end
            selection_window_label = args.window_label
            publish_backfill = getattr(args, "backfill", False)
            rerun_run_id = getattr(args, "rerun_run_id", None)

            if args.publish_command == "run" and rerun_run_id:
                _validate_publish_rerun_request(args)
                rerun_selection = _resolve_publish_rerun_selection(
                    root_path=path_normalize(args.root_path),
                    rerun_run_id=rerun_run_id,
                )
                publish_environment = rerun_selection.environment
                selection_domain = None
                selection_publish = None
                selection_window_start = rerun_selection.window_start
                selection_window_end = rerun_selection.window_end
                selection_window_label = rerun_selection.window_label
                publish_backfill = rerun_selection.backfill
                selected_ids = set(rerun_selection.publish_ids)
                selected_definitions = [
                    definition
                    for definition in discovered_definitions
                    if definition.publish_id in selected_ids
                ]
                missing_publish_ids = sorted(
                    publish_id
                    for publish_id in selected_ids
                    if all(
                        definition.publish_id != publish_id
                        for definition in discovered_definitions
                    )
                )
                if missing_publish_ids:
                    raise ConfigValidationError(
                        message=(
                            "Rerun selection references publish definitions "
                            "that are not present"
                        ),
                        context={
                            "package_path": str(args.package_path),
                            "rerun_run_id": rerun_run_id,
                            "missing_publish_ids": missing_publish_ids,
                        },
                    )
            else:
                selected_definitions = filter_publish_definitions(
                    discovered_definitions,
                    domain=args.domain,
                    publish_name=args.publish_name,
                )
                if not selected_definitions:
                    raise ConfigValidationError(
                        message="No publish definitions matched the requested selection",
                        context={
                            "package_path": str(args.package_path),
                            "domain": args.domain,
                            "publish_name": args.publish_name,
                        },
                    )

            if (
                rerun_run_id
                and publish_environment != runtime_context.selected_environment()
            ):
                _publish_runtime_overrides = _load_runtime_overrides_from_env_or_args(
                    config_path=(
                        str(args.config_path)
                        if getattr(args, "config_path", None)
                        else None
                    ),
                    environment=publish_environment,
                )
            else:
                _publish_runtime_overrides = runtime_context.as_runtime_overrides()

            cli_window = _build_cli_window_selection(
                window_start=selection_window_start,
                window_end=selection_window_end,
                window_label=selection_window_label,
                backfill=publish_backfill,
            )
            run_context = build_job_runtime(
                stage=StageName.publish,
                job_name=getattr(args, "job_name", "publish-explain"),
                environment=publish_environment,
                trigger_type=getattr(args, "trigger_type", "manual"),
                window=cli_window,
                backfill=publish_backfill,
                attributes=_with_orchestration_attributes(
                    {
                        "package_path": str(args.package_path),
                        "domain_selection": selection_domain or "",
                        "publish_selection": selection_publish or "",
                        "rerun_of_run_id": rerun_run_id or "",
                    }
                ),
            ).to_run_context()

            if args.publish_command == "explain":
                payload = {
                    "command": "publish.explain",
                    "run_id": run_context.run_id,
                    "environment": publish_environment,
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": selection_domain,
                        "publish_name": selection_publish,
                        "window_start": _serialize_datetime(cli_window.start),
                        "window_end": _serialize_datetime(cli_window.end),
                        "window_label": cli_window.label,
                        "backfill": publish_backfill,
                    },
                    "publish_count": len(selected_definitions),
                    "plans": explain_publish_definitions(
                        root_path=path_normalize(args.root_path),
                        run_context=run_context,
                        definitions=selected_definitions,
                    ),
                }
                print(json.dumps(payload, indent=2))
                return 0

            if args.publish_command == "run":
                _validate_iceberg_catalog_binding(
                    args, runtime_overrides=_publish_runtime_overrides
                )
                publish_spark = build_spark_session(
                    **_resolve_iceberg_session_kwargs(
                        args=args,
                        app_name=f"elt-pipeline-publish-{getattr(args, 'job_name', 'publish-run')}",
                        runtime_overrides=_publish_runtime_overrides,
                    )
                )
                serving_endpoint = _build_serving_endpoint(
                    args, runtime_overrides=_publish_runtime_overrides
                )
                try:
                    result = run_publish_definitions_locally(
                        root_path=path_normalize(args.root_path),
                        run_context=run_context,
                        environment=publish_environment,
                        package_path=args.package_path,
                        warehouse_root=args.warehouse_root,
                        spark=publish_spark,
                        definitions=selected_definitions,
                        serving_endpoint=serving_endpoint,
                    )
                finally:
                    publish_spark.stop()
                payload = {
                    "command": "publish.run",
                    "run_id": run_context.run_id,
                    "environment": publish_environment,
                    "package_path": str(args.package_path),
                    "selection": {
                        "domain": selection_domain,
                        "publish_name": selection_publish,
                        "window_start": _serialize_datetime(cli_window.start),
                        "window_end": _serialize_datetime(cli_window.end),
                        "window_label": cli_window.label,
                        "backfill": publish_backfill,
                        "rerun_run_id": rerun_run_id,
                    },
                    "warehouse_root": str(args.warehouse_root),
                    "publish_count": len(result.results),
                    "results": [
                        {
                            "publish_id": publish_result.publish_id,
                            "row_count": publish_result.row_count,
                            "artifacts": [
                                artifact.model_dump(mode="json")
                                for artifact in publish_result.artifacts
                            ],
                            "validations": [
                                validation.model_dump(mode="json")
                                for validation in publish_result.validations
                            ],
                        }
                        for publish_result in result.results
                    ],
                    "artifacts": {
                        "artifact_root": str(result.artifacts.artifact_root),
                        "run_dir": str(result.artifacts.run_dir),
                        "export_manifest_path": (
                            str(result.artifacts.export_manifest_path)
                            if result.artifacts.export_manifest_path is not None
                            else None
                        ),
                        "audit_path": (
                            str(result.artifacts.audit_path)
                            if result.artifacts.audit_path is not None
                            else None
                        ),
                        "log_path": (
                            str(result.artifacts.log_path)
                            if result.artifacts.log_path is not None
                            else None
                        ),
                        "lineage_path": (
                            str(result.artifacts.lineage_path)
                            if result.artifacts.lineage_path is not None
                            else None
                        ),
                        "error_path": (
                            str(result.artifacts.error_path)
                            if result.artifacts.error_path is not None
                            else None
                        ),
                    },
                    "serving_endpoint": serving_endpoint,
                }
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "schedule":
            plan = load_schedule_plan(args.plan_path)
            continue_on_error = args.continue_on_error or plan.continue_on_error
            payload, exit_code = _run_schedule_plan(
                plan=plan,
                plan_path=path_normalize(args.plan_path),
                continue_on_error=continue_on_error,
            )
            print(json.dumps(payload, indent=2))
            return exit_code
    except (ConfigValidationError, ValidationError) as exc:
        error_record = build_error_record(
            run_id="unassigned",
            error_code="CONFIG_VALIDATION_FAILED",
            error_category="config_error",
            message=str(exc),
            retryable=False,
        )
        print(json.dumps(error_record.model_dump(mode="json"), indent=2), file=sys.stderr)
        return 2
    except PipelineError as exc:
        error_record = build_error_record(
            run_id="unassigned",
            error_code=exc.error_code,
            error_category=exc.error_category.value,
            message=str(exc),
            retryable=exc.retryable,
            context=exc.context,
        )
        print(json.dumps(error_record.model_dump(mode="json"), indent=2), file=sys.stderr)
        return 1

    parser.error(f"Unhandled command: {args.command}")
    return 1


def _add_sql_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("package_path", type=Path)
    parser.add_argument(
        "--stage",
        choices=[stage.value for stage in SqlModelStage],
    )
    parser.add_argument("--domain")
    parser.add_argument("--model")
    parser.add_argument("--environment", default="default")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--vars-json")
    parser.add_argument(
        "--partition",
        action="append",
        default=[],
        help="Partition override in key=value form. May be passed multiple times.",
    )


def _add_publish_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("package_path", type=Path)
    parser.add_argument("--domain")
    parser.add_argument("--publish", dest="publish_name")
    parser.add_argument("--environment", default="default")


def _run_schedule_plan(
    *,
    plan: SchedulePlan,
    plan_path: str,
    continue_on_error: bool,
) -> tuple[dict[str, Any], int]:
    job_results: list[dict[str, Any]] = []
    overall_exit_code = 0

    for position, job in enumerate(plan.jobs, start=1):
        exit_code, stdout_text, stderr_text = _invoke_cli_job(job.argv)
        job_results.append(
            {
                "name": job.name,
                "position": position,
                "argv": job.argv,
                "status": "success" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "output": parse_schedule_payload(stdout_text),
                "error": parse_schedule_payload(stderr_text),
            }
        )
        if exit_code != 0:
            overall_exit_code = exit_code
            if not continue_on_error:
                break

    return (
        {
            "command": "schedule.run",
            "plan_path": str(plan_path),
            "job_count": len(plan.jobs),
            "executed_count": len(job_results),
            "continue_on_error": continue_on_error,
            "success": overall_exit_code == 0,
            "jobs": job_results,
        },
        overall_exit_code,
    )


def _invoke_cli_job(argv: list[str]) -> tuple[int, str, str]:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    exit_code = 0
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exit_code = main(argv)
    except SystemExit as exc:
        if isinstance(exc.code, int):
            exit_code = exc.code
        else:
            exit_code = 1
    except Exception as exc:  # pragma: no cover - defensive containment for schedule runs
        exit_code = 1
        stderr_buffer.write(str(exc))
    return exit_code, stdout_buffer.getvalue().strip(), stderr_buffer.getvalue().strip()


def _parse_vars_json(raw_value: str | None) -> dict[str, Any]:
    if raw_value is None:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse --vars-json payload: {exc}",
            context={"vars_json": raw_value},
        ) from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError(
            message="--vars-json must decode to a JSON object",
            context={"vars_json": raw_value},
        )
    return payload


def _parse_partition_values(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        key, separator, raw_partition_value = value.partition("=")
        if not separator or not key.strip():
            raise ConfigValidationError(
                message="Partition values must use key=value format",
                context={"partition": value},
            )
        parsed[key.strip()] = raw_partition_value
    return parsed


def _parse_datetime_argument(*, field_name: str, raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized:
        raise ConfigValidationError(
            message=f"{field_name} must not be empty",
            context={field_name: raw_value},
        )
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfigValidationError(
            message=f"{field_name} must be an ISO-8601 date or datetime",
            context={field_name: raw_value},
        ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _build_cli_window_selection(
    *,
    window_start: str | None,
    window_end: str | None,
    window_label: str | None,
    backfill: bool,
) -> ExecutionWindow:
    start = _parse_datetime_argument(field_name="window_start", raw_value=window_start)
    end = _parse_datetime_argument(field_name="window_end", raw_value=window_end)
    if backfill and start is None:
        raise ConfigValidationError(
            message="--backfill requires --window-start",
            context={"window_start": window_start},
        )
    label = window_label.strip() if window_label and window_label.strip() else None
    try:
        return ExecutionWindow(start=start, end=end, label=label)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="window_end must be greater than or equal to window_start",
            context={
                "window_start": window_start,
                "window_end": window_end,
            },
        ) from exc


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _validate_normalize_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.source:
        conflicting_args.append("--source")
    if args.entity:
        conflicting_args.append("--entity")
    if args.manifest_path:
        conflicting_args.append("--manifest-path")
    if args.window_start:
        conflicting_args.append("--window-start")
    if args.window_end:
        conflicting_args.append("--window-end")
    if args.window_label:
        conflicting_args.append("--window-label")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message=(
                "normalize reruns must not specify an explicit selection "
                "alongside --rerun-run-id"
            ),
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _validate_sql_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.stage:
        conflicting_args.append("--stage")
    if args.domain:
        conflicting_args.append("--domain")
    if args.model:
        conflicting_args.append("--model")
    if args.include_deps:
        conflicting_args.append("--include-deps")
    if args.start_date:
        conflicting_args.append("--start-date")
    if args.end_date:
        conflicting_args.append("--end-date")
    if args.vars_json:
        conflicting_args.append("--vars-json")
    if args.partition:
        conflicting_args.append("--partition")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message="sql reruns must not specify an explicit selection alongside --rerun-run-id",
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _validate_publish_rerun_request(args: argparse.Namespace) -> None:
    conflicting_args = []
    if args.domain:
        conflicting_args.append("--domain")
    if args.publish_name:
        conflicting_args.append("--publish")
    if args.window_start:
        conflicting_args.append("--window-start")
    if args.window_end:
        conflicting_args.append("--window-end")
    if args.window_label:
        conflicting_args.append("--window-label")
    if args.backfill:
        conflicting_args.append("--backfill")
    if args.environment != "default":
        conflicting_args.append("--environment")
    if conflicting_args:
        raise ConfigValidationError(
            message=(
                "publish reruns must not specify an explicit selection "
                "alongside --rerun-run-id"
            ),
            context={
                "rerun_run_id": args.rerun_run_id,
                "conflicting_args": conflicting_args,
            },
        )


def _resolve_entity_selections(
    config: PipelineConfig,
    *,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
) -> list[ResolvedEntityConfig]:
    if entity_name and not source_name:
        raise ConfigValidationError(
            message="--entity requires --source",
            context={"entity_name": entity_name},
        )

    if source_name:
        try:
            source = config.get_source(source_name)
        except LookupError as exc:
            raise ConfigValidationError(
                message=str(exc),
                context={"source_name": source_name},
            ) from exc
        entities = [entity.name for entity in source.entities]
        if entity_name is not None:
            entities = [entity_name]
        return [
            resolve_entity_config(
                config,
                environment=environment,
                source_name=source_name,
                entity_name=selected_entity_name,
            )
            for selected_entity_name in entities
        ]

    return [
        resolve_entity_config(
            config,
            environment=environment,
            source_name=source.name,
            entity_name=entity.name,
        )
        for source in config.sources
        for entity in source.entities
    ]


def _run_ingest_entity(
    *,
    resolved_config: ResolvedEntityConfig,
    root_path: str,
    job_name: str,
    trigger_type: str,
    kafka_log_path: str | None,
    cli_window: ExecutionWindow,
    backfill: bool,
) -> dict[str, Any]:
    connector_type = resolved_config.connector_type
    root_path = path_normalize(root_path)
    checkpoint_override = _resolve_checkpoint_override(
        root_path=root_path,
        resolved_config=resolved_config,
        window=cli_window,
        backfill=backfill,
    )
    try:
        run_context = build_job_runtime(
            stage=StageName.ingest,
            job_name=job_name,
            environment=resolved_config.environment,
            trigger_type=trigger_type,
            source_name=resolved_config.source_name,
            entity_name=resolved_config.entity_name,
            window=cli_window,
            backfill=backfill,
            checkpoint_seed=checkpoint_override.value,
            attributes=_with_orchestration_attributes(
                {
                    "connector_type": connector_type,
                    "backfill": backfill,
                }
            ),
        ).to_run_context()
    except ValueError as exc:
        raise ConfigValidationError(
            message=str(exc),
            context={
                "job_name": job_name,
                "trigger_type": trigger_type,
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
            },
        ) from exc

    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)
    artifact_store.append_log_event(
        run_context=run_context,
        environment=resolved_config.environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="ingest",
            event_type="ingest_run_start",
            message="Ingest run started",
            details={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
                "connector_type": connector_type,
                "window_start": _serialize_datetime(cli_window.start),
                "window_end": _serialize_datetime(cli_window.end),
                "window_label": cli_window.label,
                "backfill": backfill,
            },
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=resolved_config.environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="source",
                    name=f"{resolved_config.source_name}/{resolved_config.entity_name}",
                    facets={"connector_type": connector_type},
                )
            ],
        ),
    )

    completed_at: datetime | None = None
    status = "success"
    error_summary: dict[str, str] | None = None
    failure: PipelineError | None = None
    result = None

    try:
        if connector_type == "rest":
            result = _CliLocalRestConnector(
                config=RestConnectorConfig.from_resolved_entity_config(resolved_config),
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "sql":
            result = _CliLocalSqlConnector(
                config=SqlConnectorConfig.from_resolved_entity_config(resolved_config),
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "object_storage":
            result = _CliLocalObjectStorageConnector(
                config=ObjectStorageConnectorConfig.from_resolved_entity_config(resolved_config),
                run_context=run_context,
                root_path=root_path,
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        elif connector_type == "kafka":
            result = _CliLocalKafkaConnector(
                config=KafkaConnectorConfig.from_resolved_entity_config(resolved_config),
                run_context=run_context,
                root_path=root_path,
                log_path=_resolve_kafka_log_path(
                    resolved_config=resolved_config,
                    explicit_log_path=kafka_log_path,
                ),
                checkpoint_override=checkpoint_override,
                window=cli_window,
            ).run()
        else:
            raise ConfigValidationError(
                message="Unsupported connector type for local ingest CLI",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": connector_type,
                },
            )
        completed_at = datetime.now(tz=UTC)
    except PipelineError as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = exc
        error_summary = {
            "error_code": exc.error_code,
            "error_category": exc.error_category.value,
            "message": str(exc),
        }
        artifact_store.append_error_record(
            run_context=run_context,
            environment=resolved_config.environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=exc.error_code,
                error_category=exc.error_category,
                message=str(exc),
                retryable=exc.retryable,
                context=exc.context,
            ),
        )
    except Exception as exc:
        completed_at = datetime.now(tz=UTC)
        status = "failed"
        failure = PipelineError(
            message="Unexpected ingest failure",
            error_code="INGEST_UNEXPECTED_ERROR",
            error_category=ErrorCategory.unexpected_runtime_error,
            retryable=True,
            context={
                "exception_type": type(exc).__name__,
                "message": str(exc),
            },
        )
        error_summary = {
            "error_code": failure.error_code,
            "error_category": failure.error_category.value,
            "message": str(failure),
        }
        artifact_store.append_error_record(
            run_context=run_context,
            environment=resolved_config.environment,
            error_record=build_error_record(
                run_id=run_context.run_id,
                error_code=failure.error_code,
                error_category=failure.error_category,
                message=str(failure),
                retryable=failure.retryable,
                context=failure.context,
            ),
        )
    finally:
        manifests: list[Level1ArtifactManifest] = []
        if result is not None:
            manifests = list(getattr(result, "manifests", []) or [])

        total_record_estimates = [
            manifest.record_count_estimate
            for manifest in manifests
            if manifest.record_count_estimate is not None
        ]
        records_written = (
            sum(total_record_estimates) if total_record_estimates else None
        )
        metrics_extra: dict[str, int | float | str] = {
            "connector_type": connector_type,
            "backfill": str(backfill).lower(),
        }
        for field_name in (
            "request_count",
            "response_count",
            "query_count",
            "row_count",
            "message_count",
            "objects_discovered",
            "objects_copied",
            "bytes_copied",
        ):
            value = getattr(result, field_name, None) if result is not None else None
            if isinstance(value, (int, float)):
                metrics_extra[field_name] = value

        artifact_store.write_audit_record(
            run_context=run_context,
            environment=resolved_config.environment,
            audit_record=AuditRecord(
                run_id=run_context.run_id,
                stage=run_context.stage.value,
                job_name=run_context.job_name,
                trigger_type=run_context.trigger_type,
                started_at=run_context.started_at,
                completed_at=completed_at,
                status=status,
                config_version=None,
                metrics_summary=MetricsSummary(
                    records_read=records_written,
                    records_written=records_written,
                    files_written=len(manifests),
                    extra=metrics_extra,
                ),
                error_summary=error_summary,
                context={
                    "environment": resolved_config.environment,
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": connector_type,
                    "root_path": str(root_path),
                    "window_start": _serialize_datetime(cli_window.start) or "",
                    "window_end": _serialize_datetime(cli_window.end) or "",
                    "window_label": cli_window.label or "",
                    "checkpoint_seeded": str(checkpoint_override.active).lower(),
                },
            ),
        )
        lineage_adapter.emit(
            run_context=run_context,
            environment=resolved_config.environment,
            lineage_event=LineageEvent(
                event_type="COMPLETE" if status == "success" else "FAIL",
                run_id=run_context.run_id,
                job_name=run_context.job_name,
                inputs=[
                    DatasetRef(
                        namespace="source",
                        name=f"{resolved_config.source_name}/{resolved_config.entity_name}",
                        facets={"connector_type": connector_type},
                    )
                ],
                outputs=[
                    DatasetRef(
                        namespace="local",
                        name=manifest.data_path,
                        facets={
                            "artifact_id": manifest.artifact_id,
                            "content_hash": manifest.content_hash,
                            "payload_format": manifest.payload_format,
                            "manifest_path": manifest.manifest_path,
                        },
                    )
                    for manifest in manifests
                ],
            ),
        )
        artifact_store.append_log_event(
            run_context=run_context,
            environment=resolved_config.environment,
            log_event=build_log_event(
                run_context=run_context,
                severity="INFO" if status == "success" else "ERROR",
                component="ingest",
                event_type="ingest_run_complete",
                message="Ingest run completed",
                details={
                    "status": status,
                    "artifact_count": len(manifests),
                },
            ),
        )

    if failure is not None:
        raise failure

    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": resolved_config.source_name,
        "entity_name": resolved_config.entity_name,
        "connector_type": connector_type,
        "window_start": _serialize_datetime(cli_window.start),
        "window_end": _serialize_datetime(cli_window.end),
        "window_label": cli_window.label,
        "backfill": backfill,
        "result": result.model_dump(mode="json"),
    }


def _resolve_kafka_log_path(
    *,
    resolved_config: ResolvedEntityConfig,
    explicit_log_path: str | None,
) -> str:
    if explicit_log_path is not None:
        return path_normalize(explicit_log_path)
    candidate = (
        resolved_config.extraction.get("log_path")
        or resolved_config.settings.get("log_path")
        or resolved_config.state.get("log_path")
    )
    if not candidate:
        raise ConfigValidationError(
            message="Kafka local ingest requires log_path in config or --kafka-log-path",
            context={
                "source_name": resolved_config.source_name,
                "entity_name": resolved_config.entity_name,
            },
        )
    return path_normalize(str(candidate))


def _select_level1_manifests(
    *,
    root_path: str,
    environment: str,
    source_name: str | None,
    entity_name: str | None,
    explicit_manifest_paths: list[str],
    window_start: datetime | None,
    window_end: datetime | None,
) -> list[Level1ArtifactManifest]:
    if entity_name and not source_name:
        raise ConfigValidationError(
            message="--entity requires --source",
            context={"entity_name": entity_name},
        )

    root_path = path_normalize(root_path)
    manifests = [
        _read_level1_manifest(path=manifest_path, root_path=root_path)
        for manifest_path in (
            explicit_manifest_paths
            if explicit_manifest_paths
            else sorted(path_rglob(join_paths(root_path, "level1"), "*.manifest.json"))
        )
    ]
    filtered_manifests = [
        manifest
        for manifest in manifests
        if manifest.environment == environment
        and (source_name is None or manifest.source_name == source_name)
        and (entity_name is None or manifest.entity_name == entity_name)
        and _manifest_matches_window(
            manifest=manifest,
            window_start=window_start,
            window_end=window_end,
        )
    ]
    if not filtered_manifests:
        raise ConfigValidationError(
            message="No level1 manifests matched the requested selection",
            context={
                "root_path": root_path,
                "environment": environment,
                "source": source_name,
                "entity": entity_name,
                "manifest_paths": explicit_manifest_paths,
                "window_start": _serialize_datetime(window_start),
                "window_end": _serialize_datetime(window_end),
            },
        )
    return filtered_manifests


def _read_level1_manifest(*, path: str, root_path: str) -> Level1ArtifactManifest:
    manifest_path = (
        path if detect_scheme(path) != _StorageScheme.local_unschemed or path.startswith("/")
        else join_paths(root_path, path)
    )
    if not path_exists(manifest_path):
        raise ConfigValidationError(
            message="Level1 manifest path does not exist",
            context={"manifest_path": manifest_path},
        )
    try:
        payload = json.loads(path_read_text(manifest_path, encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse level1 manifest JSON: {exc}",
            context={"manifest_path": manifest_path},
        ) from exc
    try:
        return Level1ArtifactManifest.model_validate(payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Level1 manifest validation failed",
            context={
                "manifest_path": manifest_path,
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def _resolve_normalize_rerun_manifest(
    *,
    root_path: str,
    rerun_run_id: str,
) -> Level1ArtifactManifest:
    root_path = path_normalize(root_path)
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.normalize,
        rerun_run_id=rerun_run_id,
    )
    manifest_path = audit.context.get("input_manifest_path")
    if manifest_path:
        return _read_level1_manifest(path=str(manifest_path), root_path=root_path)

    artifact_id = audit.context.get("input_artifact_id")
    if not artifact_id:
        raise ConfigValidationError(
            message="Normalize rerun audit is missing input_artifact_id",
            context={"rerun_run_id": rerun_run_id},
        )

    for candidate_path in sorted(
        path_rglob(join_paths(root_path, "level1"), "*.manifest.json"),
    ):
        manifest = _read_level1_manifest(path=candidate_path, root_path=root_path)
        if manifest.artifact_id == artifact_id:
            return manifest

    raise ConfigValidationError(
        message="Normalize rerun could not resolve the prior level1 manifest",
        context={"rerun_run_id": rerun_run_id, "input_artifact_id": artifact_id},
    )


def _resolve_sql_rerun_selection(
    *,
    root_path: str,
    rerun_run_id: str,
) -> _SqlRerunSelection:
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.sql,
        rerun_run_id=rerun_run_id,
    )
    environment = audit.context.get("environment")
    selected_models = [
        model_id
        for model_id in audit.context.get("selected_models", "").split(",")
        if model_id
    ]
    if not environment or not selected_models:
        raise ConfigValidationError(
            message="SQL rerun audit is missing the required selection context",
            context={"rerun_run_id": rerun_run_id},
        )
    return _SqlRerunSelection(
        environment=environment,
        model_ids=tuple(selected_models),
        start_date=audit.context.get("window_start") or None,
        end_date=audit.context.get("window_end") or None,
        partition_values=_load_json_string_dict(
            raw_value=audit.context.get("partition_values"),
            field_name="partition_values",
            rerun_run_id=rerun_run_id,
        ),
        extra_values=_load_json_object_dict(
            raw_value=audit.context.get("extra_values"),
            field_name="extra_values",
            rerun_run_id=rerun_run_id,
        ),
    )


def _resolve_publish_rerun_selection(
    *,
    root_path: str,
    rerun_run_id: str,
) -> _PublishRerunSelection:
    audit = _load_stage_audit_record(
        root_path=root_path,
        stage=StageName.publish,
        rerun_run_id=rerun_run_id,
    )
    environment = audit.context.get("environment")
    selected_publishes = [
        publish_id
        for publish_id in audit.context.get("selected_publish_ids", "").split(",")
        if publish_id
    ]
    if not environment or not selected_publishes:
        raise ConfigValidationError(
            message="Publish rerun audit is missing the required selection context",
            context={"rerun_run_id": rerun_run_id},
        )
    return _PublishRerunSelection(
        environment=environment,
        publish_ids=tuple(selected_publishes),
        window_start=audit.context.get("window_start") or None,
        window_end=audit.context.get("window_end") or None,
        window_label=audit.context.get("window_label") or None,
        backfill=(
            audit.trigger_type == "backfill"
            or audit.context.get("checkpoint_mode") == "backfill"
        ),
    )


def _load_stage_audit_record(
    *,
    root_path: str,
    stage: StageName,
    rerun_run_id: str,
) -> AuditRecord:
    stage_root = join_paths(path_normalize(root_path), "runs", f"stage={stage.value}")
    if not path_exists(stage_root):
        raise ConfigValidationError(
            message="No run artifacts exist for the requested stage",
            context={
                "root_path": path_normalize(root_path),
                "stage": stage.value,
                "rerun_run_id": rerun_run_id,
            },
        )
    audit_paths = sorted(
        path_rglob(
            stage_root,
            f"run_id={rerun_run_id}/audit.json",
        ),
    )
    if not audit_paths:
        raise ConfigValidationError(
            message="No prior run artifacts matched --rerun-run-id",
            context={
                "root_path": path_normalize(root_path),
                "stage": stage.value,
                "rerun_run_id": rerun_run_id,
            },
        )
    audit_path = audit_paths[0]
    try:
        payload = json.loads(path_read_text(audit_path, encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse audit artifact JSON: {exc}",
            context={"audit_path": audit_path, "rerun_run_id": rerun_run_id},
        ) from exc
    try:
        return AuditRecord.model_validate(payload)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Audit artifact validation failed",
            context={
                "audit_path": audit_path,
                "rerun_run_id": rerun_run_id,
                "errors": exc.errors(include_url=False),
            },
        ) from exc


def _load_json_string_dict(
    *,
    raw_value: str | None,
    field_name: str,
    rerun_run_id: str,
) -> dict[str, str]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must contain valid JSON",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        ) from exc
    if not isinstance(payload, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must decode to a string map",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        )
    return payload


def _with_orchestration_attributes(
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    combined_attributes = dict(attributes or {})
    orchestration_metadata = load_orchestration_metadata_from_env()
    if orchestration_metadata is not None:
        combined_attributes.update(orchestration_metadata.to_run_attributes())
    return combined_attributes


def _load_json_object_dict(
    *,
    raw_value: str | None,
    field_name: str,
    rerun_run_id: str,
) -> dict[str, Any]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must contain valid JSON",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        ) from exc
    if not isinstance(payload, dict) or any(not isinstance(key, str) for key in payload):
        raise ConfigValidationError(
            message=f"Rerun audit field '{field_name}' must decode to an object",
            context={"rerun_run_id": rerun_run_id, field_name: raw_value},
        )
    return payload


def _run_normalize_manifest(
    *,
    manifest: Level1ArtifactManifest,
    resolved_config: ResolvedEntityConfig,
    root_path: str,
    job_name: str,
    trigger_type: str,
    backfill: bool,
    rerun_run_id: str | None,
    partition_strategy: PartitionStrategy,
    spark: SparkSession,
) -> dict[str, Any]:
    try:
        run_context = build_job_runtime(
            stage=StageName.normalize,
            job_name=job_name,
            environment=manifest.environment,
            trigger_type=trigger_type,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            window=ExecutionWindow(
                start=manifest.window_start,
                end=manifest.window_end,
                label=manifest.window_label,
            ),
            backfill=backfill,
            attributes=_with_orchestration_attributes(
                {
                    "input_artifact_id": manifest.artifact_id,
                    "input_manifest_path": manifest.manifest_path,
                    "rerun_of_run_id": rerun_run_id or "",
                }
            ),
        ).to_run_context()
    except ValueError as exc:
        raise ConfigValidationError(
            message=str(exc),
            context={
                "job_name": job_name,
                "trigger_type": trigger_type,
                "source_name": manifest.source_name,
                "entity_name": manifest.entity_name,
                "input_artifact_id": manifest.artifact_id,
            },
        ) from exc
    level2_mode = resolved_config.level2_mode
    if level2_mode == "bypass_level2":
        return _bypass_normalize_manifest(
            manifest=manifest,
            root_path=path_normalize(root_path),
            run_context=run_context,
            rerun_run_id=rerun_run_id,
            level2_mode=level2_mode,
        )

    summary = normalize_level1_to_local_level2(
        root_path=path_normalize(root_path),
        run_context=run_context,
        manifest=manifest,
        payload=join_paths(path_normalize(root_path), manifest.data_path),
        spark=spark,
        partition_strategy=partition_strategy,
    )
    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": manifest.source_name,
        "entity_name": manifest.entity_name,
        "level2_mode": level2_mode,
        "bypassed": False,
        "input_artifact_id": manifest.artifact_id,
        "mapping_catalog_path": summary.mapping_catalog_path,
        "table_manifests": [
            table_manifest.model_dump(mode="json") for table_manifest in summary.table_manifests
        ],
    }


def _bypass_normalize_manifest(
    *,
    manifest: Level1ArtifactManifest,
    root_path: str,
    run_context,
    rerun_run_id: str | None,
    level2_mode: Level2Mode,
) -> dict[str, Any]:
    artifact_store = LocalArtifactStore(root_path)
    lineage_adapter = build_lineage_adapter(root_path)

    artifact_store.append_log_event(
        run_context=run_context,
        environment=manifest.environment,
        log_event=build_log_event(
            run_context=run_context,
            severity="INFO",
            component="normalize",
            event_type="normalize_bypassed",
            message="Physical level2 normalization was bypassed by configuration",
            details={
                "source_name": manifest.source_name,
                "entity_name": manifest.entity_name,
                "level2_mode": level2_mode,
                "input_artifact_id": manifest.artifact_id,
            },
        ),
    )
    lineage_adapter.emit(
        run_context=run_context,
        environment=manifest.environment,
        lineage_event=LineageEvent(
            event_type="START",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="local",
                    name=manifest.data_path,
                    facets={
                        "artifact_id": manifest.artifact_id,
                        "content_hash": manifest.content_hash,
                    },
                )
            ],
        ),
    )

    audit = AuditRecord(
        run_id=run_context.run_id,
        stage=run_context.stage.value,
        job_name=run_context.job_name,
        trigger_type=run_context.trigger_type,
        started_at=run_context.started_at,
        completed_at=run_context.started_at,
        status="success",
        config_version=None,
        metrics_summary=MetricsSummary(
            records_read=manifest.record_count_estimate,
            records_written=manifest.record_count_estimate,
            files_written=0,
            extra={
                "level2_mode": level2_mode,
                "bypassed_level2": "true",
            },
        ),
        context={
            "environment": manifest.environment,
            "source_name": manifest.source_name,
            "entity_name": manifest.entity_name,
            "input_artifact_id": manifest.artifact_id,
            "input_manifest_path": manifest.manifest_path,
            "level2_mode": level2_mode,
            "bypassed": "true",
            "source_data_path": manifest.data_path,
        },
    )
    if rerun_run_id:
        audit.context["rerun_of_run_id"] = rerun_run_id
    artifact_store.write_audit_record(
        run_context=run_context,
        environment=manifest.environment,
        audit_record=audit,
    )

    lineage_adapter.emit(
        run_context=run_context,
        environment=manifest.environment,
        lineage_event=LineageEvent(
            event_type="COMPLETE",
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            inputs=[
                DatasetRef(
                    namespace="local",
                    name=manifest.data_path,
                    facets={
                        "artifact_id": manifest.artifact_id,
                        "content_hash": manifest.content_hash,
                    },
                )
            ],
            outputs=[
                DatasetRef(
                    namespace="local",
                    name=manifest.data_path,
                    facets={
                        "artifact_id": manifest.artifact_id,
                        "level2_mode": level2_mode,
                        "bypassed": True,
                    },
                )
            ],
        ),
    )

    return {
        "run_id": run_context.run_id,
        "job_name": run_context.job_name,
        "trigger_type": run_context.trigger_type,
        "source_name": manifest.source_name,
        "entity_name": manifest.entity_name,
        "level2_mode": level2_mode,
        "bypassed": True,
        "input_artifact_id": manifest.artifact_id,
        "mapping_catalog_path": None,
        "table_manifests": [],
        "source_data_path": manifest.data_path,
        "source_manifest_path": manifest.manifest_path,
    }


def _manifest_matches_window(
    *,
    manifest: Level1ArtifactManifest,
    window_start: datetime | None,
    window_end: datetime | None,
) -> bool:
    if window_start is None and window_end is None:
        return True

    manifest_start = manifest.window_start or manifest.ingest_started_at
    manifest_end = manifest.window_end or manifest.ingest_completed_at
    if window_start is not None and manifest_end < window_start:
        return False
    if window_end is not None and manifest_start > window_end:
        return False
    return True


def _resolve_checkpoint_override(
    *,
    root_path: str,
    resolved_config: ResolvedEntityConfig,
    window: ExecutionWindow,
    backfill: bool,
) -> _CheckpointOverride:
    if not backfill:
        return _CheckpointOverride(active=False, value=None)

    checkpoint_store = LocalCheckpointStore(root_path)
    seed_entry = checkpoint_store.resolve_backfill_seed(
        environment=resolved_config.environment,
        source_name=resolved_config.source_name,
        entity_name=resolved_config.entity_name,
        window_start=window.start,
    )
    return _CheckpointOverride(
        active=True,
        value=seed_entry.checkpoint_after if seed_entry is not None else None,
    )


class _CliCheckpointOverrideMixin:
    def __init__(
        self,
        *,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        self._checkpoint_override = checkpoint_override
        self._cli_window = window

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        if self._checkpoint_override.active:
            return self._checkpoint_override.value
        return super().resolve_checkpoint_before()


class _CliLocalRestConnector(_CliCheckpointOverrideMixin, LocalRestConnector):
    def __init__(
        self,
        *,
        config: RestConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalRestConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def resolve_window(self):
        if self._cli_window.start is None and self._cli_window.end is None:
            return super().resolve_window()
        return RestRequestWindow(
            start=self._cli_window.start,
            end=self._cli_window.end,
            label=self._cli_window.label,
        )


class _CliLocalSqlConnector(_CliCheckpointOverrideMixin, LocalSqlConnector):
    def __init__(
        self,
        *,
        config: SqlConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalSqlConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "sql"},
        )
        return None


class _CliLocalObjectStorageConnector(
    _CliCheckpointOverrideMixin,
    LocalObjectStorageConnector,
):
    def __init__(
        self,
        *,
        config: ObjectStorageConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalObjectStorageConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "object_storage"},
        )
        return None


class _CliLocalKafkaConnector(_CliCheckpointOverrideMixin, LocalKafkaConnector):
    def __init__(
        self,
        *,
        config: KafkaConnectorConfig,
        run_context,
        root_path: str,
        log_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalKafkaConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
            log_path=log_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "kafka"},
        )
        return None

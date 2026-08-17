"""Runtime configuration SINGLETON (Mercell/Camellos pattern).

This module implements the exact config pattern from Mercell/Camellos:

1. **One-shot materialization at entry point**
   :func:`initialize` is called exactly ONCE from the top of
   :func:`elt_pipeline.cli.main` (or any bootstrap entry point).  It runs the
   full 4-tier cascade and stores the **FINAL resolved value** for every known
   config key — not "overrides".

2. **Singleton + immutable**
   The materialized store is a module-level singleton (``_SINGLETON``) that,
   once set, cannot be mutated.  :func:`initialize` raises
   :class:`RuntimeError` if called twice.

3. **Zero OS-specific magic for framework consumers**
   The only place in the ENTIRE framework that reads ``os.environ``
   (``ELT_PIPELINE_*``) or parses the YAML is the materializer in this module.
   Downstream components use :func:`get`, :func:`get_dict`, or
   :func:`repo_root` — plain key-value lookups with no platform specifics.

4. **Any framework component can reference it**
   No threading of ``runtime_overrides`` through 5 layers of calls.  Any
   consumer (CLI helpers, Spark builder, SQL executor, Trino bootstrap) simply
   imports this module and calls :func:`get`.

4-tier cascade (applied exactly ONCE here, never in consumers)::

    1. explicit kwargs passed to initialize()         (highest)
    2. OS ENV ELT_PIPELINE_*                           (optional override)
    3. pipeline.yaml runtime: section                  (user edits this)
    4. frozen dataclass defaults in runtime_manifest   (floor)

Typical usage::

    # At ENTRY POINT (cli.main, once)
    from elt_pipeline.config import runtime_context
    runtime_context.initialize(config_path_arg=None, environment_arg=None)

    # Anywhere else in the framework
    from elt_pipeline.config import runtime_context
    port = runtime_context.get("trino_serving.port")
    writer_type = runtime_context.get("iceberg_writer.catalog_type")
    repo = runtime_context.repo_root()
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .loader import load_runtime_overrides
from .runtime_manifest import runtime_manifest

# ---------------------------------------------------------------------------
# Singleton storage
# ---------------------------------------------------------------------------

_SINGLETON: "_RuntimeSingleton | None" = None


@dataclass(frozen=True)
class _RuntimeSingleton:
    """Immutable single source of truth for runtime configuration.

    Frozen dataclass (immutable) prevents any downstream mutation of the
    composed configuration — eliminates drift from components that would
    otherwise "tweak" values mid-pipeline.
    """

    repo_root: Path
    """Absolute repo-root anchor — explicitly captured here, never re-derived."""

    config_path_resolved: Path | None
    """Absolute path to the pipeline YAML actually loaded, or None."""

    config_path_source: str
    """Origin label: "arg" | "env" | "repo_root_auto" | "manifest_fallback"."""

    environment: str | None
    """Selected environment overlay name passed to the YAML loader."""

    values: dict[str, Any] = field(default_factory=dict)
    """Final materialized values.  Flat dotted keys like
    ``"spark.enable_iceberg"`` → final resolved value.

    Also exposes nested-dict access via :func:`get_dict` for consumers that
    prefer the original ``RuntimeConfig`` shape.
    """

    nested: dict[str, Any] = field(default_factory=dict)
    """Final materialized values as a nested dict (mirrors RuntimeConfig).
    ``nested["spark"]["enable_iceberg"]`` → same final value.
    """


# ---------------------------------------------------------------------------
# Dotted-key helpers
# ---------------------------------------------------------------------------


def _flatten(d: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested dict into flat dotted keys.

    >>> _flatten({"spark": {"enable_iceberg": True}})
    {'spark.enable_iceberg': True}
    """
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, Mapping):
            out.update(_flatten(v, key))
        else:
            out[key] = v
    return out


def _dotted_get(mapping: Mapping[str, Any], dotted: str, default: Any = None) -> Any:
    """Traverse a nested dict by dotted key: ``spark.enable_iceberg`` → value."""
    node: Any = mapping
    for part in dotted.split("."):
        if isinstance(node, Mapping):
            node = node.get(part)
        else:
            return default
        if node is None:
            return default
    return node if node is not None else default


# ---------------------------------------------------------------------------
# Materializer — the ONLY place that reads os.environ / YAML
# ---------------------------------------------------------------------------


def _materialize(
    *,
    config_path_arg: str | None,
    environment_arg: str | None,
) -> _RuntimeSingleton:
    """Materialize the singleton by running the full 4-tier cascade.

    **CRITICAL CONTRACT:** This function (together with
    :func:`_resolve_final_key`) is the ONLY place in the ENTIRE framework that
    is allowed to read ``os.environ`` or load the YAML file.  All downstream
    components read from the resulting singleton via :func:`get`.
    """
    env = runtime_manifest.env
    paths = runtime_manifest.paths

    # ---- 1) Resolve config_path + repo_root (same as _compose_runtime_context)
    repo_root = Path(__file__).resolve().parents[3]

    cp_source: str
    cp_resolved: Path | None
    if config_path_arg:
        cp_resolved = Path(config_path_arg).resolve()
        cp_source = "arg"
    else:
        env_cp = os.environ.get("ELT_PIPELINE_CONFIG_PATH", "").strip()
        if env_cp:
            cp_resolved = Path(env_cp).resolve()
            cp_source = "env"
        else:
            auto = repo_root / "pipeline.yaml"
            if auto.is_file():
                cp_resolved = auto
                cp_source = "repo_root_auto"
            else:
                cp_resolved = None
                cp_source = "manifest_fallback"

    # ---- 2) Load YAML (3-YAML-layer merge via existing helper)
    if cp_resolved is not None:
        ro = load_runtime_overrides(str(cp_resolved), environment=environment_arg)
    else:
        ro = {}

    # ---- 3) CLI default root/warehouse defaults (repo_run_dir → YAML → manifest)
    env_rrd = os.environ.get(env.repo_run_dir, "").strip()
    if env_rrd:
        repo_run_dir: Path | None = Path(env_rrd).expanduser()
    else:
        yaml_rrd = ro.get("repo_run_dir") if isinstance(ro, dict) else None
        if yaml_rrd:
            repo_run_dir = Path(str(yaml_rrd)).expanduser()
        else:
            home = Path(os.path.expanduser("~"))
            fallback_root = home / paths.default_user_repo_run_home
            canonical = fallback_root / paths.repo_run_results_elt_relpath
            repo_run_dir = canonical if fallback_root.exists() else None

    if repo_run_dir is not None:
        cli_default_root_path = str((repo_run_dir / "runtime").as_posix())
        cli_default_warehouse_root = str((repo_run_dir / "warehouse").as_posix())
    else:
        yaml_cr = (
            ro.get("cli_default_root_path") if isinstance(ro, dict) else None
        ) or None
        yaml_cw = (
            ro.get("cli_default_warehouse_root") if isinstance(ro, dict) else None
        ) or None
        cli_default_root_path = (
            str(yaml_cr) if yaml_cr else paths.cli_default_root_path
        )
        cli_default_warehouse_root = (
            str(yaml_cw) if yaml_cw else paths.cli_default_warehouse_root
        )

    # ---- 4) Per-key 4-tier cascade — apply once, store FINAL value
    #
    # For every known infrastructure key we resolve:
    #   (a) explicit arg to initialize (not used today, reserved for programmatic)
    #   (b) os.environ ELT_PIPELINE_*  — optional override
    #   (c) YAML via ro dotted path
    #   (d) manifest frozen default
    #
    # Consumers NEVER do this layering again. They just get() the final value.
    nested: dict[str, Any] = {}

    # repo_run_dir + cli defaults (flat: "repo_run_dir", "cli_default_root_path", …)
    nested["repo_run_dir"] = str(repo_run_dir) if repo_run_dir else None
    nested["cli_default_root_path"] = cli_default_root_path
    nested["cli_default_warehouse_root"] = cli_default_warehouse_root

    def _final(
        env_key: str | None,
        yaml_path: tuple[str, ...],
        manifest_default: Any,
        *,
        _explicit: Any = None,
        _lower: bool = False,
        _strip: bool = False,
    ) -> Any:
        """Resolve a single final value using the 4-tier cascade.

        This helper IS the materializer.  No consumer of the singleton should
        ever do this layering on their own.
        """
        if _explicit is not None:
            return _explicit
        if env_key:
            v = os.environ.get(env_key, "").strip() if _strip else os.environ.get(env_key, "")
            if _lower:
                v = v.lower()
            if v != "" and v is not None:
                return v
        from_ro = _dotted_get(ro if isinstance(ro, dict) else {}, ".".join(yaml_path))
        if from_ro is not None and from_ro != "":
            return from_ro
        return manifest_default

    # --- spark section
    cat = runtime_manifest.catalogs
    serv = runtime_manifest.serving
    ver = runtime_manifest.versions

    spark_conf: dict[str, Any] = {}
    spark_conf["master"] = _final(env.spark_master, ("spark", "master"), "local[*]")
    spark_conf["app_name"] = _final(env.spark_app_name, ("spark", "app_name"), "elt-pipeline")
    spark_conf["driver_host"] = _final(env.spark_driver_host, ("spark", "driver_host"), None)
    spark_conf["driver_bind_address"] = _final(
        env.spark_driver_bind_address, ("spark", "driver_bind_address"), None
    )
    spark_conf["shuffle_partitions"] = int(
        _final(
            env.spark_shuffle_partitions,
            ("spark", "shuffle_partitions"),
            runtime_manifest.spark.default_shuffle_partitions,
        )
    )
    spark_conf["default_parallelism"] = int(
        _final(
            env.spark_default_parallelism,
            ("spark", "default_parallelism"),
            runtime_manifest.spark.default_parallelism,
        )
    )
    spark_conf["enable_iceberg"] = (
        _final(
            env.iceberg_enabled,
            ("spark", "enable_iceberg"),
            None,
            _lower=True,
        )
    )
    # normalize enable_iceberg: ENV strings "true/1/yes/on" → True etc.
    ei = spark_conf["enable_iceberg"]
    if isinstance(ei, str):
        e_low = ei.lower()
        if e_low in ("true", "1", "yes", "on"):
            spark_conf["enable_iceberg"] = True
        elif e_low in ("false", "0", "no", "off"):
            spark_conf["enable_iceberg"] = False
    spark_conf["adaptive_query_execution"] = (
        _final(
            env.spark_aqe,
            ("spark", "adaptive_query_execution"),
            runtime_manifest.spark.default_adaptive_enabled,
        )
    )
    # normalize AQE to bool if string
    aqe = spark_conf["adaptive_query_execution"]
    if isinstance(aqe, str):
        spark_conf["adaptive_query_execution"] = aqe.lower() in (
            "true",
            "1",
            "yes",
            "on",
        )
    nested["spark"] = spark_conf

    # --- iceberg_writer
    writer_conf: dict[str, Any] = {}
    writer_conf["catalog_name"] = _final(
        env.iceberg_catalog_name,
        ("iceberg_writer", "catalog_name"),
        cat.default_catalog_name,
    )
    writer_conf["catalog_type"] = (
        _final(
            env.iceberg_writer_catalog_type,
            ("iceberg_writer", "catalog_type"),
            cat.workstation_default_writer_catalog,
            _lower=True,
        )
        or _final(
            env.iceberg_catalog_type_legacy,
            (),
            cat.workstation_default_writer_catalog,
            _lower=True,
        )
    )
    writer_conf["warehouse_dir"] = _final(
        env.iceberg_warehouse_dir,
        ("iceberg_writer", "warehouse_dir"),
        "",
    )
    writer_conf["catalog_uri"] = _final(
        env.iceberg_catalog_uri,
        ("iceberg_serving", "catalog_uri"),
        "",
    )
    writer_conf["rest_token"] = _final(
        env.iceberg_rest_token,
        ("iceberg_writer", "rest_token"),
        "",
    )
    writer_conf["rest_warehouse"] = _final(
        env.iceberg_rest_warehouse,
        ("iceberg_writer", "rest_warehouse"),
        "",
    )
    writer_conf["glue_region"] = _final(
        env.iceberg_glue_region,
        ("iceberg_writer", "glue_region"),
        "",
    )
    writer_conf["hive_metastore_uri"] = _final(
        env.iceberg_hive_metastore_uri,
        ("iceberg_writer", "hive_metastore_uri"),
        "",
    )
    writer_conf["catalog_impl_override"] = _final(
        None,
        ("iceberg_writer", "catalog_impl_override"),
        None,
    )
    nested["iceberg_writer"] = writer_conf

    # --- iceberg_serving
    serving_conf: dict[str, Any] = {}
    serving_conf["catalog_name"] = _final(
        env.iceberg_catalog_name,
        ("iceberg_serving", "catalog_name"),
        cat.default_catalog_name,
    )
    serving_conf["catalog_type"] = _final(
        env.iceberg_serving_catalog_type,
        ("iceberg_serving", "catalog_type"),
        cat.workstation_default_serving_catalog,
        _lower=True,
    )
    serving_conf["catalog_uri"] = _final(
        env.iceberg_catalog_uri,
        ("iceberg_serving", "catalog_uri"),
        "",
    )
    serving_conf["jdbc_driver"] = _final(
        env.iceberg_jdbc_driver,
        ("iceberg_serving", "jdbc_driver"),
        "org.sqlite.JDBC",
    )
    # Auto-derive sqlite JDBC URI when jdbc + driver is sqlite + uri empty.
    # Prevents "catalog_uri required" validator errors for the zero-service
    # workstation default binding.
    _sct = str(serving_conf.get("catalog_type") or "").lower()
    _uri = str(serving_conf.get("catalog_uri") or "").strip()
    _drv = str(serving_conf.get("jdbc_driver") or "").lower()
    if (
        _sct == "jdbc"
        and not _uri
        and "sqlite" in _drv
        and repo_run_dir is not None
    ):
        _elt_run = (repo_run_dir / paths.repo_run_results_elt_relpath).as_posix()
        _tmpl = cat.workstation_default_serving_jdbc_sqlite_uri_template
        try:
            serving_conf["catalog_uri"] = _tmpl.format(repo_run_elt_dir=_elt_run)
        except Exception:  # noqa: BLE001 — never crash materializer due to bad template
            pass
    serving_conf["catalog_impl_override"] = _final(
        None,
        ("iceberg_serving", "catalog_impl_override"),
        None,
    )
    nested["iceberg_serving"] = serving_conf

    # --- trino_serving
    trino_conf: dict[str, Any] = {}
    trino_conf["version"] = _final(
        env.trino_version, ("trino_serving", "version"), ver.trino_server
    )
    trino_conf["port"] = int(
        _final(env.trino_port, ("trino_serving", "port"), serv.default_trino_port)
    )
    trino_conf["host"] = _final(env.trino_host, ("trino_serving", "host"), serv.default_trino_host)
    trino_conf["jvm_xms_mb"] = int(
        _final(
            env.trino_jvm_xms_mb,
            ("trino_serving", "jvm_xms_mb"),
            serv.default_trino_xms_mb,
        )
    )
    trino_conf["jvm_xmx_mb"] = int(
        _final(
            env.trino_jvm_xmx_mb,
            ("trino_serving", "jvm_xmx_mb"),
            serv.default_trino_xmx_mb,
        )
    )
    trino_conf["http_authentication_type"] = _final(
        None,
        ("trino_serving", "http_authentication_type"),
        serv.default_http_server_authentication_type,
    )
    trino_conf["coordinator"] = bool(
        _final(
            None,
            ("trino_serving", "coordinator"),
            serv.default_coordinator,
        )
    )
    trino_conf["include_coordinator"] = bool(
        _final(
            None,
            ("trino_serving", "include_coordinator"),
            serv.default_include_coordinator,
        )
    )
    trino_conf["node_environment"] = _final(
        None,
        ("trino_serving", "node_environment"),
        serv.default_node_environment,
    )
    trino_conf["fs_hadoop_enabled"] = bool(
        _final(
            None,
            ("trino_serving", "fs_hadoop_enabled"),
            serv.always_emit_fs_hadoop_enabled,
        )
    )
    trino_conf["register_table_procedure_enabled"] = bool(
        _final(
            None,
            ("trino_serving", "register_table_procedure_enabled"),
            serv.always_enable_register_table_procedure,
        )
    )
    nested["trino_serving"] = trino_conf

    # --- spark extra (ivy_home)
    ivy_env_raw = os.environ.get(env.ivy_home, "").strip()
    if ivy_env_raw:
        spark_conf["ivy_home"] = str(Path(ivy_env_raw).expanduser().resolve())
    else:
        yaml_ivy = (
            _dotted_get(ro if isinstance(ro, dict) else {}, "spark.ivy_home")
        )
        if yaml_ivy not in (None, ""):
            spark_conf["ivy_home"] = str(Path(str(yaml_ivy)).expanduser().resolve())
        else:
            import pathlib as _pl

            cwd_default = _pl.Path.cwd() / paths.spark_ivy_relpath
            spark_conf["ivy_home"] = str(cwd_default.resolve())
    nested["spark"] = spark_conf

    # --- iceberg_serving extra (jdbc fields)
    serving_conf["jdbc_jars_extra"] = _final(
        env.iceberg_jdbc_jars_extra,
        ("iceberg_serving", "jdbc_jars_extra"),
        "",
    )
    serving_conf["jdbc_schema_version"] = _final(
        env.iceberg_jdbc_schema_version,
        ("iceberg_serving", "jdbc_schema_version"),
        "V1",
    )

    # --- publish
    _pmr_env = os.environ.get(env.publish_max_rows, "").strip()
    if _pmr_env:
        try:
            _pmr_val = int(_pmr_env)
            if _pmr_val > 0:
                publish_max_rows = _pmr_val
            else:
                publish_max_rows = 1_000_000
        except (TypeError, ValueError):
            publish_max_rows = 1_000_000
    else:
        _pmr_yaml = (
            _dotted_get(ro if isinstance(ro, dict) else {}, "publish.max_rows")
        )
        if _pmr_yaml not in (None, ""):
            try:
                _pmr_val = int(str(_pmr_yaml))
                publish_max_rows = _pmr_val if _pmr_val > 0 else 1_000_000
            except (TypeError, ValueError):
                publish_max_rows = 1_000_000
        else:
            publish_max_rows = 1_000_000
    nested["publish"] = {"max_rows": publish_max_rows}

    # --- env overlay metadata
    nested["environment"] = environment_arg
    nested["config_path_source"] = cp_source
    nested["config_path_resolved"] = str(cp_resolved) if cp_resolved else None
    nested["repo_root"] = str(repo_root)

    flat = _flatten(nested)

    return _RuntimeSingleton(
        repo_root=repo_root,
        config_path_resolved=cp_resolved,
        config_path_source=cp_source,
        environment=environment_arg,
        values=flat,
        nested=nested,
    )


# ---------------------------------------------------------------------------
# Public API — used by EVERY framework component
# ---------------------------------------------------------------------------


def initialize(
    *,
    config_path_arg: str | None = None,
    environment_arg: str | None = None,
) -> None:
    """Initialize the runtime singleton (ENTRY POINT — call exactly ONCE).

    Called from the top of :func:`elt_pipeline.cli.main` immediately after
    argument parsing.  Running the 4-tier cascade here means **no downstream
    framework component ever has to read ``os.environ`` or parse YAML**.

    Raises:
        RuntimeError: If called a second time (guards against drift from
            accidental re-materialization with different inputs).
    """
    global _SINGLETON
    if _SINGLETON is not None:
        raise RuntimeError(
            "runtime_context.initialize() called a second time — singleton must "
            "be initialized exactly once from the single framework entry point "
            "(Mercell/Camellos pattern).  If you are testing, call "
            "runtime_context._reset_for_tests() first."
        )
    _SINGLETON = _materialize(
        config_path_arg=config_path_arg,
        environment_arg=environment_arg,
    )


def is_initialized() -> bool:
    """Return True if the singleton has been materialized."""
    return _SINGLETON is not None


def _ensure() -> _RuntimeSingleton:
    if _SINGLETON is None:
        # Lazy bootstrap fallback for tests / direct API consumers that haven't
        # gone through main().  Note: this still goes through the SAME
        # materializer — so the 4-tier cascade still happens ONCE here.  It
        # does NOT open a second config-read path.
        initialize(config_path_arg=None, environment_arg=None)
        assert _SINGLETON is not None, "initialize should have set _SINGLETON"
    return _SINGLETON


# ---- Public accessors (any framework component can call these)


def repo_root() -> Path:
    """Absolute repo root anchor.

    Entry-point explicit; never re-derived from ``__file__`` downstream.
    """
    return _ensure().repo_root


def config_path() -> Path | None:
    """Return the YAML path that was actually loaded (or None for manifest-floor-only)."""
    return _ensure().config_path_resolved


def config_source() -> str:
    """Origin label: "arg" | "env" | "repo_root_auto" | "manifest_fallback"."""
    return _ensure().config_path_source


def selected_environment() -> str | None:
    """Environment overlay name used for YAML layer merge."""
    return _ensure().environment


def get(dotted_key: str, default: Any = None) -> Any:
    """Look up a final materialized value by dotted key.

    Examples::

        runtime_context.get("spark.enable_iceberg")  # True | False | None
        runtime_context.get("iceberg_writer.catalog_type")  # "hadoop" | "rest" | …
        runtime_context.get("trino_serving.port")  # 8080

    This is the canonical interface in the Mercell/Camellos pattern: flat
    dotted keys, simple lookups.  The value returned has ALREADY been through
    the full 4-tier cascade (arg > ENV > YAML > manifest) — consumers should
    NEVER apply additional tiers.
    """
    return _ensure().values.get(dotted_key, default)


def get_dict(namespace: str | None = None) -> dict[str, Any]:
    """Return the materialized values as a nested dict (original RuntimeConfig shape).

    :func:`get` with dotted keys is preferred.  This accessor exists for
    legacy callers (Spark builder, ``load_runtime_overrides`` style nested
    iteration).
    """
    n = _ensure().nested
    if namespace is None:
        return n
    sub = n.get(namespace)
    return sub if isinstance(sub, dict) else {}


def as_runtime_overrides() -> dict[str, Any]:
    """Return the singleton nested dict shaped like ``runtime_overrides``.

    Drop-in for code that currently expects a ``runtime_overrides`` dict from
    :func:`load_runtime_overrides` — e.g. ``build_spark_session`` internal
    layered resolution.  Eventually callers should be migrated to dotted
    :func:`get`, but this keeps existing code working during the transition.
    """
    n = _ensure().nested
    ro_shape: dict[str, Any] = {}
    for k in (
        "repo_run_dir",
        "spark",
        "iceberg_writer",
        "iceberg_serving",
        "trino_serving",
        "cli_default_root_path",
        "cli_default_warehouse_root",
    ):
        if k in n:
            ro_shape[k] = n[k]
    return ro_shape


# ---------------------------------------------------------------------------
# Test hook (NOT for production use — resets the singleton)
# ---------------------------------------------------------------------------


def _reset_for_tests() -> None:
    """Reset singleton state.  Only for tests.

    Explicitly NOT exported; production code should never call this.
    """
    global _SINGLETON
    _SINGLETON = None

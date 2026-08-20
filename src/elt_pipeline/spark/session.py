from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from elt_pipeline.config import runtime_context
from elt_pipeline.config.runtime_manifest import runtime_manifest
from elt_pipeline.shared.secrets import resolve_secret_ref

_DEFAULT_MASTER = runtime_manifest.spark.default_master

_DEFAULT_ICEBERG_CATALOG_NAME = runtime_manifest.catalogs.default_catalog_name
_DEFAULT_ICEBERG_WRITER_CATALOG_TYPE = (
    runtime_manifest.catalogs.workstation_default_writer_catalog
)


# ---------------------------------------------------------------------------
# Spark Hadoop FS cloud config builder (BACKLOG item B-4)
#
# Strategy: resolve the standard Spark Hadoop FS keys for S3 (s3a://), GCS (gs://),
# and ADLS Gen2 (abfss://).  Credential values are secret_ref URIs
# resolved through resolve_secret_ref() with strict=True — if the operator
# explicitly gave a ref but it's missing, we fail fast and sharp.  When no
# explicit credentials are provided (empty strings), we emit NO credential keys at
# all — Spark's default credential chain takes over:
#   - S3: DefaultAWSCredentialsProviderChain (env → instance profile
#   - GCS: ADC / workload identity / metadata service
#   - ADLS: DefaultAzureCredential / MSI / metadata service
# This matches platform convention and avoids breaking ambient-IAM deployments.
# ---------------------------------------------------------------------------


def _resolve_cred_ref(ref: str | None, *, label: str) -> str | None:
    """Resolve a single credential secret_ref URI.

    * ``ref`` is non-empty string → treat as secret_ref, resolve strict=True.
      Raises ``Secret*Error`` on failure (fail-fast: operator explicitly
      opted in, we must honour it).
    * ``ref`` is None/empty → return None → caller skips the Spark config key
      (default credential chain).
    """
    if ref is None:
        return None
    stripped = str(ref).strip()
    if not stripped:
        return None
    val = resolve_secret_ref(stripped, strict=True)
    return str(val)


def _resolve_path_ref(ref: str | None, *, label: str) -> str | None:
    """Resolve a secret_ref to a filesystem PATH string (NOT the file's contents).

    Used for GCS SA keyfile, where Spark's Hadoop FS connector expects a
    *filesystem path* to a JSON SA keyfile (``json.keyfile`` config key),
    not the in-memory JSON contents.

    * ``file:///abs/path`` → return ``/abs/path`` verbatim (Spark's JVM side reads it).
    * ``env://VAR`` → resolve env var value, treat the value string as a filesystem path.
    * bare ref (no scheme) → default to env:// (same secret_ref convention as everywhere else).
    * None/empty → None (default ADC / workload identity chain).
    * Unknown explicit schemes → raise SecretRefSyntaxError (fail-fast, same as secrets subsystem).
    """
    from elt_pipeline.shared.secrets import SecretScheme, parse_secret_ref

    if ref is None:
        return None
    stripped = str(ref).strip()
    if not stripped:
        return None
    parsed = parse_secret_ref(stripped)
    if parsed.scheme == SecretScheme.file:
        return parsed.path
    if parsed.scheme == SecretScheme.env:
        val = resolve_secret_ref(stripped, strict=True)
        return str(val).strip()
    from elt_pipeline.shared.secrets import SecretRefSyntaxError

    raise SecretRefSyntaxError(
        message=(
            f"{label}: only file:// and env:// schemes are supported for path-type "
            f"refs (got scheme {parsed.scheme.value!r}). For cloud-secret schemes, "
            f"store the keyfile path in an env var and reference env://VAR."
        ),
        context={"ref_repr": stripped},
    )


def build_spark_fs_hadoop_configs(
    *,
    s3_access_key_ref: str | None = None,
    s3_secret_key_ref: str | None = None,
    s3_region: str | None = None,
    s3_endpoint: str | None = None,
    gcs_sa_keyfile_ref: str | None = None,
    gcs_project_id: str | None = None,
    adls_account_name: str | None = None,
    adls_account_key_ref: str | None = None,
    adls_tenant_id: str | None = None,
    adls_client_id_ref: str | None = None,
    adls_client_secret_ref: str | None = None,
    adls_use_msi: str | bool | None = None,
) -> dict[str, str]:
    """Build a dict of ``spark.hadoop.fs.*`` configs for the configured backends.

    The returned dict uses Spark Hadoop FS config keys ready to be passed to
    ``SparkSession.Builder.config(key, value)`` one by one.  Only backends
    with at least one explicitly configured value are emitted; keys are emitted;
    backends with nothing configured are omitted so Spark's defaults apply unchanged.

    Returned keys are always ``str → str`` (Spark always stringifies everything anyway.

    Raises any ``Secret*Error`` from :func:`resolve_secret_ref` (strict mode)
    when an explicit credential ref fails to resolve — fail-fast with a clear
    message that names the parameter (backed by secrets subsystem error codes).
    """
    out: dict[str, str] = {}

    if isinstance(adls_use_msi, str):
        _adls_msi_norm = adls_use_msi.strip().lower()
        _adls_use_msi: bool = _adls_msi_norm in ("true", "1", "yes", "on")
    elif isinstance(adls_use_msi, bool):
        _adls_use_msi = adls_use_msi
    else:
        _adls_use_msi = False

    # ----- S3 (s3a://) ----------------------------------------------------------
    s3_active = any(
        v is not None and str(v).strip() != ""
        for v in (s3_access_key_ref, s3_secret_key_ref, s3_region, s3_endpoint)
    )
    if s3_active:
        out["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        s3_ak = _resolve_cred_ref(s3_access_key_ref, label="s3_access_key_ref")
        s3_sk = _resolve_cred_ref(s3_secret_key_ref, label="s3_secret_key_ref")
        if s3_ak is not None and s3_sk is not None:
            out["spark.hadoop.fs.s3a.access.key"] = s3_ak
            out["spark.hadoop.fs.s3a.secret.key"] = s3_sk
        elif s3_ak is not None or s3_sk is not None:
            from elt_pipeline.shared.errors import ErrorCategory, PipelineError

            raise PipelineError(
                message=(
                    "spark_fs S3 configuration incomplete: both s3_access_key_ref and "
                    "s3_secret_key_ref must be set together (got only one "
                    "was provided. Omit both to use the default AWS credential "
                    "chain (instance profile / env vars)."
                ),
                error_code="SPARK_FS_S3_CRED_MISMATCH",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "s3_access_key_ref_set": s3_ak is not None,
                    "s3_secret_key_ref_set": s3_sk is not None,
                },
            )
        s3_reg = (s3_region or "").strip()
        if s3_reg:
            out["spark.hadoop.fs.s3a.endpoint.region"] = s3_reg
        s3_ep = (s3_endpoint or "").strip()
        if s3_ep:
            out["spark.hadoop.fs.s3a.endpoint"] = s3_ep

    # ----- GCS (gs://) ----------------------------------------------------------
    gcs_active = any(
        v is not None and str(v).strip() != ""
        for v in (gcs_sa_keyfile_ref, gcs_project_id)
    )
    if gcs_active:
        out["spark.hadoop.fs.gs.impl"] = (
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        out["spark.hadoop.fs.AbstractFileSystem.gs.impl"] = (
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        gcs_proj = (gcs_project_id or "").strip()
        if gcs_proj:
            out["spark.hadoop.fs.gs.project.id"] = gcs_proj
        gcs_keyfile = _resolve_path_ref(gcs_sa_keyfile_ref, label="gcs_sa_keyfile_ref")
        if gcs_keyfile is not None:
            out["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
            out[
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile"
            ] = gcs_keyfile

    # ----- ADLS Gen2 (abfss://) ---------------------------------------------------
    adls_any_creds_configured = any(
        v is not None and str(v).strip() != ""
        for v in (adls_account_key_ref, adls_client_id_ref, adls_client_secret_ref)
    )
    adls_active = adls_any_creds_configured or _adls_use_msi or (
        adls_account_name is not None and str(adls_account_name).strip() != ""
    )
    if adls_active:
        acct = (adls_account_name or "").strip()
        if not acct:
            from elt_pipeline.shared.errors import ErrorCategory, PipelineError

            raise PipelineError(
                message=(
                    "spark_fs ADLS configuration requires "
                    "spark_fs.adls_account_name when any other ADLS config "
                    "(account_key / client creds / MSI) is configured."
                ),
                error_code="SPARK_FS_ADLS_ACCOUNT_REQUIRED",
                error_category=ErrorCategory.config_error,
                retryable=False,
            )
        acct_host = f"{acct}.dfs.core.windows.net"

        # Determine auth mode: shared_key → Service Principal → MSI → (default)
        acct_key = _resolve_cred_ref(adls_account_key_ref, label="adls_account_key_ref")
        client_id = _resolve_cred_ref(adls_client_id_ref, label="adls_client_id_ref")
        client_secret = _resolve_cred_ref(
            adls_client_secret_ref, label="adls_client_secret_ref"
        )

        if acct_key is not None:
            out[f"spark.hadoop.fs.azure.account.key.{acct_host}"] = acct_key
        elif client_id is not None and client_secret is not None and adls_tenant_id:
            out[f"spark.hadoop.fs.azure.account.auth.type.{acct_host}"] = "OAuth"
            out[
                f"spark.hadoop.fs.azure.account.oauth.provider.type.{acct_host}"
            ] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            out[f"spark.hadoop.fs.azure.account.oauth2.client.id.{acct_host}"] = client_id
            out[
                f"spark.hadoop.fs.azure.account.oauth2.client.secret.{acct_host}"
            ] = client_secret
            out[
                f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{acct_host}"
            ] = f"https://login.microsoftonline.com/{adls_tenant_id}/oauth2/token"
        elif client_id is not None or client_secret is not None or adls_tenant_id:
            from elt_pipeline.shared.errors import ErrorCategory, PipelineError

            raise PipelineError(
                message=(
                    "spark_fs ADLS Service Principal auth: adls_tenant_id, "
                    "adls_client_id_ref, and adls_client_secret_ref must all be "
                    "set together."
                ),
                error_code="SPARK_FS_ADLS_SP_INCOMPLETE",
                error_category=ErrorCategory.config_error,
                retryable=False,
            )
        elif _adls_use_msi:
            out[f"spark.hadoop.fs.azure.account.auth.type.{acct_host}"] = "OAuth"
            out[
                f"spark.hadoop.fs.azure.account.oauth.provider.type.{acct_host}"
            ] = "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        # else: no cred keys → Spark default chain

    return out


def _apply_spark_fs_configs(
    builder: Any,
    fs_conf: dict[str, Any],
) -> Any:
    """Apply fs_conf values into a ``SparkSession.Builder``.

    ``fs_conf`` is shaped like the materialized ``spark_fs`` nested dict from
    runtime_context. Converts to the flat spark.hadoop.fs.* keys via
    :func:`build_spark_fs_hadoop_configs` and calls ``.config(k, v)`` for each.

    Returns the builder (for chaining: ``builder = _apply_spark_fs_configs(builder, fs_conf)``).
    """
    if not isinstance(fs_conf, dict):
        return builder
    configs = build_spark_fs_hadoop_configs(
        s3_access_key_ref=fs_conf.get("s3_access_key_ref"),
        s3_secret_key_ref=fs_conf.get("s3_secret_key_ref"),
        s3_region=fs_conf.get("s3_region"),
        s3_endpoint=fs_conf.get("s3_endpoint"),
        gcs_sa_keyfile_ref=fs_conf.get("gcs_sa_keyfile_ref"),
        gcs_project_id=fs_conf.get("gcs_project_id"),
        adls_account_name=fs_conf.get("adls_account_name"),
        adls_account_key_ref=fs_conf.get("adls_account_key_ref"),
        adls_tenant_id=fs_conf.get("adls_tenant_id"),
        adls_client_id_ref=fs_conf.get("adls_client_id_ref"),
        adls_client_secret_ref=fs_conf.get("adls_client_secret_ref"),
        adls_use_msi=fs_conf.get("adls_use_msi"),
    )
    for k, v in configs.items():
        builder = builder.config(k, v)
    return builder


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
        # The WRITER catalog URI resolves from writer config only. It must NOT fall
        # back to the SERVING catalog URI: the serving catalog is a distinct (often
        # sqlite-JDBC) metastore, and inheriting it would let a rest/jdbc/nessie writer
        # catalog silently bind to a nonsensical URI instead of failing the
        # "requires iceberg_catalog_uri" guard below. Writer and serving are separate
        # catalogs by design (PRD 10 §7 hybrid dual-catalog binding).
        catalog_uri = _resolve(
            iceberg_catalog_uri,
            singleton_key="iceberg_writer.catalog_uri",
            override_path=("iceberg_writer", "catalog_uri"),
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

    # ----- Spark Hadoop FS cloud configs (B-4) -----------------------------------
    # Resolve spark_fs values through the same 4-tier cascade used by all other
    # builder knobs: explicit-param → singleton → ro → None (empty default).
    # Credential values remain as secret_ref URIs; _apply_spark_fs_configs calls
    # resolve_secret_ref(strict=True) to fail fast on any explicitly-configured
    # but unresolvable ref.  Empty/missing refs → default credential chain.
    fs_conf: dict[str, Any] = {
        "s3_access_key_ref": _resolve(
            None,
            singleton_key="spark_fs.s3_access_key_ref",
            override_path=("spark_fs", "s3_access_key_ref"),
        ),
        "s3_secret_key_ref": _resolve(
            None,
            singleton_key="spark_fs.s3_secret_key_ref",
            override_path=("spark_fs", "s3_secret_key_ref"),
        ),
        "s3_region": _resolve(
            None,
            singleton_key="spark_fs.s3_region",
            override_path=("spark_fs", "s3_region"),
        ),
        "s3_endpoint": _resolve(
            None,
            singleton_key="spark_fs.s3_endpoint",
            override_path=("spark_fs", "s3_endpoint"),
        ),
        "gcs_sa_keyfile_ref": _resolve(
            None,
            singleton_key="spark_fs.gcs_sa_keyfile_ref",
            override_path=("spark_fs", "gcs_sa_keyfile_ref"),
        ),
        "gcs_project_id": _resolve(
            None,
            singleton_key="spark_fs.gcs_project_id",
            override_path=("spark_fs", "gcs_project_id"),
        ),
        "adls_account_name": _resolve(
            None,
            singleton_key="spark_fs.adls_account_name",
            override_path=("spark_fs", "adls_account_name"),
        ),
        "adls_account_key_ref": _resolve(
            None,
            singleton_key="spark_fs.adls_account_key_ref",
            override_path=("spark_fs", "adls_account_key_ref"),
        ),
        "adls_tenant_id": _resolve(
            None,
            singleton_key="spark_fs.adls_tenant_id",
            override_path=("spark_fs", "adls_tenant_id"),
        ),
        "adls_client_id_ref": _resolve(
            None,
            singleton_key="spark_fs.adls_client_id_ref",
            override_path=("spark_fs", "adls_client_id_ref"),
        ),
        "adls_client_secret_ref": _resolve(
            None,
            singleton_key="spark_fs.adls_client_secret_ref",
            override_path=("spark_fs", "adls_client_secret_ref"),
        ),
        "adls_use_msi": _resolve(
            None,
            singleton_key="spark_fs.adls_use_msi",
            override_path=("spark_fs", "adls_use_msi"),
        ),
    }
    builder = _apply_spark_fs_configs(builder, fs_conf)

    return builder.getOrCreate()

"""Import-boundary guard for the elt_pipeline façade pattern (PCO modularisation).

- `cli` is a thin public façade over single-purpose internal modules (`_cli_*`)
- `shared.storage_backends` is a thin public façade over single-purpose internal
  modules (`_protocol`, `_clients`, `_local_backend`, `_s3_backend`, `_gcs_backend`,
  `_adls_backend`, `_registry`).
- `shared.secrets` is a thin public façade over single-purpose internal modules
  (`_models`, `_errors`, `_protocol`, `_providers_env`, `_providers_cloud`, `_registry`).
- `integrations.metrics`, `integrations.quality`, `integrations.orchestration` are
  each thin public facades over sibling `_*.py` implementation modules inside the
  package folder.

Consumers must import the public façade; never the internal submodules.
Otherwise the split cannot evolve without breaking downstream imports.
"""

from __future__ import annotations

import importlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def test_cli_facade_surface_is_stable() -> None:
    """The CLI façade re-exports the stable public + internal test surface."""
    cli = importlib.import_module("elt_pipeline.cli")

    required_callables = [
        "main",
        "build_parser",
        "_run_schedule_plan",
        "_invoke_cli_job",
        "_build_serving_endpoint",
        "_resolve_iceberg_session_kwargs",
        "_validate_iceberg_catalog_binding",
        "_compose_runtime_context",
        "_load_runtime_overrides_from_env_or_args",
        "_run_ingest_entity",
        "_run_normalize_manifest",
        "_bypass_normalize_manifest",
    ]
    for name in required_callables:
        assert hasattr(cli, name), f"elt_pipeline.cli missing required facade symbol: {name}"
        assert callable(getattr(cli, name))

    required_dataclasses = [
        "_RuntimeContext",
        "_CheckpointOverride",
        "_SqlRerunSelection",
        "_PublishRerunSelection",
    ]
    for name in required_dataclasses:
        assert hasattr(cli, name), f"elt_pipeline.cli missing required facade symbol: {name}"


def test_storage_backends_facade_surface_is_stable() -> None:
    """The storage_backends façade re-exports backends, helpers, and registry."""
    sb = importlib.import_module("elt_pipeline.shared.storage_backends")

    required_backends = [
        "StorageBackend",
        "SwapMode",
        "LocalBackend",
        "S3Backend",
        "GCSBackend",
        "ADLSBackend",
    ]
    for name in required_backends:
        assert hasattr(sb, name), f"storage_backends facade missing required symbol: {name}"

    required_registry = [
        "get_backend",
        "register_backend",
        "validate_swap_scheme",
        "atomic_swap",
        "build_staging_path",
        "best_effort_delete_staging",
    ]
    for name in required_registry:
        assert hasattr(sb, name), f"storage_backends facade missing required symbol: {name}"
        assert callable(getattr(sb, name))

    required_client_helpers = [
        "_S3_CLIENT",
        "_GCS_CLIENT",
        "_ADLS_CLIENT",
        "_get_s3_client",
        "_split_s3_path",
        "_get_gcs_client",
        "_split_gcs_path",
        "_get_adls_client",
        "_split_adls_path",
    ]
    for name in required_client_helpers:
        assert hasattr(sb, name), f"storage_backends facade missing internal symbol: {name}"


def test_secrets_facade_surface_is_stable() -> None:
    """The secrets façade re-exports wrappers, providers, and registry surface."""
    sec = importlib.import_module("elt_pipeline.shared.secrets")

    required_models = [
        "SecretValue",
        "SecretScheme",
        "ParsedSecretRef",
    ]
    for name in required_models:
        assert hasattr(sec, name), f"secrets facade missing model: {name}"

    required_errors = [
        "SecretsError",
        "SecretRefSyntaxError",
        "SecretNotFoundError",
        "SecretsNotImplementedError",
    ]
    for name in required_errors:
        assert hasattr(sec, name), f"secrets facade missing error type: {name}"

    required_providers = [
        "EnvVarSecrets",
        "FileSecrets",
        "AWSSecretsManagerSecrets",
        "AzureKeyVaultSecrets",
        "GCPSecretManagerSecrets",
        "VaultSecrets",
    ]
    for name in required_providers:
        assert hasattr(sec, name), f"secrets facade missing provider: {name}"

    required_registry = [
        "register_provider",
        "get_provider",
        "resolve_secret_ref",
        "resolve_secret_refs",
        "redact_secret",
        "parse_secret_ref",
    ]
    for name in required_registry:
        assert hasattr(sec, name), f"secrets facade missing registry fn: {name}"
        assert callable(getattr(sec, name))


def test_integrations_facade_surfaces_are_stable() -> None:
    """Each integrations facade re-exports the expected adapter + model surface."""
    metrics = importlib.import_module("elt_pipeline.integrations.metrics")
    metrics_required = [
        "ObservabilityPolicy",
        "MetricsExporter",
        "TraceExporter",
        "AlertHook",
        "PrometheusRemoteWriteExporter",
        "OtlpHttpTraceExporter",
        "WebhookAlertHook",
        "ObservabilityAdapter",
        "build_observability_adapter",
    ]
    for name in metrics_required:
        assert hasattr(metrics, name), f"metrics facade missing: {name}"

    quality = importlib.import_module("elt_pipeline.integrations.quality")
    quality_required = [
        "QualityHookPolicy",
        "QualityCheckStatus",
        "QualityDatasetRef",
        "QualityHookRequest",
        "QualityCheckResult",
        "QualityHookSummary",
        "QualityHookBackend",
        "RowCountQualityHook",
        "BuiltinQualityHook",
        "QualityHookAdapter",
        "build_quality_hook",
        "raise_for_blocking_quality_failures",
        "quality_error_already_recorded",
    ]
    for name in quality_required:
        assert hasattr(quality, name), f"quality facade missing: {name}"

    orch = importlib.import_module("elt_pipeline.integrations.orchestration")
    orch_required = [
        "OrchestrationMetadata",
        "CliInvocationRequest",
        "CliInvocationResult",
        "OrchestrationCliInvoker",
        "SubprocessCliInvoker",
        "AirflowCliWrapper",
        "DagsterCliWrapper",
        "PrefectCliWrapper",
        "MageCliWrapper",
        "load_orchestration_metadata_from_env",
        "build_airflow_orchestration_metadata",
        "build_dagster_orchestration_metadata",
        "build_prefect_orchestration_metadata",
        "build_mage_orchestration_metadata",
    ]
    for name in orch_required:
        assert hasattr(orch, name), f"orchestration facade missing: {name}"


def test_no_sibling_module_imports_cli_internals() -> None:
    """Only cli itself may import its `_cli_*` internals."""
    src_dir = REPO_ROOT / "src" / "elt_pipeline"
    offenders: list[str] = []

    banned_prefixes = (
        "from elt_pipeline._cli",
        "import elt_pipeline._cli",
    )

    for p in sorted(src_dir.rglob("*.py")):
        if p.name == "cli.py":
            continue
        if p.name.startswith("_cli"):
            continue
        txt = _read_text(p)
        if any(b in txt for b in banned_prefixes):
            offenders.append(str(p.relative_to(REPO_ROOT)))

    assert offenders == [], (
        "Sibling modules must import from elt_pipeline.cli facade, never _cli_* internals. "
        f"Offenders: {offenders}"
    )


def test_no_sibling_module_imports_storage_backends_internals() -> None:
    """Only storage_backends/__init__.py may import its `_*.py` internals."""
    sb_dir = REPO_ROOT / "src" / "elt_pipeline" / "shared" / "storage_backends"
    impl_dir = REPO_ROOT / "src" / "elt_pipeline"

    offenders: list[str] = []

    banned_prefixes = (
        "from elt_pipeline.shared.storage_backends._",
        "import elt_pipeline.shared.storage_backends._",
        "from .shared.storage_backends._",
    )

    for p in sorted(impl_dir.rglob("*.py")):
        if sb_dir in p.parents and p.name != "__init__.py":
            continue
        if p.name == "__init__.py" and p.parent == sb_dir:
            continue
        txt = _read_text(p)
        if any(b in txt for b in banned_prefixes):
            offenders.append(str(p.relative_to(REPO_ROOT)))

    assert offenders == [], (
        "Sibling modules must import from elt_pipeline.shared.storage_backends facade, "
        f"never _* internals. Offenders: {offenders}"
    )


def test_no_sibling_module_imports_secrets_internals() -> None:
    """Only shared/secrets/__init__.py may import its `_*.py` internals."""
    sec_dir = REPO_ROOT / "src" / "elt_pipeline" / "shared" / "secrets"
    impl_dir = REPO_ROOT / "src" / "elt_pipeline"

    offenders: list[str] = []

    banned_prefixes = (
        "from elt_pipeline.shared.secrets._",
        "import elt_pipeline.shared.secrets._",
        "from .shared.secrets._",
    )

    for p in sorted(impl_dir.rglob("*.py")):
        if sec_dir in p.parents and p.name != "__init__.py":
            continue
        if p.name == "__init__.py" and p.parent == sec_dir:
            continue
        txt = _read_text(p)
        if any(b in txt for b in banned_prefixes):
            offenders.append(str(p.relative_to(REPO_ROOT)))

    assert offenders == [], (
        "Sibling modules must import from elt_pipeline.shared.secrets facade, "
        f"never _* internals. Offenders: {offenders}"
    )


def test_no_sibling_module_imports_integrations_facade_internals() -> None:
    """Only each integrations facade __init__.py may import its own _* internals."""
    src_dir = REPO_ROOT / "src" / "elt_pipeline"
    offenders: list[str] = []

    package_banned_prefixes = {
        "metrics": (
            "from elt_pipeline.integrations.metrics._",
            "import elt_pipeline.integrations.metrics._",
        ),
        "quality": (
            "from elt_pipeline.integrations.quality._",
            "import elt_pipeline.integrations.quality._",
        ),
        "orchestration": (
            "from elt_pipeline.integrations.orchestration._",
            "import elt_pipeline.integrations.orchestration._",
        ),
    }

    for pkg, banned_prefixes in package_banned_prefixes.items():
        pkg_dir = REPO_ROOT / "src" / "elt_pipeline" / "integrations" / pkg
        for p in sorted(src_dir.rglob("*.py")):
            if pkg_dir in p.parents and p.name != "__init__.py":
                continue
            if p.name == "__init__.py" and p.parent == pkg_dir:
                continue
            txt = _read_text(p)
            if any(b in txt for b in banned_prefixes):
                offenders.append(f"{pkg}:{p.relative_to(REPO_ROOT)}")

    assert offenders == [], (
        "Sibling modules must import from each integrations facade, never _* internals. "
        f"Offenders: {offenders}"
    )

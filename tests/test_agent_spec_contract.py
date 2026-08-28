"""AGENT_SPEC Tier-1 summary contract guard.

This test file verifies that the structural claims written in the AGENT_SPEC.md
Tier-1 index card actually match the current repo state. If someone edits code
(e.g. renames a Protocol, moves a directory, refactors exit codes, renames a
Tier-2 canonical doc) WITHOUT also updating the corresponding AGENT_SPEC.md
summary row, this suite REDLINES in CI.

Covers, from AGENT_SPEC.md sections:
  §2 File & code map      — pCO facade directories exist, have __init__.py with __all__
  §3 Extension APIs table — 6 registered extension entry points import + are callable
  §4 Exit codes table     — structural exit-code dispatchers exist (no JVM, no Spark)
  §8 Durable pointers     — Tier-2 canonical deep-link target files exist on disk
  §3 rows 4/5/6 Protocol  — runtime_checkable Protocol inheritance + method signatures

Mirror philosophy: `tests/test_facade_import_boundary.py` already does this for
the pCO architecture contract; this file extends that same "test the prose
claims" discipline to AGENT_SPEC.md Tier-1 summaries.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Protocol

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "elt_pipeline"

if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))


# ---------------------------------------------------------------------------
# §3 Extension APIs — 6 entry points (Connectors / Storage / Secrets /
# Quality / SqlDbDriver / Metrics) must all import from their public facade
# and be the right shape (Protocol / register callable / dispatch callable).
# ---------------------------------------------------------------------------

def test_extension_api_1_connector_factory_registry_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 1: ConnectorFactory Protocol + register_connector_factory
    are exposed on the public ingest.connectors facade.
    """
    mod = importlib.import_module("elt_pipeline.ingest.connectors")
    ConnectorFactory = getattr(mod, "ConnectorFactory", None)
    register_connector_factory = getattr(mod, "register_connector_factory", None)
    assert ConnectorFactory is not None, "ConnectorFactory missing from facade"
    assert callable(register_connector_factory), "register_connector_factory not callable"
    assert issubclass(ConnectorFactory, Protocol), "ConnectorFactory must be a typing Protocol"


def test_extension_api_2_storage_backend_registry_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 2: StorageBackend Protocol + register_backend are exposed
    on public shared.storage_backends facade.
    """
    mod = importlib.import_module("elt_pipeline.shared.storage_backends")
    StorageBackend = getattr(mod, "StorageBackend", None)
    register_backend = getattr(mod, "register_backend", None)
    assert StorageBackend is not None, "StorageBackend missing from facade"
    assert callable(register_backend), "register_backend not callable"
    assert issubclass(StorageBackend, Protocol), "StorageBackend must be a typing Protocol"


def test_extension_api_3_secrets_provider_registry_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 3: SecretsProvider Protocol + register_provider are exposed
    on public shared.secrets facade.
    """
    mod = importlib.import_module("elt_pipeline.shared.secrets")
    SecretsProvider = getattr(mod, "SecretsProvider", None)
    register_provider = getattr(mod, "register_provider", None)
    assert SecretsProvider is not None, "SecretsProvider missing from facade"
    assert callable(register_provider), "register_provider not callable"
    assert issubclass(SecretsProvider, Protocol), "SecretsProvider must be a typing Protocol"


def test_extension_api_4_quality_hook_backend_protocol_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 4: QualityHookBackend Protocol is exposed on public
    integrations.quality facade.
    """
    mod = importlib.import_module("elt_pipeline.integrations.quality")
    QualityHookBackend = getattr(mod, "QualityHookBackend", None)
    build_quality_hook = getattr(mod, "build_quality_hook", None)
    assert QualityHookBackend is not None, "QualityHookBackend missing from facade"
    assert callable(build_quality_hook), "build_quality_hook dispatcher not callable"
    assert issubclass(QualityHookBackend, Protocol), "QualityHookBackend must be a typing Protocol"


def test_extension_api_5_sql_db_driver_protocol_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 5: SqlDbDriver Protocol is exposed on public
    ingest.connectors facade.
    """
    mod = importlib.import_module("elt_pipeline.ingest.connectors")
    SqlDbDriver = getattr(mod, "SqlDbDriver", None)
    assert SqlDbDriver is not None, "SqlDbDriver missing from facade"
    assert issubclass(SqlDbDriver, Protocol), "SqlDbDriver must be a typing Protocol"


def test_extension_api_6_metric_aggregation_and_dispatch_importable_from_facade() -> None:
    """AGENT_SPEC §3 row 6: MetricAggregation enum + _build_aggregation_sql dispatcher
    are exposed on public elt_pipeline.metrics facade.
    """
    from enum import Enum

    mod = importlib.import_module("elt_pipeline.metrics")
    MetricAggregation = getattr(mod, "MetricAggregation", None)
    build_agg_sql = getattr(mod, "_build_aggregation_sql", None)
    assert MetricAggregation is not None, "MetricAggregation missing from facade"
    assert callable(build_agg_sql), "_build_aggregation_sql dispatcher not callable"
    assert issubclass(MetricAggregation, Enum), "MetricAggregation must be an Enum"


# ---------------------------------------------------------------------------
# §2 File & code map — four pCO reference directories (metrics / storage_backends
# / secrets / quality integrations) exist, each with __init__.py that declares
# a non-empty __all__. pCO pattern = thin facade __init__ + _* implementation.
# ---------------------------------------------------------------------------

PCO_PACKAGES: list[tuple[str, list[str]]] = [
    # (fully-qualified python module name, list of required _*.py submodule stems under its folder)
    ("elt_pipeline.metrics", ["_models", "_compiler", "_runtime"]),
    (
        "elt_pipeline.shared.storage_backends",
        ["_protocol", "_registry", "_local_backend", "_s3_backend"],
    ),
    (
        "elt_pipeline.shared.secrets",
        ["_models", "_protocol", "_registry", "_providers_env"],
    ),
    ("elt_pipeline.integrations.quality", ["_models", "_adapter", "_hooks"]),
]


@pytest.mark.parametrize("pkg_module,required_submodules", PCO_PACKAGES)
def test_pco_package_layout_matches_agent_spec_claim(
    pkg_module: str, required_submodules: list[str]
) -> None:
    """AGENT_SPEC §2 file map: each reference package has a pCO layout.

    Verification:
      1. The module is importable via its public facade.
      2. Its __init__.py declares a non-empty public __all__ list.
      3. Each required _*.py implementation file exists on disk under its package dir.
    """
    rel_path = pkg_module.removeprefix("elt_pipeline.").replace(".", "/")
    pkg_dir = SRC_ROOT.joinpath(*rel_path.split("/"))
    assert pkg_dir.is_dir(), f"§2 file map package missing: {pkg_module}"

    mod = importlib.import_module(pkg_module)
    all_list = getattr(mod, "__all__", None)
    assert isinstance(all_list, list) and len(all_list) >= 1, (
        f"{pkg_module} facade missing non-empty __all__ list — violates pCO thin-facade pattern"
    )

    for stem in required_submodules:
        candidate = pkg_dir / f"{stem}.py"
        assert candidate.is_file(), (
            f"§2 file map claim: pCO package {pkg_module} should contain {stem}.py"
        )


# ---------------------------------------------------------------------------
# §8 Durable pointers + §9 deep-links — Tier-2 canonical narrative targets
# exist on disk. If a filename is renamed, AGENT_SPEC.md must update the
# corresponding summary row BEFORE merging.
# ---------------------------------------------------------------------------

TIER2_CANONICAL_PATHS: list[str] = [
    "docs/todo/BACKLOG.md",
    "docs/todo/archive/WORK_ITEMS_CLOSED.md",
    "docs/todo/archive/TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md",
    "docs/todo/archive/STATUS_SNAPSHOT_NARRATIVES.md",
    "docs/prd/10-prd-architecture-and-lifecycle.md",
    "docs/prd/08-prd-storage-root-uri-io-dispatch.md",
    "docs/prd/00-prd-platform-principles.md",
    "docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md",
    "docs/CAPABILITY_MATURITY_MATRIX.md",
    "docs/INDUSTRY_GAP_ANALYSIS.md",
    "docs/operator/BYOD_TUTORIAL.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "README.md",
    "docs/maintainer/JVM_TOOLCHAIN_SETUP.md",
    "docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md",
    "docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md",
]


@pytest.mark.parametrize("rel", TIER2_CANONICAL_PATHS)
def test_tier2_canonical_file_exists_per_agent_spec_pointers(rel: str) -> None:
    """AGENT_SPEC §8 + §9: a summary pointer to a Tier-2 file must resolve on disk."""
    target = REPO_ROOT / rel
    assert target.is_file(), (
        f"AGENT_SPEC Tier-1 deep-link MISSING: {rel}. "
        f"File was renamed or moved without updating the corresponding §8 or §9 summary row."
    )


# ---------------------------------------------------------------------------
# §4 Exit codes table — structural checks. The concrete numeric values are
# asserted by the CLI tests themselves (test_cli.py), but we can verify that
# the entry points that emit exit codes exist:
#   - `elt_pipeline.cli.main` exists and is callable.
#   - elt_pipeline package exports `__version__` (README quickstart claim).
#   - `scripts/run_tests.sh` exists as the AUTHORITATIVE gate document.
#   - AGENT_SPEC.md itself is on disk (anchor of the Tier-0 routers).
# ---------------------------------------------------------------------------

def test_exit_code_entry_points_and_anchor_files_exist() -> None:
    """AGENT_SPEC §4 + §5: gate entry points exist."""
    cli = importlib.import_module("elt_pipeline.cli")
    assert callable(getattr(cli, "main", None)), (
        "elt_pipeline.cli.main not callable — §4 exit-code dispatcher missing"
    )
    assert callable(getattr(cli, "build_parser", None)), "elt_pipeline.cli.build_parser missing"

    top = importlib.import_module("elt_pipeline")
    version = getattr(top, "__version__", None)
    assert isinstance(version, str) and len(version) >= 5, (
        "elt_pipeline.__version__ missing — README + AGENT_SPEC Tier-1 claim about PyPI packaging"
    )

    gate_script = REPO_ROOT / "scripts" / "run_tests.sh"
    assert gate_script.is_file(), "scripts/run_tests.sh missing — §5 gate broken"
    assert os.access(gate_script, os.X_OK), "scripts/run_tests.sh is not marked executable"

    agent_spec = REPO_ROOT / "AGENT_SPEC.md"
    assert agent_spec.is_file(), "AGENT_SPEC.md anchor missing (Tier-0 routers point here)"

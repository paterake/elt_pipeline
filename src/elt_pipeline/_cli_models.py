from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from elt_pipeline.config.runtime_manifest import runtime_manifest

_MODULE_MANIFEST_ROOT_PATH_DEFAULT: str = runtime_manifest.paths.cli_default_root_path
_MODULE_MANIFEST_WH_PATH_DEFAULT: str = runtime_manifest.paths.cli_default_warehouse_root


@dataclass(frozen=True)
class _RuntimeContext:
    repo_root: Path
    config_path_resolved: Path | None
    config_path_source: str
    environment: str | None
    runtime_overrides: dict[str, Any]
    cli_default_root_path: str
    cli_default_warehouse_root: str


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

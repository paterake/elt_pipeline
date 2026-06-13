from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.sql.models import DiscoveredSqlModel, SqlModelManifest, SqlModelStage

_MANIFEST_FILE_CANDIDATES = ("manifest.yaml", "manifest.yml")


def discover_sql_models(package_root: str | Path) -> list[DiscoveredSqlModel]:
    root_path = Path(package_root)
    if not root_path.exists():
        raise ConfigValidationError(
            message=f"SQL model package does not exist: {root_path}",
            context={"package_root": str(root_path)},
        )
    if not root_path.is_dir():
        raise ConfigValidationError(
            message=f"SQL model package must be a directory: {root_path}",
            context={"package_root": str(root_path)},
        )

    discovered: list[DiscoveredSqlModel] = []
    for stage in SqlModelStage:
        stage_path = root_path / stage.value
        if not stage_path.exists():
            continue

        for domain_path in sorted(path for path in stage_path.iterdir() if path.is_dir()):
            for model_path in sorted(path for path in domain_path.iterdir() if path.is_dir()):
                manifest_path = _resolve_manifest_path(model_path)
                sql_path = model_path / "model.sql"
                if manifest_path is None or not sql_path.exists():
                    raise ConfigValidationError(
                        message="SQL model directories must contain manifest.yaml and model.sql",
                        context={
                            "package_root": str(root_path),
                            "model_path": str(model_path),
                        },
                    )
                discovered.append(
                    _load_sql_model(
                        package_root=root_path,
                        stage=stage,
                        domain=domain_path.name,
                        model_path=model_path,
                        manifest_path=manifest_path,
                        sql_path=sql_path,
                    )
                )

    return sorted(
        discovered,
        key=lambda model: (
            model.manifest.stage.value,
            model.manifest.domain,
            model.manifest.name,
        ),
    )


def filter_sql_models(
    models: list[DiscoveredSqlModel],
    *,
    stage: str | None = None,
    domain: str | None = None,
    model_name: str | None = None,
) -> list[DiscoveredSqlModel]:
    filtered = models
    if stage is not None:
        filtered = [model for model in filtered if model.manifest.stage.value == stage]
    if domain is not None:
        filtered = [model for model in filtered if model.manifest.domain == domain]
    if model_name is not None:
        filtered = [model for model in filtered if model.manifest.name == model_name]
    return filtered


def _resolve_manifest_path(model_path: Path) -> Path | None:
    for name in _MANIFEST_FILE_CANDIDATES:
        candidate = model_path / name
        if candidate.exists():
            return candidate
    return None


def _load_sql_model(
    *,
    package_root: Path,
    stage: SqlModelStage,
    domain: str,
    model_path: Path,
    manifest_path: Path,
    sql_path: Path,
) -> DiscoveredSqlModel:
    raw_manifest = _read_yaml_file(manifest_path)
    try:
        manifest = SqlModelManifest.model_validate(raw_manifest)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="SQL model manifest validation failed",
            context={
                "manifest_path": str(manifest_path),
                "errors": exc.errors(include_url=False),
            },
        ) from exc

    _validate_manifest_location(
        manifest=manifest,
        expected_stage=stage,
        expected_domain=domain,
        expected_name=model_path.name,
        manifest_path=manifest_path,
    )

    sql_text = sql_path.read_text(encoding="utf-8").strip()
    if not sql_text:
        raise ConfigValidationError(
            message="SQL model file must not be empty",
            context={"sql_path": str(sql_path)},
        )

    return DiscoveredSqlModel(
        manifest=manifest,
        package_root=package_root,
        manifest_path=manifest_path,
        sql_path=sql_path,
        sql_text=sql_text,
    )


def _read_yaml_file(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse SQL model manifest: {exc}",
            context={"manifest_path": str(path)},
        ) from exc

    if not isinstance(payload, dict):
        raise ConfigValidationError(
            message="SQL model manifest must decode to a mapping",
            context={"manifest_path": str(path)},
        )
    return payload


def _validate_manifest_location(
    *,
    manifest: SqlModelManifest,
    expected_stage: SqlModelStage,
    expected_domain: str,
    expected_name: str,
    manifest_path: Path,
) -> None:
    if manifest.stage != expected_stage:
        raise ConfigValidationError(
            message="SQL model manifest stage does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_stage": manifest.stage.value,
                "expected_stage": expected_stage.value,
            },
        )
    if manifest.domain != expected_domain:
        raise ConfigValidationError(
            message="SQL model manifest domain does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_domain": manifest.domain,
                "expected_domain": expected_domain,
            },
        )
    if manifest.name != expected_name:
        raise ConfigValidationError(
            message="SQL model manifest name does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_name": manifest.name,
                "expected_name": expected_name,
            },
        )

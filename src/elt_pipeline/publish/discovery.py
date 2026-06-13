from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from elt_pipeline.publish.models import (
    DiscoveredPublishDefinition,
    PublishManifest,
    PublishSelectionMode,
)
from elt_pipeline.shared.errors import ConfigValidationError

_MANIFEST_FILE_CANDIDATES = ("manifest.yaml", "manifest.yml")


def discover_publish_definitions(package_root: str | Path) -> list[DiscoveredPublishDefinition]:
    root_path = Path(package_root)
    if not root_path.exists():
        raise ConfigValidationError(
            message=f"Publish definition package does not exist: {root_path}",
            context={"package_root": str(root_path)},
        )
    if not root_path.is_dir():
        raise ConfigValidationError(
            message=f"Publish definition package must be a directory: {root_path}",
            context={"package_root": str(root_path)},
        )

    discovered: list[DiscoveredPublishDefinition] = []
    for domain_path in sorted(path for path in root_path.iterdir() if path.is_dir()):
        for publish_path in sorted(path for path in domain_path.iterdir() if path.is_dir()):
            manifest_path = _resolve_manifest_path(publish_path)
            if manifest_path is None:
                raise ConfigValidationError(
                    message="Publish definition directories must contain manifest.yaml",
                    context={
                        "package_root": str(root_path),
                        "publish_path": str(publish_path),
                    },
                )
            discovered.append(
                _load_publish_definition(
                    package_root=root_path,
                    expected_domain=domain_path.name,
                    expected_name=publish_path.name,
                    manifest_path=manifest_path,
                )
            )

    return sorted(
        discovered,
        key=lambda publish: (publish.manifest.domain, publish.manifest.name),
    )


def filter_publish_definitions(
    definitions: list[DiscoveredPublishDefinition],
    *,
    domain: str | None = None,
    publish_name: str | None = None,
) -> list[DiscoveredPublishDefinition]:
    filtered = definitions
    if domain is not None:
        filtered = [definition for definition in filtered if definition.manifest.domain == domain]
    if publish_name is not None:
        filtered = [
            definition
            for definition in filtered
            if definition.manifest.name == publish_name
        ]
    return filtered


def _resolve_manifest_path(publish_path: Path) -> Path | None:
    for candidate_name in _MANIFEST_FILE_CANDIDATES:
        candidate = publish_path / candidate_name
        if candidate.exists():
            return candidate
    return None


def _load_publish_definition(
    *,
    package_root: Path,
    expected_domain: str,
    expected_name: str,
    manifest_path: Path,
) -> DiscoveredPublishDefinition:
    raw_manifest = _read_yaml_file(manifest_path)
    try:
        manifest = PublishManifest.model_validate(raw_manifest)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Publish manifest validation failed",
            context={
                "manifest_path": str(manifest_path),
                "errors": exc.errors(include_url=False),
            },
        ) from exc

    if manifest.domain != expected_domain:
        raise ConfigValidationError(
            message="Publish manifest domain does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_domain": manifest.domain,
                "expected_domain": expected_domain,
            },
        )
    if manifest.name != expected_name:
        raise ConfigValidationError(
            message="Publish manifest name does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_name": manifest.name,
                "expected_name": expected_name,
            },
        )

    query_path = manifest_path.parent / "query.sql"
    query_text: str | None = None
    if manifest.source.selection_mode == PublishSelectionMode.query:
        if not query_path.exists():
            raise ConfigValidationError(
                message="Query-based publish definitions must contain query.sql",
                context={"manifest_path": str(manifest_path)},
            )
        query_text = query_path.read_text(encoding="utf-8").strip()
        if not query_text:
            raise ConfigValidationError(
                message="Publish query.sql must not be empty",
                context={"query_path": str(query_path)},
            )
    elif query_path.exists():
        query_text = query_path.read_text(encoding="utf-8").strip() or None

    return DiscoveredPublishDefinition(
        manifest=manifest,
        package_root=package_root,
        manifest_path=manifest_path,
        query_path=query_path if query_path.exists() else None,
        query_text=query_text,
    )


def _read_yaml_file(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse publish manifest: {exc}",
            context={"manifest_path": str(path)},
        ) from exc

    if not isinstance(payload, dict):
        raise ConfigValidationError(
            message="Publish manifest must decode to a mapping",
            context={"manifest_path": str(path)},
        )
    return payload

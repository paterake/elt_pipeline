from __future__ import annotations

import fnmatch
import hashlib
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.governance import SqlColumnSpec

from ._models import CompiledMetric, DiscoveredMetric, MetricManifest

_MANIFEST_FILE_CANDIDATES = ("metric.yaml", "metric.yml", "manifest.yaml", "manifest.yml")


def discover_metrics(package_root: str | Path) -> list[DiscoveredMetric]:
    root_path = Path(package_root)
    if not root_path.exists():
        raise ConfigValidationError(
            message=f"Metric package does not exist: {root_path}",
            context={"package_root": str(root_path)},
        )
    if not root_path.is_dir():
        raise ConfigValidationError(
            message=f"Metric package must be a directory: {root_path}",
            context={"package_root": str(root_path)},
        )

    metrics_root = root_path / "metrics"
    if not metrics_root.exists():
        return []
    if not metrics_root.is_dir():
        raise ConfigValidationError(
            message=f"metrics/ must be a directory: {metrics_root}",
            context={"metrics_root": str(metrics_root)},
        )

    discovered: list[DiscoveredMetric] = []
    for domain_path in sorted(path for path in metrics_root.iterdir() if path.is_dir()):
        for metric_path in sorted(path for path in domain_path.iterdir() if path.is_dir()):
            manifest_path = _resolve_manifest_path(metric_path)
            if manifest_path is None:
                raise ConfigValidationError(
                    message=(
                        "Metric directories must contain a manifest file "
                        "(metric.yaml, metric.yml, manifest.yaml, manifest.yml)"
                    ),
                    context={
                        "package_root": str(root_path),
                        "metric_path": str(metric_path),
                    },
                )
            discovered.append(
                _load_metric(
                    package_root=root_path,
                    domain=domain_path.name,
                    metric_path=metric_path,
                    manifest_path=manifest_path,
                )
            )

    return sorted(
        discovered,
        key=lambda metric: (
            metric.manifest.domain,
            metric.manifest.name,
        ),
    )


def filter_metrics(
    metrics: list[DiscoveredMetric],
    *,
    domain: str | None = None,
    metric_name: str | None = None,
) -> list[DiscoveredMetric]:
    filtered = metrics
    if domain is not None:
        filtered = [m for m in filtered if m.manifest.domain == domain]
    if metric_name is not None:
        filtered = [m for m in filtered if fnmatch.fnmatch(m.manifest.name, metric_name)]
    return filtered


def compile_metric(
    *,
    metric: DiscoveredMetric,
    sql_models: list | None = None,
) -> CompiledMetric:
    parts = metric.manifest.query_ref.split(".")
    stage, model_domain, model_name, column = parts[0], parts[1], parts[2], parts[3]
    query_ref_model_id = f"{stage}.{model_domain}.{model_name}"
    query_ref_column = column

    if sql_models is not None:
        matched_model = None
        for sql_model in sql_models:
            model_id = getattr(sql_model, "model_id", None)
            if model_id == query_ref_model_id:
                matched_model = sql_model
                break
        if matched_model is None:
            raise ConfigValidationError(
                message=f"query_ref model not found in sql_models: {query_ref_model_id}",
                context={
                    "metric_id": metric.metric_id,
                    "query_ref_model_id": query_ref_model_id,
                    "query_ref_column": query_ref_column,
                    "missing": "model",
                },
            )
        governance = getattr(matched_model, "governance", None)
        if governance is None:
            manifest = getattr(matched_model, "manifest", None)
            if manifest is not None:
                governance = getattr(manifest, "governance", None)
        if governance is not None:
            columns = getattr(governance, "columns", [])
            column_names = {col.name for col in columns if isinstance(col, SqlColumnSpec)}
            if query_ref_column not in column_names:
                raise ConfigValidationError(
                    message=f"query_ref column not found in model governance: {query_ref_column}",
                    context={
                        "metric_id": metric.metric_id,
                        "query_ref_model_id": query_ref_model_id,
                        "query_ref_column": query_ref_column,
                        "missing": "column",
                    },
                )

    dim_names = sorted(d.name for d in metric.manifest.dimensions)
    dim_clause = ", ".join(dim_names)
    select_dims = f"{dim_clause}, " if dim_clause else ""
    group_by_clause = f"GROUP BY {dim_clause}" if dim_clause else ""
    agg_sql = metric.manifest.aggregation.value
    metric_name = metric.manifest.name
    normalized_sql = (
        f"SELECT {select_dims}{agg_sql}({query_ref_column}) AS {metric_name} "
        f"FROM {query_ref_model_id} {group_by_clause}"
    ).strip()

    generated_sql_hash = hashlib.sha256(normalized_sql.encode("utf-8")).hexdigest()

    return CompiledMetric(
        metric_id=metric.metric_id,
        domain=metric.manifest.domain,
        name=metric.manifest.name,
        query_ref_model_id=query_ref_model_id,
        query_ref_column=query_ref_column,
        aggregation=metric.manifest.aggregation,
        dimensions=metric.manifest.dimensions,
        filters=metric.manifest.filters,
        required_role=metric.manifest.required_role,
        generated_sql_hash=generated_sql_hash,
        manifest_path=metric.manifest_path,
    )


def compile_all_metrics(
    *,
    package_root: str | Path,
    sql_models: list | None = None,
    domain: str | None = None,
    metric_name: str | None = None,
) -> list[CompiledMetric]:
    discovered = discover_metrics(package_root)
    filtered = filter_metrics(discovered, domain=domain, metric_name=metric_name)
    compiled = [compile_metric(metric=m, sql_models=sql_models) for m in filtered]
    return sorted(compiled, key=lambda m: m.metric_id)


def _resolve_manifest_path(metric_path: Path) -> Path | None:
    for name in _MANIFEST_FILE_CANDIDATES:
        candidate = metric_path / name
        if candidate.exists():
            return candidate
    return None


def _load_metric(
    *,
    package_root: Path,
    domain: str,
    metric_path: Path,
    manifest_path: Path,
) -> DiscoveredMetric:
    raw_manifest = _read_yaml_file(manifest_path)
    try:
        manifest = MetricManifest.model_validate(raw_manifest)
    except ValidationError as exc:
        raise ConfigValidationError(
            message="Metric manifest validation failed",
            context={
                "manifest_path": str(manifest_path),
                "errors": exc.errors(include_url=False),
            },
        ) from exc

    _validate_manifest_location(
        manifest=manifest,
        expected_domain=domain,
        expected_name=metric_path.name,
        manifest_path=manifest_path,
    )

    return DiscoveredMetric(
        manifest=manifest,
        package_root=package_root,
        manifest_path=manifest_path,
    )


def _read_yaml_file(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            message=f"Failed to parse metric manifest: {exc}",
            context={"manifest_path": str(path)},
        ) from exc

    if not isinstance(payload, dict):
        raise ConfigValidationError(
            message="Metric manifest must decode to a mapping",
            context={"manifest_path": str(path)},
        )
    return payload


def _validate_manifest_location(
    *,
    manifest: MetricManifest,
    expected_domain: str,
    expected_name: str,
    manifest_path: Path,
) -> None:
    if manifest.domain != expected_domain:
        raise ConfigValidationError(
            message="Metric manifest domain does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_domain": manifest.domain,
                "expected_domain": expected_domain,
            },
        )
    if manifest.name != expected_name:
        raise ConfigValidationError(
            message="Metric manifest name does not match directory structure",
            context={
                "manifest_path": str(manifest_path),
                "manifest_name": manifest.name,
                "expected_name": expected_name,
            },
        )

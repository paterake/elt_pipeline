from __future__ import annotations

from collections import defaultdict, deque

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.sql.models import DiscoveredSqlModel


def topologically_sort_sql_models(models: list[DiscoveredSqlModel]) -> list[DiscoveredSqlModel]:
    models_by_id = {model.model_id: model for model in models}
    inbound_edges: dict[str, set[str]] = {model_id: set() for model_id in models_by_id}
    outgoing_edges: dict[str, set[str]] = defaultdict(set)

    for model in models:
        for dependency_id in resolve_dependency_ids(model=model, models_by_id=models_by_id):
            inbound_edges[model.model_id].add(dependency_id)
            outgoing_edges[dependency_id].add(model.model_id)

    ready = deque(sorted(model_id for model_id, inbound in inbound_edges.items() if not inbound))
    ordered_model_ids: list[str] = []
    remaining_inbound = {model_id: set(values) for model_id, values in inbound_edges.items()}

    while ready:
        model_id = ready.popleft()
        ordered_model_ids.append(model_id)
        for child_id in sorted(outgoing_edges.get(model_id, set())):
            remaining_inbound[child_id].discard(model_id)
            if not remaining_inbound[child_id]:
                ready.append(child_id)

    if len(ordered_model_ids) != len(models):
        unresolved = sorted(model_id for model_id, inbound in remaining_inbound.items() if inbound)
        raise PipelineError(
            message="SQL model dependency cycle detected",
            error_code="SQL_MODEL_DEPENDENCY_CYCLE",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={"unresolved_models": unresolved},
        )

    return [models_by_id[model_id] for model_id in ordered_model_ids]


def resolve_selected_model_ids(
    *,
    all_models: list[DiscoveredSqlModel],
    selected_models: list[DiscoveredSqlModel],
    include_dependencies: bool,
) -> set[str]:
    models_by_id = {model.model_id: model for model in all_models}
    selected_ids = {model.model_id for model in selected_models}
    if not include_dependencies:
        return selected_ids

    resolved_ids = set(selected_ids)
    stack = list(selected_ids)
    while stack:
        current_id = stack.pop()
        current_model = models_by_id[current_id]
        for dependency_id in resolve_dependency_ids(model=current_model, models_by_id=models_by_id):
            if dependency_id not in resolved_ids:
                resolved_ids.add(dependency_id)
                stack.append(dependency_id)
    return resolved_ids


def resolve_dependency_ids(
    *,
    model: DiscoveredSqlModel,
    models_by_id: dict[str, DiscoveredSqlModel],
) -> list[str]:
    resolved: list[str] = []
    for dependency_reference in model.manifest.depends_on:
        dependency_id = _normalize_dependency_reference(
            dependency_reference=dependency_reference,
            default_stage=model.manifest.stage.value,
            default_domain=model.manifest.domain,
        )
        if dependency_id not in models_by_id:
            raise PipelineError(
                message=f"SQL model dependency '{dependency_reference}' could not be resolved",
                error_code="SQL_MODEL_DEPENDENCY_MISSING",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "model_id": model.model_id,
                    "dependency_reference": dependency_reference,
                },
            )
        resolved.append(dependency_id)
    return resolved


def _normalize_dependency_reference(
    *,
    dependency_reference: str,
    default_stage: str,
    default_domain: str,
) -> str:
    parts = [part.strip() for part in dependency_reference.split(".") if part.strip()]
    if len(parts) == 1:
        return f"{default_stage}.{default_domain}.{parts[0]}"
    if len(parts) == 2:
        return f"{default_stage}.{parts[0]}.{parts[1]}"
    if len(parts) == 3:
        return ".".join(parts)
    raise PipelineError(
        message=f"Invalid SQL dependency reference '{dependency_reference}'",
        error_code="SQL_MODEL_DEPENDENCY_INVALID",
        error_category=ErrorCategory.config_error,
        retryable=False,
        context={"dependency_reference": dependency_reference},
    )

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.observability import MetricPoint, MetricType
from elt_pipeline.shared.path_utils import (
    join_paths,
    path_mkdir,
    path_open_for_append,
)

from ._models import CompiledMetric, MetricAuditRecord


def _build_aggregation_sql(metric: CompiledMetric, *, source_table_ref: str) -> str:
    dims_sorted = sorted(d.name for d in metric.dimensions)
    dim_clause = ", ".join(dims_sorted) if dims_sorted else ""
    group_clause = dim_clause
    agg_sql = metric.aggregation.value
    col = metric.query_ref_column
    metric_name = metric.name
    filter_predicates = " AND ".join(f.predicate for f in metric.filters)
    where_clause = f"WHERE {filter_predicates}" if metric.filters else ""
    group_by_clause = f"GROUP BY {group_clause}" if dims_sorted else ""
    select_dims = f"{dim_clause}, " if dims_sorted else ""
    return (
        f"SELECT {select_dims}{agg_sql}({col}) AS {metric_name} "
        f"FROM {source_table_ref} {where_clause} {group_by_clause}"
    ).strip()


def _compute_sql_hash(normalized_sql: str) -> str:
    return hashlib.sha256(normalized_sql.encode("utf-8")).hexdigest()


def _check_consistency_or_raise(
    *,
    metric_id: str,
    mode_a: str,
    sql_hash_a: str,
    mode_b: str,
    sql_hash_b: str,
    tol: float = 1e-9,
) -> None:
    if sql_hash_a != sql_hash_b:
        raise PipelineError(
            error_code="METRIC_MODE_INCONSISTENT",
            error_category=ErrorCategory.data_integrity_error,
            retryable=False,
            message=(
                f"Cross-mode SQL hash mismatch for metric {metric_id}: "
                f"{mode_a} hash={sql_hash_a} != {mode_b} hash={sql_hash_b}"
            ),
            context={
                "metric_id": metric_id,
                "mode_a": mode_a,
                "sql_hash_a": sql_hash_a,
                "mode_b": mode_b,
                "sql_hash_b": sql_hash_b,
            },
        )


def run_metric_mode_materialize(
    *,
    metric: CompiledMetric,
    spark: Any,
    target_catalog: str,
    target_namespace: str,
    table_prefix: str = "",
) -> tuple[str, int, str]:
    source_ref = f"{target_catalog}.{metric.query_ref_model_id.replace('.', '.')}"
    table_name = f"{table_prefix}metric_{metric.domain}_{metric.name}"
    full_table = f"{target_catalog}.{target_namespace}.{table_name}"
    sql = _build_aggregation_sql(metric, source_table_ref=source_ref)
    final_sql = f"CREATE OR REPLACE TABLE {full_table} USING iceberg AS {sql}"
    spark.sql(final_sql)
    sql_hash = _compute_sql_hash(_build_aggregation_sql(metric, source_table_ref="SOURCE_TABLE"))
    return (full_table, 0, sql_hash)


def run_metric_mode_view(
    *,
    metric: CompiledMetric,
    trino_sql_executor: Callable[[str], Any] | None = None,
    target_schema: str,
) -> tuple[str, str]:
    source_ref = metric.query_ref_model_id
    agg_sql = _build_aggregation_sql(metric, source_table_ref=source_ref)
    view_name = f"metric_{metric.domain}_{metric.name}"
    view_comment = f"metric_id={metric.domain}.{metric.name}"
    view_target = f"{target_schema}.{view_name}"
    if metric.required_role is not None:
        view_sql = (
            f"CREATE OR REPLACE SECURITY DEFINER VIEW {view_target} "
            f"COMMENT '{view_comment}' AS {agg_sql}"
        )
    else:
        view_sql = (
            f"CREATE OR REPLACE VIEW {view_target} "
            f"COMMENT '{view_comment}' AS {agg_sql}"
        )
    sql_hash = _compute_sql_hash(_build_aggregation_sql(metric, source_table_ref="SOURCE_TABLE"))
    return (view_sql, sql_hash)


def run_metric_mode_prometheus(
    *,
    metric: CompiledMetric,
    value_extractor: Callable[[CompiledMetric], dict[str, float]],
) -> tuple[list[MetricPoint], str]:
    sql_hash = _compute_sql_hash(_build_aggregation_sql(metric, source_table_ref="SOURCE_TABLE"))
    return (
        [
            MetricPoint(
                metric_name=f"elt.metric.{metric.domain}.{metric.name}",
                metric_type=MetricType.gauge,
                value=0.0,
                labels={},
            )
        ],
        sql_hash,
    )


def write_metric_audit(
    *,
    root_path: str,
    run_id: str,
    record: MetricAuditRecord,
) -> str:
    audit_dir = join_paths(root_path, "runs", run_id, "metrics")
    path_mkdir(audit_dir, exist_ok=True)
    audit_path = join_paths(audit_dir, "metric_audit.jsonl")
    line = json.dumps(record.model_dump(mode="json"), sort_keys=True, default=str) + "\n"
    with path_open_for_append(audit_path) as f:
        f.write(line)
    return str(audit_path)

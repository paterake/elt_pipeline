from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from elt_pipeline.metrics import (
    CompiledMetric,
    MetricAggregation,
    MetricAuditRecord,
    MetricDimensionSpec,
    MetricManifest,
    _build_aggregation_sql,
    _check_consistency_or_raise,
    _compute_sql_hash,
    compile_all_metrics,
    compile_metric,
    discover_metrics,
    filter_metrics,
    run_metric_mode_materialize,
    run_metric_mode_prometheus,
    run_metric_mode_view,
    write_metric_audit,
)
from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError
from elt_pipeline.shared.governance import (
    DataClassification,
    SqlColumnSpec,
    SqlModelGovernance,
)
from elt_pipeline.shared.observability import MetricType
from elt_pipeline.sql.models import (
    DiscoveredSqlModel,
    SqlModelManifest,
    SqlModelOwner,
    SqlModelStage,
)


def _write_metric_package(
    tmp_path: Path,
    *,
    extra_metrics: list[tuple[str, str, dict]] | None = None,
) -> Path:
    package = tmp_path / "pkg"
    metrics_finance = package / "metrics" / "finance"

    sales_path = metrics_finance / "monthly_sales"
    sales_path.mkdir(parents=True, exist_ok=True)
    sales_manifest = {
        "name": "monthly_sales",
        "domain": "finance",
        "description": "Total monthly sales revenue",
        "query_ref": "level3.finance.orders.amount",
        "aggregation": "sum",
        "dimensions": [
            {"name": "order_month", "is_time_dimension": True},
            {"name": "region"},
        ],
        "filters": [{"predicate": "status = 'completed'"}],
        "owners": [{"name": "finance-team"}],
    }
    (sales_path / "metric.yaml").write_text(json.dumps(sales_manifest))

    customer_path = metrics_finance / "active_customers"
    customer_path.mkdir(parents=True, exist_ok=True)
    cust_manifest = {
        "name": "active_customers",
        "domain": "finance",
        "query_ref": "level4.finance.customer_metrics.customer_id",
        "aggregation": "count_distinct",
        "dimensions": [{"name": "account_tier"}],
        "required_role": "internal",
    }
    (customer_path / "metric.yaml").write_text(json.dumps(cust_manifest))

    if extra_metrics:
        for domain, mname, mdef in extra_metrics:
            mpath = package / "metrics" / domain / mname
            mpath.mkdir(parents=True, exist_ok=True)
            merged = {"name": mname, "domain": domain, **mdef}
            (mpath / "metric.yaml").write_text(json.dumps(merged))
    return package


def _make_mock_sql_models() -> list[DiscoveredSqlModel]:
    order_manifest = SqlModelManifest(
        name="orders",
        stage=SqlModelStage.level3,
        domain="finance",
        target={"table_name": "orders"},
        owner=SqlModelOwner(name="owner"),
        governance=SqlModelGovernance(
            columns=[
                SqlColumnSpec(name="amount", type="DECIMAL(18,2)"),
                SqlColumnSpec(name="order_month", type="DATE"),
                SqlColumnSpec(name="region", type="STRING"),
                SqlColumnSpec(name="status", type="STRING"),
            ]
        ),
    )
    cm_manifest = SqlModelManifest(
        name="customer_metrics",
        stage=SqlModelStage.level4,
        domain="finance",
        target={"table_name": "customer_metrics"},
        owner=SqlModelOwner(name="owner"),
        governance=SqlModelGovernance(
            columns=[
                SqlColumnSpec(name="customer_id", type="STRING"),
                SqlColumnSpec(name="account_tier", type="STRING"),
            ]
        ),
    )
    models: list[DiscoveredSqlModel] = []
    for i, m in enumerate([order_manifest, cm_manifest]):
        models.append(
            DiscoveredSqlModel(
                manifest=m,
                package_root=Path("/x"),
                manifest_path=Path(f"/x/m{i}.yaml"),
                sql_path=Path(f"/x/m{i}.sql"),
                sql_text="SELECT 1",
            )
        )
    return models


# ── Manifest validation (Compile-adjacent) ──────────────────────────────────


def test_metric_manifest_validates_name_underscore_rules() -> None:
    with pytest.raises(ValidationError):
        MetricManifest(
            name="1invalid",
            domain="d",
            query_ref="level3.a.b.c",
            aggregation=MetricAggregation.sum,
        )
    with pytest.raises(ValidationError):
        MetricManifest(
            name="has space",
            domain="d",
            query_ref="level3.a.b.c",
            aggregation=MetricAggregation.sum,
        )
    ok = MetricManifest(
        name="good_name_123",
        domain="d",
        query_ref="level3.a.b.c",
        aggregation=MetricAggregation.sum,
    )
    assert ok.name == "good_name_123"


def test_metric_manifest_cumulative_rolling_requires_time_dimension() -> None:
    with pytest.raises(ValidationError):
        MetricManifest(
            name="rolling_balance",
            domain="finance",
            query_ref="level3.finance.accounts.balance",
            aggregation="cumulative_rolling",
            dimensions=[MetricDimensionSpec(name="region")],
        )
    ok = MetricManifest(
        name="rolling_balance",
        domain="finance",
        query_ref="level3.finance.accounts.balance",
        aggregation="cumulative_rolling",
        dimensions=[MetricDimensionSpec(name="report_date", is_time_dimension=True)],
    )
    assert ok.aggregation == MetricAggregation.cumulative_rolling


def test_metric_manifest_rejects_short_query_ref() -> None:
    with pytest.raises(ValidationError):
        MetricManifest(
            name="m",
            domain="d",
            query_ref="only.three.parts",
            aggregation="sum",
        )


# ── Checklist Group: 3 Compile tests ────────────────────────────────────────


def test_compile_metric_structural_only_no_sql_refs(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    assert len(discovered) == 2
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)
    assert compiled.metric_id == "finance.monthly_sales"
    assert compiled.query_ref_model_id == "level3.finance.orders"
    assert compiled.query_ref_column == "amount"
    assert compiled.aggregation == MetricAggregation.sum
    assert len(compiled.dimensions) == 2
    assert len(compiled.generated_sql_hash) == 64


def test_compile_metric_with_sql_refs_ok(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sql_models = _make_mock_sql_models()
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales, sql_models=sql_models)
    assert compiled.query_ref_column == "amount"
    assert compiled.required_role is None
    cust = next(d for d in discovered if d.manifest.name == "active_customers")
    compiled_cust = compile_metric(metric=cust, sql_models=sql_models)
    assert compiled_cust.required_role == DataClassification.internal


def test_compile_metric_with_sql_refs_detects_missing_model_and_column(
    tmp_path: Path,
) -> None:
    package = _write_metric_package(
        tmp_path,
        extra_metrics=[
            (
                "ops",
                "bad_model_ref",
                {
                    "query_ref": "level3.ghost.does_not_exist.x",
                    "aggregation": "sum",
                    "dimensions": [],
                },
            ),
            (
                "ops",
                "bad_col_ref",
                {
                    "query_ref": "level3.finance.orders.nonexistent_col",
                    "aggregation": "sum",
                    "dimensions": [],
                },
            ),
        ],
    )
    discovered = discover_metrics(package)
    sql_models = _make_mock_sql_models()
    bad_model = next(d for d in discovered if d.manifest.name == "bad_model_ref")
    with pytest.raises(ConfigValidationError) as exc_info:
        compile_metric(metric=bad_model, sql_models=sql_models)
    assert exc_info.value.context["missing"] == "model"
    assert exc_info.value.context["query_ref_model_id"] == "level3.ghost.does_not_exist"

    bad_col = next(d for d in discovered if d.manifest.name == "bad_col_ref")
    with pytest.raises(ConfigValidationError) as exc_info2:
        compile_metric(metric=bad_col, sql_models=sql_models)
    assert exc_info2.value.context["missing"] == "column"
    assert exc_info2.value.context["query_ref_column"] == "nonexistent_col"


# ── Checklist Group: 1 Glob selector test ───────────────────────────────────


def test_filter_metrics_glob_sales_wildcard(tmp_path: Path) -> None:
    package = _write_metric_package(
        tmp_path,
        extra_metrics=[
            (
                "finance",
                "sales_ytd",
                {
                    "query_ref": "level3.finance.orders.amount",
                    "aggregation": "sum",
                    "dimensions": [],
                },
            ),
            (
                "finance",
                "inventory_turn",
                {
                    "query_ref": "level3.ops.inventory.qty",
                    "aggregation": "average",
                    "dimensions": [],
                },
            ),
        ],
    )
    discovered = discover_metrics(package)
    finance_only = filter_metrics(discovered, domain="finance")
    assert len(finance_only) == 4
    sales_wildcard = filter_metrics(discovered, metric_name="*sales*")
    sales_names = sorted(d.manifest.name for d in sales_wildcard)
    assert sales_names == ["monthly_sales", "sales_ytd"]
    for m in sales_wildcard:
        assert "sales" in m.manifest.name


# ── Checklist Group: 4 Materialize tests ────────────────────────────────────


def test_materialize_generates_expected_sql(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)

    class _FakeSpark:
        def __init__(self) -> None:
            self.sql_calls: list[str] = []

        def sql(self, stmt: str) -> object:
            self.sql_calls.append(stmt)
            return None

    spark = _FakeSpark()
    fqn, rows, sql_hash = run_metric_mode_materialize(
        metric=compiled,
        spark=spark,
        target_catalog="spark_catalog",
        target_namespace="metrics",
    )
    assert rows == 0
    assert fqn == "spark_catalog.metrics.metric_finance_monthly_sales"
    assert len(spark.sql_calls) == 1
    stmt = spark.sql_calls[0]
    assert "CREATE OR REPLACE TABLE" in stmt
    assert "USING iceberg" in stmt
    assert "status = 'completed'" in stmt
    assert "sum(amount) AS monthly_sales" in stmt
    assert "GROUP BY order_month, region" in stmt
    assert len(sql_hash) == 64


def test_materialize_sql_hash_is_stable_for_deterministic_layout(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    c1 = compile_metric(metric=sales)
    c2 = compile_metric(metric=sales)
    assert c1.generated_sql_hash == c2.generated_sql_hash
    same_package = discover_metrics(package)
    sales2 = next(d for d in same_package if d.manifest.name == "monthly_sales")
    c3 = compile_metric(metric=sales2)
    assert c3.generated_sql_hash == c1.generated_sql_hash


def test_materialize_prefix_is_applied(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    cust = next(d for d in discovered if d.manifest.name == "active_customers")
    compiled = compile_metric(metric=cust)

    class _FakeSpark:
        def sql(self, _: str) -> object:
            return None

    spark = _FakeSpark()
    fqn, _, _ = run_metric_mode_materialize(
        metric=compiled,
        spark=spark,
        target_catalog="glue_prod",
        target_namespace="metrics_na",
        table_prefix="team_foo_",
    )
    assert fqn == "glue_prod.metrics_na.team_foo_metric_finance_active_customers"


def test_materialize_write_metric_audit_jsonl(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)

    class _FakeSpark:
        def sql(self, _: str) -> object:
            return None

    spark = _FakeSpark()
    fqn, _, sql_hash = run_metric_mode_materialize(
        metric=compiled,
        spark=spark,
        target_catalog="spark_catalog",
        target_namespace="metrics",
    )
    started = "2026-08-27T10:00:00+00:00"
    finished = "2026-08-27T10:00:12+00:00"
    record = MetricAuditRecord(
        metric_id=compiled.metric_id,
        mode="materialize",
        started_at_iso=started,
        finished_at_iso=finished,
        generated_sql_hash=sql_hash,
        output_location=f"iceberg:{fqn}",
        success=True,
        total_sum=123456.78,
        non_null_count=42,
        min_value=1.5,
        max_value=999.99,
    )
    root_path = str(tmp_path / "runtime")
    audit_path = write_metric_audit(
        root_path=root_path,
        run_id="r_abc",
        record=record,
    )
    assert Path(audit_path).exists()
    lines = Path(audit_path).read_text().splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["metric_id"] == "finance.monthly_sales"
    assert data["mode"] == "materialize"
    assert data["success"] is True
    assert data["total_sum"] == 123456.78
    assert data["non_null_count"] == 42


# ── Checklist Group: 3 Trino VIEW tests ─────────────────────────────────────


def test_view_no_required_role_no_security_definer(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)
    view_sql, sql_hash = run_metric_mode_view(
        metric=compiled,
        target_schema="metrics_schema",
    )
    assert "SECURITY DEFINER" not in view_sql
    assert "CREATE OR REPLACE VIEW metrics_schema.metric_finance_monthly_sales" in view_sql
    assert "COMMENT 'metric_id=finance.monthly_sales'" in view_sql
    assert "status = 'completed'" in view_sql
    assert len(sql_hash) == 64


def test_view_required_role_injects_security_definer(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    cust = next(d for d in discovered if d.manifest.name == "active_customers")
    compiled = compile_metric(metric=cust)
    view_sql, _ = run_metric_mode_view(
        metric=compiled,
        target_schema="bi_layer",
    )
    assert (
        "CREATE OR REPLACE SECURITY DEFINER VIEW "
        "bi_layer.metric_finance_active_customers" in view_sql
    )
    assert "COMMENT 'metric_id=finance.active_customers'" in view_sql
    assert "count_distinct(customer_id) AS active_customers" in view_sql


def test_view_sql_hash_matches_materialize_norm(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)
    view_sql, view_hash = run_metric_mode_view(
        metric=compiled,
        target_schema="metrics",
    )

    class _FakeSpark:
        def sql(self, _: str) -> object:
            return None

    spark = _FakeSpark()
    _, _, mat_hash = run_metric_mode_materialize(
        metric=compiled,
        spark=spark,
        target_catalog="c",
        target_namespace="metrics",
    )
    assert view_hash == mat_hash
    # Ensure hash is from normalized SOURCE_TABLE-agnostic SQL
    assert view_hash == _compute_sql_hash(
        _build_aggregation_sql(compiled, source_table_ref="SOURCE_TABLE")
    )


# ── Checklist Group: 3 Prometheus export tests ──────────────────────────────


def test_prometheus_metric_naming_gauge_type(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)
    points, sql_hash = run_metric_mode_prometheus(
        metric=compiled,
        value_extractor=lambda m: {"value": 42.0},
    )
    assert len(points) == 1
    point = points[0]
    assert point.metric_name == "elt.metric.finance.monthly_sales"
    assert point.metric_type == MetricType.gauge
    assert point.value == 0.0
    assert point.labels == {}
    assert len(sql_hash) == 64


def test_prometheus_hash_matches_materialize_and_view(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    cust = next(d for d in discovered if d.manifest.name == "active_customers")
    compiled = compile_metric(metric=cust)
    _, prom_hash = run_metric_mode_prometheus(
        metric=compiled,
        value_extractor=lambda m: {},
    )
    _, view_hash = run_metric_mode_view(metric=compiled, target_schema="m")

    class _FakeSpark:
        def sql(self, _: str) -> object:
            return None

    _, _, mat_hash = run_metric_mode_materialize(
        metric=compiled, spark=_FakeSpark(), target_catalog="c", target_namespace="m"
    )
    assert prom_hash == view_hash == mat_hash


def test_prometheus_export_required_role_audit_matches(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    cust = next(d for d in discovered if d.manifest.name == "active_customers")
    compiled = compile_metric(metric=cust)
    points, _ = run_metric_mode_prometheus(
        metric=compiled,
        value_extractor=lambda m: {},
    )
    assert points[0].metric_name == "elt.metric.finance.active_customers"
    assert compiled.required_role == DataClassification.internal


# ── Checklist Group: 2 Consistency guardrail tests ──────────────────────────


def test_consistency_guardrail_raises_on_mismatch() -> None:
    with pytest.raises(PipelineError) as exc_info:
        _check_consistency_or_raise(
            metric_id="finance.monthly_sales",
            mode_a="materialize",
            sql_hash_a="aaaa" * 16,
            mode_b="view",
            sql_hash_b="bbbb" * 16,
        )
    err = exc_info.value
    assert err.error_code == "METRIC_MODE_INCONSISTENT"
    assert err.error_category == ErrorCategory.data_integrity_error
    assert err.context["metric_id"] == "finance.monthly_sales"
    assert err.context["mode_a"] == "materialize"
    assert err.context["sql_hash_a"] == "aaaa" * 16
    assert err.context["mode_b"] == "view"
    assert err.context["sql_hash_b"] == "bbbb" * 16


def test_consistency_guardrail_passes_on_match(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    discovered = discover_metrics(package)
    sales = next(d for d in discovered if d.manifest.name == "monthly_sales")
    compiled = compile_metric(metric=sales)
    _, view_hash = run_metric_mode_view(metric=compiled, target_schema="x")

    class _FakeSpark:
        def sql(self, _: str) -> object:
            return None

    _, _, mat_hash = run_metric_mode_materialize(
        metric=compiled,
        spark=_FakeSpark(),
        target_catalog="c",
        target_namespace="x",
    )
    _, prom_hash = run_metric_mode_prometheus(
        metric=compiled,
        value_extractor=lambda m: {},
    )
    _check_consistency_or_raise(
        metric_id=compiled.metric_id,
        mode_a="materialize",
        sql_hash_a=mat_hash,
        mode_b="view",
        sql_hash_b=view_hash,
    )
    _check_consistency_or_raise(
        metric_id=compiled.metric_id,
        mode_a="materialize",
        sql_hash_a=mat_hash,
        mode_b="prometheus",
        sql_hash_b=prom_hash,
    )


# ── End-to-end compile_all_metrics smoke test ───────────────────────────────


def test_compile_all_metrics_with_sql_models_smoke(tmp_path: Path) -> None:
    package = _write_metric_package(tmp_path)
    compiled = compile_all_metrics(
        package_root=package,
        sql_models=_make_mock_sql_models(),
    )
    ids = sorted(m.metric_id for m in compiled)
    assert ids == ["finance.active_customers", "finance.monthly_sales"]
    for m in compiled:
        assert len(m.generated_sql_hash) == 64
        assert isinstance(m, CompiledMetric)


# ── Glob and domain filter via compile_all_metrics ──────────────────────────


def test_compile_all_metrics_domain_and_name_filters(tmp_path: Path) -> None:
    package = _write_metric_package(
        tmp_path,
        extra_metrics=[
            (
                "ops",
                "ops_daily_volume",
                {
                    "query_ref": "level3.ops.logins.count",
                    "aggregation": "sum",
                    "dimensions": [{"name": "login_date", "is_time_dimension": True}],
                },
            )
        ],
    )
    all_compiled = compile_all_metrics(package_root=package)
    assert len(all_compiled) == 3

    ops_only = compile_all_metrics(package_root=package, domain="ops")
    assert [m.metric_id for m in ops_only] == ["ops.ops_daily_volume"]

    sales_wildcard = compile_all_metrics(package_root=package, metric_name="*sales*")
    names = sorted(m.name for m in sales_wildcard)
    assert names == ["monthly_sales"]

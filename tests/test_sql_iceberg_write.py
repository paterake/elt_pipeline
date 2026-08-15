from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from elt_pipeline.sql.models import (
    CompiledSqlModel,
    SqlLoadMode,
    SqlModelStage,
    SqlQualityExpectations,
)
from elt_pipeline.sql.spark_executor import (
    SparkSqlModelExecutor,
    _iceberg_catalog_name,
    _iceberg_table_fq,
)
from elt_pipeline.spark.session import build_spark_session


class DecimalLike:
    def __init__(self, value: float | int) -> None:
        self.value = Decimal(value)

    def __eq__(self, other: object) -> bool:
        try:
            if isinstance(other, DecimalLike):
                return abs(other.value - self.value) < Decimal("0.001")
            return abs(Decimal(str(other)) - self.value) < Decimal("0.001")
        except Exception:
            return False

    def __repr__(self) -> str:
        return f"≈{self.value}"


@pytest.fixture(scope="module")
def iceberg_spark(tmp_path_factory) -> SparkSession:  # type: ignore[no-untyped-def]
    module_root = tmp_path_factory.mktemp("iceberg-ivy")
    ivy_home = str(module_root / "ivy2")
    os.environ["ELT_PIPELINE_IVY_HOME"] = ivy_home
    test_warehouse_root = tmp_path_factory.mktemp("iceberg-gatei1")
    warehouse_dir = str(test_warehouse_root / "iceberg_warehouse")
    spark = build_spark_session(
        app_name="elt_pipeline_iceberg_gate_i1_test",
        iceberg_enabled=True,
        iceberg_catalog_type="hadoop",
        iceberg_warehouse_dir=warehouse_dir,
    )
    yield spark
    spark.stop()


def _base_orders_model(
    *,
    compiled_sql: str,
    load_mode: SqlLoadMode,
    partition_columns: list[str],
    row_count_min: int,
    name_suffix: str = "",
) -> CompiledSqlModel:
    name = f"base_orders{name_suffix}"
    return CompiledSqlModel(
        model_id=f"level3.sales.{name}",
        stage=SqlModelStage.level3,
        domain="sales",
        name=name,
        target_table_name=name,
        load_mode=load_mode,
        materialization="table",  # type: ignore[arg-type]
        manifest_path=Path("/tmp/_fake_manifest.yaml"),
        sql_path=Path("/tmp/_fake.sql"),
        compiled_sql=compiled_sql,
        partition_columns=list(partition_columns),
        quality=SqlQualityExpectations(row_count_min=row_count_min),
    )


def test_full_refresh_creates_or_replaces_table(
    iceberg_spark: SparkSession,
    tmp_path: Path,
) -> None:
    warehouse_root = str(tmp_path / "wh_fullref")
    executor1 = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-fullref-1",
    )
    sql_rows_3 = (
        "SELECT "
        "CAST('orders_api' AS STRING) AS source_name, "
        "CAST('2026-08-15' AS DATE) AS business_date, "
        "CAST(100 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-1' AS STRING) AS order_id "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-15', 200, 'o-2' "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-16', 300, 'o-3'"
    )
    model = _base_orders_model(
        compiled_sql=sql_rows_3,
        load_mode=SqlLoadMode.full_refresh,
        partition_columns=[],
        row_count_min=3,
    )
    result1 = executor1.execute(models=[model])
    assert 3 == result1.executed_models[0].row_count
    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3, domain="sales", name="base_orders"
    )
    assert 3 == iceberg_spark.table(fq).count()

    executor2 = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-fullref-2",
    )
    sql_rows_1 = (
        "SELECT CAST('x' AS STRING) AS source_name, "
        "CAST('2026-08-20' AS DATE) AS business_date, "
        "CAST(500 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-99' AS STRING) AS order_id"
    )
    model.compiled_sql = sql_rows_1
    model.quality = SqlQualityExpectations(row_count_min=1)
    result2 = executor2.execute(models=[model])
    assert 1 == result2.executed_models[0].row_count
    # full_refresh = replacement → no accumulation, exactly 1 row.
    assert 1 == iceberg_spark.table(fq).count()


def test_append_accumulates_rows(
    iceberg_spark: SparkSession,
    tmp_path: Path,
) -> None:
    warehouse_root = str(tmp_path / "wh_append")
    executor1 = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-append-1",
    )
    model = _base_orders_model(
        name_suffix="_append",
        compiled_sql=(
            "SELECT CAST('orders_api' AS STRING) AS source_name, "
            "CAST('2026-08-15' AS DATE) AS business_date, "
            "CAST(100 AS DECIMAL(18,2)) AS amount, "
            "CAST('o-a' AS STRING) AS order_id"
        ),
        load_mode=SqlLoadMode.append,
        partition_columns=[],
        row_count_min=1,
    )
    executor1.execute(models=[model])

    executor2 = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-append-2",
    )
    model.compiled_sql = (
        "SELECT CAST('orders_api' AS STRING) AS source_name, "
        "CAST('2026-08-15' AS DATE) AS business_date, "
        "CAST(200 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-b' AS STRING) AS order_id"
    )
    executor2.execute(models=[model])

    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3,
        domain="sales",
        name="base_orders_append",
    )
    assert 2 == iceberg_spark.table(fq).count()


def test_partition_overwrite_targets_single_partition(
    iceberg_spark: SparkSession,
    tmp_path: Path,
) -> None:
    warehouse_root = str(tmp_path / "wh_po")
    initial_sql = (
        "SELECT CAST('orders_api' AS STRING) AS source_name, "
        "CAST('2026-08-15' AS DATE) AS business_date, "
        "CAST(100 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-init-1' AS STRING) AS order_id "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-15', 101, 'o-init-2' "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-16', 200, 'o-init-3' "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-16', 201, 'o-init-4' "
        "UNION ALL SELECT 'orders_backfill', DATE '2026-08-15', 50, 'o-init-5'"
    )
    bootstrap_exec = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-po-bootstrap",
        partition_values={},
    )
    model = _base_orders_model(
        name_suffix="_po",
        compiled_sql=initial_sql,
        load_mode=SqlLoadMode.full_refresh,
        partition_columns=["source_name", "business_date"],
        row_count_min=5,
    )
    bootstrap_exec.execute(models=[model])
    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3,
        domain="sales",
        name="base_orders_po",
    )
    assert 5 == iceberg_spark.table(fq).count()

    overwrite_sql = (
        "SELECT CAST('orders_api' AS STRING) AS source_name, "
        "CAST('2026-08-15' AS DATE) AS business_date, "
        "CAST(500 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-new-1' AS STRING) AS order_id "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-15', 888, 'o-new-2' "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-15', 999, 'o-new-3'"
    )
    model.compiled_sql = overwrite_sql
    model.load_mode = SqlLoadMode.partition_overwrite
    model.quality = SqlQualityExpectations(row_count_min=3)
    overwrite_exec = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-po-2",
        partition_values={
            "source_name": "orders_api",
            "business_date": "2026-08-15",
        },
    )
    overwrite_exec.execute(models=[model])
    # 3 new in partition + sibling partition 2026-08-16=2 + unrelated backfill=1
    assert 6 == iceberg_spark.table(fq).count()
    amounts_target = sorted(
        Decimal(str(row["amount"]))
        for row in iceberg_spark.sql(
            f"SELECT amount FROM {fq} WHERE source_name = 'orders_api' "
            "AND business_date = DATE '2026-08-15' ORDER BY amount"
        ).collect()
    )
    assert amounts_target == [Decimal("500"), Decimal("888"), Decimal("999")]
    sibling_amounts = sorted(
        Decimal(str(row["amount"]))
        for row in iceberg_spark.sql(
            f"SELECT amount FROM {fq} WHERE source_name = 'orders_api' "
            "AND business_date = DATE '2026-08-16' ORDER BY amount"
        ).collect()
    )
    assert sibling_amounts == [Decimal("200"), Decimal("201")]
    unrelated_amounts = sorted(
        Decimal(str(row["amount"]))
        for row in iceberg_spark.sql(
            f"SELECT amount FROM {fq} WHERE source_name = 'orders_backfill' "
            "ORDER BY amount"
        ).collect()
    )
    assert unrelated_amounts == [Decimal("50")]


def test_iceberg_same_path_rebuild_reads_via_self_query(
    iceberg_spark: SparkSession,
    tmp_path: Path,
) -> None:
    from elt_pipeline.sql.spark_executor import _is_iceberg_enabled

    assert _is_iceberg_enabled(iceberg_spark) is True
    warehouse_root = str(tmp_path / "wh_samepath")
    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3, domain="sales", name="canonical_orders"
    )
    init_rows_sql = (
        "SELECT "
        "CAST('orders_api' AS STRING) AS source_name, "
        "CAST('2026-08-15' AS DATE) AS business_date, "
        "CAST(100 AS DECIMAL(18,2)) AS amount, "
        "CAST('o-1' AS STRING) AS order_id "
        "UNION ALL SELECT 'orders_api', DATE '2026-08-15', 200, 'o-2'"
    )
    executor_seed = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-samepath-seed",
    )
    seed_model = CompiledSqlModel(
        model_id="level3.sales.canonical_orders",
        stage=SqlModelStage.level3,
        domain="sales",
        name="canonical_orders",
        target_table_name="canonical_orders",
        load_mode=SqlLoadMode.full_refresh,
        materialization="table",  # type: ignore[arg-type]
        manifest_path=Path("/tmp/_fake_manifest.yaml"),
        sql_path=Path("/tmp/_fake.sql"),
        compiled_sql=init_rows_sql,
        partition_columns=["source_name", "business_date"],
        quality=SqlQualityExpectations(row_count_min=2),
    )
    executor_seed.execute(models=[seed_model])
    assert 2 == iceberg_spark.table(fq).count()
    rebuild_sql = (
        "SELECT source_name, business_date, "
        "(amount * CAST(2 AS DECIMAL(18,2))) AS amount, order_id "
        f"FROM {fq}"
    )
    rebuild_model = CompiledSqlModel(
        model_id="level3.sales.canonical_orders",
        stage=SqlModelStage.level3,
        domain="sales",
        name="canonical_orders",
        target_table_name="canonical_orders",
        load_mode=SqlLoadMode.full_refresh,
        materialization="table",  # type: ignore[arg-type]
        manifest_path=Path("/tmp/_fake_manifest.yaml"),
        sql_path=Path("/tmp/_fake.sql"),
        compiled_sql=rebuild_sql,
        partition_columns=["source_name", "business_date"],
        quality=SqlQualityExpectations(row_count_min=2),
    )
    executor_rebuild = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-samepath-rebuild",
    )
    executor_rebuild.execute(models=[rebuild_model])
    rows = sorted(
        [tuple(r) for r in iceberg_spark.table(fq).select("amount", "order_id").collect()],
        key=lambda t: t[1],
    )
    assert 2 == len(rows)
    assert rows[0][1] == "o-1"
    assert rows[1][1] == "o-2"
    assert DecimalLike(200).__eq__(rows[0][0])
    assert DecimalLike(400).__eq__(rows[1][0])
    snapshots_df = iceberg_spark.sql(
        f"SELECT snapshot_id FROM {fq}.snapshots ORDER BY committed_at"
    ).collect()
    assert len(snapshots_df) >= 2


def test_partition_columns_preserved_and_metadata_files_exist(
    iceberg_spark: SparkSession,
    tmp_path: Path,
) -> None:
    warehouse_root = str(tmp_path / "wh_meta")
    executor = SparkSqlModelExecutor(
        spark=iceberg_spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id="run-meta-1",
    )
    model = _base_orders_model(
        name_suffix="_part",
        compiled_sql=(
            "SELECT CAST('orders_api' AS STRING) AS source_name, "
            "CAST('2026-08-15' AS DATE) AS business_date, "
            "CAST(100 AS DECIMAL(18,2)) AS amount"
        ),
        load_mode=SqlLoadMode.full_refresh,
        partition_columns=["source_name", "business_date"],
        row_count_min=1,
    )
    executor.execute(models=[model])
    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3,
        domain="sales",
        name="base_orders_part",
    )
    snapshots = iceberg_spark.sql(f"SELECT * FROM {fq}.snapshots").collect()
    assert len(snapshots) >= 1
    warehouse_conf = iceberg_spark.conf.get(
        f"spark.sql.catalog.{_iceberg_catalog_name()}.warehouse", ""
    )
    if warehouse_conf:
        wh_path = Path(warehouse_conf.removeprefix("file:"))
        meta = wh_path / "level3" / "sales" / "base_orders_part" / "metadata"
        if meta.exists():
            assert list(meta.glob("*.json")), "Iceberg metadata JSON missing"
            assert list(meta.glob("*.avro")), "Iceberg manifests .avro missing"


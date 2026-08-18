from __future__ import annotations

import re

from elt_pipeline.sql.merge_sql_generator import (
    build_merge_into_from_schema,
    build_merge_into_sql,
)


def test_build_merge_explicit_value_and_key_columns():
    sql = build_merge_into_sql(
        target_table="iceberg.level3.sales.base_orders",
        source_query="SELECT * FROM staging_updates",
        key_columns=["row_id"],
        value_columns=["amount", "order_id", "source_name", "business_date"],
        partition_columns=["source_name", "business_date"],
    )
    # Star patterns NOT in UPDATE / INSERT clauses (explicit columns only)
    non_using_clause = sql.split("USING (SELECT * FROM staging_updates) s", 1)[1]
    assert "*" not in non_using_clause
    assert "MERGE INTO iceberg.level3.sales.base_orders t" in sql
    assert "USING (SELECT * FROM staging_updates) s" in sql
    assert "ON t.row_id = s.row_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    # UPDATE SET includes non-key value columns (amount,order_id) or all non-key:
    for col in ("amount", "order_id", "source_name", "business_date"):
        assert f"t.{col} = s.{col}" in sql
    # INSERT lists columns explicitly:
    assert re.search(r"WHEN NOT MATCHED THEN INSERT \((.|\n)*row_id", sql)
    assert "VALUES (" in sql


def test_build_merge_from_schema_orders_all_columns_except_keys_in_update():
    schema = [
        "row_id",
        "source_name",
        "business_date",
        "amount",
        "order_id",
    ]
    sql = build_merge_into_from_schema(
        target_table="iceberg.level3.sales.base_orders",
        source_query="SELECT row_id, source_name, business_date, amount, order_id FROM delta",
        schema_columns=schema,
        key_columns=["row_id"],
        partition_columns=["source_name", "business_date"],
    )
    assert "ON t.row_id = s.row_id" in sql
    # UPDATE SET should NOT touch key column row_id:
    assert "t.row_id = s.row_id" not in sql.split("WHEN MATCHED")[1].split("WHEN NOT MATCHED")[0]
    assert "t.amount = s.amount" in sql
    assert "t.order_id = s.order_id" in sql


def test_merge_requires_at_least_one_action():
    err = False
    try:
        build_merge_into_sql(
            target_table="iceberg.x.y",
            source_query="SELECT 1",
            key_columns=["id"],
            when_matched_update=False,
            when_not_matched_insert=False,
        )
    except ValueError:
        err = True
    assert err


def test_merge_requires_key():
    err = False
    try:
        build_merge_into_sql(
            target_table="iceberg.x.y",
            source_query="SELECT 1",
            key_columns=[],
        )
    except ValueError:
        err = True
    assert err


def test_merge_insert_only_preserves_partition_cols_in_insert_list():
    sql = build_merge_into_sql(
        target_table="iceberg.level3.inventory.canonical_shipments",
        source_query=(
            "SELECT 1 AS row_id, 'inv' AS source_name, "
            "DATE '2026-08-15' AS business_date, 5 qty"
        ),
        key_columns=["row_id"],
        partition_columns=["source_name", "business_date"],
        when_matched_update=False,
        when_not_matched_insert=True,
    )
    assert "WHEN MATCHED" not in sql
    m = re.search(r"INSERT \(([^)]+)\)", sql, flags=re.MULTILINE)
    assert m
    cols = [c.strip().lower() for c in m.group(1).split(",")]
    for required in ("source_name", "business_date"):
        assert required in cols

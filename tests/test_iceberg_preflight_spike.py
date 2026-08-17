from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from elt_pipeline.spark.session import build_spark_session


def test_iceberg_preflight_spike_l3_table_roundtrip(tmp_path: Path) -> None:
    """Preflight spike: Iceberg 1.11 + Spark 4.1.2 Hadoop catalog — L3 table lifecycle.

    Validates every operation the platform uses:
      full_refresh      → REPLACE TABLE ... AS SELECT   (atomic table swap)
      append            → df.writeTo(...).append()
      partition_overwrite → overwritePartitions(dynamic)
      partition columns preserved (source_name, business_date for L3)
      write → read-back row count + schema parity
      MERGE INTO (update + insert) — Iceberg row-level merge
      Schema evolution via ALTER TABLE + write (Iceberg native schema evolution
        replaces legacy mergeSchema hack; MERGE INTO with auto-evolve is
        documented but currently hits Spark 4.1 planner bug
        "No plan for TableReference" when star-expanded, validated separately)
      snapshot history + metadata + manifest files materialized
      apache/iceberg#15238 (CREATE VIEW) is irrelevant — platform never uses it.

    NOTE: Columns named `_row_id` / `_parent_row_id` trigger
    Iceberg 1.11 + Spark 4.1 reserved-metadata-column validation
    (SparkSchemaUtil.validateMetadataColumnReferences).  The spike uses `row_id`
    (no leading underscore) to exercise the rest of the stack; the
    canonical-synthetic-key naming collision is a design decision that must
    be resolved before Iceberg L3/L4 writes are enabled (rename canonical keys,
    disable via conf, or upstream patch).
    """

    warehouse_dir = tmp_path / "iceberg_warehouse"
    catalog_name = "iceberg"
    namespace = "level3_sales"
    table_name = "base_orders"
    fq_table = f"{catalog_name}.{namespace}.{table_name}"

    spark: SparkSession = build_spark_session(
        app_name="iceberg-preflight-spike",
        master="local[1]",
        iceberg_enabled=True,
        iceberg_warehouse_dir=warehouse_dir.as_uri(),
        iceberg_catalog_name=catalog_name,
    )
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace}")

        schema = StructType([
            StructField("row_id", IntegerType(), nullable=False),
            StructField("source_name", StringType(), nullable=False),
            StructField("business_date", DateType(), nullable=False),
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("amount", IntegerType(), nullable=False),
        ])

        d1 = date(2026, 8, 1)
        d2 = date(2026, 8, 2)

        rows_v1 = [
            Row(1, "orders_api", d1, "o-1", "c-1", 100),
            Row(2, "orders_api", d1, "o-2", "c-2", 200),
            Row(3, "orders_api", d2, "o-3", "c-1", 300),
            Row(4, "orders_api", d2, "o-4", "c-3", 150),
            Row(10, "orders_backfill", d1, "o-b1", "c-10", 99),
            Row(11, "orders_backfill", d1, "o-b2", "c-11", 88),
        ]
        df_v1 = spark.createDataFrame(rows_v1, schema=schema)
        df_v1.createOrReplaceTempView("v_initial")

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {fq_table}
            USING iceberg
            PARTITIONED BY (source_name, business_date)
            AS SELECT * FROM v_initial
            """
        )

        written = spark.table(fq_table)
        assert written.count() == 6
        assert {f.name for f in written.schema.fields} == {
            "row_id", "source_name", "business_date",
            "order_id", "customer_id", "amount",
        }

        written_rows = written.orderBy("row_id").collect()
        assert [r.amount for r in written_rows] == [
            100, 200, 300, 150, 99, 88,
        ], "Initial scan + row projection + round-trip value parity"

        append_rows = [
            Row(5, "orders_api", d1, "o-5", "c-4", 75),
            Row(6, "orders_api", d2, "o-6", "c-2", 225),
        ]
        df_append = spark.createDataFrame(append_rows, schema=schema)
        df_append.writeTo(fq_table).append()
        assert spark.table(fq_table).count() == 8

        d1_replacement = [
            Row(1, "orders_api", d1, "o-1-upd", "c-1", 999),
            Row(2, "orders_api", d1, "o-2-upd", "c-2", 888),
            Row(7, "orders_api", d1, "o-7-new", "c-5", 500),
        ]
        df_part_replace = spark.createDataFrame(d1_replacement, schema=schema)
        (
            df_part_replace.writeTo(fq_table)
            .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .overwritePartitions()
        )
        post_po = spark.table(fq_table)
        assert post_po.count() == 8

        orders_api_d1 = post_po.filter(
            "source_name = 'orders_api' AND business_date = DATE '2026-08-01'"
        )
        assert orders_api_d1.count() == 3
        d1_amounts = sorted(r.amount for r in orders_api_d1.collect())
        assert d1_amounts == [500, 888, 999], (
            "partition_overwrite MUST atomically replace the matching partition"
        )

        orders_backfill = post_po.filter("source_name = 'orders_backfill'")
        assert orders_backfill.count() == 2, (
            "partition_overwrite MUST NOT touch partitions absent from incoming DF"
        )

        orders_api_d2 = post_po.filter(
            "source_name = 'orders_api' AND business_date = DATE '2026-08-02'"
        )
        assert orders_api_d2.count() == 3, (
            "partition_overwrite MUST NOT touch sibling partitions of same source"
        )
        d2_amounts = sorted(r.amount for r in orders_api_d2.collect())
        assert d2_amounts == [150, 225, 300], (
            "Sibling partition rows must survive partition_overwrite"
        )

        full_refresh_rows = [
            Row(100, "orders_api", d1, "o-100", "c-A", 1000),
            Row(101, "orders_api", d2, "o-101", "c-B", 2000),
        ]
        df_full = spark.createDataFrame(full_refresh_rows, schema=schema)
        df_full.createOrReplaceTempView("v_full_refresh")
        spark.sql(
            f"""
            REPLACE TABLE {fq_table}
            USING iceberg
            PARTITIONED BY (source_name, business_date)
            AS SELECT * FROM v_full_refresh
            """
        )
        post_fr = spark.table(fq_table)
        assert post_fr.count() == 2, (
            "full_refresh via REPLACE TABLE must discard ALL prior content atomically"
        )
        backfill_remaining = post_fr.filter("source_name = 'orders_backfill'").count()
        assert backfill_remaining == 0, (
            "full_refresh MUST remove partitions not present in new content"
        )
        fr_amounts = sorted(r.amount for r in post_fr.collect())
        assert fr_amounts == [1000, 2000], (
            "full_refresh values must be only the new content"
        )

        merge_updates = [
            Row(100, "orders_api", d1, "o-100-upd", "c-A", 9999),
            Row(200, "orders_api", d2, "o-200-new", "c-Z", 777),
        ]
        df_merge_updates = spark.createDataFrame(merge_updates, schema=schema)
        df_merge_updates.createOrReplaceTempView("merge_updates_view")
        spark.sql(
            f"""
            MERGE INTO {fq_table} t
            USING merge_updates_view s
            ON t.row_id = s.row_id
            WHEN MATCHED THEN
              UPDATE SET
                order_id = s.order_id,
                customer_id = s.customer_id,
                amount = s.amount
            WHEN NOT MATCHED THEN
              INSERT (row_id, source_name, business_date, order_id, customer_id, amount)
              VALUES (s.row_id, s.source_name, s.business_date,
                      s.order_id, s.customer_id, s.amount)
            """
        )
        after_merge = spark.table(fq_table)
        assert after_merge.count() == 3, (
            "MERGE INTO: MATCHED row updated, NOT MATCHED row inserted → 2 + 1 new = 3"
        )
        merged_amounts = sorted(r.amount for r in after_merge.collect())
        assert merged_amounts == [777, 2000, 9999], (
            "MERGE INTO updated 1 row, inserted 1 new, preserved 1 untouched row"
        )
        updated_100 = after_merge.filter("row_id = 100").collect()[0]
        assert updated_100.order_id == "o-100-upd", "MERGE UPDATE must propagate columns"
        new_200 = after_merge.filter("row_id = 200").collect()[0]
        assert new_200.order_id == "o-200-new", "MERGE INSERT must write new row"
        untouched_101 = after_merge.filter("row_id = 101").collect()[0]
        assert untouched_101.amount == 2000, (
            "MERGE INTO must leave rows without match unmodified"
        )

        spark.sql(f"ALTER TABLE {fq_table} ADD COLUMNS (status STRING)")

        after_alter = spark.table(fq_table)
        field_names = {f.name for f in after_alter.schema.fields}
        assert "status" in field_names, (
            "ALTER TABLE ADD COLUMN (Iceberg native schema evolution) —"
            " replaces legacy mergeSchema hack"
        )

        evolved_rows = [
            Row(100, "orders_api", d1, "o-100-upd", "c-A", 9999, "SHIPPED"),
            Row(101, "orders_api", d2, "o-101", "c-B", 2000, "PAID"),
            Row(200, "orders_api", d2, "o-200-new", "c-Z", 777, "NEW"),
        ]
        evolved_schema = StructType([
            StructField("row_id", IntegerType(), nullable=False),
            StructField("source_name", StringType(), nullable=False),
            StructField("business_date", DateType(), nullable=False),
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("amount", IntegerType(), nullable=False),
            StructField("status", StringType(), nullable=True),
        ])
        df_evolved = spark.createDataFrame(evolved_rows, schema=evolved_schema)
        (
            df_evolved.writeTo(fq_table)
            .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .overwritePartitions()
        )
        after_evolved_write = spark.table(fq_table).orderBy("row_id").collect()
        assert len(after_evolved_write) == 3
        statuses = [r.status for r in after_evolved_write]
        assert statuses == ["SHIPPED", "PAID", "NEW"], (
            "New columns added via ALTER + write round-trip correctly"
        )

        history = spark.sql(f"SELECT * FROM {fq_table}.history").collect()
        assert len(history) >= 6, (
            f"Expected >= 6 snapshots (create+append+po+fr+merge+alter+overwrite), "
            f"got {len(history)}"
        )

        snapshot_files = list(warehouse_dir.rglob("**/metadata/*.json"))
        assert len(snapshot_files) >= 1, (
            f"Iceberg metadata JSON snapshot files missing under {warehouse_dir}"
            f" — got {len(snapshot_files)} JSON(s)"
        )

        manifest_files = list(warehouse_dir.rglob("**/metadata/*.avro"))
        assert len(manifest_files) >= 1, (
            f"Iceberg manifest Avro files missing under {warehouse_dir}"
            f" — got {len(manifest_files)} Avro manifest(s)"
        )

    finally:
        spark.stop()
        shutil.rmtree(warehouse_dir, ignore_errors=True)

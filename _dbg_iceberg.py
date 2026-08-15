import shutil
import tempfile
from datetime import date
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from elt_pipeline.spark.session import build_spark_session

tmp = Path(tempfile.mkdtemp())
warehouse = tmp / "wh"
spark = build_spark_session(
    app_name="dbg",
    master="local[1]",
    iceberg_enabled=True,
    iceberg_warehouse_dir=warehouse.as_uri(),
    iceberg_catalog_name="iceberg",
)
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.level3_sales")
    fq = "iceberg.level3_sales.base_orders"
    schema = StructType([
        StructField("_row_id", IntegerType(), False),
        StructField("source_name", StringType(), False),
        StructField("business_date", DateType(), False),
        StructField("amount", IntegerType(), False),
    ])
    d1 = date(2026, 8, 1)
    d2 = date(2026, 8, 2)
    rows1 = [
        Row(1, "orders_api", d1, 100),
        Row(2, "orders_api", d1, 200),
        Row(3, "orders_api", d2, 300),
        Row(4, "orders_api", d2, 150),
    ]
    spark.createDataFrame(rows1, schema=schema).writeTo(fq).partitionedBy(
        "source_name", "business_date"
    ).create()
    print("After create:", spark.table(fq).count())
    spark.table(fq).groupBy("source_name", "business_date").count().show()

    append = [Row(5, "orders_api", d1, 75), Row(6, "orders_api", d2, 225)]
    spark.createDataFrame(append, schema=schema).writeTo(fq).append()
    print("After append 2:", spark.table(fq).count())
    spark.table(fq).groupBy("source_name", "business_date").count().show()

    bp = [Row(10, "bfill", d1, 99), Row(11, "bfill", d1, 88)]
    spark.createDataFrame(bp, schema=schema).writeTo(fq).append()
    print("After new partition:", spark.table(fq).count())
    spark.table(fq).groupBy("source_name", "business_date").count().show()

    repl = [
        Row(1, "orders_api", d1, 999),
        Row(2, "orders_api", d1, 888),
        Row(7, "orders_api", d1, 500),
    ]
    (
        spark.createDataFrame(repl, schema=schema)
        .writeTo(fq)
        .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .overwritePartitions()
    )
    print("After partition overwrite:", spark.table(fq).count())
    spark.table(fq).groupBy("source_name", "business_date").count().show()
    spark.table(fq).orderBy("_row_id").show(truncate=False)
finally:
    spark.stop()
    shutil.rmtree(tmp, ignore_errors=True)

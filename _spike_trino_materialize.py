from __future__ import annotations

from datetime import date
from pathlib import Path
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType, IntegerType, StringType, StructField, StructType,
)
from elt_pipeline.spark.session import build_spark_session

import os

REPO_ROOT = Path(__file__).resolve().parent
_HOME = Path(os.path.expanduser("~"))
_DEFAULT_RUN_DIR = _HOME / "Documents" / "__data" / "repo_run" / "results" / "elt_pipeline"
_env_override = os.environ.get("ELT_PIPELINE_REPO_RUN_DIR", "").strip()
REPO_RUN_DIR = Path(_env_override).expanduser().resolve() if _env_override else _DEFAULT_RUN_DIR
ART_DIR = REPO_RUN_DIR / ".artifacts"
WAREHOUSE = ART_DIR / "iceberg_trino_spike_warehouse"
H2_BASE = ART_DIR / "iceberg_trino_spike_derby"
CATALOG_NAME = "iceberg"
NAMESPACE = "level3_sales"
TABLE = "base_orders"
FQ = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE}"


def main() -> None:
    import shutil
    REPO_RUN_DIR.mkdir(parents=True, exist_ok=True)
    for d in (WAREHOUSE,):
        if d.exists():
            shutil.rmtree(d)
    H2_PARENT = H2_BASE.parent
    H2_PARENT.mkdir(parents=True, exist_ok=True)
    for suffix in (".mv.db", ".trace.db"):
        p = H2_BASE.with_suffix(suffix) if suffix == ".mv.db" else H2_BASE.parent / f"{H2_BASE.name}{suffix}"
        if p.exists():
            p.unlink()
    ART_DIR.mkdir(parents=True, exist_ok=True)

    h2_uri = f"jdbc:h2:file:{H2_BASE.as_posix()};DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE"

    spark: SparkSession = build_spark_session(
        app_name="trino-spike-materialize-jdbc",
        master="local[1]",
        iceberg_enabled=True,
        iceberg_warehouse_dir=WAREHOUSE.as_uri(),
        iceberg_catalog_name=CATALOG_NAME,
        iceberg_catalog_type="jdbc",
        iceberg_catalog_uri=h2_uri,
    )
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")
        schema = StructType([
            StructField("row_id", IntegerType(), False),
            StructField("source_name", StringType(), False),
            StructField("business_date", DateType(), False),
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", IntegerType(), False),
        ])
        d1 = date(2026, 8, 1)
        d2 = date(2026, 8, 2)
        rows = [
            Row(1, "orders_api", d1, "o-1", "c-1", 100),
            Row(2, "orders_api", d1, "o-2", "c-2", 200),
            Row(3, "orders_api", d2, "o-3", "c-1", 300),
            Row(4, "orders_api", d2, "o-4", "c-3", 150),
            Row(10, "orders_backfill", d1, "o-b1", "c-10", 99),
            Row(11, "orders_backfill", d1, "o-b2", "c-11", 88),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("v")
        spark.sql(
            f"CREATE OR REPLACE TABLE {FQ} USING iceberg"
            f" PARTITIONED BY (source_name, business_date) AS SELECT * FROM v"
        )
        n = spark.table(FQ).count()
        print(f"Materialized {FQ}: {n} rows")
        print(f"Warehouse URI: {WAREHOUSE.as_uri()}")
        print(f"H2 URI: {h2_uri}")
        hist = spark.sql(
            f"SELECT snapshot_id, made_current_at FROM {FQ}.history"
        ).collect()
        print(f"Snapshots: {len(hist)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

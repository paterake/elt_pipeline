-- Canonical orders: late-arrival repartitioning pattern.
--
-- Why this works:
--  1. We filter L2 rows by ingest_date = when the data ARRIVED (the run window). This is always ingest_date,
--     from the pipeline run). We read everything we received today.
--  2. We include business_date = when the event actually HAPPENED, from the payload itself.
--  3. Spark writes the output with partitionBy(source_name, business_date) defaults, so rows land
--     in their event-day bucket, NOT the arrival-day bucket. A row that arrived on
--     ingest_date=2026-08-10 but whose business_date is 2026-07-31 correctly lands in
--     business_date=2026-07-31, replacing only that (source_name, business_date) partition.
--  4. Re-running the same ingest_date window is idempotent — dynamic partition overwrite
--     replaces the same partitions with the same rows, no duplicates, no orphaned data.

WITH cte_src_base AS (
    SELECT
        order_id,
        amount,
        order_date,
        customer__customer_id AS customer_id,
        customer__name AS customer_name,
        CAST(NULL AS STRING) AS customer_email,
        CAST(NULL AS STRING) AS customer_phone,
        CAST(NULL AS STRING) AS billing_zip,
        amount AS order_total_usd,
        source_name,
        ingest_date,
        order_date AS business_date
    FROM raw_orders
    WHERE order_date >= '{{ window.start_date }}'
      AND order_date <= '{{ window.end_date }}'
)
SELECT * FROM cte_src_base

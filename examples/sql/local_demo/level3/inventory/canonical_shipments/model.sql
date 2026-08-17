-- Canonical shipments: late-arrival repartitioning pattern (by design).
--
-- Late-arrival flow (4 steps, by design not bolt-on):
--   1. Read L2 by INGEST_DATE: filter the source-aligned level2 table on when the data
--      ARRIVED (ingest_date = '{{ window.start_date }}'). This is always the run window.
--   2. Carry BUSINESS_DATE in the SELECT: ship_date from the payload is the event date —
--      when the shipment actually happened. It is renamed to business_date below so the
--      executor's default L3 partition convention (source_name, business_date) picks it up.
--   3. Spark writes by EVENT DATE: partitionBy(source_name, business_date) is the default
--      for L3 models with no explicit partition_columns + load_mode != full_refresh. So a
--      row that arrived on ingest_date=2026-08-10 but whose ship_date (business_date) is
--      2026-07-31 lands in partition business_date=2026-07-31/, NOT clustered with the
--      on-time 2026-08-10 arrivals.
--   4. Idempotent replay: re-running this model for the same ingest_date window produces
--      the same output rows, and dynamic partition overwrite replaces only the exact
--      (source_name, business_date) combinations appearing in the batch. Unrelated
--      partitions (other dates, other sources) are never touched — zero blast radius.

cte_src_base AS (
    SELECT
        shipment_id,
        order_id,
        carrier,
        tracking_number,
        ship_date,
        ship_date AS business_date,
        source_name,
        ingest_date,
        _run_id
    FROM raw_shipments
    WHERE ingest_date = '{{ window.start_date }}'
)
SELECT * FROM cte_src_base

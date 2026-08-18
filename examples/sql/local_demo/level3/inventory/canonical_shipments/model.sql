-- Canonical shipments: event-day partitioning with dynamic partition overwrite.
--
--   1. Read events in the run window: filter the source-aligned level2 table on
--      ship_date — when the shipment actually happened — between window.start_date and
--      window.end_date. (canonical_orders filters order_date the same way; both read by
--      EVENT date so a fixed historical window reprocesses a fixed set of events.)
--   2. Carry BUSINESS_DATE in the SELECT: ship_date is the event date. It is renamed to
--      business_date below so the executor's default L3 partition convention
--      (source_name, business_date) picks it up.
--   3. Spark writes by EVENT DATE: the manifest declares explicit partition_columns
--      (source_name, business_date), so each shipment lands in its own
--      business_date=<ship_date>/ partition rather than being clustered by arrival day.
--   4. Idempotent replay: load_mode full_refresh rewrites the table from the window each
--      run, so re-running the same window reproduces the same partitioned output.

WITH cte_src_base AS (
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
    WHERE ship_date >= '{{ window.start_date }}'
      AND ship_date <= '{{ window.end_date }}'
)
SELECT * FROM cte_src_base

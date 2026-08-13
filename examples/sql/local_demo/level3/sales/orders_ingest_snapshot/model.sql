-- Orders ingest snapshot: OPT-IN override of the default date partition convention.
--
-- Default pattern (see canonical_orders / canonical_shipments): partition by
-- (source_name, business_date) — late-arriving data lands in the correct event-day
-- partition automatically. This is the RIGHT CHOICE for 95% of canonical models.
--
-- Snapshot pattern (THIS MODEL): explicit partition_columns: [source_name, ingest_date]
-- in the manifest overrides the convention. Output is partitioned by ARRIVAL day instead
-- of event day. Use this for:
--   * Audit: "What did the data look like on the specific day we ingested it?"
--   * Compliance: proving exactly what was received on each ingest_date before any
--     downstream corrections or re-partitioning.
--   * Troubleshooting late arrivals: comparing ingest_date=D1's snapshot of business_date
--     rows against a later ingest_date=D2 snapshot of the same business_date rows.
--
-- Because we filter on ingest_date AND partition on ingest_date, each run appends or
-- overwrites exactly one day's snapshot — the WHERE clause and the write partition are
-- the same key, unlike the default late-arrival pattern where they differ.

cte_snapshot_base AS (
    SELECT
        order_id,
        amount,
        order_date AS business_date,
        customer_id,
        source_name,
        ingest_date,
        _run_id
    FROM raw_orders
    WHERE ingest_date = '{{ window.start_date }}'
)
SELECT * FROM cte_snapshot_base

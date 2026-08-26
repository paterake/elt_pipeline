-- =============================================================================
-- elt_pipeline — Medium / GitHub Quick Start: Trino SQL queries
-- =============================================================================
-- Prerequisites (run these from the repo root first):
--
--   Docker path (~5 minutes):
--     1. docker compose build
--     2. docker compose run --rm demo
--     3. docker compose up -d trino
--     4. sleep 30   # wait for Trino healthcheck to pass
--
--   No-Docker path (requires uv + Temurin 23 JDK):
--     1. uv sync --extra dev --extra spark
--     2. export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
--     3. elt-pipeline ingest run   examples/configs/local_object_storage_orders.yaml
--     4. elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml
--     5. elt-pipeline sql run --include-deps --environment workstation \
--          --start-date 2026-01-01 --end-date 2026-01-31 --domain sales \
--          --iceberg-enabled examples/sql/local_demo
--     6. elt-pipeline trino start-foreground   # in a second terminal
--
-- Then connect:
--     docker compose exec trino trino --catalog iceberg
--   OR (no-Docker):
--     trino --catalog iceberg --server http://127.0.0.1:8080
--
-- Paste queries below one at a time. Expected row counts are in comments.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1/6 — DISCOVERY: show what the pipeline wrote into Iceberg
-- ---------------------------------------------------------------------------
-- Expected: 2 schemas  (inventory, sales)
SHOW SCHEMAS;

-- Expected per schema:
--   inventory → canonical_shipments
--   sales     → base_orders, canonical_orders, order_summary, orders_ingest_snapshot
SHOW TABLES FROM sales;
SHOW TABLES FROM inventory;

-- ---------------------------------------------------------------------------
-- 2/6 — L4 MART: daily revenue from the BI-ready aggregation
-- ---------------------------------------------------------------------------
-- This is the "you get paid for this" table — the ELT's end product.
-- Expected: 2 rows (2026-01-01 $10 / 2026-01-02 $25)
SELECT
  order_date,
  total_amount                                AS daily_revenue_usd,
  to_char(total_amount, '$999,999,990.00')   AS daily_revenue_fmt
FROM sales.order_summary
ORDER BY order_date;

-- ---------------------------------------------------------------------------
-- 3/6 — L3 CANONICAL: row-level orders (repeatable, replayable pattern)
-- ---------------------------------------------------------------------------
-- Expected: 2 rows — Alice A-100 / Bob A-200
SELECT
  order_id,
  order_date,
  business_date,
  source_name,
  customer_id,
  customer_name,
  order_total_usd,
  ingest_date
FROM sales.canonical_orders
ORDER BY order_date;

-- Top customer by revenue over the window (Alice = $10, Bob = $25)
-- Expected: 2 rows — Bob first, Alice second
SELECT
  customer_id,
  customer_name,
  COUNT(*)          AS order_count,
  SUM(order_total_usd) AS lifetime_revenue_usd
FROM sales.canonical_orders
GROUP BY 1, 2
ORDER BY lifetime_revenue_usd DESC;

-- ---------------------------------------------------------------------------
-- 4/6 — CROSS-DOMAIN JOIN: orders ⋈ shipments (sales L3 + inventory L3)
-- ---------------------------------------------------------------------------
-- Fulfillment view: each order, when it shipped, who carried it, tracking #.
-- Expected: 2 rows — A-100 shipped Jan 1 / A-200 shipped Jan 2
SELECT
  o.order_id,
  o.customer_name,
  o.order_total_usd,
  o.order_date,
  s.shipment_id,
  s.carrier,
  s.tracking_number,
  s.ship_date,
  DATE_PART('day', s.ship_date - o.order_date) AS days_to_ship
FROM      sales.canonical_orders     o
LEFT JOIN inventory.canonical_shipments  s  ON s.order_id = o.order_id
ORDER BY o.order_date;

-- Carrier performance (in the tiny demo this is just acme_freight x 2)
-- Expected: 1 row — acme_freight: 2 shipments, avg days to ship 0
SELECT
  carrier,
  COUNT(*)                                   AS shipment_count,
  AVG(DATE_PART('day', s.ship_date - o.order_date))  AS avg_days_to_ship
FROM      inventory.canonical_shipments  s
LEFT JOIN sales.canonical_orders         o  ON o.order_id = s.order_id
GROUP BY carrier
ORDER BY shipment_count DESC;

-- ---------------------------------------------------------------------------
-- 5/6 — ICEBERG HOUSEKEEPING: snapshots & audit metadata
-- ---------------------------------------------------------------------------
-- Apache Iceberg is a table-format with versioned snapshots. The pipeline's
-- `maintain run` step calls expire_snapshots + rewrite_data_files(compact).
-- Show the current snapshot metadata for the canonical_orders table:
SELECT
  committed_at,
  snapshot_id,
  parent_id,
  operation,
  manifest_list,
  summary
FROM iceberg.sales."canonical_orders$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- History of table changes (each run writes a new snapshot; maintain cleans up)
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM iceberg.sales."canonical_orders$history"
ORDER BY made_current_at DESC;

-- ---------------------------------------------------------------------------
-- 6/6 — SANITY CHECK: row counts across all layers
-- ---------------------------------------------------------------------------
-- If you see these counts, the pipeline did its job end-to-end.
SELECT 'L3 → sales.canonical_orders'        AS tbl, COUNT(*) AS n FROM sales.canonical_orders
UNION ALL
SELECT 'L3 → inventory.canonical_shipments' AS tbl, COUNT(*) AS n FROM inventory.canonical_shipments
UNION ALL
SELECT 'L4 → sales.order_summary'           AS tbl, COUNT(*) AS n FROM sales.order_summary
ORDER BY tbl;
-- Expected:
--   tbl                             n
--   L3 → inventory.canonical_shipments  2
--   L3 → sales.canonical_orders         2
--   L4 → sales.order_summary            2

-- =============================================================================
-- End of quick-start queries.  What next?
--   • Re-run `sql run` with --start-date 2026-01-01 --end-date 2026-12-31 to
--     replay a wider window (idempotent: dynamic partition overwrite replaces
--     only the touched business_date partitions).
--   • Drop your own JSON into examples/data/object_storage/orders/ and re-run
--     ingest → normalize → sql → maintain → query via Trino.
--   • Read docs/CAPABILITY_MATURITY_MATRIX.md for the 6 writer catalogs,
--     7 serving catalogs, 4 secrets providers, and multi-DB SQL connectors.
-- =============================================================================

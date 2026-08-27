# Bring Your Own Data — 30-Minute Tutorial

Goal: You have a CSV. You want to get it from raw CSV → governed Iceberg `level3` → BI-ready mart `level4` → a semantic metric you can run in 3 modes (materialize / Trino view / Prometheus gauge). Everything in this tutorial uses local POSIX storage and the existing CLI commands — no Spark code, no core edits, no cloud.

We will build a tiny e-commerce refunds domain (different from the existing sales one in `local_demo`, so you can copy-paste and rename without collisions).

Target table you will produce at the end:
| Level | Name | What it is |
|---|---|---|
| L3 | `level3.refunds.base_refunds` | Canonical, one-row-per-refund, typed, classified, retention-tagged |
| L4 | `level4.refunds.monthly_refunds` | Grouped by month — total_refund_amount + refund_count rows |
| Metric | `refunds.monthly_total_refund_amount` | Sum of total_refund_amount, dimension=month, 3 run modes |

Estimated time: 30 minutes. Time-boxed checkpoints every 10 minutes.

---

## Prerequisites (5 minutes)

Before starting, confirm:

```bash
cd <your-repo-root>
# Step 1: Python env + JVM per CONTRIBUTING.md
uv sync --extra dev --extra spark
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"

# Step 2: JVM + gate smoke (2 minutes)
uv run python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').appName('smoke').getOrCreate().stop(); print('JVM OK')"
```

If gate smoke fails, see the `4-core Apple Silicon` troubleshooting row in [docs/maintainer/JVM_TOOLCHAIN_SETUP.md](../maintainer/JVM_TOOLCHAIN_SETUP.md) to set `ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g`, then retry.

Create a workspace directory for this tutorial. We'll put it under `examples/sql/byod_refunds/` — same pattern as `local_demo`, so `elt-pipeline sql run examples/sql/byod_refunds` works.

```bash
mkdir -p examples/sql/byod_refunds/{level3/refunds/base_refunds,level4/refunds/monthly_refunds}
mkdir -p examples/sql/byod_refunds/metrics/refunds/monthly_total_refund_amount
```

---

## Step 1 — Your CSV (2 minutes)

Create the input. Save this file anywhere convenient (for the tutorial, we'll put it at `examples/data/byod_refunds.csv`):

```csv
refund_id,customer_id,refund_amount,refund_status,refund_date,merchant_id,channel
10001,2001,49.99,approved,2026-01-03T09:12:00,MERCH-1,card
10002,2002,120.00,approved,2026-01-08T14:33:00,MERCH-2,bank_transfer
10003,2003,12.50,rejected,2026-01-11T02:01:00,MERCH-1,card
10004,2001,200.00,approved,2026-01-17T18:44:00,MERCH-3,card
10005,2004,7.25,approved,2026-01-22T10:20:00,MERCH-1,card
10006,2005,500.00,approved,2026-02-02T07:45:00,MERCH-2,bank_transfer
10007,2002,35.00,approved,2026-02-09T16:00:00,MERCH-3,card
10008,2006,250.00,rejected,2026-02-15T21:30:00,MERCH-1,card
10009,2003,80.00,approved,2026-02-21T11:08:00,MERCH-2,bank_transfer
10010,2007,15.75,approved,2026-02-28T13:15:00,MERCH-1,card
```

Checkpoint (end of 10 min): File created. Columns: 1 PK `refund_id`, 2 numerical `refund_amount`, 4 categorical, 1 datetime.

---

## Step 2 — level3.base_refunds manifest (10 minutes)

`level3` = canonical, typed, one-row-per-business-event, with classification + retention. Save this to:

**`examples/sql/byod_refunds/level3/refunds/base_refunds/manifest.yaml`**

```yaml
id: base_refunds
version: 1
description: Canonical refunds — one row per refund event, typed + classified.
level: 3
domain: refunds
materialization: table

sources:
  - entity: refunds_csv
    schema: level2.refunds.raw_refunds_csv

materialization:
  strategy: create_or_replace_table
  partition_by:
    - refund_month

columns:
  - name: refund_id
    data_type: bigint
    semantic_type: identifier
    description: Stable per-refund PK from source.
    expression: refund_id
  - name: customer_id
    data_type: bigint
    semantic_type: identifier
    description: Customer ID that requested the refund.
    expression: customer_id
    masking: hash_sha256          # DataClassification.confidential → masks at Trino serving
  - name: refund_amount
    data_type: decimal(18,2)
    semantic_type: measure
    description: Amount refunded in USD (source currency).
    expression: refund_amount
  - name: refund_status
    data_type: string
    semantic_type: dimension
    description: approved | rejected.
    expression: refund_status
  - name: merchant_id
    data_type: string
    semantic_type: dimension
    expression: merchant_id
  - name: channel
    data_type: string
    semantic_type: dimension
    expression: channel
  - name: refund_date
    data_type: timestamp
    semantic_type: event_time
    description: Source timestamp of the refund request.
    expression: refund_date
  - name: refund_month
    data_type: string
    semantic_type: dimension
    description: ISO year-month bucket (2026-01, 2026-02). Derived.
    expression: date_format(refund_date, 'yyyy-MM')

governance:
  classification: internal        # DataClassification enum — also used by our metric
  owner_team: finance-data
  owner_email: data-governance@example.com
  retention_days: 365             # maintain run's expiry uses this
  retention_partition_column: refund_month
  sla_daily_ready_hour_utc: 7
  quality_checks:
    - type: uniqueness
      columns: [refund_id]
      severity: critical
    - type: not_null
      columns: [refund_id, refund_amount, refund_date]
      severity: critical
    - type: allowed_values
      column: refund_status
      allowed: [approved, rejected]
      severity: warn
    - type: range
      column: refund_amount
      min_inclusive: 0
      severity: critical
  columns:
    - name: refund_id
      data_type: bigint
      description: Stable per-refund PK
      primary_key: true
    - name: customer_id
      data_type: bigint
      description: Customer PK (confidential — masked at serving)
      classification: confidential
    - name: refund_amount
      data_type: decimal(18,2)
      description: Refunded USD amount
      classification: internal
    - name: refund_status
      data_type: string
      description: Final status of refund request
      classification: internal
    - name: merchant_id
      data_type: string
      description: Merchant identifier string
      classification: internal
    - name: channel
      data_type: string
      description: Refund payment channel (card / bank_transfer)
      classification: internal
    - name: refund_date
      data_type: timestamp
      description: Source timestamp
      classification: internal
    - name: refund_month
      data_type: string
      description: ISO month bucket
      classification: internal
```

Quick validation smoke — no JVM, no Spark, pure compile:

```bash
uv run elt-pipeline sql compile examples/sql/byod_refunds \
    --level 3 --domain refunds --model base_refunds --format summary
```

Expected exit code `0` + a summary line saying `level3.refunds.base_refunds: 8 cols, 1 source, 4 quality checks`. If it fails, 99% of the time it's a YAML indentation error — the error messages include exact line numbers, so fix and re-run.

Checkpoint (end of 20 min): L3 manifest compiles clean.

---

## Step 3 — level4.monthly_refunds mart manifest (5 minutes)

`level4` = BI-ready aggregations. Save to:

**`examples/sql/byod_refunds/level4/refunds/monthly_refunds/manifest.yaml`**

```yaml
id: monthly_refunds
version: 1
description: Refund totals and counts per ISO month, including only approved refunds.
level: 4
domain: refunds
materialization: table

sources:
  - entity: base_refunds
    schema: level3.refunds.base_refunds

materialization:
  strategy: create_or_replace_table
  partition_by:
    - refund_month

columns:
  - name: refund_month
    data_type: string
    semantic_type: dimension
    description: ISO year-month bucket (2026-01 / 2026-02).
    expression: refund_month
  - name: total_refund_amount
    data_type: decimal(18,2)
    semantic_type: measure
    description: Sum of approved refund amounts for the month.
    expression: sum(refund_amount)
  - name: refund_count
    data_type: bigint
    semantic_type: measure
    description: Number of approved refunds for the month.
    expression: count(*)
  - name: distinct_customers
    data_type: bigint
    semantic_type: measure
    description: Count of distinct customers that had at least one approved refund.
    expression: count(distinct customer_id)

filters:
  - name: approved_only
    predicate: refund_status = 'approved'
    required: true

group_by:
  - refund_month

governance:
  classification: internal
  owner_team: finance-analytics
  owner_email: bi-eng@example.com
  retention_days: 365
  sla_daily_ready_hour_utc: 8
  columns:
    - name: refund_month
      data_type: string
      description: ISO year-month bucket
      classification: internal
    - name: total_refund_amount
      data_type: decimal(18,2)
      description: Approved refunds summed per month
      classification: internal
    - name: refund_count
      data_type: bigint
      description: Approved refunds count per month
      classification: internal
    - name: distinct_customers
      data_type: bigint
      description: Distinct customers with ≥1 approved refund
      classification: internal
```

Validate + optionally include-deps (so L3 + L4 compile in one command):

```bash
uv run elt-pipeline sql compile examples/sql/byod_refunds \
    --include-deps --level 4 --domain refunds --model monthly_refunds --format summary
```

Expected: Two lines in the output (`level3.refunds.base_refunds` as a dep + `level4.refunds.monthly_refunds`).

Checkpoint (end of 25 min): L3 + L4 compile.

---

## Step 4 — Semantic metric manifest (3 minutes)

This is the new GAP-4 capability. Save to:

**`examples/sql/byod_refunds/metrics/refunds/monthly_total_refund_amount/metric.yaml`**

```yaml
name: monthly_total_refund_amount
description: >
  Approved refund total per ISO month. Mirrors the total_refund_amount
  column of level4.refunds.monthly_refunds, re-aggregated to allow the
  framework to run it in 3 modes (Iceberg materialize table, Trino
  SECURITY DEFINER VIEW, Prometheus gauge) with bidirectional hash
  guardrail when ≥2 modes are combined.
domain: refunds

query_ref: level4.refunds.monthly_refunds.total_refund_amount
aggregation: sum
cumulative_rolling: false

dimensions:
  - name: refund_month
    is_time_dimension: true
    description: ISO year-month bucket (from L4 group_by).

owners:
  - name: Finance Analytics
    email: bi-eng@example.com
    role: product_owner
  - name: Data Eng Oncall
    email: data-oncall@example.com
    role: primary_oncall

# This required_role field triggers SECURITY DEFINER VIEW when we use the
# "view" run mode (G-6 classification masking). If you want a plain VIEW
# for development, delete or comment out this line — the metric still works
# identically otherwise.
required_role: internal

filters:
  - name: exclude_zeros
    predicate: total_refund_amount > 0
    required: false
```

Smoke test with ref-validation (GAP-4 `--with-sql-refs`):

```bash
uv run elt-pipeline metric compile examples/sql/byod_refunds \
    --with-sql-refs --format summary
```

Expected output:

```
[OK] refunds.monthly_total_refund_amount: sum(total_refund_amount) on level4.refunds.monthly_refunds dims=[refund_month*] hash=<12-char hex>…
```

Exit code `0`. If it says `ConfigValidationError` with context key `missing: "column"` — you misspelled `total_refund_amount` in query_ref vs the L4 manifest's governance.columns[] array.

Checkpoint (end of 28 min): All 3 manifests compile with zero errors.

---

## Step 5 — Run everything end-to-end (2 minutes + optional Spark)

Now that everything compiles, the hard part is done. The rest is just `sql run` and `metric run` against actual data.

**For a no-Spark dry-run to confirm your entire package structure is right:**

```bash
uv run elt-pipeline metric run examples/sql/byod_refunds \
    --mode view \
    --environment workstation \
    --root-path .ignore/runtime_byod \
    --warehouse-root .ignore/warehouse_byod \
    --target-catalog iceberg \
    --target-namespace refunds
```

Expected exit code `0` (metric run writes success JSON even without a live Trino to execute the view DDL string — the runtime returns the DDL, execution is the operator connector's job).

**To actually materialize Iceberg tables (L3 + L4) AND run the metric against them in a single invocation (needs Spark, ~1 minute on a laptop):**

```bash
# 1. L3 + L4 materialized (with --iceberg-enabled, --include-deps ensures L3 runs first)
uv run elt-pipeline sql run examples/sql/byod_refunds \
    --include-deps --iceberg-enabled \
    --environment workstation \
    --start-date 2026-01-01 --end-date 2026-02-28 \
    --root-path .ignore/runtime_byod --warehouse-root .ignore/warehouse_byod \
    --target-catalog iceberg --target-namespace refunds

# 2. Metric materialize+prometheus+view 3-way with bidirectional guardrail
uv run elt-pipeline metric run examples/sql/byod_refunds \
    --mode materialize --mode view --mode prometheus \
    --environment workstation \
    --root-path .ignore/runtime_byod --warehouse-root .ignore/warehouse_byod \
    --target-catalog iceberg --target-namespace refunds
```

The 3-mode combination triggers the hash guardrail — all three modes compute a SHA-256 `generated_sql_hash` from the identical normalized SQL (using the `SOURCE_TABLE` placeholder, decoupled from your catalog/namespace prefix). If any two hashes don't match it fails with `METRIC_MODE_INCONSISTENT` instead of writing divergent outputs.

After the run, check the audit:

```bash
# One JSONL line per metric per mode (3 here)
cat .ignore/runtime_byod/runs/*/metrics/metric_audit.jsonl
```

Each line includes: `metric_id`, `mode`, ISO timestamps, `total_sum`, `non_null_count`, `generated_sql_hash`, `output_location`, `success`, `error_message`.

---

## What you just built — mapped to platform capabilities

1. **L3 base_refunds (governed canonical):** Compliance-ready Iceberg table with `governance.retention_days=365` + partitioned by `refund_month` → the `maintain run` subcommand's `expire_snapshots` + `rewrite_manifests` know exactly where to cut. The `customer_id` column is `DataClassification.confidential` and tagged `masking: hash_sha256` — Trino serving path (M-4) hashes it at query time without L4 engineers having to remember.

2. **L4 monthly_refunds (mart):** `filters.approved_only required=true` ensures this mart NEVER accidentally counts rejected refunds — compile-time fail if filter is dropped from future edits. GROUP BY mirror matches the metric's dimension list.

3. **Metric 3-way run with guardrail:** Same metric definition reused 3 ways. Your data platform team owns the YAML; mode selection is a CLI flag day-of-run. If you need an extra aggregation (p95 refund amount, distinct count), extend the `MetricAggregation` enum in `src/elt_pipeline/metrics/_models.py` + 1 arm in `_build_aggregation_sql` — zero other edits anywhere, 3 modes + guardrail + audit work automatically.

---

## Next steps after this tutorial

- **Add a second metric** (`refunds.monthly_refund_count` against `level4.refunds.monthly_refunds.refund_count`). Two metrics share an L4 table 99% of the time. The metric compile/run CLI handles any number of manifests inside `metrics/<domain>/` transparently.
- **Publish a CSV export.** Copy `examples/publish/local_demo/` to `examples/publish/byod_refunds/` and point `source_model: level4.refunds.monthly_refunds`. Run `uv run elt-pipeline publish run … --window-label 2026-01`.
- **Plug into orchestration.** Copy `examples/orchestration/airflow/reference_dag.py` and substitute the commands above. The CLI remains authoritative.
- **Want a new capability not yet listed?** Follow §5 of [CONTRIBUTING.md](../../../CONTRIBUTING.md) — 9 times out of 10 you'll implement it as a registered Protocol plugin (register_connector_factory / register_backend / register_provider / QualityHookBackend / SqlDbDriver / MetricAggregation enum) with ZERO core file edits.

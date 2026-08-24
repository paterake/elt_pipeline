# Governance, Retention, and Right-to-Erasure Operator Runbook

## Overview

This runbook documents the governance subsystem delivered via BACKLOG item **G-6**. It covers:

1. Data-classification tiers and how to tag columns in model manifests
2. How classification tags propagate to Iceberg table properties (L3/L4)
3. Column-level masking at the Trino serving layer using role-based views
4. Retention policy execution via partition drop + Iceberg snapshot expiry
5. Right-to-erasure (RTBF) procedure using Iceberg row-level DELETE + maintenance sweep

**Prerequisites:**
- Iceberg catalog bound for L3/L4 writes (section 03 PRD)
- Trino JDBC serving endpoint running for masked views (L5, PRD 06)
- G-1 Iceberg maintenance subsystem for post-operation sweeps (compaction / snapshot expiry / orphan cleanup)

---

## 1. Data Classification Tiers

| Tier | `DataClassification` enum | Description | Default Masking If Unspecified |
|---|---|---|---|
| Public | `public` | Open data, no sensitivity. Examples: product SKU, publicly-known country codes. | None. |
| Internal | `internal` | Data internal to the business. Examples: internal cost centre IDs, gross revenue. | `truncate_end` (8-char prefix + `***`). |
| Confidential | `confidential` | Sensitive business data, access-restricted. Examples: order totals, non-PII customer behaviour. | `truncate_end` (4-char prefix + `***`). |
| Restricted PII | `restricted_pii` | Personally-identifiable information subject to GDPR/CCPA/HIPAA etc. Examples: email, phone, SSN, street address. | `nullify` (NULL on non-role access unless explicit masking chosen). |

**Cross-tier invariant:** a column's explicit `masking` strategy MUST be compatible with its classification. The `SqlColumnSpec` validator enforces this at manifest-parse time. Pattern-specific strategies (`redact_email`, `redact_ssn`) are only valid for `restricted_pii`.

### 1.1 Tagging in Model Manifests

Add a `governance:` block to any L3 or L4 model `manifest.yaml`:

```yaml
governance:
  classification: confidential            # table-level default tier
  retention_days: 2555                      # 7-year rolling window
  retention_partition_column: dt            # Hive partition col for window ops
  columns:
    - name: customer_email
      description: Customer account email
      classification: restricted_pii        # override default for this col
      masking: redact_email                 # explicit strategy
    - name: customer_phone
      classification: restricted_pii
      masking: truncate_middle
    - name: billing_zip
      classification: confidential
      masking: truncate_end
    - name: order_total_usd
      classification: internal              # inherits table default, explicit OK
  custom_properties:
    data_owner: "Sales Ops"
    sla_tier: gold
```

When a column has no explicit `classification`, it inherits the table-level `governance.classification`. If that is also unset, it defaults to `public`-equivalent behaviour.

### 1.2 Verifying Tags in Iceberg

After a model runs (any write mode: full_refresh / append / partition_overwrite), the framework applies tags via `ALTER TABLE … SET TBLPROPERTIES`. Verify:

```sql
-- Spark shell or sql/runtime.py EXPLAIN harness
DESCRIBE TABLE EXTENDED iceberg.level3.sales.canonical_orders;

-- Filter: any key under elt.governance.*
-- Expected output:
--   elt.governance.classification          restricted_pii    (strictest across all cols)
--   elt.governance.retention_days          2555
--   elt.governance.retention_partition_column  dt
--   elt.governance.column.classification.customer_email   restricted_pii
--   elt.governance.column.masking.customer_email          redact_email
--   elt.governance.column.description.customer_email      Customer account email
--   elt.governance.custom.data_owner        Sales Ops
--   elt.governance.owner_name               platform
--   elt.governance.owner_email              data-platform@example.com
```

Trino/JDBC equivalent:
```sql
SELECT key, value FROM system.metadata.table_properties
WHERE catalog = 'iceberg' AND schema = 'level3_sales'
  AND key LIKE 'elt.governance.%'
ORDER BY key;
```

---

## 2. Column-Level Masking at the Trino Serving Layer (L5)

Masking is enforced at the **serving view layer** using role-based Trino SQL. The framework ships a pure SQL generator — you run the output. This avoids modifying base L3/L4 data; masking is purely a function of which role the connecting user holds.

### 2.1 Generator API

```python
from elt_pipeline.shared.governance import (
    SqlColumnSpec,
    SqlModelGovernance,
    build_trino_masking_view,
)

columns = [
    SqlColumnSpec(name="order_id"),
    SqlColumnSpec(name="dt"),
    SqlColumnSpec(name="customer_email", classification="restricted_pii", masking="redact_email"),
    SqlColumnSpec(name="customer_phone", classification="restricted_pii", masking="truncate_middle"),
    SqlColumnSpec(name="order_total_usd", classification="internal"),
]
governance = SqlModelGovernance(classification="confidential", retention_days=2555)

sql = build_trino_masking_view(
    base_table_fq="iceberg.level3_sales.canonical_orders",
    view_fq="analytics.level3_sales.v_canonical_orders_masked",
    columns=columns,
    governance=governance,
    unmask_role="analytics_pii_unmask",   # members of this role see raw values
)
print(sql)
```

Grant `analytics_pii_unmask` only to auditors and automated pipelines that need raw values. Everyone else hits the masked expressions.

### 2.2 Masking Semantics

| Strategy | Output Example (input = `user@example.com`) |
|---|---|
| `none` | `user@example.com` |
| `nullify` | `NULL` |
| `hash_sha256` | `to_hex(sha256(to_utf8(...)))` (deterministic, irreversible) |
| `redact_email` | `u**r@example.com` (1st + last char of local-part, domain preserved) |
| `redact_ssn` | `***-**-6789` (last-4 preserved, rest masked) |
| `truncate_middle` | `u****************************n` (1st + 1st char, rest `*`) |
| `truncate_end` | `user***` (8-char prefix + `***` for `internal`, 4-char prefix + `***` for `confidential`) |

### 2.3 Serving-Layer Enforcement Checklist

1. **Never** expose base L3/L4 tables directly to end-user JDBC roles. Only expose views generated with the masking generator.
2. Bind Trino `system.security` or a custom `SystemAccessControl` that restricts direct table SELECT to a service account only.
3. Verify with `SHOW GRANTS` that end-user roles have access ONLY to the masked views.
4. Audit access via Trino event-log query (event type `QueryCreated` + `QueryCompleted` + user tag).

---

## 3. Retention Policy Execution

Retention combines two operations:
1. **Drop data outside the window** (fast, partition-aware)
2. **Purge history** (snapshot expiry + orphan file sweep via G-1)

### 3.1 Standard Retention Run

Given:
- `retention_days = 2555`
- `retention_partition_column = dt` (Hive-style `dt` partition)
- Table FQN = `iceberg.level3.sales.canonical_orders`

**Step 1 — Drop partitions outside retention:**
```sql
-- Option A: DELETE-based (row-level, works for any partition scheme)
DELETE FROM iceberg.level3.sales.canonical_orders
 WHERE dt < DATE '2019-09-01';  -- today - 2555 days

-- Option B: ALTER TABLE DROP PARTITION (FAST, for partition schemes Spark can prune)
ALTER TABLE iceberg.level3.sales.canonical_orders DROP PARTITION (dt < '2019-09-01');
```

Use the helper to build the predicate string:
```python
from elt_pipeline.shared.governance import build_retention_delete_statement
print(build_retention_delete_statement(
    table_fq="iceberg.level3.sales.canonical_orders",
    partition_col="dt",
    retention_days=2555,
))
# DELETE FROM iceberg.level3.sales.canonical_orders WHERE dt < DATE '…'
```

**Step 2 — Expire old snapshots and remove orphans:**
```bash
elt maintain run \
    --table iceberg.level3.sales.canonical_orders \
    --snapshot-retain-days 7 \
    --orphan-older-than-days 3 \
    --compact \
    --expire-snapshots \
    --remove-orphan-files
```

See G-1 closure notes for full CLI semantics. Run this within 1-2 days of any retention window drop.

### 3.2 Non-Partitioned Tables

For non-partitioned L4 marts:
```sql
DELETE FROM iceberg.level4.sales.order_summary
 WHERE run_date < (CURRENT_DATE - INTERVAL '2555' DAY);
```

Follow with the same G-1 maintenance sweep.

---

## 4. Right-to-Erasure (GDPR RTBF / CCPA / Individual Delete Requests)

Use Iceberg row-level DELETE for the user's data, then run G-1 to physically remove data files.

**Never hard-delete before confirming audit:**
1. Extract a signed manifest of the IDs/records to be deleted (PII team sign-off).
2. Write the manifest to a quarantined path with write-only access for the erasure operator.
3. Run the delete in a staging environment first, verify row counts.

### 4.1 Standard Erasure Procedure

**Step 1 — Build and run the DELETE statement:**

```python
from elt_pipeline.shared.governance import (
    build_erasure_statement,
    build_row_level_erasure_statement,
)

# Option A: by composite key (name + email + zip)
sql = build_erasure_statement(
    table_fq="iceberg.level3.sales.canonical_orders",
    where_conditions={
        "customer_email": "user@example.com",
        "customer_phone": "555-01-9999",
    },
)
print(sql)
# DELETE FROM iceberg.level3.sales.canonical_orders
#  WHERE customer_email = 'user@example.com' AND customer_phone = '555-01-9999'

# Option B: by batch of order_ids (batched automatically if > batch_size)
sql = build_row_level_erasure_statement(
    table_fq="iceberg.level3.sales.canonical_orders",
    id_column="order_id",
    ids_to_erase=["ORD-1001", "ORD-1002", "ORD-1003"],
    batch_size=500,
)
```

**Step 2 — Apply to EVERY L3 + L4 table holding the individual's data:**
- L3 canonical tables (`canonical_orders`, `canonical_shipments`, etc.)
- L4 aggregation marts (`order_summary`, `customer_360`, etc.)
- Derived feature tables in ML / analytics schemas (if any)

**Step 3 — Confirm erasure completeness:**
```sql
SELECT count(*) AS residual_rows
  FROM iceberg.level3.sales.canonical_orders
 WHERE customer_email = 'user@example.com';
-- → 0
```

**Step 4 — Purge snapshots and orphans (MANDATORY):**

Run G-1 across *all* tables touched by the erasure:

```bash
elt maintain run \
    --table iceberg.level3.sales.canonical_orders \
    --table iceberg.level3.sales.canonical_shipments \
    --table iceberg.level4.sales.order_summary \
    --snapshot-retain-days 1 \
    --orphan-older-than-days 1 \
    --expire-snapshots \
    --remove-orphan-files \
    --compact
```

With `--snapshot-retain-days 1`, no pre-delete snapshot survives past 24h. This removes access to historical SELECTs using `VERSION AS OF`.

**Step 5 — Log the erasure operation:**
Append to the run's `errors.jsonl` or `alerts.jsonl` (G-2 subsystem) with event type:
```json
{
  "event": "rtbf_erasure",
  "ticket_ref": "GDPR-2026-0824-001",
  "tables_touched": ["level3.sales.canonical_orders", "…"],
  "ids_erased_count": 17,
  "operator": "jane.smith@example.com",
  "completed_at": "2026-08-24T10:12:45Z"
}
```

### 4.2 RTBF Validation Gate

*Do not close an erasure ticket until all four pass:*
1. ✅ Residual row count = 0 in every touched table.
2. ✅ No snapshots older than 1 day survive (run `elt maintain run --dry-run` to verify).
3. ✅ Orphan file list = empty after sweep (run with `--dry-run` → reports intended orphan deletes).
4. ✅ Masked serving view confirms the individual cannot be identified: query the view with role=masked-user, verify all PII is NULL/redacted even before the hard delete's purge-tick completes (defence-in-depth).

---

## 5. Retention + Erasure Operator: Daily Cron

Sample 04:00 UTC daily job combining retention for all L3 + L4 tables:

```yaml
# deploy overlay: cronjob-daily-elt.yaml (G-4 pattern)
- name: retention-erasure-sweep
  schedule: '0 4 * * *'
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec:
          containers:
            - name: retention
              args:
                - maintain
                - run
                - --all-level3
                - --all-level4
                - --compact
                - --expire-snapshots
                - --snapshot-retain-days
                - '7'
                - --remove-orphan-files
                - --orphan-older-than-days
                - '3'
```

Individual RTBF deletes run **on-demand**, never in cron.

---

## 6. Audit Trail and Retention for Governance Operations

The framework's existing audit trail (G-2) records every run. In addition, ensure:

- **Retention/erasure operations are run with an explicit `job_name` prefix** (`retention_` / `rtbf_`) so they are easy to search in audit logs.
- **Ticket references** are injected via the CLI `--attribute` flag → `RunContext.attributes` → persisted in the audit record.

```bash
elt maintain run \
    --table iceberg.level3.sales.canonical_orders \
    --attribute ticket_ref:GDPR-2026-0824-001 \
    --attribute operator:jane.smith@example.com \
    --snapshot-retain-days 1 --orphan-older-than-days 1 \
    --expire-snapshots --remove-orphan-files
```

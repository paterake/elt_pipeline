# Local Operator Runbook

This runbook covers the local v1 operator flows for on-demand execution, targeted reruns, backfills, and deterministic schedule execution.

## Prerequisites

- Run commands from the repository root.
- Install dependencies with `uv sync --extra dev --extra spark`.
- `normalize`, `sql`, and `publish` run on Apache Spark and require a local JVM (Java 17+) with `JAVA_HOME` set. See [JVM_TOOLCHAIN_SETUP.md](../maintainer/JVM_TOOLCHAIN_SETUP.md) to install one.
- Use a writable runtime root such as `.ignore/runtime-demo`.
- Validate configs before running jobs.

```bash
uv run elt-pipeline validate-config examples/configs/local_object_storage_orders.yaml
```

## Standard Stage Runs

Run ingest:

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-demo
```

Run normalization against the same root:

```bash
uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-demo
```

Compile or run downstream SQL models:

```bash
uv run elt-pipeline sql compile examples/sql/local_demo \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

uv run elt-pipeline sql run examples/sql/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Validate, explain, or run downstream `level5` publish definitions:

```bash
uv run elt-pipeline publish validate examples/publish/local_demo

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --environment default \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --environment default \
  --publish daily_order_export \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --environment default \
  --publish daily_order_export_tsv \
  --window-label 2026-01

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --window-start 2026-01-01T00:00:00+00:00 \
  --window-end 2026-01-31T23:59:59+00:00 \
  --window-label jan-2026 \
  --backfill
```

## Backfill Runs

Use backfill mode when you want a historical ingest or normalization run to seed checkpoint state from prior history.

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-demo \
  --window-start 2026-01-01T00:00:00+00:00 \
  --window-end 2026-01-31T23:59:59+00:00 \
  --window-label jan-2026 \
  --backfill
```

Notes:

- `--backfill` requires `--window-start`.
- Use the same runtime root so prior checkpoints and artifacts remain discoverable.
- Pair the same window selection with `normalize run` when replaying historical landed data.

## Targeted Normalize Reruns

Normalize reruns reuse the exact input manifest captured by a prior normalize run.

1. Find the prior normalize `run_id` under `runs/stage=normalize/`.
2. Re-run with `--rerun-run-id`.

```bash
uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-demo \
  --rerun-run-id <prior-normalize-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--source`, `--entity`, `--manifest-path`, or window filters.
- The CLI resolves the original manifest path from the stored audit artifact.

## Normalize Engine Selection (Python Driver vs. Spark-Native Relationalization)

The L1 → L2 normalize stage ships a **dual-engine gate** behind the programmatic
`normalize_engine` parameter and the CLI `--normalize-engine` flag (values
`"python"` or `"spark"`; **default `"spark"`** since the Gate 5 cutover). Both
engines produce byte-identical L2 manifests, `mapping_version` hashes, table
physical names, column layouts, and on-disk `level2/` directory layout — they
differ only in where the per-row relationalization work executes.

| Engine | Relationalizer location | When to use |
|---|---|---|
| `normalize_engine = "spark"` (**default**) | Spark-native `StructType` metadata walk on the driver **produces a `NormalizationPlan`**; `posexplode_outer` + struct-flatten + `uuid()` FK plumbing executes on Spark executors. Driver holds only schema + plan metadata (KBs), not data rows. | **Production default.** Recommended for all payload sizes. Eliminates the driver-memory ceiling described in the Known Limitations section below. `mapping_version`, table-name policy, and column naming are produced from the same shared `_policy.py` code as the legacy engine so L3 path lookups remain stable. |
| `normalize_engine = "python"` (escape hatch) | Pure-Python driver walk over every dict/list/row value. Spark writes the finished list[dict] rows to parquet. | Reach for this **only** when triaging an edge payload the Spark planner has not yet reproduced. The Python engine is scheduled for removal (Gate C3) following an agreed soak window on Spark-default. |

**Programmatic access:** the dual-engine switch is exposed via the
`normalize_engine=` keyword of `normalize_level1_to_local_level2(...)` in
[normalize/pipeline.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py#L106-L117).
**CLI access:** `normalize run --normalize-engine spark|python` (default:
`spark`). Row-level parity (Contract C1) was signed off on Temurin 17.0.20 +
PySpark 4.1.2, unblocking the default flip.

**Parity guarantee (Contracts C1–C3, Gate 2 verified):**
- `mapping_version` 16-hex SHA-256 prefix is byte-identical for the same
  logical schema (verified on a 3-deep nested-array fixture; metadata-only
  test runs without a JVM).
- Table physical names follow the same 63-char cap + SHA-8 suffix collision
  guard.
- Level2TableManifest `data_path` / `manifest_path` relative-layout semantics
  are preserved byte-identical.

## SQL Overwrite Protocol (Mercell/Camelot Staging-Swap)

SQL model materialization for `load_mode: full_refresh` and
`load_mode: partition_overwrite` now follows the **Mercell/Camelot staging-swap
write protocol** to eliminate the Spark 4.x same-path overwrite DAG hazard.
This hazard occurs whenever a SQL model reads from the canonical table path and
writes back into it (a "self-querying rebuild" such as a canonical table that
unions prior rows with new rows): Spark's plain-parquet `SaveMode.Overwrite`
deletes input files before the DAG recomputes, producing spurious
`No such file or directory` errors mid-execution.

**Write sequence for overwrite modes (Track B, Gate 3):**
1. Compute staging path: `{staging_root}/stage={level}/table={table_name}/run_id={run_id}/`.
   Default staging root is `{warehouse_root}/_staging/`; per-model override via
   `SqlModelManifest.staging_root` (useful for teams with separate temp-vs-perm
   S3 buckets).
2. Write the materialized DataFrame into the staging path with **identical**
   `mode("overwrite").partitionBy(*cols).parquet(...)` semantics the direct
   write previously used.
3. Read staging output once for `validate_stage` → row count is captured from
   this read (**eliminates the second full parquet re-read** the old code did
   after the direct `writer.parquet(target_path)` call).
4. Scheme-dispatched `atomic_swap` moves the staging directory tree into the
   canonical `target_path`:
   - **POSIX (`file://`, local unschemed paths):** `rmtree(target)` then
     `rename(staging, target)` for `full_refresh`; partition subdirectory
     rename for `partition_overwrite` (merge semantics). `rename(2)` is atomic
     when source and dest are on the same filesystem.
   - **S3 (`s3://`):** `CopyObject` (staging key → target key) for every part
     file, then batch `DeleteObjects` on old target keys, then batch
     `DeleteObjects` on staging keys. Readers only ever see OLD-valid,
     EMPTY-transient, or NEW-valid state per partition key — CopyObject always
     succeeds before DeleteObject of the old copy.
5. Best-effort delete of staging leftovers on any failure.

**Load modes excluded from swap (OD-4):**
- `load_mode: append` — remains direct `mode("append").parquet(target_path)`.
  Appending new part files never conflicts with re-reading existing input
  files, so no staging required.
- L2 normalize writes — each normalize run writes a fresh `run_id=` directory
  that no in-flight action reads back, so same-path hazard is structurally
  impossible. Writes stay direct `mode("error")`.

**Scheme guard, consistent with PRD 08's dispatch pattern:**
Staging-swap is fail-fast restricted to `{file, local_unschemed, s3}`. Any
other storage scheme raises `staging_scheme_unsupported` with an operator hint
pointing at PRD 08's supported scheme set.

**Error codes introduced by the swap protocol:**
- `staging_write_failed` — staging parquet could not be read back after the
  write call returned.
- `atomic_swap_failed` — the scheme-dispatched swap step raised mid-operation.
  The error context preserves `staging_path` and `target_path` so operators
  can inspect for partial state; staging contents survive on a best-effort
  basis when the error occurs mid-copy.
- `staging_scheme_unsupported` — the overwrite load mode was used against a
  scheme the swap layer has no known-atomic semantics for.

**Partition-overwrite merge semantics preserved (Contract C4):**
`partition_overwrite` continues to mean **dynamic partition overwrite** (only
partitions present in the incoming DataFrame are replaced; other partitions
under `target_path` survive). The swap layer implements this by operating on
partition-subdirectory granularity, not whole-table granularity, for the S3
and POSIX branches. The reference `canonical_orders` late-arrival replay
procedure below continues to work unchanged — it merely writes to staging
first and then swaps.

## Targeted SQL Reruns

SQL reruns restore the prior model and window selection from SQL audit artifacts.

1. Find the prior SQL `run_id` under `runs/stage=sql/`.
2. Re-run with the same package path, root path, and warehouse root.

```bash
uv run elt-pipeline sql run examples/sql/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --rerun-run-id <prior-sql-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--stage`, `--domain`, `--model`, `--include-deps`, date filters, partitions, or vars.
- Use `--validate-only` or `--explain` first if you want to confirm the plan before writing tables.

## Targeted Publish Reruns

Publish reruns restore the prior publish selection and execution window from publish audit artifacts.

1. Find the prior publish `run_id` under `runs/stage=publish/`.
2. Re-run with the same runtime root, publish package, and warehouse root.

```bash
uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-demo \
  --warehouse-root .ignore/warehouse \
  --rerun-run-id <prior-publish-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--domain`, `--publish`, window filters, `--backfill`, or a non-default `--environment`.
- The CLI restores the original publish IDs, environment, and window selection from the stored publish audit record.
- The rerun manifest and audit artifacts record `rerun_of_run_id` so operators can trace replayed deliveries.

## Schedule-Driven Execution

Schedule plans call existing CLI commands in a fixed order.

```bash
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml --continue-on-error
```

Operator guidance:

- Keep paths in the schedule plan aligned with the config, SQL package, and runtime root you intend to use.
- Use the plan-level `continue_on_error` setting for deterministic defaults.
- Use the CLI flag `--continue-on-error` only when you intentionally want to override the plan.

## Optional Lineage Backend Operations

Optional remote lineage emission is available without changing the CLI contract for ingest, normalize, SQL, publish, or schedule-driven runs.

```bash
export ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http
export ELT_PIPELINE_LINEAGE_URL=http://localhost:5000/api/v1/lineage
export ELT_PIPELINE_LINEAGE_POLICY=best_effort
export ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS=10
```

Optional authentication:

```bash
export ELT_PIPELINE_LINEAGE_AUTH_HEADER="Bearer <token>"
```

Operator guidance:

- Keep local `runs/.../lineage.jsonl` artifacts as the system of record even when remote emission is enabled.
- Use `best_effort` for normal local-first operation; remote failures are then captured locally in `logs.jsonl` and `errors.jsonl` without changing stage success.
- Use `blocking` only when an environment explicitly requires remote lineage submission to succeed.
- Set `ELT_PIPELINE_LINEAGE_URL` to the full OpenLineage-compatible HTTP endpoint, such as Marquez at `http://localhost:5000/api/v1/lineage`.
- Validate connectivity with a small CLI run first; if the backend is unavailable, the stage audit still lets operators confirm whether the core stage itself succeeded.
- All `ELT_PIPELINE_LINEAGE_*` values are trimmed before validation.
- `ELT_PIPELINE_LINEAGE_BACKEND` and `ELT_PIPELINE_LINEAGE_POLICY` are accepted case-insensitively, but lowercase values are still the recommended operational form.

## Optional Airflow Wrapper Operations

The repository now includes one reference Airflow wrapper that preserves the authoritative CLI contract.

- Reference helper: `elt_pipeline.integrations.AirflowCliWrapper`
- Reference example DAG: `examples/orchestration/airflow/reference_dag.py`
- Execution model: Airflow calls `python -m elt_pipeline ...` through the wrapper; it does not replace the platform CLI with an Airflow-native runtime API.

Operator guidance:

- Keep local `runs/.../audit.json`, `logs.jsonl`, and `lineage.jsonl` as the system of record even when runs are launched from Airflow.
- Pass the same `--root-path`, package paths, selection flags, and window arguments you would use in a direct CLI run; the wrapper is intentionally thin.
- Expect supplemental audit attributes such as `orchestration_platform`, `orchestration_flow_name`, `orchestration_flow_run_id`, and `orchestration_task_name` when the wrapper injects Airflow context.
- Disable the integration by running the CLI directly or by removing the wrapper from the Airflow DAG; no platform config change is required.
- If an Airflow task fails, inspect the local run artifacts first to determine whether the core stage failed or whether the failure happened at the wrapper/orchestrator layer.

## Optional Data-Quality Operations

Optional quality hooks are available for post-normalize and post-SQL output checks without changing the core CLI contract.

```bash
export ELT_PIPELINE_QUALITY_BACKEND=row_count_threshold
export ELT_PIPELINE_QUALITY_ROW_COUNT_MIN=1
export ELT_PIPELINE_QUALITY_POLICY=best_effort
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql
```

Operator guidance:

- The current reference backend is `row_count_threshold`; it evaluates each emitted normalization or SQL dataset against the configured minimum row count.
- Leave `ELT_PIPELINE_QUALITY_BACKEND` unset to disable the integration cleanly.
- Use `best_effort` when quality failures should be recorded as supplemental evidence without failing the stage.
- Use `blocking` when any failed quality result should stop the stage and write `QUALITY_CHECK_FAILED` into local error artifacts.
- Expect backend execution problems to be recorded as `QUALITY_BACKEND_EXECUTION_FAILED`; this distinguishes optional integration failure from a core normalize or SQL runtime failure.
- Review `runs/.../audit.json` for `validation_results`, `runs/.../logs.jsonl` for `quality_hook_complete` or `quality_hook_failed`, and stage metrics for `quality.pass`, `quality.warn`, `quality.fail`, and `quality.skipped`.
- Restrict `ELT_PIPELINE_QUALITY_STAGES` to `normalize`, `sql`, or both; publish-stage quality remains out of scope in the current approved contract.
- All `ELT_PIPELINE_QUALITY_*` values are trimmed before validation.
- `ELT_PIPELINE_QUALITY_BACKEND`, `ELT_PIPELINE_QUALITY_POLICY`, and `ELT_PIPELINE_QUALITY_STAGES` are accepted case-insensitively, but lowercase values are still the recommended operational form.

## Publish Operator Guidance

Use `publish validate` when reviewing new or changed publish packages before touching runtime outputs.

- It confirms discovery, manifest schema, directory naming, and query-file presence.
- It is safe to run in CI or before a release because it does not read the warehouse or write artifacts.

Use `publish explain` when you want a dry preview of the paths a publish run would target.

- Pass the same `--root-path`, selection filters, and optional `--window-label` that you expect to use in `publish run`.
- Review `run_scoped_path` for the immutable history location and `stable_delivery_path` when the publish definition uses `overwrite_in_place` or `append_new_artifact`.
- Review `archive_run_scoped_path` and `archive_stable_delivery_path` when a publish definition declares `delivery.packaging.archive_format: zip`.
- Expect `stable_delivery_path` to be `null` in explain output when the publish definition uses `versioned_delivery`; only `overwrite_in_place` and `append_new_artifact` maintain a consumer-facing stable path.

Use `publish run` only after the upstream `level4` parquet table already exists under the target warehouse root (produced by `sql run`).

- The current implementation supports CSV, `jsonl`, and `tsv` outputs.
- The current implementation supports `versioned_delivery`, `overwrite_in_place`, and `append_new_artifact`.
- A publish definition may also request a zip archive bundle in addition to the primary file output.
- `append_new_artifact` writes the immutable run-scoped artifact and also copies a uniquely named delivery file into the consumer-facing artifact path without mutating prior deliveries.
- The bundled `daily_order_export_tsv` example demonstrates `tsv` plus `append_new_artifact`; the stable delivery filename includes the originating `run_id`.
- The bundled `daily_order_export_bundle` example demonstrates a CSV delivery with an additional stable `.zip` bundle produced from the same run-scoped output.
- Use `--backfill` with an explicit publish window when replaying a historical delivery slice; it records the run as a backfill in stage audit artifacts.
- A successful run writes the exported file and `manifest.json` under `artifacts/level5/`, and writes stage audit/log/lineage records under `runs/stage=publish/`.
- Reuse the same runtime root for repeatable operator workflows so historical run artifacts remain available for inspection.

## Environment and Storage Root Convention

The pipeline uses a **two-root, per-environment** storage layout. Environment is NEVER embedded in filesystem paths — isolation is achieved entirely by pointing each environment at its own pair of roots. This matches standard cloud lakehouse patterns (Databricks per-env storage accounts, EMR per-env buckets, Glue per-env catalog IDs) and enables clean env-to-env promotion, point-in-time restore, and IAM prefix boundaries.

| Root | Purpose | Contents | Managed by flag |
|---|---|---|---|
| `--root-path` | Raw / source-aligned storage | `level1/` landed payloads, `level2/` normalized parquet, `runs/` audit/logs, `state/` checkpoints, `artifacts/` level5 exports | All stages (`ingest`, `normalize`, `sql`, `publish`) |
| `--warehouse-root` | Curated / canonical warehouse | `level3/` canonical tables, `level4/` mart tables | `sql run`, `publish run`, `sql plan/explain` |

**Setup convention — one pair per environment:**

```bash
# Local / development
DEV_ROOT=.ignore/runtime-dev
DEV_WAREHOUSE=.ignore/warehouse-dev

# Staging
STAGING_ROOT=/data/elt/staging-runtime
STAGING_WAREHOUSE=/data/elt/staging-warehouse

# Production
PROD_ROOT=/data/elt/prod-runtime
PROD_WAREHOUSE=/data/elt/prod-warehouse
```

**Always pass both roots together** for the same environment. Never mix a dev root with a prod warehouse:

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path $DEV_ROOT

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path $DEV_ROOT

uv run elt-pipeline sql run examples/sql/local_demo \
  --root-path $DEV_ROOT \
  --warehouse-root $DEV_WAREHOUSE \
  --environment dev \
  --start-date 2026-01-01 --end-date 2026-01-31
```

Operator guidance:

- Keep `--environment` values consistent across runs for the same logical env (dev/staging/prod). Environment is still recorded in audit/logs/manifests for traceability even though it is not in paths.
- Two environments sharing one `--root-path` or `--warehouse-root` will collide — every env MUST have its own pair.
- In cloud deployments, map each pair to its own bucket / storage account with env-scoped IAM policies.

## Late-Arriving Data Recovery

Canonical `level3` tables follow the **Camelot late-arrival repartitioning pattern**: L2 rows carry both `ingest_date` (arrival day, for filtering the read window) and `business_date` (event day from the payload, for output partitioning). This means a row that arrived late (e.g. received on 2026-08-10 but describing an event that happened on 2026-07-31) is correctly written into the `business_date=2026-07-31` partition when the L3 model reads the `ingest_date=2026-08-10` window.

The `level3` default partition convention (`partitionBy(source_name, business_date)`) combined with Spark's `partitionOverwriteMode=dynamic` ensures that re-running a window **only overwrites the exact `(source_name, business_date)` tuples present in the incoming batch** — all other dates and sources are left untouched. The replay is safe and idempotent.

**Standard recovery procedure — replay a late-arrival window:**

1. **Identify which ingest_date window contains the late-arriving rows.** For example: late data for `business_date=2026-07-31` landed in `ingest_date=2026-08-10`. (You can audit L2 manifests under `level2/source=*/entity=*/ingest_date=2026-08-10/` or the run audit record to confirm which run it was in.)

2. **Re-run normalize for the affected ingest_date window** to make sure the L2 parquet is fresh:

   ```bash
   uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
     --root-path $DEV_ROOT \
     --window-start 2026-08-10T00:00:00+00:00 \
     --window-end 2026-08-10T23:59:59+00:00
   ```

   (Or use `--rerun-run-id <normalize-run-id>` if you have the exact run.)

3. **Re-run the L3 SQL models with the same ingest_date window:**

   ```bash
   uv run elt-pipeline sql run examples/sql/local_demo \
     --root-path $DEV_ROOT \
     --warehouse-root $DEV_WAREHOUSE \
     --environment dev \
     --stage level3 \
     --start-date 2026-08-10 \
     --end-date 2026-08-10
   ```

4. **Verify** by checking the destination `business_date` partition under the warehouse:

   ```bash
   ls -la $DEV_WAREHOUSE/level3/canonical_orders/source_name=local_files/business_date=2026-07-31/
   ```

   The `.parquet` files in this directory should be freshly written. Because `partitionOverwriteMode=dynamic` is enabled at the session level, only this specific partition was replaced; `business_date=2026-06-01` or any unrelated date is untouched.

5. **Re-run downstream L4 models** if they consume the updated canonical `level3` table:

   ```bash
   uv run elt-pipeline sql run examples/sql/local_demo \
     --root-path $DEV_ROOT \
     --warehouse-root $DEV_WAREHOUSE \
     --environment dev \
     --stage level4 \
     --include-deps \
     --start-date 2026-07-31 \
     --end-date 2026-07-31
   ```

Operator guidance:

- A replayed window is idempotent — running it twice with identical inputs produces the same output row counts and leaves the same files. No deduplication step is needed.
- When replaying **multiple consecutive ingest_dates**, batch the `--start-date` / `--end-date` range. Spark will overwrite each `(source_name, business_date)` tuple that appears in any of the replayed L2 rows.
- If you are unsure which `ingest_date` window carried the late rows, use Spark shell or a quick L2 query to find them: L2 carries `ingest_date` as both a Spark-discovered partition column and an in-data column, so `SELECT DISTINCT ingest_date FROM level2_parquet WHERE business_date = '2026-07-31'` gives you the exact windows to replay.
- The reference example `examples/sql/local_demo/level3/sales/canonical_orders/` implements this pattern end-to-end; use it as a template for new L3 models.

## Cloud Native (No-Mounts) EMR / S3 Execution Pattern

This section applies when running the pipeline on **AWS EMR** (or any cloud
Spark runtime) with S3 as storage, using the platform's native URI dispatch
**without FUSE, Mountpoint for S3, or any file-system mount layer.**

The storage-root contract is sharp and zero-inference — per
[PRD 08](../prd/08-prd-storage-root-uri-io-dispatch.md):

- Every root is a **string URI**. The scheme prefix on the string is the single
  routing key for dispatch.
- `s3://` URIs are handed **verbatim** to Spark parquet reads and writes.
  No `pathlib.Path` wrapping, no URI mangling, no prefix reconstruction, no
  POSIX assumptions applied in Python code.
- Config (YAML + CLI args) is the **sole** dictum of prefix + root.

### IAM Roles and Bucket Setup

Use environment-scoped buckets. Do not put dev/staging/prod on one bucket with
prefixes — use peer buckets. Example:

```text
s3://corp-elt-dev-runtime-us-east-1/       # --root-path for dev (L1/L2 raw)
s3://corp-elt-dev-warehouse-us-east-1/     # --warehouse-root for dev (L3/L4 curated)

s3://corp-elt-staging-runtime-us-east-1/   # --root-path for staging
s3://corp-elt-staging-warehouse-us-east-1/ # --warehouse-root for staging

s3://corp-elt-prod-runtime-us-east-1/      # --root-path for prod
s3://corp-elt-prod-warehouse-us-east-1/    # --warehouse-root for prod
```

Attach the following as an **instance profile** on the EMR primary/core nodes
(or as an execution-role ARN on EMR Serverless). Scope `Resource` to the exact
env/buckets above. **Never use long-lived credentials inside the Spark job.**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::corp-elt-dev-runtime-us-east-1",
        "arn:aws:s3:::corp-elt-dev-runtime-us-east-1/*",
        "arn:aws:s3:::corp-elt-dev-warehouse-us-east-1",
        "arn:aws:s3:::corp-elt-dev-warehouse-us-east-1/*"
      ]
    }
  ]
}
```

Each source bucket that the object-storage or kafka connectors read from gets a
similar, separate IAM grant. Keep connector `bucket_path` source buckets and
platform `--root-path / --warehouse-root` buckets on separate IAM scopes so
audit trail distinguishes reads from writes.

### 4-Stage EMR Step Pattern (Validate → Ingest → Normalize → SQL → Publish)

Run each stage as its **own EMR Step** / EMR Serverless job run. Keeping stages
separate gives independent retry, per-stage Spark config, and clear failure
boundaries.

```bash
# Stage 0 — validate config + package shapes (single-process, negligible CPU)
spark-submit --deploy-mode client \
  --packages org.apache.hadoop:hadoop-aws:3.3.6 \
  -m elt_pipeline validate-config s3://corp-elt-dev-runtime-us-east-1/configs/local_object_storage_orders.yaml \
  --environment dev --source local_files --entity orders

# Stage 1 — ingest (connector reads → L1 raw files on s3 runtime bucket)
spark-submit --deploy-mode cluster \
  --packages org.apache.hadoop:hadoop-aws:3.3.6 \
  -m elt_pipeline ingest run \
    s3://corp-elt-dev-runtime-us-east-1/configs/local_object_storage_orders.yaml \
    --environment dev \
    --root-path s3://corp-elt-dev-runtime-us-east-1/

# Stage 2 — normalize (L1 raw → L2 parquet tables on s3 runtime bucket)
spark-submit --deploy-mode cluster \
  --packages org.apache.hadoop:hadoop-aws:3.3.6 \
  -m elt_pipeline normalize run \
    s3://corp-elt-dev-runtime-us-east-1/configs/local_object_storage_orders.yaml \
    --environment dev \
    --root-path s3://corp-elt-dev-runtime-us-east-1/ \
    --window-start 2026-08-13T00:00:00+00:00 \
    --window-end   2026-08-13T23:59:59+00:00

# Stage 3 — sql (L2 reads → L3 canonical tables → L4 marts on s3 warehouse bucket)
spark-submit --deploy-mode cluster \
  --conf spark.sql.sources.partitionOverwriteMode=DYNAMIC \
  --packages org.apache.hadoop:hadoop-aws:3.3.6 \
  -m elt_pipeline sql run \
    s3://corp-elt-dev-runtime-us-east-1/sql_packages/local_demo \
    --environment dev \
    --root-path     s3://corp-elt-dev-runtime-us-east-1/ \
    --warehouse-root s3://corp-elt-dev-warehouse-us-east-1/ \
    --start-date 2026-08-13 --end-date 2026-08-13

# Stage 4 — publish (L4 reads → level5 CSV/TSV/ZIP delivery artifacts)
spark-submit --deploy-mode cluster \
  --packages org.apache.hadoop:hadoop-aws:3.3.6 \
  -m elt_pipeline publish run \
    s3://corp-elt-dev-runtime-us-east-1/publish_packages/local_demo \
    --environment dev \
    --root-path     s3://corp-elt-dev-runtime-us-east-1/ \
    --warehouse-root s3://corp-elt-dev-warehouse-us-east-1/ \
    --start-date 2026-08-13 --end-date 2026-08-13
```

Note: `configs/`, `sql_packages/`, `publish_packages/` are **package-file
paths** (local YAML/SQL inputs to the platform), not storage roots. They live
on the EMR primary node's local FS (uploaded as part of the bootstrap step or
a step dependency zip) and intentionally remain `pathlib.Path`-typed within
the platform code. Storage roots (the `s3://…` strings above) are always
string-typed and flow verbatim through the dispatch layer to Spark.

### Scheme Prefix Troubleshooting

The guard `validate_config_root_schemes(...)` in
`src/elt_pipeline/config/loader.py` runs a fail-fast check on every storage
root and bucket path before I/O or Spark starts. If you see an
`Unsupported storage scheme` / `ConfigValidationError`:

| Error | Cause | Fix |
|---|---|---|
| `unsupported-scheme-prefix:s3a` | Config or CLI arg used legacy `s3a://` prefix. | Switch to `s3://` — EMR + `hadoop-aws` bundle handle `s3://` natively with EMRFS. |
| `unsupported-scheme-prefix:hdfs` or `gs`, `abfss`, `wasb(s)` | Platform explicitly does not support these schemes today. | Separate PRD required if you need to add new scheme members to `_StorageScheme` in `src/elt_pipeline/shared/path_utils.py`. |
| `s3-bucket-empty` | Root was literally `s3://` or `s3:///` with no bucket name. | Use full `s3://<bucket-name>/<optional-prefix>/` — bucket + prefix required. |
| `unsupported-file-scheme-single-slash:file:/...` | Triple-slash `file:/…` / `file:///…` was used inconsistently. | Write `file://` + absolute path; or drop the scheme entirely and pass a plain POSIX path (preferred for local dev). |
| `root-not-string` | Someone called the runner with a `pathlib.Path` object instead of `str`. | Fix the call site; storage-root call signatures are `str`, not `Path \| str`. |

### Cloud Operator Guidance

- **Do not introduce FUSE / Mountpoint for S3 at any point.** The code contract
  explicitly avoids mounts. Adding a mount layer after the platform already
  handles `s3://` natively is double-indirection and breaks the Mercell/Camelot
  sharp-root convention.
- `spark.sql.sources.partitionOverwriteMode=DYNAMIC` on the Stage 3 SQL submit
  is **required** for the Mercell re-co-location + Camelot late-arrival
  pattern. Without it, `partition_overwrite` load modes behave like
  full-refresh and destroy sibling `(source_name, business_date)` partitions.
- **Late-arrival replays on EMR** are identical in form to the local recovery
  procedure above. Replay stages 1–3 pointing at the same env roots but with a
  historical or expanded `--window-start/--end-date`; the dynamic partition
  overwrite will surgically fix only the touched partitions.
- For EMR Serverless, add the EMR Serverless `s3` access policy to the
  application's runtime role, and pass the same bucket set above. Nothing
  changes in the command lines — the string roots are unchanged.

## Known Limitations

These are current, intentional constraints of the local-first Spark implementation. They are
not bugs; know them before running at larger scale.

- **`level1` -> `level2` relationalization defaults to driver-Python with a native-Spark
  alternative behind a dual-engine gate.** The legacy path flattens each nested-structure
  payload in the driver's Python process (`normalize/runner.py`); a parallel
  `normalize_engine = "spark"` path moves relationalization into Spark executors via
  `StructType` metadata walk → `posexplode_outer`/struct-flatten plan. Both paths emit
  byte-identical manifests and `mapping_version` hashes (Contracts C1–C3). The legacy
  `"python"` engine is the CLI default during the Gate 5 transition window; a single
  very large or deeply nested source payload held under the default engine can exhaust
  driver memory — keep per-artifact payload sizes bounded at ingest, or switch to the
  `"spark"` engine for those sources. See **Normalize Engine Selection** above for the
  full parity table and programmatic access.
- **`publish run` collects results to the driver.** The publish SQL result set is materialized
  in driver memory via `.collect()` so each delivery can be written as a single local file.
  This caps output size to driver memory. It matches the prior sqlite `fetchall()` behaviour
  (not a regression) but is a real ceiling for very large `level4` result sets.
- **`level2` source/entity filtering is structural via the entity_root path.** `source` and
  `entity` are baked into the `level2/source=S/entity=E` parent directory the reader points at,
  so narrowing to a source/entity pair happens at the filesystem level. `mapping_version`,
  `ingest_date`, `table`, and `run_id` path segments below that are recovered by Spark as
  genuine discovered partition columns and can appear in `WHERE` predicates for automatic
  partition pruning. Additionally, every `level2` row carries `source_name`, `ingest_date`,
  and `_run_id` as real in-data columns, so `level3` SQL models can also filter or project them
  without relying on path metadata. See `docs/todo/archive/TODO_PATHING_COMPLETED.md` for the full pathing and
  partition contract design record.
- **`--warehouse-root` isolation is per-environment by convention.** There is no `environment=`
  segment in `level3`/`level4` paths, so each environment (dev/staging/prod) MUST point at
  its own warehouse root pair. See the **Environment and Storage Root Convention** section
  above for the standard setup pattern. Sharing one `--warehouse-root` between environments
  will collide tables.
- **SQL overwrite staging-swap scheme set is fail-fast restricted.** `full_refresh` and
  `partition_overwrite` load modes require the warehouse root scheme to be in
  `{file, local_unschemed, s3}`. Models using overwrite modes against other schemes (or
  warehouse roots pointed at non-URI local paths whose inferred scheme falls outside this
  set) raise `staging_scheme_unsupported` before writing anything. `append` mode has no
  scheme restriction because it bypasses the swap layer. See **SQL Overwrite Protocol**
  above for the full write sequence and operator guidance.

## Audit and State Locations

A local runtime root persists these operator-visible directories:

- `level1/`: landed payloads and manifest metadata
- `level2/`: Spark-written parquet datasets and mapping catalogs. The `source=`/`entity=` prefix narrows the reader structurally; below that, `mapping_version=`/`ingest_date=`/`table=`/`run_id=` are Spark-discovered partition columns, and every row also carries `source_name`/`ingest_date`/`_run_id` as in-data columns.
- `artifacts/level5/`: publish/export delivery artifacts and run-scoped manifests
- `runs/`: stage-scoped audit, logs, lineage, and rerun metadata
- `state/`: checkpoint history used for incremental runs and backfills

A separate warehouse root (passed via `--warehouse-root` to `sql run` and `publish run`) persists Spark-written `level3/` and `level4/` parquet tables, one directory per table.

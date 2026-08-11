# Path & Partition Management Backlog

Active backlog for how `elt_pipeline` maps physical storage location to logical table
identity and partitioning across `level1`–`level5`. This document is a self-contained
handoff: a fresh session should be able to continue the work from here without re-deriving
the analysis.

## Status

- Backlog status: active (decisions finalized, PRD update pending, implementation not started)
- Driver: architectural review after the Spark engine migration (see
  `docs/todo/archive/TODO_SPARK_COMPLETED.md`) + research validation against Spark medallion
  best practice
- Gate: the changes below alter the `level2` data contract and the `level3`/`level4` path
  grammar, so per repo governance they require a PRD update
  (`docs/prd/02-*`, `docs/prd/03-*`, `docs/prd/00-prd-architecture-levels-and-governance.md`)
  **before** implementation.
- Decision protocol: open questions resolved per research-based recommendation (see "Resolved
  Decisions" below). Ambiguities surfaced to user; user defaulted to Spark standard patterns.

## Why This Exists

The Spark migration replaced sqlite/JSONL with Spark parquet across `level2`–`level5`. In
doing so it made several **implicit** decisions about physical pathing, table identity, and
partitioning that were never explicitly reviewed — they fell out of the storage-layout code.
A design review (captured below) found that the refactor did **not** preserve the legacy
platforms' single uniform path grammar, and that `source_name`/`ingest_date` are dropped from
both the path (as queryable keys) and the data columns downstream, which blocks a
medallion-correct canonical layer and weakens path-based governance.

The user has explicitly accepted one consequence as fine (see "Accepted As-Is") and wants the
rest revisited.

## Current State (verified against code — pre-change baseline)

Three different path grammars across two roots, with `environment=` inconsistently in-path:

| Level | Root | Grammar (CURRENT, to be changed) |
|---|---|---|
| L1 | `--root-path` | `level1/environment=<env>/source=<src>/entity=<entity>/ingest_date=<date>[/window=<label>]/run_id=<id>/<file>` |
| L2 | `--root-path` | `level2/environment=<env>/source=<src>/entity=<entity>/mapping_version=<v>/ingest_date=<date>/table=<physical>/run_id=<id>/*.parquet` |
| L3–L4 | `--warehouse-root` | `level{3,4}/<table_name>/[<partcol>=<val>/...]` (flat, no source/entity/date) |

## Target State (agreed grammar — changes everything below)

Two roots retained (raw vs curated have different lifecycle / RBAC). **`environment=` dropped from
ALL paths** — environment is handled exclusively by which root/bucket is pointed at. Grammar
unified WITHIN each level type:

| Level | Root | Grammar (TARGET) |
|---|---|---|
| L1 | `--root-path` | `level1/source=<src>/entity=<entity>/ingest_date=<date>[/window=<label>]/run_id=<id>/<file>` |
| L2 | `--root-path` | `level2/source=<src>/entity=<entity>/mapping_version=<v>/ingest_date=<date>/table=<tbl>/run_id=<id>/*.parquet` |
| L3 | `--warehouse-root` | `level3/<table_name>/source_name=<src>/<date_col>=<date>/*.parquet` — date_col = `business_date` (default, canonical) or `ingest_date` (snapshot/audit) |
| L4 | `--warehouse-root` | `level4/<table_name>/<date_col>=<date>/*.parquet` — date_col per model; omit only for non-temporal dimensions |

**CRITICAL DISTINCTION — `ingest_date` vs `business_date` date partitions:**

At L1 and L2 (source-aligned levels), the path date key is **always `ingest_date`** (= when the
data was received). This is immutable once written and serves as the unit of replay.

At L3 and L4 (canonical / mart levels), the path date key is **chosen per model**:
- **Default: `business_date`** (= when the event actually happened, from the payload). This is
  the Camelot late-arrival pattern: data received on `ingest_date=2026-08-10` whose business date
  is `2026-07-31` correctly lands in partition `business_date=2026-07-31/`. Late arrivals are
  handled by re-running the L3 model with the new L2 input — Spark's dynamic partition overwrite
  (already enabled in `spark.sql.sources.partitionOverwriteMode=dynamic` in
  [session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L17))
  replaces only the `(source_name, business_date)` tuple present in the incoming dataframe.
- **Opt-in: `ingest_date`** for snapshot/audit tables where "what did the data look like on the
  day we received it?" is the question. Model manifest sets `target.partition_columns` explicitly
  to override the default.

Additional invariants:
- Partition keys in path **MUST** also exist as real parquet data columns (Spark requirement for
  metastore registration, filtering, and joins). The date column (`business_date` or
  `ingest_date`) must be produced by the SQL model's SELECT; P1's L2 lineage columns give the
  model access to `ingest_date` always.
- `partitionBy(*columns)` applies to **all three load modes** (`full_refresh`, `append`,
  `partition_overwrite`). Load mode controls write semantics (overwrite whole table / append /
  overwrite only matching partitions); `partitionBy` controls physical layout. These are
  orthogonal — current code incorrectly couples them.
- L2 reader reads parent directories (not explicit leaf `run_id=*` paths) so Spark auto-discovers
  `source`, `entity`, `mapping_version`, `ingest_date` as queryable partition columns. Filter to
  specific runs via `.where("run_id IN (...)")` after read; Spark prunes partitions.
- L3 pattern = Mercell re-co-location: one canonical table, multiple `source_name=<src>`
  partitions side-by-side. Each source's L3 pipeline independently `partition_overwrite`s its
  own `(source_name, business_date)` tuple (per the default).

## Late-Arriving Data Repartitioning (explicit Camelot capability preserved)

This capability is not only preserved — it works better under the agreed architecture because the
`ingest_date` / `business_date` split is by design, not bolt-on.

**Standard flow:**
1. **L2 write (arrival day).** Data for a `business_date=2026-07-31` event arrives late on
   `ingest_date=2026-08-10`. Normalize writes it to L2 under
   `level2/source=X/entity=Y/.../ingest_date=2026-08-10/...`. The L2 row carries both
   `ingest_date=2026-08-10` and `source_name=X` (from P1 lineage columns) PLUS the payload
   column `business_date=2026-07-31` from the flattened JSON.
2. **L3 SQL model reads by `ingest_date`, rewrites by `business_date`.** The L3 model's SELECT
   does:
   ```sql
   cte_src_base AS (
     SELECT *, business_date  -- from payload; P1 also gives us ingest_date, source_name
     FROM t.level2_X_Y
     WHERE ingest_date = '{{ window.start_date }}'  -- reads everything that arrived today
   ),
   cte_joined AS (...)
   SELECT * FROM cte_joined  -- business_date is in the output columns
   ```
3. **L3 write to the correct date partition.** The L3 writer does
   `.partitionBy("source_name", "business_date").mode("overwrite").parquet(...)` with dynamic
   partition overwrite. Spark writes the output to
   `level3/canonical_table/source_name=X/business_date=2026-07-31/`, replacing only that one
   partition. Other partitions (other dates, other sources) are untouched.
4. **Idempotent replay.** Re-running the same L3 model for `ingest_date=2026-08-10` produces the
   same output and overwrites the same `(source_name, business_date)` partition — safe and
   deterministic.

**Why this is better than the Camelot implementation:**
- Camelot required a separate explicit "repartition" step / job. Here, it's the default behavior
  of any L3 model that selects `business_date` in its output. No extra tooling.
- `ingest_date` is preserved as a queryable column at L3, so you can still audit: "which
  ingest_date run wrote these rows into business_date=2026-07-31?"
- `partition_overwrite` + dynamic mode = zero risk of accidentally overwriting unrelated source
  or date partitions, even if the WHERE clause in the SELECT is wrong (Spark restricts overwrite
  to partition values present in the output dataframe).

Key code references:

- L1 layout: `src/elt_pipeline/ingest/storage.py` (`LocalArtifactLayout.level1_run_dir`)
- L2 layout + writer: `src/elt_pipeline/normalize/level2_storage.py`
  (`LocalLevel2Layout.table_run_dir`, `SparkLevel2Writer.write_table`)
- L2 read for SQL: `src/elt_pipeline/sql/level2_source.py` (`Level2DatasetLocator.read`)
- L3/L4 write: `src/elt_pipeline/sql/spark_executor.py` (`_table_path`, `_execute_model`)
- Partition strategy (L2): `src/elt_pipeline/normalize/partitioning.py` (`PartitionStrategy`)
- Normalize row columns: `src/elt_pipeline/normalize/runner.py`
- SQL token namespace: `src/elt_pipeline/sql/compiler.py` (`build_token_context`)

Verified facts that matter:

1. **L2 is not `partitionBy`.** `SparkLevel2Writer.write_table` does
   `dataframe.write.mode("error").parquet(data_dir)` — plain `.parquet()`. The
   `source=`/`entity=`/`mapping_version=`/`ingest_date=` fragments are baked into the path
   string only.
2. **L2 partition-ish segments are not recovered as columns.** `Level2DatasetLocator` globs
   leaf `run_id=*` dirs and passes explicit leaf paths to
   `spark.read.option("mergeSchema","true").parquet(*paths)`. Explicit leaf paths suppress
   Spark partition discovery on parent `key=value` segments, so `source`, `entity`,
   `ingest_date`, `mapping_version` never arrive as queryable columns.
3. **Normalize does not carry `source_name`/`ingest_date` as data columns.** The runner writes
   only `_row_id`, `_parent_row_id`, `_array_index`, `value`, and the flattened source fields.
   So L2 parquet has no `source_name` column for downstream SQL to select or partition on.
4. **No `source` token exists.** `build_token_context` exposes `environment`, `run_id`,
   `model.*`, `window.*`, `partition.*` — there is no `source.*`, so a model author cannot even
   synthesize `SELECT '{{ source.name }}' AS source_name`.
5. **L3/L4 partitioning is opt-in and narrow.** `_execute_model` applies
   `partitionBy(*target.partition_columns)` **only** for `load_mode: partition_overwrite`;
   `full_refresh` and `append` write with no partitioning at all. L3/L4 paths contain no
   `environment`/`source`/`entity`/`date` segments — just `level{3,4}/<table_name>/`.

## Legacy Reference Model (Camelot / Mercell)

Both legacy platforms used **one uniform path grammar at every level**:

```
s3://<bucket>/level_<n>/env=<env>/entity_name=<entity>/source_name=<src>/<insert_date>=<yyyymmdd>/
```

- `entity_name` = the table; `source_name` + `insert_date` were **genuine Glue partition
  columns** registered via `CREATE EXTERNAL TABLE ... PARTITIONED BY (...)`.
- Uniformity bought: same partition columns everywhere, uniform `WHERE source_name=... AND
  insert_date=...` filtering, uniform partition-overwrite/replay semantics, and clean
  **governance-by-path** (lock a level, or a source within a level, by prefix).
- Jobs were **one-source-one-date `spark-submit` invocations**, which is *why* `source_name`
  was a natural universal partition — the job always knew its source.
- **Camelot**: many sources co-located under one `entity_name` table as `source_name`
  partitions, reconciled by Glue schema evolution + `mergeSchema` (sources analysed together).
- **Mercell**: sources kept physically separate at L2 (source-prefixed / hashed table names),
  then **re-co-located and conformed at L3** into a shared canonical table (e.g. `dcp_notice`)
  **partitioned by `source_name`** — i.e. "conform each source, then bring them back together
  as source-partitions of one canonical table."

## What The Refactor Decided (implicitly)

1. **Adopted the Mercell source-separated model universally at L1/L2** (`source=` above
   `entity=` in the path), and did not implement Camelot's shared-table-with-source-partitions
   model.
2. **Flipped source/entity ordering vs legacy** — refactor is `source/entity`, legacy was
   `entity/source`.
3. **Abandoned the uniform grammar at L3/L4** — flat `level{3,4}/<table_name>/`, dropping
   `environment`/`source`/`entity`/`date` from the path entirely.
4. **Split roots** — L1/L2 under `--root-path`, L3/L4 under `--warehouse-root`.
5. **Handles schema divergence by segregation + read-time merge** (`mapping_version` subtree +
   `mergeSchema=true`) rather than a single evolving catalog table.
6. **Environment handling is internally inconsistent** — `environment=` is a path segment at
   L1/L2 (the single-root pattern the user considers a mistake) but is absent from L3/L4 paths,
   which rely on pointing each environment at a separate `--warehouse-root` (the per-bucket
   pattern the user prefers). Two environments sharing one `--warehouse-root` would collide.

## Accepted As-Is (do not "fix")

- **L1/L2 losing Camelot-style cross-source co-location is fine.** The user confirmed data is
  generally queried per-source at the source-aligned levels, so keeping sources physically
  separate at L1/L2 is acceptable (and is arguably more medallion-correct: L2 = source-aligned,
  conformance belongs at L3).

## Open Problems With Resolution

### P1 — `source_name` (and `ingest_date`) are not carried as columns downstream (ROOT CAUSE — FIX FIRST)
**Status:** Open. **Resolution:** Real-column approach (not token-only).

Modify the normalize pipeline to emit `source_name`, `ingest_date`, and `_run_id` as real data
columns in every L2 parquet row. Do this in two places for belt-and-suspenders:
1. [runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py)
   `NormalizationRunner.normalize_level1_json` — inject these three fields into every row of
   every table in `state.build_tables()` before returning.
2. [level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py)
   `SparkLevel2Writer.write_table` — as a safety net, add the three columns via
   `dataframe.withColumn(...)` before `.parquet()` if not present (defense in depth against
   future callers that bypass the runner).

This unblocks P2, P3, P4 simultaneously. L2 parquet becomes self-describing — columns exist
even if partition discovery is bypassed.

### P2 — Source cannot be a partition of the canonical `level3` table
**Status:** Blocked on P1. **Resolution:** Mercell re-co-location pattern.

Once `source_name` + `ingest_date` are real L2 columns:
- L3 defaults `partitionBy=["source_name", "business_date"]` on every model (see default
  partition convention in Resolved Decisions). This matches the late-arrival-correct pattern:
  output partitioned by event day from the payload, NOT arrival day.
- L3 opt-in override: `partitionBy=["source_name", "ingest_date"]` for snapshot/audit tables
  where arrival-day semantics are the question.
- L3 `partition_overwrite` semantics = overwrite the `(source_name, business_date)` tuple (or
  `(source_name, ingest_date)` if overridden) for the source being run. Dynamic overwrite
  (session-level `partitionOverwriteMode=dynamic`) ensures only matching tuple is replaced.
- Result: one canonical Spark table with `source_name` partitions side-by-side (e.g.
  `level3/canonical_notice/source_name=mercell/...` and `.../source_name=camelot/...` as peers).
  Late arrivals from source X on ingest_date D1 correctly land in business_date D2 partition.
- `source.*` token namespace is **optional** after P1 — SQL authors can just `SELECT source_name`
  from L2. Still add the token for ergonomics, but it is no longer a hard dependency.

### P3 — Governance-by-path is weakened at L3/L4
**Status:** Blocked on P1 + P2. **Resolution:** Genuine partition columns = governance-by-path.

When L3 writes `partitionBy(source_name, business_date)` (the default), the physical layout
becomes `level3/<table>/source_name=<src>/business_date=<date>/`, which directly supports:
- IAM prefix policies: deny `level3/canonical_notice/source_name=external_partner/*` to
  internal analysts. `source_name` is the critical governance partition — date partition adds
  time-windowed IAM granularity if needed.
- ABAC filtering: `WHERE source_name = <current_user_allowed_sources>`.
- Metastore-level GRANTs once a Glue/Hive metastore is added.

Snapshot/audit tables using the `ingest_date` override get identical governance structure, just
with `ingest_date` as the date segment. This is a standard Spark pattern — the path IS the
governance surface. Nothing custom needed beyond correct `partitionBy`.

### P4 — Non-uniform path grammar across levels
**Status:** Open. **Resolution:** Unified within level-type (see Target State table).

Accept that L1/L2 (source-aligned) and L3/L4 (canonical/mart) serve different purposes and
cannot share one identical grammar. Instead, define two consistent sub-grammars:
- Source-aligned (L1/L2): `levelN/source=S/entity=E/.../ingest_date=D/...`. Path segments map
  1:1 to run inputs. Date key is always `ingest_date` (arrival day, immutable once written).
- Canonical (L3/L4): `levelN/table/source_name=S/<date_col>=D`. Path segments map 1:1 to Spark
  `partitionBy` columns. Date key default = `business_date` (event day from payload, enables
  late-arrival repartitioning); override to `ingest_date` only for snapshot/audit tables.

Within each sub-grammar, every segment is a genuine `partitionBy` column recovered via Spark
partition discovery. No orphan path segments.

### P5 — Inconsistent environment handling
**Status:** Decision made. **Resolution:** Per-environment roots/buckets. Drop `environment=`
from ALL paths.

- L1 layout: remove the `environment=<env>/` segment from
  [ingest/storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/ingest/storage.py).
- L2 layout: remove the `environment=<env>/` segment from
  [level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py).
- Retain `environment` on manifests and `RunContext` — it is still needed for logging, audit,
  and config selection. Just remove it from filesystem paths.
- Enforcement: CI check that `environment=` does not appear in any generated path string, or
  runtime assertion in layout classes.

Rationale: aligns with user preference, matches all cloud lakehouse patterns (Databricks
workspaces = per-env storage accounts; EMR = per-env buckets; Glue = per-env catalog IDs).
In-path env breaks point-in-time restore, env-to-env promotion, and IAM prefix boundaries.

## Agreed Implementation Direction (decisions final)

All open questions resolved per Spark medallion best-practice research + user concurrence,
including explicit confirmation that Camelot's late-arriving data repartitioning capability is
preserved (by design, not bolt-on — see dedicated section).

P1–P4 share one linchpin: **carry `source_name` and `ingest_date` as real data columns from
normalize onward, and make them first-class Spark `partitionBy` columns at L3/L4.** The
direction has four pillars:

1. **Normalize emits lineage columns (P1 — real-column approach, not tokens-only).** Runner +
   L2 writer both add `source_name`, `ingest_date`, `_run_id` to every L2 parquet row (see P1
   resolution for the two-layer injection pattern). This makes L2 self-describing and unblocks
   P2/P3/P4 in one step. Critically, this gives every L3 model access to `ingest_date` for
   filtering the L2 read window, even as it rewrites output by a different `business_date`.
2. **Drop `environment=` from ALL filesystem paths (P5 — per-env roots only).** Keep env on
   manifests/context for audit, but never as a path segment. Env = which `--root-path` and
   `--warehouse-root` are pointed at; this maps cleanly to cloud IAM boundaries.
3. **Default partition convention with orthogonal write semantics + dual date semantics (fixes
   L3/L4 partitioning scope AND preserves Camelot late-arrival repartitioning).** `partitionBy`
   and `load_mode` are decoupled in
   [spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py):
   - L3 default: `partitionBy=["source_name", "business_date"]` for `full_refresh`, `append`,
     AND `partition_overwrite`. This is the late-arrival-correct default: output partition is
     based on event date from payload, not arrival date.
   - L3 opt-in override: `partitionBy=["source_name", "ingest_date"]` for snapshot/audit tables
     where "what data looked like on arrival day" is the semantic question.
   - L4 default: `partitionBy=["business_date"]` (same semantics); opt out entirely only for
     non-temporal dimensions.
   - `load_mode` controls WHAT gets overwritten (whole table vs specific partitions), not
     WHETHER there are partitions.
   - Dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`) is
     already set in
     [session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L17)
     — this is what makes partition-level overwrite safe and correct.
4. **L2 reader uses parent-directory partition discovery.** Stop globbing explicit `run_id=*`
   leaf paths in
   [level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py).
   Read the L2 parent prefix, let Spark recover `source`, `entity`, `mapping_version`,
   `ingest_date` as queryable partition columns. Narrow to runs via `.where("run_id IN (...)")`
   or `.where("ingest_date BETWEEN ...")` after read — Spark prunes, so no extra data is
   scanned.

## Resolved Decisions (no longer open)

| # | Former Open Question | Decision | Rationale / Justification |
|---|---|---|---|
| 1 | Canonical L3: re-co-locate sources (Mercell) vs one-table-per-(source,entity) | **Mercell re-co-location.** One canonical L3 table, `source_name` + `business_date` as default partition columns. | Medallion-correct: conformance happens at L3, not L2. Independent per-source replay via `partition_overwrite(source_name, business_date)`. Matches legacy Mercell pattern AND preserves Camelot late-arrival repartitioning in one move. |
| 2 | Default partition convention vs fully opt-in per model | **Default convention with opt-out/override.** L3 default → `source_name` + `business_date` (late-arrival-correct); override → `source_name` + `ingest_date` (snapshot/audit). L4 default → `business_date`. | Uniform governance-by-path, uniform query patterns, zero author overhead for the 95% case. Default `business_date` is the correct semantic for canonical tables (late arrivals land in the right partition). |
| 3 | Per-env roots vs in-path `environment=` | **Per-env roots / buckets.** Drop `environment=` from paths entirely. | Cloud lakehouse standard. Aligns with user preference. Breaks IAM/promotion/restore otherwise. |
| 4 | Two roots (`--root-path` + `--warehouse-root`) vs unified single root | **Two roots retained.** L1/L2 raw under `--root-path`; L3/L4 curated under `--warehouse-root`. | Different lifecycle, retention, RBAC, and encryption between raw and curated. Standard lakehouse pattern. |
| 5 | Pull in deferred `s3a://` object-storage URI work now? | **No, defer to its own PRD.** | P1–P5 changes are orthogonal to S3 URIs. Add path abstraction in layout classes (so `s3a://` is a small later change) but do NOT gate pathing on storage backend. |
| 6 (new) | Camelot late-arriving data repartitioning capability preserved? | **Yes, by design.** Default L3 `business_date` partition + Spark dynamic overwrite = this is the default behavior of any L3 model that selects `business_date`. No separate "repartition job" needed. | Was implicit; now explicitly called out as a design requirement. See "Late-Arriving Data Repartitioning" section for the 4-step flow. |

## Implementation Phase Order (concrete next steps)

Do these in order. Each phase is independently testable; do not skip phases. Late-arriving
repartition end-to-end test runs as part of Phase 4 (it exercises P1 + Phase 3 + Phase 4 together).

**Phase 0 — PRD update (gate, do first)**
1. Update `docs/prd/01-prd-ingestion-raw-to-level1.md` with target L1 grammar (drop `environment=`; keep `ingest_date=` as the sole date key at L1).
2. Update `docs/prd/02-prd-level1-to-level2.md` with target L2 grammar, lineage column contract (`source_name`, `ingest_date`, `_run_id` mandatory in every L2 parquet row), and parent-directory read semantics. Explicitly document the `ingest_date` vs `business_date` distinction: L2 partitions are always by arrival day; downstream rewrites by event day happen at L3.
3. Update `docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md` with:
   - Target L3/L4 grammar (`business_date` default date key, `ingest_date` override for snapshots).
   - Default partition convention (L3: `source_name` + `business_date`; L4: `business_date`) with per-model opt-out/override.
   - Decoupled `partitionBy` vs `load_mode` (partitions apply to all three load modes).
   - Late-arriving data flow: read L2 by `ingest_date` window → write L3 by `business_date` partition.
4. Update `docs/prd/00-prd-architecture-levels-and-governance.md` governance-by-path section to reference L3 `(source_name, business_date)` partitions and per-env roots.

**Phase 1 — P1 (linchpin): carry lineage columns in L2**
5. Modify [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) `NormalizationRunner.normalize_level1_json`: inject `source_name` (from manifest), `ingest_date` (from manifest `ingest_started_at.date()` or equivalent), `_run_id` (from run context — thread parameter through if not currently available) into every row of every table.
6. Modify [normalize/level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py) `SparkLevel2Writer.write_table`: belt-and-suspenders `dataframe.withColumn(...)` for the three columns if missing, before `.parquet()`. Safety net against future callers that bypass the runner.
7. Add / update tests:
   - `tests/test_normalize_runner.py` asserts `source_name`, `ingest_date`, `_run_id` present in every table's result rows.
   - New `tests/test_level2_storage.py` (or extend existing) asserts the three columns are present in the written parquet dataframe after `SparkLevel2Writer.write_table`.

**Phase 2 — P5: drop `environment=` from paths**
8. Modify [ingest/storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/ingest/storage.py) `LocalArtifactLayout.level1_run_dir` and related methods: remove the `environment=<env>/` path segment. Keep `manifest.environment` populated (still needed for audit / config / logging).
9. Modify [normalize/level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py) `LocalLevel2Layout.table_run_dir`: remove the `environment=<env>/` path segment.
10. Update any tests that assert on generated L1/L2 path strings. Add a runtime assertion in both layout classes that the resulting path string does NOT contain the substring `environment=` (fail-fast guard).

**Phase 3 — L2 reader: parent-directory partition discovery**
11. Modify [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py) `Level2DatasetLocator.read`: compute the L2 parent prefix (NOT glob to `run_id=*` leaf dirs). Pass the parent dir to `spark.read.option("mergeSchema","true").parquet(parent_path)`.
12. Apply run_id / source / entity / ingest_date filters via `.where(...)` post-read. Because the parent `key=value` segments are now discovered as Spark partition columns, these filters prune at the filesystem level (no full table scan). Verify in tests via `df.explain(True)` that the physical plan shows `PartitionFilters` for the relevant columns.

**Phase 4 — L3/L4: default partitions + decouple from load_mode + late-arrival end-to-end test**
13. Modify [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py) `_execute_model`:
    - Compute EFFECTIVE `partition_columns`:
      - If `stage == level3` and `target.partition_columns` is empty → default to `["source_name", "business_date"]`.
      - If `stage == level4` and `target.partition_columns` is empty → default to `["business_date"]`.
      - If `target.partition_columns` is explicitly set → use the manifest's value (opt-out / override mechanism). Exception: manifest can set an empty list only if `load_mode == full_refresh` to explicitly request no partitioning.
      - Validate: if the effective partition columns reference a column (e.g. `business_date`) that is not produced by the SQL model's SELECT, Spark will raise a readable error at write time — that's the right enforcement, no extra check needed.
    - Apply `.partitionBy(*effective_partition_columns)` for ALL THREE load modes (`full_refresh`, `append`, `partition_overwrite`), not just `partition_overwrite`.
    - Keep existing `partition_overwrite` dynamic partition overwrite logic (it relies on `partitionBy` + the session-level `partitionOverwriteMode=dynamic` setting).
14. Add / update `_table_path` if needed: the base path should remain `<warehouse_root>/level3/<table_name>` and `partitionBy` will add the `source_name=.../business_date=.../` segments beneath it. No other changes needed here.
15. **Late-arriving repartition E2E test** (exercises P1 + Phase 3 + Phase 4 together, proves Camelot capability):
    - Ingest / normalize two synthetic L2 records: `record_A = {business_date: "2026-07-31", ..., ingest_date: "2026-08-10", source_name: "S"}` and `record_B = {business_date: "2026-08-10", ..., ingest_date: "2026-08-10", source_name: "S"}`.
    - Run an L3 model whose SELECT: filters `WHERE ingest_date = "2026-08-10"` (reads everything that arrived on Aug 10), passes `business_date` through in output columns.
    - Configure L3 model with `load_mode: partition_overwrite`, default partition columns (so: `source_name`, `business_date`).
    - Assert on written filesystem layout: both `level3/<table>/source_name=S/business_date=2026-07-31/` AND `.../business_date=2026-08-10/` exist, each containing the correct single record.
    - Assert re-run idempotency: run the L3 model again with the same inputs → file layout identical, no extra files or duplicate rows.
    - Assert overwrite is partition-scoped (not whole-table): pre-seed a `source_name=S/business_date=2026-06-01/` partition with dummy data → run the L3 model for `ingest_date=2026-08-10` → assert the `2026-06-01` partition is untouched.

**Phase 5 — Ergonomics and hardening (optional but recommended)**
16. Add `source.*` token namespace to [sql/compiler.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/compiler.py) `build_token_context` for ergonomics (not required — P1 already exposes `source_name` via L2 data columns).
17. Add examples: new SQL model under `examples/sql/local_demo/level3/` that implements the late-arrival pattern: reads an L2 table, SELECTs `business_date` alongside other columns, and produces one canonical L3 table with multiple `(source_name, business_date)` partitions. Include a short comment in the SQL explaining the late-arrival flow.
18. Update operator runbook in `docs/operator/LOCAL_OPERATOR_RUNBOOK.md` with: (a) per-env-roots setup convention (dev/staging/prod each get their own `--root-path` and `--warehouse-root`), (b) the standard late-arrival recovery procedure (replay an ingest_date window → L3 correctly rewrites only the affected business_date partitions).

A fresh session picking this up should start at Phase 0 (PRD updates) after re-verifying the five code references in "Current State" against HEAD.

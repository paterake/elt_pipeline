# Path & Partition Management Backlog

Active backlog for how `elt_pipeline` maps physical storage location to logical table
identity and partitioning across `level1`–`level5`. This document is a self-contained
handoff: a fresh session should be able to continue the work from here without re-deriving
the analysis.

## Status

- Backlog status: active (analysis complete, no implementation started)
- Driver: architectural review after the Spark engine migration (see
  `docs/todo/archive/TODO_SPARK_COMPLETED.md`)
- Gate: the changes below alter the `level2` data contract and the `level3`/`level4` path
  grammar, so per repo governance they require a PRD update
  (`docs/prd/02-*`, `docs/prd/03-*`, `docs/prd/00-prd-architecture-levels-and-governance.md`)
  **before** implementation.

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

## Current State (verified against code)

Three different path grammars across two roots:

| Level | Root | Grammar |
|---|---|---|
| L1 | `--root-path` | `level1/environment=<env>/source=<src>/entity=<entity>/ingest_date=<date>[/window=<label>]/run_id=<id>/<file>` |
| L2 | `--root-path` | `level2/environment=<env>/source=<src>/entity=<entity>/mapping_version=<v>/ingest_date=<date>/table=<physical>/run_id=<id>/*.parquet` |
| L3–L4 | `--warehouse-root` | `level{3,4}/<table_name>/[<partcol>=<val>/...]` |

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

## Open Problems To Address

### P1 — `source_name` (and `ingest_date`) are not carried as columns downstream
Root cause behind P2 and P3. Because normalize does not emit `source_name`/`ingest_date` as
data columns and there is no `source` token, no downstream stage can filter, partition, or
govern by source without the author manually inventing the column.

### P2 — Source cannot be a partition of the canonical `level3` table
This is the medallion-correct pattern both legacy platforms used at L3 ("conform separate
sources into a shared canonical table, partitioned by `source_name`, each source individually
replayable via partition overwrite"). It is currently unreachable: there is no `source_name`
column to `partitionBy`, and the SQL stage is model-package-scoped (not source-scoped), so
there is no ambient source identity to stamp.

### P3 — Governance-by-path is weakened at L3/L4
`docs/prd/00-prd-architecture-levels-and-governance.md` calls for RBAC by level-path and ABAC
by level + source. The flat `level{3,4}/<table_name>/` layout supports "lock a whole level" but
not "restrict by source within a level," because `source` is not in the path.

### P4 — Non-uniform path grammar across levels
Three grammars across two roots (see Current State). The legacy's single grammar is what made
governance and incremental replay uniform; the refactor did not keep it.

### P5 — Inconsistent environment handling
`environment=` in-path at L1/L2 vs separate-root at L3/L4, neither enforced. Decide one model.
(The user leans toward separate-root/bucket-per-env over in-path env.)

## Proposed Direction (candidate — needs decision)

P1–P4 share one root cause and one fix: **carry `source_name` and `ingest_date` as real data
columns from normalize onward, and make them first-class partition columns.** Concretely:

1. **Normalize emits lineage columns.** Have the runner (or the L2 writer) add `source_name`
   and `ingest_date` (and consider `run_id`) as actual columns in every L2 parquet row —
   mirroring the legacy `source_name` / `*_insert_date` / `bi_bdp_source_filename` columns that
   were carried through every level. This makes L2 self-describing and unblocks everything
   below. (Alternative/adjunct: add a `source.*` token namespace so SQL can synthesize the
   column — weaker, prefer the real-column approach.)
2. **Adopt a single consistent path grammar.** Candidate:
   - source-aligned levels (L1/L2): keep `environment/source/entity/...`.
   - canonical/mart levels (L3/L4): `level{3,4}/<table_name>/source_name=<src>/<date>=<val>/`
     with `source_name` and the date as genuine `partitionBy` columns.
   This restores uniform governance-by-path **and** fixes P2 in one move.
3. **Make source/date partitioning available under `full_refresh`/`append`, not only
   `partition_overwrite`** — or define a default partition convention so canonical tables are
   source/date-partitioned by default (closer to the legacy universal convention).
4. **Decide environment model (P5).** Either commit to per-environment roots/buckets (drop
   `environment=` from paths) or keep it in-path everywhere — but make it uniform.

## Open Questions (require user / PRD decision)

- Should the canonical layer re-co-locate sources into shared tables partitioned by
  `source_name` (Mercell pattern), or keep one physical table per (source, entity) at L3 too?
- Is a default source/date partition convention wanted, or should partitioning stay fully
  opt-in per model?
- Per-environment roots vs in-path `environment=` — pick one.
- Do we keep two roots (`--root-path` + `--warehouse-root`) or unify under one storage root
  with a single tier grammar?
- Does this pull in the deferred object-storage URI work (`s3a://`) from
  `TODO_SPARK_COMPLETED.md`, since a uniform storage grammar is a natural place to introduce
  it?

## First Steps For The Next Session

1. Read this doc, then confirm the Current State facts against the five code references above
   (they were verified during the review but re-check before changing anything).
2. Get a decision on the Open Questions — these change the L2 contract, so do not implement
   first.
3. Update `docs/prd/02-*` and `docs/prd/03-*` (and the governance PRD) with the agreed grammar
   before code.
4. Implement P1 (carry `source_name`/`ingest_date` as L2 columns) first — it is the smallest,
   highest-leverage change and unblocks P2/P3. Everything else builds on it.

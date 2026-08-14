# Custom-Code Parity (Pure-Python Driver → Spark-Native Execution) Backlog

## Purpose

This file is the **active implementation backlog** for the analysis and remediation work identified on 2026-08-14, answering the architectural question:

> *Ignoring ingestion, is the whole transform layer progression Apache Spark only — or has custom code been developed that could instead be done inside Spark?*

The audit was performed against two prior-platform conventions (`mercell`, `camelot`) and specifically against the developer's memory of:
1. The **Mercell/Camelot transform execution split** (L1→L2 relationalize + L2→L3/L4 SQL materialization as two distinct Spark-bound stages).
2. The **Mercell/Camelot "write-to-temp-area-then-move"** overwrite pattern required to avoid the Spark DAG read-from-same-path-as-write-back conflict.

The current repository *did* implement the SQL stage in all-Spark correctly, but **the L1→L2 normalize/relationalize stage is implemented entirely as a single-process pure-Python driver walk over row/value instances (no PySpark imports)**, plus three smaller driver-side data-scale helpers that Spark already has primitives for.

Additionally: **the Spark 4.x `SaveMode.Overwrite` path still does NOT resolve the original read-from-same-location-then-write-back-into-it DAG hazard** — the exact same hazard that the Mercell/Camelot temp-area-then-move patterns were written to solve. Current code does direct `mode("overwrite").parquet(target_path)` on the SQL side with no staging, which will fail whenever a model queries the same table path it overwrites.

This backlog is the single session-continuity document for this implementation initiative. Once complete, it will be moved to `docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md`.

The work product *is not* scope drift: it directly aligns the execution model to the exact Mercell/Camelot conventions the developer originally built and intends to preserve, and closes the same-path overwrite hazard those conventions solved.

---

## Current Status

- **Phase 0: Analysis / Audit (2026-08-14)** ✅ COMPLETED 2026-08-14.
  - Scope recorded in § Audit Findings below.
  - Mercell/Camelot parity decision rationale recorded in § Open Decisions below.
  - Two distinct remediation tracks identified: (A) normalize Spark-native relationalization; (B) same-path DAG overwrite hazard (temp-area swap).
- **Phase 1: Gate 1 (Design record + target contracts)** ⏳ NEXT UP.
  - Gate deliverable: gated spec for Track A + Track B, preserving (byte-identical where possible) current output contracts:
    - `Level2TableManifest` fields + `data_path` / `manifest_path` relative-layout semantics.
    - `MappingCatalog.mapping_version` 16-hex hash computation must produce identical SHA-256 prefix on equivalent logical plan.
    - `NormalizedTable` physical-name policy (63-char cap + SHA-8 suffix collision guard).
    - `SqlLoadMode.partition_overwrite` semantics must match the existing contract (DYNAMIC required flag in runbook).
  - Gate deliverable: add one new extras bucket `delta = ["delta-spark>=4.0,<5.0"]` for teams that want Delta Lake ACID over staging-swap (recorded as Open Decision OD-1 path (3)).
- **Phase 2: Gate 2 (Track A: normalize Spark-native relationalization)** ⏳ PENDING design.
- **Phase 3: Gate 3 (Track B: same-path overwrite hazard — staging-swap write protocol)** ⏳ PENDING design.
- **Phase 4: Gate 4 (Hardening / quality / docs sweep)** ⏳ PENDING.
- **Phase 5: Gate 5 (Environment sign-off — same scope as PRD 08 Gate 5: JVM 17+ on workstation + EMR E2E)** ⏳ PENDING (environment-only).

Active workstream as of 2026-08-14.

---

## Audit Findings (2026-08-14)

### Finding 1 — L2→L3→L4 (SQL transforms) are all-Spark, correctly (no remediation required for data path)

All data operations on the SQL side are native Spark — no per-row Python driver work. Driver Python is control-flow only (DAG order, lineage/audit/quality-hook callbacks). Evidence:

- Execution entry: `run_sql_models_locally` in [sql/runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/runtime.py#L39-L55)
- Per-model inputs read via `spark.read.parquet(dependency_path)` and registered as temp views: [SparkSqlModelExecutor._register_execute_inputs](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L123-L152)
- User SQL executed via `spark.sql(select_sql)`; materialization uses DataFrameWriter with `mode("overwrite"|"append"|"partition_overwrite").partitionBy(...).parquet(target_path)`: [_execute_model](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L165-L196)
- Validations (`row_count_min`, `unique_columns`, `not_null_columns`) run on the executor-side DataFrame: [_validate_model](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L197-L290)

**Remediation status:** Finding 1 requires no code changes for data-plane. Finding 2 below (same-path overwrite hazard) still applies to this same code.

---

### Finding 2 — Same-path overwrite DAG hazard is NOT solved in Spark 4.x (direct driver bug; Mercell/Camelot pattern needed)

This is **not** a style/scale concern; it is a concrete correctness / reproducibility bug waiting to happen the first time a user writes a SQL model that "reads from the canonical path and writes back."

#### Current state (SQL transforms, write path)

All three overwrite/append branches in `_execute_model` at [spark_executor.py:L165-L196](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L165-L196) write directly:

```python
# full_refresh and partition_overwrite both:
writer = dataframe.write.mode("overwrite")
# ...
writer.parquet(target_path)                       # <- target_path is table path
```

There is **no staging path.** There is **no atomic swap.** There is **no pre-cache()/checkpoint() of the materialized DataFrame before the delete-first write.**

#### Why this breaks in Spark 4.x (unchanged from earlier Spark) — evidence

1. **Apache Spark official docs on SaveMode** (all versions up to and including 4.x): SaveMode.Overwrite "delete[s] the data before writing out the new data" for plain Parquet / file sources. No locking. No transaction. No stage. (Ref web search: official save-mode docs — `Overwrite mode means ... data will be deleted before writing out the new data.`)
2. **2026 contemporary reports (Apr 13 '26 question)** — the exact same-path read→overwrite pattern fails with `No such file or directory 's3://.../part-...snappy.parquet'` because FileFormatWriter's overwrite branch deletes the input files before the DAG re-reads them on recomputation (even with .cache() applied). (Ref exchangetuts Spark same-location overwrite answer 2026.)
3. **Static tooling rules** now flag this exact pattern as an error-ranked SDK anti-pattern (SDK042 "detect read and write to same path — data loss risk"). (Ref blog/doctor rule SDK042, 2026.)
4. **PRD 08 § atomic write semantics** already codifies per-file temp-then-replace pattern for leaf JSON/text writes: write candidate `.tmp`, replace. See [08-prd-storage-root-uri-io-dispatch.md:L111](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md#L111-L111). Parquet table directories are the same atomic-write concern scaled to directories, not files.
5. **Current normalize L2 write path** also uses `mode("error")` [level2_storage.py:L179-L181](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py#L179-L181) — this *is* safe because level2 write-target is a fresh `run_id=...` directory never read in the same normalize action, so same-path hazard cannot occur. That correctness guarantee does not extend to the SQL stage, where canonical_orders (for example) can be a self-querying rebuild.

The exact Mercell/Camelot convention (staging-area write, then atomic move/rename-or-delete-then-rename) is therefore **still required** in 2026 on Spark 4.x, same as the day you wrote it.

#### Three remediation paths (Open Decision OD-1 records the default choice)

| Path | Summary | When to use |
|---|---|---|
| **(1) Mercell/Camelot staging-swap write protocol (DEFAULT TRACK B)** | `write → {warehouse}/_staging/stage={s}/table={t}/run_id={run}/ → swap`; swap semantics: POSIX rename(local) or S3 list-delete-rename-list-copy(s3); use same `_NO_STAGING_MOVE` fail-fast when storage is not POSIX-or-S3. | Default. Requires no new dependencies. Matches developer's prior pattern. Works with the plain-parquet contract PRD 03/08 require. |
| **(2) Pre-checkpoint + in-memory unlink before write** | `dataframe.checkpoint(eager=True)` → new lineage, read plan no longer references `target_path` input files; then `mode("overwrite").parquet(target_path)` safe. | Quick fix for single-node, local-only; not recommended for EMR (checkpoint dirs still on shared storage). |
| **(3) Delta Lake ACID writes; keep staging-swap as plain-parquet fallback** | Register `delta` extras bucket; allow user to specify `storage_format=delta` per model. Delta Lake does not have this hazard (transaction log commits). | Optional future gate; only if PRD 03 contract is amended to permit non-parquet materializations. |

---

### Finding 3 — L1→L2 NormalizeRunner relationalize/relationalize walk is pure Python driver, data-scale (major remediation = Track A)

Module: [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py). 0 PySpark imports. Entire relationalizer runs on one Python GIL-bound core on the Spark driver.

Breakdown by code section:

| Section | What it does | Current implementation | Scale characteristic |
|---|---|---|---|
| `normalize_level1` dispatcher | CSV vs JSON | `payload_format` string check, call into subroutines. | Metadata (fine). |
| `_load_json_payload`, `_load_text_payload` | Decode L1 payload bytes → Python native `dict`/`list`/`str` | `json.loads` / UTF-8 decode, fail fast on errors | Data-scale. Entire payload held on driver heap. |
| `normalize_level1_json` + recursion (`_append_row`, `_populate_from_object`) | Walks every dict/list value instance; flattens dicts; splits arrays into child `_TableState` | Recursion over *values*, not schema: every dict→`_populate_from_object`, every array→`for index,item in enumerate(array): self._append_row(item,...)` | O(rows × nesting × fields) Python function calls on driver. Single core. |
| `normalize_level1_csv` | CSV header + row loop | `csv.DictReader(StringIO(csv_text))` → `for source_row in reader: root_table.rows.append(row)` with `uuid4()` per row. | O(rows) Python loop on driver with GIL-held per-row. |
| `_RunnerState.build_tables()` | Emit per-table rows with lineage columns | `for row in table.rows: row.setdefault(source_name/ingest_date/_run_id)` | O(rows) per-table Python loop on the already-materialized list[dict] copy. |
| `_RunnerState.build_mapping_catalog()` + `_build_mapping_version()` | Derive `TableMappingEntry` list + hash 16-hex mapping version | Hash canonical JSON of metadata entries; identifier sanitization + hashed suffix collision policy | O(schema nodes) metadata-scale. FINE to keep Python. |
| `_build_hashed_identifier` | 63-char max table name policy | `sha256(logical_path).hexdigest()[:8]` suffix on long names, with collision guard. | Metadata only. FINE to keep. |

#### Spark-native parity that exists (so the data-scale Python walk is *not* required to achieve arbitrary-depth relationalize)

The premise "Spark's built-in relationalize is one level, hence loop required" is **valid on the primitive level, but the loop can run on metadata instead of data rows.** Exact parity:

| Current Python-walk result | Spark-native primitive chain (metadata loop only) |
|---|---|
| Arbitrary-depth dict flattening: `$.a.b.c` → physical `a__b__c` via `make_column_name()` | `spark.read.json(...)` — already infers arbitrary-depth StructType. Fixed-point metadata loop: while any `field.dataType is StructType`, `df = df.select(flatten_structs_once)` → columns renamed to `a__b__c`. Driver loop: O(schema_nodes). Catalyst collapses to one projection. |
| Array at `$.items` → child table with `_parent_row_id`, `_array_index`, one row per item | `posexplode_outer(col("items")) as (_array_index, item)` projected with `_row_id as _parent_row_id`. Recursion on the schema of `item` struct for nested arrays. |
| `_row_id` / `_parent_row_id` / `_array_index` FK per row | `uuid()` or `monotonically_increasing_id()` column; inheritance via `select(...)` on child DataFrames. Generated executor-side. |
| source_name / ingest_date / _run_id lineage constants | `withColumn(col, lit(constant))` — standard Spark. |

**Critical boundary:** MappingCatalog / identifier-policy / layout-code must remain driver-side Python (platform policy, Spark has no opinion) — that's custom code, and fine to keep. The *data walk* is what moves into Spark.

#### Parquet write handoff overhead compounds the Python walk

After materializing list[dict] per table in driver memory, current code does a double-parse:

- `_rows_to_dataframe` in [level2_storage.py:L73-L78](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py#L73-L78):
  ```python
  json_lines = [json.dumps(row, sort_keys=True, default=str) for row in rows]  # driver python loop
  rdd = spark.sparkContext.parallelize(json_lines)                              # driver→executor transfer
  return spark.read.json(rdd)                                                    # executor RE-parse each row JSON
  ```
That double-parse + driver→executor copy disappears entirely when normalize starts from `spark.read.json` on the raw bytes.

#### Data-scale size summary on driver

For an N-row / MB-sized payload, the driver simultaneously holds:
1. The raw L1 bytes payload (from ingest storage read).
2. The `json.loads()` decoded tree: ~2–3× raw size in Python objects.
3. One list[dict] per derived table (copy of all scalars).
4. The `json_lines` list (one JSON string per row × per table) before parallelize (~same size as raw).

Peak ~(5–8)× the payload size in Python heap. In Spark-native design, driver heap holds only schema + plan (KBs).

---

### Finding 4 — `_summarize_parquet_dir` size computation is driver-per-file, s3 branch reads full body (data scale, correctness bug on s3)

Location: [level2_storage.py:L81-L101](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py#L81-L101).

Behavior by scheme:
- `file` / `local_unschemed`: `os.stat(local_path).st_size` per part file. (Acceptable but still driver O(part_files).)
- **`s3` / non-file:** `total_bytes += len(path_read_bytes(part_file))` **per part file.**
  - Full GET of the part body to sum bytes. Not HEAD.
  - On EMR with 1000 part files this is 1000 expensive S3 GETs of multi-MB payloads just for a manifest size number.

Spark-native parity available without this loop:
- `file_count = len(dataframe.inputFiles())`
- `total_bytes` can be obtained from storage-specific size via write query-summary metrics or, if unavailable, `HEAD` ContentLength on each inputFile (not full GET). The pure-s3 dispatcher in `shared/path_utils.py` needs a `path_content_length` primitive for this. Finding 4 → Track A Gate 2 item.

---

### Finding 5 — `Level2DatasetLocator` does metadata glob on driver; SQL read already Spark (partial, low-priority)

Level2 source discovery at [level2_source.py:L23-L66](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py#L23-L66) does `path_rglob` + `path_is_dir` enumeration on the driver to fail-fast "no L2 data found" — then reads the entire `entity_root` anyway with `mergeSchema=true` and `filter(table == sanitized_table)`. The glob is metadata-scale, low-priority to change. Potential improvement: replace with `spark.read.parquet(...).select("table").distinct().collect()` but at the cost of losing the explicit `level2_source_not_found` error framing. Finding 5 → optional Gate 4 polish item.

---

## Scope of the Backlog

Two tracks, which can be implemented sequentially (recommended so SQL swap protocol is available before any normalize self-query canonical builds are possible):

### Track A — Normalize Pure-Python → Spark-Native Relationalization

Goal: preserve current output contracts (byte-identical mapping_version hashes where possible; identical Level2TableManifest directory layout; same data column names) while moving all per-row relationalization work into Spark executors.

Deliverables:
- New module `normalize/planner.py` (pure driver, metadata scale): walks a StructType (produced by Spark) into a `NormalizationPlan` with one entry per logical table. Re-uses `_sanitize_identifier`, `_RunnerState.make_table_name`/hashed-identifier, `_build_mapping_version` — the exact same policy code to guarantee identical hashes on the same logical schema shapes.
- New module `normalize/spark_runner.py`: takes `NormalizationPlan` + raw payload DataFrame, emits one DataFrame per planned table. Uses standard `posexplode_outer` + struct flattening + `uuid()`/FK plumbing.
- Rewire `normalize_level1_to_local_level2` in `pipeline.py`: planner → spark_runner → SparkLevel2Writer with dataframes instead of list[dict].
- Replace `_rows_to_dataframe` dead path.
- Replace `_summarize_parquet_dir` s3 full-read path with a new dispatcher primitive `path_content_length` → s3 HEAD Object; local `os.stat` (add to `shared/path_utils.py` as part of this track).
- One new compatibility test fixture: a JSON payload with nested arrays at 3 depth levels (root → items → tax_breakdowns → jurisdictions) + a CSV fixture → assert that mapping_version, table names, column names, FK pairs, and row-level JSON-representation of output match the legacy runner. Mapping-version parity is a hard requirement.

### Track B — Same-Path Overwrite Hazard (Staging-Swap Write Protocol)

Goal: eliminate the direct `mode("overwrite").parquet(target_path)` pattern in `_execute_model` for `full_refresh` and `partition_overwrite` load modes. Append is safe for read→write back so remains direct.

Deliverables:
- New config key in Sql model manifest: optional `staging_root: str | null`; if null default is `join_paths(warehouse_root, "_staging")`.
- `_execute_model` for overwrite modes:
  (1) Compute `staging_path = join_paths(staging_root, stage.value, table_name, "run_id=" + run_context.run_id)`.
  (2) `writer.parquet(staging_path)` write.
  (3) `validate_stage(staging_path, expected_row_count)` — read+count once; also capture row_count from this read (eliminates the second `self.spark.read.parquet(target_path).count()` call we do on line 195 today, saving a full read).
  (4) `atomic_swap(staging_path, target_path, *, scheme)`: POSIX → `rename(2)` (atomic); S3 → batch list staging → CopyObject (staging → target path same key) → DELETE staging copy → list target to confirm. Fail-fast if scheme is not file/local_unschemed/s3 (record same decision pattern as PRD 08's storage dispatch guard).
  (5) On failure mid-swap: best-effort delete staging + report clear operator error.
- One new test: create a model whose SQL body unions the existing canonical rows with new rows (so it reads `target_path` via the dependency path and writes back). Run under both local POSIX and in a mocked s3 dispatch harness; assert that input files are not deleted mid-DAG-recompute and final output is correct.
- Add `delta` extras bucket `pyproject.toml` as optional remediation path (3); no code change in same gate, just the extras entry + a note in Open Decisions.

---

## Open Decisions

### OD-1 (2026-08-13): Same-path overwrite default remediation path

**Decision:** DEFAULT → path (1) Mercell/Camelot staging-swap write protocol.

**Rationale:**
- Explicit parity with developer's prior `mercell`/`camelot` solution, which was battle-tested for exactly this Spark DAG hazard.
- Zero new required dependencies (works under PRD 03's plain-parquet materialization contract).
- Path (2) checkpoint-only is fragile on EMR (checkpoint locations on shared S3 can still conflict for concurrent runs) and not the operator's conventional muscle memory.
- Path (3) Delta Lake ACID is an allowed *optional* future path — not default. `delta` extras added, but PRD 03 does not require non-parquet files as default.

**Follow-up:** Gate 3 Track B implements staging-swap. PRD 03 can be annotated to list Delta as optional materialization format at a later PRD.

### OD-2 (2026-08-13): MappingCatalog version parity is required (byte-identical)

**Decision:** `mapping_version` 16-hex SHA-256 prefix must compute identically for equivalent logical plans before and after Track A migration.

**Rationale:**
- `mapping_version` is in every downstream path (L2 layout `mapping_version=` segment); changing it on day-1 of the new relationalizer would break L3 canonical partition lookups and appear as "data vanished" to operators.
- The hash input is metadata-only (canonical sorted JSON of `TableMappingEntry` list) which is already stable — no reason to change.

**Implementation detail:** `normalize/planner.py` re-uses verbatim `_build_mapping_version` and identifier-construction policy code from current `_RunnerState` (lift into a shared policy helper used by both old + new paths during transition).

### OD-3 (2026-08-13): Legacy pure-Python runner is kept behind a feature flag during transition

**Decision:** Keep `NormalizationRunner` (legacy pure-Python) available as `--normalize-engine python | spark` CLI switch for at least one release cycle after Track A ships.

**Rationale:**
- Any edge payload that produced a mapping_version hash under the old walker that *doesn't* reproduce under the new Spark planner (due to JSON schema inference vs Python dict walk differences) can be pinned to the old engine while the case is triaged.
- De-risk deployments in G5 EMR E2E: operators can roll back to python engine for one source if Spark planner edge case found.

### OD-4 (2026-08-13): Same-path overwrite apply-bounds

**Decision:** Staging-swap applies to `SqlLoadMode.full_refresh` and `SqlLoadMode.partition_overwrite`. Does NOT apply to `SqlLoadMode.append` (Spark's append semantics for new files don't conflict with reads of existing files). Normalize L2 writes keep `mode("error")` and direct write (each normalize run has a fresh `run_id=` directory so same-path hazard is structurally impossible).

### OD-5 (2026-08-13): s3 size summary stops reading full part-file bodies

**Decision:** Finding 4 → implement `path_content_length(uri: str) -> int` in `shared/path_utils.py` as a new dispatcher primitive (local: `os.stat().st_size`; s3: boto3 `head_object` `ContentLength`). Retrofit `_summarize_parquet_dir` to use it for all schemes. This is an correctness/cost fix on the s3 path independent of Track A but bundled with Track A because both change normalize/level2_storage.py.

---

## Completion Checklist

- [ ] Gate 1 design record written; pyproject delta extras added.
- [ ] Track A: normalize/planner.py + metadata walk + mapping_version parity test passes vs legacy.
- [ ] Track A: normalize/spark_runner.py + posexplode/struct-flatten execution produces identical row-level outputs for the 3-deep nested fixture + CSV fixture.
- [ ] Track A: pipeline.py rewire complete; `_rows_to_dataframe` no longer on hot path; `path_content_length` dispatcher added + s3 HEAD.
- [ ] Track B: staging_root config, `_execute_model` overwrite branches use staging_path + atomic_swap (POSIX rename + S3 batch copy/delete).
- [ ] Track B: same-path self-query model test passes on both local POSIX and mocked s3.
- [ ] Gate 4: ruff all clean; diagnostics 0; regression all 113 existing non-Spark tests green; Gate 5 environment sign-off same scope as PRD 08.
- [ ] Update operator runbook § overwrite protocol.
- [ ] Move this TODO file → `docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md`; update top-level `docs/todo/TODO.md` index row.

---

## Cross-References

- [PRD 02: Level 1 → Level 2](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/02-prd-level1-to-level2.md)
- [PRD 03: SQL Level 2 → Level 3 and Level 3 → Level 4](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md)
- [PRD 08: URI-Aware Storage Root Paths and Explicit-Config I/O Dispatch](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md)
- Archived backlogs that reaffirm Mercell/Camelot partition/write conventions:
  - [TODO_PATHING_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_PATHING_COMPLETED.md) (Mercell re-co-location + Camelot late-arrival repartitioning)
  - [TODO_STORAGE_URI_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_STORAGE_URI_COMPLETED.md) (scheme dispatch; same s3/posix file swap atomic pattern PRD 08 § per-leaf files now extends to parquet dir swap)
- Current code hot-spots:
  - [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) (Finding 3)
  - [normalize/level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py) (Findings 3.5 + 4)
  - [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py) (Finding 2 — overwrite without staging)

### Source Archive Locator Record (2026-08-14 workstation-local baseline, non-committed)

Track A (normalize Spark-schema-walk pattern) and Track B (staging-swap write protocol) are explicitly ported **from the Scala reference implementations of Mercell and Camelot**, not from scratch. The provenance-recorded legacy baselines map to the following workstation paths as of this session:

| Label in this TODO | Repository archive path (this workstation) | Review emphasis in this backlog |
|---|---|---|
| **Camelot** | `~/Documents/__code/archive/camelot/__code/bi-bdp-elt` | Track B Mercell/Camelot staging-swap write protocol pattern; also L2→L3 relationalize/write pattern in `transform-glue`. |
| **Mercell** | `~/Documents/__code/archive/mercell/__code/edp-elt-ingestion-main` | Track A Spark `StructType` schema walker → `posexplode`/FK child-table plan; mapping-version / table naming policy parity. |
| **Camelot cfg** | `~/Documents/__code/archive/camelot/__code/bi-bdp-elt-cfg` | Config/layering anti-pattern review (see IMPLEMENTATION_SOURCE_PROVENANCE pain points). |
| **Mercell cfg (transformation)** | `~/Documents/__code/archive/mercell/__code/edp-elt-transformation-cfg-main` | Schema evolution / mapping catalog pattern parity. |
| **Mercell cfg (ingestion)** | `~/Documents/__code/archive/mercell/__code/edp-elt-ingestion-cfg-main` | Config/layering anti-pattern review. |

**Governance note (matches IMPLEMENTATION_SOURCE_PROVENANCE.md § Client-Neutral Guardrails):**
- These paths are **workstation-local reference only.** This repository MUST remain client-neutral.
- Do NOT commit proprietary code, verbatim config, or absolute workstation paths to any committed contract file beyond this session-specific provenance record in the active session TODO (which may be moved to archive with the archive move step).
- For future sessions on a different machine, update IMPLEMENTATION_SOURCE_PROVENANCE § **Local Machine Source Map (Not Committed)** via a gitignored `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.local.md` rather than editing this row. See [IMPLEMENTATION_SOURCE_PROVENANCE.md:L44-L53](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/IMPLEMENTATION_SOURCE_PROVENANCE.md#L44-L53).

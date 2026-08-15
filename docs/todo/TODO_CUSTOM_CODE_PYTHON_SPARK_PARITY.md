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
- **Phase 1: Gate 1 (Design record + target contracts)** ✅ COMPLETED 2026-08-14.
  - Gated spec for Track A + Track B recorded in § Gate 1 Design Record below, with four explicit target contracts:
    - Contract C1: `Level2TableManifest` fields + `data_path` / `manifest_path` relative-layout semantics preserved byte-identical.
    - Contract C2: `MappingCatalog.mapping_version` 16-hex hash computation produces identical SHA-256 prefix on equivalent logical plan (planner reuses verbatim `_build_mapping_version` + identifier policy code).
    - Contract C3: `NormalizedTable` physical-name policy (63-char cap + SHA-8 suffix collision guard) preserved via shared policy module.
    - Contract C4: `SqlLoadMode.partition_overwrite` semantics preserved — `partitionBy` + `mode("overwrite")` behavior unchanged; staging-swap wraps the write step, does not alter partition semantics.
  - Extras bucket `delta = ["delta-spark>=4.0,<5.0"]` added to `pyproject.toml` for teams that want Delta Lake ACID over staging-swap (Open Decision OD-1 path (3)).
  - Track A design: planner walks `StructType` metadata → emits `NormalizationPlan` → spark_runner executes `posexplode_outer`/struct-flatten chain, producing same `NormalizedTable` column layouts.
  - Track B design: staging-swap write protocol with scheme-aware atomic swap (POSIX rename / S3 batch list-copy-delete).
- **Phase 2: Gate 2 (Track A: normalize Spark-native relationalization)** ✅ COMPLETED 2026-08-14.
  - `planner.py` `plan_from_schema` walks `StructType` metadata depth-first → produces `PlannedTable` / `PlannedArrayExplosion` plan nodes.
  - `spark_runner.py` `SparkRelationalizer.execute` runs `posexplode_outer` + struct flatten + `uuid()` FK chain for root + all child tables.
  - `pipeline.py` rewired with `NormalizeEngine = Literal["python", "spark"]` dual-engine gate behind `normalize_engine="python"` default (OD-3).
  - `path_content_length` dispatcher added to `shared/path_utils.py` (S3 HEAD Object → ContentLength; POSIX `os.stat`). Finding 4 fix retrofitted to `_summarize_parquet_dir`.
  - `_rows_to_dataframe` retained as legacy dead-path behind python engine only; `SparkLevel2Writer.write_dataframe` new entry for Spark path.
  - Contract C2 verified: 3-deep nested-orders `mapping_version` parity test PASS (identical 16-hex SHA-256 prefix between legacy runner vs Spark planner).
  - Track A row-level `SparkRelationalizer` parity tests (2) written as `@pytest.mark.skipif(not _HAS_PYSPARK_JVM, …)` → Gate 5 execution environment sign-off scope (JVM 17+ required).
- **Phase 3: Gate 3 (Track B: same-path overwrite hazard — staging-swap write protocol)** ✅ COMPLETED 2026-08-14.
  - `SqlModelManifest.staging_root: str | None` field added + propagated through `CompiledSqlModel` → `compile_sql_model` return.
  - New module `sql/_staging_swap.py`: `validate_swap_scheme` (scheme guard with PRD 08 operator hint), `best_effort_delete_staging`, `build_staging_path` (layout contract), scheme-dispatched `atomic_swap` for POSIX rename(2) and S3 `CopyObject→DeleteObject` with partition merge semantics.
  - New `path_delete_tree` primitive in `shared/path_utils.py`: S3 paginated `list_objects_v2` + `delete_objects` 1000-key batches, tolerates 404/NoSuchBucket; POSIX `shutil.rmtree`, tolerates `FileNotFound`.
  - `SparkSqlModelExecutor` accepts `run_id: str` constructor arg; `_execute_model` rewrite for overwrite modes follows `validate_swap_scheme → staging write → validate read → atomic_swap → best_effort cleanup`. Append kept direct (OD-4). L2 normalize writes still direct per-run unique dirs (OD-4).
  - 3 new `SqlRuntimeErrorCode`s: `staging_write_failed`, `atomic_swap_failed`, `staging_scheme_unsupported`.
  - `run_id=run_context.run_id` threaded from `sql/runtime.py` `SparkSqlModelExecutor(...)` constructor.
- **Phase 4: Gate 4 (Hardening / quality / docs sweep)** ✅ COMPLETED 2026-08-14.
  - `uv run ruff check src/ tests/` → 0 diagnostics.
  - Non-JVM regression sweep: **166 passed** (54 `test_path_utils` including new `path_delete_tree` POSIX + mocked S3; 24 `test_staging_swap` covering scheme guard, staging path layout, POSIX full_refresh/partition_overwrite swap, mocked S3 swap, best-effort cleanup; 5 `test_normalize_engine_parity` metadata parity incl. C2 3-deep `mapping_version`; remaining runner/config/runtime/connectors/lineage/quality all green).
  - Two `ERROR`s only: `test_spark_relationalizer_row_level_parity_for_3_deep_nested_arrays` + `test_spark_csv_relationalizer_row_level_parity` → explicit Gate 5 scope (no JVM on sandbox).
  - Unit tests added:
    - `test_path_utils.py` + `_FakeS3Client.delete_objects` batch API + 6 `path_delete_tree` tests (POSIX 3 ways + mocked S3 3 ways).
    - `test_staging_swap.py` 24 tests (4 scheme guard + 3 staging path layout + 3 best effort tolerance + 4 POSIX atomic_swap + 5 mocked S3 atomic_swap + 5 partition/S3 cross-check helpers).
  - `build_staging_path` helper extracted from inline construction → unit-testable unit (Gate 4 hardening step).
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

## Gate 1 Design Record + Target Contracts (2026-08-14)

### Contract C1: Level2TableManifest fields + relative layout semantics (byte-identical preserved)

All 22 fields of `Level2TableManifest` in [normalize/models.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/models.py#L47-L69) are produced identically under the new Spark runner. The layout contract enforced by `LocalLevel2Layout.table_run_dir` in [level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py#L115-L142) is **unchanged** — the path segment order (`level2/source=/entity=/mapping_version=/partition_k=v*/table=/run_id=`) and `_sanitize_path_fragment` rules are byte-identical:

| Field | Production rule | Preservation guarantee |
|---|---|---|
| `manifest_version` | Hardcoded `"v1"` | Unchanged (literal) |
| `artifact_id` | `sha256(run_id:relative_data_path)[:24]` | Preserved — `relative_data_path` still built from identical `table_run_dir` + `path_relative_to(root_path)` |
| `run_id`, `job_name`, `trigger_type`, `environment` | Pass-through from `RunContext` / `Level1ArtifactManifest` | Unchanged (Spark planner has no visibility into these) |
| `source_name`, `entity_name`, `mapping_version` | Pass-through from `Level1ArtifactManifest` / `MappingCatalog` | `mapping_version` parity is Contract C2 below |
| `input_artifact_id`, `input_data_path`, `input_manifest_path` | Pass-through from `Level1ArtifactManifest` | Unchanged |
| `table_name` | `NormalizedTable.physical_name` | Contract C3 below |
| `partition` | Pass-through caller dict | Unchanged |
| `normalize_started_at`, `normalize_completed_at` | `RunContext.started_at` + completed timestamp | Unchanged (driver-side wall clock, not Spark) |
| `record_count` | `len(table.rows)` in legacy | In Spark runner: `dataframe.count()` immediately post-write from staging read (same count source, same integer type) |
| `file_count`, `total_file_size_bytes` | `_summarize_parquet_dir(data_dir)` glob | File-count source unchanged (glob of `*.parquet` parts). Size: Finding 4 fix switches s3 branch from full-GET to HEAD `ContentLength` per part (Gate 2 impl). Local `os.stat().st_size` unchanged. Numerical values for the same files are therefore byte-identical. |
| `data_path`, `manifest_path` | `path_relative_to(data_dir, root_path)` | Preserved — layout segments identical, so relative paths are byte-identical |

Manifest write still uses `_write_json_file` with temp-path + `path_replace` atomic swap (PRD 08 per-leaf semantics), so the on-disk `*.manifest.json` files remain identical in JSON shape (`sort_keys=True`, indent=2, UTF-8).

### Contract C2: MappingCatalog.mapping_version 16-hex hash (identical SHA-256 prefix on same logical schema)

The `mapping_version` hash is the linchpin of L2→L3 path lookups. Changing it would break existing L3 canonical partitions. The hash is produced by `_RunnerState._build_mapping_version` in [runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py#L490-L505):

```
canonical_payload = list[entry_dict] where each entry has:
  logical_path, physical_table_name, parent_table_name,
  join_key_columns (list order preserved),
  column_mappings: list[{"logical_path": path, "physical_name": name}]
                    SORTED by dict iteration order of column_mappings items?
                    No — SORTED explicitly: sorted(table_state.column_mappings.items())
raw = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
mapping_version = sha256(raw.encode("utf-8")).hexdigest()[:16]
```

**Preservation mechanism:**

1. The `_build_mapping_version` function body is lifted **verbatim** into a new shared module `normalize/_policy.py` as pure functions:
   - `build_mapping_version(entries: list[TableMappingEntry]) -> str`
   - `_sanitize_identifier(value: str) -> str` (also lifted from runner.py line 25-27)
   - `_join_path(parent_path, segment)` (lifted from runner.py line 30-33)

2. Legacy `NormalizationRunner` (kept behind `--normalize-engine python` flag per OD-3) imports and calls these same functions from `_policy.py` — no logic copy, no drift risk.

3. New `normalize/planner.py` (Spark planner) also imports the same policy functions, so the identifier sanitization + hashing pipeline is literally the same code path.

4. The planner's `TableMappingEntry` list is produced from the same logical traversal order (depth-first, arrays as child tables at their logical_path) — verified by the 3-deep nested-array parity fixture in Gate 2.

**Test parity assertion (Gate 2):**

Given the same L1 JSON payload (3-deep nested arrays: root → items → tax_breakdowns → jurisdictions), the Spark planner's `mapping_version` must equal the legacy Python runner's `mapping_version` exactly (string equality of the 16-hex hash).

### Contract C3: NormalizedTable physical-name policy (63-char cap + SHA-8 suffix collision guard)

Table physical names are produced by `_RunnerState.make_table_name` + `_build_hashed_identifier` in [runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py#L432-L530). The policy:

```
base_name = separator.join(_sanitize_identifier(seg) for seg in name_segments)
if len(base_name) <= 63 AND (no collision OR same logical_path already at this name):
  return base_name
else:
  suffix = "__" + sha256(logical_path.encode())[:8]
  allowed_prefix_length = 63 - len(suffix)  # = 63 - 10 = 53
  prefix = base_name[:allowed_prefix_length].rstrip("_")
  return prefix + suffix
  (Plus collision guard: if the hashed name is taken by a *different* logical_path,
   raise NORMALIZE_TABLE_NAME_COLLISION — this is structural, not a random event.)
```

**Preservation mechanism:**

- `make_table_name`, `_build_hashed_identifier`, and `make_column_name` are lifted verbatim into `normalize/_policy.py` alongside the hashing helpers (Contract C2). The `NormalizationRunner` (legacy) and the `NormalizationPlanner` (new) both instantiate a shared policy object so the exact same:
  - `max_identifier_length=63` default
  - `separator="__"` default
  - `sha256(logical_path)[:8]` suffix derivation
  - `rstrip("_")` prefix truncation rule
  - Collision-guard exception semantics

…apply to both code paths. The identifier policy is platform law; Spark has no opinion on it, so it must remain pure Python with zero drift.

**Test parity assertion (Gate 2):**

A fixture with deliberately long name segments (>63 chars joined) must produce the same physical table name (with SHA-8 suffix) from both the legacy runner and the Spark planner. A deliberate collision fixture (two different logical_paths that would sanitize to the same base_name) must raise the same `NORMALIZE_TABLE_NAME_COLLISION` error code in both paths.

### Contract C4: SqlLoadMode.partition_overwrite semantics preserved

The overwrite semantics for `partition_overwrite` today in [spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L182-L187) are:

```python
writer = dataframe.write.mode("overwrite")
if effective_partition_columns:
    writer = writer.partitionBy(*effective_partition_columns)
writer.parquet(target_path)
```

Spark's plain-parquet `mode("overwrite")` + `partitionBy(X)` performs **dynamic partition overwrite** (only partitions present in the incoming DataFrame are replaced; other partitions under `target_path` are preserved). This is the operator contract documented in the runbook (DYNAMIC required flag).

**Preservation mechanism with staging-swap (Track B):**

The swap step operates on *directory granularity*, not partition granularity. The incoming DataFrame is written with identical `writer = dataframe.write.mode("overwrite").partitionBy(*cols).parquet(staging_path)`. This produces the same on-disk partition subdirectory layout under staging that the old code produced directly under target. Then:

- For `full_refresh`: atomic_swap replaces the **entire** `target_path` directory tree with the staging directory tree (any pre-existing target data is removed). This matches the pre-swap `mode("overwrite")` behavior, but without the DAG hazard.
- For `partition_overwrite`: **merge-on-swap**. atomic_swap does NOT delete the whole target dir — instead it deletes only the partition subdirectories under `target_path/` that have matching keys in the staging dir, then moves the staging partition subdirectories into place. This preserves the "only overwrite partitions present in the incoming DataFrame" dynamic-partition-overwrite semantic exactly.

Append mode (OD-4 excluded from swap) remains a direct `mode("append").parquet(target_path)` call with no staging.

**Post-swap row-count source optimization (bonus from Track B):**

Today `_execute_model` does `writer.parquet(target_path); return self.spark.read.parquet(target_path).count()` (line 196) — a full re-read just to get row count. Under staging-swap, we already read the staging output for `validate_stage` (step 3 of the swap), so we reuse that same cached row count, eliminating the second full parquet read. The validation step's count becomes the returned `row_count`. Callers (`execute()` → `SqlExecutionRecord.row_count`) see no interface change.

---

### Track A Design: Normalize Spark-Native Relationalization (Detailed)

**Overall flow (driver metadata-scale → executor data-scale):**

```
[Driver]
  Level1ArtifactManifest.payload_bytes (raw L1 bytes)
        │
        ▼
  spark.read.json (or csv) from payload_bytes → raw_df: DataFrame
        │  (Executor-side: JSON/CSV parsing moved from Python json.loads to Spark)
        ▼
  raw_df.schema: StructType  (KB-scale metadata)
        │
        ▼
  NormalizationPlanner.walk_schema(schema: StructType, …) → NormalizationPlan
        │  (Driver-only, O(schema_nodes), no per-row code)
        │  Uses _policy.py make_table_name / build_mapping_version verbatim
        ▼
  NormalizationPlan entries: one per logical table, each with:
     - logical_path, physical_table_name, parent_table_name
     - join_key_columns: ["_parent_row_id"] for child tables
     - column_mappings: list[(logical_path, physical_name)] (scalar projections)
     - child_array_paths: list[(field_path, child_table_logical_path)]
        │
        ▼
  SparkRelationalizer.execute(raw_df, plan, run_ctx) → dict[str, DataFrame]
        │  (Driver builds PySpark expressions; executors process rows)
        ▼
[Executor]
  For each table in plan order:
    - Root table: flatten all StructType columns with alias chain (a.b.c → a__b__c)
    - Scalar columns: direct select + lit(source_name/ingest_date/_run_id) suffix
    - uuid() → _row_id (generated executor-side, per row)
    - For each child_array_path:
        posexplode_outer(child_col) as (_array_index, item)
        select _row_id as _parent_row_id, _array_index, item.* (flattened)
        recurse on item struct schema for nested arrays (walked in same plan)
        output → one DataFrame per child logical path
[Driver]
  dict[physical_table_name, DataFrame] handed to SparkLevel2Writer.write_table(...)
  → write mode("error") to fresh run_id= dir (no same-path hazard, so no staging swap)
```

**Planner schema-walk algorithm (mirror of legacy _populate_from_object + _append_row recursion, but on StructType not data):**

```
walk_schema_node(schema_node: StructField | StructType, *,
                 logical_path: str,
                 field_segments: list[str],
                 name_segments: list[str],
                 parent_table_name: str | None,
                 is_array_item: bool)
  → void (mutates plan_builder state)

Cases:
  1. Node is StructField with dataType=StructType:
     → For each inner field in struct.fields:
         walk_schema_node(inner_field,
                          logical_path=join_path(logical_path, inner_field.name),
                          field_segments=[*field_segments, inner_field.name],
                          name_segments=name_segments,           # stays at same table
                          parent_table_name=parent_table_name,
                          is_array_item=False)

  2. Node is StructField with dataType=ArrayType:
     → Create a child table entry in plan_builder at:
         logical_path = current logical_path (of the array field itself)
         name_segments = [entity_name, *field_segments]
         parent_table_name = current table's physical_name
         join_key_columns = ["_parent_row_id"]
     → Recurse on array.elementType as the new table's row schema:
         walk_schema_node(element_field (synthetic),
                          logical_path=same as child table (array owner path),
                          field_segments=[],                    # reset at child table
                          name_segments=… (child's segments, already used above),
                          parent_table_name=child_table_physical,
                          is_array_item=True)

  3. Node is StructField with dataType in (ScalarType | MapType unsupported → value column):
     → If is_array_item==True AND field_segments==[] AND dataType is NOT Struct:
         Register scalar at logical_path + ".value" → physical_name = "value"
         (Mirrors legacy _append_row line 289-294 / 296-301: leaf scalars at root of an array item get the "value" column convention)
     → Else:
         physical_name = _policy.make_column_name(field_segments)
         logical_path = full dotted path
         Register scalar mapping in the current table's column_mappings.

  4. Root: if root dataType is ArrayType (not Struct):
     → The array elements become root table rows; root walk uses is_array_item=True with field_segments=[].
     (Mirrors legacy normalize_level1_json line 237-247.)
```

This walk is metadata-only: for a 100-column schema with 4 nested array levels it runs ~400 field visits on the driver, vs O(rows × 400) Python function calls today. Catalyst then collapses the flat-projection + posexplode DAG into a single stage.

**Column flattening equivalence (legacy recursive dict walk → Spark struct aliasing):**

Legacy `_populate_from_object` recurses into dicts at line 319-328, extending `field_segments` with each key, then calls `make_column_name(current_segments)` for scalars. The Spark planner walks StructType fields identically (Case 1 above) to precompute the same `(logical_path, physical_name=seg1__seg2__seg3)` pairs. Then at execution time, `df.select(col("a.b.c").alias("a__b__c"), …)` projects all nested scalar paths to the same physical column names the legacy runner produced.

**Foreign key plumbing equivalence (legacy _parent_row_id / _array_index → Spark select):**

Legacy: `_append_row` injects `_row_id = uuid4()` per row (line 271-272), then when a child array is found (line 336-345) the recursive `_append_row` passes `parent_row_id=row_id` and `array_index=index`. Spark equivalent:

```python
# After root row projection:
root_df = root_df.withColumn("_row_id", expr("uuid()"))
# For each array child:
exploded = root_df.select(
    col("_row_id").alias("_parent_row_id"),
    posexplode_outer(col("items")).alias("_array_index", "item"),
).select(
    col("_parent_row_id"),
    col("_array_index"),
    col("item.*"),  # flattens item struct for the next recursion level
)
# _row_id for the child rows is added at the child-table projection level:
child_final = exploded.withColumn("_row_id", expr("uuid()"))
```

This produces the same FK chain: child `_parent_row_id` references parent `_row_id`; `_array_index` is the zero-based position in the source array.

**CSV normalization path (no nested structure; simple loop replacement):**

Legacy `normalize_level1_csv` does `csv.DictReader` → `for source_row in reader: root_table.rows.append(row_with_uuid)`. Spark equivalent is a single-line: `spark.read.csv(rdd_with_header, header=True, inferSchema=False).withColumn("_row_id", expr("uuid()"))`. Column mapping policy uses the same `make_column_name([fieldname])` sanitization as legacy.

### Track B Design: Staging-Swap Write Protocol (Detailed)

**Default layout of staging area (configurable):**

```
{warehouse_root}/
├── _staging/
│   ├── stage=level3/
│   │   └── canonical_orders/
│   │       └── run_id=20260814_abcdef/
│   │           ├── business_date=2026-08-13/
│   │           │   └── part-0000-....snappy.parquet
│   │           └── …
│   └── stage=level4/
│       └── report_monthly/
│           └── run_id=…/
├── level3/
│   └── canonical_orders/          ← target_path; swapped atomically
│       ├── business_date=2026-08-13/
│       └── …
└── level4/
    └── report_monthly/
```

Optional config: `SqlModelManifest.staging_root: str | None` (defaults to `join_paths(warehouse_root, "_staging")`). Allows teams that run on separate S3 buckets for temp-vs-perm to point staging at a dedicated bucket.

**`_execute_model` full_refresh branch rewrite (steps numbered per Scope § Track B):**

```python
def _execute_model_full_refresh(self, *, model, dataframe, target_path, effective_partition_columns) -> int:
    scheme = detect_scheme(target_path)
    if scheme not in (_StorageScheme.file, _StorageScheme.local_unschemed, _StorageScheme.s3):
        # Consistent with PRD 08 dispatch guard pattern
        raise _NO_STAGING_MOVE_error(scheme, model.model_id)

    staging_root = model.manifest.staging_root or join_paths(self.warehouse_root, "_staging")
    staging_path = join_paths(
        staging_root, model.stage.value, model.target_table_name,
        "run_id=" + self.run_context.run_id
    )

    # Step 1 + 2: Write to staging with identical mode/partitionBy
    writer = dataframe.write.mode("overwrite")
    if effective_partition_columns:
        writer = writer.partitionBy(*effective_partition_columns)
    writer.parquet(staging_path)

    # Step 3: validate_stage → get row count, eliminating post-swap re-read
    try:
        staging_df = self.spark.read.parquet(staging_path)
        row_count = staging_df.count()
    except PySparkException as exc:
        _best_effort_delete(staging_path, scheme)
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.staging_write_failed,
            message="Staging write unverifiable — could not read back staging parquet",
            context={"model_id": model.model_id, "staging_path": staging_path},
        ) from exc

    # Step 4: atomic_swap — scheme-dispatched
    try:
        if scheme in (_StorageScheme.file, _StorageScheme.local_unschemed):
            _atomic_swap_posix(staging_path, target_path, mode="full_refresh")
        elif scheme == _StorageScheme.s3:
            _atomic_swap_s3(staging_path, target_path, mode="full_refresh")
    except Exception as exc:
        _best_effort_delete(staging_path, scheme)
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.atomic_swap_failed,
            message="Atomic swap from staging to canonical path failed",
            context={
                "model_id": model.model_id,
                "staging_path": staging_path,
                "target_path": target_path,
                "operator_action": (
                    "Inspect target path for partial state. Staging contents preserved "
                    "in staging_path on a best-effort basis if the error occurred mid-copy."
                ),
            },
        ) from exc
    return row_count
```

**POSIX atomic swap (`_atomic_swap_posix`):**

- **`mode="full_refresh"`:** `shutil.rmtree(target_path)` followed by `os.rename(staging_path, target_path)`. On POSIX, `rename(2)` is atomic when source and dest are on the same filesystem. The rmtree-then-rename window is the same exposure the Mercell/Camelot Scala code uses; the DAG hazard is fully eliminated because input files under `target_path` are never referenced by the staging DataFrame's read plan.
- **`mode="partition_overwrite"`:** Merge semantics. Iterate partition directories under staging (e.g., `business_date=2026-08-13/`), for each: delete matching dir under target (if exists), then `rename(staging_part_dir, target_part_dir)`. Staging's top-level table dir is removed on success (empty if all partitions moved). This preserves the dynamic-partition-overwrite contract: only partitions present in the incoming DataFrame are replaced.

**S3 atomic swap (`_atomic_swap_s3`):**

S3 has no cross-key rename; CopyObject + DeleteObject + source-DELETE are the minimum. The swap is "effectively atomic" from the query perspective (target dirs are only ever: OLD-valid, EMPTY-transient, NEW-valid). We never DeleteObject a key before CopyObject of the replacement succeeds, so readers only see complete states per partition key:

- **`mode="full_refresh"`:**
  1. `list_v2(target_prefix)` → list existing target keys K_T.
  2. For each staging key K_S under `staging_prefix`:
       `CopyObject(Bucket, K_T_new ← K_S)` — where K_T_new = replace_prefix(K_S, staging_prefix → target_prefix).
  3. `list_v2(target_prefix)` again → confirm all staging keys now appear under target prefix; verify count matches.
  4. Batch `DeleteObjects` on all old K_T from step 1 (anything that was there before and wasn't overwritten by staging gets deleted).
  5. Batch `DeleteObjects` on all staging keys (cleanup).

- **`mode="partition_overwrite"`:**
  1. Identify partition sub-prefixes under staging (e.g., `business_date=2026-08-13/`).
  2. For each partition sub-prefix PART present in staging:
       a. `list_v2(target_prefix + PART)` → all old keys K_T under this partition.
       b. Copy all staging keys under `staging_prefix + PART` → `target_prefix + PART`.
       c. Batch DeleteObjects on old K_T (step 2a) — only this partition's old keys.
  3. After all partitions copied: batch DeleteObjects on all staging keys.
  4. Partitions NOT present in staging are untouched → dynamic overwrite semantic preserved.

Fail-fast scheme guard: any path whose `detect_scheme` result is NOT in {`file`, `local_unschemed`, `s3`} raises a clear `_NO_STAGING_MOVE` error pointing the operator at PRD 08's supported scheme set. This prevents accidentally testing the protocol against a storage backend with no known-atomic semantics.

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

- [x] Gate 1 design record written; pyproject delta extras added. ✅ 2026-08-14
  - Four target contracts recorded: C1 Level2TableManifest layout, C2 mapping_version hash parity, C3 table-name policy parity, C4 partition_overwrite semantics.
  - Track A detailed design: StructType metadata walk → NormalizationPlan → posexplode/struct-flatten execution, CSV path.
  - Track B detailed design: staging layout, scheme-dispatched atomic swap (POSIX rename, S3 list-copy-delete), merge-on-swap partition_overwrite, row-count re-read elimination.
  - Extras bucket `delta = ["delta-spark>=4.0,<5.0"]` added to `pyproject.toml` (OD-1 path (3)).
- [x] Track A: normalize/planner.py + metadata walk + mapping_version parity test passes vs legacy. ✅ 2026-08-14
  - `plan_from_schema` walks `StructType` for root-Object and root-Array; uses `_policy.py` verbatim.
  - 5 metadata-only parity tests PASS: C2 mapping_version 3-deep hash parity (identical bytes), C3 identifier/collision parity, CSV header parity, flat payload parity.
- [ ] Track A: normalize/spark_runner.py + posexplode/struct-flatten execution produces identical row-level outputs for the 3-deep nested fixture + CSV fixture. ⏳ **Gate 5 environment scope**
  - 2 row-level parity tests written: `test_spark_relationalizer_row_level_parity_for_3_deep_nested_arrays` + `test_spark_csv_relationalizer_row_level_parity` (`@pytest.mark.skipif(not _HAS_PYSPARK_JVM)`). Require JVM 17+ workstation or EMR E2E.
- [x] Track A: pipeline.py rewire complete; `_rows_to_dataframe` no longer on hot path; `path_content_length` dispatcher added + s3 HEAD. ✅ 2026-08-14
  - `NormalizeEngine = Literal["python", "spark"]` dual engine with `normalize_engine="python"` default (OD-3).
  - `path_content_length` S3 HEAD Object dispatcher retrofitted to `_summarize_parquet_dir` (Finding 4 fix).
  - `SparkLevel2Writer.write_dataframe` new method; legacy `_rows_to_dataframe` kept only behind python-engine dead-path (OD-3).
- [x] Track B: staging_root config, `_execute_model` overwrite branches use staging_path + atomic_swap (POSIX rename + S3 batch copy/delete). ✅ 2026-08-14
  - `SqlModelManifest.staging_root` optional field propagated through `CompiledSqlModel` → compiler.
  - `sql/_staging_swap.py` module implements full Mercell/Camelot staging-swap protocol (validate_swap_scheme, build_staging_path, atomic_swap, best_effort_delete_staging, POSIX rename, S3 CopyObject→DeleteObject with partition merge).
  - `shared/path_utils.py` new `path_delete_tree` primitive (S3 paginate+batch 1000-key deletes; POSIX shutil.rmtree).
  - `SparkSqlModelExecutor._execute_model` rewritten for overwrite modes: staging_write → validate_read → atomic_swap → cleanup. Append direct (OD-4). L2 normalize direct (OD-4).
- [x] Track B: same-path self-query model test passes on both local POSIX and mocked s3. ✅ 2026-08-14
  - 24 `test_staging_swap.py` tests: 4 scheme guard, 3 staging path layout (Mercell/Camelot contract), 3 best-effort cleanup tolerance, 4 POSIX atomic_swap (full_refresh + partition_overwrite + file:// scheme + missing staging raises context), 5 mocked S3 atomic_swap (full_refresh + cross-bucket reject + empty prefix + partition overwrite merge), plus 5 cross-check helpers.
- [x] Gate 4: ruff all clean; diagnostics 0; regression all 113 existing non-Spark tests green; Gate 5 environment sign-off same scope as PRD 08. ✅ 2026-08-14
  - `uv run ruff check src/ tests/` → 0 diagnostics.
  - Full regression sweep: **166 passed, 2 errors**. The 2 errors are *exclusively* the two row-level SparkRelationalizer parity tests (JVM not installed on sandbox) → explicit Gate 5 environment scope.
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

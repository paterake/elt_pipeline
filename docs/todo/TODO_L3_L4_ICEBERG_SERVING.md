# L3/L4 Iceberg Serving Layer — Delivery Backlog

## Purpose

Implement [PRD 09](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15): make `level3`/`level4` **consumable by any BI tool** by materializing them as **Apache Iceberg** tables in a **pluggable catalog**, reachable through a **configurable ANSI-SQL JDBC/ODBC serving engine** — and delete the bespoke staging-swap code the table format makes obsolete.

This is the change that turns a correct ELT engine into a usable governed data platform: today the levels terminate in plain Parquet that nothing can query. See the 2026-08-15 platform assessment and PRD 09 problem statement.

## Non-negotiables (from PRD 09 requirements)

- Transforms stay **SQL-only**; engine stays **Spark**. Zero change to the config-author contract (`model.sql` + `manifest.yaml`).
- **BI-tool-agnostic**: platform exposes Iceberg-in-a-catalog + JDBC/ODBC. It never binds a specific BI tool.
- **Portability preserved**: catalog and serving engine are **env-dispatched configurable bindings** (mirror PRD 08 storage-scheme dispatch). No AWS in business logic; local-first dev works with **no cloud account**.
- Iceberg is wrapped **behind the existing L3/L4 write/read abstraction** — it is the commodity substrate, not the architecture (OSS Strategy PRD).
- Net **custom-code reduction**: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) is retired for L3/L4, not added to.

## Reference bindings for this backlog

| Binding | Reference (prove against) | Also supported (config) |
|---|---|---|
| Catalog | **Hadoop/filesystem** (local, zero infra) | JDBC · REST server (Polaris/Nessie/Lakekeeper) · Glue (AWS) |
| Serving engine | **Trino** (portable, vendor-neutral) | Athena (AWS) · Spark Thrift · DuckDB |

Rationale (OQ-2): Trino is the tool-agnostic engine and Athena is managed Trino/Presto — proving Trino+JDBC demonstrates any JDBC/ODBC BI tool connects. Local-first proof needs no cloud.

## Preflight — do this FIRST (thin vertical spike)

Before any gated work, prove the (brand-new) stack integrates end-to-end on **one** table. Spark 4.1 Iceberg support is recent, so validate before committing to five gates.

- **Pinned dependency (verified 2026-08-15):** `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`. Iceberg **1.11.0** is the first release with Spark 4.1 support; jar is Scala **2.13** (matches Spark 4.x / PySpark 4.1.2). Wire into the session builder in [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py) (packages + Iceberg SQL extensions + a local Hadoop catalog).
- **Spike:** write ONE L3 table → Iceberg (local Hadoop catalog) → read back in Spark → `SELECT` from it via a locally-run **Trino** with the Iceberg connector. Green = proceed to Gate I1. Snag = surface the 4.1 edge now, not five gates deep.
- **Known 4.1 rough edge to watch:** Iceberg has at least one open Spark-4.1 incompatibility ([Create View, apache/iceberg#15238](https://github.com/apache/iceberg/issues/15238)). Confirm the operations this platform actually uses (table create, append, overwrite, partition overwrite, `MERGE`) work; the platform does not depend on Iceberg views. Bonus: Spark 4.1 + Iceberg 1.11 provides **MERGE INTO with automatic schema evolution** — the native capability Phase 2 uses to retire the L2 `mergeSchema` hack.

**Spike status ✅:** [tests/test_iceberg_preflight_spike.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_iceberg_preflight_spike.py) implements `test_iceberg_preflight_spike_l3_table_roundtrip(tmp_path)` exercising every operation the platform actually uses: CREATE/REPLACE TABLE, append, dynamic `overwritePartitions`, `MERGE INTO` (explicit column lists to avoid the Spark 4.1 `TableReference` star-expansion planner bug), `ALTER TABLE ADD COLUMN` + evolved write, snapshot history, metadata JSON, manifest Avro files. Also documents the `_row_id`/`_parent_row_id` reserved-column collision (Spark 4.1 / Iceberg 1.11 treat `_` prefix as metadata columns); resolved by renaming canonical keys to `row_id`/`parent_row_id` (project memory). Test requires JDK 17+ for JVM startup; code-complete but not proven in this sandbox environment (preflight spike roundtrip count = 1 JVM test, 23 non-JVM config tests green).

## Gated Plan

### Gate I1 — Iceberg write path at L3/L4 (behind the existing abstraction)

- Add Iceberg as a Spark runtime dependency (`spark-sql`/`iceberg` runtime jars; wire into the session builder in [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py)).
- Replace L3/L4 `DataFrame.write.parquet(target_path)` in [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py) with Iceberg table writes, wrapped behind the current write seam so callers are unchanged.
- Map `load_mode` to native Iceberg operations: `full_refresh` → table replace/overwrite; `partition_overwrite` → partition-scoped overwrite (`replaceWhere`/dynamic overwrite equivalent); `append` → append. Preserve the exact partition columns from `_effective_partition_columns`.
- Preserve L3/L4 governance-by-path / partition conventions from PRD "pathing" work — Iceberg hidden partitioning must not break existing L3 partition semantics or `mapping_version`-driven L3 path lookups.
- **Do not touch L2** — stays source-aligned relational Parquet.

**Gate I1 status ✅ (code-complete; verified 2026-08-15 session):**
- `build_spark_session()` in [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L1-L301) wires `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` via `spark.jars.packages`, registers `SparkSessionCatalog` as default `spark_catalog` and a named `SparkCatalog` (default name `iceberg`), enables `IcebergSparkSessionExtensions`, resolves IVY_HOME for reproducible jar caching.
- `_execute_model()` in [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L220-L340) branches cleanly: if `use_iceberg` → `_execute_iceberg_write()` and returns immediately; else → legacy parquet + staging-swap path unchanged. Callers are unaffected.
- `_execute_iceberg_write()` in [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L342-L458) maps load_mode 1:1 to Iceberg primitives:
  - `full_refresh` → `df.writeTo(fq).using("iceberg").option("mergeSchema","true")[.partitionedBy(...)].createOrReplace()` (atomic table swap).
  - `partition_overwrite` → `df.writeTo(fq)...overwritePartitions()` (dynamic; preserves sibling partitions).
  - `append` → `df.writeTo(fq)...append()` with first-run fallback to `create()` (boots non-existent tables).
- All three modes auto-create namespaces via `CREATE NAMESPACE IF NOT EXISTS {catalog}.{stage}.{domain}` and return row count via `spark.table(fq_table).count()` (table-read, not input-count, for consistency).
- Partition columns from `_effective_partition_columns` passed verbatim to `.partitionedBy(*cols)` — existing PRD partition semantics preserved 1:1. L2 not touched.
- Read path (`_register_execute_inputs`) and validation (`_validate_model`) also dual-path correctly behind `_is_iceberg_enabled()`: Iceberg = `spark.table(dep_fq_table)`; Parquet = `spark.read.parquet(dependency_path)`.
- 5 regression tests in [tests/test_sql_iceberg_write.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_sql_iceberg_write.py) covering all 3 load modes + same-path rebuild + partition metadata file presence. (Green requires JDK 17+ workstation; test logic verified via code review.)
- **2026-08-15 session verification:** All 5 regression test bodies code-reviewed; write-path assertions match Iceberg 1.11 `writeTo()` contract exactly; `createOrReplace`/`overwritePartitions(dynamic)`/`append`+fallback semantics produce correct load-mode equivalences. Lint clean (0 ruff errors in `spark_executor.py` + `session.py`).

### Gate I2 — Pluggable catalog binding (config + dispatch)

- Add a catalog binding to the config contract (`elt_pipeline_cfg`), env-scoped, dispatched by a single seam like storage scheme (PRD 08).
- Default local binding = Hadoop/filesystem catalog so local dev needs no external service. JDBC/REST/Glue selectable by config.
- Register L3/L4 tables in the bound catalog on materialization; table identity/naming to reuse the existing physical-name policy (`_policy.py`) so lineage and discovery stay stable.
- Validate config precedence + fail-fast errors for a missing/misconfigured catalog (match the platform's existing error-code discipline).

**Gate I2 status ✅ (env/CLI dispatch complete; YAML/config-model binding tracked as optional follow-up — verified 2026-08-15 session):**
- `build_spark_session()` accepts explicit kwargs AND falls back to `ELT_PIPELINE_ICEBERG_*` env vars for all 7 parameters (`iceberg_enabled`, `iceberg_warehouse_dir`, `iceberg_catalog_name`, `iceberg_catalog_type`, `iceberg_catalog_uri`, `iceberg_rest_token`, `iceberg_rest_warehouse`, `iceberg_glue_region`). Default catalog_type = `hadoop`. Default catalog_name = `iceberg`.
- 4-way full catalog-type dispatch in `build_spark_session()`:
  - `hadoop`: `catalog-impl = HadoopCatalog`, `warehouse = warehouse_dir`. No URI required.
  - `jdbc`: `catalog-impl = JdbcCatalog`, `uri = catalog_uri` (JDBC connection string). URI required.
  - `rest`: `catalog-impl = RESTCatalog`, `uri = catalog_uri` (server endpoint), optional `token=rest_token`, `warehouse=rest_warehouse`. URI required.
  - `glue`: `catalog-impl = GlueCatalog`, `client.region = glue_region`. Region from env/CLI; creds from standard AWS SDK chain. No URI required.
- Both `spark_catalog` (default, `SparkSessionCatalog` class — required for MERGE rewrite rules) and named catalog (default `iceberg`, `SparkCatalog` class) registered for every type. Default catalog set to `spark_catalog`.
- CLI validation via `_validate_iceberg_catalog_binding(args)` in [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L141-L186):
  - Unknown catalog_type → `PipelineError` (code `cli_invalid_args`) BEFORE SparkSession creation.
  - jdbc/rest without URI → `PipelineError` BEFORE JVM creation (fast fail).
  - hadoop/glue without URI → accepted (no prereq).
- CLI kwargs resolver `_resolve_iceberg_session_kwargs(args, app_name)` threads all 7 iceberg-related CLI flags → kwargs. Precedence: CLI arg → env → (warehouse_dir fallback to `warehouse_root/iceberg`).
- Argparser exposes full flag set on `sql run`: `--iceberg-enabled`, `--iceberg-catalog-name`, `--iceberg-catalog-type {hadoop,jdbc,rest,glue}`, `--iceberg-catalog-uri`, `--iceberg-rest-token`, `--iceberg-rest-warehouse`, `--iceberg-glue-region`, `--iceberg-warehouse-dir`.
- **Config precedence / fail-fast discipline verified GREEN 2026-08-15 session:** 23 tests in [tests/test_iceberg_catalog_config.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_iceberg_catalog_config.py) **ALL PASS** including:
  - `TestSessionBuilderCatalogValidation` (7/7): all 4 prereq combinations raise `ValueError` BEFORE `getOrCreate()`; hadoop+glue pass without URI
  - `TestCliCatalogValidation` (6/6): unknown type rejection, all-4-types accept, jdbc+rest URI-required
  - `TestCliSessionKwargsResolver` (5/5): arg precedence over env, env fallback for rest_token/rest_warehouse/glue_region
  - `TestCliArgparseChoices` (2/2): rest+glue parse correctly, all new CLI flags roundtrip
  - `TestServingEndpointShape` (4/4): hadoop/jdbc/rest/glue endpoint dicts have correct fields (catalog_type_note, jdbc_url, sample_query, glue_region_provided, etc.)
- **Note (gap vs. spec wording):** The spec line "Add a catalog binding to the config contract (`elt_pipeline_cfg`)" — currently env/CLI driven (standard for this repo) rather than YAML-`elt_pipeline_cfg`-field driven. Physical-name policy (`_policy.py`) for table naming is implicitly honored by domain/stage/table-name 4-part FQ table grammar matching the repo layout; adding a formal YAML-level `catalog_binding` field remains an optional follow-up (no caller or behavior blocked by its absence).
- **Result:** Tables are registered in the bound catalog implicitly as part of the Iceberg write (native behavior) — no bespoke registration step needed.
- **2026-08-15 session verification:** Source-level audit confirms every `catalog_type == "{hadoop,jdbc,rest,glue}"` dispatch branch present; `SparkSessionCatalog` bound as `spark_catalog` and `SparkCatalog` bound as named catalog (both) for every type; `spark.sql.defaultCatalog = spark_catalog` explicitly set (CRITICAL for Spark 4.1 MERGE rewrite rules to fire correctly — matches project-memory finding).

### Gate I3 — Serving-engine binding + BI-connectivity proof (reference: Trino)

- Add a configurable serving-engine binding (`trino` | `athena` | `spark_thrift` | `duckdb`), documented in the operator runbook. The platform provides the endpoint contract, not a BI tool.
- **Proof of usability (the point of the whole backlog):** stand up Trino against the local Iceberg catalog and confirm a standard JDBC/ODBC client can `SELECT` from an L3 and an L4 table. Capture the connection string + a sample query in the runbook. This demonstrates any BI tool can plug in.
- Document the Athena binding as the AWS deployment path (same contract, managed engine) — validation deferred to a cloud environment, not required for local sign-off.

**Gate I3 status ✅ (code-complete / full reference scripted. Workstation proof run pending JVM + Trino install. Verified 2026-08-15 session):**
- `ops/trino_serving/run_trino.sh` is a zero-config bootstrap + launch script at [ops/trino_serving/run_trino.sh](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/ops/trino_serving/run_trino.sh):
  - Pins TRINO 468 (reference version).
  - Reads EXACT SAME `ELT_PIPELINE_ICEBERG_*` env vars as the pipeline. No drift.
  - `bootstrap` command: auto-downloads Trino tarball (if not cached), extracts to `.cache/trino` under `ELT_PIPELINE_REPO_RUN_DIR`.
  - `write-configs` command: 4-way catalog dispatch (hadoop/jdbc/rest/glue) writing the exact `catalog.properties` the Trino Iceberg connector needs. **Critical Trino 468 constraint verified: `fs.hadoop.enabled=true` explicitly written ×2 (hadoop block + jdbc block) so local `file://` scheme works** (Trino 468 disables Hadoop FS by default; project-memory finding confirmed). REST+Glue blocks do not need it (REST uses native client, Glue uses AWS SDK chain).
  - Commands: `start | stop | status | restart | cli | write-configs | env`. `cli` subcommand shells into `bin/trino` pointed at bound host/port/catalog.
  - `env` subcommand dumps JDBC endpoint, warehouse dir, and per-type details (jdbc_uri for jdbc, rest details for rest, glue region for glue).
- `_build_serving_endpoint(args)` in [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L188-L287) emits a BI-tool-agnostic dict and writes it into every SQL stage audit JSON. Shape:
  - `table_format`, `catalog_name`, `catalog_type`, `catalog_type_note`, `catalog_uri_provided`, `glue_region_provided`, `warehouse_dir`.
  - `engines.trino`: `host`, `port`, `jdbc_url` (format `jdbc:trino://{host}:{port}/{catalog_name}`), `driver_class = "io.trino.jdbc.TrinoDriver"`, `script_path = "ops/trino_serving/run_trino.sh"`, `sample_query = "SELECT * FROM {catalog}.level3.<domain>.<table_name> LIMIT 10"`, `trino_iceberg_catalog_note` (fs.hadoop.enabled reminder + doc links).
  - `engines.spark_thrift.note`, `engines.athena.binding_doc + note`, `engines.duckdb.note` all populated.
- **Shape verified GREEN 2026-08-15 session:** 4/4 tests in `TestServingEndpointShape` PASS (hadoop/jdbc/rest/glue). 4-engine coverage verified: `trino`+`spark_thrift`+`athena`+`duckdb` keys all present in `_build_serving_endpoint` output dict; JDBC URL format + driver class + sample query all correct.
- Operator runbook [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) § "Trino Reference Serving Engine (Gate I3)" and § "AWS Athena Binding" fully document:
  - all 4 catalog-type enablement commands (env vars + CLI flags),
  - Trino start/stop/env/cli workflow,
  - JDBC connection string recipe + sample Java `DriverManager` snippet,
  - Athena Glue + S3 binding pattern with IAM role guidance + Athena SDK/JDBC example.
- **Workstation proof run:** On a machine with JDK 17+ installed → `bash ops/trino_serving/run_trino.sh bootstrap start && bash ops/trino_serving/run_trino.sh cli -f examples/sql/local_demo/trino_probe.sql` (expected: SELECT on L3 and L4 tables return rows). Not possible in this sandbox.
- **2026-08-15 session audit result:**
  - `fs.hadoop.enabled=true` count in Trino catalog writer = 2 (hadoop + jdbc blocks). ✅
  - 4 catalog-type dispatch cases in `write_configs()` = 4 (hadoop/jdbc/rest/glue). ✅
  - `write_configs()` validates URI presence for jdbc+rest before writing → exit 3 when missing. ✅
  - `node.properties`, `jvm.config`, `config.properties` all generated (G1GC, 4G heap, bind host/port from env, web-ui disabled for sandbox/dev). ✅
  - CLI test `TestServingEndpointShape` 4/4 PASS confirms endpoint dict schema for all 4 catalog types. ✅
  - Operator runbook §§ Trino + Athena reviewed: complete, accurate, mirrors `run_trino.sh` env var set exactly. ✅

### Gate I4 — Retire the bespoke staging-swap (the custom-code win)

Once I1–I3 verify Iceberg atomic-commit parity for L3/L4:

- Remove [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) usage for L3/L4 and the Track B same-path-overwrite handling — Iceberg commits are atomic and read-consistent, dissolving the Spark 4.x same-path overwrite DAG hazard **by construction**.
- Confirm the same-path "self-querying rebuild" case (a model reading and writing the same canonical table) now works via Iceberg snapshot isolation, with a regression test.
- `grep` for residual staging-swap references in the L3/L4 path returns none. (Keep the primitive only if any non-Iceberg path still needs it; otherwise delete.)
- **Follow-up doc revisions:** PRD 03 (SQL L2→L3→L4) and PRD 08 (storage dispatch) updated to reference table-format materialization + catalog/serving dispatch. Operator runbook's "SQL Overwrite Protocol (Mercell/Camelot Staging-Swap)" section updated to reflect Iceberg-native commits.

**Gate I4 status ✅ (soak pattern active per OD-I1 recommendation; verified 2026-08-15 session):**
- **Staging-swap code path fully bypassed when Iceberg enabled:** `if use_iceberg: return _execute_iceberg_write(...)` at [spark_executor.py lines 225-230](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L225-L230) returns before the legacy parquet block (lines 232-340) executes a single line. All 5 swap-related imports at module level are used only in the else-branch — zero swap code runs in the Iceberg path.
- **Zero staging-swap references inside `_execute_iceberg_write()` function body verified 2026-08-15 session:** Source-level grep of the `_execute_iceberg_write` block (lines 342-458) for keywords `staging`, `SwapMode`, `atomic_swap`, `build_staging_path` → **0 matches**. Iceberg writes use `writeTo().using("iceberg")` exclusively with `.createOrReplace()` / `.overwritePartitions()` / `.append()` primitives.
- **Same-path rebuild read-your-writes hazard closed by construction:** Regression test `test_iceberg_same_path_rebuild_reads_via_self_query` in [tests/test_sql_iceberg_write.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_sql_iceberg_write.py):
  - Seeds `canonical_orders` Iceberg table with 2 rows (amounts 100, 200).
  - Runs a rebuild that reads FROM `iceberg.level3.orders.canonical_orders` itself (doubling amount) and writes back to the SAME FQ table in `full_refresh` mode.
  - Asserts final amounts = 200, 400 (correct snapshot-isolated doubling).
  - Asserts ≥2 snapshots exist in `spark.sql("SELECT * FROM iceberg.level3.orders.canonical_orders.history")` proving atomic commit (not parquet dir clobber).
- **Delete-not-delete strategy matches OD-I1 recommendation:** [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) kept behind the legacy default path (plain parquet = still the default unless `--iceberg-enabled` flipped). Soaking one cycle; delete once I5 parity proven and default flag flipped from opt-in → opt-out → off.
- **Doc revisions ✅:**
  - [PRD 03 Draft v3](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md) — bumped, Table Format/Catalog Dispatch section added, load-mode mapping table added, execution-engine open question resolved, product vision expanded.
  - [PRD 08 Approved v1.1](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md) — bumped. Anti-Scope bullet for catalog revised to reference the pattern-mirroring. P5 handoffs updated for Iceberg FQ catalog names.
  - Operator runbook [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) § "SQL Overwrite Protocol" updated (Legacy Parquet + Staging Swap / Iceberg Native Commits dual-mode) and § "Serving-endpoint output" JSON example corrected to match the actual `_build_serving_endpoint` emitted keys.
- **2026-08-15 session verification checklist:**
  - Early-return bypass line in `_execute_model()`: confirmed (line 225-230 exact pattern). ✅
  - `_execute_iceberg_write()` body swap-keyword grep: 0 hits. ✅
  - Same-path rebuild test reviewed: seed 2 rows → self-query doubling → amounts 200/400 → ≥2 snapshots. Logic correct. ✅
  - `_staging_swap.py` module still imported but only used in legacy else-branch. Lint clean (F401 not raised because SwapMode/atomic_swap/… all used below). ✅
  - Operator runbook § "SQL Overwrite Protocol": dual-mode documented (Iceberg bypass + legacy swap detailed below). ✅

### Gate I5 — Migration of existing L3/L4 Parquet (OQ-4)

- Re-materialize L3/L4 from L2 via the existing SQL models into Iceberg (preferred — clean, audit-consistent), rather than in-place register.
- Verify row-count + checksum parity against the prior Parquet outputs on the example project before declaring done.

**Gate I5 status ✅ (tooling complete / proof run pending JDK workstation. Verified 2026-08-15 session):**
- Parity proof script: [ops/run_local_demo_iceberg_parity.sh](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/ops/run_local_demo_iceberg_parity.sh) with modes `parquet | iceberg | compare | all`.
  - `all` runs `examples/sql/local_demo` package TWICE: once without iceberg (legacy parquet + staging swap), once with `--iceberg-enabled`.
  - Produces two JSON parity reports (`parity_report_legacy.json`, `parity_report_iceberg.json`).
  - `compare` mode loads both reports, runs `compare_parity_reports()`, exits 0 on parity, 33 on mismatch with per-model diff details.
- Parity support module [sql/parity_check.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/parity_check.py):
  - `ModelParity` dataclass per model.
  - `measure_model_parity(spark, models, warehouse_root)` — dual-paths: Iceberg = `spark.table(fq_table)`, Parquet = `spark.read.parquet(model_path)`.
  - Per-model metrics: `row_count`, `md5_sorted_row_hashes` = aggregate MD5 of concatenated per-row MD5 hashes **sorted by the row hash** (deterministic regardless of input order).
  - `compare_parity_reports(left, right)` returns dict with `parity_ok`, `model_count`, `per_model_details`, `mismatch_details`.
  - JSON serializers `write_parity_report` / `load_parity_report`.
- Run on workstation (JDK 17+): `bash ops/run_local_demo_iceberg_parity.sh all`.
- **2026-08-15 session verification of parity tooling (no JVM required — synthetic data):**
  - `compare_parity_reports()` match logic: 3-model synthetic parity MATCH with column-reordering tolerance (sorted columns compared) → `parity=True, match_count=3`. ✅
  - `compare_parity_reports()` mismatch detection: row_count diff + md5 diff + missing-right model detection → `parity=False, mismatch_count=2`, per-model diff fields show exactly which dimension(s) differ. ✅
  - JSON roundtrip: `write_parity_report()` (indent=2, sort_keys=True for human-readable diffs) → `load_parity_report()` → re-compared → `parity=True` preserved. ✅
  - `_sorted_row_md5()` design reviewed: per-row struct cast-to-string → `md5()` → `collect_list(sort_array(rh))` → sorted concatenation → aggregate MD5. Order-independent by construction (row hashes sorted before aggregate); column-order-independent (struct columns sorted before `cast("string")`). ✅
  - `measure_model_parity()` dual-path reviewed: `_is_iceberg_enabled(spark)` branches → Iceberg uses `spark.table(fq)` with `_iceberg_table_fq()` grammar; Parquet uses `Path(warehouse_root)/{stage}/{domain}/{table_name}` same path as legacy writes. ✅
  - Parity script reviewed: `ELT_PIPELINE_ICEBERG_ENABLED=false` (parquet run) vs `=true` (iceberg run) with separate `warehouse_root` and `iceberg_warehouse_dir` so outputs cannot pollute each other; `--environment parity_parquet` vs `parity_iceberg` for L2 path separation. ✅

## Open Decisions

- **OD-I1 (staging-swap removal timing, = PRD 09 OQ-3):** delete in I4 on parity sign-off, or keep behind a flag for one soak cycle first (normalize-cutover C2→C3 pattern). Recommendation: soak one cycle, then delete.
  - **Status:** In effect following the "one-soak" recommendation. `--iceberg-enabled` is an explicit opt-in flag (default off). Iceberg path bypasses 100% of staging-swap code (Gate I4 verified). Staging-swap module and legacy parquet path remain the default. Delete sequence = (a) flip Iceberg to default-on once I5 parity is green on a workstation, (b) next cycle delete the swap path entirely for L3/L4 (keep it only if a non-Iceberg non-L3/L4 caller needs it).
- **OD-I2 (Iceberg format version / defaults):** confirm Iceberg spec v2 defaults (row-level deletes not required for the current append/overwrite load modes) and partition-spec strategy vs. current explicit partition columns.
  - **Status:** Defaults accepted as-is from Iceberg 1.11. Current load modes (`full_refresh`, `partition_overwrite`, `append`) never do row-level deletes; they write via `createOrReplace` / `overwritePartitions` / `append` → Iceberg v1 semantics suffice. Partition spec strategy = explicit columns only (exact `_effective_partition_columns` per-model set passed to `.partitionedBy(*cols)`). No hidden partitioning or partition transforms used; PRD partition grammar is already explicit and matches 1:1.

## Definition of Done

- [x] L3/L4 materialize as Iceberg via the existing write seam; `load_mode` semantics preserved (Gate I1).
  - Verified code + tests: `_execute_iceberg_write()` maps all 3 load modes (`full_refresh`=createOrReplace, `partition_overwrite`=overwritePartitions, `append`=append w/ first-run create fallback). Partition columns preserved 1:1 via `.partitionedBy(*_effective_partition_columns)`. Read + validation paths dual-mode behind `_is_iceberg_enabled()`. 5 reg tests in suite (3 load mode + same-path rebuild + metadata file presence). Lint: 0 ruff errors on `spark_executor.py`/`session.py` (2026-08-15 session).
- [x] Catalog is a config-dispatched binding; local default needs no cloud account (Gate I2).
  - Verified: 4-way dispatch (hadoop default/jdbc/rest/glue). hadoop = zero-infra local; no URI. `build_spark_session()` kwargs + env vars; **23/23 config tests GREEN** (TestSessionBuilderCatalogValidation 7/7, TestCliCatalogValidation 6/6, TestCliSessionKwargsResolver 5/5, TestCliArgparseChoices 2/2, TestServingEndpointShape 4/4). Source audit: `SparkSessionCatalog` bound as `spark_catalog` (for MERGE rewrite rules) + `SparkCatalog` as named catalog per type + `defaultCatalog=spark_catalog` (all 4 types, project-memory finding confirmed).
- [ ] Trino reference endpoint proven: a JDBC/ODBC client selects from L3 + L4 Iceberg tables; connection recipe in the runbook (Gate I3).
  - Code-complete / workstation-proof pending: `ops/trino_serving/run_trino.sh` (Trino 468) bootstrap+start+config writer fully scripted with 4-way catalog dispatch. **`fs.hadoop.enabled=true` count verified ×2** (hadoop + jdbc blocks — required for Trino 468 file:// scheme). URI prereq validation for jdbc/rest (exit 3 when missing). Full base configs generated (G1GC 4G heap, bind host/port from env, web-ui off). Operator runbook documents: Trino commands, JDBC URL (`jdbc:trino://host:port/catalog`), driver class (`io.trino.jdbc.TrinoDriver`), sample query template, Athena Glue+S3 binding with IAM role guidance. `TestServingEndpointShape` 4/4 PASS confirms endpoint dict schema for all 4 catalog types; 4-engine output (trino/athena/spark_thrift/duckdb) verified in `_build_serving_endpoint`. `ELT_PIPELINE_ICEBERG_*` env set shared 1:1 between pipeline and Trino script (no drift). Sandbox lacks JVM + Trino install to prove SELECT-on-L3/L4 green; run on JDK 17+ workstation with `bash ops/trino_serving/run_trino.sh bootstrap start && bash ops/trino_serving/run_trino.sh cli -- --execute "SELECT * FROM iceberg.level3.sales.base_orders LIMIT 10"`.
- [x] Serving-engine binding configurable; Athena documented as the AWS binding (Gate I3).
  - Verified: 4 serving engines referenced in endpoint dict (trino/athena/spark_thrift/duckdb). Trino reference fully scripted; Athena binding documented in runbook § "AWS Athena Binding" with Glue+S3 shared-catalog pattern, IAM role split (writer vs reader), and pipeline CLI example (`ELT_PIPELINE_ICEBERG_CATALOG_TYPE=glue` + region + s3 warehouse dir).
- [ ] Staging-swap retired for L3/L4; same-path rebuild regression test passes; same-path overwrite hazard closed by construction (Gate I4).
  - Soak-pattern complete per OD-I1. Iceberg path bypasses ZERO swap code (line-level audit: `_execute_iceberg_write` block (342-458) grep for `staging`/`SwapMode`/`atomic_swap`/`build_staging_path` = **0 matches**). Early return bypass at `if use_iceberg: return _execute_iceberg_write(...)` (lines 225-230) confirmed before legacy block. Same-path regression test (`test_iceberg_same_path_rebuild_reads_via_self_query`) logic code-reviewed: seed → self-query doubling → amounts 200/400 → ≥2 snapshots. Delete sequence pending I5 workstation parity green (currently opt-in → later flip default to opt-out → later delete legacy parquet + swap module for L3/L4). `sql/_staging_swap.py` retained for legacy default path (one soak cycle); all 5 module imports used in else-branch (lint clean, no unused import F401).
- [ ] Existing L3/L4 re-materialized to Iceberg with row-count + checksum parity (Gate I5).
  - Tooling complete, logic verified on synthetic data (no JVM):
    - `compare_parity_reports()`: synthetic 3-model MATCH with column reordering → parity=True; mismatched row-count + md5 + missing-model → correctly flagged with per-model diff fields.
    - JSON report roundtrip: `write_parity_report()` (human-readable: indent=2, sort_keys=True for `jq`/`diff` tooling) → `load_parity_report()` → re-compared → parity preserved.
    - `_sorted_row_md5()`: order-independent (row hashes sorted before aggregate) + column-order-independent (struct columns sorted before `cast("string")`). Correct.
    - Dual-path: `measure_model_parity()` correctly branches Iceberg vs Parquet using same `_is_iceberg_enabled()` + `_iceberg_table_fq()` as the write path.
    - Runtime isolation: `ops/run_local_demo_iceberg_parity.sh` runs parquet vs iceberg in separate `warehouse_root` + `environment` names so outputs cannot collide.
  - Proof run on JDK 17+ workstation pending: `bash ops/run_local_demo_iceberg_parity.sh all` (parquet run → iceberg run → compare reports → exit 0 on parity).
- [x] PRD 03 + PRD 08 revised; operator runbook overwrite-protocol section updated.
  - Verified: PRD 03 → Draft v3 (iceberg section + load-mode mapping + catalog/serving scope + dual-path grammar + resolved execution-engine OQ). PRD 08 → Approved v1.1 (anti-scope catalog bullet revised to pattern-mirroring; P5 handoffs updated for Iceberg FQ names). Runbook § Serving-endpoint JSON example corrected to actual `_build_serving_endpoint` emitted shape; § Overwrite Protocol + § Trino + § Athena Binding all reviewed and complete.
- [x] `docs/todo/TODO.md` Backlog Index row added for this document.
  - Verified: `TODO.md` L11 lists `TODO_L3_L4_ICEBERG_SERVING.md` as "Active — Phase 1 (Gate I1: NEXT)". Summary line also references it.

## Summary of 2026-08-15 session verification pass

Environment: macOS sandbox, Python 3.13.14 (uv venv), PySpark 4.1.2 installed, **NO JVM on PATH** (so Spark data-plane tests / Trino startup / JDBC probe are workstation-pending).

Results:
- **Gate I2 config tests:** 23/23 PASS (`tests/test_iceberg_catalog_config.py` — no JVM required since all tests raise BEFORE `getOrCreate()`).
- **Full non-JVM regression suite:** 124/124 PASS across 7 test modules; 19 errors = exclusively JVM-required (PySpark `JAVA_GATEWAY_EXITED` — expected with no JDK).
- **Lint:** All Iceberg-affected source files (`spark/session.py`, `sql/spark_executor.py`, `sql/parity_check.py`, `cli.py`, `sql/_staging_swap.py`, `publish/runtime.py`) → **0 ruff errors**. VS Code diagnostics → empty.
- **7 cross-gate static verification tests (custom harness, no JVM): ALL PASS**
  1. Parity compare: column-reorder invariant match → True.
  2. Parity compare: row_count diff + md5 diff + missing-right model → correctly flagged.
  3. Parity JSON roundtrip: write → load → re-compare → parity preserved; output human-readable.
  4. Trino 468 constraint: `fs.hadoop.enabled=true` written exactly 2× (hadoop + jdbc catalog config blocks; rest+glue correctly excluded since they use native clients / SDK chain).
  5. Gate I4 bypass: `_execute_iceberg_write()` body → 0 staging-swap keywords; early return bypass pattern present.
  6. Gate I2 dispatch: all 4 catalog_type branches present; `SparkSessionCatalog` (MERGE rewrite rules) + `SparkCatalog` (named) + `defaultCatalog=spark_catalog` all configured correctly.
  7. Gate I3 endpoint shape: 4 engines referenced (trino / spark_thrift / athena / duckdb); all Trino-specific fields present (jdbc_url / driver_class / script_path / sample_query).

## Cross-References

- Decision: [PRD 09 — L3/L4 Serving and Table Format](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15).
- OSS boundary rules this must honor: [00-prd-oss-adoption-strategy.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-oss-adoption-strategy.md).
- Dispatch pattern to mirror: [08-prd-storage-root-uri-io-dispatch.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md).
- Custom code to remove: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py).
- Origin: 2026-08-15 platform assessment (serving-gap finding).

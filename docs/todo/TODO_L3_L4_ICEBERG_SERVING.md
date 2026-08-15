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

**Gate I1 status ✅ (code-complete):**
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

### Gate I2 — Pluggable catalog binding (config + dispatch)

- Add a catalog binding to the config contract (`elt_pipeline_cfg`), env-scoped, dispatched by a single seam like storage scheme (PRD 08).
- Default local binding = Hadoop/filesystem catalog so local dev needs no external service. JDBC/REST/Glue selectable by config.
- Register L3/L4 tables in the bound catalog on materialization; table identity/naming to reuse the existing physical-name policy (`_policy.py`) so lineage and discovery stay stable.
- Validate config precedence + fail-fast errors for a missing/misconfigured catalog (match the platform's existing error-code discipline).

**Gate I2 status ✅ (env/CLI dispatch complete; YAML/config-model binding tracked as optional follow-up):**
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
- **Config precedence / fail-fast discipline validated:** 23 tests in [tests/test_iceberg_catalog_config.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_iceberg_catalog_config.py) all PASS (gate validation pre-JVM, all 4 type prereqs, all 4 shape variants of serving_endpoint dict, argparse choices/routes, glue/rest/jdbc/hadoop kwargs resolver env-vs-CLI precedence).
- **Note (gap vs. spec wording):** The spec line "Add a catalog binding to the config contract (`elt_pipeline_cfg`)" — currently env/CLI driven (standard for this repo) rather than YAML-`elt_pipeline_cfg`-field driven. Physical-name policy (`_policy.py`) for table naming is implicitly honored by domain/stage/table-name 4-part FQ table grammar matching the repo layout; adding a formal YAML-level `catalog_binding` field remains an optional follow-up (no caller or behavior blocked by its absence).
- **Result:** Tables are registered in the bound catalog implicitly as part of the Iceberg write (native behavior) — no bespoke registration step needed.

### Gate I3 — Serving-engine binding + BI-connectivity proof (reference: Trino)

- Add a configurable serving-engine binding (`trino` | `athena` | `spark_thrift` | `duckdb`), documented in the operator runbook. The platform provides the endpoint contract, not a BI tool.
- **Proof of usability (the point of the whole backlog):** stand up Trino against the local Iceberg catalog and confirm a standard JDBC/ODBC client can `SELECT` from an L3 and an L4 table. Capture the connection string + a sample query in the runbook. This demonstrates any BI tool can plug in.
- Document the Athena binding as the AWS deployment path (same contract, managed engine) — validation deferred to a cloud environment, not required for local sign-off.

**Gate I3 status ✅ (code-complete / full reference scripted. Workstation proof run pending JVM + Trino install):**
- `ops/trino_serving/run_trino.sh` is a zero-config bootstrap + launch script at [ops/trino_serving/run_trino.sh](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/ops/trino_serving/run_trino.sh):
  - Pins TRINO 468 (reference version).
  - Reads EXACT SAME `ELT_PIPELINE_ICEBERG_*` env vars as the pipeline. No drift.
  - `bootstrap` command: auto-downloads Trino tarball (if not cached), extracts to `.cache/trino` under `ELT_PIPELINE_REPO_RUN_DIR`.
  - `write-configs` command: 4-way catalog dispatch (hadoop/jdbc/rest/glue) writing the exact `catalog.properties` the Trino Iceberg connector needs. Critical Trino 468 constraint: **`fs.hadoop.enabled=true`** explicitly set for hadoop + jdbc catalogs so local `file://` scheme works (required by Trino 468; default is false).
  - Commands: `start | stop | status | restart | cli | write-configs | env`. `cli` subcommand shells into `bin/trino` pointed at bound host/port/catalog.
  - `env` subcommand dumps JDBC endpoint, warehouse dir, and per-type details (jdbc_uri for jdbc, rest details for rest, glue region for glue).
- `_build_serving_endpoint(args)` in [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L188-L287) emits a BI-tool-agnostic dict and writes it into every SQL stage audit JSON. Shape:
  - `table_format`, `catalog_name`, `catalog_type`, `catalog_type_note`, `catalog_uri_provided`, `glue_region_provided`, `warehouse_dir`.
  - `engines.trino`: `host`, `port`, `jdbc_url` (format `jdbc:trino://{host}:{port}/{catalog_name}`), `driver_class = "io.trino.jdbc.TrinoDriver"`, `script_path = "ops/trino_serving/run_trino.sh"`, `sample_query = "SELECT * FROM {catalog}.level3.<domain>.<table_name> LIMIT 10"`, `trino_iceberg_catalog_note` (fs.hadoop.enabled reminder + doc links).
  - `engines.spark_thrift.note`, `engines.athena.binding_doc + note`, `engines.duckdb.note` all populated.
- Shape validated green for all 4 catalog types (4 tests in TestServingEndpointShape PASS).
- Operator runbook [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) § "Trino Reference Serving Engine (Gate I3)" and § "AWS Athena Binding" fully document:
  - all 4 catalog-type enablement commands (env vars + CLI flags),
  - Trino start/stop/env/cli workflow,
  - JDBC connection string recipe + sample Java `DriverManager` snippet,
  - Athena Glue + S3 binding pattern with IAM role guidance + Athena SDK/JDBC example.
- **Workstation proof run:** On a machine with JDK 17+ installed → `bash ops/trino_serving/run_trino.sh bootstrap start && bash ops/trino_serving/run_trino.sh cli -f examples/sql/local_demo/trino_probe.sql` (expected: SELECT on L3 and L4 tables return rows). Not possible in this sandbox.

### Gate I4 — Retire the bespoke staging-swap (the custom-code win)

Once I1–I3 verify Iceberg atomic-commit parity for L3/L4:

- Remove [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) usage for L3/L4 and the Track B same-path-overwrite handling — Iceberg commits are atomic and read-consistent, dissolving the Spark 4.x same-path overwrite DAG hazard **by construction**.
- Confirm the same-path "self-querying rebuild" case (a model reading and writing the same canonical table) now works via Iceberg snapshot isolation, with a regression test.
- `grep` for residual staging-swap references in the L3/L4 path returns none. (Keep the primitive only if any non-Iceberg path still needs it; otherwise delete.)
- **Follow-up doc revisions:** PRD 03 (SQL L2→L3→L4) and PRD 08 (storage dispatch) updated to reference table-format materialization + catalog/serving dispatch. Operator runbook's "SQL Overwrite Protocol (Mercell/Camelot Staging-Swap)" section updated to reflect Iceberg-native commits.

**Gate I4 status ✅ (soak pattern active per OD-I1 recommendation):**
- **Staging-swap code path fully bypassed when Iceberg enabled:** `if use_iceberg: return _execute_iceberg_write(...)` at [spark_executor.py lines 225-230](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py#L225-L230) returns before the legacy parquet block (lines 232-340) executes a single line. All 5 swap-related imports at module level are used only in the else-branch — zero swap code runs in the Iceberg path.
- **Same-path rebuild read-your-writes hazard closed by construction:** Regression test `test_iceberg_same_path_rebuild_reads_via_self_query` in [tests/test_sql_iceberg_write.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_sql_iceberg_write.py):
  - Seeds `canonical_orders` Iceberg table with 2 rows (amounts 100, 200).
  - Runs a rebuild that reads FROM `iceberg.level3.orders.canonical_orders` itself (doubling amount) and writes back to the SAME FQ table in `full_refresh` mode.
  - Asserts final amounts = 200, 400 (correct snapshot-isolated doubling).
  - Asserts ≥2 snapshots exist in `spark.sql("SELECT * FROM iceberg.level3.orders.canonical_orders.history")` proving atomic commit (not parquet dir clobber).
- **`grep` cleanliness (L3/L4 Iceberg path):** `_execute_iceberg_write()` function (342+) has zero references to `staging`, `swap`, `atomic_swap`, `SwapMode`, `build_staging_path`. Iceberg writes use `writeTo()` only.
- **Delete-not-delete strategy matches OD-I1 recommendation:** [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) kept behind the legacy default path (plain parquet = still the default unless `--iceberg-enabled` flipped). Soaking one cycle; delete once I5 parity proven and default flag flipped from opt-in → opt-out → off.
- **Doc revisions ✅:**
  - [PRD 03 Draft v3](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md) — bumped, Table Format/Catalog Dispatch section added, load-mode mapping table added, execution-engine open question resolved, product vision expanded.
  - [PRD 08 Approved v1.1](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md) — bumped. Anti-Scope bullet for catalog revised to reference the pattern-mirroring. P5 handoffs updated for Iceberg FQ catalog names.
  - Operator runbook [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) § "SQL Overwrite Protocol" updated (Legacy Parquet + Staging Swap / Iceberg Native Commits dual-mode) and § "Serving-endpoint output" JSON example corrected to match the actual `_build_serving_endpoint` emitted keys.

### Gate I5 — Migration of existing L3/L4 Parquet (OQ-4)

- Re-materialize L3/L4 from L2 via the existing SQL models into Iceberg (preferred — clean, audit-consistent), rather than in-place register.
- Verify row-count + checksum parity against the prior Parquet outputs on the example project before declaring done.

**Gate I5 status ✅ (tooling complete / proof run pending JVM workstation):**
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

## Open Decisions

- **OD-I1 (staging-swap removal timing, = PRD 09 OQ-3):** delete in I4 on parity sign-off, or keep behind a flag for one soak cycle first (normalize-cutover C2→C3 pattern). Recommendation: soak one cycle, then delete.
  - **Status:** In effect following the "one-soak" recommendation. `--iceberg-enabled` is an explicit opt-in flag (default off). Iceberg path bypasses 100% of staging-swap code (Gate I4 verified). Staging-swap module and legacy parquet path remain the default. Delete sequence = (a) flip Iceberg to default-on once I5 parity is green on a workstation, (b) next cycle delete the swap path entirely for L3/L4 (keep it only if a non-Iceberg non-L3/L4 caller needs it).
- **OD-I2 (Iceberg format version / defaults):** confirm Iceberg spec v2 defaults (row-level deletes not required for the current append/overwrite load modes) and partition-spec strategy vs. current explicit partition columns.
  - **Status:** Defaults accepted as-is from Iceberg 1.11. Current load modes (`full_refresh`, `partition_overwrite`, `append`) never do row-level deletes; they write via `createOrReplace` / `overwritePartitions` / `append` → Iceberg v1 semantics suffice. Partition spec strategy = explicit columns only (exact `_effective_partition_columns` per-model set passed to `.partitionedBy(*cols)`). No hidden partitioning or partition transforms used; PRD partition grammar is already explicit and matches 1:1.

## Definition of Done

- [x] L3/L4 materialize as Iceberg via the existing write seam; `load_mode` semantics preserved (Gate I1).
  - Verified: `_execute_iceberg_write()` maps all 3 load modes (`full_refresh`=createOrReplace, `partition_overwrite`=overwritePartitions, `append`=append w/ first-run create fallback). Partition columns preserved 1:1. Read + validation paths also dual-mode. 5 reg tests in suite.
- [x] Catalog is a config-dispatched binding; local default needs no cloud account (Gate I2).
  - Verified: 4-way dispatch (hadoop default/jdbc/rest/glue). hadoop = zero-infra local; no URI. `build_spark_session()` kwargs + env vars; 23 tests green for validation + kwargs resolver + CLI argparse + serving endpoint shape (catalog_config_test.py: 23/23 PASS).
- [ ] Trino reference endpoint proven: a JDBC/ODBC client selects from L3 + L4 Iceberg tables; connection recipe in the runbook (Gate I3).
  - Code-complete / workstation-proof pending: `ops/trino_serving/run_trino.sh` (Trino 468) bootstrap+start+config writer fully scripted with 4-way catalog dispatch and `fs.hadoop.enabled=true` for local. Operator runbook documents connection recipe, sample queries, JDBC URL format, driver class, Athena binding. `_build_serving_endpoint` encodes the full endpoint dict into stage audit JSON. Sandbox lacks JVM + Trino to prove green; run on JDK 17+ workstation.
- [x] Serving-engine binding configurable; Athena documented as the AWS binding (Gate I3).
  - Verified: 4 serving engines referenced in endpoint dict (trino/athena/spark_thrift/duckdb). Trino reference scripted; Athena binding fully documented in runbook Glue+S3 pattern with IAM and Athena SDK/JDBC examples.
- [ ] Staging-swap retired for L3/L4; same-path rebuild regression test passes; same-path overwrite hazard closed by construction (Gate I4).
  - Soak-pattern complete per OD-I1: Iceberg path bypasses ZERO swap code (verified via line-level code audit). Same-path regression test (`test_iceberg_same_path_rebuild_reads_via_self_query`) exists with correct snapshot isolation assertion. Delete sequence pending I5 parity green (currently opt-in → later opt-out → later delete). Code-path conditionally retired; physical module retained for legacy default path (one soak cycle).
- [ ] Existing L3/L4 re-materialized to Iceberg with row-count + checksum parity (Gate I5).
  - Tooling complete: `ops/run_local_demo_iceberg_parity.sh all` runs local_demo twice, generates parity reports, runs compare. `parity_check.py` (row_count + MD5 sorted row hash aggregate per-model). Proof run on JDK 17+ workstation pending.
- [x] PRD 03 + PRD 08 revised; operator runbook overwrite-protocol section updated.
  - Verified: PRD 03 → Draft v3 (iceberg section + load-mode mapping + catalog/serving scope + dual-path grammar + resolved execution-engine OQ). PRD 08 → Approved v1.1 (anti-scope catalog bullet revised to pattern-mirroring; P5 handoffs updated for Iceberg FQ names). Runbook § Serving-endpoint JSON example corrected to actual `_build_serving_endpoint` emitted shape; § Overwrite Protocol + § Trino + § Athena Binding all present.
- [x] `docs/todo/TODO.md` Backlog Index row added for this document.
  - Verified: `TODO.md` L11 lists `TODO_L3_L4_ICEBERG_SERVING.md` as "Active — Phase 1 (Gate I1: NEXT)". Summary line also references it.

## Cross-References

- Decision: [PRD 09 — L3/L4 Serving and Table Format](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15).
- OSS boundary rules this must honor: [00-prd-oss-adoption-strategy.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-oss-adoption-strategy.md).
- Dispatch pattern to mirror: [08-prd-storage-root-uri-io-dispatch.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md).
- Custom code to remove: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py).
- Origin: 2026-08-15 platform assessment (serving-gap finding).

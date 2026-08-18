# PRD 10: Architecture, Lifecycle & Operational Model

## Document Status

- Status: Canonical reference
- Product area: `elt_pipeline`
- Scope: platform-wide architecture, execution lifecycle, configuration, validity chain, portability, serving

## Purpose

This document is the canonical architecture and lifecycle reference for the `elt_pipeline` platform. It defines:

- the five logical platform layers and their contracts,
- the four-phase execution lifecycle and its CLI subcommands,
- the four-tier configuration cascade that governs every runtime value,
- the four-tier SQL validity chain from token compile through post-write quality,
- L2 schema-evolution guarantees (per-run schema infer + merge reader),
- portability across storage backends and cloud environments,
- the L3/L4 hybrid Iceberg catalog model (writer-optimized + serving-optimized dual binding),
- the JDBC serving endpoint model with Trino,
- and the three-tier write-mode override (append / overwrite / partition_overwrite).

This PRD consolidates architecture conclusions from the platform design sessions. Cross-references to sibling PRDs are provided where a topic is elaborated in full detail.

## Positioning

`elt_pipeline` is a config-driven, Spark/Iceberg/Trino data platform runtime for local-first workstation development with native AWS S3 cloud portability and a roadmap for multi-cloud storage. The execution engine is Spark (writer) and the serving engine is Trino (JDBC reader). Iceberg provides the table-format contract at L3/L4; L2 is written as plain parquet with no catalog entity, ensuring the simplest possible staging model before value accrues at conformed layers.

The platform is explicitly designed so that:
- A pipeline author can run the full end-to-end loop (ingest → normalize → sql → publish) on a laptop with zero cloud dependencies.
- The same package manifest (YAML + SQL files) runs identically on **local POSIX** and **AWS S3** storage backends — configuration-only, zero lines of code change. Multi-cloud storage (GCS, ADLS, Databricks) is roadmap (see §6.3).
- JDBC serving is a first-class spoke: a BI tool or Jupyter notebook can always connect to a queryable `jdbc:trino://` endpoint emitted by every publish stage.

---

## 1. Platform Layers

The platform is organized in five logical levels, defined as architecture and governance boundaries rather than implementation steps. The full semantics are defined in [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md). The summary here is the reference used by other PRDs and runbooks.

### L1 (Landing / Raw Capture)

- Immutable raw payloads. Preserves source fidelity and replayability.
- Every L1 file carries a lineaged manifest.
- No business logic; no relational modelling.

### L2 (Lake / Source-Aligned Relational)

- First structured layer. Nested payloads are relationalized into flat parent/child tables via Spark JSON/CSV schema infer.
- **No catalog entity.** L2 is plain parquet directories partitioned by `source_name`, ingest-date column, and `run_id`.
- Source-aligned; no business conformance.
- For developer DX, a Jupyter two-import template reads L2 directly:
  ```python
  from elt_pipeline import l2, pipeline  # package manifests auto-registered
  df = l2.read("s3://…/level2/…/source_name=shipbob")
  ```

### L3 (Canonical / Conformed Model)

- Conformed business entities. Shared dimensions, shared facts, standard definitions.
- Written as **Iceberg tables** with catalog-managed metadata.
- Partitioning is by `source_name` (side-by-side re-co-location of multiple sources in one canonical table) and the business-date column.
- Late-arriving data is handled by design: models read L2 by ingest-date window, write L3 by business-date column. Spark's dynamic partition overwrite patches only the affected partitions.

### L4 (Consumer Datamarts / Gold)

- Consumer-optimized outputs: denormalized, aggregated, wide tables.
- Iceberg tables. Partitioning aligned to consumption patterns.
- BI and Jupyter consumers connect here via the JDBC serving spoke.

### L5 (Publish / Exports)

- Static delivery artifacts: CSV/Parquet/JSON extracts, canned reports, feed packages.
- Every L5 publish execution emits an audit record with `serving_endpoint` (the `jdbc:trino://` URL for the associated L3/L4 tables) so file consumers also have the queryable live path.

### Layer Boundary Principle

Even when L2→L3 and L3→L4 both use Spark SQL, the level boundaries are preserved because they:
- define semantic contracts for what logic belongs where,
- create operational boundaries for replay and backfill,
- and expose governance boundaries (IAM by path, column-level access per layer).

---

## 2. Four-Phase Execution Lifecycle

Every pipeline run follows a linear four-phase lifecycle. Each phase maps to one (or two) CLI subcommands. The lifecycle is intentionally explicit — there is no "auto-run everything" sugar that hides the phase boundary from the operator.

### Phase 1 — INGEST (`elt ingest run`)

- Pulls raw source payloads from a configured connector.
- Writes to L1 with replayable lineage manifest.
- Output: L1 parquet + manifest.

### Phase 2 — NORMALIZE (transform-ingest) (`elt normalize run`)

- Runs Spark SQL-free relationalization: infers schema from L1 payload, walks StructType, produces flat parent/child tables.
- Computes `mapping_version = sha256(canonical JSON(StructType mappings), sort_keys=True)[:16]` for change-detection.
- Writes L2 parquet partitioned by `source_name`, ingest-date, `run_id`.
- Output: L2 tables + per-run mapping_version digest.

### Phase 3 — SQL RUN (transform-sql) (`elt sql run`)

- Evaluates L3 and L4 model SQLs under `packages/<pkg>/sql/level{3,4}/…/model.sql` with `manifest.yaml` (one model per directory).
- Passes through the **four-tier SQL validity chain** (§4) before any write.
- On successful validity: writes L3/L4 as Iceberg tables (default writer catalog = `hadoop` for workstation).
- `--validate-only` runs the full validity chain with zero rows written.

### Phase 4 — PUBLISH (`elt publish run`)

- Creates static L5 exports and final consumer artifacts.
- Registers or refreshes Iceberg tables in the Trino serving catalog via `CALL iceberg.system.register_table(schema, table, hadoop_path)`.
- Emits audit JSON containing `serving_endpoint = jdbc:trino://host:port/catalog/schema` for the published L3/L4 tables.

### Subcommand Summary

```
elt ingest run          # Phase 1
elt normalize run       # Phase 2
elt sql run             # Phase 3
elt publish run         # Phase 4
elt sql run --validate-only
elt publish run --validate-only
```

---

## 3. Four-Tier Configuration Cascade

Every runtime value flows through a single cascade. There is exactly **one** singleton (`RuntimeContext.materialize()`) that composes the final values. No other module reads `os.environ` for ELT-prefixed keys.

### Precedence (highest → lowest)

1. **CLI arguments.** Explicit flags on the command line always win.
2. **Environment variables.** Prefixed `ELT_PIPELINE_*`.
3. **`pipeline.yaml` operator defaults.** Values read from the operator-facing manifest at project root.
4. **Frozen manifest defaults** in `runtime_manifest.py`. The last-resort values baked into the code.

### Singleton Contract

- Compose-once at CLI entry. The singleton is populated before any stage-specific logic runs.
- All downstream consumers (Spark, Trino, SQL executor, stage runners) read from the singleton.
- "Singleton-True-Non-Binding" for feature flags: a YAML default of `True` is advisory; the actual runtime vote is determined by probe (e.g. "is the Spark `IcebergSparkSessionExtensions` actually loaded?"). Only an **explicit False** (CLI arg or env) short-circuits the feature OFF.

### Three-Tier Override — Write Mode

Write mode (`load_mode` = `append` | `overwrite` | `partition_overwrite`) is resolved through the same cascade with operator-specific knobs:
- CLI `--load-mode X` wins (1),
- then `ELT_PIPELINE_LOAD_MODE` (2),
- then `pipeline.yaml` defaults (3),
- then frozen manifest default `append` (4).

---

## 4. Four-Tier SQL Validity Chain

Every L3/L4 SQL model passes through four progressively stronger validation tiers before any write occurs. `--validate-only` runs all four tiers and exits.

### Tier 1 — Token Compile

Jinja-style `{{ token }}` substitution with strict resolve validation. Every referenced token must have a value in the composed context; missing tokens fail immediately with file+line attribution.

### Tier 2 — Partition-Requirement Validation

For `overwrite` mode: the model's output partition columns must be present and typed correctly; for `partition_overwrite` mode: the dynamic partition key set must match Iceberg's `write.distribution-mode` requirements. Missing partition keys fail before Spark submit.

### Tier 3 — Catalyst EXPLAIN FORMATTED

The final resolved DataFrame is passed to `df.explain("formatted")`. The planner walks:
- parse → analyze → resolve → optimize.
- L2 tables referenced by the SQL are registered as temporary views via the L2 glob-reader (this is how L2 participates in EXPLAIN without a catalog entity).
- Any unresolved column, missing relation, or type mismatch is surfaced with plan line attribution.

### Tier 4 — Post-Write Quality Hooks

After successful write, the model's `manifest.yaml` quality hooks execute:
- `row_count_min` — fail if output row count below threshold,
- `unique_columns` — fail if any column set is not unique,
- `not_null_columns` — fail if any column contains NULL.

### Validate-Only Contract

`elt {sql|publish} run --validate-only` runs tiers 1-2-3-4 on the output sample without committing a write transaction. This guarantees CI parity for SQL pull requests.

---

## 5. L2 Schema Evolution

L2 schema evolution follows a deliberate pattern:

1. **Per-run schema infer.** Every NORMALIZE run re-infers the L1 payload schema via Spark JSON/CSV. No cached schema is trusted; the payload of the current run is the ground truth.
2. **Mapping version alert.** `mapping_version = sha256(canonical JSON(all table + column mappings), sort_keys=True)[:16]`. If `mapping_version` differs between runs, the change is recorded in the run manifest and (if configured) surfaced as a CI warning. This is an "alert first, merge second" model — schema drift is never silently applied.
3. **mergeSchema reader.** The L2 glob-reader unions rows across `**/table=X/run_id=*` directories with `.option("mergeSchema", "true")` set on the parquet reader, so older runs with narrower schemas read cleanly alongside newer runs with wider schemas.

### Rationale for No L2 Catalog Entity

A catalog at L2 is explicitly rejected because:
- L2 is a transient, source-aligned staging layer — Iceberg ACID, time-travel, and schema-on-read guarantees are irrelevant to a per-run immutable intermediate.
- The common pain point (L2 deletion by table glob) is solvable with 10× simpler registrar scaffolding if a real L2 JDBC consumer appears.
- Table-format value accrues for downstream consumers at L3/L4. Adding Iceberg to L2 is second-system uniformity, not value.

If a real L2 JDBC consumer appears later, the preferred remediation is a thin external catalog overlay over the existing L2 parquet directories — zero data migration required.

#### Trigger for Adding the L2 Thin Catalog Overlay (Not for Uniformity)

Do **not** build the overlay proactively. Build it (or adopt an off-the-shelf
catalog overlay component such as a DuckDB / Trino view registrar script)
**only when all of the following are true simultaneously**:

1. A downstream consumer has a concrete requirement to **query L2 directly over
   JDBC/ODBC** (e.g. a BI tool ad-hoc drill, a compliance audit query, or a
   data-scientist workflow where the 2-line Jupyter `L2TableReader` helper is
   not sufficient).
2. The same requirement **cannot be satisfied by promoting the query to an L3
   canonical or L4 mart model** with quality-hook coverage. L3/L4 is where
   table-format value accrues; L2 JDBC access is explicitly a last-resort
   escape hatch.
3. Platform owners have confirmed that **no L2 pipeline ownership model
   changes are required** for the overlay — the overlay is a read-only
   registerer of already-committed run_id directories and **must never**
   mutate L2 parquet files, directory layouts, or `mapping_version`
   semantics.
4. The consumer accepts that the overlay is a **thin registrar only**, not a
   table-format:
   - No L2 ACID / time-travel / snapshot semantics.
   - No DDL against L2 tables via the overlay.
   - No schema-write path — the overlay re-reads L2 `mergeSchema` semantics
     on every registration call.

If all 4 conditions are met, the recommended implementation scope is ~60–120
LOC: iterate `**/table=X/run_id=*` roots, call `SparkSession.read.parquet(...,
mergeSchema=True)`, infer the current effective schema, then `CREATE OR REPLACE
VIEW <overlay_namespace>.<table>` in the chosen registrar (Trino views, DuckDB
ATTACH'd catalog, Glue external-table registrar, or Hive Metastore external
table). **No L2→L2 Iceberg conversion, no schema-write path, no data migration,
and no mapping_version mutation are permitted in this scope.**

---

## 6. Portability

The platform is portable across storage backends and Iceberg catalog backends via configuration only. The storage-portability contract aligns exactly with [PRD 08 §P2](08-prd-storage-root-uri-io-dispatch.md): scheme prefix is the single routing key, unsupported schemes fail fast.

### 6.1 Storage Backends — Implemented Scope

L1/L2 file handoff is via string URIs (scheme + absolute path) — never via `pathlib` joins at root. **Implemented and tested storage schemes:**

| Scheme | Backend | Notes |
|--------|---------|-------|
| `s3://` | AWS S3 (Python control plane: `boto3`; Spark data plane: Spark's native S3 / EMRFS) | Unit-tested with an in-process S3 fake. |
| `file://` | Explicit local POSIX | Handled via `pathlib.Path` at leaf I/O call sites only. |
| *(no scheme)* | Bare local POSIX path | Treated as local filesystem; `pathlib.Path` at leaf I/O. |

**Unsupported storage schemes fail fast and sharp** with a clear error — they are never silently coerced. The current fail-fast list includes (at minimum): `s3a://`, `gs://`, `abfss://`, `wasbs://`, `dbfs://`, `hdfs://`. See [PRD 08 §P2](08-prd-storage-root-uri-io-dispatch.md) for the exact error message.

### 6.2 Cloud & Catalog Portability — Implemented Scope

| Environment | Storage backend | Supported catalog types | Required changes |
|-------------|-----------------|-------------------------|------------------|
| **Workstation / laptop** | Local POSIX (`file://` or bare paths) | `hadoop` (writer default), `jdbc` (serving default, SQLite-backed) | 0 LOC. Defaults. |
| **AWS (S3 + Glue)** | `s3://` (AWS S3) | `glue`, `jdbc`, `rest`, `nessie`, `hive_metastore`, `hadoop` | Set `ELT_PIPELINE_WRITER_CATALOG_TYPE=glue` + S3 URI roots. 0 LOC config change. Spark FS credentials use ambient IAM role on EMR; see [PRD 08 §P4](08-prd-storage-root-uri-io-dispatch.md). |

### 6.3 Multi-Cloud Storage & Catalogs — Roadmap (Not Yet Implemented)

The architecture (single-seam scheme dispatch in [PRD 08](08-prd-storage-root-uri-io-dispatch.md), 6-way catalog enum) is designed to support the environments below. They are **not implemented in the current release** — adding each requires scheme branches in `elt_pipeline/shared/path_utils.py` (≈18 functions per backend), Spark Hadoop FS config and credential wiring, and emulator-backed integration tests. Tracked per-backend items are added when pulled forward from this roadmap.

| Environment | Storage scheme(s) needed | Catalog approach | Roadmap notes |
|-------------|---------------------------|------------------|---------------|
| GCP (GCS + BigLake) | `gs://` | `catalog_type=rest` (BigLake Iceberg REST) or `hive_metastore` (Dataproc) | Requires GCS SDK branch in `path_utils`; Spark `spark.hadoop.fs.gs.*` config; credentials model (workload identity). |
| Azure (ADLS Gen2) | `abfss://` | `catalog_type=rest` or OneLake-compatible | Requires Azure SDK branch; authority parsing (`account@account.dfs.core.windows.net`); `spark.hadoop.fs.azure.*` + credential config. `wasbs://` (ADLS Gen1 / blob) is **not** on the roadmap; fail-fast as unsupported. |
| Databricks (Unity) | Covered by the backing-store scheme above (S3/GCS/ADLS — `dbfs://` as an explicit scheme is **not** on the roadmap) | `catalog_type=rest` (Unity Catalog REST) — Databricks storage is ADLS/S3/GCS underneath + Unity as the catalog | Recommendation: document the Unity-as-REST-catalog config + add a Databricks example YAML; do NOT add a `dbfs://` scheme branch. |
| Snowflake Polaris — Pattern A | Backing-store scheme from above (S3/GCS/ADLS) | Polaris ships an Iceberg-compatible REST catalog. Set `catalog_type=rest` with Polaris endpoint; drop the `polaris-catalog.jar` into Spark jars. | 0 LOC platform change; 1 jar + config. |
| Snowflake Polaris — Pattern B | Backing-store scheme from above | If Polaris REST protocol diverges from standard Iceberg REST: add one dispatch branch (~20 LOC) in `spark/session.py` to select the Snowflake-specific `Catalog` implementation. No stage code change. |

### Six-Way Catalog Type Enum

Both the writer catalog and the serving catalog accept the same six-way enum. The values are case-sensitive in Trino config (uppercase `JDBC`/`REST`/`NESSIE`/`GLUE`/`SNOWFLAKE`/`HIVE_METASTORE` — lowercase is invalid; the `_uc_cat()` helper uppercase-and-validates with exit 11 on `hadoop` for serving):

```
hadoop | jdbc | rest | nessie | hive_metastore | glue
```

Catalog types are genuinely implemented across both writer and serving paths; combining a catalog type with a **non-implemented storage scheme** (row 2+ in the roadmap table above) is the combination that is not yet shippable.

---

## 7. L3/L4 Iceberg — Dual Catalog Hybrid Binding

For the workstation default, L3/L4 Iceberg tables use a hybrid two-catalog arrangement:

- **Writer catalog (`catalog_type = hadoop`)** — Spark HadoopCatalog writes metadata and data files directly. Optimized for write throughput and zero metastore process on a laptop.
- **Serving catalog (`catalog_type = jdbc` + sqlite backend)** — Trino JDBC plugin reads the same table via a SQLite-backed metastore. The SQLite metastore file is auto-disposable (lives under `ops/trino_serving/var/data/`) and re-registerable at any time.

This keeps the write path fast (no RPC round-trips to a metastore) while still exposing a full JDBC serving spoke for BI tools.

### Registration (Serving Binding)

At every publish run, the four canonical L3/L4 models are registered (or refreshed) in the serving catalog via:
```sql
CALL iceberg.system.register_table('<schema>', '<table>', '<hadoop_path>')
```

This bridges writer metadata to the JDBC reader. Registration is idempotent; duplicate calls are no-ops.

### Switching Writer Catalogs

When moving to cloud, override:
```bash
ELT_PIPELINE_WRITER_CATALOG_TYPE=glue          # AWS
ELT_PIPELINE_WRITER_CATALOG_TYPE=rest          # GCP/Nessie/Polaris
ELT_PIPELINE_SERVING_CATALOG_TYPE=jdbc         # Trino remains JDBC-spoke
```

---

## 8. JDBC Serving Endpoint (Trino 468)

The platform ships Trino 468 as the first-class JDBC serving spoke. Operator docs live in [LOCAL_OPERATOR_RUNBOOK.md § Serving with Trino](../operator/LOCAL_OPERATOR_RUNBOOK.md).

### Operator Surface

- Launch: `bash ops/trino_serving/run_trino.sh start` (launches single-node Trino + `discovery.uri=http://localhost`. Workstation default.)
- Stop: `bash ops/trino_serving/run_trino.sh stop`
- Query:
  - via REST `/v1/statement` paginator (works without `bin/trino` client),
  - via JDBC driver: `jdbc:trino://localhost:8080/iceberg/local_demo`.

### Auto-Injected Dependency

The Trino Iceberg plugin requires `sqlite-jdbc-3.46.0.0.jar` inside the plugin dir. If missing on launch, the script:
1. Checks the local Ivy cache for the jar.
2. Falls back to `curl` directly from Maven with SHA verify.
3. Fails fast with copy-paste curl if both paths fail.

### Iceberg Default Flip

Iceberg is opt-out, not opt-in:
- In the config singleton, `_iceberg_effective_enabled()` returns `True` if not overridden.
- `--no-iceberg-enabled` (`action=store_false dest=iceberg_enabled default=None`) is available on both `elt sql run` and `elt publish run` to short-circuit OFF.
- The actual runtime vote is `has_extension` = is `IcebergSparkSessionExtensions` actually loaded. A YAML `true` is never binding on its own (prevents accidental wrong-branch parity-parquet when the JAR is missing).

### JDK 23 Workarounds

JDK 23 removed the SecurityManager. Both Spark and Trino need JVM flags injected:
```
-Djava.security.manager=allow -Djdk.security.allowAllPermissions=true
```
Spark: set via `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`.
Trino: set via `jvm.config`.

A three-tier JDK bootstrap closes the `mise` PATH shim gap:
1. `mise which java` → prefer.
2. `mise list-installs java` glob.
3. `/Library/Java/JavaVirtualMachines/*` and `sdkman` glob.
The resolved JDK bin is prepended to `os.environ["PATH"]` inside `cli.py` main, so all subprocess invocations (Spark submit, Trino launcher) see a real `java` binary, not a broken shim.

### PYSPARK Driver/Worker Version Mismatch

In environments where `sys.executable` and the default `python3` differ (e.g. Python 3.13 driver vs 3.13-venv worker), `cli.py` pins:
```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

---

## 9. Write Protocols — Staging-Swap vs Iceberg

Two distinct write protocols co-exist, each appropriate to its layer:

### Plain-Parquet Staging-Swap (L2 and parity path)

```
Write to  → {warehouse}/_staging/stage={s}/table={t}/run_id={run}/
Atomic swap → target location
```

- Solves the Spark DAG read-from-same-location hazard (Spark 4.x direct `SaveMode.Overwrite` is still broken for this).
- POSIX `rename(2)` for local; S3 `CopyObject → DeleteObject` for cloud.
- `_NO_STAGING_MOVE` fails fast if the storage scheme does not support an atomic move-equivalent.

### Iceberg Transactions (L3/L4)

- L3/L4 writes bypass the staging-swap layer entirely.
- Iceberg's own snapshot protocol handles atomic commits and reader isolation.
- The staging-swap protocol is therefore never invoked for Iceberg paths.

---

## 10. Cross-References

| Topic | Document |
|-------|----------|
| Layer semantics & governance boundaries | [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md) |
| Platform founding principles | [00-prd-platform-principles.md](00-prd-platform-principles.md) |
| L2→L3 SQL model manifest format, quality hooks, partition contracts | [03-prd-sql-level2-to-level3-and-level3-to-level4.md](03-prd-sql-level2-to-level3-and-level3-to-level4.md) |
| Storage URI dispatch (string URIs, scheme routing, no pathlib roots) | [08-prd-storage-root-uri-io-dispatch.md](08-prd-storage-root-uri-io-dispatch.md) |
| L3/L4 serving & table-format trade-offs | [09-prd-level3-level4-serving-and-table-format.md](09-prd-level3-level4-serving-and-table-format.md) |
| Operator runbook (Trino launch, CLI examples, removal triggers) | [LOCAL_OPERATOR_RUNBOOK.md](../operator/LOCAL_OPERATOR_RUNBOOK.md) |
| Troubleshooting (JDK, Spark, staging-swap) | [TROUBLESHOOTING.md](../operator/TROUBLESHOOTING.md) |
| Release & smoke-test checklist | [LOCAL_DEVELOPMENT_AND_RELEASE.md](../maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md) |
| JDK / JVM toolchain & JDK 23 workarounds | [JVM_TOOLCHAIN_SETUP.md](../maintainer/JVM_TOOLCHAIN_SETUP.md) |

---

## 11. Follow-Up Operator Triggers (No Backlog File Required)

All workstream decisions are folded into the canonical PRDs (03, 08, 09, 10) and operator/maintainer docs in this directory. No separate backlog, tracker, or historical audit-trail folder is maintained inside canonical `docs/` as part of the public repo surface.
New follow-up work is triggered **in-line by the conditions below**, not by a backlog file. Open a ticket / PR when, and only when, the stated condition is met.

### 11.1 Retire the `python` normalize engine (escape hatch)

Trigger — open a PR to delete when **all** hold:
1. No new bug required falling back to `normalize_engine="python"` for 60
   consecutive days of production use.
2. `uv run pytest tests/test_normalize_engine_parity.py` — all Spark-native
   fixtures still pass.
3. Grep for `normalize_engine: python` across platform configs returns 0
   in-scope references (or all refs were migrated to `spark` and re-tested).
4. Troubleshooting for `normalize run` driver OOM points solely at ingest
   payload splitting + Spark engine.

Also documented in [LOCAL_OPERATOR_RUNBOOK.md](../operator/LOCAL_OPERATOR_RUNBOOK.md) under "Removal trigger: retire the `python` normalize engine".

### 11.2 Retire the staging-swap write escape hatch for L3/L4

Trigger — open a PR to delete when **all** hold:
1. No new bug required `--no-iceberg-enabled` for 60 consecutive days of
   production use.
2. `uv run pytest tests/test_sql_iceberg_write.py` — all load-mode +
   same-path-rebuild regression tests still pass.
3. Grep for `spark.enable_iceberg:\s*false`,
   `ELT_PIPELINE_ICEBERG_ENABLED=0`, and `--no-iceberg-enabled` across
   platform configs + operator playbooks returns **0** in-scope references
   (or all refs were migrated to Iceberg and re-tested).
4. This PRD, the runbook, and troubleshooting no longer mention swap-layer
   operator steps for L3/L4.

Also documented in [LOCAL_OPERATOR_RUNBOOK.md](../operator/LOCAL_OPERATOR_RUNBOOK.md) under "Removal trigger: retire the staging-swap escape hatch for L3/L4".

### 11.3 Build the thin L2 external catalog overlay

Trigger — open a scoping PR only when **all 4** L2 overlay conditions in
§5 hold simultaneously (concrete JDBC consumer, not solvable at L3/L4, no
ownership-model changes, thin-only scope accepted). Otherwise do nothing —
L2 intentionally has no catalog.

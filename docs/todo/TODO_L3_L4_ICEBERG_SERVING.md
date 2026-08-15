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

## Gated Plan

### Gate I1 — Iceberg write path at L3/L4 (behind the existing abstraction)

- Add Iceberg as a Spark runtime dependency (`spark-sql`/`iceberg` runtime jars; wire into the session builder in [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py)).
- Replace L3/L4 `DataFrame.write.parquet(target_path)` in [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py) with Iceberg table writes, wrapped behind the current write seam so callers are unchanged.
- Map `load_mode` to native Iceberg operations: `full_refresh` → table replace/overwrite; `partition_overwrite` → partition-scoped overwrite (`replaceWhere`/dynamic overwrite equivalent); `append` → append. Preserve the exact partition columns from `_effective_partition_columns`.
- Preserve L3/L4 governance-by-path / partition conventions from PRD "pathing" work — Iceberg hidden partitioning must not break existing L3 partition semantics or `mapping_version`-driven L3 path lookups.
- **Do not touch L2** — stays source-aligned relational Parquet.

### Gate I2 — Pluggable catalog binding (config + dispatch)

- Add a catalog binding to the config contract (`elt_pipeline_cfg`), env-scoped, dispatched by a single seam like storage scheme (PRD 08).
- Default local binding = Hadoop/filesystem catalog so local dev needs no external service. JDBC/REST/Glue selectable by config.
- Register L3/L4 tables in the bound catalog on materialization; table identity/naming to reuse the existing physical-name policy (`_policy.py`) so lineage and discovery stay stable.
- Validate config precedence + fail-fast errors for a missing/misconfigured catalog (match the platform's existing error-code discipline).

### Gate I3 — Serving-engine binding + BI-connectivity proof (reference: Trino)

- Add a configurable serving-engine binding (`trino` | `athena` | `spark_thrift` | `duckdb`), documented in the operator runbook. The platform provides the endpoint contract, not a BI tool.
- **Proof of usability (the point of the whole backlog):** stand up Trino against the local Iceberg catalog and confirm a standard JDBC/ODBC client can `SELECT` from an L3 and an L4 table. Capture the connection string + a sample query in the runbook. This demonstrates any BI tool can plug in.
- Document the Athena binding as the AWS deployment path (same contract, managed engine) — validation deferred to a cloud environment, not required for local sign-off.

### Gate I4 — Retire the bespoke staging-swap (the custom-code win)

Once I1–I3 verify Iceberg atomic-commit parity for L3/L4:

- Remove [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) usage for L3/L4 and the Track B same-path-overwrite handling — Iceberg commits are atomic and read-consistent, dissolving the Spark 4.x same-path overwrite DAG hazard **by construction**.
- Confirm the same-path "self-querying rebuild" case (a model reading and writing the same canonical table) now works via Iceberg snapshot isolation, with a regression test.
- `grep` for residual staging-swap references in the L3/L4 path returns none. (Keep the primitive only if any non-Iceberg path still needs it; otherwise delete.)
- **Follow-up doc revisions:** PRD 03 (SQL L2→L3→L4) and PRD 08 (storage dispatch) updated to reference table-format materialization + catalog/serving dispatch. Operator runbook's "SQL Overwrite Protocol (Mercell/Camelot Staging-Swap)" section updated to reflect Iceberg-native commits.

### Gate I5 — Migration of existing L3/L4 Parquet (OQ-4)

- Re-materialize L3/L4 from L2 via the existing SQL models into Iceberg (preferred — clean, audit-consistent), rather than in-place register.
- Verify row-count + checksum parity against the prior Parquet outputs on the example project before declaring done.

## Open Decisions

- **OD-I1 (staging-swap removal timing, = PRD 09 OQ-3):** delete in I4 on parity sign-off, or keep behind a flag for one soak cycle first (normalize-cutover C2→C3 pattern). Recommendation: soak one cycle, then delete.
- **OD-I2 (Iceberg format version / defaults):** confirm Iceberg spec v2 defaults (row-level deletes not required for the current append/overwrite load modes) and partition-spec strategy vs. current explicit partition columns.

## Definition of Done

- [ ] L3/L4 materialize as Iceberg via the existing write seam; `load_mode` semantics preserved (Gate I1).
- [ ] Catalog is a config-dispatched binding; local default needs no cloud account (Gate I2).
- [ ] Trino reference endpoint proven: a JDBC/ODBC client selects from L3 + L4 Iceberg tables; connection recipe in the runbook (Gate I3).
- [ ] Serving-engine binding configurable; Athena documented as the AWS binding (Gate I3).
- [ ] Staging-swap retired for L3/L4; same-path rebuild regression test passes; same-path overwrite hazard closed by construction (Gate I4).
- [ ] Existing L3/L4 re-materialized to Iceberg with row-count + checksum parity (Gate I5).
- [ ] PRD 03 + PRD 08 revised; operator runbook overwrite-protocol section updated.
- [ ] `docs/todo/TODO.md` Backlog Index row added for this document.

## Cross-References

- Decision: [PRD 09 — L3/L4 Serving and Table Format](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15).
- OSS boundary rules this must honor: [00-prd-oss-adoption-strategy.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-oss-adoption-strategy.md).
- Dispatch pattern to mirror: [08-prd-storage-root-uri-io-dispatch.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md).
- Custom code to remove: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py).
- Origin: 2026-08-15 platform assessment (serving-gap finding).

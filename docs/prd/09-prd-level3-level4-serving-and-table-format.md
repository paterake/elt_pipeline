# PRD 09: Level3/Level4 Serving Layer and Table Format

## Document Status

- Status: **Accepted** — Apache Iceberg at `level3`/`level4` is the serving-layer contract. Delta Lake is retained as a documented fallback for a future provably Spark/Databricks-only deployment.
- Product area: `elt_pipeline`
- Scope: `level3`, `level4`, and the serving contract to downstream consumers
- Depends on: [00-prd-platform-principles.md](00-prd-platform-principles.md), [00-prd-oss-adoption-strategy.md](00-prd-oss-adoption-strategy.md), [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md), [03-prd-sql-level2-to-level3-and-level3-to-level4.md](03-prd-sql-level2-to-level3-and-level3-to-level4.md), [08-prd-storage-root-uri-io-dispatch.md](08-prd-storage-root-uri-io-dispatch.md)

## Purpose

Close the platform's serving gap. `elt_pipeline` currently transforms governed data through `level1`→`level5` correctly and lands `level3`/`level4` as **plain Parquet directories**. Nothing downstream can query those directories directly. This PRD decides how `level3`/`level4` become **consumable by BI tools (e.g. Qlik, Tableau, Quicksight) and ad-hoc SQL** without abandoning the platform's portability and client-neutrality principles.

The platform's stated reason to exist is a governed data layer that BI tools can sit on. Until a serving contract exists, the pipeline is ELT-complete but consumption-incomplete — not a usable end state.

## Problem Statement

### Current state (verified 2026-08-15)

- `level3`/`level4` are written with `DataFrame.write.parquet(target_path)` — plain Parquet, no table format, no catalog. See [sql/spark_executor.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/spark_executor.py) (`writer.parquet(...)`).
- There is **no metastore/catalog**: no Glue, Hive, Iceberg, or Delta anywhere in `src/`. Table discovery is by filesystem path convention only.
- Atomic/overwrite correctness is provided by a **bespoke staging-swap protocol** ([sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py)) — scheme-dispatched POSIX rename / S3 copy-delete, plus a manual partition-overwrite merge.

### Why this blocks consumption

BI and SQL consumers do not connect to raw Parquet paths. They connect to one of:

- a **catalog** (Glue / Hive / REST / JDBC) that exposes tables, or
- a **table format** (Apache Iceberg / Delta Lake) that provides snapshots, schema, and a catalog entry, or
- a **query endpoint** (Athena / Trino / Spark Thrift / DuckDB) that resolves one of the above.

With none of these, `level3`/`level4` are inert to every downstream tool named in the platform goal.

### The custom-code irony

The staging-swap protocol is **custom platform code re-implementing a commodity problem** — atomic overwrite, partition overwrite, snapshot isolation — that table formats solve natively. This runs against the platform's own direction of "library purity, not custom-code maintenance." A table format both closes the serving gap **and** lets us delete this bespoke code.

## Requirements

1. `level3`/`level4` datasets must be queryable by an external SQL/BI consumer through a catalog or query endpoint, without a custom exporter per tool.
2. Transforms remain **SQL-only**; the execution engine remains **Spark** (Platform Principles 3–4). No change to the authoring model — a user still writes `model.sql` + `manifest.yaml`.
3. Portability and client-neutrality preserved (Platform Principle 2): no hard AWS/Glue dependency in business logic; the catalog binding must be pluggable the same way storage scheme is dispatched today (PRD 08).
4. The level model and governance boundaries stay **platform-owned** (OSS Strategy): the table format is wrapped behind the existing write/read abstraction; it must not define the level contract.
5. Overwrite/partition-overwrite semantics currently guaranteed by the staging-swap protocol must be preserved or improved.
6. **BI-tool agnosticism is a first-class platform contract.** The platform must not bind to any specific BI tool (Quicksight/Qlik/Tableau/Power BI/…). It exposes **open standard interfaces** — Iceberg tables in a catalog, reachable via an **ANSI SQL JDBC/ODBC endpoint** — and treats the **serving engine as a configurable dispatch binding** (the same pattern as storage-scheme dispatch in PRD 08), so any BI tool connects via standard drivers and the engine is chosen per environment in config, never in code.

## Options Considered

### Option A — Status quo: plain Parquet + bespoke staging-swap (rejected)
- **Pros:** zero new dependency; already built.
- **Cons:** fails Requirement 1 outright — nothing can query it. Perpetuates custom table-management code. **Rejected: it is the non-usable end state this PRD exists to fix.**

### Option B — Apache Iceberg at L3/L4 (recommended)
- Write `level3`/`level4` as Iceberg tables via the Spark `iceberg` format; register in a **pluggable catalog** — Hadoop/filesystem catalog locally, Glue (or REST/JDBC) in cloud — dispatched by environment, mirroring PRD 08 storage-scheme dispatch.
- **Pros:** broadest engine interoperability — Athena (native), Trino, Spark, Flink, Snowflake read Iceberg, so Quicksight/Tableau/Qlik reach it via Athena/Trino/Spark SQL. Snapshot isolation, hidden partitioning, schema/partition evolution, time travel — **replaces the staging-swap protocol natively.** Open, vendor-neutral (Apache), aligns to client-neutrality. Catalog is swappable, preserving portability.
- **Cons:** new dependency + catalog concept to operate; maintainers learn Iceberg semantics; local dev needs a filesystem/JDBC catalog wired.

### Option C — Delta Lake at L3/L4
- Write as Delta via the `delta` format.
- **Pros:** simplest Spark-side ergonomics; ACID + time travel; strong if the world is Spark-centric.
- **Cons:** engine interoperability is narrower and historically Databricks-centric; Athena/Trino Delta support is thinner than Iceberg's. Weaker fit for a **tool-agnostic, portable** serving goal. Catalog story less uniform outside Spark.

### Option D — External catalog over existing Parquet (Glue/Hive), no table format
- Keep plain Parquet, register tables in a catalog so Athena/Trino can read paths.
- **Pros:** minimal write-path change; makes data queryable.
- **Cons:** no ACID/snapshots — keeps the staging-swap custom code (fails the de-duplication goal); schema evolution and partition changes remain manual; re-introduces AWS-Glue coupling unless a portable catalog is added anyway. Solves consumption but not the custom-code or correctness concerns.

## Recommendation

**Adopt Option B — Apache Iceberg at `level3`/`level4`, behind a pluggable catalog and the existing storage abstraction.**

Rationale, mapped to principles:

- **Closes the serving gap** (Req 1): Iceberg tables are natively queryable by Athena/Trino/Spark SQL, which is how Qlik/Tableau/Quicksight connect. This is the usable end state.
- **Deletes custom code** (Req 5 + OSS Strategy): Iceberg's atomic commits, partition overwrite (`overwrite`/`MERGE`/`replaceWhere`-equivalent), and snapshot isolation **replace [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) and the manual partition merge.** Net custom-code *reduction* — advancing "library purity" further than the dual-engine normalize selection does.
- **Preserves portability** (Req 3): open Apache format on any Spark; catalog is env-dispatched (Hadoop/JDBC local, Glue/REST cloud) exactly like storage scheme in PRD 08. No AWS in business logic.
- **Keeps the model owned** (Req 4): the level contract, mapping catalog, audit/lineage, and SQL authoring model are unchanged. Iceberg is wrapped behind the L3/L4 write/read layer — it is the commodity substrate, not the architecture.
- **No change to the user contract** (Req 2): transforms stay SQL, engine stays Spark. The change is in *how the result is materialized*, invisible to config authors.

Delta (Option C) is the documented fallback if a future deployment is provably Spark/Databricks-only and interoperability breadth is not required.

### Ratification (2026-08-15)

Option B accepted. The decision rests on **fit for a client-neutral, portable, BI-tool-agnostic governed platform** — Iceberg is the vendor-neutral interchange standard, natively reachable by the platform's BI targets, and it lets us delete the bespoke staging-swap code — **not** on raw installed-base counts (where Delta, via Databricks, remains large). Iceberg's momentum as the open standard (broad engine support across Athena/Trino/Snowflake/BigQuery/Flink; the industry's convergence on it) is the durable basis for the choice.

## Serving Topology (how consumers actually connect)

The platform owns everything up to and including a **JDBC/ODBC SQL endpoint over Iceberg tables**. The serving engine and catalog are **configurable bindings**; the BI tool is outside the platform boundary and connects via a standard driver.

```
                         ┌──────────── PLATFORM BOUNDARY (owned + configurable) ───────────┐
                         │                                                                  │
level3/level4  ──(Iceberg tables)──►  Catalog binding            Serving-engine binding     │
                         │            (Iceberg REST spec):       (ANSI SQL / JDBC-ODBC):     │
                         │            Hadoop·JDBC local          Trino  (portable ref)       │
                         │            REST server (Polaris/      Athena (AWS binding)        │
                         │              Nessie/Lakekeeper)       Spark Thrift (no extra infra)│
                         │            Glue (AWS)                 DuckDB (local dev)          │
                         │                                          │                        │
                         └──────────────────────────────────────────┼────────────────────────┘
                                                                     │  JDBC / ODBC (standard driver)
                            ┌───────────────┬────────────────┬───────┴────────┬───────────────┐
                         Tableau          Qlik           Quicksight        Power BI        Superset
                                              (any BI tool — deployment concern, not platform code)
```

Two open contracts make this work for *any* tool: the **Iceberg REST Catalog spec** (metadata) and **ANSI SQL over JDBC/ODBC** (access). The serving-engine binding is selected per environment in config — `trino` (portable default) · `athena` (AWS) · `spark_thrift` · `duckdb` — none hard-wired.

### Serving role by level (altitude)

One table format (Iceberg) spans every SQL-consumable layer for consistency — but the *role* of each level differs, and the platform must not over-build the thin layers or under-serve the real ones:

| Level | Role | Table format | Catalog | Served to BI? |
|---|---|---|---|---|
| `level1` | Raw immutable landing | none (raw payloads) | no | no |
| `level2` | **Thin queryable materialisation of L1** — normalised, source-aligned. Catalogued so it *can* be queried for lineage/debugging; **not** the consumer workhorse. | Iceberg | yes | exploration only |
| `level3` | Canonical model (silver) — **real consumer SQL surface** | Iceberg | yes | **yes** |
| `level4` | Consumer marts (gold) — **real consumer SQL/BI surface** | Iceberg | yes | **yes** |
| `level5` | **Static canned delivery files** (CSV/TSV/JSONL) | none | **no** | **no** |

Iceberg at L2 is chosen for **platform consistency and custom-code deletion** (one format L2–L4; retires the L2 glob + `mergeSchema` hack), **not** because L2 consumers need snapshots/time-travel. The real consumer work is L3 and beyond.

`level5` publish/export is a **pre-rendered static delivery path**: single-file CSV/TSV/JSONL artifacts with a checksum. It has **no catalog, no table format, and no BI serving** — a BI tool never connects to L5. It renders *from* the served L3/L4 layers upstream and remains orthogonal to this serving layer (a "nice to have" derived-delivery output).

## Impact and Migration

- **Write path:** L3/L4 materialization switches from `writer.parquet(target_path)` to Iceberg table writes behind the same abstraction. `load_mode` (`full_refresh`, `partition_overwrite`, `append`) maps to Iceberg native operations.
- **Staging-swap:** [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) and the plain-parquet same-path-overwrite handling become **obsolete** for L3/L4 (Iceberg commits are atomic and read-consistent). Removal is in scope once 60 consecutive days of production use show zero `--no-iceberg-enabled` opt-outs, all L3/L4 load-mode + same-path-rebuild tests pass green, and the operator runbook no longer describes swap-layer steps for L3/L4. This also dissolves the Spark 4.x same-path overwrite DAG hazard by construction.
- **Catalog config:** a catalog binding enters the config contract (`elt_pipeline_cfg`) — env-scoped, defaulting to a local filesystem/JDBC catalog so local-first dev keeps working with no cloud account.
- **L2:** **No catalog entity — explicitly rejected.** L2 is a transient,
source-aligned staging layer. Value accrues for downstream consumers at L3/L4;
adding Iceberg to L2 is second-system uniformity, not value. The common pain
point (L2 deletion by table glob) is solvable with a thin external catalog
overlay over existing parquet directories if a real L2 JDBC consumer appears
later; L2 itself remains plain parquet with `mergeSchema` read semantics
(see [PRD 10 §5](10-prd-architecture-and-lifecycle.md) for full rationale
and 4-condition trigger for the overlay). The discovery glob + `mergeSchema`
pattern in [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py)
is the canonical L2 read mechanism (parity Finding 5). **L1 remains raw**
immutable landing — no table format. Trino / the serving binding exposes
**L3/L4** as the governed BI surface; L2 is accessed either via the 2-line
Jupyter `L2TableReader` helper (developer flow) or, if the 4-condition
overlay trigger is met, via a thin read-only registrar.


## Open Questions

- **OQ-1 (catalog binding) — RESOLVED 2026-08-15:** the catalog is a **configurable binding on the Iceberg REST Catalog spec**, env-dispatched like storage scheme (PRD 08). Defaults: **Hadoop/filesystem catalog** local (zero infra, keeps local-first dev working with no cloud account); **JDBC** as the portable cross-env option; **Glue** as the AWS binding; a **REST catalog server** (Polaris/Nessie/Lakekeeper) supported for teams that want a shared standalone catalog. No catalog implementation is hard-wired.
- **OQ-2 (serving engine / BI target) — RESOLVED 2026-08-15:** the platform stays **BI-tool-agnostic** (Requirement 6). It does **not** select a BI tool. The **serving engine is a configurable binding** exposing ANSI SQL over JDBC/ODBC — `trino` (portable reference) · `athena` (AWS) · `spark_thrift` · `duckdb`. **Trino is the reference binding for connectivity validation** because it is the vendor-neutral engine and Athena is managed Trino/Presto — proving Trino+JDBC demonstrates that any JDBC/ODBC BI tool (Quicksight, Qlik, Tableau, Power BI, Superset) can connect. Reasoning: the platform exists to avoid hard-wired BI coupling; selecting a single BI tool would defeat the neutrality principle.
- **OQ-3 (staging-swap removal timing):** delete when no config opts out of Iceberg for 60 consecutive days of production use, all L3/L4 load-mode + same-path-rebuild regression tests pass, and the operator runbook no longer describes swap-layer steps for L3/L4.
- **OQ-4 (migration of existing L3/L4 Parquet):** re-materialize from L2 via existing SQL models (clean, preferred) vs. in-place register. Re-materialize is simpler and audit-clean.

## Consequences

- **Positive:** platform becomes consumable — the stated goal is met; net custom-code reduction; same-path overwrite hazard eliminated by construction; schema/partition evolution and time-travel gained; portability and SQL-only authoring preserved.
- **Negative / cost:** a real new dependency and the operational concept of a catalog; maintainer ramp on Iceberg; local-dev catalog wiring; follow-up revisions to PRD 03/08 and `elt_pipeline_cfg`.
- **Net:** this is the change that turns a correct ELT engine into a usable governed data platform. Without it, the level model terminates in data nothing can read.

## Cross-References

- Serving gap identified in the 2026-08-15 platform assessment (all-Spark alignment review).
- Custom table-management code targeted for removal: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) once the plain-parquet escape hatch is no longer used for L3/L4.
- Storage/scheme dispatch pattern to mirror for catalog dispatch: [08-prd-storage-root-uri-io-dispatch.md](08-prd-storage-root-uri-io-dispatch.md).

# PRD 03: SQL Transforms from Level 2 to Level 3 and Level 3 to Level 4

## Document Status

- Status: Draft v2 (pathing revision: default partitions, decouple partitionBy/load_mode, late-arrival flow)
- Product area: `elt_pipeline`
- Stages: `level2` -> `level3`, `level3` -> `level4`
- Proposed implementation language: Python
- Proposed packaging and environment management: `uv`

## Background

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

The level model and its medallion mapping is defined in [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md).

The existing platforms use SQL-driven transformation to promote source-aligned datasets into business-conformed models and then into consumer-facing or reporting-ready datasets. The discovered `legacy stack A` implementation shows several durable ideas worth preserving:

- configuration-driven SQL files,
- ordered execution for dependent entities,
- environment-aware token replacement,
- separate semantics for `level3` and `level4`,
- support for incremental and full refresh patterns.

The new `elt_pipeline` should retain these strengths while simplifying configuration, reducing operational coupling, and aligning the transformation model to a Python-first runtime.

## Problem Statement

The current SQL transformation approach spans multiple repositories, storage conventions, and runtime assumptions. Over years of growth, this created several problems:

- model logic is difficult to reason about end-to-end,
- dependencies and execution order are not expressed as a unified product contract,
- environment and catalog concerns leak into SQL runtime behavior,
- promotion from structured source tables to conformed and consumer layers is powerful but operationally complex.

The new platform needs a clear SQL transformation product that defines what each stage owns, how models are configured and executed, and how lineage, quality, and backfills work.

## Product Vision

Build a configuration-driven SQL transformation framework in Python that:

- turns source-aligned `level2` data into conformed `level3` models,
- turns conformed `level3` models into consumer-facing `level4` datasets,
- supports deterministic dependency ordering,
- handles incremental and full-refresh patterns,
- and exposes strong lineage, validation, and operational control.

## Stage Definitions

### Level 3

`level3` is the conformed business model layer.

It is responsible for:

- applying business rules,
- standardizing entity definitions across sources where required,
- creating shared dimensions and facts,
- implementing quality and conformance rules,
- producing stable analytical foundations for downstream consumers.

### Level 4

`level4` is the consumer-facing analytical serving and datamart layer for this phase of the platform.

It is responsible for:

- domain-specific reporting datasets,
- denormalized consumer-oriented tables,
- performance-oriented aggregations,
- mart-style outputs aligned to business use cases.

Consumers may analyze data directly from `level4` tables.

This PRD intentionally stops at `level4`. Static file outputs, canned reports, and other downstream publish/export mechanics belong in `level5` and are defined separately.

## Level 3 and Level 4 Path Grammar (Source-Aligned vs Canonical)

L1/L2 (source-aligned levels) and L3/L4 (canonical/mart levels) serve different purposes and have two distinct but internally consistent sub-grammars. Environment is handled by per-env roots (never in-path), consistent with L1/L2.

### Level 3 (Canonical) Path Grammar

```
level3/<table_name>/source_name=<src>/<date_col>=<date>/*.parquet
```

- `date_col` is `business_date` by default (event day from the payload). This is the late-arrival-correct convention: data received on `ingest_date=2026-08-10` whose business date is `2026-07-31` correctly lands in partition `business_date=2026-07-31/`.
- `date_col` may be explicitly overridden to `ingest_date` for snapshot/audit tables where "what did the data look like on arrival day?" is the semantic question.
- `source_name=<src>` partitions are the Mercell re-co-location pattern: one canonical Spark table, multiple sources co-located side-by-side as peer partitions. Each source's L3 pipeline independently overwrites only its own `(source_name, date_col)` tuple.

### Level 4 (Consumer Mart) Path Grammar

```
level4/<table_name>/<date_col>=<date>/*.parquet
```

- `date_col` defaults to `business_date` for temporal marts.
- `date_col` may be omitted only for non-temporal dimension tables that are always full-refreshed (no date partitioning needed).
- `source_name` is not a default L4 partition — L4 marts are typically already conformed and joined, so source filtering is a column-level concern rather than a path-level concern. The option to include `source_name` explicitly via manifest `target.partition_columns` is retained.

### Environment Handling (Consistent with L1/L2)

- `environment` SHALL NOT appear in any L3 or L4 path segment.
- Environment is handled exclusively by which `--warehouse-root` the pipeline is pointed at.
- Each environment (dev, staging, prod) gets its own independent warehouse root / bucket.
- Two environments sharing one `--warehouse-root` would write to the same `level3/<table>/` paths and collide; this is intentionally prevented by the per-env-roots convention.

### Path Segments Are Real Spark `partitionBy` Columns

All path segments above SHALL correspond 1:1 to actual Spark `partitionBy` columns on the write. This means:
- Partition columns in the path MUST also exist as real data columns produced by the SQL model's SELECT (Spark requirement for metastore registration, filtering, and joins).
- Spark dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`) is enabled at session level and is the mechanism that makes partition-scoped overwrites correct.
- The base table path for `_table_path` remains `<warehouse_root>/level3/<table_name>` or `<warehouse_root>/level4/<table_name>`; `partitionBy()` adds the `<key>=<value>/` segments beneath it automatically.

## Goals

- Define a single SQL model execution framework for `level3` and `level4`.
- Preserve clear separation between conformance (`level3`) and consumption (`level4`).
- Support model ordering and dependencies without embedding orchestration logic in code.
- Standardize incremental, merge, and full-refresh behaviors.
- Provide model-level validation, lineage, and operational metadata.
- Enable gradual migration from legacy SQL/config repositories into the new Python-based solution.

## Non-Goals

- Designing every business model in this document.
- Rebuilding legacy SQL one file at a time before the new execution contract is defined.
- Managing Level 5 external outputs in the first PRD set.

## Users and Stakeholders

- Analytics engineers writing SQL models.
- Data platform engineers building execution runtime, metadata, and orchestration.
- Business intelligence and downstream consumer teams relying on `level4` datasets.
- Platform operators responsible for backfills, scheduling, and incident response.

## Scope

This PRD covers:

- SQL model packaging and configuration,
- `level2` to `level3` execution semantics,
- `level3` to `level4` execution semantics,
- dependency ordering,
- tokenization and environment binding,
- incremental processing,
- lineage, quality checks, and replay controls.

This PRD excludes:

- source ingestion,
- non-SQL normalization from raw data,
- external export file generation beyond `level4`.

## Functional Requirements

### FR1. SQL as a First-Class Product Artifact

The framework shall treat SQL models and their metadata as versioned product artifacts.

Each model must declare at minimum:

- model name,
- stage (`level3` or `level4`),
- source domain or package,
- materialization type,
- dependency list or execution order,
- load mode,
- target dataset/table name,
- quality expectations,
- ownership metadata.

### FR2. Stage-Specific Semantics

The framework shall enforce different responsibilities by stage.

`level3` models may:

- conform source structures,
- join related entities,
- create facts and dimensions,
- standardize identifiers and business definitions.

`level4` models may:

- denormalize `level3` data,
- build reporting and mart tables,
- aggregate or precompute consumer views,
- optimize for consumption patterns.

`level4` models must not become a catch-all for raw source cleanup that belongs earlier in the pipeline.

### FR3. Dependency Management

The framework shall support:

- explicit model dependency declarations,
- ordered execution for models with sequential requirements,
- selective runs for one model plus its upstream dependencies,
- stage-scoped runs such as all `level3` models for a source domain,
- deterministic re-runs.

The preferred design is dependency metadata rather than only flat text ordering files, though order files may be supported during migration.

### FR4. Materialization Strategies

The framework shall support:
- full refresh,
- append,
- partition overwrite,
- merge or upsert,
- snapshot or slowly changing dimension patterns where needed.

Each model must declare its materialization strategy explicitly.

#### FR4.1. Default Partition Convention (Orthogonal to Load Mode)

`partitionBy` (physical partitioning layout) and `load_mode` (write semantics — what gets overwritten) SHALL be **orthogonal** concerns. Partitioning controls how the data is laid out on disk; load mode controls whether overwrite replaces the whole table, appends, or overwrites only matching partitions. Both apply independently.

Previously, `partitionBy` was applied only for `load_mode: partition_overwrite`. This is incorrect — partitions govern read efficiency and governance-by-path regardless of load mode. The corrected behavior is:

**Effective partition columns are computed as follows:**
| Stage | Manifest `target.partition_columns` | Effective partition columns |
|---|---|---|
| L3 | empty / unset | `["source_name", "business_date"]` (default) |
| L3 | explicitly set to `["source_name", "ingest_date"]` | `["source_name", "ingest_date"]` (snapshot/audit override) |
| L3 | explicitly set to custom list | custom list (override) |
| L3 | explicitly set to `[]` (empty) AND `load_mode == full_refresh` | `[]` (no partitions, explicit opt-out) |
| L4 | empty / unset | `["business_date"]` (default) |
| L4 | explicitly set to custom list | custom list (override) |
| L4 | explicitly set to `[]` (empty) | `[]` (no partitions, for non-temporal dimensions) |

**Default rationale:**
- L3 default `["source_name", "business_date"]`:
  - `source_name` enables Mercell re-co-location (multiple sources side-by-side in one canonical table), per-source independent replay, and governance-by-path (IAM prefix controls per source).
  - `business_date` is the late-arrival-correct default. Data received on `ingest_date=2026-08-10` with payload date `business_date=2026-07-31` correctly lands in the `business_date=2026-07-31` partition. Spark dynamic partition overwrite replaces only the matching `(source_name, business_date)` tuple.
- L3 override `["source_name", "ingest_date"]`: Use for snapshot/audit tables where "what did the data look like on the day we received it?" is the semantic question. Arrival-day semantics, not event-day semantics.
- L4 default `["business_date"]`: Consumer marts are typically already conformed, so `source_name` is no longer a required path-level partition. Date partitioning still provides query pruning and governance-by-time-window.
- L4 empty `[]`: Only for non-temporal dimensions (reference tables, SCD Type 1 full snapshots) that are always full-refreshed.

**Validation rule:** If the effective partition columns reference a column (e.g. `business_date`) that is not produced by the SQL model's SELECT, Spark will raise a readable error at write time. This is the correct enforcement mechanism — no extra validation layer needed.

#### FR4.2. `partitionBy` Applies to All Three Load Modes

The effective partition columns (per FR4.1) SHALL be applied via `.partitionBy(*effective_partition_columns)` for **all three** load modes, not only `partition_overwrite`:

- `full_refresh`: `df.write.mode("overwrite").partitionBy(*cols).parquet(...)` — overwrites the whole table, but still partitions the new data (consistent layout, query pruning).
- `append`: `df.write.mode("append").partitionBy(*cols).parquet(...)` — appends new rows into the correct partition directories.
- `partition_overwrite`: `df.write.mode("overwrite").partitionBy(*cols).parquet(...)` — with session-level dynamic mode (`partitionOverwriteMode=dynamic`), replaces **only** the partitions whose values appear in the incoming dataframe. All other partitions (other dates, other sources) are untouched. This is the workhorse for incremental runs and per-source replay.

This guarantees uniform physical layout across all load modes, so that a table switched from `append` to `full_refresh` (e.g., during a backfill) continues to produce the same partitioned directory structure.

#### FR4.3. Dynamic Partition Overwrite Prerequisites

The `partition_overwrite` mode relies on:
1. Session-level `spark.sql.sources.partitionOverwriteMode=dynamic` already being configured.
2. All partition columns existing in the output dataframe (enforced by Spark at write time).
3. The model's SELECT producing the correct partition column values — these values drive which existing partitions get replaced.

For the default L3 case, this means the SQL model MUST:
- Produce a `source_name` column in its SELECT (already available from L2 via P1 lineage columns — just `SELECT source_name` from the L2 source).
- Produce a `business_date` column in its SELECT (either pass through from payload, or derive from event timestamp fields).

The model author does NOT need to inject these via tokens; they are plain columns from the L2 read.

### FR5. Runtime Parameterization

The framework shall support controlled token replacement for runtime-bound values such as:

- environment,
- processing date or date range,
- source domain,
- catalog, schema, or namespace,
- target table names where parameterized by environment.

Tokenization must be simple, deterministic, and validated before execution.

### FR6. Backfills and Date Windows

The platform shall support:
- single-date runs,
- date-range backfills,
- partition-aware replay,
- stage-only reruns,
- selective model reruns after SQL changes.

The runtime must preserve a record of what dates, partitions, and model versions were executed.

### FR6a. Late-Arriving Data Repartitioning (Camelot Capability, Preserved by Design)

The platform SHALL preserve and improve upon the Camelot late-arriving data repartitioning capability. This is not a separate "repartition job" — it is the default behavior of any L3 model that selects `business_date` in its output.

**Standard 4-step flow for late arrivals:**
1. **L2 write (arrival day).** Data for a `business_date=2026-07-31` event arrives late on `ingest_date=2026-08-10`. The normalization step writes it to L2 under `level2/source=X/entity=Y/.../ingest_date=2026-08-10/...`. The L2 parquet row carries both:
   - `ingest_date=2026-08-10` and `source_name=X` (mandatory lineage columns, per PRD 02 FR8.2), PLUS
   - the payload column `business_date=2026-07-31` from the flattened source JSON.
2. **L3 SQL model reads by `ingest_date`, writes by `business_date`.** The L3 model's SELECT is structured as:
   ```sql
   cte_src_base AS (
     SELECT *, business_date
     FROM t.level2_X_Y
     WHERE ingest_date = '{{ window.start_date }}'
   ),
   cte_joined AS (...)
   SELECT * FROM cte_joined
   ```
   The `WHERE ingest_date = ...` reads everything that arrived on the specified day (the replay unit). `business_date` is passed through in the output columns.
3. **L3 write to the correct event-date partition.** The L3 writer applies the default partition convention (`partitionBy=["source_name", "business_date"]`) with `mode("overwrite")` and session-level dynamic partition overwrite. Spark writes the output to:
   ```
   level3/canonical_table/source_name=X/business_date=2026-07-31/
   ```
   replacing **only** the `(source_name=X, business_date=2026-07-31)` partition. Other date partitions and other sources are untouched.
4. **Idempotent replay.** Re-running the same L3 model for `ingest_date=2026-08-10` produces the same output and overwrites the same `(source_name, business_date)` partition — safe and deterministic.

**Why this is better than the Camelot implementation:**
- No separate explicit "repartition" step or job needed. It's the default behavior of every L3 model that selects `business_date`.
- `ingest_date` is preserved as a queryable data column at L3, so auditors can answer: "Which ingest_date run wrote these rows into business_date=2026-07-31?"
- Dynamic partition overwrite + the fact that Spark restricts overwrite to partition values present in the output dataframe = zero risk of accidentally overwriting unrelated source or date partitions, even if the WHERE clause in the SELECT is wrong. This is a correctness guardrail.

**Snapshot/audit override:** If the semantic question is "what did the data look like on arrival day?" rather than "what happened on event day?", the model manifest overrides `target.partition_columns` to `["source_name", "ingest_date"]`. This writes the output into `source_name=X/ingest_date=2026-08-10/` instead, preserving arrival-day semantics.

**Key design invariants that make this work:**
- At L1/L2, path date key = `ingest_date` (arrival day, immutable once written, unit of replay).
- At L3/L4, default path date key = `business_date` (event day from payload, enables late-arrival repartitioning).
- Both dates are always available as real data columns at L2 (lineage columns give `ingest_date`; payload gives `business_date`), so the L3 model's SELECT can read by one and write by the other.
- `partitionBy` is applied for all load modes (not only `partition_overwrite`), so the physical layout is consistent no matter how the table is written.

### FR7. Data Quality Controls

The SQL product shall support model-level validations including:

- row-count thresholds,
- uniqueness expectations,
- not-null constraints on declared key columns,
- referential integrity checks where practical,
- freshness and completeness checks for source partitions.

Validation outcomes must be captured as part of the run record.

### FR8. Lineage and Auditability

The shared observability, audit, and error-handling contract for all stages is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

For SQL model execution, the system shall record:

- model name and version,
- input models or source entities,
- target object,
- execution window,
- execution status,
- row counts in and out where available,
- validation outcomes,
- runtime environment,
- triggering workflow or operator.

### FR9. Failure Handling

The shared error-handling contract is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

For SQL execution, the runtime shall distinguish:

- SQL compilation or parse errors,
- missing dependency failures,
- data quality failures,
- write conflicts,
- transient engine or infrastructure failures.

Operators must be able to retry safely after transient failures without guessing the previous partial state.

### FR10. Separation of Code and Model Logic

The Python package shall provide the execution engine, metadata handling, and orchestration interface.

SQL logic and model metadata shall remain declarative and maintainable without code changes for standard transformations.

### FR11. Environment Promotion

The platform shall support promoting the same model package across environments with environment-bound configuration, rather than separate SQL copies per environment.

### FR12. Consumer Contract for Level 4

`level4` models must define a consumer-facing contract that includes:

- business purpose,
- grain,
- key columns,
- update cadence,
- freshness expectation,
- known caveats.

## Non-Functional Requirements

### NFR1. Determinism

- Given the same inputs, configuration, and model version, runs must be reproducible.

### NFR2. Performance

- The framework must support efficient execution of both large conformance models and smaller consumer marts.
- Incremental models should avoid unnecessary full recomputation.

### NFR3. Reliability

- Failed runs should not silently leave targets in ambiguous states.
- Model-level restart behavior must be explicit.

### NFR4. Usability

- SQL authors should be able to add or modify models with minimal platform code changes.
- The local development loop should be lightweight and compatible with the Python `uv` project setup.

### NFR5. Observability

- Execution logs, metrics, lineage, and quality results must be queryable by run, model, stage, and domain.

## Proposed Product Design

### SQL Model Package

The new solution should organize models in a stage-aware structure such as:

- `elt_pipeline/sql/level3/<domain>/`
- `elt_pipeline/sql/level4/<domain>/`
- `elt_pipeline/models/<stage>/<domain>/model.yml`

The exact shape may vary, but the runtime must be able to discover:

- SQL text,
- metadata,
- dependencies,
- validation rules,
- materialization strategy.

### Python Runtime Components

Suggested package areas:

- `elt_pipeline.sql.runtime`
- `elt_pipeline.sql.discovery`
- `elt_pipeline.sql.compiler`
- `elt_pipeline.sql.executor`
- `elt_pipeline.sql.lineage`
- `elt_pipeline.sql.validation`

### Execution Interface

The runtime should support commands such as:

- run all models for a stage,
- run one domain,
- run one model plus dependencies,
- backfill a date range,
- validate models without executing writes,
- compile models with tokens resolved for inspection.

### Migration-Friendly Dependency Strategy

To accelerate migration from the legacy platforms, the runtime should support:

- explicit dependency graphs as the target state,
- optional compatibility with legacy ordered lists during transition,
- automated checks that order files and declared dependencies do not disagree.

## Data Contract for Level 3

Each `level3` model must document:
- business grain,
- conformance rules applied,
- source entities used,
- key and partition columns,
- refresh pattern,
- quality expectations.

**Level 3 path and partition contract:**
- Path grammar (default): `level3/<table_name>/source_name=<src>/business_date=<date>/*.parquet`
- Path grammar (snapshot/audit override): `level3/<table_name>/source_name=<src>/ingest_date=<date>/*.parquet`
- Default effective partition columns (when manifest is empty): `["source_name", "business_date"]`
- Opt-out: manifest may set `target.partition_columns` explicitly to override or disable (empty list only allowed with `full_refresh`)
- The SQL model's SELECT MUST produce the effective partition columns as real output columns (Spark enforces this at write time with a readable error)
- `business_date` (default date partition) = event day from the payload, enables late-arrival repartitioning per FR6a
- `ingest_date` (override) = arrival day, for snapshot/audit semantics
- `source_name` = the source this data came from, enables Mercell re-co-location (multiple sources side-by-side in one canonical table) and governance-by-path

## Data Contract for Level 4

Each `level4` model must document:
- consumer use case,
- business grain,
- freshness SLA or expectation,
- downstream dependency or audience,
- quality expectations,
- breaking change policy.

**Level 4 path and partition contract:**
- Path grammar (default): `level4/<table_name>/business_date=<date>/*.parquet`
- Path grammar (no partitioning): `level4/<table_name>/*.parquet` (non-temporal dimensions only)
- Default effective partition columns (when manifest is empty): `["business_date"]`
- Opt-out: manifest may set `target.partition_columns = []` for non-temporal dimensions that are always full-refreshed
- `source_name` is not a default L4 partition, but may be added explicitly via manifest `target.partition_columns` when a mart needs source-level partitioning for governance or query patterns
- As with L3, effective partition columns MUST be produced by the SQL model's SELECT

## Success Metrics

- SQL authors can add a new `level3` or `level4` model without modifying runtime code.
- Dependency-aware execution replaces hand-managed sequencing for most models.
- Backfills and replay windows are routine operational actions rather than bespoke jobs.
- Model-level lineage and validation status are available for every run.
- The platform cleanly separates conformed models from consumer marts.

## Acceptance Criteria

- A Python `uv` project can discover and execute SQL models for both `level3` and `level4`.
- Model metadata declares stage, dependencies, materialization, and validation rules.
- The runtime supports date-scoped execution and backfills.
- At least one representative dependency chain executes deterministically across multiple models.
- Validation results are stored with the run outcome.
- Operators can rerun one failed model or one domain without rebuilding the entire platform.

## Migration Considerations

- Begin with one representative business domain and migrate both its `level3` and `level4` paths together.
- Normalize legacy ordering conventions into explicit dependency metadata wherever possible.
- Preserve legacy SQL semantics first, then simplify once parity and confidence are established.
- Avoid carrying forward environment-specific SQL duplication.

## Risks

- Implicit dependencies in legacy SQL may not be obvious until migration begins.
- Environment-specific token behavior can create hard-to-debug differences if not constrained early.
- Consumer marts may continue to absorb conformance logic unless stage boundaries are actively reviewed.

## Assumptions

- `level3` is the conformed analytical layer and `level4` is the consumer-facing mart layer for the first release of `elt_pipeline`.
- SQL remains the primary medium for these stages even though the execution framework moves to Python.
- This document is informed by the discovered `legacy stack A` implementation and the user-provided description of `legacy stack B`, as no local `legacy stack B` folder was found during authoring.

## Open Questions

- Should model metadata live inline with SQL files or in adjacent manifest files?
- What is the target execution engine for SQL in the new platform?
- How should cross-domain shared dimensions be owned and versioned?
- Which quality checks are mandatory for every model versus optional by domain?
- What is the promotion process for breaking schema changes in `level4`?

## Delivery Recommendation

Phase the SQL product as follows:

1. Build the Python model discovery, compilation, and execution runtime.
2. Support metadata manifests, dependency graphs, and date-scoped execution.
3. Migrate one representative domain across `level3` and `level4`.
4. Add standardized validation, lineage, and selective rerun tooling before wider adoption.

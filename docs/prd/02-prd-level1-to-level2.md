# PRD 02: Level 1 to Level 2 Normalization

## Document Status

- Status: Draft
- Product area: `elt_pipeline`
- Stage: `level1` -> `level2`
- Proposed implementation language: Python
- Proposed packaging and environment management: `uv`

## Background

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

The level model and its medallion mapping is defined in [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md).

Production-grade ELT platforms describe `level2` as the first structured and queryable layer. In practice, some deployments use `level2` heavily for converting JSON, XML, CSV, and stream payloads into structured tables, while others treat `level2` as largely redundant except when the source payload is nested or otherwise non-tabular.

The `elt_pipeline` should preserve the useful role of `level2` without forcing every source through an unnecessary stage. This PRD defines `level2` as a normalization layer used only when it adds value.

## Problem Statement

The historical `level2` concept mixes two different needs:

- a necessary parsing and normalization step for semi-structured inputs,
- an unnecessary extra hop for already-tabular or analytics-ready data.

This creates avoidable latency, duplicated configuration, extra storage churn, and confusion about where structure should be introduced.

The new platform needs a crisp product definition for `level2` so teams know:

- when to use it,
- what it is allowed to do,
- what it must never become,
- and when a source may safely bypass it.

## Product Vision

Build a lightweight, explicit normalization framework that converts raw `level1` assets into queryable `level2` datasets only when raw source structure requires it. `level2` exists to parse, flatten, type, and expose source-aligned tables, not to implement business conformance or reporting semantics.

## Goals

- Make `level2` optional by design rather than mandatory by convention.
- Preserve `level2` for nested, semi-structured, multi-record, or schema-ambiguous inputs.
- Standardize parsing, schema application, flattening, and source-aligned table materialization.
- Allow already-tabular inputs to bypass or minimize `level2`.
- Provide downstream consumers with a stable, queryable handoff into SQL-driven modeling.

## Non-Goals

- Business conformance, golden model design, or domain harmonization.
- Consumer/reporting marts.
- Publish/export products.
- Recreating `level2` tables that exist only because of historical design constraints rather than source structure requirements.

## Product Definition

In `elt_pipeline`, `level2` is the source-normalized analytical landing zone.

It is used when at least one of the following is true:

- raw payloads are nested or hierarchical,
- raw payloads require parsing from XML, JSON, CSV variants, Avro, or binary-derived structures,
- raw inputs contain arrays that must be exploded into structured tables,
- raw inputs require type enforcement before reliable SQL use,
- raw source files bundle multiple logical entities into one asset.

It may be bypassed when:

- the source already lands in a tabular format with stable schema,
- the raw extract is directly queryable and fit for SQL modeling,
- the only `level2` value would be mechanical copying without normalization.

## Users and Stakeholders

- Data platform engineers building parsers and source mappings.
- Analytics engineers and SQL model authors consuming source-aligned tables.
- Source owners needing predictable structure from complex raw feeds.
- Platform operators responsible for runtime efficiency and reprocessing behavior.

## Scope

This PRD covers:

- source-to-table normalization from `level1`,
- parsing and schema application,
- structural flattening and array expansion,
- write semantics into `level2`,
- optional stage bypass rules,
- observability and replay requirements for normalization.

This PRD excludes:

- business-rule enrichment,
- cross-source conformance,
- downstream marts and exports.

## Functional Requirements

### FR1. Optional Stage Behavior

The product shall support two execution modes:

- `normalize`: transform `level1` data into structured `level2` tables,
- `bypass`: register or expose source data for downstream SQL without an unnecessary intermediate rewrite.

The decision to normalize or bypass must be explicit in source configuration.

### FR2. Source Mapping Configuration

The framework shall provide source-level mapping definitions that specify:

- source name,
- entity name,
- source format,
- compression type,
- record selection strategy,
- schema definition or schema reference,
- flattening and explode rules,
- write mode,
- partition strategy,
- bypass eligibility.

The configuration should be human-maintainable and versioned independently of runtime code.

### FR3. Supported Input Patterns

The normalization runtime shall support patterns evidenced in the existing stack:

- JSON documents and document collections,
- XML payloads with row-tag or path-based record extraction,
- CSV and delimiter-driven files with configurable headers,
- Avro or message-derived structured payloads,
- raw file manifests or metadata feeds,
- already-tabular Parquet or equivalent sources.

### FR4. Parsing and Extraction

For normalized sources, the framework shall:

- locate records within a raw payload,
- parse source-specific structures,
- extract one or more logical tables from a single asset,
- preserve source-identifying keys back to `level1` artifacts.

### FR5. Schema Enforcement

The framework shall support:

- explicit schemas,
- schema inference under controlled conditions,
- schema evolution policies,
- required versus optional fields,
- type casting with error capture,
- nullability and default handling.

Schema handling must be deterministic and visible in run metadata.

### FR6. Flattening and Expansion

The runtime shall support:

- flattening nested structs,
- exploding arrays into child tables,
- parent-child key propagation,
- extraction of repeated nested objects into separate entities,
- controlled naming conventions for derived tables and columns.

### FR6a. Derived Table Naming and Identifier Limits

The normalization runtime shall produce human-readable and deterministic derived table names.

Hashing of derived table names must be a fallback only, and must trigger only when:

- the derived name would exceed the configured maximum identifier length, or
- the derived name would collide with an existing derived name.

When hashing is applied, the system must emit a mapping artifact that links:

- logical source path (e.g., JSONPath/XPath),
- derived physical table name,
- parent entity/table,
- join key columns.

### FR6b. Mapping Catalog Lifecycle

The normalization runtime shall treat the table and column mapping catalog as a logical schema artifact, not as a per-partition write artifact.

The runtime must:

- emit a mapping catalog per source/entity/mapping version,
- reuse the same mapping catalog across partitions and reruns when the logical structure has not changed,
- create a new mapping version only when the extracted relational structure changes materially,
- record the mapping version used by each normalization run in run metadata.

The runtime must not create a new mapping catalog for every date partition by default.

### FR7. Source-Aligned Table Semantics

`level2` outputs must remain source-aligned rather than business-conformed.

Allowed behavior:

- parsing,
- flattening,
- typing,
- renaming for technical clarity,
- splitting one payload into multiple structured entities.

Disallowed behavior:

- applying domain-wide golden rules,
- joining unrelated sources,
- embedding consumer-specific aggregations,
- introducing reporting semantics that belong in `level3` or `level4`.

### FR8. Write Semantics

The framework shall support:
- append and overwrite modes where appropriate,
- incremental loads for partitioned windows,
- physically efficient file/table output for downstream SQL use.

#### FR8.1. Level 2 Path Grammar

The `level2` storage path contract shall be:

```
level2/source=<src>/entity=<entity>/mapping_version=<v>/ingest_date=<date>/table=<tbl>/run_id=<id>/*.parquet
```

The standardized path segments are:
- stage name: `level2`
- source name: `source=<src>`
- entity name: `entity=<entity>`
- mapping version: `mapping_version=<v>` (the hash of the mapping catalog, changes only when the relational extraction structure changes)
- ingestion date partition: `ingest_date=<YYYY-MM-DD>` (always arrival day from the L1 manifest, immutable once written)
- table name: `table=<tbl>` (the physical name of the normalized child table or root table)
- run identifier: `run_id=<id>`

**Environment handling (consistent with L1):**
- `environment` SHALL NOT appear as an in-path segment.
- Environment is handled exclusively by which `--root-path` (storage root / bucket) the pipeline is pointed at.
- Each environment (dev, staging, prod) gets its own independent storage root.
- `environment` is still retained on manifests and `Level2TableManifest` for audit purposes.

#### FR8.2. Mandatory Lineage Columns (Real Parquet Data Columns)

Every `level2` parquet row SHALL carry the following three lineage columns as real, materialized parquet data columns (not just path segments or tokens):

| Column | Type | Source | Purpose |
|---|---|---|---|
| `source_name` | STRING | L1 manifest `source_name` | Downstream SQL can SELECT this directly; enables L3 `source_name` partitioning |
| `ingest_date` | DATE | L1 manifest `ingest_started_at.date()` | Enables L2 window filtering by arrival day; always present for L3 models to read |
| `_run_id` | STRING | Run context `run_id` | Low-level audit: which specific normalization run produced this row |

These three columns are injected by the normalization runner (primary) and safety-netted by the L2 writer (belt-and-suspenders, `withColumn` if missing before write). Every table in the normalized output, including child tables from array expansion, carries these columns on every row.

This makes `level2` self-describing — the columns exist even if partition discovery is bypassed, and downstream SQL model authors do not need special tokens or configuration to access them.

#### FR8.3. `ingest_date` vs `business_date` Distinction

At `level2` (and `level1`), the date key in the path AND the materialized `ingest_date` column are **always arrival day** (= when the data was received into the platform). This value is immutable once written and serves as the unit of replay.

A separate column `business_date` (= when the event actually happened, extracted from the payload content) may appear in the flattened source fields when the source provides it. Downstream `level3` models read L2 by `ingest_date` window and choose whether to re-partition their output by `business_date` (the default for canonical tables) or retain `ingest_date` partitioning (for snapshot/audit tables). This late-arriving data repartitioning flow is defined in PRD 03.

#### FR8.4. Partitioning Strategy at Level 2

`level2` path segments (`source`, `entity`, `mapping_version`, `ingest_date`) are embedded in the directory structure. These SHALL be recoverable as queryable Spark partition columns via parent-directory reads (see FR10.2), enabling `WHERE source = '...' AND ingest_date BETWEEN ...` filters to prune at the filesystem level.

The `level2` writer does NOT apply Spark `partitionBy()`; the layout is achieved via explicit directory paths. The mandatory lineage columns (FR8.2) ensure the data is self-describing regardless.

#### FR8.5. Load Mode Semantics at Level 2

Writes are always run-scoped: one `run_id=<id>` directory per normalization run. The writer refuses to overwrite an existing run directory (error-on-existing). Replay is achieved by writing a new run, then downstream readers filter by run_id or rely on Spark dynamic partition overwrite at level3.

#### FR8. Writer and Partition Strategy Separation

The normalization runtime shall separate:

- logical relational structure generation,
- mapping catalog generation,
- physical file/table writing,
- partition layout decisions.

Partitioning must be configurable per output entity or table.

The writer contract must support:

- no partitioning when appropriate,
- ingestion-date partitioning,
- business-date partitioning,
- source-aligned key partitioning,
- future extension to composite partition strategies.

The physical partition strategy must not force regeneration of mapping catalogs when the logical mapping is unchanged.

### FR9. Error Isolation and Quarantine

When parsing or typing fails, the system shall support:

- record-level error capture where feasible,
- file-level quarantine when record-level isolation is not practical,
- replay from the original `level1` artifact,
- metrics on dropped, quarantined, and successfully normalized records.

### FR10. Level 1 Traceability

Every `level2` record or partition must be traceable back to:
- source name,
- entity name,
- `level1` artifact path or manifest reference,
- run id,
- normalization mapping version.

#### FR10.1. Real-Column Lineage over Tokens

The primary mechanism for downstream SQL to access lineage information SHALL be the three real parquet data columns defined in FR8.2 (`source_name`, `ingest_date`, `_run_id`). SQL authors can `SELECT source_name, ingest_date FROM t.level2_table` without relying on template tokens or external configuration.

An optional `source.*` token namespace may be added for ergonomics (e.g., `SELECT '{{ source.name }}' AS source_name`), but this is not required and SHALL NOT be the only mechanism for accessing this information.

#### FR10.2. Parent-Directory Read Semantics (Spark Partition Discovery)

The `level2` reader used by downstream SQL models SHALL read parent directories (NOT explicit leaf `run_id=*` paths) so that Spark auto-discovers path segments as queryable partition columns. Specifically:

- **Read pattern:** Read the entity parent prefix (`level2/source=S/entity=E/mapping_version=V/`) or the level2 root prefix, NOT individual `run_id=*` leaf directories.
- **Spark behavior:** When reading parent directories containing `key=value` segments, Spark automatically discovers these as partition columns and makes them available for WHERE-clause filtering with partition pruning (no full table scan).
- **Filter pattern:** To narrow to specific runs or dates, apply `.where("run_id IN (...)")` or `.where("ingest_date BETWEEN ...")` after the read. Spark prunes filesystems partitions, so no extra data is scanned.
- **Result columns:** After read, the dataframe contains all user payload columns + the three mandatory lineage columns (FR8.2) + Spark-discovered partition columns from the path (`source`, `entity`, `mapping_version`, `ingest_date`, `table`, `run_id`).

This pattern avoids the anti-pattern of globbing explicit leaf paths, which suppresses Spark partition discovery and turns path segments into dead string fragments rather than queryable columns.

#### FR10a. Lineage Event Standard (OpenLineage)

The shared observability, audit, and error-handling contract for all stages is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

The normalization runtime shall emit lineage events in an OpenLineage-compatible format.

- At minimum, events must capture: run id, job name, start/end timestamps, input dataset identifiers, output dataset identifiers, and run status.
- The implementation may use a Spark OpenLineage agent or integration to avoid bespoke lineage event schemas.
- Storing lineage events locally is acceptable in v1, but the event format must remain compatible with OpenLineage to enable later integration with a lineage backend.

### FR10b. Low-Cost Output Metrics (Avoid Full Row Counts)

The normalization runtime shall not rely on full-data scans, such as Spark `count()`, as the default mechanism to compute row counts for `level2` outputs.

- If row counts are captured, the preferred method is to read row counts from Parquet footer or row-group metadata after write by summing `num_rows` across output files.
- Alternatively, Spark execution or write metrics may be captured when available, without triggering extra actions that rescan the full dataset.
- Any full-count behavior must be explicitly opt-in and documented as potentially expensive.

### FR10c. Audit Record Per Run

The normalization runtime shall emit one authoritative audit record per run.

The audit record must capture at minimum:

- `run_id`,
- `stage`,
- `job_name`,
- `source_name`,
- `entity_name`,
- `trigger_type`,
- `window_start` and `window_end`,
- `started_at` and `completed_at`,
- `status`,
- `mapping_version`,
- `config_version` or `schema_version`,
- input `level1` artifacts consumed,
- output `level2` tables or partitions produced,
- metrics summary,
- error summary when the run is not successful.

The audit record is the authoritative run summary and must be stored independently of transient console output.

### FR10d. Structured Execution and Error Logging

The normalization runtime shall emit structured execution logs for each run.

Execution logs must:

- include `run_id` on every event,
- use structured fields rather than free-form text only,
- identify component or module, event type, severity, and timestamp,
- capture important lifecycle events such as run start, mapping load, input discovery, write start, write completion, metrics capture, and run completion.

Error logging must be structured and classified.

Each error event must capture at minimum:

- `run_id`,
- `error_code`,
- `error_category`,
- human-readable message,
- source, entity, and table context where available,
- input artifact reference where available,
- retryable versus non-retryable classification.

The implementation may additionally retain Spark engine logs for low-level diagnostics, but Spark logs must not replace the runtime audit record or structured execution log.

### FR11. Replay and Backfill

Operators shall be able to:

- rerun normalization for a specific `level1` partition or date range,
- rerun after schema/mapping changes,
- rebuild a `level2` dataset from the immutable raw zone without re-ingesting the source.

## Non-Functional Requirements

### NFR1. Performance

- The framework must handle large nested payloads efficiently.
- Normalization should scale horizontally by source, entity, and partition.

### NFR2. Cost Efficiency

- `level2` should only exist where it adds technical value.
- Sources that can safely bypass `level2` should avoid unnecessary rewrites.

### NFR3. Reliability

- Failed normalization runs must not corrupt or partially advance target partitions silently.
- Rebuilds from `level1` must be deterministic.

### NFR4. Maintainability

- Source-specific parsing logic should be isolated behind reusable readers or adapters.
- Mapping definitions should be simpler than the current fragmented legacy config model.

### NFR5. Observability

- Metrics must capture payloads processed, records produced, parse failures, schema drift, and output partitions written.

## Proposed Product Design

### Normalization Runtime

The Python implementation should provide a shared normalization engine that:

- loads source mappings,
- reads raw `level1` assets,
- dispatches to format-specific readers,
- applies schema and flattening rules,
- materializes one or more `level2` outputs,
- emits run lineage and quality metrics.

### Suggested Package Areas

- `elt_pipeline.normalize.readers`
- `elt_pipeline.normalize.mappings`
- `elt_pipeline.normalize.schema`
- `elt_pipeline.normalize.writer`
- `elt_pipeline.normalize.lineage`

### Configuration Model

The new configuration approach should replace the current split between HOCON, JSON source mappings, and code-level assumptions with a simpler contract.

Each source mapping should express:

- ingestion source reference,
- raw format and compression,
- record path strategy,
- entity extraction plan,
- schema definitions,
- table naming strategy,
- mapping catalog policy,
- physical partition strategy,
- bypass policy.

### Stage Bypass Policy

The platform should support three source classes:

1. `required_level2`
   Use `level2` because the source is nested, semi-structured, or needs structural normalization.

2. `lightweight_level2`
   Use `level2` for minimal typing or layout standardization, but avoid unnecessary transformations.

3. `bypass_level2`
   Skip physical normalization and expose the source directly to `level3` modeling when technically safe.

## Data Contract for Level 2

Each `level2` entity must publish:
- source name,
- entity name,
- schema version,
- mapping version,
- source run id,
- `level1` lineage reference,
- partition keys,
- record counts,
- error and quarantine counts.

Every `level2` parquet row SHALL include the following mandatory lineage columns (see FR8.2):
| Column | Type | Present |
|---|---|---|
| `source_name` | STRING | Always, in every table (root + child) |
| `ingest_date` | DATE | Always, in every table (root + child) |
| `_run_id` | STRING | Always, in every table (root + child) |

These are real, materialized data columns in the parquet files — not tokens, not path-only fragments. They are accessible via plain `SELECT` in any downstream SQL model.

The `level2` path grammar (see FR8.1) is:
```
level2/source=<src>/entity=<entity>/mapping_version=<v>/ingest_date=<date>/table=<tbl>/run_id=<id>/*.parquet
```

`ingest_date` at level2 is **always arrival day** (when the data was received, from the L1 manifest). Event-date partitioning (`business_date`) happens downstream at L3, not here. See FR8.3 and PRD 03 for the late-arriving data repartitioning flow.

The mapping catalog for a normalized source/entity must publish:
- mapping version,
- logical source path,
- physical table name,
- parent table reference where applicable,
- join key definitions,
- logical-to-physical column mappings.

## Success Metrics

- Teams can clearly decide whether `level2` is required for a source.
- A nested source can be normalized through configuration plus reusable framework code.
- A tabular source can bypass `level2` without custom exceptions in orchestration.
- `level2` rebuilds are possible from immutable raw data.
- Downstream SQL authors receive stable, queryable source-aligned datasets.

## Acceptance Criteria

- The Python `uv` project can normalize at least one JSON source and one XML or CSV source from `level1` into `level2`.
- The framework supports child-table extraction from nested data.
- The runtime records lineage from `level2` outputs back to `level1` artifacts.
- At least one already-tabular source can be configured to bypass physical `level2` transformation.
- Schema errors are surfaced with quarantine or explicit run failure semantics.
- Mapping catalogs are versioned by logical structure and are not regenerated per partition by default.
- At least two different partition strategies can be configured without changing logical mapping behavior.

## Migration Considerations

- Historical `level2` datasets should be classified into required, lightweight, and bypass candidates.
- Historical `level2` tables that only copy tabular source data should be strong candidates for removal.
- Sources with brittle nested parsing should be migrated early enough to validate the normalization framework.

## Risks

- Teams may continue to push business logic into `level2` unless boundaries are enforced in design and review.
- Stage bypass can create inconsistency if not reflected clearly in lineage and orchestration.
- Deeply nested sources may require more expressive mapping rules than a minimal configuration model initially supports.

## Assumptions

- `level2` remains part of the architecture, but only as a technical normalization layer.
- A core design constraint applies: `level2` should not exist as mandatory ceremony.
- This document defines baseline normalization design requirements consistent with production-grade ELT best practices, including optional stage bypass, nested source flattening, and schema-stable source-aligned table materialization.

## Open Questions

- Should bypassed sources materialize metadata-only registrations, or should they be referenced directly by downstream models?
- Which schema definition approach should be primary: declarative files, Python models, or generated contracts?
- How much source-specific parsing logic belongs in reusable readers versus per-source plugins?
- What is the target table format for `level2` in the new platform?

## Delivery Recommendation

Phase the normalization product as follows:

1. Implement the common normalization engine and source mapping contract.
2. Prove nested extraction on a representative complex source.
3. Add bypass support for a representative tabular source.
4. Classify existing sources and migrate them by source class rather than one-for-one pipeline parity.

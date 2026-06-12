# PRD 03: SQL Transforms from Level 2 to Level 3 and Level 3 to Level 4

## Document Status

- Status: Draft v1
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

`level4` is the consumer/publish-facing analytical serving layer for this phase of the platform.

It is responsible for:

- domain-specific reporting datasets,
- denormalized consumer-oriented tables,
- performance-oriented aggregations,
- mart-style outputs aligned to business use cases.

This PRD intentionally stops at `level4`. External file exports and other downstream publish mechanics can be defined later if needed.

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

## Data Contract for Level 4

Each `level4` model must document:

- consumer use case,
- business grain,
- freshness SLA or expectation,
- downstream dependency or audience,
- quality expectations,
- breaking change policy.

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

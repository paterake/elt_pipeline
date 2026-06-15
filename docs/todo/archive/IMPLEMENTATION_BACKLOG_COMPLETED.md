# Implementation Backlog (Completed Snapshot)

## Purpose

This file is the completed snapshot of the implementation continuity backlog for `elt_pipeline`.

It is kept for historical continuity and detailed review.

## Contents

The snapshot below reflects the fully completed backlog for the currently approved scope.

---

# Implementation Backlog

## Purpose

This file is the continuity backlog for implementation sessions.

Use it as the working handoff document between sessions.

It captures:

- the approved requirements baseline
- the agreed implementation order
- the next build steps
- the open decisions still allowed to evolve during implementation

## Implementation Baseline

Implementation must follow the approved PRDs under `docs/prd/`, especially:

- `00-prd-platform-principles.md`
- `00-prd-architecture-levels-and-governance.md`
- `00-prd-shared-observability-audit-and-error-handling.md`
- `00-prd-oss-adoption-strategy.md`
- `01-prd-ingestion-raw-to-level1.md`
- `02-prd-level1-to-level2.md`
- `03-prd-sql-level2-to-level3-and-level3-to-level4.md`
- `04-ingestion-inventory-legacy-baseline.md`
- `05-ingestion-technique-deep-dive.md`
- `06-prd-level4-to-level5-publish-and-export.md`

## Confirmed v1 Decisions

- Custom platform solution; OSS only where it supports the framework
- Client-neutral product contract
- DAMA-DMBOK v2-aligned architecture and runtime principles
- YAML config in `elt_pipeline_cfg`
- Local storage target for v1
- Local JSON state/checkpoint store for v1
- Scheduled and on-demand execution for v1
- `rest` connector family implemented first
- Kafka support via Kafka client consumer, not Spark streaming
- `level1` is raw landing
- `level2` is the first relationalised/source-aligned parquet layer
- `level3` is canonical/conformed
- `level4` and `level5` are consumer/publish layers
- Hashing of derived table names is fallback-only
- Mapping catalogs are versioned logical artifacts, not regenerated per partition by default
- `_row_id` is the standard row identifier name and is generated as a GUID/UUID

## Implementation Order

1. Runtime foundation
2. Config loading and validation from `elt_pipeline_cfg`
3. Shared run context
4. Shared audit/logging/error-handling framework
5. Local level1 writer
6. Local state/checkpoint store
7. REST connector family
8. Level1 to Level2 normalization for REST-shaped payloads
9. SQL connector family
10. Object storage connector family
11. Kafka connector family

## Active Backlog

### Phase 1: Runtime Foundation

- [x] Create Python project scaffold managed by `uv`
- [x] Create package layout under `src/elt_pipeline/`
- [x] Add CLI entrypoint structure
- [x] Add test scaffold and baseline test runner
- [x] Define module boundaries for `config`, `ingest`, `normalize`, `sql`, and `shared`

### Phase 2: Config Contract

- [x] Implement config loader for `elt_pipeline_cfg`
- [x] Implement YAML parsing and schema validation
- [x] Implement config layering:
- [x] global defaults -> environment overlay -> source -> entity
- [x] Implement `schema_version` handling
- [x] Add config validation errors aligned to runtime error taxonomy

### Phase 3: Shared Runtime Contracts

- [x] Implement `run_id` generation and run context object
- [x] Implement authoritative audit record model
- [x] Implement structured execution logging model
- [x] Implement structured error taxonomy and error records
- [x] Implement lineage event model with OpenLineage-compatible shape
- [x] Implement low-cost metrics capture contract

### Phase 4: Local Storage and State

- [x] Implement local level1 storage writer
- [x] Implement level1 manifest / artifact metadata contract
- [x] Implement local JSON state/checkpoint store
- [x] Implement local run/audit artifact storage layout
- [x] Implement replay and backfill primitives over local state

### Phase 5: REST Connector

- [x] Implement REST connector base abstractions
- [x] Implement request templating
- [x] Implement date/window parameter handling
- [x] Implement auth strategy support for v1 REST flows
- [x] Implement token acquisition + token injection
- [x] Implement retries / timeout / error classification
- [x] Implement pagination
- [x] Implement envelope extraction support
- [x] Implement payload decoding hooks
- [x] Implement raw landing of responses into level1
- [x] Implement checkpoint update only after durable persistence

### Phase 6: Level1 to Level2 Normalization

- [x] Implement normalization runner
- [x] Implement source-aligned relationalisation for REST-shaped payloads
- [x] Implement flattening of nested structures
- [x] Implement parent-child table generation
- [x] Implement `_row_id`, `_parent_row_id`, and `_array_index`
- [x] Implement table naming rules with hash fallback only
- [x] Implement mapping catalog lifecycle and persistence
- [x] Implement per-entity partition strategy
- [x] Implement lineage from level1 artifacts to level2 outputs
- [x] Implement low-cost output metrics via parquet metadata or Spark metrics
- [x] Add CSV normalization support for source-aligned tabular level1 payloads
- [x] Add explicit `level2_mode` config and `bypass_level2` handling for tabular sources

### Phase 7: Next Connector Families

- [x] Implement SQL connector family
- [x] Implement object storage connector family
- [x] Implement Kafka client-based connector family

### Phase 8: SQL Transforms (Level2->Level3 and Level3->Level4)

- [x] Add SQL model package layout and discovery rules
- [x] Define model manifest schema and validation aligned to shared error taxonomy
- [x] Implement tokenization/parameterization and compile-only mode
- [x] Implement dependency graph build and deterministic topological execution
- [x] Implement local materializations (full refresh, append, partition overwrite)
- [x] Implement SQL-stage audit/log/lineage emission to local artifact store
- [x] Implement model validations and capture results in audit record
- [x] Add minimal example SQL model package for local mode

### Phase 9: Pipeline CLI Commands

- [x] Add `elt-pipeline ingest ...` commands to run configured sources/entities
- [x] Add `elt-pipeline normalize ...` commands to run level1->level2 normalization
- [x] Add `elt-pipeline sql compile ...` and `elt-pipeline sql run ...` commands
- [x] Add CLI selection flags (stage/domain/model + deps, window/backfill, validate-only)

### Phase 10: Orchestration (Scheduled and On-Demand v1)

- [x] Define job/trigger/window runtime contract feeding run context + checkpoints
- [x] Implement local scheduler option that calls CLI deterministically
- [x] Implement selective rerun primitives for normalization and SQL stages

### Phase 11: Testing and Hardening

- [x] Add SQL transform runtime tests (discovery, graph, compile, materializations, validations)
- [x] Add end-to-end local integration tests (ingest -> normalize -> SQL transforms)
- [x] Add structured error codes for SQL runtime aligned to shared taxonomy
- [x] Add dry-run/explain mode for SQL stage execution planning

### Phase 12: Documentation and Operator Enablement

- [x] Expand `README.md` with install, CLI, and local workflow guidance
- [x] Add example schedule plan under `examples/schedules/`
- [x] Document end-to-end local usage from ingest through SQL execution
- [x] Add runnable example pipeline configs under `examples/` for local connector workflows
- [x] Add operator runbook for rerun, backfill, and schedule-driven execution
- [x] Add troubleshooting guidance for local artifacts, checkpoints, and audit outputs

## Current Status

The implementation backlog for the approved v1 PRD scope is complete through `level4`, local scheduling, and operator documentation.

The first post-v1 design step for `level5` has now been approved in `docs/prd/06-prd-level4-to-level5-publish-and-export.md`.

The next post-v1 design baseline for optional integrations is now approved in `docs/prd/07-prd-optional-platform-integrations.md` and implementation can proceed in the sequenced order defined there.

The approved `level5` direction is:

- `level1` is raw ingested data
- `level2` is relationalized/parquet source-aligned data
- `level3` is the canonical standardized warehouse model
- `level4` is the consumer-specific datamart table layer for direct analysis
- `level5` is transformed static output that a consumer can pick up directly

Consumers may either analyze `level4` tables directly or consume static/canned `level5` outputs.

The product positioning has also been clarified for handoff purposes:

- `elt_pipeline` should be described as a governed data platform runtime, not only as an ingestion/transformation tool
- the `level1` through `level5` model is the platform's chosen architecture pattern
- that layer model is aligned to DAMA-DMBOK v2 concerns such as data architecture, integration, metadata, quality, security, and auditability
- the docs should not imply that DAMA-DMBOK v2 prescribes these exact level names directly

Further work should be treated as post-v1 continuation and must remain within the approved `level5` contract unless the PRD is expanded first.

## Post-v1 Continuation Backlog

Some items below are ready for implementation planning, while others require PRD/design approval before build work begins.

### Phase 13: Release Engineering and CI

- [x] Add repository CI to run `uv sync --extra dev`, `uv run ruff check .`, and `uv run pytest`
- [x] Add package build validation for source and wheel artifacts
- [x] Add smoke validation that bundled examples still parse and execute the documented happy paths
- [x] Document the supported local development and release workflow for maintainers

### Phase 14: Optional Platform Integrations

This phase is approved for implementation, provided local-first behavior and the authoritative CLI/runtime contracts are preserved.

- [x] Draft PRD/design baseline for optional lineage, orchestration, and data-quality integrations
- [x] Define adapter boundaries for optional lineage backend emission while preserving local-first runtime behavior
- [x] Add one reference lineage backend integration aligned to the OpenLineage-compatible event contract
- [x] Define adapter boundaries for optional orchestration wrappers while keeping the CLI contract authoritative
- [x] Add one reference orchestration integration that invokes the existing CLI entrypoints without redefining runtime contracts
- [x] Define optional data-quality hook points around normalization and SQL stage outputs

### Phase 15: `level5` Publish and Export Capability

This phase is now approved for initial implementation of local file-based delivery.

- [x] Draft a PRD for `level5` publish/export outputs
- [x] Review and approve the `level5` PRD before implementation begins
- [x] Preserve the agreed boundary that `level4` remains queryable datamarts and `level5` remains static pickup outputs
- [x] Preserve the agreed positioning that `elt_pipeline` is a governed data platform runtime aligned to DAMA-DMBOK v2 principles
- [x] Define publish/export artifact manifests, delivery metadata, and rerun semantics
- [x] Confirm the first local export formats and replacement modes for implementation
- [x] Sequence the initial `level5` runtime and CLI work after PRD approval
- [x] Add a `publish` runtime package and CLI command structure aligned to the level boundary model
- [x] Implement publish definition discovery and manifest validation
- [x] Add `publish explain` / validation-first CLI behavior for local definitions
- [x] Implement one representative local CSV export target with run-scoped manifest generation
- [x] Add stage-aware audit, log, and lineage emission for `level5` publish runs
- [x] Add focused publish discovery, runtime, and CLI tests
- [x] Add operator guidance and runnable examples for publish/export workflows

## First Session Target

The next implementation session should aim to complete:

- [x] Python project scaffold
- [x] package structure
- [x] CLI skeleton
- [x] config loader skeleton
- [x] shared run context
- [x] audit/logging/error framework skeleton

## Next Session Target

The next implementation session should aim to complete:

- [x] Add runnable `examples/publish/` definitions and local happy-path documentation
- [x] Add operator runbook guidance for `publish validate`, `publish explain`, and `publish run`
- [x] Extend publish runtime beyond CSV-only execution to include `jsonl`
- [x] Add first replacement-mode enforcement beyond `versioned_delivery` and `overwrite_in_place`
- [x] Define rerun/backfill semantics for publish runs using prior audit artifacts

## Future Session Target

A later session should aim to complete:

- [x] Add packaging/archive support and broader file-based delivery formats (zip bundle + tsv)
- [x] Add focused publish/runtime tests for zip bundle + tsv modes
- [x] Add operator runbook updates and runnable examples for zip bundle + tsv publish definitions

## Session Handoff Notes

Use this when resuming in a new session:

- Resume prompt: `from docs/todo/IMPLEMENTATION_BACKLOG.md continue with the next approved post-v1 item`
- Implementation provenance inputs (legacy baseline sources + config mapping notes) are captured in `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.md`.
- The authoritative `level5` PRD is `docs/prd/06-prd-level4-to-level5-publish-and-export.md`.
- The approved Phase 14 integration baseline is `docs/prd/07-prd-optional-platform-integrations.md`.
- The key clarification is that `level4` is still the consumer datamart/table layer.
- `level5` is not another datamart layer; it is for transformed static files or canned outputs a consumer picks up directly.
- `elt_pipeline` should be described as a governed data platform runtime aligned to DAMA-DMBOK v2 principles.
- The `level1` through `level5` structure is a platform-defined architecture model aligned to DAMA-DMBOK v2 concerns, not a claim that DAMA-DMBOK v2 prescribes those exact level names.
- The approved first build slice should focus on local file-based delivery only, not external transports.
- The first required export formats are CSV and `jsonl`, with `tsv` and zip bundling now available as broader local file-based publish options.
- Every publish run must produce run-scoped artifacts and a run-scoped manifest even if a stable consumer-facing path is also maintained.
- The current implementation supports publish definition discovery, validation, explain-mode, and local CSV, `jsonl`, and `tsv` execution against sqlite-backed `level4` tables, plus optional zip bundle packaging.
- The current implementation supports `versioned_delivery`, `overwrite_in_place`, and `append_new_artifact` as executable replacement behaviors.
- The current implementation supports `publish run --rerun-run-id <prior-run-id>` to restore publish selection and window scope from prior publish audit artifacts, plus `--backfill` tagging for historical publish windows.
- The current Phase 14 implementation now routes lineage writes through an internal adapter boundary that preserves local `lineage.jsonl` artifacts and records optional remote-emission failures in local logs/errors.
- The current Phase 14 implementation now includes a reference `openlineage_http` backend integration enabled by environment variables, preserving local-first lineage artifacts and supporting `best_effort` or `blocking` remote emission policy.
- The current Phase 14 implementation now includes an orchestration wrapper boundary in `elt_pipeline.integrations.orchestration` that standardizes CLI subprocess invocation and optional orchestration metadata propagation via environment variables while keeping the CLI authoritative.
- The current Phase 14 implementation now includes a reference `AirflowCliWrapper` integration and bundled `examples/orchestration/airflow/reference_dag.py`, both of which call existing CLI entrypoints and attach Airflow metadata without requiring Airflow in the base install.
- The current Phase 14 implementation now includes an optional quality-hook boundary in `elt_pipeline.integrations.quality` that runs after normalization and SQL outputs, records pass/warn/fail/skipped results in local audit/log artifacts, and supports a local reference `row_count_threshold` backend with `best_effort` or `blocking` policy.
- The current Phase 14 implementation is now documented in `README.md` and `docs/operator/LOCAL_OPERATOR_RUNBOOK.md`, including enablement, disablement, and failure-mode guidance for the optional lineage, orchestration, and quality integrations.
- Optional Phase 14 integration work must preserve local-first execution, keep the CLI authoritative, and treat local audit/log/lineage artifacts as first-class even when external systems are added.

## Approved Scope Status

All currently approved implementation items in this backlog are complete.

The current repository state also passes the local automated verification baseline:

- `uv run pytest` passes
- there are no current workspace diagnostics reported by the IDE tooling

No further feature implementation should begin until one of the following is true:

- a new PRD is added and approved
- an existing approved PRD is expanded and re-approved
- or the next session is explicitly limited to bug fixing, hardening, or documentation corrections that stay within the already approved contracts

## Next Session Guidance

Use the following order when resuming after this point:

1. Confirm whether the session is meant to stay within the current approved scope or begin a new PRD/design step.
2. If staying within scope, prioritize bug fixes, test hardening, operator experience gaps, or implementation cleanup that does not change the platform contract.
3. If expanding scope, update the relevant PRD first, then update this backlog, then implement.

## Open Decisions Still Allowed During Implementation

These may still be refined during implementation, provided they stay consistent with the PRDs:

- exact Python library choices for YAML/schema validation
- exact local filesystem layout for audit, logs, lineage, and state artifacts
- exact OpenLineage event file persistence approach for local mode
- exact CLI command naming and subcommand breakdown
- exact package/module naming within the approved architectural boundaries

## Change Control

When implementation reveals a meaningful contract change:

1. update the relevant PRD first
2. update this backlog
3. then implement the change

Do not let implementation silently redefine the platform contract.

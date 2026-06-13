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
- [ ] Implement selective rerun primitives for normalization and SQL stages

### Phase 11: Testing and Hardening

- [ ] Add SQL transform runtime tests (discovery, graph, compile, materializations, validations)
- [ ] Add end-to-end local integration tests (ingest -> normalize -> SQL transforms)
- [x] Add structured error codes for SQL runtime aligned to shared taxonomy
- [x] Add dry-run/explain mode for SQL stage execution planning

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

- [ ] Implement selective rerun primitives for normalization and SQL stages
- [ ] Add SQL transform runtime tests (discovery, graph, compile, materializations, validations)
- [ ] Add end-to-end local integration tests (ingest -> normalize -> SQL transforms)

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
EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-8341bcebd9c84df6a41b5cf183e91cba/cwd.txt'; exit "$__tr_native_ec"

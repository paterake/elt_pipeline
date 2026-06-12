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
- [ ] Implement retries / timeout / error classification
- [ ] Implement pagination
- [ ] Implement envelope extraction support
- [ ] Implement payload decoding hooks
- [ ] Implement raw landing of responses into level1
- [ ] Implement checkpoint update only after durable persistence

### Phase 6: Level1 to Level2 Normalization

- [ ] Implement normalization runner
- [ ] Implement source-aligned relationalisation for REST-shaped payloads
- [ ] Implement flattening of nested structures
- [ ] Implement parent-child table generation
- [ ] Implement `_row_id`, `_parent_row_id`, and `_array_index`
- [ ] Implement table naming rules with hash fallback only
- [ ] Implement mapping catalog lifecycle and persistence
- [ ] Implement per-entity partition strategy
- [ ] Implement lineage from level1 artifacts to level2 outputs
- [ ] Implement low-cost output metrics via parquet metadata or Spark metrics

### Phase 7: Next Connector Families

- [ ] Implement SQL connector family
- [ ] Implement object storage connector family
- [ ] Implement Kafka client-based connector family

## First Session Target

The next implementation session should aim to complete:

- [x] Python project scaffold
- [x] package structure
- [x] CLI skeleton
- [x] config loader skeleton
- [x] shared run context
- [x] audit/logging/error framework skeleton

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

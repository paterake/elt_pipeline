# Spark Correction Backlog

This file is the active backlog for aligning `elt_pipeline` to the clarified requirement that Spark is the only execution engine used for downstream processing.

This backlog also assumes file-based storage is the persistence model for the platform (no sqlite-backed warehouse runtime and no dependency on a separate database engine for `level2+`).

## Status

- Backlog status: active
- Driver: corrected target-state requirement
- Scope type: corrective alignment
- Priority: high

## Why This Backlog Exists

The archived implementation backlog was completed against a local-first execution slice that currently uses:

- local file persistence for `level1`
- local `jsonl` persistence for current `level2` outputs
- sqlite-backed SQL execution for current `level3` and `level4` materialization
- sqlite-backed publish input for current `level5` exports

That implementation is not sufficient if Spark is the only approved engine from transformation onward.

## Target Correction

The corrected target state is:

- ingest sources into `level1`
- transform `level1 -> level2` using Spark-compatible storage and processing
- use Spark SQL for `level2 -> level3`
- use Spark SQL for `level3 -> level4`
- use Spark-backed inputs for `level4 -> level5` publish/export flows
- persist `level2+` outputs as file-backed datasets (paths) suitable for Spark reads and writes
- remove sqlite as the execution engine for SQL-stage and publish-stage runtime paths
- treat storage access as URI/path based so the same contracts can work across local filesystems and object stores (for example `file://` and `s3a://`)

## Storage Model Stance

- Data-plane (`level2+` datasets): handled by Spark reads/writes over URI locations (Hadoop filesystem connectors), so `elt_pipeline` should not introduce a separate storage facade for dataset IO.
- Control-plane (run-scoped audit/log/error/lineage/manifests): must have an explicit policy:
  - either remain local filesystem even when datasets are remote, or
  - become URI-addressable and written via Spark/Hadoop filesystem access, or
  - be handled by a small platform storage abstraction that supports local and object-store backends.

## Current Gap Summary

- `level2` storage is not currently written in the intended Spark-first physical contract
- SQL runtime is currently sqlite-based rather than Spark SQL based
- publish runtime currently reads from sqlite-backed `level4` outputs rather than Spark-accessible warehouse outputs
- examples, tests, and operator guidance currently describe or depend on local sqlite execution for downstream stages
- the docs and backlog status need to distinguish the archived local-first completion from the active Spark correction path
- file storage access for control-plane artifacts (audit/log/error/manifest/lineage) is currently implemented as local filesystem paths and needs an explicit stance for non-local backends

## Active Backlog

### Phase 1: Requirements and Contract Correction

- [ ] Confirm the authoritative physical format for `level2` under the Spark-only model
- [ ] Confirm whether the storage standard is parquet-only or a table format backed by files (delta/iceberg/hudi)
- [ ] Confirm whether Spark is required only from `level2` onward or also for selected ingestion paths
- [ ] Update PRD wording anywhere the current local sqlite execution path is implied as acceptable target state
- [ ] Define the canonical runtime contract for Spark session creation, configuration, and environment handling
- [ ] Confirm whether a metastore/catalog is allowed or whether the system must remain path-only
- [ ] Confirm the storage access model:
  - Spark-handled storage for data-plane datasets (Spark reads/writes via `file://`, `s3a://`, etc)
  - platform-handled storage for control-plane artifacts (audit/log/error/manifest/lineage), or Spark/Hadoop filesystem access for those too
  - confirm which URI schemes must be supported (`file://`, `s3a://`, others)

### Phase 2: Level2 Physical Storage Alignment

- [ ] Replace current `level2` local `jsonl` table persistence with Spark-first physical output
- [ ] Preserve manifesting, partitioning, mapping catalog persistence, and deterministic artifact paths
- [ ] Preserve run-scoped audit, log, lineage, and error artifacts during Spark-backed writes
- [ ] Define how schema evolution and partition overwrite semantics work for Spark-written `level2`
- [ ] Decide whether `level2` datasets are written purely by Spark (recommended) and whether non-local storage URIs must be supported in the initial correction slice

### Phase 3: SQL Runtime Replacement

- [ ] Replace sqlite execution in the SQL stage with Spark SQL execution
- [ ] Preserve model discovery, validation, dependency ordering, tokenization, and explain-mode behavior
- [ ] Define Spark-native materialization behavior for `full_refresh`, `append`, and `partition_overwrite`
- [ ] Rework validations and execution metrics to use Spark-native query/write evidence
- [ ] Ensure `level3` and `level4` outputs are materialized as file-backed datasets (and only registered as tables/views when needed for SQL execution)

### Phase 4: Publish Runtime Alignment

- [ ] Replace sqlite-backed publish reads with Spark-backed reads from approved `level4` outputs
- [ ] Preserve direct-vs-query selection behavior where still approved
- [ ] Define whether publish queries execute through Spark SQL, DataFrame export logic, or both
- [ ] Preserve run-scoped manifests, audit/log/error artifacts, and lineage for publish runs
- [ ] Reconfirm export format and packaging behavior against the Spark-backed runtime

### Phase 5: CLI, Examples, and Operator Flow

- [ ] Update CLI contracts where engine-specific wording or assumptions currently point to sqlite
- [ ] Replace sqlite-based local examples with Spark-backed runnable examples
- [ ] Update operator runbooks to describe Spark runtime prerequisites and execution patterns
- [ ] Update README guidance to describe the corrected Spark-first architecture

### Phase 6: Testing and Verification

- [ ] Replace or expand sqlite-based downstream tests with Spark-backed tests
- [ ] Add focused tests for Spark-backed `level2` writes, SQL materialization, and publish exports
- [ ] Add end-to-end verification from ingest through publish using the corrected Spark path
- [ ] Reconfirm diagnostics, local development workflow, and CI expectations for Spark-enabled execution

## Completion Criteria

This backlog is complete only when all of the following are true:

- no downstream runtime path depends on sqlite for `level2`, `level3`, `level4`, or `level5`
- the implemented transformation engine from `level2` onward is Spark-based
- publish/export reads from Spark-backed file outputs rather than sqlite-backed tables
- examples, tests, runbooks, and README all reflect the Spark-only target
- the top-level `docs/todo/TODO.md` tracker can mark this backlog as archived or complete

## Notes

- Archived backlog files remain valid as a historical record of the earlier local-first implementation slice.
- They should not be interpreted as proof that the Spark-only target has already been delivered.

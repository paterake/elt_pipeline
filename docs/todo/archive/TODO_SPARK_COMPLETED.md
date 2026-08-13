# Spark Correction Backlog (Completed Snapshot)

This file is the completed snapshot of the Spark correction backlog. It is the authoritative history; a summary entry is indexed in `docs/todo/TODO.md` under Archived backlogs.

## Completion Summary

All completion criteria below were met:

- No downstream runtime path (`normalize`, `sql`, `publish`) depends on sqlite for `level2`, `level3`, `level4`, or `level5`. `src/elt_pipeline/sql/executor.py` (sqlite-based) was deleted and replaced by `src/elt_pipeline/sql/spark_executor.py`.
- `level2` is written as Spark parquet via `SparkLevel2Writer` (`src/elt_pipeline/normalize/level2_storage.py`). Note: written with plain `.parquet(path)`, not `.partitionBy()`; the `source=`/`entity=`/`ingest_date=` path segments are addressing metadata, not queryable partition columns. See `docs/todo/archive/TODO_PATHING_COMPLETED.md` for the completed path/partition management backlog that followed this one.
- `level3`/`level4` are materialized as Spark-written parquet under an explicit `--warehouse-root`, one directory per table, flat-namespaced by `target.table_name` (`src/elt_pipeline/sql/spark_executor.py`).
- The level2->level3 gap identified during this work (no code previously loaded level2 into the SQL stage) was closed by adding an explicit `sources` field to level3 model manifests (`SqlModelSourceRef`), resolved via `src/elt_pipeline/sql/level2_source.py` (`Level2DatasetLocator`) into Spark temp views.
- `publish` reads `level4` parquet via Spark (`src/elt_pipeline/publish/runtime.py`) instead of a sqlite connection.
- Examples (`examples/sql/local_demo/`, `examples/publish/local_demo/`, `examples/schedules/local_demo.yaml`), CLI-level tests (`tests/test_cli.py`, `tests/test_publish_cli.py`, `tests/test_examples.py`), README.md, and operator/maintainer docs all reflect the Spark-only target. CI (`.github/workflows/ci.yml`) installs a JDK for `pyspark`.

Deliberately descoped (not part of this backlog's approved scope, per user decisions captured during implementation):

- Object storage URIs (`s3a://` etc.) for `level2+` — local filesystem only, consistent with "local filesystem on a developer laptop" as the default initial environment.
- A persistent Hive/Glue-style metastore or catalog — path-only, Spark temp views per run.
- Delta/Iceberg table formats — plain Parquet, path-based, using Spark's dynamic `partitionOverwriteMode` for `partition_overwrite` semantics instead.
- Wiring up `elt_pipeline_cfg` (the sibling config repo) — config remains single-file YAML.
- Rewriting the level1->level2 relationalization walk as native Spark DataFrame transforms — it stays a single-process Python walk (`normalize/runner.py`), with Spark introduced only at the write boundary.

## Status (at completion)

- Backlog status: complete
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
- transform `level1 -> level2` using Spark-based processing and Spark-compatible storage
- use Spark SQL for `level2 -> level3`
- use Spark SQL for `level3 -> level4`
- use Spark-backed inputs for `level4 -> level5` publish/export flows
- persist `level2+` outputs as file-backed datasets (paths) suitable for Spark reads and writes
- remove sqlite as the execution engine for SQL-stage and publish-stage runtime paths
- treat storage access as URI/path based so the same contracts can work across local filesystems and object stores (for example `file://` and `s3a://`)

## Scope

- In scope:
  - Spark-based execution for `level1 -> level2 -> level3 -> level4 -> level5`
  - global storage-tier configuration for `level1` through `level5`
  - file-backed datasets for all `level2+` persistence (filesystem initially, object storage later)
- Out of scope:
  - replacing the ingestion connectors with Spark (ingest remains non-Spark and produces `level1`)

## Storage Model Stance

- Data-plane (`level2+` datasets): handled by Spark reads/writes over URI locations (Hadoop filesystem connectors), so `elt_pipeline` should not introduce a separate storage facade for dataset IO.
- Control-plane (run-scoped audit/log/error/lineage/manifests): must have an explicit policy:
  - either remain local filesystem even when datasets are remote, or
  - become URI-addressable and written via Spark/Hadoop filesystem access, or
  - be handled by a small platform storage abstraction that supports local and object-store backends.
- Storage-tier definition should be global, not repeated per source/entity config.
- The global contract should define:
  - storage backend/type (for example `file`, `s3`)
  - root path/prefix where applicable
  - physical name for each tier: `level1`, `level2`, `level3`, `level4`, `level5`
- For object storage, each tier name may map to a distinct bucket/container or to a tier-specific prefix under a shared root, but the contract must make that explicit and consistent.

## Proposed Global Storage Contract

The preferred direction is a single global storage section in the pipeline config rather than duplicating storage definitions across individual source/entity entries.

Illustrative shape:

```yaml
storage:
  backend: s3
  root_path: s3://platform-root
  tiers:
    level1: raw-ingest
    level2: source-aligned
    level3: canonical-warehouse
    level4: consumer-datamarts
    level5: consumer-exports
```

Interpretation rules to confirm during implementation:

- For `file`, tier names likely become directory names under `root_path`.
- For `s3`, tier names may be bucket names or prefixes depending on the agreed storage convention.
- The runtime should resolve all dataset paths from this global contract rather than from per-entity storage declarations.

## Current Gap Summary

- `level2` storage is not currently written in the intended Spark-first physical contract
- SQL runtime is currently sqlite-based rather than Spark SQL based
- publish runtime currently reads from sqlite-backed `level4` outputs rather than Spark-accessible warehouse outputs
- examples, tests, and operator guidance currently describe or depend on local sqlite execution for downstream stages
- the docs and backlog status need to distinguish the archived local-first completion from the active Spark correction path
- file storage access for control-plane artifacts (audit/log/error/manifest/lineage) is currently implemented as local filesystem paths and needs an explicit stance for non-local backends

## Active Backlog

### Phase 0: Spark Runtime Enablement

- [ ] Add a Spark runtime dependency strategy:
  - `pyspark` as an optional extra, or
  - external Spark install only (and Python uses `spark-submit`), or
  - both supported with a single contract
- [ ] Define how a Spark session is created (builder config, app name, warehouse dir, packages)
- [ ] Define how Spark config is provided:
  - pipeline config `storage` section
  - environment variables
  - CLI overrides
- [ ] Define the default initial environment: local filesystem on a developer laptop

### Phase 1: Requirements and Contract Correction

- [ ] Confirm the authoritative physical format for `level2` under the Spark-only model
- [ ] Confirm whether the storage standard is parquet-only or a table format backed by files (delta/iceberg/hudi)
- [ ] Confirm whether Spark is required only from `level2` onward or also for selected ingestion paths
- [ ] Update PRD wording anywhere the current local sqlite execution path is implied as acceptable target state
- [ ] Define the canonical runtime contract for Spark session creation, configuration, and environment handling
- [ ] Confirm whether a metastore/catalog is allowed or whether the system must remain path-only
- [ ] Define the global storage-tier contract in config:
  - backend/type
  - root path
  - `level1` through `level5` physical names
  - bucket-vs-prefix interpretation rules for object storage
- [ ] Confirm the storage access model:
  - Spark-handled storage for data-plane datasets (Spark reads/writes via `file://`, `s3a://`, etc)
  - platform-handled storage for control-plane artifacts (audit/log/error/manifest/lineage), or Spark/Hadoop filesystem access for those too
  - confirm which URI schemes must be supported (`file://`, `s3a://`, others)

### Phase 2: Level1 Reading Contract for Spark

- [ ] Define how Spark reads `level1` artifacts based on manifest metadata:
  - payload format mapping (`json`, `jsonl`, `csv`, etc)
  - compression handling
  - selecting the correct `level1` manifest when both envelope and extracted items exist
- [ ] Decide whether envelope extraction remains an ingest concern or becomes a normalization selection concern for Spark runs
- [ ] Define a normalization selection policy:
  - normalize only extracted `items` manifests when present, otherwise normalize the envelope
  - or require an explicit config flag that chooses which manifest type is normalized
- [ ] Ensure Spark normalization can take a list of `level1` manifests and process them deterministically

### Phase 2: Level2 Physical Storage Alignment

- [ ] Replace current `level2` local `jsonl` table persistence with Spark-written physical output
- [ ] Preserve manifesting, partitioning, mapping catalog persistence, and deterministic artifact paths
- [ ] Preserve run-scoped audit, log, lineage, and error artifacts during Spark-backed writes
- [ ] Define how schema evolution and partition overwrite semantics work for Spark-written `level2`
- [ ] Decide whether `level2` datasets are written purely by Spark (recommended) and whether non-local storage URIs must be supported in the initial correction slice
- [ ] Define the `level2` dataset contract per table:
  - dataset location (URI)
  - format (parquet/delta/etc)
  - partition columns/values
  - schema representation (and how it is validated and versioned)
- [ ] Define whether the existing mapping catalog remains the authoritative logical-to-physical mapping artifact for Spark runs
- [ ] Replace row-count and file metrics collection with Spark-derived metrics where applicable

### Phase 3: SQL Runtime Replacement

- [ ] Replace sqlite execution in the SQL stage with Spark SQL execution
- [ ] Preserve model discovery, validation, dependency ordering, tokenization, and explain-mode behavior
- [ ] Define Spark-native materialization behavior for `full_refresh`, `append`, and `partition_overwrite`
- [ ] Rework validations and execution metrics to use Spark-native query/write evidence
- [ ] Ensure `level3` and `level4` outputs are materialized as file-backed datasets (and only registered as tables/views when needed for SQL execution)
- [ ] Define how `level2` datasets are registered for Spark SQL (temporary views vs catalog tables) using only the global storage-tier contract
- [ ] Define how table names and namespaces are derived deterministically for Spark SQL runs

### Phase 4: Publish Runtime Alignment

- [ ] Replace sqlite-backed publish reads with Spark-backed reads from approved `level4` outputs
- [ ] Preserve direct-vs-query selection behavior where still approved
- [ ] Define whether publish queries execute through Spark SQL, DataFrame export logic, or both
- [ ] Preserve run-scoped manifests, audit/log/error artifacts, and lineage for publish runs
- [ ] Reconfirm export format and packaging behavior against the Spark-backed runtime
- [ ] Define the Spark-based export implementation for CSV/JSONL/TSV and optional packaging
- [ ] Define how publish selection references `level4` inputs (dataset paths and/or Spark SQL table identifiers)

### Phase 5: CLI, Examples, and Operator Flow

- [ ] Update CLI contracts where engine-specific wording or assumptions currently point to sqlite
- [ ] Replace sqlite-based local examples with Spark-backed runnable examples
- [ ] Update operator runbooks to describe Spark runtime prerequisites and execution patterns
- [ ] Update README guidance to describe the corrected Spark-first architecture
- [ ] Add a Spark-first end-to-end example run that works on a laptop filesystem without external services

### Phase 6: Testing and Verification

- [ ] Replace or expand sqlite-based downstream tests with Spark-backed tests
- [ ] Add focused tests for Spark-backed `level2` writes, SQL materialization, and publish exports
- [ ] Add end-to-end verification from ingest through publish using the corrected Spark path
- [ ] Reconfirm diagnostics, local development workflow, and CI expectations for Spark-enabled execution
- [ ] Decide how Spark tests run in CI (local mode, containerized Spark, or integration-only)

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

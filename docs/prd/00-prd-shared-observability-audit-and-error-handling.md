# PRD 00: Shared Observability, Audit, and Error Handling

## Document Status

- Status: Draft v1
- Product area: `elt_pipeline`
- Scope: all runtime stages
- Proposed implementation language: Python
- Proposed packaging and environment management: `uv`

## Purpose

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

This document defines the shared runtime contract for observability, auditability, execution logging, error handling, and lineage-compatible event emission across all stages of `elt_pipeline`.

The purpose of this PRD is to avoid duplicating the same audit, logging, and error-handling requirements in each stage PRD.

Stage-specific PRDs must reference this shared contract and define only their stage-specific additions.

## Scope

This PRD applies to:

- source to `level1` ingestion
- `level1` to `level2` normalization
- `level2` to `level3` SQL execution
- `level3` to `level4` SQL execution

## Shared Principles

- every run must have a stable `run_id`
- audit records, execution logs, and error records are first-class runtime artifacts
- structured runtime logging is required; console output alone is insufficient
- lineage, audit, and error records must be related by `run_id`
- Spark engine logs may be retained for diagnostics, but they do not replace runtime audit records or structured execution logs

## Shared Runtime Artifacts

### Audit Record

Every run shall emit one authoritative audit record.

The audit record must capture at minimum:

- `run_id`
- `stage`
- `job_name`
- `trigger_type`
- `started_at`
- `completed_at`
- `status`
- `config_version` or `schema_version`
- metrics summary
- error summary when the run is not successful

Each stage may add stage-specific required fields.

### Structured Execution Log

Every run shall emit structured execution logs.

Execution logs must:

- include `run_id` on every event
- include timestamp, severity, component, and event type
- use structured fields rather than free-form text only
- capture key lifecycle events from run start through run completion

### Structured Error Record

Every error event must be structured and classified.

Each error record must capture at minimum:

- `run_id`
- `error_code`
- `error_category`
- human-readable message
- retryable versus non-retryable classification
- stage-specific context when available

## Shared Error Taxonomy

The platform shall use a stable error taxonomy that can be extended by stage.

Common categories include:

- `config_error`
- `validation_error`
- `input_contract_error`
- `processing_error`
- `storage_write_error`
- `lineage_error`
- `unexpected_runtime_error`

Each stage may define narrower subcategories.

## Shared Metrics Contract

Metrics capture is required, but the runtime should avoid expensive default behaviors.

- metrics should be captured from low-cost sources where possible
- expensive full scans, such as Spark `count()`, must not be the default path for row counts
- when row counts are captured for parquet outputs, the preferred approach is to use parquet metadata or Spark execution/write metrics

## Shared Lineage Compatibility

The platform shall emit lineage-compatible events for runs and datasets.

- OpenLineage-compatible event formats are preferred
- local storage of lineage events is acceptable in v1
- lineage events must remain linkable to audit and log records through `run_id`

## Stage-Specific Specialization Rules

### Ingestion to Level 1

The ingestion stage must extend the shared contract with source-pull-specific details such as:

- source endpoint, topic, table, or object location
- extraction mode
- checkpoint before and after
- raw artifacts written to `level1`

### Level 1 to Level 2

The normalization stage must extend the shared contract with:

- `level1` inputs consumed
- `level2` outputs produced
- `mapping_version`
- partition strategy and output table details

### Level 2 to Level 3 and Level 3 to Level 4

The SQL stages must extend the shared contract with:

- model name and version
- input models or source entities
- target object
- validation outcomes
- materialization strategy

## Acceptance Criteria

- every runtime stage emits one authoritative audit record per run
- every runtime stage emits structured execution logs with `run_id` on every event
- every runtime stage emits structured, classified error records
- every runtime stage can be correlated across audit, log, metrics, and lineage artifacts by `run_id`
- stage PRDs reference this shared contract rather than redefining the full common contract inline
EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-fb8fb607285048989b62fa2baed91eac/cwd.txt'; exit "$__tr_native_ec"
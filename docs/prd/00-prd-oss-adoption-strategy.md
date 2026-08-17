# PRD 00: OSS Adoption Strategy

## Document Status

- Status: Draft
- Product area: `elt_pipeline`
- Scope: all runtime stages

## Purpose

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

This document defines how `elt_pipeline` should incorporate open source software.

The goal is to adopt OSS for commodity concerns while retaining ownership of the platform-specific contracts that define the architecture, governance boundaries, and runtime behavior of the pipeline.

## Core Statement

`elt_pipeline` is a custom platform solution.

OSS dependencies are incorporated selectively only where they support the platform framework.

OSS tools must not define the core platform contracts, level model, governance boundaries, or runtime behavior.

## Principles

- adopt OSS where it solves a commodity problem well
- do not let an OSS tool define the platform architecture
- wrap OSS behind internal interfaces where future replacement is plausible
- keep the platform-specific contracts owned inside `elt_pipeline` and `elt_pipeline_cfg`

## What the Platform Should Own

The following are platform-specific and should remain first-class parts of the `elt_pipeline` design:

- level architecture and governance boundaries
- the configuration contract in `elt_pipeline_cfg`
- the runtime contract for audit, logging, error handling, and replay
- the mapping catalog lifecycle
- the `level1` to `level2` relationalisation behavior
- connector abstractions and execution contracts

## OSS to Use in v1

### Execution Engine

- `PySpark`
  - primary engine for normalization and SQL transforms

### Configuration Parsing and Validation

- a YAML parsing library
- a schema-validation library such as `pydantic`, JSON Schema, or an equivalent validation mechanism

### Lineage Event Standard

- `OpenLineage`
  - use an OpenLineage-compatible event model for lineage emission

## OSS to Design for, But Not Require in v1

### Lineage / Metadata Backend

- `Marquez`
- `DataHub`

These may be adopted later as lineage or metadata backends, but v1 should not depend on them to function.

### Data Quality Frameworks

- `Great Expectations`
- `Soda Core`
- `Deequ`

The runtime should leave clean integration points for these, but v1 does not require deep integration.

### Orchestration

- `Airflow`
- `Dagster`
- `Prefect`

V1 should support local and scheduled execution without requiring a specific orchestration framework.

## OSS to Avoid as the Primary Runtime Contract

The platform should avoid making the following the primary architecture contract for `elt_pipeline`:

- generic low-code or no-code ingestion suites
- third-party connector platforms that force a conflicting source-definition model
- metadata/catalog products as mandatory control planes for core runtime behavior

These tools may still be integrated later, but the platform must remain operable without them.

## Adoption Strategy by Layer

### Ingestion

Use OSS where it provides standards or utilities, but keep source definitions, checkpointing, raw landing, and replay contracts owned by the platform.

### Normalization

Use `PySpark` as the engine, but keep flattening rules, mapping catalogs, naming strategy, and partition strategy owned by the platform.

### SQL Execution

Use Spark SQL as the execution engine, but keep model discovery, execution contracts, audit/logging, and stage semantics owned by the platform.

## Acceptance Criteria

- `elt_pipeline` can run without requiring heavyweight external metadata or orchestration products
- lineage events are emitted in an OSS-compatible format
- configuration is validated with an OSS-backed validation approach or equivalent schema mechanism
- the platform-specific contracts remain defined by project PRDs and configuration, not by a third-party tool
EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-455662d2742542ddbe900970729513e2/cwd.txt'; exit "$__tr_native_ec"
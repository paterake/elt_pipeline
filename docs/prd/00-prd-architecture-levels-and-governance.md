# PRD 00: Architecture Levels and Governance Boundaries

## Document Status

- Status: Draft v1
- Product area: `elt_pipeline`
- Scope: all stages and levels

## Purpose

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

This document defines:

- the meaning of `level1` through `level5`,
- how these levels map to a medallion-style architecture,
- and why the level boundaries exist even when the compute pattern is the same.

Other PRDs must reference this document rather than duplicating level definitions.

## Medallion Mapping

`elt_pipeline` uses five logical levels.

These levels map cleanly onto common medallion language:

- `level1`: landing (raw capture)
- `level2`: lake / source-aligned relational layer (bronze to early silver boundary)
- `level3`: canonical business model (silver)
- `level4`: consumer-facing marts (gold)
- `level5`: publish/export outputs and derived delivery artifacts (gold)

## Level Definitions

### `level1` (Landing)

- Immutable raw capture of source payloads.
- Preserves source fidelity and replayability.
- Contains lineage manifests and minimal metadata.
- Avoids business logic and relational modelling.

### `level2` (Lake / Relationalised Source-Aligned)

- First structured layer intended for SQL consumption.
- Relationalises nested structures into flat tables with parent/child relationships.
- Remains source-aligned and avoids business conformance.

### `level3` (Canonical Model)

- Conformed business entities.
- Shared dimensions and facts.
- Applies business rules and standardizes definitions.

### `level4` (Consumer Datamarts)

- Consumer-optimized outputs.
- Denormalized and aggregated datasets aligned to use cases.
- Intended for broad analytic consumption.

### `level5` (Publish/Exports)

- Delivery-oriented outputs (exports, extracts, feeds).
- Acknowledges different operational semantics from tables (delivery cadence, file formats, packaging).

## Why Levels Matter Even When Compute Is the Same

Even if `level2` to `level3` and `level3` to `level4/5` are implemented using Spark SQL transforms, the levels remain valuable because they create:

- a semantic contract for what logic belongs where,
- clear operational boundaries for replay and backfill,
- and governance boundaries for controlling access.

## Governance and Access Control

Level boundaries are intended to support clean RBAC and ABAC enforcement.

- RBAC: deny or allow access to entire levels by storage path.
- ABAC: restrict access by level and by domain/source where appropriate.

The practical governance intent is that users can be locked out from a whole level at a filesystem/storage layer, independent of the query engine.

Fine-grained policies (row/column controls) may be layered later via the query engine or catalog, but level boundaries provide a strong coarse-grained control plane.

## References

- [01-prd-ingestion-raw-to-level1.md](01-prd-ingestion-raw-to-level1.md)
- [02-prd-level1-to-level2.md](02-prd-level1-to-level2.md)
- [03-prd-sql-level2-to-level3-and-level3-to-level4.md](03-prd-sql-level2-to-level3-and-level3-to-level4.md)
- [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md)
- [00-prd-oss-adoption-strategy.md](00-prd-oss-adoption-strategy.md)
EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-7d4ae8cd0d654af49d971d42ebe0c4ae/cwd.txt'; exit "$__tr_native_ec"
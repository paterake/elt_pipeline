# PRD 00: Architecture Levels and Governance Boundaries

## Document Status

- Status: Draft
- Product area: `elt_pipeline`
- Scope: all stages and levels

## Purpose

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

This document defines:

- the meaning of `level1` through `level5`,
- how these levels map to a medallion-style architecture,
- and why the level boundaries exist even when the compute pattern is the same.

Other PRDs must reference this document rather than duplicating level definitions.

This level model is intended to align with DAMA-DMBOK v2 thinking around governed data architecture, integration, metadata, quality, security, and operational control. It is a platform-specific implementation pattern rather than a claim that DAMA-DMBOK v2 prescribes these exact level names.

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
- Intended for broad analytic consumption through queryable tables.

### `level5` (Publish/Exports)

- Delivery-oriented static outputs such as exports, extracts, canned reports, and feeds.
- Intended for consumer pickup when a file-based handoff is preferred over direct table access.
- Acknowledges different operational semantics from tables (delivery cadence, file formats, packaging).

## Why Levels Matter Even When Compute Is the Same

Even if `level2` to `level3` and `level3` to `level4/5` are implemented using Spark SQL transforms, the levels remain valuable because they create:

- a semantic contract for what logic belongs where,
- clear operational boundaries for replay and backfill,
- and governance boundaries for controlling access.

They also help operationalize DAMA-DMBOK v2-aligned concerns in a concrete platform design:

- `level1` supports source traceability, replay, and raw evidence retention,
- `level2` supports structured integration and source-aligned normalization,
- `level3` supports canonical enterprise modeling and standard definitions,
- `level4` supports consumer-facing analytical serving,
- `level5` supports controlled delivery and interoperability through static outbound artifacts.

## Governance and Access Control

Level boundaries are intended to support clean RBAC and ABAC enforcement.

- RBAC: deny or allow access to entire levels by storage path.
- ABAC: restrict access by level and by domain/source where appropriate.

The practical governance intent is that users can be locked out from a whole level at a filesystem/storage layer, independent of the query engine.

Fine-grained policies (row/column controls) may be layered later via the query engine or catalog, but level boundaries provide a strong coarse-grained control plane.

### Governance-by-Path on Level 3 Partitions

Beyond level-wide boundaries, the canonical `level3` layer adds finer granularity via its partition structure, which SHALL be:

```
level3/<table_name>/source_name=<src>/business_date=<date>/*.parquet
```

This directly enables:
- **IAM prefix policies:** Lock or grant access to a specific source within a canonical table, e.g. deny `level3/canonical_notice/source_name=external_partner/*` to internal analysts while granting the rest.
- **Time-windowed IAM:** Date partitions add finer granularity for access windows (e.g. allow analysts to query only `business_date` within the last 13 months).
- **ABAC column filtering:** `WHERE source_name IN (<current_user_allowed_sources>)` works uniformly in queries because `source_name` is both a real data column and a path partition key.
- **Metastore-level GRANTs:** Once a Glue/Hive/Iceberg catalog is added, `source_name` and `business_date` become registered partition columns that can be GRANTed on directly.

Audit/snapshot tables using the `ingest_date` override get identical governance structure, just with `ingest_date` as the date segment. This is a standard Spark pattern — the path IS the governance surface, and no custom tooling is needed beyond correct `partitionBy` on write.

### Per-Environment Roots / Buckets (No `environment=` In-Path)

Environment isolation SHALL NOT be implemented by baking `environment=<env>` into the filesystem path at any level. Instead:

- **L1 + L2 (raw root):** Each environment (dev, staging, prod) points at its own independent `--root-path` (storage bucket / account / directory).
- **L3 + L4 (warehouse root):** Each environment points at its own independent `--warehouse-root`.
- **Audit trail:** `environment` is retained on manifests, `RunContext`, logs, and audit records — it is only removed from filesystem paths.
- **Rationale:** This is the cloud lakehouse standard pattern (Databricks workspaces = per-env storage accounts; EMR = per-env buckets; Glue = per-env catalog IDs). In-path environment segments break point-in-time restore, env-to-env promotion, and clean IAM prefix boundaries. Sharing one root across two environments would cause the same `level3/<table>/` path to be written by both environments and collide.

The two-root split (raw vs curated) is retained for the same reason: raw data (L1/L2) and curated data (L3/L4) have different lifecycles, retention policies, RBAC profiles, and encryption requirements. This is also standard medallion practice.

## References

- [01-prd-ingestion-raw-to-level1.md](01-prd-ingestion-raw-to-level1.md)
- [02-prd-level1-to-level2.md](02-prd-level1-to-level2.md)
- [03-prd-sql-level2-to-level3-and-level3-to-level4.md](03-prd-sql-level2-to-level3-and-level3-to-level4.md)
- [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md)
- [00-prd-oss-adoption-strategy.md](00-prd-oss-adoption-strategy.md)
EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-7d4ae8cd0d654af49d971d42ebe0c4ae/cwd.txt'; exit "$__tr_native_ec"

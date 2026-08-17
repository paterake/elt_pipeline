# PRD 00: Platform Principles

## Document Status

- Status: Draft
- Product area: `elt_pipeline`
- Scope: platform-wide

## Purpose

This document defines the founding principles of `elt_pipeline`.

These principles are the non-negotiable design rules for the solution and must guide architecture, configuration, runtime behavior, governance, and implementation choices.

All other platform and stage PRDs must be consistent with this document.

## Positioning

`elt_pipeline` is a client-neutral, config-driven data pipeline framework designed to support DAMA-DMBOK v2-aligned data management principles.

The platform is not only an ingestion and transformation engine. It is a governed data platform runtime for moving data through explicit architectural levels with strong auditability, lineage, metadata discipline, replayability, and access-control boundaries.

`elt_pipeline` should be understood as:

- a governed data platform with explicit level contracts,
- a reusable runtime for ingestion, normalization, transformation, and publish/export flows,
- a metadata-aware and audit-first operating model for local-first data movement,
- and a client-neutral implementation pattern aligned to DAMA-DMBOK v2 principles.

The platform does not claim that DAMA-DMBOK v2 mandates this exact `level1` through `level5` structure. Instead, `elt_pipeline` uses these levels as a product-specific architecture that aligns to DAMA-DMBOK v2 concerns such as:

- data architecture,
- data integration and interoperability,
- metadata management,
- data quality,
- governance and security,
- and operational auditability.

## Founding Principles

### 1. DAMA-DMBOK v2 Alignment

The platform shall align to DAMA-DMBOK v2 principles where relevant to pipeline execution and data movement.

This includes support for:

- data architecture
- metadata management
- data quality
- data security
- data integration and interoperability
- operations and auditability

The platform should not claim to implement all of DAMA-DMBOK v2, but it must support these concerns explicitly in its architecture and runtime contracts.

### 2. Client Neutrality

The platform must remain client-neutral.

- client names, vendor names, and legacy repository names must not appear in the product contract
- legacy implementations may be used as internal baselines for coverage, but must not define the public shape of the solution
- configuration examples must remain generic and non-identifying

### 3. Layered Architecture as Contract

`level1` through `level5` are first-class architectural boundaries.

These levels exist to define:

- what kind of logic belongs where
- what data contract is expected at each level
- how governance and access can be enforced cleanly

The levels are not merely implementation steps. They are semantic and governance boundaries.

Within `elt_pipeline`, those levels mean:

- `level1`: raw landed source data,
- `level2`: relationalized source-aligned structured data,
- `level3`: canonical and standardized warehouse-style data,
- `level4`: consumer-facing datamarts for direct analytical use,
- `level5`: transformed static outputs or canned deliverables for consumer pickup.

### 4. Governance by Design

Governance must be built into the platform architecture.

- access control should be enforceable at level boundaries
- auditability must exist at every stage
- metadata and lineage are required runtime concerns, not optional extras
- replayability and controlled backfill must be designed into the system

### 5. Config-Driven Runtime

The platform shall prefer versioned configuration over hardcoded, source-specific logic wherever practical.

- source behavior should be expressed declaratively when possible
- configuration must be reviewable, versioned, and environment-aware
- runtime code should remain reusable across many source patterns

### 6. OSS-Aware, Not OSS-Defined

The platform is a custom solution.

OSS is incorporated selectively only where it supports the platform framework.

The platform should adopt OSS where it solves commodity concerns well, but the platform architecture must remain owned by the solution.

- OSS standards and components may be integrated
- third-party tools must not define the core platform contract
- the architectural model, governance boundaries, and runtime behavior remain first-class platform responsibilities

### 7. Replayable, Auditable, Observable by Default

Every stage must support:

- stable run identifiers
- authoritative audit records
- structured execution logs
- structured error records
- lineage-compatible events
- deterministic replay and backfill behavior

## Architectural Consequences

These principles imply the following:

- platform-level PRDs define the level model, observability contract, and OSS strategy
- stage PRDs specialize the shared contracts rather than redefining them from scratch
- source configuration is separated from runtime implementation
- governance boundaries must remain visible in storage layout and runtime behavior
- new capabilities should extend shared contracts rather than create ad hoc exceptions
- the layer model is used to operationalize DAMA-DMBOK v2-aligned platform concerns rather than to mirror a named DAMA standard verbatim

## Out of Scope

This document does not define:

- connector-specific implementation detail
- the full DAMA-DMBOK v2 framework
- specific runtime infrastructure choices beyond principle-level guidance

## References

- [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md)
- [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md)
- [00-prd-oss-adoption-strategy.md](00-prd-oss-adoption-strategy.md)
- [01-prd-ingestion-raw-to-level1.md](01-prd-ingestion-raw-to-level1.md)
- [02-prd-level1-to-level2.md](02-prd-level1-to-level2.md)
- [03-prd-sql-level2-to-level3-and-level3-to-level4.md](03-prd-sql-level2-to-level3-and-level4.md)

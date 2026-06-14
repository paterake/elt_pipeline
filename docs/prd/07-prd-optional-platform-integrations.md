# PRD 07: Optional Platform Integrations

## Document Status

- Status: Draft for approval
- Product area: `elt_pipeline`
- Scope: optional post-v1 lineage, orchestration, and data-quality integrations
- Depends on: `00-prd-platform-principles.md`, `00-prd-architecture-levels-and-governance.md`, `00-prd-shared-observability-audit-and-error-handling.md`, `06-prd-level4-to-level5-publish-and-export.md`

## Purpose

This document defines the approved design boundaries that must exist before optional platform integrations are implemented.

It covers three post-v1 extension areas:

- optional lineage backend emission,
- optional orchestration wrappers,
- and optional data-quality hook points.

The goal is to enable selected integrations without letting external tools redefine the platform's runtime contracts.

## Background

The current repository state already implements the platform's local-first runtime through `level5`.

That implemented runtime includes:

- CLI entrypoints as the authoritative execution interface,
- local artifact persistence for audit, logs, lineage, and checkpoints,
- an OpenLineage-compatible lineage event shape stored locally,
- deterministic local schedule execution,
- and stage-specific audit semantics across ingest, normalize, SQL, and publish flows.

The OSS adoption strategy in [00-prd-oss-adoption-strategy.md](00-prd-oss-adoption-strategy.md) explicitly allows future use of OSS lineage, orchestration, and data-quality tools, but it also requires those tools to remain behind platform-owned contracts.

This PRD defines how optional integrations may be added later without weakening:

- local-first operability,
- the `level1` through `level5` architecture model,
- stage-aware auditability and replay semantics,
- or the rule that `elt_pipeline` owns the authoritative runtime contract.

## Problem Statement

The platform now has a complete local-first execution path, but it lacks an approved contract for connecting to optional external control-plane or observability tools.

Without a design baseline, later integration work could drift into one or more undesirable patterns:

- a lineage backend becoming required for normal execution,
- an orchestration framework redefining job inputs or stage semantics,
- or a data-quality framework forcing engine-specific logic directly into stage runtimes.

These outcomes would conflict with the approved platform principles and create hidden dependencies that are not required for the product to function.

## Product Vision

Add optional integration surfaces that let `elt_pipeline`:

- emit lineage events to a reference backend in addition to local artifacts,
- run inside an orchestration wrapper that still invokes the existing CLI contract,
- and invoke focused data-quality checks around normalization and SQL outputs,

while preserving the local runtime as the authoritative and complete operating mode.

## Goals

- Define adapter boundaries for optional lineage backend emission.
- Define wrapper boundaries for optional orchestration integration.
- Define hook points for optional data-quality checks around normalization and SQL stages.
- Preserve local-first operability when no optional integrations are installed or configured.
- Preserve current stage contracts, rerun semantics, and audit correlation by `run_id`.

## Non-Goals

- Making any external lineage backend mandatory for runtime success.
- Replacing the CLI with framework-specific operators, DAG semantics, or SDK-only execution paths.
- Introducing a full enterprise data-quality product contract in this phase.
- Expanding the approved `level1` through `level5` semantics.
- Redefining publish transports or non-local delivery targets in this document.

## Users and Stakeholders

- Data platform engineers extending the runtime with optional integrations.
- Platform operators who need consistent observability and rerun behavior across local and wrapped execution.
- Maintainers evaluating OSS adoption choices such as Marquez, DataHub, Airflow, Dagster, Prefect, Great Expectations, or Soda Core.

## Foundational Constraints

The following constraints are mandatory for any implementation that follows this PRD:

- The CLI remains the authoritative execution contract for all runtime stages.
- Local audit, log, lineage, and checkpoint artifacts remain supported and authoritative even when external backends are enabled.
- Optional integrations must degrade cleanly to no-op behavior when not configured.
- Integration failures must not silently erase or weaken local stage evidence.
- The platform-owned `run_id` remains the shared correlation key across local and external artifacts.
- External tools may enrich runtime behavior, but they must not redefine stage boundaries, config layering, or rerun semantics.

## Scope

This PRD covers:

- adapter boundaries for optional lineage backend emission,
- wrapper boundaries for optional orchestration frameworks,
- optional quality-check hook points around normalization and SQL execution,
- runtime configuration principles for enabling or disabling these integrations,
- and audit/error expectations when optional integrations are active.

This PRD excludes:

- implementation of every possible third-party backend,
- deep catalog or metadata synchronization beyond lineage event emission,
- orchestration-native execution models that bypass the CLI,
- and quality rule authoring standards for every dataset or domain.

## Functional Requirements

### FR1. Local Runtime Remains Authoritative

Optional integrations must layer on top of the current runtime rather than replacing it.

The platform shall continue to support complete local execution with:

- no external lineage backend,
- no external orchestrator,
- and no external data-quality product.

Any optional integration must behave as an additive extension to the existing runtime contract.

### FR2. Lineage Adapter Boundary

The runtime shall define an internal lineage-emission boundary that accepts the platform's existing OpenLineage-compatible event payloads.

That boundary must support at minimum:

- local artifact persistence as the baseline implementation,
- optional fan-out to one configured external backend emitter,
- and stage-aware correlation using the same `run_id`, job name, and dataset references already used locally.

The adapter contract must not require upstream stages to construct backend-specific payloads.

### FR3. Reference Lineage Backend Shape

The first reference lineage integration, when implemented, must consume the internal lineage adapter contract rather than bypassing it.

The reference implementation may target one approved backend such as:

- Marquez,
- or another backend that accepts OpenLineage-compatible event submission.

The first reference integration must preserve the existing local lineage artifact write even when remote emission is enabled.

### FR4. Orchestration Wrapper Boundary

The platform shall define an orchestration wrapper boundary that invokes existing CLI entrypoints rather than introducing a second authoritative execution API.

Wrapper behavior may include:

- passing stage selections and window parameters,
- mapping schedule metadata into existing CLI arguments,
- surfacing stdout, stderr, and exit status for orchestration monitoring,
- and attaching orchestration metadata to audit context as optional supplemental fields.

The wrapper boundary must not:

- redefine stage names,
- bypass shared audit/log/error handling,
- or create framework-specific semantics for checkpoints, reruns, or backfills.

### FR5. Reference Orchestration Integration Shape

The first reference orchestration integration, when implemented, must call the existing CLI commands for ingest, normalize, SQL, publish, or schedule execution.

It may provide:

- example DAG or flow definitions,
- environment-variable mapping,
- and wrapper documentation for deployment.

It must not require the runtime package to be imported as an orchestration-specific SDK entrypoint for normal operation.

### FR6. Data-Quality Hook Boundary

The runtime shall define optional hook points around:

- normalization outputs after `level1` to `level2` completion,
- SQL outputs after `level2` to `level3` or `level3` to `level4` completion,
- and publish outputs later only if a future PRD explicitly extends this contract.

The hook boundary must receive enough context to evaluate focused checks, including:

- `run_id`,
- stage name,
- output dataset identifiers,
- materialization target information,
- and low-cost metrics already collected by the runtime where available.

### FR7. Quality Hook Execution Rules

Quality hooks must be optional and configuration-driven.

For the first implementation phase, quality hooks shall support outcomes such as:

- pass,
- warn,
- fail,
- or skipped.

The runtime must capture the result in the authoritative audit record and structured logs.

The hook boundary must allow a stage to fail fast when a configured rule declares a blocking failure.

### FR8. Configuration and Enablement

Optional integrations must be enabled through platform-controlled configuration rather than ad hoc code changes.

Configuration for these integrations must:

- preserve safe defaults with all integrations disabled,
- identify the selected adapter or wrapper type explicitly,
- capture required connection or execution metadata,
- and validate with the same structured error taxonomy used elsewhere in the runtime.

Environment-specific enablement is allowed, but the base runtime contract must remain stable when an integration is turned off.

### FR9. Error Handling and Auditability

The runtime shall distinguish between:

- core stage failure,
- optional integration failure that blocks the stage by policy,
- and optional integration failure recorded as non-blocking supplemental error evidence.

Audit artifacts must let operators determine:

- whether the core stage completed successfully,
- whether local evidence was written,
- whether external emission or wrapper behavior succeeded,
- and what remediation path is appropriate.

### FR10. Packaging and Dependency Isolation

Optional integrations should be installable as targeted extras or similarly isolated dependencies where practical.

The base project installation must remain usable without:

- lineage backend client libraries,
- orchestration SDKs,
- or data-quality framework packages.

### FR11. Documentation and Operator Contract

Each optional integration implemented after this PRD must include:

- operator-facing setup guidance,
- enablement and disablement instructions,
- failure-mode guidance,
- and clear statements that local artifacts remain authoritative for replay and investigation.

## Non-Functional Requirements

### NFR1. Determinism

- Optional integrations must not change the selected stage inputs, windows, or outputs compared with an equivalent local CLI run.

### NFR2. Resilience

- Local audit, log, and lineage persistence must survive even when an external backend is unavailable.

### NFR3. Observability

- Operators must be able to correlate integration activity with the core stage using the same `run_id`.

### NFR4. Portability

- The project must still install and run on a local developer machine without external control-plane services.

### NFR5. Replaceability

- Adapter and wrapper boundaries should allow one tool choice to be replaced later without redefining the platform contract.

## Proposed Product Design

### Integration Package Areas

Suggested package areas for later implementation are:

- `elt_pipeline.integrations.lineage`
- `elt_pipeline.integrations.orchestration`
- `elt_pipeline.integrations.quality`

The exact layout may vary, but integration-specific code should remain clearly separated from the core stage runtimes.

### Lineage Adapter Shape

An illustrative lineage boundary should support:

- one platform-owned event model as input,
- one local persistence implementation,
- zero or one configured remote emitter implementations for the first phase,
- and best-effort or blocking policy selection controlled by configuration.

### Orchestration Wrapper Shape

An orchestration wrapper should be modeled as:

- a thin framework-specific layer,
- that assembles approved CLI arguments,
- invokes the CLI as a subprocess or equivalent boundary,
- and records wrapper metadata without changing runtime semantics.

### Quality Hook Shape

A quality integration should be modeled as:

- a stage-aware hook invocation boundary,
- a normalized result contract independent of any one framework,
- and adapters that translate framework-native results into platform-owned audit fields.

## Acceptance Criteria

- A design reviewer can identify one internal boundary for lineage emission, one for orchestration wrappers, and one for quality hooks.
- The design makes clear that the CLI remains authoritative and local artifacts remain first-class.
- The design allows one reference lineage backend integration without forcing backend-specific payload construction into stage runtimes.
- The design allows one reference orchestration integration that invokes existing CLI entrypoints without redefining runtime contracts.
- The design allows optional quality checks around normalization and SQL outputs without requiring a framework dependency in the base runtime.

## Risks

- Optional integrations may still leak framework-specific assumptions into the core runtime if internal boundaries are not kept narrow.
- Operators may misinterpret external system success as more authoritative than local artifacts unless documentation is explicit.
- Quality hooks may become expensive or engine-specific if they ignore the low-cost metrics principles already defined for the platform.

## Assumptions

- The current local-first runtime remains the supported default operating mode.
- Existing lineage events remain OpenLineage-compatible rather than backend-specific.
- External orchestrators will be treated as wrappers around the CLI, not as replacements for it.
- Early quality integrations should focus on normalization and SQL stages before any broader publish-stage expansion.

## Sequencing Recommendation

Implement Phase 14 in this order after this PRD is approved:

1. Add the internal lineage adapter boundary while preserving local artifact writes.
2. Add one reference lineage backend integration against that boundary.
3. Add the orchestration wrapper boundary and one reference wrapper that invokes the CLI.
4. Add optional normalization and SQL quality hook boundaries with one lightweight reference integration if still desired.

## Change Control

If later work would:

- make an external system mandatory,
- expand quality hooks to publish or ingestion stages,
- redefine the CLI contract,
- or introduce backend-specific lineage payloads into stage runtimes,

then this PRD must be updated and re-approved before implementation begins.

# PRD 06: Publish and Export from Level 4 to Level 5

## Document Status

- Status: Approved
- Product area: `elt_pipeline`
- Stages: `level4` -> `level5`
- Proposed implementation language: Python
- Proposed packaging and environment management: `uv`

## Approval Summary

The `level5` publish/export contract is approved for initial implementation with the following first-slice decisions:

- `level4` remains the authoritative analytical datamart layer and `level5` remains the static delivery layer.
- The first implementation supports local filesystem delivery only.
- The first mandatory output formats are CSV and newline-delimited JSON (`jsonl`).
- Parquet extracts, archive bundles, and canned report artifacts remain reserved follow-on capabilities.
- A publish definition may either export an upstream `level4` dataset directly or use an adjacent declarative selection query.
- Every run must write a run-scoped manifest and run-scoped artifact path even when a stable consumer-facing path is also maintained.
- Required client-neutral delivery metadata includes owning domain, owner team, consumer label, and delivery purpose.

## Background

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

The level model and governance boundaries are defined in [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md).

The shared observability, audit, and error-handling contract is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

The existing PRD set defines `level1` through `level4`, but intentionally stops before external publish and export behavior. That leaves an important gap:

- `level4` consumer datamarts can be produced,
- but delivery-oriented extracts and packaging rules are not yet defined,
- and operators do not yet have a first-class contract for repeatable static export runs.

The architecture model already reserves `level5` for publish/export outputs and derived delivery artifacts. This PRD defines what that stage means in product terms without introducing external platform coupling.

Within the broader product positioning, `elt_pipeline` is a governed data platform runtime aligned to DAMA-DMBOK v2 principles. The `level1` through `level5` structure used here is a platform-defined layering model that operationalizes governed data movement, metadata discipline, quality boundaries, and controlled delivery.

## Problem Statement

Downstream consumers may either analyze queryable tables in `level4` or pick up static transformed outputs from `level5`. These outputs have different semantics from `level4` datasets:

- delivery may be file-oriented rather than table-oriented,
- output formats may vary by consumer,
- artifacts may require packaging and manifests,
- reruns must preserve traceability of what was delivered and when,
- and operators need deterministic replay without guessing whether previous artifacts should be overwritten, versioned, or retained.

Without a formal `level5` contract, publish/export behavior risks being implemented as ad hoc scripts or direct `level4` side effects, which would weaken auditability, lineage, and governance boundaries.

## Level Recap

For clarity, the intended level boundaries are:

- `level1`: raw ingested data
- `level2`: ingested data relationalized and stored in parquet form
- `level3`: canonical warehouse-style model with standardized formats and definitions
- `level4`: consumer-specific datamarts that users can analyze directly as tables
- `level5`: transformed static outputs or canned reports that a consumer can pick up directly

This PRD only covers the `level4` to `level5` step.

## DAMA-DMBOK v2 Alignment

This `level5` stage aligns to DAMA-DMBOK v2-oriented platform concerns without claiming that DAMA-DMBOK v2 prescribes this exact naming model.

In this architecture:

- `level1` supports raw evidence retention and replay,
- `level2` supports structured source-aligned integration,
- `level3` supports canonical warehouse-style standardization,
- `level4` supports consumer-facing analytical datamarts,
- `level5` supports controlled delivery and interoperability through static outbound artifacts.

The specific role of `level5` is to preserve a governed boundary between:

- queryable analytical serving in `level4`, and
- static outbound delivery for consumer pickup in `level5`.

That separation strengthens metadata control, auditability, lineage, rerun semantics, and operational ownership for outbound artifacts.

## Product Vision

Build a local-first publish/export framework in Python that:

- takes approved `level4` consumer datamarts as input,
- produces delivery-oriented `level5` static artifacts and manifests,
- supports deterministic reruns, backfills, and replacement semantics,
- records audit, lineage, and packaging metadata for every delivery run,
- and keeps the CLI/runtime contract authoritative even when future delivery adapters are added later.

## Stage Definition

### Level 5

`level5` is the publish/export stage for transformed static outputs.

It is responsible for:

- transforming approved `level4` outputs into delivery-ready artifacts,
- generating static files or canned report outputs that a consumer can pick up,
- applying delivery-oriented formatting and partition selection,
- packaging related files together when required,
- publishing a manifest that describes what was produced,
- preserving a clear record of intended recipients or destinations as metadata,
- and supporting operational reruns and backfills with explicit replacement semantics.

`level5` is not responsible for upstream conformance, reporting mart design, or source-aligned cleanup. Those concerns belong in earlier levels.

Consumers may choose either:

- direct analysis from `level4` datamart tables, or
- pickup and downstream ingestion of static `level5` outputs.

## Goals

- Define `level5` as a distinct static-output stage rather than an ad hoc export utility.
- Standardize delivery artifact manifests and metadata.
- Support deterministic local file-based exports and canned report outputs as the first implementation target.
- Preserve strong auditability, lineage, and replayability for delivery runs.
- Keep the publish contract compatible with future optional transport or orchestration adapters.

## Non-Goals

- Designing every downstream consumer extract in this document.
- Defining network transport, SFTP, email, API push, or cloud delivery as part of the first implementation.
- Replacing `level4` marts with consumer-specific business logic that belongs earlier in the pipeline.
- Defining optional lineage backends or orchestration wrappers in this PRD.

## Users and Stakeholders

- Data platform engineers implementing publish/export runtime behavior.
- Analytics and data product teams defining delivery-ready static outputs from `level4`.
- Platform operators responsible for reruns, backfills, and incident response.
- Downstream consumers who depend on stable exported files and delivery metadata.

## Scope

This PRD covers:

- `level4` to `level5` publish/export semantics for static consumer outputs,
- export model metadata and discovery,
- local file-based delivery targets for consumer pickup,
- artifact naming, partition and window selection,
- manifest generation,
- rerun and replacement behavior,
- audit, logging, error, and lineage specialization for `level5`.

This PRD excludes:

- ingestion and normalization behavior,
- `level2` to `level4` SQL semantics already defined elsewhere,
- external transport integrations,
- consumer-side acknowledgment workflows,
- optional data-quality framework expansion beyond focused publish checks.

## Functional Requirements

### FR1. Publish Artifacts as First-Class Product Artifacts

The framework shall treat publish/export definitions and generated delivery artifacts as versioned product artifacts.

Each publish/export definition must declare at minimum:

- publish name,
- stage (`level5`),
- source dataset or model reference from `level4`,
- source selection mode (`direct` dataset export or declarative query-driven export),
- output format,
- destination target type,
- delivery path template,
- load or replacement mode,
- partition or window strategy,
- ownership metadata,
- consumer label,
- delivery purpose,
- and optional packaging rules.

### FR2. Level Boundary Enforcement

`level5` publish runs must consume approved upstream datasets rather than embedding earlier-stage business transformation logic.

The runtime shall enforce that `level5` definitions reference:

- `level4` datasets directly,
- or an explicitly approved `level3` input only when a future PRD extension allows it.

The default contract for this phase is `level4` to `level5`.

### FR3. Local File-Based Delivery Targets

The first implementation shall support local filesystem delivery targets for static consumer pickup.

The runtime shall support writing delivery artifacts such as:

- delimited files such as CSV,
- newline-delimited JSON where appropriate,
- parquet extracts where a file-based analytical handoff is required,
- static pre-rendered report artifacts where the consumer contract requires a canned deliverable,
- and packaged bundles containing one or more related files plus metadata.

For the first implementation phase, the mandatory supported output formats are:

- CSV
- newline-delimited JSON (`jsonl`)

Parquet extracts, canned report artifacts, and archive bundles are explicitly reserved for follow-on phases after the core publish contract is proven.

### FR4. Delivery Metadata and Artifact Manifest

Every successful publish run must produce a machine-readable manifest for the generated artifacts.

The manifest must capture at minimum:

- `run_id`,
- publish definition name and version,
- source dataset identifiers,
- execution window or partition scope,
- output format,
- artifact file paths,
- stable delivery paths when different from run-scoped paths,
- file sizes where available,
- row counts where available at low cost,
- content checksums where practical,
- replacement mode used,
- produced timestamp,
- owning domain and owner team metadata,
- consumer label,
- delivery purpose,
- validation results summary,
- and references to superseded artifacts when replacement semantics require them.

The manifest is the authoritative description of a `level5` delivery output.

### FR5. Replacement and Versioning Modes

The framework shall support explicit publish semantics for reruns and backfills.

Supported modes must include at minimum:

- overwrite-in-place,
- append-new-artifact,
- partition-replace,
- and versioned delivery where each run writes to a unique run-scoped location.

Each publish definition must declare its default replacement behavior explicitly.

For the first implementation:

- `overwrite-in-place` updates a stable delivery path after a successful staged write and finalize step,
- `append-new-artifact` writes an additional uniquely named artifact without mutating prior outputs,
- `partition-replace` replaces only the targeted partition path while retaining untouched partitions,
- and `versioned-delivery` always writes to a unique run-scoped delivery path while optionally updating a stable pointer or alias later.

### FR6. Windowing, Backfills, and Replay

The platform shall support:

- single-window publish runs,
- date-range backfills,
- partition-scoped reruns,
- deterministic replay using the same publish definition and source selection,
- and targeted reruns for one publish definition without rebuilding unrelated outputs.

The runtime must preserve a record of what windows and artifacts were produced for each run.

### FR7. Delivery Validation

The publish/export product shall support focused delivery validations including:

- schema compatibility checks against the publish definition,
- required column presence,
- file existence and non-empty checks where appropriate,
- low-cost row count or record count capture where practical,
- and naming or path-template validation before writes begin.

Validation outcomes must be captured as part of the authoritative audit record and manifest metadata when relevant.

### FR8. Audit, Logging, and Lineage

The shared observability contract defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md) applies to `level5`.

For publish/export execution, the runtime shall additionally record:

- publish definition name and version,
- upstream dataset references,
- artifact paths produced,
- replacement mode,
- delivery target type,
- execution window,
- validation outcomes,
- and any retained previous artifact references when a rerun replaces earlier output.

Lineage events must link upstream `level4` datasets to generated `level5` artifacts.

### FR9. Failure Handling

The publish runtime shall distinguish at minimum:

- invalid publish definition or config errors,
- missing upstream inputs,
- schema compatibility failures,
- local write failures,
- packaging failures,
- replacement-conflict errors,
- and unexpected runtime failures.

Operators must be able to determine from audit artifacts whether a publish run:

- wrote no outputs,
- wrote a complete set of outputs,
- or wrote partial outputs that require cleanup or a safe rerun.

### FR10. Separation of Runtime and Publish Definitions

The Python package shall provide the execution engine, metadata handling, and CLI interface.

Publish/export logic for standard workflows shall remain declarative wherever practical through adjacent manifests or similar metadata files, rather than requiring new Python code for each delivery.

For the first implementation, publish definitions may use either:

- direct export of a referenced upstream `level4` dataset, or
- an adjacent declarative SQL selection file that reads from approved upstream tables and shapes only the final delivery projection.

### FR11. Packaging Rules

The framework shall support optional packaging instructions for cases where one delivery unit contains multiple artifacts.

Packaging behavior may include:

- grouping multiple files under one delivery prefix,
- generating a companion manifest,
- producing a metadata sidecar,
- and creating an archive bundle when a consumer contract requires a single deliverable.

Archive generation is optional for the first implementation, but the product contract must reserve packaging metadata for it.

### FR12. Consumer-Facing Publish Contract

Each `level5` publish definition must document:

- business purpose,
- expected recipients or consuming system names in generic metadata,
- grain of the delivered data,
- delivery cadence,
- output format,
- whether the output is a direct static extract or a canned report artifact,
- retention expectation,
- replacement policy,
- and breaking-change guidance.

## Non-Functional Requirements

### NFR1. Determinism

- Given the same publish definition, source inputs, and execution window, the runtime must produce reproducible outputs and manifests.

### NFR2. Reliability

- Failed publish runs must not silently leave ambiguous delivery state.
- The runtime should favor explicit temporary-write then finalize patterns where practical.

### NFR3. Observability

- Operators must be able to trace every artifact back to `run_id`, publish definition, and upstream input.

### NFR4. Local-First Usability

- The initial development and operator workflow must remain fully runnable on a local filesystem without requiring external infrastructure.

### NFR5. Extensibility

- The publish contract should support future delivery adapters without redefining the core artifact, manifest, and rerun semantics.

## Proposed Product Design

### Publish Package Structure

The new solution should organize publish definitions in a stage-aware structure such as:

- `elt_pipeline/publish/<domain>/<publish_name>/manifest.yaml`
- `elt_pipeline/publish/<domain>/<publish_name>/query.sql`
- `elt_pipeline/publish/<domain>/<publish_name>/template.*`

The exact layout may vary, but the runtime must be able to discover:

- publish metadata,
- optional SQL or selection logic,
- output formatting instructions,
- path templates,
- packaging rules,
- and validation rules.

### Python Runtime Components

Suggested package areas:

- `elt_pipeline.publish.runtime`
- `elt_pipeline.publish.discovery`
- `elt_pipeline.publish.compiler`
- `elt_pipeline.publish.executor`
- `elt_pipeline.publish.storage`
- `elt_pipeline.publish.manifest`

### CLI Interface

The runtime should support commands such as:

- run all publish definitions for a domain,
- run one publish definition,
- preview or explain what artifacts would be produced,
- validate publish definitions without writing outputs,
- backfill a date range,
- and rerun one publish definition with an explicit replacement mode override.

### Local Artifact Layout

The first implementation should store `level5` artifacts in a level-aware local layout that keeps publish outputs separate from earlier stages.

An illustrative pattern is:

- `artifacts/level5/<domain>/<publish_name>/window=<...>/`
- `artifacts/level5/<domain>/<publish_name>/run_id=<...>/manifest.json`
- `artifacts/level5/<domain>/<publish_name>/current/` for stable consumer-facing outputs when the selected replacement mode requires one

The final directory naming convention may vary, but it must preserve:

- stable level separation,
- deterministic discovery,
- and support for run-scoped audit correlation.

## Data Contract for Level 5

Each `level5` publish definition must document:

- upstream source dataset or model,
- delivery grain,
- selected columns or exported schema,
- output format and encoding rules,
- partition or window logic,
- path template,
- replacement behavior,
- retention expectation,
- validation expectations,
- and ownership metadata.

Required client-neutral metadata fields for the first implementation are:

- `owning_domain`
- `owner_team`
- `consumer_label`
- `delivery_purpose`

## Success Metrics

- Platform engineers can add a new `level5` definition without modifying runtime code for standard file-based outputs.
- Operators can rerun one publish definition for one window without guessing cleanup behavior.
- Every publish run produces a manifest that fully describes the generated artifacts.
- Lineage and audit artifacts clearly connect `level4` inputs to `level5` outputs.
- The local-first implementation supports at least one representative delivery workflow end to end.

## Acceptance Criteria

- A Python `uv` project can discover and execute `level5` publish definitions.
- Publish metadata declares source inputs, output format, destination path, and replacement mode.
- The runtime supports local file-based delivery for at least one representative export type.
- Every successful run emits a manifest describing generated artifacts.
- Audit and lineage records capture publish-specific metadata in addition to the shared stage contract.
- Operators can perform date-scoped reruns and backfills for one publish definition.

## Migration Considerations

- Start with one representative file-based export pattern rather than multiple delivery styles at once.
- Prefer outputs derived from stable `level4` contracts instead of allowing consumer-specific logic to bypass earlier stages.
- Keep destination-specific transport concerns out of the core runtime until the artifact contract is proven.
- Preserve room for future orchestration wrappers by keeping the CLI contract authoritative.

## Risks

- Consumer expectations for replacement behavior may differ unless the contract is explicit per publish definition.
- File-oriented delivery can produce partial state if writes are not finalized carefully.
- Export-specific logic may drift upstream into `level5` unless stage boundaries are actively reviewed.
- Downstream consumers may treat file names and folder structures as hard contracts, raising the cost of later changes.

## Assumptions

- `level4` remains the primary source for `level5` delivery outputs.
- The first implementation target is local file-based export rather than network transport.
- Shared audit, error, and lineage conventions remain authoritative for `level5` as they do for earlier stages.

## Resolved First-Implementation Decisions

- The first mandatory output formats are CSV and `jsonl`.
- Publish definitions may either export an approved upstream dataset directly or provide an adjacent declarative query for final delivery shaping.
- Superseded artifacts are retained in run-scoped history by default; stable delivery paths may be replaced according to the declared replacement mode, but manifest history must continue to reference both new and superseded artifacts where relevant.
- Versioned run-scoped paths are required for every run even when a stable consumer-facing path is also maintained.
- Required client-neutral delivery metadata is `owning_domain`, `owner_team`, `consumer_label`, and `delivery_purpose`.

## Delivery Recommendation

Phase the `level5` product as follows:

1. Implement publish definition discovery, manifest validation, and explain-mode for local definitions.
2. Implement one representative local file-based export path with manifest generation, starting with CSV.
3. Add `jsonl` export support, rerun and backfill handling, and replacement-mode enforcement.
4. Add operator guidance, examples, and focused tests before considering optional external delivery adapters.

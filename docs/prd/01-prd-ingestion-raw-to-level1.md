# PRD 01: Raw Data Ingestion to Level 1

## Document Status

- Status: Draft
- Product area: `elt_pipeline`
- Stage: Source systems -> `level1`
- Proposed implementation language: Python
- Proposed packaging and environment management: `uv`

## Background

The founding principles for the platform are defined in [00-prd-platform-principles.md](00-prd-platform-principles.md).

The level model and its medallion mapping is defined in [00-prd-architecture-levels-and-governance.md](00-prd-architecture-levels-and-governance.md).

The existing ELT platforms under `legacy stack A` and the user-described `legacy stack B` solution both implement a multi-stage medallion architecture. Over time, these platforms accumulated a wide range of ingestion patterns, configuration approaches, orchestration styles, and stage-specific conventions.

The new `elt_pipeline` solution will consolidate the useful patterns from both platforms into a single Python-based implementation with a simpler, more explicit architecture. This PRD defines the first stage: ingesting raw data from external and internal sources into `level1`.

`level1` is the immutable landing zone for source data. It stores data as close to the source representation as practical, preserving replayability, auditability, and downstream reprocessing.

## Problem Statement

The current ingestion landscape has several issues:

- Source onboarding is slow because implementation and configuration are spread across multiple codebases and conventions.
- Operational behavior differs by source type, making scheduling, retries, and troubleshooting inconsistent.
- Security, state tracking, and run metadata are not expressed as a single product-level contract.
- The future Python replacement needs a clear boundary for what "ingestion" owns versus what later transformation stages own.

Without a well-defined ingestion product, the new platform risks recreating historical complexity instead of reducing it.

## Product Vision

Build a single, configuration-driven ingestion framework in Python that can:

- connect to heterogeneous source systems,
- extract raw payloads reliably,
- persist them into `level1` with strong lineage and replay semantics,
- expose consistent operational behavior across all source types, and
- serve as the foundation for downstream normalization and SQL promotion.

## Goals

- Standardize raw ingestion behavior across REST, database, file, object storage, message, and event-driven sources.
- Persist source data into `level1` with immutable, replayable storage semantics.
- Separate connector logic from source-specific configuration.
- Modularize cross-cutting ingestion concerns into shared libraries rather than duplicating them per connector.
- Support both batch and near-real-time ingestion patterns.
- Provide consistent run metadata, audit events, and error handling.
- Package the solution as a Python project managed by `uv`.

## Non-Goals

- Complex business transformation or conformance logic.
- Consumer-facing marts or publish/export behavior.
- Defining SQL modeling rules beyond what is required to hand off to later stages.
- Recreating every legacy source connector before the new framework is proven.

## Users and Stakeholders

- Data platform engineers maintaining ingestion connectors and shared runtime behavior.
- Analytics engineering and downstream ELT developers consuming `level1` data.
- Platform and DevOps engineers responsible for scheduling, secrets, deployments, and observability.
- Source owners and data stewards onboarding new source systems.

## Scope

This PRD covers:

- extraction of source data,
- optional pagination/windowing/stateful incremental capture,
- raw payload persistence into `level1`,
- ingestion metadata and lineage,
- operational control plane concerns for the ingestion stage.

This PRD does not cover:

- flattening nested source structures into analytics-ready tables,
- SQL modeling in `level3` or `level4`,
- final publish/output artifacts.

## Source Types In Scope

The framework must support source patterns already visible in the existing solutions:

- REST and HTTP APIs
- JDBC-accessible relational databases
- file drops and object storage inputs
- Kafka or equivalent event streams
- internal system feeds and metadata tables

The design should allow new source adapters to be added without changing the core orchestration model.

## Target Product Outcome

For every configured source entity, the system produces:

- raw files or payload objects in `level1`,
- run metadata describing what was extracted,
- source checkpoint or cursor state when incremental extraction is enabled,
- structured audit events for success, partial success, failure, and retry.

## Functional Requirements

### FR1. Connector Framework

The solution shall provide a pluggable connector model with first-class support for:

- REST and HTTP ingestion,
- SQL and relational database ingestion,
- Kafka and stream ingestion,
- object storage ingestion.

Each connector must implement a shared lifecycle:

- validate configuration,
- resolve secrets,
- initialize source client,
- extract data,
- persist raw payloads,
- emit run metadata,
- update checkpoints.

Connector implementations must remain thin and delegate shared cross-cutting behavior to common framework libraries wherever possible.

### FR2. Configuration-Driven Source Definitions

The solution shall externalize source configuration from connector code.

Configuration must define at minimum:

- source name,
- entity name,
- connector type,
- schedule or trigger mode,
- authentication and secret references,
- extraction parameters,
- persistence target pattern,
- save mode semantics,
- state/checkpoint policy.

The preferred direction is a human-readable configuration format such as YAML or TOML, with JSON reserved for structured subdocuments where necessary.

### FR3. Secret and Environment Resolution

The solution shall not store plaintext credentials in repository-managed source definitions.

It must support:

- environment-specific secret resolution,
- a common path or naming convention for secret lookup,
- runtime validation that required secrets exist before execution,
- redaction of sensitive values in logs and metrics.

### FR4. Authentication Strategy Management

The ingestion framework shall distinguish secret storage from authentication workflow execution.

It must support:

- username and password secrets,
- API key secrets,
- static bearer token secrets,
- client id and client secret pairs,
- certificate, PFX, truststore, and CA material,
- runtime acquisition of access tokens before data requests,
- injection of retrieved tokens into subsequent requests,
- per-run token reuse where appropriate,
- refresh or re-acquisition when tokens expire or are rejected,
- redaction of all tokens and credentials from logs and diagnostics.

Authentication strategy must be defined declaratively per source or entity and not be hardcoded into connector-specific source implementations.

### FR5. Batch and Incremental Extraction

The framework shall support:

- full loads,
- date-windowed loads,
- watermark-based incremental loads,
- cursor-based or offset-based progress tracking,
- idempotent reprocessing for a specified time window or batch key.

### FR6. Raw Persistence Contract

`level1` storage must preserve source fidelity and support replay.

For each extracted unit, the framework shall persist:

- the raw payload or source file,
- ingestion timestamp,
- source identifier,
- entity identifier,
- execution/run identifier,
- batch window or checkpoint metadata,
- basic content metadata such as format, compression, and record hint counts when available.

### FR7. Pathing and Partitioning

The framework shall define a standard `level1` layout that is easy to query and replay.

The storage path contract shall be:

```
level1/source=<src>/entity=<entity>/ingest_date=<date>[/window=<label>]/run_id=<id>/<file>
```

The standardized path segments are:
- stage name: `level1`
- source name: `source=<src>`
- entity name: `entity=<entity>`
- ingestion date partition: `ingest_date=<YYYY-MM-DD>` (always arrival day, immutable once written)
- optional extraction window label: `window=<label>`
- run identifier: `run_id=<id>`

**IMPORTANT — Environment handling:**
- `environment` SHALL NOT appear as an in-path segment.
- Environment is handled exclusively by which `--root-path` (storage root / bucket) the pipeline is pointed at.
- Each environment (dev, staging, prod) gets its own independent storage root; this aligns with cloud lakehouse patterns and preserves clean IAM prefix boundaries, point-in-time restore, and environment-to-environment promotion.
- `environment` is still retained on manifests and `RunContext` for audit, logging, and config selection purposes — it is only removed from filesystem paths.

The exact partition shape shall be standardized across connectors as described above.

### FR8. Idempotency and Deduplication

The ingestion product shall prevent accidental duplication caused by retries and reruns.

It must support:

- deterministic object naming or manifest-based writes,
- configurable deduplication keys where the source provides stable identifiers,
- safe rerun behavior for a defined batch or watermark interval,
- append-only raw retention with metadata-level de-dup if payload replacement is not safe.

### FR9. Scheduling and Triggering

The framework shall support:

- scheduled batch runs,
- ad hoc backfills,
- event-driven runs,
- manual replay of a specific source/entity/window.

All execution modes must produce the same run metadata and observability outputs.

### FR10. Operational Metadata

The shared observability, audit, and error-handling contract for all stages is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

For ingestion, every run shall emit structured metadata including:

- run id,
- source and entity,
- trigger type,
- start and end time,
- source window processed,
- status,
- record/file counts when available,
- bytes read and bytes written when available,
- checkpoint before and after the run,
- failure reason taxonomy.

### FR11. Error Handling

The shared error-handling contract is defined in [00-prd-shared-observability-audit-and-error-handling.md](00-prd-shared-observability-audit-and-error-handling.md).

For ingestion, the framework shall distinguish:

- transient errors that may be retried automatically,
- configuration errors that must fail fast,
- source data contract issues that should be captured as quarantined failures,
- downstream persistence errors that require write rollback or run invalidation semantics.

### FR12. Reprocessing

Operators shall be able to:

- re-run a source entity for a past date or date range,
- rebuild `level1` for a given source checkpoint interval,
- replay a failed window without hand-editing configuration,
- trace exactly which `level1` artifacts belong to which run.

### FR13. Ingestion Pattern Coverage

The ingestion framework shall explicitly support multiple ingestion patterns rather than treating all sources as variants of a single pull process.

The required first-class connector families for the initial release are:

- `rest`
- `sql`
- `kafka`
- `object_storage`

These are the concrete connector families that the new platform must implement as top-level runtime abstractions.

Other recurring behaviors such as pagination, delta extraction, replay, and event-driven invocation are not separate connector families. They are reusable capabilities or execution modes layered onto these four families.

Each pattern must reuse the same runtime contracts for:

- configuration loading,
- secret resolution,
- metadata emission,
- raw persistence,
- error handling,
- checkpoint updates.

### FR14. REST Pagination

The REST ingestion product shall support paginated extraction as a first-class capability.

It must support:

- page-based pagination,
- offset-based pagination,
- limit and page-size configuration,
- response-driven total record count extraction,
- termination when no more records are present,
- idempotent reruns of the same paginated window.

The pagination contract must allow configuration of:

- request-side pagination parameters,
- response-side content location,
- response-side total count location,
- starting page or offset,
- page size,
- maximum page guardrails.

### FR15. Envelope and Payload Extraction

The ingestion framework shall support responses where the transport payload is an envelope around one or more inner payloads.

This applies not only to REST responses, but to any ingest pattern where the raw response contains:

- wrapper metadata,
- business identifiers,
- embedded payload fields,
- encoded inner documents.

The product must support:

- retaining the full original envelope in `level1`,
- extracting inner payloads as separate raw artifacts when configured,
- preserving linkage between envelope and extracted payload,
- supporting one-to-many payload extraction from a single response,
- extracting identifier fields from the envelope for naming, lineage, and deduplication.

### FR16. Embedded Payload Format Handling

For envelope-based ingestion, the framework shall support inner payloads whose content type differs from the outer response format.

Examples include:

- JSON responses containing XML payload strings,
- JSON responses containing CSV payload strings,
- JSON or XML responses containing base64-encoded payloads,
- message envelopes containing nested JSON payload documents.

The product must support configured handling for:

- payload location,
- payload format,
- payload encoding,
- decompression or decoding requirements,
- naming of extracted payload entities.

### FR17. Envelope Support Across Connectors

Envelope handling shall be a shared ingestion capability, not a REST-only special case.

It must be reusable across:

- REST connectors,
- message and event connectors,
- object and file ingest connectors,
- any future connector that receives wrapped business payloads.

The product shall implement envelope handling as a reusable library or framework module with a stable interface, so that connector authors configure and invoke it rather than rewriting the logic inside each connector.

### FR18. REST Request Flexibility

The REST ingestion product shall support a broad range of request patterns, including:

- GET and POST methods,
- parameterized query strings,
- parameterized request bodies,
- date-window placeholders,
- custom headers,
- bearer token flows,
- basic authentication,
- API key injection,
- pre-request token acquisition where required,
- client-credential-style auth requests,
- form-urlencoded or JSON auth request bodies,
- custom token response extraction paths,
- configurable injection of retrieved tokens into subsequent headers, query parameters, or request bodies.

### FR19. REST Resilience Controls

The REST connector runtime shall support:

- source-specific retry policies,
- backoff strategies,
- rate limiting,
- timeout controls,
- circuit breaking or failure thresholds,
- response status classification,
- detection of malformed or partial responses.

### FR20. Database Snapshot and Delta Modes

The database ingestion product shall support both:

- full snapshot extraction,
- delta extraction based on one or more watermark columns.

These modes must be configurable per entity or table.

The framework shall support:

- `created_at`-based deltas,
- `updated_at`-based deltas,
- source-specific alternative watermark columns,
- full refresh and incremental operation side by side within the same source domain,
- different extraction SQL per table.

### FR21. SQL Template and Dynamic Filter Support

Database ingestion shall support SQL templates and dynamic filter injection.

The framework must allow configuration of:

- a base extraction query,
- one or more dynamic filter templates,
- named filter tokens such as `max_created_at` or `max_updated_at`,
- default starting watermark values,
- source-specific table lists and query assignments.

Token replacement must be deterministic and recorded in run metadata at least in redacted or compiled form.

### FR22. State Resolution from Loaded Data

The ingestion framework shall support deriving delta state from previously loaded platform data, not only from connector-local checkpoints.

It must support resolving ingestion watermarks from configurable sources such as:

- ingestion state tables,
- prior `level1` loads,
- prior `level2` loads,
- prior `level3` loads,
- other platform-managed run metadata stores.

This is required to preserve the historical capability described by the user, where downstream loaded state can drive the next source query window.

### FR23. Multi-Step Watermark Resolution

For delta database extraction, the product shall support a two-step or multi-step process:

1. determine the last successfully loaded point from configured platform state,
2. compile the source extraction SQL with the resolved watermark,
3. execute the source query and persist new raw results,
4. update checkpoint state only after successful persistence.

The state-resolution query source must be configurable per source entity.

### FR24. Multi-Table Database Domains

The database ingestion framework shall support a single source definition that manages many source tables or entities with table-specific extraction behavior.

Per-table configuration may include:

- save mode,
- extraction SQL template,
- delta strategy,
- default watermark,
- partitioning behavior,
- inclusion or exclusion from a run.

### FR25. File and Object Ingestion

The framework shall support ingestion patterns where source data already exists as files or objects.

It must support:

- pickup from object storage or shared file locations,
- copy-only raw landing,
- optional metadata capture,
- event-driven and scheduled discovery modes,
- replay of missed or failed file arrivals.

### FR26. Message and Stream Ingestion

The framework shall support message-oriented ingestion for sources such as Kafka or equivalent event systems.

It must support:

- configurable topic or stream subscriptions,
- batch or micro-batch landing to `level1`,
- offset or cursor tracking,
- optional schema-registry-aware decoding,
- preservation of original message payloads and transport metadata.

### FR27. Uniform Raw Persistence for Derived Artifacts

When ingestion extracts inner payloads from an envelope, decodes messages, or copies objects from external systems, the framework shall preserve:

- the original source artifact,
- the derived raw artifact when one is created,
- lineage between them,
- run-level metadata linking all artifacts within the same ingestion execution.

### FR28. Configuration Model for Advanced Ingestion

The ingestion configuration contract shall be expressive enough to define:

- source connector type,
- authentication strategy,
- secret references,
- auth request definitions,
- auth response mappings,
- auth injection rules,
- request or query templates,
- pagination behavior,
- envelope extraction rules,
- payload decoding rules,
- watermark state source,
- snapshot versus delta mode,
- entity-specific overrides.

The configuration model should avoid embedding business logic in code for standard source behaviors.

### FR29. Shared Capability Libraries

The ingestion framework shall modularize shared behaviors into reusable libraries or framework modules.

Examples include:

- envelope and payload extraction,
- pagination,
- payload decoding,
- compression handling,
- watermark resolution,
- checkpoint persistence,
- raw artifact naming and lineage emission.

These capabilities must be:

- independently testable,
- connector-agnostic,
- composable within multiple ingestion patterns,
- invoked through stable interfaces rather than copied source-specific implementations.

### FR30. No Per-Connector Reimplementation of Shared Logic

The product shall explicitly avoid a design where common ingestion behaviors are reimplemented separately for REST, database, file, and message connectors.

For shared concerns such as envelope payload handling, the approved architecture is:

- one shared capability implementation,
- connector-specific adaptation only where transport semantics differ,
- configuration-driven use of the shared capability by each connector.

Any exception to this rule must be justified by a source-specific technical constraint.

### FR31. Source Definition Contract

Each configured source in the new platform shall be defined as:

- one connector family,
- one or more source entities,
- one execution mode,
- zero or more enabled shared capabilities,
- one target `level1` persistence contract.

At minimum, every source definition must declare:

- `connector_type`
- `source_name`
- `entity_name` or `entity_list`
- `execution_mode`
- `capabilities_enabled`
- `auth_strategy`
- `auth_secret_refs`
- `auth_request_definition` when token acquisition is required
- `auth_response_mapping`
- `auth_injection_rule`
- `source_parameters`
- `target_level1_contract`
- `checkpoint_strategy`
- `replay_strategy`

### FR32. Execution Modes

The platform shall define execution mode separately from connector family.

Required execution modes are:

- `scheduled_batch`
- `event_driven`
- `micro_batch`
- `manual_replay`
- `backfill`

The same connector family may support multiple execution modes.

The `event_driven` mode must explicitly support EventBridge-style orchestration in addition to S3-event, SQS, and Lambda-triggered execution, reflecting capabilities present in the Legacy Stack A solution but not in the older Legacy Stack B stack.

### FR33. Top-Level Pattern Admission Rule

A new top-level ingestion connector family shall only be created when the transport contract is materially different from the existing families.

Differences such as:

- pagination style,
- authentication method,
- envelope structure,
- payload encoding,
- checkpoint source,
- snapshot versus delta behavior,

do not justify a new connector family on their own. They must be expressed as shared capabilities or source-specific configuration on top of an existing family.

## Non-Functional Requirements

### NFR1. Reliability

- Ingestion must be restartable.
- Checkpoint updates must happen only after durable persistence succeeds.
- Partial failures must be explicit and observable.

### NFR2. Scalability

- The platform must support parallel execution across sources and entities.
- Large sources must be shardable by date range, partition key, or source-native pagination.

### NFR3. Security

- Secrets remain outside repo-managed config.
- Sensitive fields must not appear in logs.
- Encryption at rest and in transit is mandatory.
- Authentication material that must be materialized to disk, such as PFX or truststore files, must be written ephemerally and cleaned up after use.

### NFR4. Observability

- Structured logs, metrics, and run events are required.
- The system must support alerting on repeated failures, lag, and missing expected arrivals.

### NFR5. Maintainability

- Shared ingestion behavior must live in reusable framework components rather than source-specific scripts.
- New source onboarding should be configuration-heavy and code-light.

### NFR6. Portability

- The Python codebase must run locally for development and in the target cloud execution environment.
- `uv` must manage project dependencies, lockfile generation, and reproducible environments.

### NFR7. Extensibility

- New ingestion patterns should be composable from shared primitives such as pagination, envelope extraction, decoding, checkpoint resolution, and raw writers.
- Connector authors should not need to reimplement pagination or envelope logic inside source-specific code.

### NFR8. Modularity

- Cross-cutting ingestion concerns must live in dedicated modules with clear interfaces and ownership.
- The architecture should minimize coupling between transport connectors and shared processing capabilities.
- Shared libraries should be reusable by future ingestion types without structural refactoring.

## Proposed Product Design

### Runtime Model

The proposed ingestion product consists of:

- a Python package containing core abstractions, connectors, state handling, storage writers, and observability components,
- source configuration files stored separately from connector code,
- a runner CLI for executing ingestion jobs locally or in orchestration environments,
- deployment wrappers for scheduled and event-driven execution.

### Package Shape

The initial solution should be organized around:

- `elt_pipeline.ingest.connectors`
- `elt_pipeline.ingest.runtime`
- `elt_pipeline.ingest.pagination`
- `elt_pipeline.ingest.envelope`
- `elt_pipeline.ingest.decoding`
- `elt_pipeline.ingest.capabilities`
- `elt_pipeline.ingest.state`
- `elt_pipeline.ingest.storage`
- `elt_pipeline.ingest.observability`
- `elt_pipeline.config`
- `elt_pipeline.cli`

### Delivery Unit

The ingestion framework should allow teams to deploy:

- a single shared runtime package,
- source configuration bundles per environment,
- orchestration definitions that invoke the CLI with source/entity/window parameters.

### Connector Capability Model

The new ingestion runtime should model advanced behaviors as reusable capabilities layered onto connectors.

The required reusable capability set for the initial release is:

- authentication strategy execution,
- pagination,
- envelope extraction,
- payload decoding,
- watermark resolution,
- snapshot or delta execution,
- cross-account access,
- event replay,
- source throttling,
- lineage and manifest emission.

This allows a REST or message connector to share the same envelope logic, and allows a database connector to share the same watermark-resolution mechanism across many tables.

### Required Connector Families

The first release of `elt_pipeline` shall implement the following connector families as first-class product components:

#### `rest`

Use for:

- HTTP and HTTPS source systems,
- API integrations,
- webhook pullbacks where the source contract is HTTP-based.

Required semantics:

- request templating,
- authentication resolution,
- token acquisition and token injection flows,
- date-window parameterization,
- response selection,
- optional pagination,
- optional envelope extraction.

#### `sql`

Use for:

- relational database sources accessed through SQL or JDBC.

Required semantics:

- full snapshot extraction,
- delta extraction,
- per-table configuration,
- SQL templates,
- watermark resolution from platform state,
- mixed snapshot and delta behavior inside one source domain.

#### `kafka`

Use for:

- Kafka topics,
- equivalent ordered event streams when the contract is offset-based streaming consumption.

Required semantics:

- topic or stream subscription,
- offset management,
- raw message landing,
- transport metadata preservation,
- replay by offset or time window where supported.

#### `object_storage`

Use for:

- S3 and blob-style object stores,
- same-account file pickup,
- cross-account file pickup,
- manifest-based object sync patterns.

Required semantics:

- list and discover objects,
- copy or sync to `level1`,
- same-account and cross-account access,
- replay by date, path, prefix, or manifest state.

### Non-Connector Patterns

The following are important platform behaviors but are not top-level connector families:

- event-driven invocation,
- scheduled execution,
- replay and backfill,
- document parsing,
- content extraction,
- lambda wrappers,
- external actors or scraper runners.

These must be modeled either as:

- execution modes,
- reusable capabilities,
- or source-specific adapters on top of the required connector families.

### Architectural Constraint

The new ingestion solution must prefer shared capability modules over connector-owned implementations for any behavior that can reasonably recur across source types.

Envelope payload handling is the canonical example:

- it must be implemented once,
- exposed through a reusable library interface,
- configured per source or entity,
- and consumed by any connector that receives wrapped payloads.

This same architectural rule should be applied to other recurring concerns such as pagination, decoding, compression, checkpointing, and lineage emission.

### Detailed Ingestion Patterns

#### REST and HTTP Ingestion

The first release should treat REST as a top-tier ingestion pattern because the discovered `legacy stack A` implementation demonstrates meaningful maturity in:

- paginated extraction,
- response-content targeting,
- envelope unwrapping,
- encoded payload extraction,
- date-window parameterization.

The new product should preserve these strengths while simplifying the configuration model.

#### Database Ingestion

The first release should also treat database extraction as a top-tier ingestion pattern because both legacy solutions are described as strong in this area.

The product must preserve the ability to:

- perform full snapshots,
- perform deltas,
- resolve the next query window from previously loaded platform state,
- manage many tables under one source domain,
- mix extraction strategies by table.

#### Other Ingestion Patterns

The product design must remain open to other ingestion patterns without redesigning the framework.

Initial extensibility targets include:

- event-driven object ingestion,
- stream ingestion,
- webhook-driven ingestion,
- source-system file exports,
- internal replay and recovery flows.

### Definition of Required Coverage

The ingestion layer is only considered pattern-complete for its first release when it can demonstrate:

- one `rest` source with pagination and envelope extraction,
- one `sql` source with full snapshot support,
- one `sql` source with downstream-state-driven delta support,
- one `kafka` source with offset or cursor tracking,
- one `object_storage` source with cross-account access,
- one shared envelope capability used by at least two connector families,
- one shared replay mechanism exercised across more than one family.

### Example Configuration Concepts

The final configuration syntax is still open, but the product must be able to express concepts such as:

- a REST source that resolves username/password secrets, calls an auth endpoint, extracts `access_token`, and injects `Authorization: Bearer <token>` into later API calls,
- a paginated REST endpoint whose response body contains a collection path and total-record-count path,
- an envelope response whose payload field contains base64 XML,
- a database table using `select_updated_at` plus a dynamic `delta_updated_at` watermark lookup,
- a source whose delta point is derived from already loaded platform data rather than only a local checkpoint.

## Data Contract for Level 1

Each `level1` write must carry enough metadata for downstream reproducibility.

Minimum metadata contract:

- `run_id`
- `source_name`
- `entity_name`
- `ingest_mode`
- `ingest_started_at`
- `ingest_completed_at`
- `window_start`
- `window_end`
- `checkpoint_before`
- `checkpoint_after`
- `payload_format`
- `compression`
- `content_hash` where practical
- `record_count_estimate` where practical

This metadata may be stored as:

- object metadata,
- sidecar manifest files,
- a dedicated run log table,
- or a combination of these.

## Success Metrics

- New source onboarding time is materially reduced from the legacy baseline.
- A new source can usually be onboarded using one of the four required connector families without adding a new top-level pattern.
- Operators can replay any failed run without code changes.
- All `level1` assets are attributable to a specific run and source window.
- The framework supports representative sources for `rest`, `sql`, `kafka`, and `object_storage` in the first release.

## Acceptance Criteria

- A Python `uv` project can execute a configured ingestion run locally and in the target runtime.
- Source configuration is externalized and validated before execution.
- The framework can persist raw data and run metadata into `level1`.
- Incremental extraction works for at least one database source and one API source.
- Failed runs do not advance checkpoints incorrectly.
- Operators can trigger replay for a prior interval.
- Envelope payload handling is implemented as a reusable shared module and exercised by more than one ingestion pattern without duplicate logic.
- The implementation provides first-class connector families for `rest`, `sql`, `kafka`, and `object_storage`.
- At least one representative source is proven for each of the four required connector families.
- Cross-account object storage ingestion is demonstrated for the `object_storage` family.
- SQL ingestion supports both snapshot and delta modes with delta state resolved from platform-managed history.

## Migration Considerations

- Existing sources should be prioritized by business criticality and connector similarity.
- The first migration wave should favor sources with clear contracts and high reuse potential.
- Legacy operational semantics that vary per source should be normalized unless there is a strong business reason to preserve them.

## Risks

- Source-specific behavior embedded in legacy jobs may not be obvious from configuration alone.
- Some sources may require custom rate limiting, anti-bot handling, or specialized pagination semantics.
- Checkpoint logic can become inconsistent if connector authors bypass the shared runtime.

## Assumptions

- The new platform will preserve a medallion-style architecture but simplify stage semantics.
- `level1` remains the canonical immutable raw zone.
- This document uses both the discovered `legacy stack A` and `legacy stack B` implementations as the current baseline, plus the user's design constraints about simplifying the future platform.

## Open Questions

- Which orchestration platform will be the primary runtime target for `elt_pipeline`?
- Should source definitions live in the same repo as implementation, or in a separate config repo?
- Is a single `level1` storage contract sufficient for both batch and streaming ingestion, or should streaming use a variant?
- Which metadata store should own run history and checkpoint state?

## Delivery Recommendation

Phase the ingestion product as follows:

1. Build the Python shared runtime and CLI.
2. Implement the four required connector families: `rest`, `sql`, `kafka`, and `object_storage`.
3. Add shared capabilities for envelope handling, pagination, checkpoint resolution, cross-account access, and replay.
4. Prove definition-of-done coverage with representative sources before broader migration.

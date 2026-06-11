# Ingestion Technique Deep Dive

## Purpose

This document defines the full required ingestion feature set for `elt_pipeline` by synthesizing the implementation techniques found in the legacy `camelot` and `mercell` solutions.

It is intended to answer:

- what the ingestion layer must support,
- which features are first-class requirements,
- which features belong to connector families versus shared capabilities,
- and which legacy behaviors are edge cases that must not be missed during redesign.

This document complements:

- [01-prd-ingestion-raw-to-level1.md](file:///Users/rpatel/Documents/__code/archive/elt_pipeline/docs/prd/01-prd-ingestion-raw-to-level1.md)
- [04-ingestion-inventory-camelot-mercell.md](file:///Users/rpatel/Documents/__code/archive/elt_pipeline/docs/prd/04-ingestion-inventory-camelot-mercell.md)

## Final Ingestion Model

The future ingestion layer should be defined using:

- **4 first-class connector families**
  - `rest`
  - `sql`
  - `kafka`
  - `object_storage`
- **shared reusable capabilities**
  - authentication and secret resolution
  - request/query templating
  - date-window tokenization
  - pagination
  - envelope payload extraction
  - payload decoding and decompression
  - snapshot vs delta execution
  - checkpoint and watermark resolution
  - replay and backfill
  - lineage, manifests, and raw artifact naming
  - event-driven wrappers

The deep dive across both codebases confirms this is sufficient to represent the full observed breadth without introducing unnecessary extra connector families.

## Definition Rules

### Connector Family

A connector family is a transport contract for reading from a source system.

Examples:

- HTTP request/response for `rest`
- JDBC/SQL extraction for `sql`
- offset-based stream consumption for `kafka`
- object listing and copy/sync for `object_storage`

### Shared Capability

A shared capability is reusable behavior that may apply to multiple connector families.

Examples:

- envelope extraction
- pagination
- checkpoint resolution
- replay
- payload decoding

### Execution Mode

Execution mode is how the connector runs, not what kind of connector it is.

Required modes:

- `scheduled_batch`
- `event_driven`
- `micro_batch`
- `manual_replay`
- `backfill`

## Required Connector Definitions

### `rest`

Use for any source whose primary data contract is HTTP or HTTPS.

The deep dive shows the new connector must support all of the following:

- GET requests
- POST requests
- query-string parameter injection
- request-body parameter injection
- bearer token authentication
- basic authentication
- token-acquisition-before-request flows
- configurable token extraction and later request injection
- mutual TLS or PFX-backed requests
- async redirect/poll workflows
- date-window token substitution
- optional entity-specific overrides
- response-content targeting
- pagination
- wrapper or envelope payload handling

Legacy evidence:

- Camelot REST batch GET, async redirect polling, and auth-driven flows in `ingest-rest`
- Camelot specialized unsubscribe POST workflow
- Mercell generic REST, bearer-token pagination, mTLS/PFX fetches, and source-specific custom runners

### `sql`

Use for any source whose primary data contract is relational query execution.

The deep dive shows the new connector must support all of the following:

- JDBC-based extraction
- full snapshot extraction
- delta extraction
- per-table extraction behavior inside one source domain
- SQL templates per table
- static filters
- dynamic filters
- placeholder substitution for watermarks
- default watermark values
- checkpoint derivation from platform-managed state
- primary/secondary database failover
- empty-table retry and optional fail-on-empty
- date-range batch iteration
- source-specific save modes

Legacy evidence:

- Camelot SQL JDBC framework with dynamic Athena-backed filters, failover, and empty-table handling
- Mercell SQL framework with dynamic filter mutation, level_1 parquet state queries, and date-range modes

### `kafka`

Use for event-stream sources whose contract is topic-based offset consumption.

The deep dive shows the new connector must support all of the following:

- topic subscription
- offset tracking
- consumer-group behavior
- raw event landing to `level1`
- event metadata preservation
- schema-aware decode, especially Avro + Schema Registry
- replay by offset or time range where supported
- event-driven and micro-batch execution modes

Legacy evidence:

- Mercell production-grade Kafka landing and direct Spark Structured Streaming transform paths
- Camelot experimental Kafka consumers and protobuf decoding utilities

### `object_storage`

Use for file/object sources where the system must copy, sync, or discover objects from storage.

The deep dive shows the new connector must support all of the following:

- object discovery by prefix
- object listing
- differential sync based on key, size, and modification state
- raw copy to `level1`
- same-account access
- cross-account access
- prefix/date-based replay
- optional processed-file move/archive semantics
- event-driven object-arrival triggering

Legacy evidence:

- Camelot S3 sync patterns for Tealium, Sprinklr, and other external buckets
- Mercell S3-event and file/object raw landing plus replay services

## Required Shared Capabilities

### 1. Secret and Config Resolution

This must be a common runtime service.

Required behavior:

- environment-scoped config loading
- secret indirection rather than plaintext credentials
- parameter-store or secrets-manager style resolution
- runtime substitution of env/date tokens
- typed extraction of arrays, maps, and nested config blocks

Observed in both stacks:

- Camelot and Mercell both rely on configuration-driven ingestion with externalized secrets

### 2. Authentication Strategy Execution

This must be a common runtime capability layered primarily onto `rest`, but usable anywhere a connector needs pre-flight authentication.

Required behavior:

- resolve username/password secrets
- resolve API keys and static bearer tokens
- resolve client id and client secret pairs
- resolve certificate, PFX, truststore, and CA material
- execute token-acquisition requests before data requests
- support auth request bodies in JSON or form-urlencoded form
- extract tokens from configurable response locations
- inject tokens into later requests as headers, query parameters, or request-body fields
- reuse tokens within a run where appropriate
- refresh or reacquire tokens when rejected or expired
- redact all credentials and tokens in logs and diagnostics

Observed strongly in both stacks:

- Camelot uses auth-first then data-call flows for REST action patterns
- Mercell uses bearer-token acquisition before paginated data extraction for sources such as Mitudbud

### 3. Request and Query Templating

The system must support template substitution across both `rest` and `sql`.

Required behavior:

- date-window placeholders
- environment placeholders
- watermark placeholders
- request-argument substitution
- SQL token substitution

This is a strong commonality across both legacy stacks.

### 4. Pagination

This is a required first-class capability, not an ad hoc source customization.

Required behavior:

- page-based pagination
- offset-based pagination
- content path extraction
- total-count extraction
- start page/offset configuration
- page size configuration
- stop conditions
- restart or consistency handling when total count changes mid-run

Observed strongly in Mercell.

### 5. Envelope Payload Extraction

This is one of the most important shared capabilities.

Required behavior:

- preserve the full outer payload
- extract identifiers from the envelope
- extract one or more inner payloads
- support payload field selection
- support different inner payload formats than the outer transport format
- preserve lineage from inner payload back to outer response or object

Required payload variants:

- JSON envelope with XML payload
- JSON envelope with CSV payload
- JSON or XML envelope with base64 payload
- message envelope with nested JSON payload

Observed strongly in Mercell at ingest time and in Camelot at downstream parsing and mapping time.

### 6. Payload Decoding and Compression Handling

The new platform must centralize decoding and decompression.

Required behavior:

- base64 decode
- gzip handling
- zip handling
- tar.gz handling
- charset-aware text decode
- optional XML/JSON parsing from a string column or nested field

Observed strongly in Mercell object processing and needed by REST envelope extraction.

### 7. Snapshot and Delta Execution

This must be a shared execution abstraction, especially for `sql`, but also reusable where appropriate for `rest` and `object_storage`.

Required behavior:

- full snapshot mode
- incremental mode
- configurable watermark columns
- configurable default watermark values
- mix of snapshot and delta inside one source domain

Observed strongly in both Camelot and Mercell SQL ingestion.

### 8. Checkpoint and Watermark Resolution

This capability must support more than a simple local state file.

Required behavior:

- checkpoint resolution from ingestion state
- checkpoint resolution from previously landed `level1`
- checkpoint resolution from `level2`
- checkpoint resolution from `level3`
- configurable state-source query per source or entity
- update checkpoint only after successful raw persistence

This is a critical requirement because both legacy stacks derive incremental logic from what is already loaded in platform layers.

### 9. Replay and Backfill

Replay must be a platform capability, not an incidental script.

Required behavior:

- rerun by date range
- rerun by partition
- rerun by source entity
- rerun by S3 prefix or manifest state
- rerun by stream offset/time window
- rerun after mapping/config changes without source re-extraction where appropriate

Observed in both stacks, especially in object/event-oriented flows.

### 10. Lineage and Raw Artifact Linking

Every ingestion run must emit enough metadata to trace outcomes.

Required behavior:

- stable run id
- source and entity metadata
- target level_1 location
- checkpoint before and after
- outer-envelope to inner-payload linkage
- original-object to copied-object linkage
- message metadata to landed-object linkage

### 11. Event-Driven Wrappers

Event-driven triggers are required, but they are wrappers around connector execution.

Required behavior:

- S3/object event triggers
- SQS/message triggers
- Kafka-triggered Lambda or stream processing
- EventBridge-triggered routing and replay
- path-based or entity-based route decisions
- ignore/suppression rules
- tuned execution profiles per source or path

Observed strongly in both stacks.

Important legacy distinction:

- Mercell includes a more modern AWS EventBridge-oriented event architecture for ingestion triggering and replay orchestration.
- Camelot predates that approach and relies more heavily on Lambda, SQS, S3 events, EMR step submission, and scheduled pipeline activities.

The future platform must preserve Mercell's EventBridge-style orchestration as an explicit supported event-wrapper pattern rather than collapsing everything into only cron- or S3-triggered execution.

## Technique-Level Deep Dive

### REST Deep Dive

The full set of REST definitions needed in `elt_pipeline` is:

- request method support: GET and POST
- auth strategies: bearer, basic, token acquisition, mTLS/PFX
- parameter strategies:
  - static params
  - dynamic date-window params
  - runtime overrides
  - downstream-state-derived params
- response strategies:
  - full raw response landing
  - content-path extraction
  - async redirect polling
  - paginated iteration
  - wrapper/envelope extraction
- resilience strategies:
  - retries
  - timeouts
  - backoff
  - malformed response handling
  - 429/5xx response handling

Important outliers from legacy systems that must be accounted for:

- Camelot async redirect/poll behavior
- Camelot POST unsubscribe/action workflows
- Mercell page-based and offset-based pagination
- Mercell envelope extraction with base64 XML payloads
- Mercell source-specific runners for APIs that do not fit the generic config shape

### SQL Deep Dive

The full set of SQL definitions needed in `elt_pipeline` is:

- connection resolution:
  - primary connection
  - optional secondary/failover connection
- extraction strategies:
  - full table
  - filtered table
  - query resource execution
  - date-range iteration
  - per-table save modes
- delta strategies:
  - `created_at`
  - `updated_at`
  - arbitrary business watermark
  - watermark query based on prior loaded platform state
- quality/safety strategies:
  - empty-table retry
  - fail-on-empty for designated tables
  - config validation for incompatible filter/query combinations

Important outliers from legacy systems that must be accounted for:

- Camelot primary/secondary DB failover
- Camelot dynamic Athena-backed filter resolution
- Mercell state resolution from prior level_1 parquet
- multi-table source domains with table-specific behavior in both stacks

### Kafka Deep Dive

The full set of Kafka definitions needed in `elt_pipeline` is:

- topic subscription
- offset tracking
- consumer group support
- schema-registry-backed decode
- raw event landing
- preservation of topic/partition/offset metadata
- business-date-aware partitioning where needed
- event replay by offset or time window
- both event-driven and micro-batch modes

Important outliers from legacy systems that must be accounted for:

- Mercell has multiple production-grade Kafka landing variants with different partitioning semantics
- Mercell supports direct streaming into the structured layer, which may influence future architecture even if initial ingestion only lands raw data
- Camelot Kafka is weaker operationally, but does show protobuf decode and streaming consumer shapes that may still matter

### Object Storage Deep Dive

The full set of object-storage definitions needed in `elt_pipeline` is:

- object listing and discovery
- differential sync
- same-account access
- cross-account access
- optional processed-file move/archive
- object-arrival event processing
- EventBridge-driven replay and path fan-out
- replay by prefix/date/manifest
- raw file preservation
- integration with downstream parsers for JSON, CSV, XML, Excel, parquet, and compressed assets

Important outliers from legacy systems that must be accounted for:

- Camelot intraday sync behavior
- Camelot processed-file move behavior
- Mercell replay tooling over S3/event paths
- Mercell EventBridge-based replay and orchestration services
- Mercell compressed-file extraction and delayed-consistency handling

## Required Format Support

Observed across the two stacks, the ingestion layer and its adjacent normalization boundary must be prepared for:

- JSON
- multiline JSON
- multi-document JSON
- CSV
- multiline CSV
- XML
- Excel
- Parquet
- gzipped files
- zip archives
- tar.gz archives
- embedded payloads in text fields

Not every format belongs in the transport connector itself, but the ingestion-layer definition must account for the raw contract and handoff requirements for all of them.

## Required Edge-Case Definitions

The new platform must explicitly define behavior for:

- empty API responses
- partially malformed API responses
- redirect-style async REST responses
- changing pagination totals mid-run
- empty source tables
- secondary database failover
- corrupted or empty compressed archives
- folder markers and pseudo-files in object events
- duplicate or re-arriving objects
- stream replay and cutoff semantics
- config shapes that do not fit the generic runner and require source adapters

## What Must Be First-Class In The Design

The deep dive shows these are not optional nice-to-haves. They must be explicit product definitions:

- pagination as a reusable library
- envelope extraction as a reusable library
- checkpoint/watermark resolution as a reusable library
- snapshot/delta execution semantics
- event-driven wrapper semantics
- EventBridge-oriented orchestration semantics
- cross-account object-storage access
- run metadata and lineage
- replay and backfill

## What Must Not Become Separate Connector Families

The deep dive also shows the following should remain capabilities or adapters, not separate top-level patterns:

- redirect polling
- token-acquisition flows
- unsubscribe/action REST posting
- document parsing
- compressed file extraction
- replay services
- lambda-triggered execution
- MongoDB

MongoDB may become a later connector if strategically required, but it is not needed to define the complete ingestion model for the current refactor.

## Implementation Guidance

To fully preserve the legacy feature set while simplifying architecture, `elt_pipeline` should implement:

1. four connector-family interfaces
2. a shared capability library for cross-cutting features
3. a typed source-definition contract
4. explicit execution-mode abstractions
5. a run metadata and replay subsystem

## Conclusion

After the deep dive across both `camelot` and `mercell`, the ingestion layer can be considered fully defined at the technique level if it explicitly supports:

- `rest`
- `sql`
- `kafka`
- `object_storage`

plus the shared capability set defined in this document.

That captures the full breadth of the legacy solutions without inheriting their structural duplication.

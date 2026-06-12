# Ingestion Technique Deep Dive

## Purpose

This document defines the required ingestion feature set for `elt_pipeline`.

The feature set is derived from techniques observed across mature legacy ELT stacks, but this repository remains client-neutral:

- no client names, vendor names, or legacy repository names appear in this document
- legacy stacks are used only as an internal baseline to define a generic superset of ingestion patterns

This document complements:

- [01-prd-ingestion-raw-to-level1.md](01-prd-ingestion-raw-to-level1.md)
- [04-ingestion-inventory-legacy-baseline.md](04-ingestion-inventory-legacy-baseline.md)

## Final Ingestion Model

The ingestion layer is defined using:

- 4 first-class connector families: `rest`, `sql`, `kafka`, `object_storage`
- shared reusable capabilities layered onto those families
- execution modes that control scheduling/triggering semantics

## Definition Rules

### Connector Family

A connector family is a transport contract for reading from a source system.

- `rest`: HTTP request/response
- `sql`: JDBC/SQL query execution
- `kafka`: offset-based stream consumption
- `object_storage`: object listing and copy/sync

### Shared Capability

A shared capability is reusable behavior that may apply to multiple connector families.

Examples:

- authentication strategy execution
- request/query templating
- pagination
- envelope extraction
- payload decoding
- snapshot/delta execution
- checkpoint and watermark resolution
- replay/backfill
- lineage and raw artifact naming

### Execution Mode

Execution mode is how the connector runs, not what kind of connector it is.

Required modes:

- `scheduled_batch`
- `event_driven`
- `micro_batch`
- `manual_replay`
- `backfill`

## Required Connector Families

### `rest`

Required semantics:

- GET and POST
- query string and body templating
- auth strategies: bearer, basic, api key, token acquisition before data requests, mTLS material references
- date-window token substitution
- response content targeting
- pagination as a reusable capability
- envelope extraction as a reusable capability
- resilience controls: retries, timeouts, backoff, status classification

### `sql`

Required semantics:

- snapshot extraction
- delta extraction
- per-table extraction behavior inside one source domain
- SQL templates per table
- static and dynamic filter injection
- watermark resolution from platform state
- checkpoint update only after successful persistence
- optional primary/secondary failover
- empty table handling policies where required

### `kafka`

Required semantics:

- topic subscription
- offset tracking
- raw event landing to `level1`
- transport metadata preservation
- optional schema-registry-aware decoding
- replay by offset range and time window

### `object_storage`

Required semantics:

- object discovery and listing
- differential sync
- raw copy to `level1`
- same-account and cross-account access
- object-arrival event triggering
- replay by prefix and date range

## Required Shared Capabilities

### Secret and Config Resolution

- environment overlays
- secret indirection via secret references
- deterministic token substitution
- validation before execution

### Authentication Strategy Execution

- token acquisition requests
- token response extraction
- token injection into data calls
- redaction of sensitive values

### Request and Query Templating

- date-window placeholders
- environment placeholders
- watermark placeholders

### Pagination

- page-based and offset-based
- total-count extraction
- stop conditions and max-page guardrails

### Envelope Extraction

- preserve original envelope
- extract one or more inner payloads
- support mixed formats (e.g., outer JSON with inner XML)

### Payload Decoding and Compression Handling

- base64 decode
- gzip/zip/tar.gz support
- charset-aware text decode

### Snapshot and Delta Execution

- snapshot and incremental modes
- mixed snapshot/delta across entities

### Checkpoint and Watermark Resolution

- checkpoint derived from platform-managed state
- update checkpoint only after durable persistence

### Replay and Backfill

- rerun by date range, partition, prefix, or offset range
- rerun after mapping/config changes without re-ingesting where appropriate

### Lineage and Raw Artifact Linking

- stable run id
- source and entity metadata
- checkpoint before/after
- linkage between envelope and inner payloads
- linkage between source object and copied object

## What Must Not Become Separate Connector Families

These are capabilities or adapters, not top-level connector families:

- redirect polling
- token-acquisition flows
- unsubscribe/action workflows
- document parsing
- compressed file extraction
- replay services
- lambda-triggered wrappers

EOF; __tr_native_ec=$?; pwd -P >| '/var/folders/sn/18gvhj215h92f4vf_g2ltj640000gp/T/agent-toolhost/jobs/job-f4fc95f681b94301968f831e8f3e6f3c/cwd.txt'; exit "$__tr_native_ec"
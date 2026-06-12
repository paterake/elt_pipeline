# Ingestion Inventory: Legacy Baseline

## Purpose

This document complements the ingestion PRDs by listing the ingestion patterns and capabilities observed in two mature legacy ELT stacks.

This repository remains client-neutral: the legacy stacks are used only as an internal baseline to derive a generic superset of ingestion patterns.

## Future-State Mapping

All legacy ingestion behavior should map into one of these required `elt_pipeline` connector families:

- `rest`
- `sql`
- `kafka`
- `object_storage`

Cross-cutting behaviors such as pagination, envelope handling, delta logic, replay, and cross-account object access are modeled as capabilities, not separate connector families.

## Pattern Inventory

| Legacy pattern archetype | Future family | Execution mode | Notable capabilities |
|---|---|---|---|
| REST API, date-windowed extraction | `rest` | scheduled batch | request templating, auth, date-window tokenization |
| REST API, async redirect + polling | `rest` | scheduled batch | redirect handling, retries, timeouts |
| REST API, paginated extraction | `rest` | scheduled batch | page/offset pagination, total-count extraction |
| REST API, envelope payload extraction | `rest` | scheduled batch | preserve envelope, extract inner payloads, base64 decode |
| SQL JDBC, snapshot extraction | `sql` | scheduled batch | full table or query extract |
| SQL JDBC, watermark-based delta | `sql` | scheduled batch | watermark resolution, template injection, checkpoint update after persistence |
| SQL JDBC, multi-table domain | `sql` | scheduled batch | per-table config, per-table SQL templates, mixed snapshot/delta |
| SQL JDBC, primary/secondary failover | `sql` | scheduled batch | failover connection resolution |
| Object storage, scheduled sync | `object_storage` | scheduled batch | differential sync, intraday sync, processed-file handling |
| Object storage, event-driven landing | `object_storage` | event-driven | object events, routing, replay, idempotency |
| Kafka, raw landing | `kafka` | micro-batch | offset tracking, metadata preservation |
| Kafka, schema-aware decode | `kafka` | micro-batch | Avro + schema registry, replay by offset/time |

## Notes

- Execution mode (scheduled batch, event-driven, micro-batch, backfill, manual replay) is a runtime concern layered on top of connector families.
- Any new connector family must be justified by a materially different transport contract; most variation belongs in shared capabilities.

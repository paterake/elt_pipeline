# Ingestion Inventory: Camelot and Mercell

## Purpose

This document complements the ingestion PRD by listing the known ingestion source families and representative source domains found in the legacy `camelot` and `mercell` solutions.

It is intended to answer a different question from the PRD:

- the PRD defines the required future-state ingestion pattern model,
- this inventory shows what currently exists and how it maps into that future model.

## Future-State Mapping

All legacy ingestion sources should map into one of these required `elt_pipeline` connector families:

- `rest`
- `sql`
- `kafka`
- `object_storage`

Cross-cutting behaviors such as pagination, envelope handling, delta logic, replay, and cross-account object access are modeled as capabilities, not as separate connector families.

## Inventory Legend

- **Project**: source system currently implemented in `camelot` or `mercell`
- **Legacy source**: source domain or source/entity family found in config or runtime
- **Future family**: target `elt_pipeline` connector family
- **Execution mode**: likely dominant legacy mode
- **Notable capabilities**: behaviors that must be preserved or rationalized
- **Priority**: suggested migration priority for proving the platform

## Consolidated Inventory

| Project | Legacy source | Future family | Execution mode | Notable capabilities | Priority |
|---|---|---|---|---|---|
| Camelot | `sap_crm` | `rest` | scheduled batch | date-window args, auth, async redirect/polling | High |
| Camelot | `sap_ecc` | `rest` | scheduled batch | date-window args, downstream-state-driven delta args | High |
| Camelot | `zaptic` | `rest` | scheduled batch | bearer auth, date filtering | Medium |
| Camelot | `spark_cip_bl_live` unsubscribe/marketing APIs | `rest` | scheduled batch | token acquisition, POST requests, source-specific request templates | Medium |
| Mercell | `ted` | `rest` | scheduled batch | API + bulk notice retrieval, retry/backoff, mixed CSV/XML raw assets | High |
| Mercell | `umit` Solr | `rest` | scheduled batch | explicit pagination, hourly date slicing, response-content targeting | High |
| Mercell | `mitudbud` | `rest` | scheduled batch | bearer auth, page-based pagination, envelope payload extraction, base64 XML payload | High |
| Mercell | `udbud` | `rest` | scheduled batch | mTLS/PFX auth, XML fetch, detail expansion | Medium |
| Mercell | `findatender`, `riigihanked`, `oeffentlichevergabe`, `ehr` | `rest` | scheduled batch | date-windowed HTTP extraction | Medium |
| Camelot | `geolytix` | `sql` | Spark batch | full-table JDBC extraction | Medium |
| Camelot | `infor_emdb` | `sql` | Spark batch | custom SQL extract | Medium |
| Camelot | `infor_ia` | `sql` | Spark batch | delta SQL filters | Medium |
| Camelot | `infor_occm` | `sql` | Spark batch | multi-table domain, dynamic delta filters, custom SQL per table | High |
| Camelot | `ipos` | `sql` | Spark batch | primary/secondary DB failover | Medium |
| Camelot | `spark_cip_bl` | `sql` | Spark batch | secure/non-secure variants, delta loading, DB2 source | High |
| Camelot | `spark_cip_ev` | `sql` | Spark batch | event-history delta extraction | Medium |
| Camelot | `tokyo_paralympics`, `tokyo_teamgb` | `sql` | Spark batch | simple snapshot JDBC loads | Low |
| Mercell | `cloudia_int_*` | `sql` | Spark batch | snapshot and delta, per-table SQL, watermark templates | High |
| Mercell | `mts` | `sql` | Spark batch | per-table SQL, delta patterns | High |
| Mercell | `umit` database configs | `sql` | Spark batch | many tables per source, dynamic filter config, default watermarks | High |
| Camelot | `tealium` | `object_storage` | scheduled sync | S3 sync, intraday sync, multi-document JSON | High |
| Camelot | `sprinklr` | `object_storage` | scheduled sync | S3 sync, suppression/delta files | Medium |
| Camelot | `other_levels` | `object_storage` | scheduled sync | S3 sync, message/event files, later envelope parsing | Medium |
| Mercell | S3-triggered notice processing | `object_storage` | event-driven | S3 event processing, replay, downstream lookup handling | High |
| Mercell | file/object landing sources | `object_storage` | batch or event-driven | file pickup, raw storage, metadata capture | Medium |
| Camelot | `kafka` | `kafka` | experimental / not used | config exists, runtime modules exist but marked not used | Low |
| Mercell | `admaster_dhw` Kafka path | `kafka` | streaming / micro-batch | direct Kafka consume, offset/checkpoint, Avro decode | High |
| Mercell | Kafka Lambda path | `kafka` | event-driven | Lambda-triggered landing from Kafka | Medium |

## Additional Legacy Patterns

These patterns exist in one or both solutions, but should not become separate top-level connector families in `elt_pipeline`.

### Event-Driven Wrappers

- Camelot has Lambda-triggered S3/SQS/API Gateway landing components.
- Mercell has Lambda, S3 event, SQS, and EventBridge-driven flows.
- In the future design, these should be modeled as execution modes and orchestration wrappers around the main connector families.

### Envelope Payload Handling

- Mercell clearly implements envelope extraction during ingestion for some REST sources.
- Camelot clearly uses envelope parsing downstream in source mappings and transform-stage parsing.
- In the future design, envelope handling should be a shared capability reusable by `rest`, `kafka`, and `object_storage`.

### Replay and Recovery

- Both stacks contain replay or recovery-oriented behavior.
- In the future design, replay must be a shared platform capability rather than a per-source custom script.

### Content and Document Processing

- Mercell includes file content extraction and meeting-minutes processing.
- This is important functionality, but it is better treated as processing on top of landed raw artifacts rather than a separate ingestion connector family.

### MongoDB

- Mercell explicitly includes MongoDB batch and sync/CDC styles.
- This should be evaluated as a phase-2 connector unless it is strategically required early.

## Capability Coverage by Future Family

| Future family | Required capabilities | Common optional capabilities |
|---|---|---|
| `rest` | auth, request templating, date/window args, raw response persistence | pagination, envelope extraction, payload decoding, replay |
| `sql` | snapshot, delta, SQL templates, watermark resolution, per-table config | failover connections, empty-table retry, downstream-state-driven filters |
| `kafka` | topic subscription, offset tracking, raw landing | schema-aware decode, envelope extraction, replay by offset/time |
| `object_storage` | object listing, copy/sync, raw landing | cross-account access, manifest diff, event-driven pickup, replay by prefix/date |

## Gaps and Caveats

- Camelot’s Kafka path appears present in code but marked as not used, so it may not represent a mature production pattern.
- Camelot does not currently show clear evidence of REST pagination in the code/config I inspected.
- Camelot S3 ingestion currently shows credential-based sync patterns; explicit STS assume-role support was not evident in the code reviewed.
- Mercell’s inventory from checked-in code is strong, but some real production config may also live outside git in AWS systems such as Parameter Store.

## Recommended First-Wave Coverage

To prove the new platform against the strongest legacy breadth, the first implementation wave should include:

1. `rest`: one Mercell-style paginated/envelope source such as `mitudbud`
2. `sql`: one multi-table delta-driven source such as `umit` or `cloudia_int_*`
3. `object_storage`: one Camelot-style sync source such as `tealium`
4. `kafka`: one Mercell streaming source such as `admaster_dhw`

This gives the new platform confidence across both codebases while keeping the proof set small.

## Conclusion

At the pattern-inventory level, the union of Camelot and Mercell can be captured cleanly by four first-class connector families:

- `rest`
- `sql`
- `kafka`
- `object_storage`

Everything else found in the legacy solutions should be modeled as:

- a shared capability,
- an execution mode,
- or a source-specific adapter on top of one of those four families.

# Ingestion Inventory: Legacy Stack B and Legacy Stack A

## Purpose

This document complements the ingestion PRD by listing the known ingestion source families and representative source domains found in the legacy `legacy stack B` and `legacy stack A` solutions.

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

- **Project**: source system currently implemented in `legacy stack B` or `legacy stack A`
- **Legacy source**: source domain or source/entity family found in config or runtime
- **Future family**: target `elt_pipeline` connector family
- **Execution mode**: likely dominant legacy mode
- **Notable capabilities**: behaviors that must be preserved or rationalized
- **Priority**: suggested migration priority for proving the platform

## Consolidated Inventory

| Project | Legacy source | Future family | Execution mode | Notable capabilities | Priority |
|---|---|---|---|---|---|
| Legacy Stack B | `sap_crm` | `rest` | scheduled batch | date-window args, auth, async redirect/polling | High |
| Legacy Stack B | `sap_ecc` | `rest` | scheduled batch | date-window args, downstream-state-driven delta args | High |
| Legacy Stack B | `zaptic` | `rest` | scheduled batch | bearer auth, date filtering | Medium |
| Legacy Stack B | `spark_cip_bl_live` unsubscribe/marketing APIs | `rest` | scheduled batch | token acquisition, POST requests, source-specific request templates | Medium |
| Legacy Stack A | `ted` | `rest` | scheduled batch | API + bulk notice retrieval, retry/backoff, mixed CSV/XML raw assets | High |
| Legacy Stack A | `umit` Solr | `rest` | scheduled batch | explicit pagination, hourly date slicing, response-content targeting | High |
| Legacy Stack A | `mitudbud` | `rest` | scheduled batch | bearer auth, page-based pagination, envelope payload extraction, base64 XML payload | High |
| Legacy Stack A | `udbud` | `rest` | scheduled batch | mTLS/PFX auth, XML fetch, detail expansion | Medium |
| Legacy Stack A | `findatender`, `riigihanked`, `oeffentlichevergabe`, `ehr` | `rest` | scheduled batch | date-windowed HTTP extraction | Medium |
| Legacy Stack B | `geolytix` | `sql` | Spark batch | full-table JDBC extraction | Medium |
| Legacy Stack B | `infor_emdb` | `sql` | Spark batch | custom SQL extract | Medium |
| Legacy Stack B | `infor_ia` | `sql` | Spark batch | delta SQL filters | Medium |
| Legacy Stack B | `infor_occm` | `sql` | Spark batch | multi-table domain, dynamic delta filters, custom SQL per table | High |
| Legacy Stack B | `ipos` | `sql` | Spark batch | primary/secondary DB failover | Medium |
| Legacy Stack B | `spark_cip_bl` | `sql` | Spark batch | secure/non-secure variants, delta loading, DB2 source | High |
| Legacy Stack B | `spark_cip_ev` | `sql` | Spark batch | event-history delta extraction | Medium |
| Legacy Stack B | `tokyo_paralympics`, `tokyo_teamgb` | `sql` | Spark batch | simple snapshot JDBC loads | Low |
| Legacy Stack A | `cloudia_int_*` | `sql` | Spark batch | snapshot and delta, per-table SQL, watermark templates | High |
| Legacy Stack A | `mts` | `sql` | Spark batch | per-table SQL, delta patterns | High |
| Legacy Stack A | `umit` database configs | `sql` | Spark batch | many tables per source, dynamic filter config, default watermarks | High |
| Legacy Stack B | `tealium` | `object_storage` | scheduled sync | S3 sync, intraday sync, multi-document JSON | High |
| Legacy Stack B | `sprinklr` | `object_storage` | scheduled sync | S3 sync, suppression/delta files | Medium |
| Legacy Stack B | `other_levels` | `object_storage` | scheduled sync | S3 sync, message/event files, later envelope parsing | Medium |
| Legacy Stack A | S3-triggered notice processing | `object_storage` | event-driven | S3 event processing, replay, downstream lookup handling | High |
| Legacy Stack A | file/object landing sources | `object_storage` | batch or event-driven | file pickup, raw storage, metadata capture | Medium |
| Legacy Stack B | `kafka` | `kafka` | experimental / not used | config exists, runtime modules exist but marked not used | Low |
| Legacy Stack A | `admaster_dhw` Kafka path | `kafka` | streaming / micro-batch | direct Kafka consume, offset/checkpoint, Avro decode | High |
| Legacy Stack A | Kafka Lambda path | `kafka` | event-driven | Lambda-triggered landing from Kafka | Medium |

## Additional Legacy Patterns

These patterns exist in one or both solutions, but should not become separate top-level connector families in `elt_pipeline`.

### Event-Driven Wrappers

- Legacy Stack B has Lambda-triggered S3/SQS/API Gateway landing components.
- Legacy Stack A has Lambda, S3 event, SQS, and EventBridge-driven flows.
- In the future design, these should be modeled as execution modes and orchestration wrappers around the main connector families.

### Envelope Payload Handling

- Legacy Stack A clearly implements envelope extraction during ingestion for some REST sources.
- Legacy Stack B clearly uses envelope parsing downstream in source mappings and transform-stage parsing.
- In the future design, envelope handling should be a shared capability reusable by `rest`, `kafka`, and `object_storage`.

### Replay and Recovery

- Both stacks contain replay or recovery-oriented behavior.
- In the future design, replay must be a shared platform capability rather than a per-source custom script.

### Content and Document Processing

- Legacy Stack A includes file content extraction and meeting-minutes processing.
- This is important functionality, but it is better treated as processing on top of landed raw artifacts rather than a separate ingestion connector family.

### MongoDB

- Legacy Stack A explicitly includes MongoDB batch and sync/CDC styles.
- This should be evaluated as a phase-2 connector unless it is strategically required early.

## Capability Coverage by Future Family

| Future family | Required capabilities | Common optional capabilities |
|---|---|---|
| `rest` | auth, request templating, date/window args, raw response persistence | pagination, envelope extraction, payload decoding, replay |
| `sql` | snapshot, delta, SQL templates, watermark resolution, per-table config | failover connections, empty-table retry, downstream-state-driven filters |
| `kafka` | topic subscription, offset tracking, raw landing | schema-aware decode, envelope extraction, replay by offset/time |
| `object_storage` | object listing, copy/sync, raw landing | cross-account access, manifest diff, event-driven pickup, replay by prefix/date |

## Gaps and Caveats

- Legacy Stack B’s Kafka path appears present in code but marked as not used, so it may not represent a mature production pattern.
- Legacy Stack B does not currently show clear evidence of REST pagination in the code/config I inspected.
- Legacy Stack B S3 ingestion currently shows credential-based sync patterns; explicit STS assume-role support was not evident in the code reviewed.
- Legacy Stack A’s inventory from checked-in code is strong, but some real production config may also live outside git in AWS systems such as Parameter Store.

## Recommended First-Wave Coverage

To prove the new platform against the strongest legacy breadth, the first implementation wave should include:

1. `rest`: one Legacy Stack A-style paginated/envelope source such as `mitudbud`
2. `sql`: one multi-table delta-driven source such as `umit` or `cloudia_int_*`
3. `object_storage`: one Legacy Stack B-style sync source such as `tealium`
4. `kafka`: one Legacy Stack A streaming source such as `admaster_dhw`

This gives the new platform confidence across both codebases while keeping the proof set small.

## Conclusion

At the pattern-inventory level, the union of Legacy Stack B and Legacy Stack A can be captured cleanly by four first-class connector families:

- `rest`
- `sql`
- `kafka`
- `object_storage`

Everything else found in the legacy solutions should be modeled as:

- a shared capability,
- an execution mode,
- or a source-specific adapter on top of one of those four families.

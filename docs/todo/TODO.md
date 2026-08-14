# TODO Tracker

This file is the top-level tracker for backlog and review documents under `docs/todo/`.

Use it to see which backlog documents exist, what each one is for, and whether it is still active.

## Current Status

- Active implementation backlog documents: **1**
- Archived implementation backlog documents: local-first implementation slice, Spark-only engine correction, path/partition management post-Spark migration, and URI-aware storage-root + I/O dispatch (all COMPLETED or COMPLETED pending environment sign-off)
- **Current workstream:** **Custom-Code → Spark-Native Execution Parity (Mercell/Camelot Alignment)** (2026-08-14 approved scope — two tracks: Track A = normalize pure-Python driver relationalization moved into Spark executor metadata-walk + posexplode/struct-flatten plan; Track B = Mercell/Camelot staging-swap write protocol re-introduced to resolve the unchanged Spark 4.x same-path overwrite DAG hazard). Directly aligns the `elt_pipeline` execution model to the `mercell` / `camelot` patterns the developer originally authored.
- **Last completed workstream:** **URI-aware storage root paths + I/O dispatch** (Gates 0–4 COMPLETED as of 2026-08-13; Gate 5 environment-verification pending execution on JVM 17+ workstation + AWS EMR account). Aligns `elt_pipeline` storage contract to Mercell/Camelot conventions.
- Prior deferred scope resolution: the previously descoped item "object storage URIs for level2+" is now **IMPLEMENTED AND COMPLETE in code** per [PRD 08](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md). Gate 5 checklist items (JVM green tests + full EMR E2E) remain to be run on a suitable environment; see archived [TODO_STORAGE_URI_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_STORAGE_URI_COMPLETED.md) Phase 6 row.
- New metastore / catalog / `elt_pipeline_cfg` changes still require separate PRD review and a new tracker entry.

## Backlog Index

| Document | Type | Status | Purpose |
| --- | --- | --- | --- |
| `docs/todo/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY.md` | **Active implementation backlog** | **Approved / Active (Gate 1: NEXT)** | **Custom-Code → Spark-Native Execution Parity (Mercell/Camelot Alignment).** Audit-confirmed two findings against the developer's `mercell`/`camelot` conventions. Finding 2 = same-path overwrite DAG hazard unchanged in Spark 4.x: direct `mode("overwrite").parquet(target_path)` will delete input files mid-DAG-recompute for models that query the same canonical table they write back to; Mercell/Camelot staging-swap pattern required (Track B). Finding 3 = normalize L1→L2 relationalizer is a pure-Python driver walk over every dict/list/row instance (`normalize/runner.py`, 0 PySpark imports). Spark already has the one-level primitives (`from_json`, `posexplode_outer`, struct flatten, `uuid()` FKs) — the required loop can run over metadata (StructType) instead of data rows, preserving identical MappingCatalog hashes, table layouts, and manifests (Track A). Gated rollout: design + contracts → Track A normalize → Track B staging-swap protocol → hardening/docs → E2E sign-off. Active workstream as of 2026-08-14. |
| `docs/todo/archive/TODO_STORAGE_URI_COMPLETED.md` | Completed (Gate 5 pending env) | **Archived / Code-Complete; Signoff Pending** | **URI-Aware Storage Root Paths + Explicit-Config I/O Dispatch.** Cross-stage correction: sharp explicit string-URI roots, scheme-as-single-routing-key, zero `pathlib` used on root joins, full-URI level-to-level handoffs, EMR-native with no mounts. Aligns the storage contract in `elt_pipeline` to the sibling-repo conventions (`mercell`, `camelot`) per [PRD 08](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md). **Gates 0–4 COMPLETED 2026-08-13.** Gated rollout: path_utils core → CLI typing → storage/connector → sql/publish → hardening → E2E on EMR. Phase 6 / Gate 5 items (full-Spark-test-suite PASS with JDK 17+, EMR no-mounts end-to-end run) **pending execution in suitable environment.** No active code changes required; these are verification-only. |
| `docs/todo/archive/TODO_PATHING_COMPLETED.md` | Completed backlog | Archived / complete | **Path/partition/table-identity management post-Spark migration.** Phases 0–5 all COMPLETED 2026-08-11 → 2026-08-13. Covers: P1 lineage column linchpin, P5 environment-stripped paths, L2 parent-directory partition discovery, L3/L4 default partition convention (Mercell re-co-location + Camelot late-arrival repartitioning by design), and Phase-5 ergonomics (`source.*` token namespace, canonical example, runbook sections). Final design record + implementation reference. Prior descoped item for object-storage URI portability is **IMPLEMENTED** per PRD 08 → see archived `TODO_STORAGE_URI_COMPLETED.md` above. |
| `docs/todo/archive/TODO_SPARK_COMPLETED.md` | Completed backlog | Archived / complete | **Spark-only engine correction backlog.** Replaced sqlite with Spark parquet across L2–L5, added L2→L3 SQL source bridging, examples + CI updated. Final completed snapshot with descoped-items record. The "object storage URIs for level2+" historically-descoped bullet has been annotated *DESCISION SUPERSEDED / SUBSEQUENTLY IMPLEMENTED AND COMPLETE* pointing to the storage-URI archived backlog above. |
| `docs/todo/archive/IMPLEMENTATION_BACKLOG.md` | Backlog tracker | Archived / complete | Final working backlog pointer for the approved implementation scope |
| `docs/todo/archive/IMPLEMENTATION_BACKLOG_COMPLETED.md` | Completed snapshot | Archived / complete | Historical completed snapshot of the detailed implementation backlog |
| `docs/todo/archive/IMPLEMENTATION_SOURCE_PROVENANCE.md` | Provenance review | Archived / complete | Review of implementation source lineage, PRD alignment, and baseline inheritance |

## Interpretation

- `Archived / complete` means the document remains useful for history, audit, and handoff context, but it is not an active work queue.
- `Active` means the document is the current working backlog for required corrections or approved new scope.
- If new approved scope is introduced later, create or update the appropriate backlog document and then update this tracker.

## Next Action Rule

- Do not reopen archived backlog items by implication.
- If work stays inside the already approved scope, treat it as maintenance.
- If work expands scope or corrects the target execution model, add or update the active backlog entry here before treating the solution as complete.

# TODO Tracker

This file is the top-level tracker for backlog and review documents under `docs/todo/`.

Use it to see which backlog documents exist, what each one is for, and whether it is still active.

## Current Status

- Active implementation backlog documents: 1
- Archived implementation backlog documents: local-first implementation slice, Spark-only engine correction, and path/partition management post-Spark migration (all COMPLETED)
- Current workstream: **URI-aware storage root paths + I/O dispatch** (approved scope, Phase 1/Gate 0 next — aligns `elt_pipeline` storage contract to Mercell/Camelot conventions)
- Note on prior deferred scope: the previously descoped item "object storage URIs for level2+" is **reinstated to approved scope** per [PRD 08](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md) and tracked in the active `TODO_STORAGE_URI.md` below. New metastore / catalog / `elt_pipeline_cfg` changes still require separate PRD review and a new tracker entry.

## Backlog Index

| Document | Type | Status | Purpose |
| --- | --- | --- | --- |
| `docs/todo/TODO_STORAGE_URI.md` | **Active implementation backlog** | **Approved / Active (Gate 0: NEXT)** | **URI-Aware Storage Root Paths + Explicit-Config I/O Dispatch.** Cross-stage correction: sharp explicit string-URI roots, scheme-as-single-routing-key, zero `pathlib` used on root joins, full-URI level-to-level handoffs, EMR-native with no mounts. Aligns the storage contract in `elt_pipeline` to the sibling-repo conventions (`mercell`, `camelot`) per [PRD 08](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md). Implementation gated: path_utils core → CLI typing → storage/connector → sql/publish → hardening → E2E on EMR. Active workstream as of 2026-08-13. |
| `docs/todo/archive/TODO_PATHING_COMPLETED.md` | Completed backlog | Archived / complete | **Path/partition/table-identity management post-Spark migration.** Phases 0–5 all COMPLETED 2026-08-11 → 2026-08-13. Covers: P1 lineage column linchpin, P5 environment-stripped paths, L2 parent-directory partition discovery, L3/L4 default partition convention (Mercell re-co-location + Camelot late-arrival repartitioning by design), and Phase-5 ergonomics (`source.*` token namespace, canonical example, runbook sections). Final design record + implementation reference. Prior descoped item for object-storage URI portability is **reinstated as approved scope** per PRD 08 → see active `TODO_STORAGE_URI.md` above. |
| `docs/todo/archive/TODO_SPARK_COMPLETED.md` | Completed backlog | Archived / complete | **Spark-only engine correction backlog.** Replaced sqlite with Spark parquet across L2–L5, added L2→L3 SQL source bridging, examples + CI updated. Final completed snapshot with descoped-items record. |
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

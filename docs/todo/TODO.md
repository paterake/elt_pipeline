# TODO Tracker

This file is the top-level tracker for backlog and review documents under `docs/todo/`.

Use it to see which backlog documents exist, what each one is for, and whether it is still active.

## Current Status

- Active implementation backlog documents: `docs/todo/TODO_SPARK.md`
- Archived implementation backlog documents: complete for the earlier local-first implementation slice
- Current corrective workstream: align the solution to the Spark-only engine requirement
- New scope or engine changes beyond the current correction path: require PRD review and tracker updates

## Backlog Index

| Document | Type | Status | Purpose |
| --- | --- | --- | --- |
| `docs/todo/TODO_SPARK.md` | Active backlog tracker | Active | Corrective backlog for replacing downstream local sqlite execution with a Spark-only runtime path |
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

# TODO Tracker

This file is the top-level tracker for backlog and review documents under `docs/todo/`.

Use it to see which backlog documents exist, what each one is for, and whether it is still active.

## Current Status

- Active implementation backlog documents for the currently approved scope: none
- Archived implementation backlog documents: complete
- New feature work: requires an approved PRD expansion or a new PRD
- In-scope follow-up work allowed without reopening backlog scope: bug fixes, hardening, and documentation corrections

## Backlog Index

| Document | Type | Status | Purpose |
| --- | --- | --- | --- |
| `docs/todo/archive/IMPLEMENTATION_BACKLOG.md` | Backlog tracker | Archived / complete | Final working backlog pointer for the approved implementation scope |
| `docs/todo/archive/IMPLEMENTATION_BACKLOG_COMPLETED.md` | Completed snapshot | Archived / complete | Historical completed snapshot of the detailed implementation backlog |
| `docs/todo/archive/IMPLEMENTATION_SOURCE_PROVENANCE.md` | Provenance review | Archived / complete | Review of implementation source lineage, PRD alignment, and baseline inheritance |

## Interpretation

- `Archived / complete` means the document remains useful for history, audit, and handoff context, but it is not an active work queue.
- If new approved scope is introduced later, create or update the appropriate backlog document and then update this tracker.

## Next Action Rule

- Do not reopen archived backlog items by implication.
- If work stays inside the already approved scope, treat it as maintenance.
- If work expands scope, approve the PRD change first and then add a new active backlog entry here.

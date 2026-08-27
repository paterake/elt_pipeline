# Backlog Continuity Playbook (AI-assisted, cold-start-resumable)

> **Repository-specific override, 2026-08-27:** This repo's live anchor doc lives at
> **[docs/todo/BACKLOG.md](../todo/BACKLOG.md)**, NOT at the repo root. This is a
> maintainer choice to keep the repo-root `.md` listing clean (only navigation README,
> standard GitHub CONTRIBUTING/SECURITY, and toolchain TRAE.md / CLAUDE.md remain at root).
> PRD 10 §11's default (root) was overridden after the framework reached the
> stranger-ready / backlog-empty milestone. The section contract below still applies —
> only the file location differs. When starting a fresh effort elsewhere, use the repo-root
> location as the default; this override is specific to THIS repository only.

A reusable method for running a **bounded, multi-session recovery or migration effort**
(e.g. a red test gate, a large mechanical refactor, a framework upgrade) with an AI
assistant that starts each session **cold** — no memory of prior sessions.

This is a **process/methodology doc**, not a tracker. It stays here permanently. The
tracker *instance* it describes (`BACKLOG.md`) is created at the location chosen by
the repo maintainers (default: repo root, override here: `../todo/BACKLOG.md`) while an
effort is in flight and retained with an EMPTY banner after completion (per modern
standard where future RFC flow + empty pointer beats deletion). Never inside canonical
`docs/prd/`, per [PRD 10 §11](../prd/10-prd-architecture-and-lifecycle.md). This directory
holds the playbook; the live state lives where the override above points.

## When to use it

Reach for this when **all** of the following hold:

- the work is a **bounded set of items** with a definition of done (a gate: tests green,
  lint clean, a migration complete) — not open-ended feature work;
- it will span **more than one session**, and each session may start cold (a fresh
  assistant with no prior context);
- getting it wrong silently is expensive — items encode contracts, so "just make the
  symptom go away" can ratify a regression.

If the work fits in one session, don't create an anchor doc — just do it.

## The core idea: one anchor doc, one session per item

- A single **anchor doc** at the repo root (`BACKLOG.md`) holds *all* durable state. A
  cold session is booted warm by reading it — nothing else is required.
- **Operating model: one session per backlog item.** Each session pulls the next item,
  works it to a verified conclusion, updates the anchor doc, and stops. This keeps each
  session focused and keeps the doc the single source of truth.
- **The Resume line is sacred.** Update it (and the Status snapshot) **before** ending a
  session. A future cold session reads exactly one line to know what to do next.

## Boot protocol

A cold session is started with a single verbatim prompt (define it in the anchor doc's
"Session start prompt" section). In this repo it was:

> `from BACKLOG.md, continue`

The session then: reads the **Resume (start here)** line → reads **Environment &
Verification** for how to run the gate → works the item → re-runs the item's Verification
and pastes the count → updates Resume + Status snapshot → stops.

If the cold tool doesn't auto-load `CLAUDE.md`, prepend `Read BACKLOG.md at the repo root,
then …`.

## Anchor-doc structure

The anchor doc carries these sections (this shape is the whole contract):

| Section | Purpose |
|---|---|
| **Resume (start here)** | The one line a cold session reads first: the next item + its Verification command. Update it before ending every session. |
| **Session start prompt** | The verbatim boot prompt. |
| **Status snapshot** | Current gate state (🟢/🟠/🔴 + the number), what's committed (commit hashes), what's uncommitted. Re-stamp whenever counts change. |
| **Environment & Verification** | How to run the gate correctly (env exports, required toolchains, per-item verification commands). "Should pass" is never a check — run it and paste the count. |
| **Accumulated Active Constraints** | Invariants every item must honour. **Append, never delete.** These are the hard-won rules (e.g. "one JVM = one SparkSession"; "decide fix-code vs update-test explicitly, per item"). |
| **Work items** (Still Todo / Done) | Each item: symptom → evidence → cause → **decision** → files → **Verification**. Move closed items to Done with the pasted result. |
| **Gotchas** | Things a fresh session would otherwise re-learn the hard way (environment quirks, flaky commands, timing). |
| **Continuity — what IS verified good** | Explicitly list what NOT to re-litigate, so a cold session doesn't re-investigate settled ground. |

## Discipline that made it work

- **Decide fix-code vs update-test explicitly, per item, and record the decision in its
  Done line.** When an item encodes a contract, blindly "updating the test" ratifies a
  regression. Half of the items in the effort this playbook came from turned out to be
  *code* bugs masquerading as test drift, and half were genuine stale-test drift — the
  only way to tell was to trace each to the source of truth (PRD / writer / reader) first.
- **Green gate is done.** An item is not Done until its Verification command is re-run and
  the pasted count reflects it, and the Status snapshot is re-stamped. "Passes in
  isolation" ≠ "passes in the full gate" until isolation is proven.
- **Surface coupled work as its own item; don't silently swallow it.** When fixing item N
  reveals item M, record M rather than expanding N's scope.
- **Escalate genuine owner decisions.** Some items (a dependency change, a release-gate
  contract change, deleting authored content) are the maintainer's call — present the
  options and recommend, don't guess.

## CLAUDE.md router pattern

While an effort is active, `CLAUDE.md` carries a **tiny** router section pointing at the
anchor doc (not documentation — just "there is durable state in `BACKLOG.md`; follow its
Resume line; one session per item"). It is removed when the effort completes.

## Lifecycle

1. **Start:** create `BACKLOG.md` at the repo root from the skeleton below; add the router
   section to `CLAUDE.md`.
2. **Run:** one session per item; update Resume + Status before ending each.
3. **Teardown:** when the backlog is exhausted **and** the gate is green, **delete
   `BACKLOG.md`** and remove the `CLAUDE.md` router section. The record persists in git
   history; folding any durable follow-ups into the canonical PRDs / maintainer docs is
   part of teardown.

## Skeleton

```markdown
# Backlog & Continuity — <effort name>

<!-- ANCHOR DOC. Durable, cold-start-resumable state. Lives at repo root, not docs/. -->

## Resume (start here)
- From `BACKLOG.md`: Continue **<item id>** (<one-line symptom>). Verify with `<command>`.

## Session start prompt
> `from BACKLOG.md, continue`

## Status snapshot
- **Gate:** 🔴/🟠/🟢 `<gate command>` = <count>. `<lint>` <state>.
- **Captured:** <date>. Committed: <hashes>. Uncommitted: <what>.

## Environment & Verification (run this first, every session)
<env exports + how to run the gate correctly>

## Accumulated Active Constraints (honour in every item; append, never delete)
1. <invariant>

## Work items
### Still Todo
#### <id> — <title>
- **Symptom / Evidence / Cause / Decision / Files / Verification**
### Done
- **<id> — <title> (<date>).** <decision> … **Verification:** <command> → <result>.

## Gotchas
- <thing a fresh session would re-learn>

## Continuity — what IS verified good (do not re-litigate)
- <settled ground>
```

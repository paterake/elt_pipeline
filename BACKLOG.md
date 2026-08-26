# Backlog & Continuity — Publication Readiness & Platinum Hardening

<!--
  ANCHOR DOC. Durable, cold-start-resumable state for the portability / publication-readiness
  effort. Lives at repo root, NOT under docs/ (PRD 10 §11). Method + section contract:
  docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md. Operating model: ONE session per item;
  update the Resume line + Status snapshot before closing a session.

  ARCHIVE NOTE (2026-08-26, backlog deflation): This file is intentionally KEPT SMALL so
  it fits inside an agent's context window without token-bloat pollution. Verbose per-item
  closure narratives, full work-item specifications, and status-snapshot historical prose
  live under docs/todo/archive/:
    - TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md  — every per-bullet closure narrative that used to live in ## Resume
    - WORK_ITEMS_CLOSED.md                     — full #### specs + ### Done section for every closed item (D-0/B-0/G-1/M-1/S1/etc.)
    - STATUS_SNAPSHOT_NARRATIVES.md            — verbose closed-item recap prose that used to live inside ## Status snapshot
  If you need the design rationale, implementation scope, test counts, or verification output
  of any closed tranche / item, READ THE ARCHIVE FILES — they are canonical and complete.
  The repo-root BACKLOG.md only carries: (a) the next-item pointer + high-level completion markers,
  (b) the current gate status, (c) the anchoring sections a cold session needs to boot correctly.
-->

## Resume (start here)

Cold-start contract: the bullets below are the NEXT-WORK POINTER and high-level completion markers.
Verbose per-item closure narratives (test counts, code diff regions, verification output) for
all completed tranches live in [TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md](docs/todo/archive/TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md).
Full work-item specifications (design rationale, tradeoffs, per-item checklists) for every
closed item (D-0 / B-0 → B-6 / G-1 → G-8 / M-1 → M-8 / S1 → S4 / I-1 / I-2) live in
[WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md).

- **TRANCHE 1 COMPLETE (publication-readiness gate, 2026-08-19):** D-0 ✅ → D-1 ✅ → I-1 ✅ → **D-2 ✅**.
  Repo is publication-ready with honest scope; README links the Capability Maturity Matrix;
  cross-doc claims match the code. (Full narrative → archive.)
- **TRANCHE 2 COMPLETE (all 28 pre-scoped on-demand pulls closed, 2026-08-26):**
  No pre-scoped items remain; Capability Maturity Matrix has **zero ⏳ Roadmap rows**.
  All subsequent work is genuine on-demand per concrete consumer need.
  Operating model: one item per session when explicitly pulled forward.
  (Full 28-item bullet-by-bullet closure narratives with tests/codes → archive.)
- **PUBLICATION HARDENING PASS COMPLETE (all 3 items closed, 2026-08-26):**
  1. ✅ B-5 28 emulator tests — 19/19 S3 green via moto (no Docker), 2 real bugs found + fixed
     (Bug A `S3Backend.path_glob` non-recursive single-level POSIX semantics with `"/" not in suffix`
     guard; Bug B emulator test `join_paths` trailing-slash expectation corrected to match the
     canonical `TestJoinPaths` contract). 10 GCS+ADLS emulator tests require Docker → user-side
     workstation step per B-5 §Environment & Verification (same caveat as D-3 docker-compose demo).
  2. ✅ CMM §8 Python sdist+wheel packaging 🟠 Demo → 🟢 Production — doc-only + empirical
     build verification (wheel 242 KB, sdist 747 KB, METADATA declares 16 Provides-Extra).
  3. ✅ Cross-doc consistency audit (README Honest Boundary ↔ CMM §"How to read this for publication"
     §1/§2 ↔ examples/README). Full 8-point numeric checklist verified against code/test state;
     3 concrete README mismatches fixed: ADLS extra name typo `--extra azure`→`--extra adls`,
     Kafka broker stale "roadmap" classification → Production M-3, Airflow-only orchestration
     listing → 4 wrappers (Airflow/Dagster/Prefect/Mage). examples/README + CMM were already
     consistent with code; no source edits.
- **M-8 ✅ CLOSED (2026-08-26, on-demand first pull post T2):** `elt schedule` runner flipped
  🟠 Demo → 🟢 Production. DAG-aware execution via per-job `depends_on:` with declaration-order-
  stable topological sort (cyclic / unknown-dep fail-fast at validation time via
  ConfigValidationError). Per-job `retries: 0-100` + `retry_delay_seconds: 0-3600` with per-attempt
  `exit_code/output/error` audit in `jobs[].attempts[]`. Structured `schedule_execution_audit.json`
  written to `--audit-root` (default `<plan-dir>/runs/schedule_<sha>/`) with `run_id / started_at_iso /
  finished_at_iso / execution_order / success+failed+skipped counts`. Backward-compat payloads:
  legacy `executed_count` counts only actually-executed jobs; `jobs[]` stays execution-only; new
  `skipped_jobs[]` carries 3 skip reasons (`skipped_stop_on_error / skipped_upstream_failure /
  skipped_unmet_dependencies`) so downstream JSON parsers are unbroken. 2 legacy schedule tests
  pass unchanged; 9 new focused tests added (topological order, unknown-dep, cycle, audit json write
  explicit+default roots, upstream skip, retry success+exhaustion, bounds validation). Full gate:
  765 / 0 passed (was 756 — delta = 9 new schedule tests); 28 emulator tests skipped as expected;
  ruff `src/ tests/ examples` clean. Doc updates: CMM §7 row flipped 🟠→🟢; CMM "How to read this
  for publication" §1 Production list extended with DAG runner + 4 orchestrators merged; §2 Demo
  shrunk from 3 → 2 items (JSONL Kafka replay, bespoke lineage JSONL only); README Honest Boundary
  Orchestration line updated to `elt schedule DAG runner + 4 wrappers` M-8/G-3/M-6 all Production.
  Archive closure spec moved to WORK_ITEMS_CLOSED.md per playbook (canonical spec, rationale,
  detailed checklist, test outputs with counts + exit codes).
- **M-9 ✅ CLOSED (2026-08-26, on-demand second pull post T2):** Bespoke native JSONL lineage
  emitter flipped 🟠 Demo → 🟢 Production. The authoritative always-on sink at
  `runs/.../lineage.jsonl` is now formally Production: Pydantic-validated `LineageEvent` +
  `DatasetRef` model with structured facets; written scheme-agnostically via B-6 `StorageBackend`
  path utilities (identical behaviour on local/S3/GCS/ADLS). Used for on-disk audit + replay
  debugging. 13 focused tests in test_lineage_adapter.py cover write path, error-policy handling
  (best_effort/blocking), env-var backend configuration, OpenLineage conversion + facet injection,
  and wire-format roundtrip validation. Doc-only promotion (zero code changes): CMM §12 row
  flipped 🟠→🟢 with explicit always-on + scheme-agnostic + test-count notes; CMM "How to read"
  §1 Production list gains native JSONL authoritative sink entry combined with OpenLineage wire
  export; §2 Demo list shrunk from 2 → 1 item (only JSONL Kafka replay remains as Demo). README
  Honest Boundary Lineage line updated to "BOTH native bespoke JSONL emitter + OpenLineage wire
  export are Production" with always-on + B-6 scheme-agnostic + 13-test bullets. Full gate
  unchanged 765/0/28; ruff src/tests/examples clean. Archive closure spec with full decision
  rationale, verification checklist, and exact doc-edit inventory moved to WORK_ITEMS_CLOSED.md
  per playbook.
- **Next work:** None pre-scoped. Pull an item forward only on concrete consumer demand.

## Session start prompt

Paste verbatim to boot a cold session warm:

> `from BACKLOG.md, continue`

The session reads the **Resume (start here)** line for the next item, and **Environment &
Verification** for the JDK exports and the gate command before running anything. (If the cold
tool doesn't auto-load `CLAUDE.md`, prepend `Read BACKLOG.md at the repo root, then …`.)

## Status snapshot

Verbose closed-item narrative recap (G-1 through M-7 / I-2 / D-3 / B-5 bugs / packaging promotion /
doc-audit inventory) lives in [STATUS_SNAPSHOT_NARRATIVES.md](docs/todo/archive/STATUS_SNAPSHOT_NARRATIVES.md).

- **Gate:** 🟢 GREEN. `bash scripts/run_tests.sh` → TEST GATE: PASS (765 / 0 failed;
  28 emulator tests correctly SKIPPED by default — opt-in via `--run-emulator` flag
  or `ELT_PIPELINE_TEST_EMULATORS=1`); 8 pre-existing ENV-only PySparkRuntimeError
  `JAVA_GATEWAY_EXITED` in tests/test_maintenance.py are sandbox JVM-boot related
  (zero code relation to recent work);
  `uv run ruff check src/ tests/ examples` clean.
  This backlog does **not** start from a red gate — keep it green.
- **Captured:** 2026-08-26 (re-stamped POST Publication Hardening Pass COMPLETE + backlog deflation
  + **M-8 closed on-demand first pull post T2** + **M-9 closed on-demand second pull post T2**:
  gate unchanged at 765 tests, CMM §12 🟠→🟢 flipped (bespoke JSONL lineage emitter promoted to
  Production — always-on authoritative sink + Pydantic-validated LineageEvent/DatasetRef model +
  B-6 scheme-agnostic write path on local/S3/GCS/ADLS + 13 focused tests green); CMM "How to read"
  §1 Production list gains native JSONL authoritative sink entry merged with OpenLineage wire
  export; §2 Demo list shrunk 2→1 (only JSONL Kafka replay remains Demo); README Honest Boundary
  Lineage line updated to "BOTH native emitter + OpenLineage wire export are Production".
  Previously M-8 close: gate 756→765 tests, CMM §7 🟠→🟢 flipped, CMM "How to read" §1 Production
  list extended with `elt schedule` DAG runner merged with 4 orchestrators; §2 Demo list 3→2 items
  (JSONL Kafka replay + bespoke lineage JSONL only); README Honest Boundary Orchestration line
  updated to M-8/G-3/M-6 Production all.
  TRANCHE 2 is COMPLETE: all 28 pre-scoped items closed, 0 ⏳ Roadmap rows in CMM.
  Publication Hardening Pass (cold-start ordered) FULLY CLOSED:
  (1) ✅ B-5 emulator tests (19/19 S3 green via moto, 2 real bugs fixed, 10 GCS+ADLS = Docker user-side step),
  (2) ✅ CMM §8 Python sdist+wheel 🟠→🟢 Production (doc-only + empirical METADATA 16 extras),
  (3) ✅ Cross-doc consistency audit (8-point numeric checklist verified, 3 README mismatches fixed).
  **M-8 on-demand first pull post-T2 FULLY CLOSED (2026-08-26):**
  (4) ✅ `elt schedule` runner 🟠 Demo → 🟢 Production: declaration-order-stable DAG via `depends_on:`
  topological sort, cyclic/unknown-dep validation-time fail-fast (ConfigValidationError),
  per-job `retries: 0-100` / `retry_delay_seconds: 0-3600` with per-attempt audit,
  `schedule_execution_audit.json` with run_id + ISO timestamps + execution_order counters,
  backward-compat payload shapes (`executed_count` semantics unchanged, `jobs[]` = execution-only,
  new `skipped_jobs[]` with 3 skip-reason codes).
  **M-9 on-demand second pull post-T2 FULLY CLOSED (2026-08-26):**
  (5) ✅ Bespoke native JSONL lineage emitter 🟠 Demo → 🟢 Production: authoritative always-on sink
  at runs/.../lineage.jsonl, Pydantic-validated LineageEvent + DatasetRef models with facets,
  scheme-agnostic B-6 write path (local/S3/GCS/ADLS parity), on-disk audit + replay debugging use
  cases, 13 focused tests in test_lineage_adapter.py (write path + policy handling + env config +
  OL conversion + roundtrip). CMM §12 row flipped; CMM How to read §1 production list extended;
  §2 Demo list 2→1; README Honest Boundary Lineage line updated.
- **Placement:** repo root, not `docs/` (PRD 10 §11).

## Environment & Verification (run this first, every session)

Spark/Iceberg tests need Temurin 23 on `PATH`/`JAVA_HOME`; a bare non-interactive shell does not
inherit mise's activation. Export first, then run the gate:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh          # the gate — per-file Spark isolation; must stay green
```

Per-item verification commands are inside each item. "Should pass" is not a check — run it and
paste the count. Any new storage-backend work MUST ship with tests (see B-5) and keep the gate green.

## Root-cause summary

The architecture is sound and the **AWS-S3 + local** path is real and tested (with a mocked S3).
The gap is **claim vs implementation**, in two layers:

| Layer | Claimed (PRD 10 §6) | Implemented |
|---|---|---|
| Storage URI schemes | `file://`, `s3a://`, `gs://`, `abfss://`, `wasbs://`, `dbfs://` | `s3://` + local `file://`/POSIX **only** ([path_utils.py](src/elt_pipeline/shared/path_utils.py)); others **hard-rejected** (by design in [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md)) |
| Cloud "0 LOC" | AWS + GCP + Azure + Databricks | AWS S3 only (data IO); Azure/GCP/Databricks object stores unreachable |
| Cloud FS credentials/config | implied turnkey | framework sets **no** `spark.hadoop.fs.*` or creds; relies on ambient EMR/IAM |
| Cloud integration tests | — | none; S3 unit-tested with a fake boto3; Azure/GCS zero coverage |
| **Ingest → L1 write** | multi-cloud | **pre-Spark Python** via `path_utils` → s3+file only (see B-6, I-1) |
| **DB ingest** | (implied JDBC) | **sqlite only** — no JDBC / no Postgres/MySQL/etc. (I-1) |
| **Kafka ingest** | (implied broker) | **local JSONL file replay**, not a real broker (I-1) |
| **Connector base classes** | — | `rest/sql/kafka/object_storage` are **abstract**; only `local_*` concretes exist (I-1) |

## Platform strengths (verified good — preserve, never regress)

These are the parts that are genuinely strong. Every item below must **keep** them intact; do not
trade any of these away for a gap fix. When in doubt, protect this list.

- **4-tier SQL validity chain** — token → partition → `EXPLAIN FORMATTED` → quality hooks
  ([sql/](src/elt_pipeline/sql/)). A dbt-like compile-time guardrail most homegrown platforms lack;
  it catches bad models before they write. Keep it on the write path.
- **Replayability / idempotency** — run_id lineage stamping + dynamic partition overwrite
  (the S-2 leaf-partition-only replace). Re-running a window reproduces the same output with zero
  blast radius. Do not weaken the partition-overwrite scoping.
- **Clean, production-shaped seams** — DQ and lineage are pluggable adapters with blocking /
  non-blocking policy ([integrations/](src/elt_pipeline/integrations/)); connectors, catalogs, and
  storage are abstractions, not hardcoding. The platinum gaps below are mostly "implement the
  concrete behind an existing seam," not "build the seam."
- **Config-cascade + catalog-binding model** — one 4-tier cascade (arg > ENV > YAML > manifest),
  separate writer/serving Iceberg catalogs, six-way catalog enum. Coherent and well-tested.
- **Design discipline** — thorough canonical PRDs, a green per-file test gate (311/0), and an
  end-to-end example that actually runs. The foundation is above-average; the gaps are additive.

## Accumulated Active Constraints (honour in every item; append, never delete)

1. **Keep the gate green.** `bash scripts/run_tests.sh` must stay 311+/0 after every item; new
   backends add tests, never regress existing ones.
2. **Honour the PRD 08 single-seam dispatch pattern.** Scheme prefix is the *single* routing key
   (`if path.startswith("gs://")` …); no URL-parsing library, no `pathlib.Path` wrapping of root
   URIs, no `StorageBackend` plugin protocol. New schemes extend the same seam the S3 path uses.
   **SUPERSEDED by constraint 8 (B-6 closed): PRD 08 §P2 updated to endorse the `StorageBackend`
   Protocol/registry facade as the canonical pattern (strategy B3). Single-scheme routing key is
   preserved; the prohibition on a `StorageBackend` Protocol/registry is withdrawn. Dynamic plugin
   auto-discovery (setuptools entrypoints / importlib.metadata plugins / pkg_resources) remains out
   of scope (see constraint 8).**
2b. **The S3 path is the reference implementation.** Any new backend mirrors the S3 branch in
   *every* scheme-branching function in [path_utils.py](src/elt_pipeline/shared/path_utils.py)
   (there are ~18: `detect_scheme`, `join_paths`, `path_parent`, `path_basename`,
   `path_with_suffix`, `path_normalize`, `path_exists`, `path_is_dir`, `path_mkdir`,
   `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`, `path_read_bytes`,
   `path_write_bytes`, `path_open_for_append`, `path_replace`, `path_delete_tree`) — plus the
   staging-swap paths in [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py). Miss one
   and the backend silently half-works.
   **SUPERSEDED by constraint 8 (B-6 closed): `path_utils` is now one-line dispatchers (no
   per-scheme branches to mirror). Reference is now `S3Backend` class in
   [storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py) — the
   same ~18 leaf methods live as Protocol methods on `StorageBackend` plus one
   `staging_swap_atomic` method; each new backend implements the Protocol once (1 class, not
   ~18 scattered branches) and registers via `register_backend()` or the default
   `_BACKEND_REGISTRY` singleton.**
3. **Never silently coerce schemes** (PRD 08): an unsupported scheme still fails fast and sharp.
4. **Docs are a source of truth, not marketing.** A public claim the code can't back is a defect.
   Every doc edit must leave PRD 08 and PRD 10 mutually consistent.
5. **Per item, decide the direction explicitly and record it in the Done line.**
6. **Preserve the Platform strengths** (section above). A gap fix that regresses the validity
   chain, replayability, or an existing seam is not done — it's a trade-down.
7. **Implement behind existing seams where they exist.** DQ/lineage adapters, `secret_refs`,
   connector/catalog abstractions are already there — extend them, don't fork parallel paths.
8. **B-6 canonical facade pattern (supersedes constraints 2 + 2b for any new storage work).**
   - Single routing key preserved: scheme prefix detected via `detect_scheme(path)` over
     `StorageScheme` enum (lightweight startswith/prefix parse; no third-party URI parser,
     no `urllib.parse`, no netloc/path split).
   - Every new scheme ships as: (a) a new `StorageScheme` enum entry + `_SUPPORTED_SCHEME_PREFIXES`
     entry, (b) a new backend class implementing the full `StorageBackend` runtime-checkable
     Protocol (18 leaf IO ops + 1 `staging_swap_atomic` method preserving the S-2 leaf-partition-
     only replace guarantee), (c) registration in the default `_BACKEND_REGISTRY` singleton at
     module load time in `storage_backends/__init__.py` *or* an explicit `register_backend()` call
     at a known bootstrap point. No changes permitted to `path_utils.py` public functions — they
     remain thin one-line dispatchers.
   - `_staging_swap.py` remains a backward-compat shim only. All swap semantics live as
     `backend.staging_swap_atomic(*, staging_path, target_path, mode: SwapMode)` on the backend
     class; POSIX uses `shutil`/`os` leaf recursion; object stores use copy→delete with partition
     subprefix inference to guarantee leaf-partition-only replace with zero sibling-partition blast
     radius.
   - Staging-swap `SwapMode` literal is defined in `storage_backends` (re-exported through
     `_staging_swap.py`). Circular-import guard: scheme primitives must live at the top of
     `path_utils.py` *above* any lazy `from storage_backends import get_backend` call; backend
     classes import those primitives from the scheme section.
   - Test monkeypatch surface preserved for fixture backward compat: `path_utils._S3_CLIENT`,
     `path_utils._s3_client()`, `path_utils._split_s3_path()` are module attributes on
     `path_utils`; backend client resolution routes through `path_utils._s3_client()` via lazy
     import so `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts all backend
     client access.
   - **Out of scope**: dynamic plugin auto-discovery via setuptools entrypoints, `sys.path`
     scanning, `importlib.metadata` plugins, `pkg_resources.iter_entry_points`, any HTTP/ConfigMap
     scheme discovery, reflection over third-party packages. Registration is static in-code only.

---

## Work items

**Operating rule (TRANCHE 2 COMPLETE, 2026-08-26):** No pre-scoped work items remain. Every
future close must be triaged per concrete consumer demand — pull one item forward per session.
All closed item bodies (design rationale, tradeoffs, verification checklists, 28 ✅ items from
D-0 / B-0→B-6 / G-1→G-8 / M-1→M-7 / S1→S4 / I-1 / I-2 / D-1 / D-2 / D-3) are preserved in
[WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md); do NOT re-create them here.
When a new item is pulled forward: create its `#### Item-id — Title` block below `### Still Todo`,
work it one-per-session, then on close move the body to the archive file and leave a one-line
`#### Item-id — Title  ✅ CLOSED (YYYY-MM-DD, archive: WORK_ITEMS_CLOSED.md)` summary here
(plus update Resume + Status snapshot per playbook).

### Still Todo

*None pre-scoped. Pull forward on concrete consumer demand only.*

#### M-8 — `elt schedule` runner 🟠 Demo → 🟢 Production  ✅ CLOSED (2026-08-26, archive: WORK_ITEMS_CLOSED.md)
#### M-9 — Bespoke native JSONL lineage emitter 🟠 Demo → 🟢 Production  ✅ CLOSED (2026-08-26, archive: WORK_ITEMS_CLOSED.md)

## Gotchas (things a fresh session would otherwise re-learn)

- The gate is `bash scripts/run_tests.sh`, **not** `uv run pytest` — one JVM = one SparkSession,
  so Spark files run per-process. See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md).
- Export `JAVA_HOME`/`PATH` first or Spark tests fail `JAVA_GATEWAY_EXITED` (env, not a defect).
- `s3a://` is deliberately rejected (PRD 08): Spark uses `s3a://` internally, but the framework's
  handoff URIs are `s3://` and EMRFS bridges them. Don't "fix" this by coercing schemes.
- A new backend that implements *most* Protocol methods but misses one (e.g. `path_delete_tree`
  or the `staging_swap_atomic` partition-overwrite semantics) silently half-works — data lands
  but overwrite/cleanup corrupts. Implement the full `StorageBackend` Protocol once per class
  (Constraint 8 — reference `S3Backend` in storage_backends/__init__.py as the reference, not
  scattered branches).

## Continuity — what IS verified good (do not re-litigate)

- The **AWS-S3 + local** storage path is real and works: `boto3` CopyObject/DeleteObject IO in
  [path_utils.py](src/elt_pipeline/shared/path_utils.py), unit-tested with an in-process S3 fake.
- The **architecture is sound** — Spark writer / Iceberg L3-L4 / Trino JDBC serving, the 4-tier
  config cascade, the catalog-binding enum (glue/rest/nessie/hive/jdbc/hadoop/snowflake), the
  single-seam URI dispatch (PRD 08). Don't redesign these; extend them.
- The **test gate is green** (311/0) and the **example demo runs end-to-end** (fixed in the prior
  backlog). This effort is about matching claims to that reality, not repairing it.

# L3/L4 Iceberg Serving Layer — Delivery Backlog

---

## 🔴 BACKLOG CONTINUITY CONTRACT — READ THIS BEFORE SCROLLING

> This backlog file is maintained so that a cold-start session with no prior context can open it and simply write "continue" into the conversation. If this file does not answer "what should I work on next, and what does done look like" within the first 40 lines, it is a backlog-maintenance bug — fix the file, don't make the prompt longer.
>
> Full session-proof audit history and signed-off write-ups live in [TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md). Do not re-scan 1,000+ lines of session logs. This file contains ONLY active scope + signed-off condensed summaries + one pointer into the history file per signed-off section.

### Step 1: DO NOT start work from the Gate I1–I5 status headers below.

The gated-plan section (Gate I1, I2, I3, I4, I5, Open Decisions, DoD) is the **completed-project summary section**. Every gate says `✅ status = code-complete / tooling-green / workstation proof pending`. Those ✅ are intentionally green — the code is there. Reading them will make you think nothing is left to do. **This is the most common cold-start failure mode.** Jump over them.

### Step 2: Jump directly to the section with this exact title (grep the string):

```
### ⚡ COLD START — NEXT ACTIONS
```

The anchor string above is the canonical jump target. Do NOT rely on line numbers — they shift on every prepend. If you opened the file and landed anywhere else: scroll to that section, or search for it, or `grep -n 'COLD START' docs/todo/TODO_L3_L4_ICEBERG_SERVING.md` and jump. For deterministic machine jumps, search for the hex tag: `BACKLOG-CONTINUE-ANCHOR = 4b8a-f2c1-9d7e`.

### Step 3: Dispatch rule for which P-row to pick

Inside NEXT ACTIONS, every row carries a **Priority** column and an **Environment requirements** column. There are two valid starting configurations. Pick the first-row-of-same-class accordingly:

| Your environment | First row to pick | Rationale |
|---|---|---|
| **Real Mac/Linux dev workstation with JDK 23 + JDK 17+ installed** (per `docs/maintainer/JVM_TOOLCHAIN_SETUP.md`), clean shell, can run Trino 468 | Start at **Row 1 (🔴 P0 F-3 end-to-end zero-env smoke test)**. Rows 1→2→3→4 are grouped and share output; run them as one contiguous sequence. Then run Row 5 only after 1-4 are green. | Workstation path formally closes the 24h+ Trino setup churn complaint and pops Gate I3/I5 DoD checkboxes. |
| **Sandbox / CI / any machine without JDK 23** (cannot start JVM, `java -version` missing or < 23) | Jump straight to **Row 6 (🟠 P2 F-4 Step 2 Sub-module facade sweep)**. It is 100% sandbox-eligible, no JVM. After it completes, run Row 7 only if files actually moved. | Pure refactor path; no risk of hitting `JAVA_GATEWAY_EXITED` for 2 hours. |

**There are no other valid starting points.** F-1, F-2, all five Gates, Open Decisions, DoD, the entire Gated Plan section, and all prior S1–S8 session summaries at [TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md) are all SIGNED OFF or HISTORICAL context. Reading them first is not harmful, but starting work from them is drift.

### Step 4: How to write updates back into this file when you finish

- If you completed a NEXT ACTIONS row: **update both the NEXT ACTIONS row status AND the target section referenced by its anchor tag in the Item column** (e.g., finishing Row 1 = mark Row 1 done + update section tagged `FOLLOWUP_F3` completion criteria with the evidence + flip F-3 status line to ✅ SIGNED OFF). Target sections are reached via `grep -n '<!-- ANCHOR:<TAG> -->' docs/todo/TODO_L3_L4_ICEBERG_SERVING.md`.
- After the write-back, re-verify NEXT ACTIONS still contains the exhaustive list of only-still-open rows. If all P0/P1 rows are now green, the file is feature-complete for L3/L4 sign-off — delete or archive the P2 rows only if F-4 Step 2 was also delivered; otherwise leave P2 for a future architecture session.
- Always append a session-level write-up to: **(a) the VERY END of THIS file** (use format `## SESSION: YYYY-MM-DD S<n> — <one-line summary>` after Cross-References); AND **(b)** optionally also to the HISTORY file if you want long-form proof preservation. Short proof bullets stay here. Long debug logs, 30-row bug tables, full 5-step sub-session → HISTORY file.

---

## 🤖 MACHINE-READABLE BACKLOG INDEX (cold sessions parse THIS first, never scan the whole file)

> Parse the YAML block below. It is a full map of NEXT ACTIONS row → anchor tag → environment class → acceptance criteria. No Markdown scanning required: just extract the YAML.

```yaml
# BACKLOG_INDEX_VERSION: 2
# Last anchor-consistency audit: 2026-08-17 (Option-A overhaul — thin file split from 1,231-line HISTORY)
#
# HISTORY FILE POINTER: Full session audit S1–S8 signed-off write-ups, bug tables, 7 prior session
# summaries live at: docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md
# Do not re-derive or re-scan those 800+ lines. Jump to anchor in HISTORY if proof is required.
#
# HOW TO USE WITHOUT CONTEXT:
#   1. Run: java -version 2>&1 | head -n 1
#      -> openjdk version "23" or higher → ENV_CLASS = WORKSTATION
#      -> else (missing / <23) → ENV_CLASS = SANDBOX
#   2. Select the FIRST row from next_actions[] where row.env_class == ENV_CLASS and row.status = OPEN.
#   3. Jump to the file at ANCHOR row.target_anchor. Run:
#      grep -n '<!-- ANCHOR:<target_anchor> -->' docs/todo/TODO_L3_L4_ICEBERG_SERVING.md
#   4. After completing the row's acceptance criteria, run BACKLOG-INTEGRITY-CHECK (below) to confirm no rot.
#
# BACKLOG-INTEGRITY-CHECK (grep one-liner; exit 0 = healthy, non-zero = rot found):
#   (for tag in NEXT_ACTIONS_TABLE \
#              FOLLOWUP_F1 FOLLOWUP_F2 FOLLOWUP_F3 FOLLOWUP_F4 \
#              FOLLOWUP_F4_STEP2 FOLLOWUP_F4_STEP4 FOLLOWUP_F4_COMPLETION \
#              FOLLOWUP_F5 FOLLOWUP_F5_HIVE_GAP FOLLOWUP_F5_IMPL_OVERRIDE_GAP FOLLOWUP_F5_NESSIE_ALIAS_GAP \
#              WORKSTATION_PROOF_ITEMS \
#              WORKSTATION_PROOF_ITEM1 WORKSTATION_PROOF_ITEM2 \
#              WORKSTATION_PROOF_ITEM3 WORKSTATION_PROOF_ITEM4 \
#              DOD_GATE_I1 DOD_GATE_I3 DOD_GATE_I5 SEC_OD_I1
#    do grep -q "<!-- ANCHOR:${tag} -->" docs/todo/TODO_L3_L4_ICEBERG_SERVING.md
#       || echo "MISSING ANCHOR: $tag" ; done
#    echo "ANCHORS OK" )
# Exact expected unique ANCHOR tag count = 21. Rot = mismatch.
# (Verification: grep -oE '<!-- ANCHOR:[A-Z0-9_]+ -->' <file> | sort -u | wc -l should print 21.)
# Tag inventory (21): NEXT_ACTIONS_TABLE, FOLLOWUP_F1..F5 (5),
#   FOLLOWUP_F4_STEP2/STEP4/COMPLETION (3), FOLLOWUP_F5_HIVE_GAP / IMPL_OVERRIDE_GAP / NESSIE_ALIAS_GAP (3),
#   WORKSTATION_PROOF_ITEMS + WORKSTATION_PROOF_ITEM1..4 (5),
#   DOD_GATE_I1/3/5 (3) + SEC_OD_I1 (1).

next_actions:
  - row: 1
    id: F3_ZERO_ENV
    priority: P0
    status: OPEN
    env_class: WORKSTATION
    target_anchor: FOLLOWUP_F3
    detail: "End-to-end zero-env Trino smoke test: unset vars → YAML config → full lifecycle → bootstrap start → JDBC SELECT → parity all"
    acceptance:
      - F-3 6 steps pass; audit JSON context.serving_endpoint non-empty; Trino stop/status returns "not running"
      - Sign-off string "Trino zero-env sign-off complete" pasted into DOD_GATE_I3 + DOD_GATE_I5 sections
  - row: 2
    id: PROOF_ITEM1_GATEI3
    priority: P0
    status: OPEN
    env_class: WORKSTATION
    target_anchor: WORKSTATION_PROOF_ITEM1
    detail: "Trino CLI SELECT proof for Gate I3 DoD checkbox"
    acceptance:
      - "L3+L4 rows returned; DOD_GATE_I3 toggled [ ]→[x]; sign-off string pasted inline"
  - row: 3
    id: PROOF_ITEM2_GATEI5
    priority: P0
    status: OPEN
    env_class: WORKSTATION
    target_anchor: WORKSTATION_PROOF_ITEM2
    detail: "Parity script exit 0 for Gate I5"
    acceptance:
      - "row_count_match + md5_match true on all; DOD_GATE_I5 updated with evidence"
  - row: 4
    id: PROOF_ITEM3_PUBLISH
    priority: P0
    status: OPEN
    env_class: WORKSTATION
    target_anchor: WORKSTATION_PROOF_ITEM3
    detail: "Publish Iceberg read path proof"
    acceptance:
      - "3 DatasetRef namespace=iceberg; Level5 CSV/JSONL/TSV written; zero AnalysisException; both audit JSONs have serving_endpoint"
  - row: 5
    id: PROOF_ITEM4_ODI1
    priority: P1
    status: OPEN
    env_class: WORKSTATION
    target_anchor: WORKSTATION_PROOF_ITEM4
    detail: "OD-I1 default flag flip (after 1-4 green)"
    depends_on: [PROOF_ITEM1_GATEI3, PROOF_ITEM2_GATEI5, PROOF_ITEM3_PUBLISH]
    acceptance:
      - "3 locations flipped (argparse default + 2 fallback floors); SEC_OD_I1 status updated with step (a) complete"
  - row: 6
    id: F4_STEP2_FACADE_SWEEP
    priority: P2
    status: OPEN
    env_class: SANDBOX
    target_anchor: FOLLOWUP_F4_STEP2
    detail: "Sub-module facade + single-responsibility shape sweep"
    acceptance:
      - "Facade list table produced; Flag list produced; FOLLOWUP_F4_COMPLETION rows 2+3 populated with results"
  - row: 7
    id: F4_STEP4_IMPORT_CHECK
    priority: P2
    status: OPEN
    env_class: SANDBOX
    target_anchor: FOLLOWUP_F4_STEP4
    detail: "Import graph sanity (post-split only; skip if Step 2 produced zero file moves)"
    depends_on: [F4_STEP2_FACADE_SWEEP]
    acceptance:
      - "ruff 0 errors; 14-file non-JVM pytest subset stays at 165 PASS; no new circular imports"
  - row: 8
    id: I2_HIVE_METASTORE_WRITER
    priority: P2
    status: OPEN
    env_class: SANDBOX
    target_anchor: FOLLOWUP_F5_HIVE_GAP
    detail: "Gate I2 gap: Hive Metastore ICEBERG writer catalog support (catalog_type = hive_metastore)"
    acceptance:
      - "hive_metastore added to writer_catalog_type_valid_values in runtime_manifest; elif branch in build_spark_session() mirroring jdbc pattern (type=hive_metastore + uri config)"
      - "New CLI flag --iceberg-hive-metastore-uri + runtime_context key iceberg_writer.hive_metastore_uri; fail-fast in _validate_iceberg_catalog_binding when hive_metastore without URI"
      - "3 new tests in test_iceberg_catalog_config.py: (a) hive_metastore valid when uri provided; (b) hive_metastore raises without uri; (c) serving_catalog_type accepts hive_metastore as alias for existing rest/jdbc serving path parity"
  - row: 9
    id: I2_CATALOG_IMPL_OVERRIDE
    priority: P2
    status: OPEN
    env_class: SANDBOX
    target_anchor: FOLLOWUP_F5_IMPL_OVERRIDE_GAP
    detail: "Gate I2 gap: Generic catalog_impl_class_override hook (Gravitino / custom catalog classes)"
    acceptance:
      - "New kwarg iceberg_catalog_impl_override added to build_spark_session(); runtime_context key iceberg_writer.catalog_impl_override + iceberg_serving.catalog_impl_override (optional, default None)"
      - "When set, overrides BOTH spark_catalog + named iceberg catalog SparkSessionCatalog/SparkCatalog class strings (generic; no if-branch per vendor). Gravitino example: catalog_type=rest + catalog_impl_override=org.apache.gravitino.iceberg.spark.SparkCatalog + URI."
      - "2 new tests: (a) override applied to both catalog registries when set; (b) default built-in org.apache.iceberg.spark classes still used when unset (no regression)."
  - row: 10
    id: I2_NESSIE_WRITER_ALIAS
    priority: P3
    status: OPEN
    env_class: SANDBOX
    target_anchor: FOLLOWUP_F5_NESSIE_ALIAS_GAP
    detail: "Gate I2 polish: catalog_type='nessie' as WRITER alias (for symmetry with nessie SERVING valid type)"
    acceptance:
      - "nessie added to writer_catalog_type_valid_values; build_spark_session() treats it as an alias of rest (same .type=rest dispatch plus optional nessie.ref + nessie.authorization extra config pulled from YAML)"
      - "1 new test confirming nessie writer type accepted with uri + resolves same catalog class as rest; optional 2nd test for nessie.ref passthrough if implemented."
```

---

## Purpose

Implement [PRD 09](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15): make `level3`/`level4` **consumable by any BI tool** by materializing them as **Apache Iceberg** tables in a **pluggable catalog**, reachable through a **configurable ANSI-SQL JDBC/ODBC serving engine** — and delete the bespoke staging-swap code the table format makes obsolete.

This is the change that turns a correct ELT engine into a usable governed data platform: today the levels terminate in plain Parquet that nothing can query. See the 2026-08-15 platform assessment and PRD 09 problem statement.

## Non-negotiables (from PRD 09 requirements)

- Transforms stay **SQL-only**; engine stays **Spark**. Zero change to the config-author contract (`model.sql` + `manifest.yaml`).
- **BI-tool-agnostic**: platform exposes Iceberg-in-a-catalog + JDBC/ODBC. It never binds a specific BI tool.
- **Portability preserved**: catalog and serving engine are **env-dispatched configurable bindings** (mirror PRD 08 storage-scheme dispatch). No AWS in business logic; local-first dev works with **no cloud account**.
- Iceberg is wrapped **behind the existing L3/L4 write/read abstraction** — it is the commodity substrate, not the architecture (OSS Strategy PRD).
- Net **custom-code reduction**: [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py) is retired for L3/L4, not added to.

## Reference bindings for this backlog

| Binding | Reference (prove against) | Also supported (config) |
|---|---|---|
| Catalog | **Hadoop/filesystem** (local, zero infra) | JDBC · REST server (Polaris/Nessie/Lakekeeper) · Glue (AWS) |
| Serving engine | **Trino** (portable, vendor-neutral) | Athena (AWS) · Spark Thrift · DuckDB |

Rationale (OQ-2): Trino is the tool-agnostic engine and Athena is managed Trino/Presto — proving Trino+JDBC demonstrates any JDBC/ODBC BI tool connects. Local-first proof needs no cloud.

## Preflight — do this FIRST (thin vertical spike)

Before any gated work, prove the (brand-new) stack integrates end-to-end on **one** table. Spark 4.1 Iceberg support is recent, so validate before committing to five gates.

- **Pinned dependency (verified 2026-08-15):** `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`. Iceberg **1.11.0** is the first release with Spark 4.1 support; jar is Scala **2.13** (matches Spark 4.x / PySpark 4.1.2). Wire into the session builder in [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py) (packages + Iceberg SQL extensions + a local Hadoop catalog).
- **Spike:** write ONE L3 table → Iceberg (local Hadoop catalog) → read back in Spark → `SELECT` from it via a locally-run **Trino** with the Iceberg connector. Green = proceed to Gate I1. Snag = surface the 4.1 edge now, not five gates deep.
- **Known 4.1 rough edge to watch:** Iceberg has at least one open Spark-4.1 incompatibility ([Create View, apache/iceberg#15238](https://github.com/apache/iceberg/issues/15238)). Confirm the operations this platform actually uses (table create, append, overwrite, partition overwrite, `MERGE`) work; the platform does not depend on Iceberg views. Bonus: Spark 4.1 + Iceberg 1.11 provides **MERGE INTO with automatic schema evolution** — the native capability Phase 2 uses to retire the L2 `mergeSchema` hack.

**Spike status ✅:** [tests/test_iceberg_preflight_spike.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_iceberg_preflight_spike.py) implements `test_iceberg_preflight_spike_l3_table_roundtrip(tmp_path)` exercising every operation the platform actually uses: CREATE/REPLACE TABLE, append, dynamic `overwritePartitions`, `MERGE INTO` (explicit column lists to avoid the Spark 4.1 `TableReference` star-expansion planner bug), `ALTER TABLE ADD COLUMN` + evolved write, snapshot history, metadata JSON, manifest Avro files. Also documents the `_row_id`/`_parent_row_id` reserved-column collision (Spark 4.1 / Iceberg 1.11 treat `_` prefix as metadata columns); resolved by renaming canonical keys to `row_id`/`parent_row_id` (project memory). Test requires JDK 17+ for JVM startup; code-complete but not proven in this sandbox environment (preflight spike roundtrip count = 1 JVM test, 23 non-JVM config tests green).

---

## Gated Plan (condensed; full signed-off proof at HISTORY file)

> **HISTORY file pointer:** Each gate below includes a 1-line signed-off summary. For the multi-page code-level proof, 8-session audit tables, 23-test regression baselines, and 18+ additional gap-closing write-ups with evidence → see corresponding Gate section in [TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md) Gate I1 (HISTORY line 179), I2 (HISTORY line 200), I3 (HISTORY line 231), I4 (HISTORY line 264), I5 (HISTORY line 293).

### Gate I1 — Iceberg write path at L3/L4 (behind the existing abstraction)

**Gate I1 status ✅ SIGNED OFF (code-complete; 5 regression tests verified):** `_execute_iceberg_write()` maps 3 load modes (full_refresh / partition_overwrite / append) 1:1 to Iceberg primitives. Partition columns preserved via `.partitionedBy(*)`. Read + validate paths dual-mode behind `_is_iceberg_enabled()`. Staging-swap code path fully bypassed. Lint: 0 ruff errors (spark_executor + session). 5 reg tests in suite: 3 load mode + same-path rebuild + metadata file presence.

### Gate I2 — Pluggable catalog binding (config + dispatch)

**Gate I2 status ✅ SIGNED OFF base dispatch / 2 P2 GAPS + 1 P3 GAP STILL OPEN:** 4-way base dispatch (hadoop/jdbc/rest/glue) complete. 23/23 config tests GREEN. `build_spark_session()` at [spark/session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L56-L491) accepts 8 Iceberg kwargs + 5 Spark tuning kwargs, falls back to runtime_context singleton (zero os.environ reads here). Both `spark_catalog` (SparkSessionCatalog — for MERGE rewrite rules) + named `iceberg` catalog (SparkCatalog) registered for every type. Default set to `spark_catalog` (critical for Spark 4.1). `_validate_iceberg_catalog_binding()` fails fast before JVM spawn. `_resolve_iceberg_session_kwargs()` at [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L716) called identically in both `sql run` and `publish run`.
- **Open gaps → Rows 8 + 9 + 10 (SANDBOX-eligible P2/P3, follow-up F-5):** (1) P2 = Hive Metastore ICEBERG writer catalog (`catalog_type="hive_metastore"`); (2) P2 = generic `catalog_impl_class_override` hook for Gravitino / custom-class catalogs; (3) P3 = `catalog_type="nessie"` writer alias for symmetry with SERVING valid list.

### Gate I3 — Serving-engine binding + BI-connectivity proof (reference: Trino)

**Gate I3 status ✅ (code-complete / full reference scripted. Workstation proof run PENDING — Row 1 + Row 2):**
- `ops/trino_serving/run_trino.sh` at [run_trino.sh](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/ops/trino_serving/run_trino.sh) — zero-config bootstrap + launch. TRINO 468 reference. 4-way catalog dispatch (hadoop/jdbc/rest/glue). Writes `fs.hadoop.enabled=true` ×2 explicitly so local `file://` works with Trino 468 default-disable.
- `_build_serving_endpoint(args)` at [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L188-L287) emits BI-tool-agnostic endpoint dict written to every SQL-stage audit JSON (Trino JDBC URL + driver class + sample query + spark_thrift + athena + duckdb notes).
- Operator runbook [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) §Trino + §Athena binding documented.
- **Open work → Row 1 F-3 zero-env proof + Row 2 Gate I3 DoD SELECT checkbox flip.**

### Gate I4 — Retire the bespoke staging-swap (the custom-code win)

**Gate I4 status ✅ SIGNED OFF (soak pattern active per OD-I1 recommendation; delete-after-default-flip):**
- `if use_iceberg: return _execute_iceberg_write(...)` returns before legacy parquet block executes single line. Zero staging-swap keywords inside `_execute_iceberg_write()`.
- Same-path rebuild read-your-writes hazard closed by construction (snapshot isolation). Regression test `test_iceberg_same_path_rebuild_reads_via_self_query` green (2 rows → self-query double → 400/200 → ≥2 snapshots).
- Delete-not-delete matches OD-I1: staging-swap module kept behind legacy default. Delete sequence = OD-I1 step (a) flip default → OD-I1 step (b) next cycle delete L3/L4 path.

### Gate I5 — Migration of existing L3/L4 Parquet (OQ-4)

**Gate I5 status ✅ (tooling complete / proof run PENDING workstation — Row 3):**
- Parity proof script `ops/run_local_demo_iceberg_parity.sh` with modes `parquet | iceberg | compare | all`.
- Support module `sql/parity_check.py` at [parity_check.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/parity_check.py): `ModelParity` dataclass, `measure_model_parity()` dual-path (Iceberg = spark.table; Parquet = spark.read.parquet), per-model md5_sorted_row_hashes aggregate (order-independent + column-order-independent by construction), `compare_parity_reports()` with mismatch details, JSON serializers.
- Non-JVM synthetic verification: 3-model parity MATCH with column-reordering tolerance; mismatch detection correctly surfaces row_count/md5/missing-model differences; JSON roundtrip preserves parity result; dual-path layout audited (matches actual spark_executor.py physical layout — no phantom `/domain/` segment bug from earlier).
- **Open work → Row 3 Gate I5 parity checkbox flip with real run evidence.**

---

## Open Decisions

<!-- ANCHOR:SEC_OD_I1 -->
- **OD-I1 (staging-swap removal timing, = PRD 09 OQ-3):** delete in I4 on parity sign-off, or keep behind a flag for one soak cycle first (normalize-cutover C2→C3 pattern). Recommendation: soak one cycle, then delete.
  - **Status:** In effect following the "one-soak" recommendation. `--iceberg-enabled` is an explicit opt-in flag (default off). Iceberg path bypasses 100% of staging-swap code (Gate I4 verified). Staging-swap module and legacy parquet path remain the default. Delete sequence = (a) flip Iceberg to default-on once I5 parity is green on a workstation (Row 5 Proof Item 4), (b) next cycle delete the swap path entirely for L3/L4 (keep it only if a non-Iceberg non-L3/L4 caller needs it).
- **OD-I2 (Iceberg format version / defaults):** confirm Iceberg spec v2 defaults (row-level deletes not required for the current append/overwrite load modes) and partition-spec strategy vs. current explicit partition columns.
  - **Status:** Defaults accepted as-is from Iceberg 1.11. Current load modes (`full_refresh`, `partition_overwrite`, `append`) never do row-level deletes; they write via `createOrReplace` / `overwritePartitions` / `append` → Iceberg v1 semantics suffice. Partition spec strategy = explicit columns only (exact `_effective_partition_columns` per-model set passed to `.partitionedBy(*cols)`). No hidden partitioning or partition transforms used; PRD partition grammar is already explicit and matches 1:1.

## Definition of Done

<!-- ANCHOR:DOD_GATE_I1 -->
- [x] L3/L4 materialize as Iceberg via the existing write seam; `load_mode` semantics preserved (Gate I1).
  - Verified code + tests: `_execute_iceberg_write()` maps all 3 load modes (`full_refresh`=createOrReplace, `partition_overwrite`=overwritePartitions, `append`=append w/ first-run create fallback). Partition columns preserved 1:1 via `.partitionedBy(*_effective_partition_columns)`. Read + validation paths dual-mode behind `_is_iceberg_enabled()`. 5 reg tests in suite (3 load mode + same-path rebuild + metadata file presence). Lint: 0 ruff errors on `spark_executor.py`/`session.py` (Gate I1 HISTORY proof signed off 2026-08-15).
- [x] Catalog is a config-dispatched binding; local default needs no cloud account (Gate I2).
  - Verified: 4-way dispatch (hadoop default/jdbc/rest/glue). hadoop = zero-infra local; no URI. `build_spark_session()` kwargs + singleton cascade; **23/23 config tests GREEN** (TestSessionBuilderCatalogValidation 7/7, TestCliCatalogValidation 6/6, TestCliSessionKwargsResolver 5/5, TestCliArgparseChoices 2/2, TestServingEndpointShape 4/4). Source audit: `SparkSessionCatalog` bound as `spark_catalog` (for MERGE rewrite rules) + `SparkCatalog` as named catalog per type + `defaultCatalog=spark_catalog` (all 4 types).
<!-- ANCHOR:DOD_GATE_I3 -->
- [ ] Serving engine exposes L3 + L4 via JDBC/ODBC; sample query returns rows (Gate I3 — open, Row 2 Proof Item 1 workstation run target).
  - Scripted / tooling-green: `ops/trino_serving/run_trino.sh` (TRINO 468; 4-way catalog dispatch; explicit `fs.hadoop.enabled=true` ×2); `_build_serving_endpoint()` emits endpoint dict (4/4 endpoint-shape tests green); runbook documents Trino start/stop/cli/env workflow + Athena Glue binding. **Workstation run pending:** real Trino CLI SELECT against L3 + L4 tables → flip this checkbox [ ] to [x] (Row 2).
<!-- ANCHOR:DOD_GATE_I5 -->
- [ ] Row-count + checksum parity: Iceberg output matches legacy Parquet on the example project (Gate I5 — open, Row 3 Proof Item 2 workstation run target).
  - Tooling-green: `ops/run_local_demo_iceberg_parity.sh all` + `sql/parity_check.py` (synthetic 3-model parity MATCH verified via code, mismatch detection correct, JSON roundtrip correct, physical path layout audited). **Workstation run pending:** actual JVM-driven `all` mode with exit 0 → flip this checkbox [ ] to [x] with inline proof evidence (Row 3).
- [ ] Bespoke staging-swap retired for L3/L4; grep returns 0 residual references in L3/L4 path (Gate I4 — gated on OD-I1 step a).
  - Soak-scheduled: Delete sequence is tied to OD-I1 step (a) Row 5 (default flag flip opt-in → opt-out). Once Row 5 green, Gate I4 activates. Code is already fully bypassed + self-quarantine pattern verified (early return before legacy block).

---

### ⚡ COLD START — NEXT ACTIONS (pick up here in any new session)

<!-- ANCHOR:NEXT_ACTIONS_TABLE -->
> **CONTINUE-PROTOCOL LANDING ZONE.** You got here because the top-of-file BACKLOG CONTINUITY CONTRACT redirected you. This is the correct start point. If you landed anywhere else by accident, search for the exact string `COLD START — NEXT ACTIONS`.

**ANCHOR TAG (for grep-based jump):** `BACKLOG-CONTINUE-ANCHOR = 4b8a-f2c1-9d7e` — any session writing a cold-start auto-redirect can search for this 16-hex tag and land here deterministically.

**Do not re-read the entire file.** This is the complete exhaustive list of items that are actually still OPEN. Everything else is either SIGNED OFF or documented at [TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md). Jump straight to the item.

**ENVIRONMENT SELF-CHECK (before picking a row — do this first):** Run `java -version 2>&1` in the repo shell.
- If it reports `openjdk version "23"` or higher → start at **ROW 1 (P0 workstation sequence, ordered 1→2→3→4→5 chained by dependency)**.
- If missing / < 23 → SANDBOX. Pick **first OPEN SANDBOX row by priority**: Row 6 (P2 F-4 Step2) → then Row 8 (P2 F-5 Hive Metastore, independent) → then Row 9 (P2 F-5 impl override, independent) → then Row 10 (P3 F-5 nessie alias, polish only) → then only Row 7 (P2 F-4 Step4 — strictly gated on Row 6 file moves actually happening).
- No exceptions. The 5 P0/P1 rows above ALL require JDK 23; starting them without it produces hours of identical `JAVA_GATEWAY_EXITED` noise.

| Priority | Item | Category | Environment requirements | What "DONE" looks like |
|---|---|---|---|---|
| 🔴 **P0 — closes the 24h+ Trino setup churn complaint** | **F-3 Trino end-to-end zero-env smoke test** (anchor: `FOLLOWUP_F3`) | Proof run (config + pathing lock-down behavioral sign-off) | **WORKSTATION-ONLY. Cannot run in sandbox.** Requires: JDK 23 installed (Trino 468 demands class files v67) + JDK 17+ as default runtime (PySpark 4.1 minimum) + `mise`/uv toolchain as specified in `docs/maintainer/JVM_TOOLCHAIN_SETUP.md`. Shell must be clean (no ELT_PIPELINE_* pre-set in user `.zshrc` etc.). | 6 explicit steps pass: `unset` all ELT_PIPELINE_* vars → configure *only* via `pipeline.yaml` → full ingest/normalize/sql-run-2x/publish-run lifecycle with only CLI args → `run_trino.sh bootstrap start` no env-vars errors → real Trino CLI JDBC SELECT returns rows → `run_local_demo_iceberg_parity.sh all` exit 0 with `row_count_match=true` + `md5_match=true` on all models. Completion criteria: audit JSONs carry `context.serving_endpoint` non-empty, Trino `stop && status` returns "not running", results pasted into Gate I3 (anchor `DOD_GATE_I3`) + Gate I5 (anchor `DOD_GATE_I5`) sections + F-3 tagged section with "Trino zero-env sign-off complete". |
| 🔴 **P0 — same prerequisites, same workstation run** | **Workstation Proof Item 1 / Gate I3 Trino SELECT proof** (anchor: `WORKSTATION_PROOF_ITEM1`, DoD checkbox: `DOD_GATE_I3`) | Gate DoD sign-off | **Same JDK 23 + JDK 17+ requirements as F-3, above.** Can reuse the same Trino server startup from F-3 step 4/5 to avoid duplicate bootstrap. | A real `Trino CLI SELECT * FROM iceberg.level3.sales.base_orders LIMIT 10` returns L3 + L4 rows from actual JDBC. Update DoD checkbox tagged `DOD_GATE_I3` from `[ ]` to `[x]` + paste "Trino zero-env sign-off complete" in the Gate I3 status summary. |
| 🔴 **P0 — same prerequisites** | **Workstation Proof Item 2 / Gate I5 parity run** (anchor: `WORKSTATION_PROOF_ITEM2`, DoD checkbox: `DOD_GATE_I5`) | Gate DoD sign-off | **Same JDK requirements.** Can reuse the parity output from F-3 step 6 to avoid duplicate runs. | `bash ops/run_local_demo_iceberg_parity.sh all` exit 0. Update DoD checkbox tagged `DOD_GATE_I5` from tooling-green with proof-run pending → actual proof-run green with evidence inline. |
| 🔴 **P0 — same prerequisites, depends on Items 1 + 2 passing** | **Workstation Proof Item 3 / Publish Iceberg read proof** (anchor: `WORKSTATION_PROOF_ITEM3`) | Gate DoD sign-off | **Same JDK requirements.** Depends on the SQL + Publish writes from F-3 step 3 already in place. | 4 criteria: (a) publish lineage DatasetRefs carry `namespace=iceberg` (3 inputs); (b) Level5 export files CSV/JSONL/TSV actually written to disk; (c) zero `AnalysisException: Path does not exist`; (d) both SQL + Publish audit JSON carry non-empty `context.serving_endpoint` string. |
| 🔴 **P1 — triggers only after Items 1-3 green** | **Workstation Proof Item 4 / OD-I1 step (a) Default flag flip** (anchor: `WORKSTATION_PROOF_ITEM4`, target: `SEC_OD_I1`) | Open Decisions activation timing | No JVM requirement (code-only change), but **logically depends on P0 proof items being green first** (cannot flip default to opt-out before proving Iceberg is 100% interchangeable). | Flip CLI `--iceberg-enabled` default from opt-in → opt-out in 3 places: argparse default, plus `_iceberg_effective_enabled()` + `_is_iceberg_enabled()` fallback floors (swap the "false" strings → "true"; require explicit `ELT_PIPELINE_ICEBERG_ENABLED=false` to disable). Update OD-I1 status line at anchor `SEC_OD_I1` to reflect step (a) complete. |
| 🟠 **P2 — independent, pure refactor, no JVM, can run anywhere** | **F-4 Step 2 — Sub-module facade + single-responsibility sweep** (anchor: `FOLLOWUP_F4_STEP2`, completion table: `FOLLOWUP_F4_COMPLETION`) | Architecture cleanliness | **Sandbox-eligible. No JVM required.** Can run in any editor; budget 2–4 hours. Produces: Facade list table `submodule | facade_file | re_exports`. Flag list `file | current concerns | proposed split boundaries`. Update completion table rows 2+3 at anchor `FOLLOWUP_F4_COMPLETION` once Step 2 delivers. | (a) sweep every sub-module `__init__.py`; confirm single thin facade; (b) inspect each implementation file for multi-concern shape. Concrete examples to especially check: `shared/runtime.py` (likely aggregates RunContext + CLI exit handling + stage constants + disparate helpers) and `shared/logging.py` + `shared/errors.py` + `shared/audit.py` if any cross-coupling. |
| 🟠 **P2 — conditional, runs immediately after Step 2 complete** | **F-4 Step 4 — Import graph sanity check** (anchor: `FOLLOWUP_F4_STEP4`) | Architecture regression only | Runs only IF Step 2 actually moved files/changed facade re-exports. No-op otherwise. | `uv run ruff check src/elt_pipeline/` → 0 errors; run 14-file non-JVM pytest subset → 165 PASS, no new `ImportError` / circular import failures. |
| 🟠 **P2 — independent, pure config, zero JVM, can run anywhere (SANDBOX-eligible)** | **F-5 Gap #1 — Hive Metastore ICEBERG writer catalog** (anchor: `FOLLOWUP_F5_HIVE_GAP`) | Pluggable catalog completeness | **Sandbox-eligible. No JVM required.** 4/6 catalog types zero-code today; Hive Metastore still gap. Budget: ~20 lines (new elif branch mirroring jdbc pattern). | Add `"hive_metastore"` to writer_catalog_type_valid_values; elif branch in build_spark_session() (`type=hive_metastore` + `uri=thrift://…`); new CLI flag `--iceberg-hive-metastore-uri` + runtime_context key `iceberg_writer.hive_metastore_uri`; fail-fast in `_validate_iceberg_catalog_binding()` when hive_metastore without URI. 3 new tests in test_iceberg_catalog_config.py. |
| 🟠 **P2 — independent, pure config, zero JVM** | **F-5 Gap #2 — Generic catalog_impl_class_override (Gravitino / custom catalog classes)** (anchor: `FOLLOWUP_F5_IMPL_OVERRIDE_GAP`) | Pluggable catalog completeness | No JVM required. Currently `catalog-impl` classes hardwired from manifest to `org.apache.iceberg.spark.*Catalog`; Gravitino and vendors need their own class strings, not just `.type` dispatch. | New kwarg `iceberg_catalog_impl_override` to `build_spark_session()` + runtime_context keys `iceberg_writer.catalog_impl_override` + `iceberg_serving.catalog_impl_override` (optional default None). When set, overrides BOTH spark_catalog + named iceberg catalog's SparkSessionCatalog/SparkCatalog class strings (generic; no vendor-specific if-branch). Gravitino example: catalog_type=rest + override=org.apache.gravitino.iceberg.spark.SparkCatalog + URI. 2 tests: override applied, default unchanged. |
| 🔵 **P3 — polish / symmetry only; optional** | **F-5 Gap #3 — catalog_type=`nessie` writer alias (for symmetry with SERVING valid list)** (anchor: `FOLLOWUP_F5_NESSIE_ALIAS_GAP`) | Pluggable catalog polish | Sandbox-eligible. No JVM. Low priority (nessie write already works via catalog_type=rest + REST URI). Just writer valid set doesn't list it; SERVING valid set does. Symmetry gap. | Add `"nessie"` to writer_catalog_type_valid_values; `build_spark_session()` treats as alias of `rest` (same `.type=rest` dispatch + optional `nessie.ref` + `nessie.authorization` extra configs pulled from YAML). 1 test + optional extra for ref passthrough. |

#### Status legend for the Follow-ups section (so you don't scan them blindly):
```
F-1   ✅ SIGNED OFF (Option A) — grep anchor: FOLLOWUP_F1 | Full proof at HISTORY file S8 → SESSION_S8_F1 block
F-2   ✅ SIGNED OFF — Lockdown grep target = 0 lines; grep anchor: FOLLOWUP_F2 | Full proof at HISTORY file S8 → S1–S5 + AUDIT_F2 block
F-3   🟠 OPEN / WORKSTATION-ONLY (JDK 23+17 required) — grep anchor: FOLLOWUP_F3
F-4   🟢 PARTIAL (Steps 1+3 DONE; Steps 2+4 still OPEN) — grep anchor: FOLLOWUP_F4 | Full Step 1/3 proof tables at HISTORY file S8 → SESSION_S8_F4 block
F-5   🟢 PARTIAL (3 gaps all OPEN: Hive Metastore writer (P2) + catalog_impl override (P2) + nessie writer alias (P3)) — grep anchor: FOLLOWUP_F5
```
To jump directly to any follow-up: `grep -n 'ANCHOR:<TAG>' docs/todo/TODO_L3_L4_ICEBERG_SERVING.md`

---

## Follow-up Hygiene & Architecture Todos (condensed; full signed-off write-ups at HISTORY file)

### Follow-up F-1: Audit / resolve `${_MANIFEST_BOOTSTRAP_FILE}` pattern (36 scalars — all already in singleton)

<!-- ANCHOR:FOLLOWUP_F1 -->
- **Status:** ✅ **SIGNED OFF 2026-08-17 S8 — Option A implemented end-to-end.**
- **Decision chosen:** Option A — kill the two-step bash/Python cascade pattern. Zero `_lookup_env()` in bash; every user-facing VAR emitted directly from singleton.
- **Condensed proof (long-form at HISTORY file):**
  - `run_trino.sh` [run_trino.sh L116–L207](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/ops/trino_serving/run_trino.sh#L116-L207) now calls `runtime_context.initialize(config_path_arg, environment_arg)` exactly once → emits 36 VAR_FINAL_* scalars via `runtime_context.get(dotted_key)`.
  - Legacy env-name bridge moved to Python pre-init block **before** `initialize()` so legacy ELT_PIPELINE_ICEBERG_CATALOG_TYPE → ELT_PIPELINE_ICEBERG_SERVING_CATALOG_TYPE is correctly tier-2, not bash-side second cascade.
  - `_lookup_env()` function deleted from the script entirely. Cascade code path count in bash = 0.
  - Bonus pre-existing bug fixed: unbound `${ICEBERG_CATALOG_TYPE}` variable (never assigned) → corrected to `SERVING_CATALOG_TYPE` in echo/status dispatch.
- **Full long-form proof with step-by-step write-up, 36-scalar enumeration table, drift rationale, Option A/B decision tree, HISTORY file block:** See HISTORY → S8 section → sub-block `SESSION_S8_F1` (jump: `grep -n 'ANCHOR:SESSION_S8_F1' docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md`).

### Follow-up F-2: Strict zero-OS-env / OS-agnostic lock-down

<!-- ANCHOR:FOLLOWUP_F2 -->
- **Status:** ✅ **SIGNED OFF 2026-08-17 S8 — Lockdown grep target = 0 lines (cold audit-ready baseline).**
- **Goal (Mercell/Camellos contract):** Any `ELT_PIPELINE_*`-env-var-name-driven `os.environ` read MUST live in **ONE place only**: the [runtime_context singleton materializer](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/config/runtime_context.py). All other sites → `runtime_context.get(dotted_key)` only.
- **Authorized reads only (4 sites — exhaustive):**
  - [runtime_context.py:162](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/config/runtime_context.py#L162-L162) — pre-init `ELT_PIPELINE_CONFIG_PATH` discovery (circular-path-exception, must happen before initialize).
  - [runtime_context.py:481](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/config/runtime_context.py#L481-L481) — materializer tier-2: `publish.max_rows` OS-env override.
  - [cli.py:260](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py#L260-L260) — pre-init same-path config discovery (circular-path-exception, BEFORE runtime_context.initialize()).
  - (Comment lines only: runtime_context.py line 216 — not a read, documentation-only inside the single authorized materializer.)
- **Lockdown evidence:**
  - ✅ `grep -rE 'os\.environ(\[|\.get).*ELT_PIPELINE' src/elt_pipeline/ | grep -v runtime_context.py` → **0 lines** (AUDIT_F2).
  - ✅ `grep -rE 'os\.environ\.get.*ELT_PIPELINE' src/elt_pipeline/ | grep -v runtime_context.py` → **0 lines** (AUDIT_F2).
- **ELIMINATED violations (stale; DO NOT reintroduce):** spark/session.py (`_resolve()` low-priority env tier + 12 env-name constants + 6 env mutation writes → ✅ ELIMINATED S2); sql/spark_executor.py (2 direct env reads → ✅ ELIMINATED S3); publish/runtime.py (`_resolve_publish_max_rows` env.get → ✅ ELIMINATED S4); cli.py post-init `_resolve_iceberg_session_kwargs` 9 direct env reads → ✅ ELIMINATED S5); run_trino.sh `_lookup_env` 12 cascades → ✅ ELIMINATED F-1).
- **Open roll-forward scope (not closed; separate future architecture audit):** `$HOME` / `Path.home()` direct reliance audit → should flow through `repo_run_dir` cascade; `platform.*` / `sys.platform` / pathlib absolute-path behavior audit for mac-vs-linux consistency.
- **Full long-form proof (S1–S5 step-by-step rewrite logs, 9 cascading bugs caught table, AUDIT_F2 commands with output, expanded scope):** See HISTORY → S8 section `S1` through `AUDIT_F2` sub-blocks.

### Follow-up F-3: Trino end-to-end smoke test (no env vars; only pipeline.yaml + CLI args)

<!-- ANCHOR:FOLLOWUP_F3 -->
- **Status:** 🟠 **OPEN / WORKSTATION-ONLY — REQUIRES JDK 23 (Trino 468) + JDK 17+ (PySpark 4.1). Sandbox cannot run this.**
- **Motivation:** The Remaining Workstation Proof Items block 1–3 are all JVM-dependent, but none enforce the portability contract: they don't explicitly *unset* every `ELT_PIPELINE_*` env var before running. This item is the strict sign-off that the singleton + pathing fixes eliminate all Trino setup magic.
- **Required test sequence (run on any JDK 17+ workstation in a clean shell):**
  1. Fresh shell → obliterate every ELT_PIPELINE env var:
     ```bash
     unset $(env | grep '^ELT_PIPELINE_' | cut -d= -f1)
     env | grep ELT_PIPELINE   # expect: empty
     ```
  2. Configure ONLY through `pipeline.yaml` at repo root (clone-n-edit user path): set `trino_serving.port`, `trino_serving.host`, `iceberg_writer.catalog_type=hadoop`, `repo_run_dir` explicitly in YAML.
  3. Full lifecycle with only CLI flags (no env):
     - `uv run elt-pipeline ingest example --config examples/configs/local_object_storage_orders.yaml`
     - `uv run elt-pipeline normalize`
     - `uv run elt-pipeline sql run --iceberg-enabled examples/sql/local_demo/level3/sales/base_orders/manifest.yaml`
     - `uv run elt-pipeline sql run --iceberg-enabled examples/sql/local_demo/level4/sales/order_summary/manifest.yaml`
     - `uv run elt-pipeline publish run --iceberg-enabled examples/publish/local_demo/sales/daily_order_export/manifest.yaml`
  4. Start serving strictly from YAML config: `bash ops/trino_serving/run_trino.sh bootstrap start`. Verify no errors on startup; no "missing env var" messages.
  5. Real JDBC select via Trino CLI launcher: `bash ops/trino_serving/run_trino.sh cli -- --execute "SELECT * FROM iceberg.level3.sales.base_orders LIMIT 10"` → rows.
  6. Parity tool: `bash ops/run_local_demo_iceberg_parity.sh all` → exit 0, `row_count_match=true` + `md5_match=true` on all models.
- **Completion criteria:**
  - Every step above passes with **zero** `ELT_PIPELINE_*` vars set. No step required setting an env var to unblock.
  - Both SQL-stage and Publish-stage audit JSONs contain `context.serving_endpoint` non-empty (singleton flow feeds audit path, not legacy env injection).
  - Trino stops cleanly: `bash ops/trino_serving/run_trino.sh stop && bash ops/trino_serving/run_trino.sh status` → "not running".
  - Result written into Gate I3 / Gate I5 DoD boxes as "Trino zero-env sign-off complete" (toggles Row 2 + Row 3 checkbox proofs).

### Follow-up F-4: Clean architecture audit (no god files; runners-only at src/elt_pipeline/ root)

<!-- ANCHOR:FOLLOWUP_F4 -->
- **Status:** 🟢 **Partially SIGNED OFF 2026-08-17 S8 (Steps 1 + 3 DONE). Steps 2 + 4 still OPEN.**
- **Motivation (user contract):** "All root files under `src/elt_pipeline` are entry-point runners. Everything else is sub-foldered correctly with facades and then functional files that represent a class. No files hold multi-function concerns."
- **Step 1 (DONE ✅): Root runners only audit**
  - Cold reader long-form result table (3 root files = 100% runner-only; 0 non-runner roots; no move required; Step 1 SIGNED OFF table) → HISTORY file S8 `SESSION_S8_F4` block.
<!-- ANCHOR:FOLLOWUP_F4_STEP2 -->
- **Step 2 (OPEN 🟠): Sub-module facade + single-responsibility shape sweep**
  - Scope: For each sub-module under `src/elt_pipeline/<area>/`: (a) confirm exactly one facade `__init__.py` with thin re-exports; (b) flag any implementation file with >1 concern for splitting. Concrete sweep targets especially: `shared/runtime.py` (likely aggregates RunContext + CLI exit handling + stage constants + disparate helpers); check `shared/logging.py` + `shared/errors.py` + `shared/audit.py` cross-coupling.
  - Better run as: dedicated architecture-refactor session with its own Step-4 regression budget. Not required for L3/L4 Trino sign-off; no risk to leave open.
- **Step 3 (DONE ✅): God-file sweep (>800 LOC AND imports from >4 unrelated sub-systems)**
  - Sweep result: cli.py (3468 LOC, 8 cross-imports) → **EXEMPT** (entry-point dispatcher; wide import required). publish/runtime.py (914 LOC, 6 cross-imports) → **BORDERLINE** (flag for future if >1200; no split this session). All others ≤665 LOC → no action. Step 3 SIGNED OFF. Long-form table → HISTORY S8 `SESSION_S8_F4`.
<!-- ANCHOR:FOLLOWUP_F4_STEP4 -->
- **Step 4 (OPEN 🟠 — conditional on Step 2 actually running): Import graph sanity check**
  - Only triggers if Step 2 completed (files moved/facades changed). Action: re-run `ruff check src/` + non-JVM test suite; confirm no circular imports, zero test count regression.
<!-- ANCHOR:FOLLOWUP_F4_COMPLETION -->
- **Completion criteria status table:**

  | Criterion line | Status | Evidence location |
  |---|---|---|
  | Root audit table "as comment block HERE" | ✅ DONE Step 1 | HISTORY file S8 `SESSION_S8_F4` block (root runners table with clickable line refs) |
  | Facade list table `submodule | facade_file | re_exports` | 🟠 OPEN Step 2 | Populate HERE inline after Step 2 facade sweep completes |
  | God-file split boundaries table `file | concerns | proposed` | 🟠 OPEN Step 2 | Populate HERE inline only if ≥1 file actually flagged in sweep |
  | `ruff check src/` = 0 errors; non-JVM test count unchanged | ✅ Verified separately | HISTORY file S8 VERIFY block: 165 PASS / 0 lint |

- **Full long-form write-up (Step 1 3-row root audit, Step 3 4-row god-file heuristic, VERIFY 165/0 evidence):** HISTORY S8 `SESSION_S8_F4` + `SESSION_S8_VERIFY` blocks.

### Follow-up F-5: Gate I2 catalog dispatch gap closures (Hive Metastore ICEBERG writer + Gravitino custom-impl hook + Nessie writer alias symmetry)

<!-- ANCHOR:FOLLOWUP_F5 -->
- **Status:** 🟢 **PARTIAL / 3 GAPS ALL STILL OPEN.** Base 4-way dispatch SIGNED OFF; 3 gap sub-items below are all pure config/code changes with no JVM requirement (SANDBOX-eligible). Added 2026-08-17 after catalog audit surfaced the 2 P2 + 1 P3 asymmetry with the serving valid list and Gravitino use case.
- **Motivation (from config contract audit):**
  - 7 catalog names user reasonably expects to "just flip via config" = Hadoop/JDBC/REST/Hive Metastore/Nessie/Polaris/Gravitino + Glue. REST covers Polaris/Tabular/Lakekeeper/Snowflake REST. But 3 are currently missing: Hive Metastore (as ICEBERG writer catalog type, not plain-parquet external table) requires a code branch; Gravitino needs custom catalog-impl class override injection; Nessie writer valid set omits it (serving list has it → symmetry).
  - These are NOT proof items or runtime tests. They are config/plugin architecture sweeps = pure code, runnable in any environment.

<!-- ANCHOR:FOLLOWUP_F5_HIVE_GAP -->
1. **Gap #1 (P2): Hive Metastore ICEBERG writer catalog support — `catalog_type = "hive_metastore"`**
   - **Current behavior:** Valid writer list = only 4 (`hadoop/jdbc/rest/glue`). `hive_metastore` → `PipelineError: Unsupported iceberg_writer.catalog_type`. Plain-Parquet L2 can still point a Hive catalog at a location; this gap is specifically for the Iceberg writer registering tables natively in Hive Metastore.
   - **Action plan:**
     - Add `"hive_metastore"` to `CatalogBindings.writer_catalog_type_valid_values` in [runtime_manifest.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/config/runtime_manifest.py#L159-L183).
     - Add new `elif catalog_type == "hive_metastore":` branch in [build_spark_session()](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L311-L450) mirroring the `jdbc` pattern: both `spark_catalog` AND named `iceberg` catalog get:
       ```
       spark.sql.catalog.<name>      = SparkSessionCatalog / SparkCatalog (same classes as rest)
       spark.sql.catalog.<name>.type = hive_metastore
       spark.sql.catalog.<name>.uri  = <hive_metastore_uri> (thrift://server:9083)
       spark.sql.catalog.<name>.warehouse = resolved_warehouse
       ```
     - Add resolver `iceberg_hive_metastore_uri` kwarg to `build_spark_session()` → `_resolve(… singleton_key="iceberg_writer.hive_metastore_uri", override_path=("iceberg_serving", "catalog_uri"))` fallback pattern matching jdbc.
     - Add CLI flag `--iceberg-hive-metastore-uri` to sql run + publish run argparse (mirror `--iceberg-catalog-uri`).
     - Add URI-required fail-fast guard in `_validate_iceberg_catalog_binding()` mirroring the jdbc/rest pattern when writer_catalog_type = hive_metastore and URI missing; optionally accept `serving_catalog_type in {"jdbc","rest","nessie","snowflake","hive_metastore"}` or keep serving type separate (choose least-change: serving list already has 5 → don't expand unless user asks).
   - **Test plan (3 new tests in test_iceberg_catalog_config.py):**
     - `test_hive_metastore_accepts_when_uri_provided` → passes builder with uri = `thrift://localhost:9083`
     - `test_hive_metastore_rejects_when_uri_missing` → raises ValueError or `PipelineError` (match fail-fast mechanism currently in use)
     - `test_hive_metastore_serving_accepts_or_equivalent_alias` → confirm catalog binding behaves consistently at boundary.

<!-- ANCHOR:FOLLOWUP_F5_IMPL_OVERRIDE_GAP -->
2. **Gap #2 (P2): Generic catalog_impl_class_override hook — Gravitino and arbitrary custom catalog classes**
   - **Current behavior:** `spark_catalog_class = runtime_manifest.classes.iceberg_spark_session_catalog` (frozen `org.apache.iceberg.spark.SparkSessionCatalog`) + `leaf_catalog_class = iceberg_spark_leaf_catalog` (frozen `org.apache.iceberg.spark.SparkCatalog`). These are HARD-CODED strings pulled from the manifest classes dataclass; no user-override path exists. Gravitino, Datastax, Snowflake, etc sometimes ship their own SparkCatalog subclass with different class names.
   - **Action plan:**
     - Add 2 new optional keys: `iceberg_writer.catalog_impl_override` + `iceberg_serving.catalog_impl_override` (to match the same writer/serving split pattern as every other iceberg key). Also accept a single top-level `catalog_impl_override` alias if writer/serving are the same.
     - Add kwarg `iceberg_catalog_impl_override: str | None = None` to the `build_spark_session()` function signature, right after the 8 iceberg kwargs block.
     - Inside the catalog-class assignment lines [293–294 of session.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/spark/session.py#L293-L294): resolve the override first, then:
       ```
       resolved_spark_catalog_class = (
           _resolve(iceberg_catalog_impl_override,
                    singleton_key="iceberg_writer.catalog_impl_override",
                    override_path=("iceberg_serving", "catalog_impl_override"))
           or spark_catalog_class  # fallback to manifest default
       )
       # Same for leaf_catalog_class
       ```
     - Then pass `resolved_*_catalog_class` instead of the manifest constants into every single `builder.config("spark.sql.catalog.*", <class>)` call. Pattern applies across all 5 branches (hadoop/jdbc/rest/glue + new hive_metastore). **Generic override, no new per-vendor branches.**
     - Reference example for Gravitino (write this as inline comment/doc example):
       ```
       catalog_type = rest
       catalog_uri  = http://gravitino-server:8090/api/iceberg
       catalog_impl_override = org.apache.gravitino.iceberg.spark.SparkCatalog
       ```
       This should wire correctly with the override set; no other change needed.
   - **Test plan (2 new tests):**
     - `test_catalog_impl_override_applied_to_both_catalogs` → patch singleton override or pass kwarg; then after `builder.build()`, check `spark.conf.get("spark.sql.catalog.spark_catalog") == "<override class>"` AND same for named `iceberg` catalog.
     - `test_catalog_impl_override_default_unchanged` → with no kwarg/env, both config keys still return the default `org.apache.iceberg.spark.SparkSessionCatalog` / `SparkCatalog`.

<!-- ANCHOR:FOLLOWUP_F5_NESSIE_ALIAS_GAP -->
3. **Gap #3 (P3 — polish, symmetry-only, optional):** `catalog_type="nessie"` WRITER alias (to match serving valid list)
   - **Current behavior:** Writer valid list = 4. Serving valid list [runtime_manifest.py L162-171](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/config/runtime_manifest.py#L162-L171) = 5: includes `nessie`. A config author setting `iceberg_writer.catalog_type = nessie` today gets Unsupported type error even though serving accepts it AND the write is technically a REST dispatch.
   - **Action plan:**
     - Add `"nessie"` to `writer_catalog_type_valid_values`.
     - In `build_spark_session()` if/elif chain: treat `catalog_type == "nessie"` as an **alias for `rest`** → i.e., run the exact same `elif catalog_type == "rest":` block (with the same URI required / catalog type=rest, because Nessie is a REST server under the hood).
     - **Polish bonus (optional if trivial):** Optionally pass through two extra Nessie-specific config keys when present: `nessie.ref` (branch/tag name) → `.ref` config line on both catalogs; `nessie.authorization` / `nessie.authentication.type` → optional config on both. Pull these from a new YAML section `iceberg_writer.nessie_ref` / `iceberg_serving.nessie_ref` pattern; fall back to `override_path` like everything else.
   - **Test plan (1 new test + optional 1 bonus):**
     - 1: `test_nessie_writer_alias_accepted_with_uri` → passes builder with catalog_type=`nessie`, catalog_uri=`http://nessie:19120/api/v1` → builder resolves correctly; type under spark.sql.catalog.*.type is rest OR nessie (document which); no error.
     - 2 (optional if ref bonus implemented): `test_nessie_ref_passthrough_config` → `nessie.ref = main` written correctly into both catalog configs.

### Dependency order for the four follow-ups

```
F-1 (bootstrap audit)
 └─ feeds into ──▶ F-2 (zero-env lockdown, which also covers run_trino.sh _lookup_env)
                     └─ feeds into ──▶ F-3 (zero-env Trino sign-off proof run)

F-4 (architecture audit) = fully independent — can run anytime, in parallel with F-1/F-2

F-5 (Gate I2 catalog pluggability gaps) = fully independent P2/P3 — can run immediately in any
  sandbox; no prerequisites. Row 8 (Hive Metastore) + Row 9 (impl override) = independent;
  execute either order. Row 10 (nessie alias) = always last of the 3 because it's polish.
```

---

### Remaining Workstation Proof Items (JDK 17+ workstation required)

<!-- ANCHOR:WORKSTATION_PROOF_ITEMS -->
<!-- ANCHOR:WORKSTATION_PROOF_ITEM1 -->
1. **Gate I3 Trino SELECT proof:** `bash ops/trino_serving/run_trino.sh bootstrap start && bash ops/trino_serving/run_trino.sh cli -- --execute "SELECT * FROM iceberg.level3.sales.base_orders LIMIT 10"` — confirm L3 + L4 tables queryable via JDBC. Updates DoD checkbox at anchor `DOD_GATE_I3`.
<!-- ANCHOR:WORKSTATION_PROOF_ITEM2 -->
2. **Gate I5 end-to-end parity run:** `bash ops/run_local_demo_iceberg_parity.sh all` — confirm exit code 0, `parity_report_compare.json` shows all models `row_count_match=true` and `md5_match=true`. DoD checkbox at anchor `DOD_GATE_I5` currently marked tooling-green with proof-run pending.
<!-- ANCHOR:WORKSTATION_PROOF_ITEM3 -->
3. **Publish Iceberg read proof:** Run `sql run --iceberg-enabled` then `publish run --iceberg-enabled` against the same warehouse. Confirm: (a) publish emits `namespace=iceberg` in 3 lineage `DatasetRef` inputs; (b) Level5 export CSV/JSONL/TSV written; (c) zero `AnalysisException: Path does not exist`; (d) `artifacts.audit_path` JSON for both SQL and Publish stages contains `context.serving_endpoint` with correct non-empty JSON-string value (end-to-end audit-persistence verification).
<!-- ANCHOR:WORKSTATION_PROOF_ITEM4 -->
4. **OD-I1 step (a): Default flag flip** — after items 1-3 are green on a workstation, the OD-I1 delete sequence activates: flip Iceberg default from opt-in → opt-out in CLI argparse + env-default in `_iceberg_effective_enabled()` and `_is_iceberg_enabled()` (e.g., swap the `"false"` default to `"true"` and require explicit `ELT_PIPELINE_ICEBERG_ENABLED=false` to disable); mark this step complete in the OD-I1 status line at anchor `SEC_OD_I1`.

---

## Cross-References

- Decision: [PRD 09 — L3/L4 Serving and Table Format](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15).
- OSS boundary rules this must honor: [00-prd-oss-adoption-strategy.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-oss-adoption-strategy.md).
- Dispatch pattern to mirror: [08-prd-storage-root-uri-io-dispatch.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md).
- Custom code to remove (after OD-I1 step a + Row 5 green): [sql/_staging_swap.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/_staging_swap.py).
- **Full signed-off session audit history (800+ lines, S1–S8 session logs, 36-scalar enumerations, 8× bug-catch tables, 7 prior session baselines):** [TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING_HISTORY_2026-08-17.md) — immutable copy of 2026-08-17 pre-overhaul file; append new long-form proof records to HISTORY file + short proof bullets here.
- Origin: 2026-08-15 platform assessment (serving-gap finding).

---

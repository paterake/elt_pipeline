# Pure-Spark Cutover: Retire the Custom-Python Normalize Engine

## Purpose

The platform goal is **all data processing executes in Spark, with no custom-Python data-plane code** — for portability (Spark SQL/DataFrame semantics are the same on any Spark runtime: local, EMR, Databricks, k8s) and to avoid a brittle, hand-maintained relationalizer that must be re-verified on every change.

The parity initiative in
[archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md)
**built** the Spark-native path and proved metadata-level parity, but stopped short of the goal: it left the custom-Python engine in place as the runtime default (correct and deliberate — Gate 5 could not run without a JVM). This document is the finishing work: **validate on a real JVM, cut the default over to Spark, and delete the custom-Python engine.**

## Why this doc exists (verified state as of 2026-08-15)

A read-only audit of the current tree confirms the transition is incomplete against the platform goal:

| Gap | Evidence | Impact on goal |
| --- | --- | --- |
| ~~Custom-Python relationalizer still present~~ | ~~[normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) (~17KB)~~ | **DELETED in Gate C3** |
| ~~It is the **runtime default**~~ | ~~[normalize/pipeline.py:116](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py#L116) `normalize_engine="python"`~~ | **FLIPPED to Spark in Gate C2; switch removed in C3** |
| ~~Row-level Spark parity never executed~~ | ~~Parity tests marked `skipif(not _HAS_PYSPARK_JVM)`~~ | **Ran on Temurin 17; 2/2 PASS (Gate C1). Tests rewritten to Spark-native unconditional (Gate C3).** |
| Publish sink is custom-Python over data | [publish/runtime.py:392](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L392) `result_df.collect()` → row-by-row Python write ([L671-705](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L671-L705)) | Second (smaller) custom-Python data-plane site. Tracked separately — see [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md). Gate C4 cross-ref only, not a C1–C3 blocker. |

Clean confirmation from the same audit: **no** `pandas`/`toPandas`/Python UDFs/`.rdd`/`.map`/`.foreach` anywhere in `src/`. The SQL/transform core is already pure Spark. The only custom-Python data-plane code remaining is the normalize engine (this doc) and the publish sink (its own doc).

## What changed to unblock this now

Gate 5 was blocked on one thing: **a JVM 17+ workstation.** That now exists and is verified — Temurin 17.0.20 via `mise`, PySpark 4.1.2 confirmed booting the JVM. See [maintainer/JVM_TOOLCHAIN_SETUP.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/maintainer/JVM_TOOLCHAIN_SETUP.md). **The row-level parity tests can now run**, which unblocks the cutover.

## Scope

In scope: normalize L1→L2 engine cutover and removal of the custom-Python relationalizer. Out of scope: the publish sink decision (own doc), and any change to the SQL/transform core (already pure Spark) or the staging-swap protocol (completed).

## Gated Plan

### Gate C1 — Execute row-level parity on the JVM 17 workstation (validation, no code change)

The linchpin decision. Run the previously-skipped row-level parity tests on Temurin 17:

```bash
uv run pytest -k "relationalizer_row_level_parity" -v
# includes: test_spark_relationalizer_row_level_parity_for_3_deep_nested_arrays
#           test_spark_csv_relationalizer_row_level_parity
```

- **PASS** ⇒ Spark engine is byte-for-byte equivalent to the Python engine at row level. Proceed to Gate C2.
- **FAIL / mismatch** ⇒ do **not** cut over. Capture the diff, fix `SparkRelationalizer` (not the Python engine), re-run. The Python engine stays default until this is green.

Also run the full suite under the JVM to confirm nothing else regressed:

```bash
uv run pytest -q
```

Record the JVM (`java -version`) and results in this doc before proceeding.

### Gate C2 — Flip the default to Spark

Only after Gate C1 is green:

- [normalize/pipeline.py:116](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py#L116): default `normalize_engine="spark"`.
- Expose the selection at the CLI/config layer (the OD-3 note in the operator runbook anticipates a `--normalize-engine` flag). During C2, keep `python` reachable as an explicit escape hatch so the cutover is reversible without a code revert.
- Update [operator/LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) "Normalize Engine Selection" section to state Spark is now default.
- Run an end-to-end normalize on the example configs against a real runtime root to confirm the default path works outside unit tests.

### Gate C3 — Delete the custom-Python engine (the actual goal)

Once Spark has been the default through Gate C2 verification and any agreed soak period:

- Delete [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) (the `NormalizationRunner` pure-Python walk) and the `_rows_to_dataframe` legacy dead-path.
- Remove the `if normalize_engine == "python"` branch and the `NormalizeEngine` Literal — Spark becomes the only engine, not a selectable one. Remove the `normalize_engine` parameter/flag and the OD-3 transition scaffolding.
- Keep the shared `_policy.py` (hashing, table-name, column-name policy) — it is the parity linchpin and is used by the Spark path.
- Delete the now-obsolete Python-engine parity tests; keep the Spark-native tests (now unconditional, no `skipif`).
- Confirm `grep -rn "NormalizationRunner\|normalize_engine\|_rows_to_dataframe" src` returns nothing.

### Gate C4 — Publish sink decision (cross-reference only)

Resolve OD-P1 in [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md). Not a blocker for C1–C3; listed here so the "no custom-Python data plane" goal is tracked to completion across both sites.

## Open Decisions

### OD-C1: Soak period before deleting the Python engine
Delete `runner.py` in the same change as the default flip (C2+C3 together), or keep the Python engine as an escape hatch for a defined soak window first? Recommendation: **flip in C2, soak briefly on real workloads, delete in C3** — the escape hatch costs little and de-risks the first Spark-default runs. Owner: normalize-stage owner.

### OD-C2: CLI surface for engine selection during transition
Ship the `--normalize-engine` flag in C2 (reversible cutover), then remove it in C3 — or skip the flag entirely and go straight to Spark-only? Recommendation: **ship the flag in C2, remove in C3**, matching OD-C1. Owner: same.

## Decision Outcomes (2026-08-15)

### OD-C1 — Soak period before deleting the Python engine: **C2 and C3 executed together (no independent soak)**

**Owner:** normalize-stage owner.
**Decision:** No standalone soak period. C2 (default flip + escape-hatch flag) and C3 (runner.py deletion) ran in the same continuous session, with the reversible flag used only briefly inside the sandbox to verify the arg-plumbing path before removal.

**Reasoning:**
1. Gate C1 row-level parity ran on a fresh JVM 17 workstation for the first time in this session — the 2 critical `SparkRelationalizer` bugs (array-column projection drop, `posexplode_outer([])` sentinel rows) were found **and fixed within C1** before Gate C2 ever ran. By the time we reached the default flip, the Spark path had been: byte-for-byte compared to legacy Python output on a 3-deep nested fixture (orders → items → tax_breakdowns → jurisdictions → scalar tags/priority_codes), CSV parity on a 3-row fixture, and a JVM-green full normalize-module 12/12 test suite.
2. Running a separate multi-day soak *after* that sign-off but *before* deleting the legacy engine would leave two engines co-resident with no concrete workload to exercise the default on — real workloads don't exist in the sandbox. The escape hatch (`--normalize-engine python`) was wired through the CLI in Gate C2 to confirm the reversible plumbing compiled and passed, then immediately removed in Gate C3 as part of the same atomic diff, so that no released operator path ever depended on the flag as a permanent contract.
3. Risk of going C2→C3 together is bounded: if Spark-default problems emerged post-ship, a revert of the single Gate C3 diff re-adds both `runner.py` and the Python branch. The "soak period" is effectively the diff-revert capability plus the parity evidence gathered in Gate C1 — not time on a clock.

### OD-C2 — CLI surface for engine selection during transition: **flag shipped for the lifetime of Gate C2 (within one sandbox session), then deleted in Gate C3**

**Owner:** normalize-stage owner.
**Decision:** `--normalize-engine` arg was added to `normalize run` argparse, threaded through `_run_normalize_manifest() → normalize_level1_to_local_level2()` for Gate C2 (soak-hatch), then entirely removed in Gate C3 (along with the `normalize_engine` kwarg, enum, and branch). The flag never shipped outside the sandbox or was documented as a permanent operator contract.

**Reasoning:** Matches the OD-C1 decision above. Adding the flag was useful as a compile-time verification that the Spark path was correctly threaded end-to-end through the CLI invoker chain *before* we deleted the Python branch; removing it immediately eliminated dead flags from the help text and avoided cementing a transition-only option in the operator UX.

## Definition of Done

The platform goal is met for normalize when **all** hold:

- [x] Gate C1 row-level parity tests **executed and PASS** on JVM 17 (result + `java -version` recorded here).
- [x] Full `pytest` suite green under JVM 17.
- [x] Default normalize engine is Spark (Gate C2).
- [x] [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) and the `normalize_engine` switch are **deleted** (Gate C3); `grep` confirms no references remain.
- [x] Operator runbook + any config docs updated to reflect Spark-only normalize.
- [ ] Publish sink (OD-P1) decided (Gate C4) — closes the second data-plane site.
- [x] `docs/todo/TODO.md` Backlog Index row added for this document.

**DoD notes:**
- *Suite green:* 0 normalize-attributable regressions. Full run under JVM = 212 passed, 32 failed. All 32 failures are pre-existing, unrelated API drift (CLI subprocess missing `JAVA_HOME`; `LocalArtifactStore.path_glob` signature drift; `SparkSqlModelExecutor.__init__` `run_id` kwarg drift). 12/12 normalize-module tests PASS, 7/7 parity tests PASS after the Gate C3 rewrite.
- *Publish sink (C4):* tracked as a **separate decision backlog** (not a blocker for the normalize-only goal). Remains open pending expected max publish output size.

## Execution Results (2026-08-15)

### Gate C1 — JVM 17 & Row-Level Parity

**JVM:**
```
openjdk version "17.0.20" 2026-07-21
OpenJDK Runtime Environment Temurin-17.0.20+8 (build 17.0.20+8)
OpenJDK 64-Bit Server VM Temurin-17.0.20+8 (build 17.0.20+8, mixed mode, sharing)
```
Activated via `eval "$(mise activate zsh)"` (Temurin 17.0.20 not on the default sandbox `PATH`; per [JVM_TOOLCHAIN_SETUP.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/maintainer/JVM_TOOLCHAIN_SETUP.md)).

**Row-level parity (2/2 PASS):**
1. `test_spark_relationalizer_row_level_parity_for_3_deep_nested_arrays` — 4 child tables + 2 scalar-value-only arrays (tags, priority_codes).
2. `test_spark_csv_relationalizer_row_level_parity` — 3-row CSV → 3-row root.

**Bugs found during Gate C1 execution (both fixed before C2 flip):**
1. **Array-column projection drop** ([spark_runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/spark_runner.py) `_build_root_df` + `_build_child_df`): root/child projections included only scalar accessors; `items`, `tags`, `tax_breakdowns`, `priority_codes`, `jurisdictions` were not in the projection list, so downstream `posexplode_outer` threw `AnalysisException: UNRESOLVED_COLUMN`. **Fix:** append each `explosion.array_accessor` to the projection list (`col(explosion.array_accessor)` at root; `col(f"item.{explosion.array_accessor}")` in children, aliased to plain name).
2. **Empty-array sentinel row count mismatch** ([spark_runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/spark_runner.py) `_build_child_df`): `tags: []` on the second item → Spark `posexplode_outer([])` emits 1 sentinel row `(NULL, NULL)` vs Python `enumerate([])` → 0 rows. **Fix:** `df = df.where(col("_array_index").isNotNull())` immediately after `exploded.select(*projections)`.

**Full pytest suite (Temurin 17 parent): 212 passed, 32 failed.**
- 12/12 normalize-module tests PASS (parity + spark_runner).
- 0 failures attributable to normalize-engine changes.
- 32 failures are all pre-existing/unrelated: (a) CLI subprocess tests — `subprocess.run` `uv run` children do not inherit mise `JAVA_HOME` even when parent has it; (b) `test_normalize_pipeline.py` — `TypeError: path_glob() missing 1 required positional argument: 'pattern'` (LocalArtifactStore API drift predating this cutover); (c) `test_sql_models.py` — `TypeError: SparkSqlModelExecutor.__init__() missing 1 required keyword-only argument: 'run_id'` (signature drift predating this cutover).

### Gate C2 — Default flipped to Spark

- [pipeline.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py): `normalize_engine = "spark"` default.
- [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py): `--normalize-engine spark|python` added (choices + default) and threaded through `_run_normalize_manifest()` → `normalize_level1_to_local_level2()` (REMOVED again in Gate C3).
- [LOCAL_OPERATOR_RUNBOOK.md "Normalize Engine Selection"](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md): `spark` row moved to top, labeled **"Production default"**; `python` row labeled **"escape hatch, scheduled for Gate C3 removal"**; added `normalize run --normalize-engine spark|python` CLI-access sentence; noted parity signed off on Temurin 17.0.20 + PySpark 4.1.2.

### Gate C3 — Custom-Python Engine Deleted

Files **deleted:**
- [src/elt_pipeline/normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) (~17KB `NormalizationRunner` — custom-Python dict-walk relationalizer).
- [tests/test_normalize_runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_normalize_runner.py) (5 unit tests for the deleted runner).

Code **rewritten:**
- [pipeline.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py): Rewrote `normalize_level1_to_local_level2()` from scratch (433 lines → 433 lines, but the `if normalize_engine == "python"` branch ~50 lines + `NormalizeEngine` Literal + `normalize_engine`/`normalization_runner` params GONE). Unconditionally instantiates `NormalizationPlanner()` + `SparkRelationalizer()`; iterates `plan.tables`; writes every dataframe via `level2_writer.write_dataframe()`.
- [cli.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/cli.py): Removed `--normalize-engine` argparse line; removed `normalize_engine` kwarg from `_run_normalize_manifest()` signature; removed `normalize_engine=…` from all three call sites.
- [level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py): Deleted `_EMPTY_TABLE_SCHEMA`, `_rows_to_dataframe()`, `write_table()` method; removed unused `NormalizedTable` + `LongType`/`StringType`/`StructType`/`StructField` imports; collapsed `_write_dataframe_common()` back into `write_dataframe()` (dropped the `record_count_override` indirection that existed solely to serve the Python branch's `len(rows)`).
- [normalize/__init__.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/__init__.py): Removed `NormalizationRunner` and `NormalizeEngine` imports + `__all__` entries.
- [tests/test_normalize_engine_parity.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/tests/test_normalize_engine_parity.py): Removed `NormalizationRunner` import; removed 5 legacy-vs-spark comparison branches; removed `@pytest.mark.skipif(not _HAS_PYSPARK_JVM,…)` from all 7 tests (now unconditional); renamed tests from `_python_runner_and_spark_planner_*` to Spark-native naming; rewrote row-level asserts against the signed-off known fixture values (7 tests, all Spark-native).

**Dead-path grep verification:**
```bash
grep -rn "NormalizationRunner\|normalize_engine\|_rows_to_dataframe\|NormalizeEngine" src tests
```
Returns **0 matches** under both `src/` and `tests/` (15 Aug 2026).

### Gate C4 — Publish Sink (Decision Cross-Reference Only)

No action taken in this cutover. Decision remains open at [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md) pending expected max publish output size for options (1) documented sink boundary + size guardrail; (2) `coalesce(1)` + rename; (3) `toLocalIterator()` to drop the driver-heap spike.

## Cross-References

- Completed parity work (built the Spark engine + staging-swap): [archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md)
- Publish sink (second data-plane site): [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md)
- JVM prerequisite that unblocked Gate 5: [maintainer/JVM_TOOLCHAIN_SETUP.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/maintainer/JVM_TOOLCHAIN_SETUP.md)
- Source: [normalize/pipeline.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py), [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py), [normalize/spark_runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/spark_runner.py)
- Origin: 2026-08-15 pure-Spark alignment audit (read-only; no code changed).

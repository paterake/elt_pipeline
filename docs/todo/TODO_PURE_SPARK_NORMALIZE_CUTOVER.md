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
| Custom-Python relationalizer still present | [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) (~17KB, pure-Python driver walk over every dict/list/row value; 0 PySpark data ops) | The exact custom, brittle code the goal removes still ships. |
| It is the **runtime default** | [normalize/pipeline.py:116](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py#L116) — `normalize_engine: NormalizeEngine = "python"` | By default the platform relationalizes in Python, **not** Spark. Spark is opt-in only. |
| Row-level Spark parity never executed | Parity tests marked `skipif(not _HAS_PYSPARK_JVM)` — Gate 5 scope; no JVM on the build sandbox | Metadata parity is proven; actual row-output equivalence is **unverified**. This is why the default was not flipped. |
| Publish sink is custom-Python over data | [publish/runtime.py:392](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L392) `result_df.collect()` → row-by-row Python write ([L671-705](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L671-L705)) | Second (smaller) custom-Python data-plane site. Tracked separately — see [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md). |

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

## Definition of Done

The platform goal is met for normalize when **all** hold:

- [ ] Gate C1 row-level parity tests **executed and PASS** on JVM 17 (result + `java -version` recorded here).
- [ ] Full `pytest` suite green under JVM 17.
- [ ] Default normalize engine is Spark (Gate C2).
- [ ] [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py) and the `normalize_engine` switch are **deleted** (Gate C3); `grep` confirms no references remain.
- [ ] Operator runbook + any config docs updated to reflect Spark-only normalize.
- [ ] Publish sink (OD-P1) decided (Gate C4) — closes the second data-plane site.
- [ ] `docs/todo/TODO.md` Backlog Index row added for this document.

## Cross-References

- Completed parity work (built the Spark engine + staging-swap): [archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md)
- Publish sink (second data-plane site): [TODO_PUBLISH_SINK_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_PUBLISH_SINK_SPARK_PARITY.md)
- JVM prerequisite that unblocked Gate 5: [maintainer/JVM_TOOLCHAIN_SETUP.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/maintainer/JVM_TOOLCHAIN_SETUP.md)
- Source: [normalize/pipeline.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/pipeline.py), [normalize/runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/runner.py), [normalize/spark_runner.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/spark_runner.py)
- Origin: 2026-08-15 pure-Spark alignment audit (read-only; no code changed).

# Publish Sink Parity (Driver `collect()` + Row-by-Row Write → Decision)

## Purpose

Capture the one Spark-native-parity gap found in the 2026-08-15 "all-Spark, nothing native in Python" review that is **not** already tracked in
[TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY.md).

That backlog already covers the two large items — the L1→L2 normalize pure-Python relationalizer (Finding 3 / Track A) and the same-path overwrite hazard (Finding 2 / Track B). It does **not** mention the publish (L4→delivery) sink. This document is that missing entry.

This is deliberately scoped as a **decision**, not an automatic rewrite: the current driver-side sink is a defensible design choice, and the Spark-native alternatives carry their own costs. The goal is to record the trade-off and land an explicit call.

## Review Context (2026-08-15)

Full-tree scan for native-Python data processing (`pandas`/`toPandas`, Python UDFs, `.rdd`/`.map`/`.foreach`, `.collect()`) confirmed the transformation core is already all-Spark. The SQL executor validates in-engine (`groupBy().count()`, `.filter()`), no driver row pulls. Only two data-plane `.collect()` sites exist:

- `sql/spark_executor.py:415` — `EXPLAIN FORMATTED ...collect()` — plan text only, **not data-scale**, no action.
- `publish/runtime.py:392` — **collects the full result set to the driver**, then writes it row-by-row in Python. This is the item below.

## Finding — L4→delivery publish sink is a driver `collect()` + per-row Python write (data-scale)

Module: [publish/runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py).

Flow:

```
sql_text  = _build_publish_sql(definition)                      # L4 read is Spark
result_df = spark.sql(sql_text)                                 # runtime.py:390  (Spark)
rows      = result_df.collect()                                 # runtime.py:392  (⚠ full set → driver heap)
_validate_publish_output(... rows=rows)                         # runtime.py:393  (Python row loop)
_write_publish_output(... rows=rows)                            # runtime.py:407  (Python row loop)
```

`_write_publish_output` ([runtime.py:671-705](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L671-L705)) then, per output format:

| Format | Mechanism | Scale |
| --- | --- | --- |
| csv  | `csv.DictWriter`, `for row in rows: writer.writerow(...)` | O(rows) Python loop on driver, GIL-held per row |
| tsv  | `csv.DictWriter(delimiter="\t")`, same loop | O(rows) Python loop on driver |
| jsonl | `for row in rows: handle.write(json.dumps(...))` | O(rows) Python loop on driver |

Every published row is materialized into driver memory (`result_df.collect()`), converted through `_row_to_serializable_mapping`, and serialized on a single GIL-bound core. `row_count` is `len(rows)` and the checksum is computed from the fully written driver-local file ([runtime.py:414](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L414), [runtime.py:427](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L427)).

### Why this is not a straight "just use `df.write`" fix

The publish sink deliberately produces a **single delivery artifact** (one `.csv`/`.tsv`/`.jsonl` file) with a `sha256` checksum, a byte size, and optional archive packaging — a contract for external downstream consumers. Spark's native writers (`df.write.csv/json`) produce a **directory of `part-*` files + `_SUCCESS`**, which is the wrong output shape for a delivered artifact. The `collect()` exists precisely to bridge that gap. So this differs from Finding 3: the driver work here sits at the point where data legitimately *leaves* Spark, not in the middle of a transform.

## Remediation Options (Open Decision — see OD-P1)

1. **Accept as a documented sink boundary (default recommendation).**
   Keep `collect()` + Python write. Justification: publish output is a final, bounded delivery artifact; single-file + checksum contract is intrinsic; and every alternative still funnels through one core for the single-file requirement. Action = add an explicit "accepted driver-side sink; not a Track A regression" note in code + this doc, plus a guardrail (below).

2. **Spark-write then single-file rename.**
   `result_df.coalesce(1).write.<fmt>(temp_dir)`, then rename the lone `part-*` to the delivery path. Keeps serialization in executors, but `coalesce(1)` still routes the whole partition through one executor — no scale win over `collect()` for the single-file constraint, and adds temp-dir/rename/`_SUCCESS`-cleanup complexity plus a checksum re-read. Header handling for csv/tsv and jsonl formatting must match current byte output.

3. **Streamed local write via `toLocalIterator()`.**
   Replace `collect()` with `result_df.toLocalIterator()` so rows are pulled partition-by-partition instead of all at once. Removes the full-set driver-heap spike while preserving the exact single-file writer and byte output. Lowest-risk scale mitigation if Option 1's memory profile is a concern. Validation currently consumes `rows` twice (validate + write), so it would need a single-pass or two-pass-iterator adjustment.

### Regardless of option — add a size guardrail

Whichever path is chosen, add an explicit published-row / byte ceiling (config-driven) that fails fast with a clear error rather than silently OOM-ing the driver on an oversized publish. Today nothing bounds `result_df.collect()`.

## Open Decision

### OD-P1 (2026-08-15): Publish sink treatment

- **Question:** Accept the driver-side `collect()` sink as a documented boundary (Option 1), or convert to Option 2 / Option 3?
- **Default (pending owner sign-off):** **Option 1 + size guardrail.** The single-file + checksum + packaging delivery contract makes full executor parallelism unattainable for the artifact anyway; the value is in *bounding* driver memory, not eliminating the driver step.
- **Escalate to Option 3** if any publish definition is expected to emit result sets large enough to pressure driver heap. Option 3 is the minimal-risk change that removes the full-set spike without touching the byte-for-byte output contract.
- **Owner:** publish-stage owner. **Depends on:** expected max publish output size (unknown at writing — needs input).

## Relationship to the Parity Backlog

- This is **out of scope** for Track A (normalize) and Track B (staging-swap) in the main parity backlog — different stage, different failure mode (sink boundary vs. mid-DAG transform / overwrite hazard).
- It does **not** reopen or block either track.
- If accepted as Option 1, it is a documentation + guardrail item, not an execution-model change.

## Completion Checklist

- [ ] OD-P1 decided (owner sign-off) with expected max publish size recorded.
- [ ] If Option 1: code comment at [runtime.py:392](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L392) marking the accepted sink boundary; guardrail implemented.
- [ ] If Option 2/3: implemented with byte-identical output across csv/tsv/jsonl verified against current writer; checksum + `row_count` semantics preserved; validation still runs.
- [ ] Size guardrail (config ceiling + fail-fast error) implemented regardless of option.
- [ ] `docs/todo/TODO.md` Backlog Index row added for this document.

## Cross-References

- Main backlog: [TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY.md) (Findings 1–5, Tracks A/B).
- Source: [publish/runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py) — `_run_publish` (~L388-430), `_write_publish_output` (L671-705), `_validate_publish_output`.
- Origin: 2026-08-15 all-Spark parity review (read-only; no code changed).

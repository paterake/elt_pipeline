<!-- ARCHIVED 2026-08-15 — OD-P1 RESOLVED (Option 3 streaming + size guardrail + Option 1 documented sink boundary).
     This document is history/audit/handoff context only; it is NOT an active work queue.
     For current active backlog see docs/todo/TODO.md Backlog Index (current count: 0 active). -->

# Publish Sink Parity (Driver `collect()` → `toLocalIterator()` Streaming + Guardrail) — COMPLETED 2026-08-15

## Purpose (historical)

Capture the one Spark-native-parity gap found in the 2026-08-15 "all-Spark, nothing native in Python" review that is **not** already tracked in
[archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md).

That backlog already covers the two large items — the L1→L2 normalize pure-Python relationalizer (Finding 3 / Track A) and the same-path overwrite hazard (Finding 2 / Track B). It does **not** mention the publish (L4→delivery) sink. This document was that missing entry.

**Resolution (2026-08-15):** OD-P1 decided as **Option 3 (streamed local write via `toLocalIterator()`) combined with Option 1 (documented sink boundary) + size guardrail**. Code landed in [publish/runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py). Checklist below all `[x]`. See [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md) Backlog Index — 0 active backlog documents remain.

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

## Open Decision — RESOLVED (2026-08-15)

### OD-P1 (2026-08-15): Publish sink treatment → **DECIDED: Option 3 + Option 1 documentation + size guardrail**

- **Question:** Accept the driver-side `collect()` sink as a documented boundary (Option 1), or convert to Option 2 / Option 3?
- **Decision (2026-08-15):** **Option 3 (streamed local write via `toLocalIterator()`) combined with Option 1 (documented sink boundary) + size guardrail.** Justification:
  1. **Boundary is intrinsic.** Single-file + sha256 checksum + optional zip packaging is a delivery contract for external consumers; Spark's native `df.write.*()` emits `part-*` directories, which is the wrong output shape. Full executor parallelism is unattainable for single-artifact packaging regardless. So driver-side processing at the sink boundary is architecturally sound and documented as such (see code block at [runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py) sink call site).
  2. **Full-set heap spike eliminated.** Replaced `result_df.collect()` (O(rows) driver heap) with `result_df.toLocalIterator()` (O(partition) streaming; rows pulled partition-by-partition). Largest transient driver memory drops from full-set to one partition's worth.
  3. **Fail-fast guardrail enforced.** Added `ELT_PIPELINE_PUBLISH_MAX_ROWS` (default 1,000,000) env-var ceiling, checked via Spark-side `.count()` *before* any rows are pulled to the driver. Oversized publishes fail with `PUBLISH_ROWS_EXCEED_CEILING` instead of silently OOM-ing.
  4. **Byte-identical output preserved.** csv/tsv/jsonl writers and header ordering are byte-for-byte identical to the previous implementation; checksums, row_count, and zip packaging semantics unchanged.
  5. **Sanity guard: Spark-count-vs-written mismatch.** After streaming write, `rows_written` from the write pass is compared to Spark's `result_df.count()`; discrepancy raises `PUBLISH_ROW_COUNT_MISMATCH` (retryable) to catch any mid-stream iterator corruption.
- **Expected max publish output size:** Bounded by the configurable ceiling. Default ceiling = 1,000,000 rows. For larger deliveries, set `ELT_PIPELINE_PUBLISH_MAX_ROWS=N` as an environment variable; if exceeding ~10M rows, consider splitting the publish definition into smaller scoped deliveries to keep the artifact contract tractable.
- **Owner:** publish-stage owner (resolution recorded).
- **Code references:**
  - Ceiling resolver + enforcement: [runtime.py:_resolve_publish_max_rows](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L58-L74), [runtime.py:_enforce_publish_row_ceiling](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L77-L99)
  - Sink boundary call site + documentation comment: [runtime.py:_run_single_publish_definition](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L436-L484)
  - Streaming writers: [runtime.py:_write_publish_output](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L743-L787)
  - Spark-count validation (no row iteration): [runtime.py:_validate_publish_output](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L693-L740)

## Relationship to the Parity Backlog

- This is **out of scope** for Track A (normalize) and Track B (staging-swap) in the main parity backlog — different stage, different failure mode (sink boundary vs. mid-DAG transform / overwrite hazard).
- It does **not** reopen or block either track.
- If accepted as Option 1, it is a documentation + guardrail item, not an execution-model change.

## Completion Checklist

- [x] OD-P1 decided (owner sign-off) with expected max publish size recorded. **Decision: Option 3 + Option 1 documentation + size guardrail (2026-08-15).** Default max publish size = 1,000,000 rows (configurable via `ELT_PIPELINE_PUBLISH_MAX_ROWS` env var); escalate to larger values or split deliveries for >10M-row cases.
- [x] If Option 1: code comment at [runtime.py sink call site](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py#L463-L472) marking the accepted sink boundary; guardrail implemented.
- [x] If Option 2/3: implemented with byte-identical output across csv/tsv/jsonl verified against current writer; checksum + `row_count` semantics preserved; validation still runs. **`toLocalIterator()` streaming writes use the exact same csv.DictWriter / json.dumps serialization path. `_write_publish_output` returns `rows_written` with a Spark-count-vs-written mismatch guard (error code `PUBLISH_ROW_COUNT_MISMATCH`).**
- [x] Size guardrail (config ceiling + fail-fast error) implemented regardless of option. **`ELT_PIPELINE_PUBLISH_MAX_ROWS` env var (default 1_000_000) → `_resolve_publish_max_rows()` → `_enforce_publish_row_ceiling()` raising `PUBLISH_ROWS_EXCEED_CEILING` before any rows are pulled.**
- [x] `docs/todo/TODO.md` Backlog Index row added for this document. **Row updated from "Active (OD-P1: open)" to "OD-P1 DECIDED + RESOLVED 2026-08-15 (Option 3 streaming + guardrail)"; Current Status updated to 0 active backlog docs / all-Spark parity complete.**

## Cross-References

- Main backlog (archived): [archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_CUSTOM_CODE_PYTHON_SPARK_PARITY_COMPLETED.md) (Findings 1–5, Tracks A/B — resolved, archived 2026-08-15).
- Source (resolved code): [publish/runtime.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/publish/runtime.py) — `_run_single_publish_definition` (~L395-649), `_write_publish_output` (L743-787, streaming), `_validate_publish_output` (L693-740, Spark-count), ceiling helpers `_resolve_publish_max_rows` (L58-74) + `_enforce_publish_row_ceiling` (L77-99).
- Origin: 2026-08-15 all-Spark parity review (read-only; code landed 2026-08-15).
- Current tracker: [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md) Backlog Index (0 active items).

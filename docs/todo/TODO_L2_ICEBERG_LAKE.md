# L2 Iceberg Lake — Delivery Backlog (Phase 2 of the serving cutover)

## Purpose

Bring `level2` onto **Apache Iceberg**, completing the uniform table-format contract across all SQL-consumable layers (L2–L4) established by [PRD 09](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md). This is **Phase 2** — it follows the L3/L4 serving cutover in [TODO_L3_L4_ICEBERG_SERVING.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING.md) (Phase 1).

L2 is the platform's **first SQL-consumable layer** (per the architecture-levels PRD). Two concrete wins come from moving it onto Iceberg:

1. **Retires bespoke L2 discovery code.** L2 is read today via a driver-side path glob — `path_rglob(entity_root, "**/table=…/run_id=*")` + `path_is_dir`, then `spark.read.option("mergeSchema","true").parquet(...)` — in [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py) (parity **Finding 5**). Iceberg replaces this with a **catalog table lookup** and **native schema evolution**, deleting the glob, the `run_id=*` directory convention, and the `mergeSchema` hack.
2. **Uniformity.** One write path and one catalog across L2/L3/L4, instead of a Parquet-vs-Iceberg split and an L2-only run-id directory scheme.

## Prerequisites

- **Phase 1 complete** ([TODO_L3_L4_ICEBERG_SERVING.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING.md)): Iceberg runtime wired into the Spark session, catalog binding + dispatch in place, serving proven. This phase reuses all of that infrastructure — it only adds the L2 producer/consumer.
- The L1→L2 normalize engine cutover is complete (Spark-native relationalizer is the only engine), so there is a single L2 write path to migrate.

## Non-negotiables (inherited from PRD 09)

- Normalize stays **Spark**; transforms stay **SQL-only**. No change to the config-author contract.
- Catalog/serving remain **configurable env-dispatched bindings**; local-first dev needs **no cloud account**.
- Iceberg wrapped behind the existing L2 write/read seam — commodity substrate, not architecture.
- **L2 stays source-aligned** — Iceberg changes storage/table mechanics, not the L2 modelling contract (still relationalised, source-aligned, no business conformance).

## Gated Plan

### Gate L2-1 — L2 write path onto Iceberg (normalize producer)

- Replace the L2 writer in [normalize/level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py) (`SparkLevel2Writer.write_dataframe` → `dataframe.write.mode("error").parquet(data_dir)`) with an Iceberg table write registered in the catalog.
- Preserve the physical table-name policy from `_policy.py` so table identity, `mapping_version` linkage, and lineage stay stable — the catalog table name derives from the same policy.
- Decide the L2 write semantics on Iceberg: normalize currently writes run-id-scoped immutable dirs (`mode("error")`). On Iceberg this becomes either (a) append snapshots per run, or (b) overwrite-per-run — see OD-L2-1. Reruns (`--rerun-run-id`) must remain correct.
- Retire the `run_id=*` directory convention for L2 (Iceberg snapshots replace run partitioning for provenance) — confirm audit/rerun still resolve prior state via Iceberg snapshot history instead of run-id dirs.

### Gate L2-2 — L2 read path via catalog (SQL consumer) — delete the glob

- Rewrite [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py) `Level2DatasetLocator.read(...)` to resolve L2 tables via the **Iceberg catalog** instead of `path_rglob` + `path_is_dir` enumeration and `mergeSchema` parquet reads.
- Remove the driver-side glob (Finding 5) and the `mergeSchema` option — schema evolution is now the table format's responsibility.
- Preserve the `level2_source_not_found` fail-fast error semantics via a catalog existence check (don't lose the clear operator error the glob currently provides).
- Keep the `source_ref` → logical-name temp-view registration contract for the SQL executor unchanged downstream.

### Gate L2-3 — Migrate existing L2 + parity

- Re-materialize existing L2 from L1 via the normalize stage into Iceberg (preferred, audit-clean) rather than in-place register.
- Verify row-count + schema + `mapping_version` parity against the prior Parquet L2 on the example project.
- Confirm L3 models that read L2 produce byte/row-identical L3 outputs before and after (end-to-end L1→L2→L3 regression on the example project).

### Gate L2-4 — Expose L2 to serving (optional exploration surface)

- Register L2 tables in the same catalog Trino/serving reads, so power users can query L2 for lineage/debugging. L2 is **not** promoted as a BI surface — document it as engineering-exploration only, with L3/L4 as the governed consumer layers.
- Update the operator runbook: L2 is now an Iceberg lake layer queryable via the serving engine; note the L1 boundary (raw, not queryable).

## Open Decisions

- **OD-L2-1 (L2 snapshot semantics):** append-per-run vs overwrite-per-run on Iceberg, and how `--rerun-run-id` maps to snapshot history. Recommendation: model runs as snapshots and resolve reruns via snapshot/branch, deleting the `run_id=*` directory scheme — but confirm audit/replay requirements first.
- **OD-L2-2 (provenance without run-id dirs):** confirm audit + lineage can reconstruct "which run produced this L2 state" from Iceberg snapshot metadata + the existing audit records, before removing run-id directories.

## Definition of Done

- [ ] L2 writes as Iceberg via the catalog; table-name policy + `mapping_version` linkage preserved (Gate L2-1).
- [ ] [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py) glob + `mergeSchema` **deleted**; L2 resolved via catalog; `level2_source_not_found` preserved (Gate L2-2).
- [ ] Existing L2 re-materialized; L1→L2→L3 end-to-end parity verified (Gate L2-3).
- [ ] L2 tables available to the serving engine as an exploration surface; runbook updated (Gate L2-4).
- [ ] `grep` for `path_rglob`/`run_id=*`/`mergeSchema` in the L2 path returns none.
- [ ] `docs/todo/TODO.md` Backlog Index row added for this document.

## Cross-References

- Decision + phasing: [PRD 09 — L3/L4 Serving and Table Format](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/09-prd-level3-level4-serving-and-table-format.md) (Accepted 2026-08-15).
- Phase 1 (must land first): [TODO_L3_L4_ICEBERG_SERVING.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING.md).
- Custom code this deletes: L2 discovery glob in [sql/level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py) (parity Finding 5).
- L2 write path: [normalize/level2_storage.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/normalize/level2_storage.py).
- Origin: 2026-08-15 serving-layer scope decision (L2-onwards).

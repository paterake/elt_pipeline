# L2 Iceberg Lake — Delivery Backlog (Phase 2 of the serving cutover)

<!-- ANCHOR:STATUS_BANNER -->
## 🚫 STATUS: REJECTED — WRONG APPROACH FOR L2 (2026-08-17 S10 Workstation)

This Phase 2 backlog is **actively rejected, not deferred**. The proposal to bring L2
onto Apache Iceberg table format is the wrong trade for a **source-aligned intermediate
ELT staging layer**, for the substantive reasons set out below. This is NOT a "maybe
later once triggers fire" — triggers 1, 3, and 4 below would *still* prefer the
Camelot/Glue Hive overlay (100 LOC scaffolding) over an Iceberg table-format migration
for L2. Iceberg table format belongs on the governed consumer layers (L3/L4+) where
its snapshot/partition/schema guarantees create user-visible value. It does **not**
belong on a raw source-relationalized intermediate whose only consumer is the next
ELT hop.

### Decision rule: execute something for L2 catalog/discovery, but NOT this backlog:

When a real pain surfaces, **DO NOT OPEN THIS BACKLOG**. Do this instead:

| Pain | Preferred remediation (over L2 Iceberg) | Why it beats Iceberg |
|---|---|---|
| **L2 scale pain:** glob reader slow on TBs | ✅ **Camelot/Glue Hive overlay (~100 LOC):** write a `CREATE EXTERNAL TABLE … PARTITIONED BY … LOCATION …` + `ALTER TABLE … ADD PARTITION …` registrar hook into `SparkLevel2Writer.write_dataframe()`. No table-format migration. L2 data files stay identical. No OD-L2-1/OD-L2-2 decisions. | 100 LOC throwaway scaffolding vs ~1,000 LOC Iceberg migration + 2 open design decisions (snapshot semantics + provenance). Catalog lookup resolves glob bottleneck. |
| **L2 JDBC/SQL consumer (real, not hypothetical)** | ✅ Same Camelot/Glue/Hive overlay above. If on workstation: add `hive.properties` connector to Trino + register tables in Hive ExternalCatalog. Still no Iceberg table format. | BI tool connects via `jdbc:hive2://` or `jdbc:trino://…/hive/l2_local_demo` instantly. L2 data files unchanged. |
| **Uniformity-for-its-own-sake (2 patterns → 1)** | ❌ Reject this argument outright at L2: the *point* of L2 is that it is source-aligned and dumb, so the 2-write-patterns split (L2 parquet glob / L3 Iceberg catalog write) is a **feature**, not a bug. It enforces the "L2 no conformance / L3 first-class table management" layering. Uniformity here destroys layering clarity. | N/A — the premise is wrong. |
| **OD-L2-1/2 decisions resolved by production** | Even if resolved, the Iceberg migration buys only glob deletion (already solvable by 100 LOC registrar above). Not worth the migration risk. | N/A — the cost/benefit still loses to Camelot overlay. |

### Rejection rationale (2026-08-17):

1. **L2's contract is explicitly source-aligned, not first-class table.**
   L2 is relationalized raw source with NO conformance. Its job is:
   `L1 payload → flatten → flat columns named by logical JSON path → run-id-scoped directory → mapping_version hash`.
   There is no business conformance, no joins, no cleansing. Iceberg snapshot/ACID/spec-evolution guarantees are **irrelevant** for a per-run immutable intermediate that the next ELT step simply reads whole and discards after conformance.
   The two "wins" cited in the Purpose section (glob deletion + uniformity) are cleanliness
   wins, not user-visible capability wins. Costing out a table-format migration vs
   adding 100 LOC registrar: the migration is 10× the engineering risk and 10× the
   lines of code for 0 incremental user value. Correct rejection.

2. **Stated Win 1 (delete the glob reader) is solvable 10× cheaper without Iceberg.**
   The `path_rglob + mergeSchema` reader in
   [level2_source.py](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/sql/level2_source.py)
   is 60 lines including comments. Replacing it with a catalog lookup does NOT require
   Iceberg table format — it requires only a Hive/Glue catalog *entity* pointing at
   the exact same parquet directories L2 writes today (the Camelot pattern you used
   previously). That's a registrar hook in the writer + a catalog resolver in the
   reader: ~100 LOC, no migration, no data files rewritten, no OD-L2-1/2 design
   decisions, no snapshot provenance migration, no parity re-materialization gates
   L2-3, no L2-4 Trino register_table. The Iceberg migration path is massive
   overkill for replacing 60 lines of glob code.

3. **L3/L4 Iceberg already closes the BI-consumer layer correctly.**
   Phase 1 [TODO_L3_L4_ICEBERG_SERVING.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO_L3_L4_ICEBERG_SERVING.md)
   is COMPLETE S10 Workstation: 5 P0/P1 rows DONE, `jdbc:trino://127.0.0.1:8080/iceberg`
   proven end-to-end with zero env, Gate I3 SELECT 2 rows visible (A-100 / A-200),
   Gate I5 parity 4/4 row_count + md5 match. L3/L4 is where Iceberg table format
   actually earns its keep: BI/JDBC consumers, schema evolution across re-runs,
   snapshot history, partition pruning *against a conformed dimensional model*.
   Extending Iceberg to L2 after this completion is the **second-system effect**:
   uniformity applied to a layer that was intentionally NOT designed to be first-class.

4. **Developer exploration is already covered without L2 catalog or Iceberg.**
   Jupyter + the existing public API (2 imports + 2 builder lines → `l2.read(...)`)
   gives developers *strictly more* exploration power than a JDBC L2 BI client would
   (full DataFrame API, inline CTE drafting, quality hook validation BEFORE running
   the pipeline). This closes the only human-use-case that didn't already have a
   solution, and it does so with ZERO infrastructure — no catalog, no DDL, no IAM.

5. **Open Decisions OD-L2-1 / OD-L2-2 have no good answers for L2.**
   - **OD-L2-1 (append vs overwrite):** Either choice is worse than the status quo
     `run_id=*` immutable directories. Append → snapshots accumulate; reruns need
     `VERSION AS OF` which the L3 executor doesn't know about yet (L2 locator only
     reads paths today). Overwrite → lose historical run data unless you snapshot
     before overwrite; now you're forcing L2 into a provenance system it doesn't
     want to be. The `run_id=*` directory convention gives you both: immutability
     + natural provenance + zero snapshot resolution logic. It is already correct.
   - **OD-L2-2 (provenance without run-id dirs):** Requires stitching snapshot IDs
     → audit log entries → run IDs, which is a brand-new subsystem. Current system
     doesn't need it: directory name IS the run_id. This is "solving a problem the
     migration itself creates".

### If you think you need to reopen this backlog anyway:

Before touching any code in the 4 gates:
1. **First, implement the Camelot/Glue Hive overlay (100 LOC registrar).** Run it in
   dev for 2 sprints against the real pain (scale or consumer).
2. **Re-assess after 2 sprints.** If the Hive overlay actually solved the pain (it
   will), close this file permanently. If it *genuinely did not*, re-open this
   backlog — and be prepared to describe in writing exactly what the overlay could
   not do that Iceberg table format can. We expect this step never happens.

### What to do with the gated plan, open decisions, DoD preserved below:

Leave the gates L2-1 through L2-4, OD-L2-1/2, and Definition of Done intact. They
are technically correct for an L2 Iceberg migration — we just shouldn't do one.
If (against expectation) the Hive overlay fails and we reopen, the gates are the
right implementation plan and no re-planning is required.

---

---

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

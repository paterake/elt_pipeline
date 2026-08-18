# Backlog & Continuity — Test-Gate Recovery

<!--
  ANCHOR DOC. This file is the durable, cold-start-resumable state for the test-gate
  recovery backlog. It adopts the anchor-doc continuity contract shape (Resume / Session
  start prompt / Done / Still Todo / Accumulated Active Constraints / Verification / Gotchas)
  used by the elt_lake PCO, WITHOUT importing the PCO scaffolding.
  Reference (not vendored): elt_lake/ai_context/pco/governance/AI_ASSISTANT_BACKLOG_CONTINUITY.md
  Operating model: ONE session per backlog item. Update the Resume line before you close a session.
-->

## Resume (start here)

- From `BACKLOG.md`: Continue **P1-8** (`sql run` / `publish run` CLI subprocesses exit 1) — re-verify after P0; the 6 residual `test_cli.py` failures (`test_sql_run_command*`, `test_end_to_end…`, `test_normalize_run_command_supports_bypass_level2_mode`) are Spark/warehouse-shared-session (S-0-class) errors, not config drift. Verify with `ELT_PIPELINE_TEST_SPARK_ICEBERG=0 uv run pytest tests/test_cli.py tests/test_publish_cli.py -q`. Then **P2-9** (which also unblocks `test_examples::test_schedule_example_runs_after_placeholder_resolution` — see its note). Workable cold; does **not** need S-0.
- **P1-7 DONE** (schedule mechanism): root cause was **not** schema drift — three real code bugs (Path→str plan_path; in-process `main()` re-entry vs once-per-process singleton; `initialize()` outside the try/except). Fixed. `uv run pytest tests/test_cli.py -k schedule -q` → **2 passed**. See Done.
- **S-0 remains BLOCKED** and gates only the single-command *full-suite* green — the shared-JVM `uv run pytest` count is dominated by S-0 contamination (dead / wrong-mode sessions), not real failures. Don't chase the full-suite number until S-0 is decided; verify per-file.
- **Done:** P0-1, P0-2 (+session.py URI bug), P0-3, P0-4, P1-5, **P1-6**, **P1-7**, **S-1**, **S-2**, P2-10. **Per-file verified:** `test_normalize_pipeline` 9/9; `test_iceberg_catalog_config` 34/34; `test_sql_models` **25/25** (iceberg-off); `test_staging_swap` 26/26; `test_iceberg_parity_and_audit` **25/25**; `test_cli -k schedule` **2/2**.

## Session start prompt

Paste this verbatim to boot a fresh session warm (no `use-context`/PCO skill exists in this repo, so this is the no-skill form):

> `from BACKLOG.md, continue`

The session reads the **Resume (start here)** line for the next item, and the **Environment & Verification** section for the JDK export and the `ELT_PIPELINE_TEST_SPARK_ICEBERG=0` knob before running anything. (If the cold session is a non–Claude-Code tool that doesn't auto-load `CLAUDE.md`, prepend `Read BACKLOG.md at the repo root, then …`.)

## Status snapshot

- **Gate:** 🟠 `uv run pytest` = **50 failed, 259 passed** (was 57/252). **This number is S-0-contaminated** — many failures are `AttributeError: 'NoneType' object has no attribute 'sc'` (a prior fixture's `spark.stop()` kills the shared JVM session) and wrong-mode session reuse, not real defects. Per-file runs are the real measure (see Resume). `ruff check` passes.
- **Captured:** 2026-08-18. P0-1..P0-4, P1-5, the session.py URI fix, `CLAUDE.md`, and this doc are committed in **`d8fb234` (elt104)**; the `ELT_PIPELINE_TEST_SPARK_ICEBERG` conftest knob landed in **elt105/elt106**; the **S-1** append-fixture fix in **elt107**. The **S-2** fix (multi-level dynamic partition overwrite in `_staging_swap.py` + regression tests) is committed on top as **elt108**. The **P1-6** fix (`_build_serving_endpoint` early-out guard) landed in **elt109**. The **P1-7** fix (three `cli.py` bugs: `path_normalize(str(plan_path))`, subprocess-per-job `_invoke_cli_job`, `runtime_context.initialize()` moved inside main's try/except; unused `io`/redirect imports removed) is in the working tree, uncommitted, at this session's end. Re-stamp whenever counts change.
- **Placement:** repo root, *not* under canonical `docs/`, per [PRD 10 §11](docs/prd/10-prd-architecture-and-lifecycle.md).
  The historical `docs/todo/` tree was deleted in elt99–elt103; this is its lightweight successor.

## Environment & Verification (run this first, every session)

The suite needs Temurin 23 on `PATH`/`JAVA_HOME`. A bare non-interactive shell does **not** inherit mise's activation, which adds spurious `JAVA_GATEWAY_EXITED` noise on top of the real failures. Export explicitly:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
uv run pytest -q            # baseline: 57 failed, 252 passed
```

With the JDK set there are **zero** Java-gateway failures. Per-item verification commands are inside each item below; "should pass" is not a check — run it and paste the count.

**Iceberg-mode test knob (until S-0):** the shared Spark fixture reads a test-only env var `ELT_PIPELINE_TEST_SPARK_ICEBERG` (default `1` = iceberg-on checkpoint). The L2/parity unit tests (`test_sql_models`, `test_normalize_pipeline`) need iceberg **off** — run them per-file with:

```bash
ELT_PIPELINE_TEST_SPARK_ICEBERG=0 uv run pytest tests/test_sql_models.py -q
```

This is the clean way to work S-1/S-2 in a cold session without editing `conftest.py`. It is a harness knob only — never read via `runtime_context`, never product config.

## Root-cause summary (3 overlapping causes)

The docs are coherent and the architecture *is* implemented. The gap: the **test suite (and some example fixtures) froze at elt60 (2026-08-13) while the code advanced through elt70–elt99**, and the Iceberg-on-by-default flip never reached the test harness. CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs `uv run pytest` on Java 23, so CI is red on this branch too.

| # | Cause | Blast radius |
|---|-------|--------------|
| A | **Iceberg opt-out default flip landed in code, not in the test harness.** The session fixture builds an Iceberg-enabled Spark session with **no warehouse dir** → `Cannot initialize HadoopCatalog because warehousePath must not be null or empty`. | most `test_sql_models`, `test_publish_models`, `test_publish_cli`, `test_cli` |
| B | **Src/test API drift (elt60 tests vs elt90–99 code).** Signatures + error wording changed; tests never updated. | `test_sql_models`, `test_normalize_pipeline` |
| C | **No runtime-singleton reset between tests + repo-root `pipeline.yaml` bleed-through.** Cleared-env validation tests never raise their expected errors. | `test_iceberg_catalog_config` (11) |

## Accumulated Active Constraints (honour in every item; append, never delete)

1. **Run the gate with the JDK exported** (see Environment above). A run without it is invalid, not a failure.
2. **No backlog/tracker files inside canonical `docs/`** (PRD 10 §11). This anchor doc lives at repo root.
3. **Storage-format contract (confirmed by owner 2026-08-18):** L1 = raw source files; **L2 = plain parquet** (normalised/relationalised from L1, no catalog); **L3/L4 = Iceberg table format + data catalog + Trino JDBC access**. L2/normalize tests assert parquet paths and want Iceberg **off**; L3/L4 table tests want Iceberg **on**. (`test_sql_models` currently exercises the L3/L4 **parquet-parity** escape-hatch path, not the Iceberg default.)
4. **One JVM = one SparkSession (S-0).** `build_spark_session().getOrCreate()` returns the first-built session for the whole process. A test that needs a different session config must run in its own process, or it silently gets the wrong one. Do not add a test that rebuilds a differently-configured session in the shared JVM and inspects it.
5. **Per item, decide fix-code vs update-test explicitly.** Several P1 items encode PRD contracts; "just update the test" there would silently ratify a regression. Record the decision in that item's Done line.
6. **Green gate is done.** An item is not Done until its Verification command is re-run and the pasted count reflects it, and the Status snapshot is re-stamped. Note "isolated file passes" ≠ "full suite passes" until S-0 is resolved.
7. **Writer and serving Iceberg catalogs are separate** (PRD 10 §7). Writer config never falls back to serving config for URIs (fixed in P0-2).

---

## Work items

Ordered. Each carries: symptom → evidence → cause → **decision** → files → **Verification**. Move a closed item's next-step out of "Still Todo" and update the Resume line + Status snapshot before ending the session.

### Still Todo

#### S-0 — Spark session isolation across tests  🚫 BLOCKED (decision needed)
- **Blocker (one sentence):** the whole suite shares ONE JVM SparkSession, but tests legitimately need different session configs (Iceberg **on** for L3/L4 table tests; **off** for L2/parity unit tests; freshly-built sessions for `test_iceberg_catalog_config`'s config-inspection) — pick the per-file process-isolation mechanism, because no single shared fixture can satisfy all three.
- **Proof:** `test_iceberg_catalog_config` passes **34/34 in isolation** but fails in the full suite (`Using an existing Spark session; only runtime SQL configurations will take effect`); flipping the shared fixture between Iceberg on/off only trades `test_sql_models`/`test_publish_models` failures for `test_sql_iceberg_write` failures (shared-JVM: iceberg-off → 57, iceberg-on → 50).
- **Second facet — cross-fixture teardown:** a module/own-session fixture (e.g. `test_sql_iceberg_write`) calls `spark.stop()`, which stops the **shared** JVM SparkContext for every test that runs after it → `AttributeError: 'NoneType' object has no attribute 'sc'`. Even `test_normalize_pipeline` (9/9 isolated) fails this way in-suite. Confirms per-file isolation is required; no in-suite fixture ordering fixes it.
- **Do NOT assume** one of these silently — list the options:
  1. **`pytest-forked`** — add to `dev` deps; each test (or file) forks a fresh JVM. Simplest; slower (JVM boot per fork). Changes the `uv run pytest` contract.
  2. **`pytest-xdist --dist loadfile -n>=<#files>`** — per-worker JVM, one file per worker. Needs enough workers to guarantee per-file isolation.
  3. **CI runs pytest per-file** (shell loop / matrix) — no new dep, but the single-command `uv run pytest` gate stays red; [LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md) would need to define the gate as per-file.
- **Recommended end-state (after the mechanism is chosen):** shared `spark_session` fixture = **Iceberg OFF** (correct for the many L2/parity unit tests); Iceberg tests keep their own iceberg-on fixtures (e.g. `test_sql_iceberg_write.py`); per-file isolation removes the cross-file JVM conflict; then P0-3/P0-4/P1-* fixes land the gate green.
- **Interim tree state:** shared fixture currently **Iceberg ON** (51/258, no regressions vs baseline) as a safe checkpoint. Flip to OFF only once isolation is in place, or `test_sql_iceberg_write` regresses in the single-command run.
- **Decision owner:** maintainer (dependency + release-gate contract change).
- **Verification (once chosen):** `uv run pytest -q` (or the chosen per-file command) → 0 session-contamination failures.

#### P1-8 — `sql run` / `publish run` CLI subprocesses exit 1  ⏳ (re-verify after P0)
- **Symptom:** `subprocess.CalledProcessError ... returned non-zero exit status 1`.
- **Evidence:** `test_cli::test_sql_run_command*` (×4), `test_publish_cli::*` (×6), `test_examples::test_sql_example_package_compile_and_run` / `..._publish_example_*`.
- **Cause:** expected **downstream of P0-1** (HadoopCatalog warehouse). A raw repro of `examples/sql/local_demo` returns `SQL_LEVEL2_SOURCE_NOT_FOUND` when L2 isn't seeded — confirm the tests seed L2 and that the only remaining failure is the warehouse one.
- **Decision:** **re-run after P0-1/P0-2**; file any residual real failures as new items.
- **Files:** [tests/test_cli.py](tests/test_cli.py), [tests/test_publish_cli.py](tests/test_publish_cli.py), [tests/test_examples.py](tests/test_examples.py).
- **Verification:** `uv run pytest tests/test_cli.py tests/test_publish_cli.py -q`

#### P2-9 — Example package model count drifted (test expects 2, package has 5)  ⏳
- **Symptom:** `assert 5 == 2` (`compile_payload["model_count"] == 2`).
- **Evidence:** `test_examples::test_sql_example_package_compile_and_run` (~line 186).
- **Cause:** [examples/](examples/) SQL package grew from 2 → 5 models; assertion not updated. The 3 added models are `level3.sales.canonical_orders`, `level3.sales.orders_ingest_snapshot`, `level3.inventory.canonical_shipments`. The first two are orders-backed (runnable); **`canonical_shipments` references a `shipments` source that does not exist in the demo** (config [local_object_storage_orders.yaml](examples/configs/local_object_storage_orders.yaml) defines only `orders`; no `examples/data/object_storage/shipments.json`).
- **Also blocks (P1-7 coupling):** `test_examples::test_schedule_example_runs_after_placeholder_resolution` — the demo schedule's `sql-run` job builds the whole package and dies on `canonical_shipments` (`SQL_LEVEL2_SOURCE_NOT_FOUND`, entity `shipments`). Same root cause; fixing P2-9 fixes both.
- **Decision needed (owner):** the added models carry deliberate teaching comments (late-arrival / snapshot patterns), so the growth looks intentional — but `canonical_shipments` is un-runnable as shipped. Pick one: **(A) complete the demo** — add `shipments.json` + a `shipments` entity + schedule ingest/normalize jobs, bump `model_count` 2→5 and schedule `executed_count` 5→7; **(B) trim** the 3 drifted models back to the 2-model orders lineage; or **(C) keep 5 but scope** the demo compile/run to `--domain sales` and update `model_count` to the 4 sales models. A/C change authored example content in opposite directions — confirm intent before editing.
- **Verification:** `uv run pytest tests/test_examples.py -q`

#### P2-10 — Error-message regex drift  ✅ RESOLVED (incidental)
- Was: `Failed: Regex pattern did not match` ×3 under the full suite. These passed once the shared session ran iceberg-off (the mismatched wording came from iceberg namespace-creation errors masking the intended message). No standalone change needed; re-file if they reappear once S-0 lands.

### Done

- **P1-7 — `elt schedule run` rejected its own plan (2026-08-18).** **Decision: fixed code** (three real product bugs, not schema drift — `scheduler.py` schema was never the problem). (1) [cli.py](src/elt_pipeline/cli.py) `_run_schedule_plan` passed `args.plan_path` (a `pathlib.Path`, from `type=Path`) into `path_normalize`, which now rejects non-`str` → `CONFIG_VALIDATION_FAILED` before any job ran. Fixed: `path_normalize(str(args.plan_path))`. (2) The scheduler invoked each job **in-process** via `main(argv)`, but `main()` calls the once-per-process `runtime_context.initialize()` (config-cascade singleton) — the second job (and, when the outer `schedule` command had already initialized, the first) tripped *"initialize() called a second time"*. Also violated the one-JVM-one-SparkSession contract (Constraint 4). Fixed: `_invoke_cli_job` now spawns each job as a **subprocess** (`sys.executable -m elt_pipeline <argv>`), giving true per-job config + Spark isolation — matches the command's own help ("calling existing CLI commands"). Removed now-unused `io` / `redirect_stdout` / `redirect_stderr` imports. (3) `runtime_context.initialize()` sat **outside** `main()`'s try/except, so a bad `--config-path` (missing file / invalid schema) escaped as a raw traceback + exit 1 instead of a structured `CONFIG_VALIDATION_FAILED` exit 2 — because materialization runs the config cascade eagerly. Fixed: moved `initialize()` inside the try. **Verification:** `uv run pytest tests/test_cli.py -k schedule -q` → **2 passed**; `ruff check src/elt_pipeline/cli.py` clean. Full-file (iceberg-off) `test_cli.py` → 11 passed / 6 failed, and the 6 are the pre-existing **P1-8** Spark/warehouse failures (unchanged by this fix). **Note — the third fix is a broad improvement:** any command with a bad `--config-path` now emits structured exit-2 instead of a traceback. **P2-9 coupling:** `test_examples::test_schedule_example_runs_after_placeholder_resolution` now exercises the fixed mechanism correctly (4/5 jobs green) but its 5th job `sql-run` fails `SQL_LEVEL2_SOURCE_NOT_FOUND` for entity `shipments` — the example SQL package's `level3/inventory/canonical_shipments` model has **no backing source** in the demo (config defines only `orders`; no `shipments.json`). That is the P2-9 example-surface drift, not a schedule defect — resolve it there.
- **P1-6 — `serving_endpoint` returned a dict when Iceberg is disabled (2026-08-18).** **Contract decision (confirmed): serving endpoint is `None` when Iceberg is off; decision = fixed code.** Serving is an Iceberg-bound spoke — [PRD 10](docs/prd/10-prd-architecture-and-lifecycle.md) §8 / lines 73 & 112 define `serving_endpoint` as the `jdbc:trino://` URL for the associated **L3/L4 Iceberg** tables. With Iceberg explicitly disabled (plain-parquet escape hatch) there is no catalog and no Trino serving spoke, so there is no endpoint. Root cause in [_build_serving_endpoint](src/elt_pipeline/cli.py) (`cli.py`): the early-out guard was `if enabled is None: return None`, but `_iceberg_effective_enabled` returns `False` (not `None`) when explicitly disabled, so the builder fell through and built a full descriptor. Fix: guard is now `if not enabled: return None` (covers both explicit-`False` and no-tier-matched-`None`). Return type was already `dict | None` and all callers (audit records at cli.py:2005/2268) already threaded the optional value, so no caller change needed. Verification: `uv run pytest tests/test_iceberg_parity_and_audit.py -q` → **25 passed**; `ruff check src/elt_pipeline/cli.py` clean.
- **S-2 — multi-level dynamic partition overwrite wiped sibling partitions (2026-08-18).** **Decision: fixed code** (real product bug, not test drift). The L3 default layout is **two-level** `source_name=<src>/business_date=<date>`, but the parquet staging-swap (`_atomic_swap_posix`) only handled **one** partition level: it `rmtree`d the whole target `source_name=<src>/` dir and moved staging's in, destroying unrelated `business_date` leaves (e.g. the pre-seeded `2026-06-01`). This violated the PRD dynamic-partition-overwrite contract ([PRD 03](docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md): replace only the `(source_name, business_date)` tuples in the new data). Fix ([_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py)): new `_swap_partition_tree_posix` recurses through nested `key=value` dirs and replaces only **leaf** partitions; removed the now-dead `_has_non_dir_files_posix`. **Same-class bug in the S3 path fixed too** — `_s3_infer_partition_subprefixes` only inspected the first segment, so it deleted the whole first-level target prefix; now returns full leaf `k=v/.../` subprefixes. Added multi-level regression tests (POSIX + S3) to [test_staging_swap.py](tests/test_staging_swap.py). Verification: `ELT_PIPELINE_TEST_SPARK_ICEBERG=0 pytest tests/test_sql_models.py -q` → **25 passed**; `pytest tests/test_staging_swap.py -q` → **26 passed**; `ruff` clean.
- **S-1 — `appended_orders` append-mode execution (2026-08-18).** Root cause was **not** an append-path defect — it was a stale (elt60) test fixture. The model's SELECT (`order_id, amount, order_date`) produced neither L3 default-partition column, so `.partitionBy("source_name","business_date")` raised `AnalysisException: Partition column 'source_name' not found in schema`. Per [PRD 03 FR4.1](docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md): L3 default partitions are `["source_name","business_date"]`, applied to **all three** load modes (incl. append), and a SELECT that omits them **is meant to fail at write time** (that write-time error is the PRD's stated enforcement mechanism — no extra validation layer). **Decision: update the test** (fixture was pre-convention; code matches the PRD). Made `_write_append_sql_package` conforming: SELECT `order_id, amount, source_name, business_date`, filter on `business_date`; seed rows carry `business_date`; read-back asserts on `str(business_date)` (partition path round-trips it back as a `date`). Verification: `ELT_PIPELINE_TEST_SPARK_ICEBERG=0 uv run pytest tests/test_sql_models.py -k append -q` → **1 passed**; full file **24 passed, 1 failed** (the 1 is S-2); `ruff check` clean.
- **P0-1 — shared Spark fixture warehouse/mode (2026-08-18).** [tests/conftest.py](tests/conftest.py) `spark_session` now builds with an explicit mode + session-scoped `iceberg_warehouse_dir` (via `tmp_path_factory`), fixing `Cannot initialize HadoopCatalog because warehousePath must not be null or empty`. **Decision:** fixed the harness (not product). **Caveat:** the on-vs-off choice is entangled with **S-0** (single JVM) — currently left **Iceberg ON** as a no-regression checkpoint; the correct end-state (Iceberg OFF shared + per-file isolation) is blocked on S-0. Verification: `uv run pytest tests/test_publish_models.py -q` (isolated) 7→ improved; full suite 57→51.
- **P0-4 — writer API rename `write_table`→`write_dataframe` (2026-08-18).** The writer also changed input type (`NormalizedTable(rows=…)` → Spark `DataFrame`). Rewrote the two safety-net tests in [tests/test_normalize_pipeline.py](tests/test_normalize_pipeline.py) to build a DataFrame via `createDataFrame` and call `write_dataframe(table_name=…, dataframe=…)`; dropped the now-unused `NormalizedTable` import. **Decision:** update tests (API drift). Verification: `uv run pytest tests/test_normalize_pipeline.py -q` → **9 passed**.
- **P1-5 — L2 `_run_id` lineage (2026-08-18).** **Decision: fixed code** (not test). The normalize pipeline now stamps `_run_id = manifest.run_id` (the L1 ingest run) on each dataframe before writing ([pipeline.py](src/elt_pipeline/normalize/pipeline.py)); the writer's `run_context.run_id` default remains a safety net (proved intended by the "does-not-overwrite-existing-`_run_id`" test). L2 partition dir still records the normalize run. Honours replayability (PRD 00 §7). Verification: `uv run pytest tests/test_normalize_pipeline.py -q` → **9 passed**.
- **P0-3 — `SparkSqlModelExecutor(run_id=…)` kwarg (2026-08-18).** Added `run_id="test-run"` to all 7 executor constructions in [tests/test_sql_models.py](tests/test_sql_models.py) (targeted, not blanket — `environment="dev"` appears in non-executor calls too). **Decision:** update tests (API drift). Verification (iceberg-off, temp-flip): `test_sql_models` 7→**23 passed, 2 failed**; the 2 remaining are genuine functional bugs, now tracked as **S-1** and **S-2** (not drift). Fully green once S-0 lets this file run iceberg-off.
- **P2-10 — regex drift: RESOLVED incidentally** (see Still Todo note).
- **P0-2 — runtime-singleton isolation + writer/serving URI bug (2026-08-18).** Added autouse `_reset_runtime_singleton` fixture in [tests/conftest.py](tests/conftest.py) calling `runtime_context._reset_for_tests()` before/after each test. **Uncovered + fixed a real product bug:** the **writer** catalog URI resolution fell back to the **serving** catalog URI ([session.py](src/elt_pipeline/spark/session.py#L251)), so a `rest`/`jdbc`/`nessie` writer catalog silently inherited the serving sqlite URI instead of raising `requires iceberg_catalog_uri`. Introduced at elt99; now writer URI resolves from writer config only. **Decision:** fix code (the fallback was a regression, not intended behaviour). Verification: `uv run pytest tests/test_iceberg_catalog_config.py -q` → **34 passed** (isolated; in-suite failures are S-0 contamination, not this bug).

## Gotchas (things a fresh session would otherwise re-learn)

- `timeout` is not installed on this macOS; don't wrap commands in it.
- A bare `uv run pytest` (mise not activated) fails Spark tests with `JAVA_GATEWAY_EXITED` — that is env, not a code defect. Always export `JAVA_HOME`/`PATH` first (see Environment).
- Running **multiple pytest/Spark suites concurrently** causes `Using an existing Spark session` and port contention → spurious extra failures. Run one suite at a time.
- Full suite takes ~2 minutes; a single Spark test ~6–10s (JVM boot dominated).
- Iceberg HadoopCatalog needs a non-empty warehouse dir; the shared `spark_session` fixture now supplies a session-scoped tmp one (P0-1 done).
- **Isolated-file pass ≠ full-suite pass.** `test_iceberg_catalog_config` is 34/34 alone but fails 5 in the full run because another file already built the JVM's SparkSession. Always confirm a fix both ways. This is the S-0 blocker.
- **A fixture calling `spark.stop()` kills the shared JVM session for all later tests** (`'NoneType' object has no attribute 'sc'`). In a shared JVM there is only one SparkContext; `test_sql_iceberg_write`'s own-fixture teardown stops it. Verify normalize/sql fixes **per-file**, not in the full suite, until S-0.
- `_reset_for_tests()` alone does not stop `runtime_context.get()` from re-materializing from repo-root `pipeline.yaml` (fixed `repo_root = Path(__file__).parents[3]`, cwd-independent). The empty `iceberg_serving.catalog_uri` there auto-derives to a truthy `jdbc:sqlite:…` path — which is exactly what leaked into writer-URI resolution (P0-2).

## Continuity — what IS verified good (do not re-litigate)

- Canonical docs are internally consistent; [PRD 10](docs/prd/10-prd-architecture-and-lifecycle.md) contracts exist in code: four-phase CLI, four-tier SQL validity chain (token/partition/`EXPLAIN FORMATTED`/quality hooks), staging-swap (`_NO_STAGING_MOVE`), `mapping_version = sha256(...)[:16]`, six-way catalog enum, `_uc_cat()`+`exit 11` (in [ops/trino_serving/run_trino.sh](ops/trino_serving/run_trino.sh)), `serving_endpoint` in publish audit.
- 252 tests pass: config loader, path utils, connectors (kafka/rest/object-storage/sql), merge-SQL generator, staging-swap, runtime, lineage/quality adapters.
- JDK toolchain works (Temurin 23 via mise); "Unable to locate a Java Runtime" is purely a non-activated-shell PATH artifact.
</content>

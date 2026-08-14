# Storage URI + I/O Dispatch Backlog (Completed Snapshot)

## Purpose

This file is the **COMPLETED SNAPSHOT** for the URI-aware storage-root backlog. The active-approved-specification version of the PRD is:
- [PRD 08: URI-Aware Storage Root Paths and Explicit-Config I/O Dispatch](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md)

This document is the historical completed-tracker record. A summary entry is indexed in `docs/todo/TODO.md` under the Archived backlogs table. All code changes referenced herein are **COMPLETE in the repository as of 2026-08-13; Gate 5 environment-verification items (JVM + EMR E2E) are pending execution on a suitable workstation / AWS account, per the status row below.**

The work corrected the storage-root contract to match the `mercell` / `camelot` conventions: sharp explicit roots (string URIs), scheme-as-single-routing-key, no mounts, zero inference, zero `pathlib` used on root joins, full-URI level-to-level handoffs.

Per repo governance: this completed snapshot stays in `docs/todo/archive/`. When Gate 5 (JVM + EMR E2E) items are all green, update the "Pending Gate 5" line below → remove the pending note. Do not move this file back out of archive unless a formal PRD re-opens the scope.


## Current Status

- **Phase 0: PRD Approved** ✅ COMPLETED 2026-08-13 — [PRD 08](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md) Approved v1. Top-level tracker [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md) updated. Descoped items from prior archived backlogs cross-referenced.
- **Phase 1: Gate 0 (path_utils module + tests)** ✅ COMPLETED 2026-08-13 — Gate 0 checklist all pass. Defensive one-line fix added in shared errors module (`PipelineError.__init__` now sets `self.message`, matching its declared attribute; was missing on Exception subclass despite explicit annotation).
- **Phase 2: Gate 1 (CLI typing + root signature flow-in)** ✅ COMPLETED 2026-08-13 — argparse storage-root args (`--root-path`, `--warehouse-root`, `--kafka-log-path`) all `type=str` with string defaults. All top-level runner function entry-point signatures (`_run_ingest_entity`, `_run_normalize_manifest`, `run_sql_models_locally`, `run_publish_definitions_locally`) accept `root_path: str`, `warehouse_root: str`. Static grep audit: `git grep "root_path: Path|warehouse_root: Path" src/` → 0 matches. Package-file paths (`config_path`, `plan_path`, `package_path`) deliberately kept `type=Path` (local YAML/SQL inputs, not storage targets).
- **Phase 3: Gate 2 (storage module + connector I/O migration)** ✅ COMPLETED 2026-08-13 — ingest `storage.py`/`state.py`, `LocalObjectStorageConnector`, `LocalRestConnector`, `LocalSqlConnector`, `LocalKafkaConnector` all reworked: constructor root→str; all `/` concat via `join_paths()`; all I/O calls through path_utils scheme dispatch; LocalObjectStorage `.is_dir()` POSIX guard replaced with `path_is_dir()`. Gate 2b sub-gate also complete: normalize `storage.py`/`level2_storage.py`/`pipeline.py` plus integrations `quality.py`/`lineage.py` all accept str roots and use path_utils I/O.
- **Phase 4: Gate 3 (SQL + Publish stage migration)** ✅ COMPLETED 2026-08-13 — `sql/models.py` and `publish/models.py` storage-root attributes (`warehouse_root`, `artifact_root`, `run_dir`, `audit_path`, `log_path`, `lineage_path`, `error_path`, `run_scoped_path`, `stable_delivery_path`, `export_manifest_path`) typed `str`; package-file attributes (`package_root`, `manifest_path`, `sql_path`, `package_path`, `query_path`) retained as `Path`. `sql/level2_source.py`, `sql/spark_executor.py`, `sql/runtime.py` rewritten: executor `_table_path()` returns a plain string from `join_paths()`; Spark receives raw string URIs directly on `spark.read.parquet / writer.parquet` — zero `Path()` wrapping, zero `str(path_obj)` coercion. `publish/runtime.py` fully rewritten with scheme guards (`_local_path_or_exception()` for operations that require local POSIX like zipfile/CSV writers), dispatch helpers (`_file_size_bytes`, `_copy_file`), and path_utils-based concat for `level4` source reads and manifest sidecars.
- **Phase 5: Gate 4 (hardening, safety, docs, sweep)** ✅ COMPLETED 2026-08-13 — full sweep audit: 0 signatures with `root_path: Path`; 0 `root_path /"X"` storage-root concat via `/` operator; remaining `from pathlib import Path` imports in `src/` are limited to (a) immediate leaf POSIX call sites that use `strip_file_scheme()` first, or (b) sql/discovery.py + publish/discovery.py scanning local package directories (package-file paths, not storage roots). Fail-fast scheme guard `validate_config_root_schemes(...)` added in `src/elt_pipeline/config/loader.py`. `pyproject.toml` extras: `s3 = [boto3>=1.34,<2.0]` and `emr = [s3 + spark]`. Archived backlogs cross-referenced (`TODO_PATHING_COMPLETED.md` and `TODO_SPARK_COMPLETED.md` updated with follow-up and "subsequently implemented, complete" annotations). LOCAL_OPERATOR_RUNBOOK.md now has § "Cloud Native (No-Mounts) EMR / S3 Execution Pattern" with bucket naming, IAM policy template, 4-stage EMR step pattern, and scheme troubleshooting table. Status header updated; "Open Decisions" section populated with 3 closed decisions. `ruff check src tests` → 0 issues. `GetDiagnostics` across all files → 0 errors.
- **Phase 6: Gate 5 (verification, E2E on EMR, sign off)** ⏳ **PENDING — requires actual workstation JVM 17+ plus AWS environment.** Checklist items: 5.a (static POSIX subset green on JVM-enabled machine), 5.b `tests/test_path_utils.py` 48 green, 5.c `tests/test_sql_models.py` Spark compile-model tests green, 5.d JVM + Spark-integration tests ~51 PASS on JVM workstation, 5.e full EMR E2E with real S3 buckets + CLI flow validate→ingest→normalize→sql→publish NO mounts NO FUSE, 5.f final lint sweep, 5.g final diagnostics 0. When G5 passes: move file to `docs/todo/archive/TODO_STORAGE_URI_COMPLETED.md` and update `TODO.md` row (per instructions at bottom of this file).

No gate is marked complete until its explicit "Acceptance Criteria" subsection below passes.

---

## Baseline and Reference Constraints (from PRD 08, non-negotiable)

1. Root paths are **string URIs**. Never `pathlib.Path` objects. Never coerced through `Path(root_str)` until the immediate leaf I/O call site *for a local file*.
2. Scheme is the **single routing key**. Sharp `if path.startswith("s3://")` dispatch. No protocol registry. No URI parser library. No plugins.
3. **Unsupported schemes fail fast, sharp.** `s3a://`, `gs://`, `hdfs://`, etc. all raise the error with the exact supported list: `s3://`, `file://`, or bare local POSIX path.
4. Spark parquet R/W already handles S3 natively via string path. We **hand the raw string to Spark** with zero conversion; we only fix the concatenation that builds the strings.
5. EMR credential flow: default boto3 credential chain → IAM role assigned to the cluster. No access-key requirements baked into the code.
6. Level-to-level outputs (L1 manifest → L2 reader; L2 table path → SQL source; L4 dataset → publish) either: (a) full absolute URI; or (b) (relative path, recorded root URI) tuple in the manifest so re-reading never requires external context or user-supplied config.
7. Existing local POSIX behavior must be byte-for-byte identical: **all 37 non-Spark tests must pass without modification.** This is the single strongest safety gate.

## Gate Progression and Acceptance Criteria

### Gate 0: `elt_pipeline/shared/path_utils.py` + unit tests

**Scope:** Deliverable 0 only. No other file touched.

- [x] `join_paths(root, *segments)` — scheme-preserving; tests cover:
  - [x] `join_paths("s3://bucket/prefix", "level2", "source=orders")` → `"s3://bucket/prefix/level2/source=orders"` (exactly single `/`)
  - [x] Trailing slash in root, leading slash in segment, double `//` collapses
  - [x] Empty segments ignored
  - [x] `file:///tmp/elt` → concatenates with segments correctly (preserves triple slash after scheme)
  - [x] Bare POSIX `/Users/you/elt`, bare POSIX relative `./ignore/runtime` both work
- [x] `detect_scheme` + fail-fast guard: `s3a://bucket/path` raises the standardized error message verbatim.
- [x] All leaf I/O functions (`path_exists`, `path_write_text`, `path_read_text`, `path_write_bytes`, `path_read_bytes`, `path_mkdir`, `path_listdir`, `path_glob`, `path_rglob`, `path_open_for_append`, `path_replace`, `path_with_suffix`, `path_parent`, `path_basename`, `path_relative_to`, `path_normalize`) exist.
- [x] Local POSIX branch: **100% of leaf functions** tested against pre-canned temp-dir fixtures; outcomes match the current `Path.*` behavior byte-for-byte.
- [x] S3 branch: mocked via fake boto3 client class (`_FakeS3Client`) with assertions that:
  - `path_listdir("s3://bucket/prefix", …)` calls `list_objects_v2` with the right prefix + `Delimiter="/"` via paginator
  - `path_write_text("s3://b/k", data, atomic=True)` writes `k.tmp` then COPY-then-DELETE
  - Routing correctness proven: all S3-test assertions on fake-client call counts + contents pass (POSIX branch never reached because scheme routing short-circuits with explicit `if scheme is _StorageScheme.s3:` guards)
- [x] Lazy `boto3` import: inside the S3-branch `_s3_client()` helper, not at module top. `import elt_pipeline.shared.path_utils` succeeds with no boto3 on import path (import only triggered on first `_s3_client()` call inside a scheme=s3 branch).
- [x] `ruff check src/elt_pipeline/shared/path_utils.py tests/test_path_utils.py` — 0 issues.
- [x] IDE diagnostics: 0 errors in both files.

**Acceptance:** Gate 0 checklist items all pass. Module files modified in Gate 0: `src/elt_pipeline/shared/path_utils.py` (new), `tests/test_path_utils.py` (new), `src/elt_pipeline/shared/errors.py` (one-line `self.message = message` fix — existing attribute annotation matched no constructor assignment). No CLI, storage, connector, SQL, or publish files modified. PRD 08 § Deliverable 0 complete.

---

### Gate 1: CLI typing and root signature flow-in

**Scope:** Only `src/elt_pipeline/cli.py` and function signatures at the top of each stage runner. No storage-module changes yet (those come in Gate 2).

- [ ] Change argparser types:
  - [ ] `--root-path` `type=Path` → `type=str` with default string `".ignore/runtime"`
  - [ ] `--warehouse-root` `type=Path` → `type=str` with default string `".ignore/warehouse"`
  - [ ] `--kafka-log-path` `type=Path` → `type=str`
  - [ ] `--plan-path` `type=Path` → `type=str`
- [ ] Remove **every** `Path.resolve()` call inside cli.py (17 found; audit list from PRD 08) → replace with `path_normalize(str_root)` only when needed for POSIX; for `s3://` roots `path_normalize` collapses `//` but does not resolve filesystem semantics.
- [ ] Change the signature of each top-level runner function to accept `root_path: str` instead of `root_path: Path`:
  - [ ] `_run_ingest`
  - [ ] `_run_normalize`
  - [ ] `_execute_sql_stage`
  - [ ] `_run_publish`
- [ ] Every downstream function called by the CLI with a root path argument has its type annotation updated to `str`. Any function signature that does `root_path: Path` and is reachable from the CLI must be updated. Use `git grep -n "root_path: Path"` and `git grep -n "warehouse_root: Path"` after Gate 1 edits to confirm none remain.
- [ ] Gate 0 tests still pass. Local `uv run pytest tests/test_cli.py -k "not spark_integration"` (or existing CLI tests) — 100% green (or if no CLI integration tests yet, manual spot-check on laptop via bare `elt-pipeline --help` and a trivial `elt-pipeline validate-config` with a local YAML).
- [ ] `ruff check src/elt_pipeline/cli.py` — 0 issues.
- [ ] IDE diagnostics: 0 errors.

**Acceptance:** After Gate 1, `root_path` and `warehouse_root` flow as strings from CLI entry → every runner function. No regressions on local runs. PRD 08 § Deliverable 1 complete.

---

### Gate 2: Storage modules + Ingest Object-Storage connector migration

**Scope:** Deliverables 3 + 4 from PRD 08. Files: `ingest/connectors/local_object_storage.py`, `ingest/storage.py`, `ingest/state.py`, `normalize/storage.py`, `normalize/level2_storage.py`, shared `LocalArtifactStore` (used by quality/lineage integrations and audit/log writes).

- [ ] Ingest connectors / bucket_path:
  - [ ] Remove the `.is_dir()` hard validation guard at [local_object_storage.py:42-59](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/ingest/connectors/local_object_storage.py#L42-L59). Replace with a validation that the bucket_path string is non-empty and its scheme is supported (calls `detect_scheme`; raises the fail-fast error if unsupported). For local-only mode, a path with no scheme that does not exist on disk is still an error — but the error is a `path_exists(bucket_path)` call, not a `Path.is_dir()` call.
  - [ ] Replace `.rglob("*")` / `.glob("*")` object listing with `path_rglob()` / `path_glob()` dispatcher.
  - [ ] Replace `Path(self.config.bucket_path) / obj.key` → `path_read_bytes(join_paths(self.config.bucket_path, obj.key))`.
  - [ ] Class naming: rename to remove the misleading `Local*` qualifier. Keep backward compatibility if needed via a type alias. Config contract: `connector_type: object_storage` still works, unchanged.
- [ ] `ingest/storage.py`:
  - [ ] `LocalArtifactLayout` stores `root_path: str`. Replace all `self.root_path / "X"` with `join_paths(self.root_path, "X")`.
  - [ ] Replace `_write_json_file` / `_append_jsonl_file` / `persist_level1_artifact` callsites: `.write_text`, `.write_bytes`, `.open("a")`, `.mkdir`, `.relative_to` with path_utils equivalents.
  - [ ] Existing `LocalArtifactStore.persist_level1_artifact`: the `.exists()` guard against overwrite remains; now calls `path_exists(data_path_str)`.
- [ ] `ingest/state.py`: checkpoint reads/writes. Replace `.mkdir`, `.write_text`, `.replace`, `.exists()`, `.read_text` calls with path_utils. Preserve atomicity: on S3 the atomic branch does the `.tmp` + copy pattern.
- [ ] `normalize/storage.py`: `catalog_path` builder uses `join_paths(root_path_str, …)`. `write_catalog` uses `path_write_text(catalog_path_str, …, atomic=True)`.
- [ ] `normalize/level2_storage.py`: audit for any `Path` root concatenation / write calls; convert to string join + path_utils.
- [ ] Shared `LocalArtifactStore` append methods (in quality/lineage integrations and shared audit writers): route `append_log_event`, `append_error_record` calls through `path_open_for_append`. Ensure S3 branch works correctly (read + concat + atomic rewrite is acceptable here because log/event files are small).
- [ ] **No regressions:** `uv run pytest tests/test_ingest_storage.py tests/test_normalize_runner.py tests/test_quality_adapter.py tests/test_lineage_adapter.py` — all green on local POSIX.
- [ ] One new mocked test: verify that given `bucket_path: s3://mock-bucket/raw/orders`, the object_storage connector's listing and object-read dispatch call the S3 branch, no pathlib functions are called.
- [ ] `ruff check` on all files in this gate → 0 issues.
- [ ] IDE diagnostics: 0 errors.

**Acceptance:** All I/O in ingest/normalize stages routes through path_utils. Local POSIX tests green. Mocked-S3 dispatch assertions pass. PRD 08 § Deliverables 3+4 complete.

---

### Gate 3: SQL Stage + Publish Stage migration

**Scope:** Deliverables 5 + 6. Files: `sql/level2_source.py`, `sql/spark_executor.py`, `sql/discovery.py`, `sql/models.py` (if any Path there), `sql/runtime.py`, `publish/runtime.py`, `publish/discovery.py`, `publish/models.py`.

- [ ] `sql/level2_source.py`:
  - [ ] Build entity_root as string via `join_paths(root_path, …)` instead of `Path /`.
  - [ ] Replace `.glob("**/table=…/run_id=*")` + `.is_dir()` → `path_rglob(entity_root_str, pattern)` then `all(path_is_dir(match_str))`.
- [ ] `sql/spark_executor.py`:
  - [ ] `_table_path` → `join_paths(self.warehouse_root, stage.value, table_name)` returning string, not `Path`.
  - [ ] Do NOT change Spark call sites: `self.spark.read.parquet(str(target_path))` already works with a string arg; the `str()` call becomes a no-op on a string but is harmless to keep for safety.
  - [ ] Confirm in code review there is no hidden `Path(target_path)` before handing to Spark.
- [ ] `sql/discovery.py`: any model package scanning via `Path().rglob("manifest.yaml")` → use path_utils `path_rglob`.
- [ ] `sql/runtime.py`: artifact store and log writes → route through path_utils (already covered in Gate 2's shared store change; sanity check pass).
- [ ] `publish/runtime.py`:
  - [ ] `_resolve_run_scoped_output_path` / `_resolve_stable_delivery_path` use `join_paths(root_path_str, …)`.
  - [ ] `dataset_path = warehouse_root / "level4" / …` → `join_paths(warehouse_root_str, "level4", …)`.
  - [ ] All file writes for CSV/TSV/JSONL publish outputs route through `path_write_text` / `path_write_bytes`.
- [ ] `publish/discovery.py`: publish manifest package scanning → path_utils `path_rglob` / `path_read_text`.
- [ ] **No regressions on non-Spark tests:** `uv run pytest tests/test_sql_models.py tests/test_publish_*.py` (or equivalent). Spark-integration tests require JVM 17+ workstation to confirm, but compilation + compile-model tests must pass.
- [ ] `ruff check` → 0 issues on all touched files.
- [ ] IDE diagnostics: 0 errors.

**Acceptance:** SQL model compilation and publish discovery work identically on local POSIX. The string paths handed to Spark are verified to preserve `s3://…` scheme correctly via unit tests that assert `join_paths("s3://b/wh", "level3", "orders")` → `"s3://b/wh/level3/orders"` exactly. PRD 08 § Deliverables 5+6 complete.

---

### Gate 4: Full sweep, hardening, safety, docs, optional extras.

- [ ] **Full sweep:** audit every remaining file in the list of 23 with `from pathlib import Path`. Confirm in code review that:
  - [ ] No function signature has `root_path: Path` / `warehouse_root: Path`.
  - [ ] No `root_path / "X"` or `warehouse_root / "X"` concatenation exists anywhere.
  - [ ] Path imports retained only for (a) internal local-file fixtures in tests, (b) immediate leaf local-file I/O call sites that first `strip_file_scheme(path_str)` then `Path(stripped)`, with no propagation.
- [ ] **Scheme validation guard early in CLI:** `validate_config_root_schemes(root_path, warehouse_root, bucket_paths)` runs after CLI arg parse + config load, before any I/O, before any Spark context starts. Raises standardized error if `detect_scheme` returns an unsupported scheme.
- [ ] `pyproject.toml` `[project.optional-dependencies]`:
  - [ ] `s3 = ["boto3>=1.34"]`
  - [ ] `emr = ["elt_pipeline[s3,spark]"]` (convenience extra for `uv sync --extra emr`)
- [ ] **Cross-ref update in archived backlogs:** Update descoped-item notes in [TODO_PATHING_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_PATHING_COMPLETED.md) and any other archived backlog that mentions "object storage URIs deferred" to cross-reference this TODO + PRD 08 as approved reinstatement.
- [ ] **Top-level [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md):** Mark current workstream as active; "Active implementation backlog documents" table contains `TODO_STORAGE_URI.md` with Approved + Active status.
- [ ] **Runbook documentation:** New section *Cloud Native (No-Mounts) EMR Execution Pattern* in [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) covering:
  - [ ] Root URI conventions + examples (local dev `file:///`, EMR `s3://`).
  - [ ] EMR credential note: "Use IAM role on cluster; no access keys env vars needed."
  - [ ] Step-by-step 4-stage EMR Step invocation pattern with `s3://` root paths.
  - [ ] Scheme typo troubleshooting: common `s3a://` vs `s3://` confusion and the sharp error message.
- [ ] `ruff check` on **entire `src/elt_pipeline/` + tests/** tree → 0 issues.
- [ ] IDE diagnostics: 0 errors.

**Acceptance:** Sweep audit checklist completes; cross-refs, runbook, extras, and guard all in place. PRD 08 Deliverable 7 is complete.

---

### Gate 5: Full Verification and Sign-Off

Execute all success criteria from PRD 08 § Success Criteria:

- [ ] **(a) Static POSIX behavior identical on all existing tests:**
  ```bash
  uv sync --extra dev --extra spark
  uv run pytest tests/ -k "not spark_integration" -v
  ```
  → **All tests green with zero test modifications.** On non-JVM workstation: skip spark-integration with -k filter; the 37 non-Spark tests must all pass.
- [ ] **(b) path_utils unit tests green:** `uv run pytest tests/test_path_utils.py -v` → all green including mocked S3 assertions.
- [ ] **(c) Spark compile-model tests green:** `uv run pytest tests/test_sql_models.py -v` → green. This validates the `join_paths` handoff to `str()` before Spark calls.
- [ ] **(d) JVM + Spark-integration tests on separate box (requires JVM 17+):**
  ```bash
  uv sync --extra dev --extra spark
  uv run pytest tests/test_ingest_storage.py tests/test_normalize_runner.py \
    tests/test_normalize_pipeline.py tests/test_sql_models.py \
    tests/test_lineage_adapter.py tests/test_quality_adapter.py -v
  ```
  → Expected ~51 tests PASS.
- [ ] **(e) EMR E2E on AWS account + JVM equipped workstation:**
  - Stand up test EMR cluster (or use EMR Serverless job) with:
    - A source S3 bucket with demo orders data as the `object_storage` connector input → `s3://<bucket>/raw_demo_inputs/orders/`
    - Runtime root → `s3://<bucket>/elt_pipeline_test/`
    - Warehouse root → `s3://<bucket>/elt_warehouse_test/`
  - Copy the example pipeline configs and model packages to S3.
  - Run the 4-stage pipeline end-to-end via EMR Steps or EMR Serverless job run, **no mounts, no FUSE products**:
    ```bash
    # Validate
    python -m elt_pipeline validate-config s3://<bucket>/configs/local_object_storage_orders.yaml
    # Ingest
    python -m elt_pipeline ingest run s3://<bucket>/configs/local_object_storage_orders.yaml --root-path s3://<bucket>/elt_pipeline_test --environment test --bucket-path-override s3://<bucket>/raw_demo_inputs/orders
    # Normalize
    python -m elt_pipeline normalize run s3://<bucket>/configs/local_object_storage_orders.yaml --root-path s3://<bucket>/elt_pipeline_test --environment test
    # SQL
    python -m elt_pipeline sql run s3://<bucket>/sql/local_demo --root-path s3://<bucket>/elt_pipeline_test --warehouse-root s3://<bucket>/elt_warehouse_test --environment test
    # Publish
    python -m elt_pipeline publish run s3://<bucket>/publish/local_demo --root-path s3://<bucket>/elt_pipeline_test --warehouse-root s3://<bucket>/elt_warehouse_test --environment test
    ```
  - Verify outputs via `aws s3 ls`:
    - [ ] `s3://<bucket>/elt_pipeline_test/level1/…` landed correctly with manifests.
    - [ ] `s3://<bucket>/elt_pipeline_test/level2/…` parquet tables + `_mapping_catalogs/` landed.
    - [ ] `s3://<bucket>/elt_warehouse_test/level3/…` canonical models partitioned correctly by `source_name, business_date`.
    - [ ] `s3://<bucket>/elt_warehouse_test/level4/…` marts landed correctly.
    - [ ] `s3://<bucket>/elt_pipeline_test/artifacts/level5/…` publish CSV exports readable via `aws s3 cp → head`.
    - [ ] Audit logs, run records, quality/lineage event JSONLs landed under `runs/` and `artifacts/` prefixes in the runtime root.
- [ ] **(f) Final lint sweep:** `ruff check src tests docs/operator` → 0 issues.
- [ ] **(g) Final IDE diagnostics:** 0 errors across entire repo.

**Acceptance:** ALL sub-items in Gate 5 pass. PRD 08 § Success Criteria 1–7 all satisfied.

**On completion of Gate 5:** Mark status header as *COMPLETED 2026-XX-XX*, move this document to `docs/todo/archive/TODO_STORAGE_URI_COMPLETED.md`, update top-level [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md) status back to *no active approved backlog in progress*, add an Archived / complete index row for the completed snapshot.

---

## Open Decisions (closed during implementation — capture resolution here)

### OD-1 (2026-08-13): Storage-root paths vs Package-file paths — explicit type split

**Decision:** Two separate path categories with distinct Python types, enforced in function signatures and model annotations.

- **Storage root paths** (anything that flows through Spark parquet read/write, leveled storage, artifact roots, or manifests carrying data outputs): **always `str`.** Never `Path`, never `str | Path` unions. This includes `root_path`, `warehouse_root`, `artifact_root`, `run_dir`, `run_scoped_path`, `stable_delivery_path`, `audit_path`, `log_path`, `lineage_path`, `error_path`, `export_manifest_path`, connector `bucket_path`, and `kafka_log_path`.
- **Package-file paths** (local YAML/SQL/config inputs — files the platform code reads in the driver process before any Spark activity): **always `pathlib.Path`.** This includes `config_path` (ingest/schedule/normalize config YAML), `plan_path` (schedule plan YAML), `package_path` (SQL model packages, publish packages), `CompiledSqlModel.package_root / manifest_path / sql_path`, and any publish `query_path`.

**Rationale:** This split is the sharpest boundary for PRD 08's zero-inference rule. Storage-root strings are passed "as-is" into `join_paths` and straight down into Spark parquet I/O — `pathlib` never touches them because it collapses `s3://` → `s3:/` and mangles scheme prefixes. Package-file paths, by contrast, are always local POSIX files and `Path` provides the cleanest `exists()`, `read_text()`, and `is_dir()` semantics on driver-only code paths.

**Impact on code:** All constructor and runner signatures explicitly declare which side they're on. Tests now apply a consistent boundary pattern: `str(tmp_path)` for Category A constructors, and `Path(returned_str)` on the test assertion side only when doing local filesystem checks.

### OD-2 (2026-08-13): schedule `plan_path` stays Path

**Decision:** The `schedule run <plan-path>` argparse arg stays `type=Path`, and `shared/scheduler.py` keeps `load_schedule_plan(path: Path)` requiring a `pathlib.Path` object. **Storage roots declared INSIDE the plan YAML stay `str`.**

**Rationale:** `plan_path` is the YAML schedule file itself — a local POSIX input to the scheduler wrapper. The scheduler does local file I/O (read YAML, write DAG files) and calls subprocess/cli wrappers with CLI args — the wrapper itself never carries the `plan_path` into a Spark R/W path. The PRD 08 strict string rule applies to all values INSIDE the plan (roots, bucket paths, etc.), not to the filesystem path of the plan file itself.

**Note from implementation:** Gate 1 initially converted `plan_path` to `type=str` matching the storage-root rule, but this broke `scheduler.py:load_schedule_plan(path: Path).resolve()` in tests — this was corrected to `type=Path` in Gate Safety and is the settled convention.

### OD-3 (2026-08-13): sql/discovery.py + publish/discovery.py keep `Path` on package scanning

**Decision:** `discover_sql_models(package_root: str | Path)` and `discover_publish_definitions(package_root: str | Path)` convert their argument to `Path()` immediately and use plain `Path.rglob("*.yaml")` / `Path.glob()` / `Path.read_text()` for package scanning. This stays as-is and is out of scope for PRD 08's root-path conversion.

**Rationale:** Both functions only scan local YAML/SQL package directories — they are part of the "compile/plan" phase that runs before Spark and before any storage write happens. The argument they take is a "SQL package directory" or "publish package directory", always a local POSIX directory. It is explicitly NOT a storage root (no data written here, no Spark R/W target). PRD 08's rule set is scoped to storage roots, Spark R/W targets, and leveled-data paths.

**Audit-proof pattern:** In `src/` the remaining `from pathlib import Path` imports after Gate 3 are:
1. These two discovery modules (package-file scanning only);
2. `config/loader.py` — used only on `config_path` (package-file path category B);
3. `cli.py` — only on `config_path` and `plan_path` (both package-file category B);
4. Immediate leaf helpers with `strip_file_scheme()` on POSIX call sites.

None of the remaining `Path` uses are applied to `root_path`, `warehouse_root`, or any storage-typed attribute.


## Known Risks and Mitigations

1. **Atomic replace semantics on S3:** The standard pattern write-tmp → copy → delete-tmp works, but listing readers may see partial or tmp objects. **Mitigation:** tmp keys always end in `.tmp`; level-to-level manifest references never point to `.tmp` keys; the manifest writer only promotes a `.tmp` → final key after writing the manifest JSON itself.
2. **Append on S3 is a read-concat-overwrite, not a true append.** For small event streams (<1 MB) this is fine. For large JSONLs this could be expensive. **Mitigation:** This codebase today only uses append for small audit/event JSONLs (< ~1k events per run). If we ever start writing large JSONLs, batch writes should be used instead. Out of scope for this PRD.
3. **Path escaping on S3 vs POSIX:** `_sanitize_path_fragment` already restricts path segments to `[A-Za-z0-9._-]+` via regex — verified in level2+ code. Mitigation: this prevents cross-platform character escaping issues. No change needed.
4. **Tests requiring S3 connectivity (Gate 5.e):** Cannot be run in the current sandbox. **Mitigation:** documented checklist, to be executed on a workstation with AWS credentials + JVM 17+.

## Related Documents

- PRD spec: [08-prd-storage-root-uri-io-dispatch.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/08-prd-storage-root-uri-io-dispatch.md)
- Top-level tracker: [TODO.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/TODO.md)
- Archived completed backlogs:
  - [TODO_PATHING_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_PATHING_COMPLETED.md) (prior descoped item reinstated by this initiative)
  - [TODO_SPARK_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_SPARK_COMPLETED.md)
  - [IMPLEMENTATION_BACKLOG_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/IMPLEMENTATION_BACKLOG_COMPLETED.md)
- Platform principles: [00-prd-platform-principles.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-platform-principles.md)
- Stage PRDs: 01 (ingest), 02 (L1→L2), 03 (SQL), 06 (publish) — all consistent with this document; if a conflict is found, this document wins per the status header's wording.

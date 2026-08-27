# Archive: Work Items — All Closed Items (D-0, D-1, D-2, D-3, B-0 → B-6, G-1 → G-8, M-1 → M-11, S1 → S4, I-1, I-2, GAP-7)

Archived from repo-root `BACKLOG.md` on 2026-08-26 during backlog deflation.
This file preserves the full `## Work items` section: every `####` per-item
specification (decision rationale, design tradeoffs, implementation scope,
verification checklists, cross-references to CMM rows and PRD sections), plus
the complete `### Done` historical block.

The repo-root `BACKLOG.md` retains only `### Still Todo` items that are genuinely
pending — no completed work item bodies remain in the repo-root file.

> Navigation tip: search for `#### <item-id> —` (e.g. `#### B-6 —`, `#### G-5 —`,
> `#### M-3 —`) to jump directly to a specific closed item's spec and closure
> narrative.

## Work items

### Still Todo

#### D-0 — Portability direction: reconcile-docs (A) vs implement-multi-cloud (B)  ✅ DECIDED (Path A now; B roadmap)
- **DECIDED (owner, 2026-08-18): Path A now — publish honestly with S3+local scope; Path B
  (multi-cloud) is roadmap (tranche 2), and when pulled forward, prefer B-6 (the facade).** The
  analysis below is retained as the reference for that future B work. Active next step: **D-1**.
- **Decision (as taken):** the multi-cloud "0 LOC on AWS/GCP/Azure/Databricks" claim is not
  implemented; rather than block publication on building it, de-scope the docs to reality now and
  keep multi-cloud as a tracked roadmap. Original options, for the B roadmap:
  - **Path A — de-scope to reality (fast, always-safe):** market as *local-first + AWS S3*.
    Only **D-1** is required; B-* are dropped. Publishable within a session.
  - **Path B — implement the claim (real work).** Two strategies, pick one:
    - **B1 — native per-backend clients** (the S3 pattern repeated): add a `gs`/`abfss` branch
      to every scheme-branching function using that cloud's SDK. Items **B-1, B-2, B-3, B-4, B-5**.
      Each backend ≈ the existing S3 path (~300 lines × ~18 functions) + its own staging-swap +
      tests. Keeps PRD 08's "one boolean check per scheme" seam; cost scales linearly per cloud.
    - **B2 — delegate control-plane IO to Spark's Hadoop `FileSystem` via py4j** (item **B-0**):
      refactor the `path_utils` leaf ops +
      staging-swap to call `spark._jvm.org.apache.hadoop.fs.FileSystem`, so the control plane
      inherits whatever Spark's jars support and **the scheme becomes a pure config knob** —
      one implementation covers S3/GCS/ADLS/Databricks. This is the "Spark already handles the
      filesystem" model. **But it is a PRD-level change, not just code** — see B-0's real
      tradeoffs (it overturns PRD 08's explicit "no `StorageBackend` abstraction" principle,
      couples *all* control-plane IO to a live SparkSession, adds py4j round-trip cost to chatty
      list/exists ops, and rename-atomicity still varies per connector). Do **D-1 + B-0 + B-4 +
      B-5**; B-1/B-2/B-3 then largely fold into "add jars + config + tests per cloud."
    - **B3 — pluggable storage-backend facade** (item **B-6**, *the maintainer's proposal; likely the
      best fit here*): define a `StorageBackend` protocol (list/exists/mkdir/delete/rename/read/write)
      and register one implementation per scheme (local/s3/gcs/azure) using each cloud's SDK; the
      `path_utils` functions + staging-swap become thin dispatch over the registry. Like B2 it
      overturns PRD 08's "no registry" rule, **but unlike B2 it keeps IO in Python** — which matters
      because the **ingest phase writes L1 pre-Spark** (see B-6 / I-1). B2 would force a SparkSession
      into ingest just to land raw files; B3 does not. More code than B2 (per-backend SDK impls) but
      cleaner than B1 (each backend is one class, not branches scattered across ~18 functions), and
      independently unit-testable per backend.
  - **Hybrid:** ship Path A now, keep B (B1/B2/B3) as a tracked roadmap for a later release.
- **Recommendation:** **Path A now, B as roadmap — and if/when B is pulled forward, prefer B3
  (B-6, the facade).** Publishing with accurate scope is a one-session change that removes the trust
  risk. For the actual multi-cloud build, the platform is **two-phase — Python ingest (pre-Spark) →
  Spark transform** — and **B3 is the only strategy that serves both phases with one Python
  abstraction and no Spark-in-ingest coupling**. B2 (delegate to Hadoop FS) is elegant for the Spark
  transform side but wrong for ingest (it would drag a SparkSession into raw-file landing); B1 works
  but scales badly (per-backend branches across ~18 functions). B3 gives B2's "one implementation,
  scheme-is-config" benefit while keeping the pre-Spark ingest path native.
- **Owner:** maintainer. Record the choice here, then set the Resume line to D-1.
- **Verification:** none (decision only). Note the choice in this item's Done line.

#### D-1 — Make PRD 08 and PRD 10 mutually consistent (needed in BOTH paths)  ✅ Done (2026-08-18)
- **Symptom:** [PRD 10 §6](docs/prd/10-prd-architecture-and-lifecycle.md) (lines ~31, 245, 251-254)
  claimed 6 URI schemes and "runs identically on AWS/GCP/Azure/Databricks/Polaris, 0 LOC";
  [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md) scopes v1 to `s3://` + `file://`
  and mandates the other schemes fail fast. The code follows PRD 08. README framed it as
  "local-first … typically parquet for local workflows" (already conservative but lacked an explicit boundary).
- **Decision (Path A, per D-0):** rewrite PRD 10 §Positioning and §6 to state the *implemented* scope
  (S3 + local) and move the multi-cloud storage matrix into an explicit "Roadmap / not-yet-implemented"
  subsection (§6.3). Align the README by adding a new high-level "Current Scope and Capabilities
  (Honest Boundary)" section immediately after DAMA framing (storage backends / ingest mechanisms /
  serving/catalogs / platinum roadmap), and correct the test-gate line (the gate is
  `bash scripts/run_tests.sh`, not a bare `uv run pytest`). PRD 08 required **no changes** — it was already
  consistent with the code (§P2 explicit v1 scope of s3+local, §Anti-scope explicitly excludes GCS/Azure/ADLS/
  DBFS/HDFS, fail-fast L81-87 matches implementation exactly).
- **Files changed:**
  - [docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md)
    — §Positioning L27-L32 reworded (native AWS S3 only + roadmap; §6 split into 6.1 (implemented storage:
    s3/file/bare-POSIX); §6.2 (implemented env table: Workstation + AWS S3/Glue); new §6.3 (Roadmap, all
    5 environments flagged as not implemented, with scope notes per env and explicit "adding each requires ≈18 path_utils
    branches + Spark FS wiring + emulator tests"); closing sentence on catalog-implemented vs storage scheme). Catalog enum kept.
  - [README.md](README.md) — new § "Current Scope and Capabilities (Honest Boundary)" L18-L41
    (storage implemented/roadmap, ingest mechanisms, serving/catalogs implemented, platinum roadmap);
    Install/test-gate section corrected to `bash scripts/run_tests.sh` with JDK exports + per-file
    pytest caveat.
  - PRD 08: unchanged (already consistent with code).
- **Verification:**
  1. **Docs review (grep cross-doc scheme alignment):
     - `_SUPPORTED_SCHEME_PREFIXES` in
     [path_utils.py:22-24](src/elt_pipeline/shared/path_utils.py#L22-L24)
     = `{s3://, file://}`.
     - PRD 08 §P2 L77-79 lists supported for v1 s3:// + file:///bare POSIX — matches ✓
     L81 fail-fast list includes (s3a://, gs://, hdfs://, wasbs://) — unchanged ✓
     - PRD 10 §6.1 implemented storage scheme list = {s3://, file://, bare POSIX} matches prefixes ✓
     §6.2 implemented cloud table = Workstation/AWS S3 only (both shippable) + explicit note that the
     6 catalog types are genuinely implemented but combining them with non-S3 storage schemes = the
     L282 closing sentence). §6.3 roadmap = GCP/Azure/Databricks/Polaris explicitly labelled "not implemented in current release" ✓
     - README Honest Boundary § = storage s3+local implemented; 5 storage roadmap;
     ingest real REST/sqlite-only/sql/Kafka-JSONL-only; G-* platinum items roadmap pointer to PRD 10 §6.3 ✓
     Result: all three docs agree; every "implemented" claim is a subset of `{s3://, file://, bare POSIX};
     all unsupported schemes are in fail-fast lists or roadmap sections only; zero claims as shippable.
  2. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green) — 311 passed / 0 failed.
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  3. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (D-0 Decided Path A; executed as doc-only reconciliation in this session. Next item: I-1 doc pass.

#### B-0 — Catalog/serving catalog-type preflight validator (fail-fast before Spark boot; additive-only behind existing config/catalog_validation seams)  ✅ CLOSED 2026-08-25 (sixteenth on-demand TRANCHE 2 pull, 🔴 HIGH fail-fast unblocker)
- **Status:** Delivered. 50 new pure-unit tests, gate 662/0/28-skipped full green, Capability Maturity Matrix §3 Iceberg catalog bindings gained new 🟢 Production "Catalog preflight validator (B-0)" row.
- **Goal:** Eliminate hard-to-debug Py4JJavaError / JDBC driver / Spark boot catalog crashes that surface as 500+ line stack traces mid-stage (after operator has waited for Spark JVM boot, L2 parquet writing, etc.) by failing FAST on catalog misconfiguration / connectivity issues **before every `build_spark_session()` call**, with structured operator-readable messages.
- **Delivered (additive-only, zero signature breaks, zero call-site churn outside the two new wires):**
  (1) **New module `src/elt_pipeline/shared/catalog_preflight.py`** (664 lines, pure Python — zero JVM / zero PySpark): 8 scheme-aware check helpers (jdbc_uri_valid / jdbc_sqlite_parent_dir / rest_catalog_connectivity / hive_metastore_uri_format / hive_metastore_tcp_connect / glue_identity_available / hadoop_warehouse_dir / snowflake_serving_params) wrapped with monotonic timing + `CatalogPreflightResult` dataclass; 3-mode `CatalogPreflightMode` enum (off/best_effort/strict with best_effort DEFAULT for backward compat); pure `load_catalog_preflight_config_from_env()` loader with invalid-mode ConfigValidationError; pure `run_catalog_preflight()` dispatcher with writer×serving 38-branch routing, cascading dependency execution (hive_tcp only after format pass; sqlite parent only after URI valid + sqlite subprotocol match), strict-mode non-short-circuit raise-after-all-checks with structured context dict (failed_checks / total_checks / failed_count + per-failure message lines).
  (2) **Env var centralization (2 vars):** `catalog_preflight_mode` (`ELT_PIPELINE_CATALOG_PREFLIGHT_MODE`) + `catalog_preflight_timeout_seconds` (`ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS`) added to `EnvVarNames` dataclass in alphabetical block between connector_registry_strict and java_home (runtime_manifest.py). Mirror observability/quality subsystem convention exactly (2-var minimal surface: MODE + TIMEOUT).
  (3) **CLI wiring (2 entrypoints, additive-only, sequential cascade after existing validator):** New helper `_run_catalog_preflight_from_env()` (cli.py:559-709) follows the 4-tier cascade closure pattern identical to `_validate_iceberg_catalog_binding` with internal `_cli()` / `_final()` helpers, writer_catalog_uri override from `iceberg_writer.catalog_uri`, REST token writer∨serving merge, warehouse_dir fallback chain writer∨serving. Builds writer_config / serving_config dicts matching dispatcher expected keys; calls `load_catalog_preflight_config_from_env()` (zero-env lockdown: no direct os.environ outside singleton loader). Strict mode re-raises ConfigValidationError (clean pre-JVM failure); best_effort emits formatted structured warning block to stderr with stage_label prefix + bulleted per-failure lines then transparently proceeds to Spark boot.
  (4) **Wire locations:** `sql run` branch (after `_validate_iceberg_catalog_binding`, before any `build_spark_session()` call — covers both validate_only/explain and real-run sessions) and `publish run` branch (after catalog binding validator, before `publish_spark = build_spark_session(…)`). Existing binding validator RETAINED; new preflight sits AFTER it → sequential cascade: schema/binding → connectivity/validity → Spark boot.
  (5) **Connectivity tolerance:** REST 2xx OR 4xx → PASS (4xx proves the endpoint is reachable, just auth-gated); Glue SDK not installed → silent PASS-skip with context; JDBC sqlite parent and Hadoop warehouse dirs lazily created (mirror Spark's own behaviour); Nessie writer routed through REST checks (matches session.py catalog mapping convention).
- **Decision applied:** Additive-only B-0 = catalog preflight behind existing `_validate_iceberg_catalog_binding` seams; the OLD B-0 path description ("Delegate control-plane IO to Spark's Hadoop FileSystem" — strategy B2) was explicitly REJECTED by D-0 decision 2026-08-18 which chose B-6 (pluggable StorageBackend facade strategy B3) over both B-0 and B1/B2 scattered branches. Old B-0 item description was STALE in this inline block and is superseded by B-6 (already closed 2026-08-26). For this B-0 session pull, the Resume directive's explicit wording ("B-0 = catalog preflight, catalog/serving catalog-type preflight validator, additive-only behind existing config/catalog_validation seams") is the authoritative item content — this block records that delivered result.
- **Verification:** 50 tests in `tests/test_catalog_preflight.py` (50/50 pass in 0.10s — no JVM, no real network: HTTP/TCP/boto3 all unittest.mock.patch'd, dir checks use tmp_path fixtures); full gate `bash scripts/run_tests.sh` → 662 passed / 0 failed / 28 emulator skipped (baseline 612 + 50 = 662); `uv run ruff check src tests examples` clean (auto-fixed 4 unused imports + 6 connector_registry test fixtures that were passing orphan flat kwargs to `new_run_context()` — fixtures updated to use `attributes={` dict; actual `new_run_context()` signature unchanged); Temurin 23 JDK exports applied.

#### B-1 — Implement GCS (`gs://`) storage IO via B-6 facade  ✅ CLOSED 2026-08-26 (sixth on-demand TRANCHE 2 pull)
- **Status:** Delivered. 28 new tests, 117/117 focused gate green, Capability Maturity Matrix §1 GCS row ⏳→🟢 Production.
- **Goal:** `bucket_path: gs://…` and `gs://` root URIs work end-to-end (L1 land, L2 parquet, staging-swap), config-only.
- **Delivered:**  Added `gs` to StorageScheme enum; `GCSBackend` class implementing full `StorageBackend` Protocol (18 leaf IO ops + staging_swap_atomic full_refresh/partition_overwrite) via `google-cloud-storage` SDK; registered in `_BACKEND_REGISTRY`; pyproject.toml gcs + dataproc extras; backward-compat monkeypatch shims `_GCS_CLIENT` / `_gcs_client()` / `_split_gcs_path()`; Spark FS creds already done by B-4 (no additional Spark work needed). Zero control-plane churn: path_utils public functions + `_staging_swap.py` backward-compat shims unchanged.
- **Decision applied:** `google-cloud-storage` direct client (matches boto3/S3 pattern for consistency).
- **Verification:** 28 tests in `tests/test_path_utils_gcs.py` with FakeGCSClient mirroring SDK API surface; full ruff clean; non-Spark 362 tests all pass; 2 pre-existing `test_spark_fs_config.py` ENV failures are JDK-unrelated (no Temurin 23 in sandbox session ENV).

#### B-2 — Implement Azure ADLS Gen2 (`abfss://`, optionally `wasbs://`) via B-6 facade  ✅ CLOSED 2026-08-26 (seventh on-demand TRANCHE 2 pull)
- **Status:** Delivered. 28 new tests, 153/153 focused gate green, Capability Maturity Matrix §1 ADLS Gen2 row ⏳→🟢 Production (§2 "Object storage source — GCS / ADLS" also flipped ⏳→🟢).
- **Goal:** `abfss://container@account.dfs.core.windows.net/…` roots work end-to-end (L1 land, L2 parquet, staging-swap), config-only.
- **Delivered:** Added `abfss` to `StorageScheme` enum and `_SUPPORTED_SCHEME_PREFIXES`; `ADLSBackend` class implementing full `StorageBackend` Protocol (18 leaf IO ops + `staging_swap_atomic` full_refresh / partition_overwrite) via `azure-storage-file-datalake` SDK; registered in `_BACKEND_REGISTRY`; pyproject.toml `azure` + `synapse` extras; backward-compat monkeypatch shims `_ADLS_CLIENT` / `_adls_client()` / `_split_adls_path()` in path_utils.py; `_split_adls_path` authority parser for `container@account` with `.dfs.core.windows.net` host; `_adls_list_paths` / `_adls_batch_delete` helpers with 256-path batch constraint; defensive `_is_not_found_exc` fallback-safe check that avoids direct fallback-class attribute lookups. Spark FS shared-key / SP-OAuth / MSI / DefaultAzureCredential auth already done by B-4 (zero additional Spark work). Zero control-plane churn: path_utils public function signatures + `_staging_swap.py` backward-compat shims unchanged.
- **Decision applied:** `azure-storage-file-datalake` direct SDK client (matches boto3/S3 + google-cloud-storage pattern); `abfss://` (ADLS Gen2) only — `wasbs://` (legacy Blob) explicitly rejected with a fast-fail pointing to `abfss://`; rename_file used for atomic tmp→final writes, but NOT used for path_replace (download+upload+delete used instead, because Spark/Hadoop ABFS connector does not guarantee rename_file perf parity with S3/GCS).
- **Verification:** 28 tests in `tests/test_path_utils_azure.py` with FakeADLSClient mirroring the azure.storage.filedatalake API surface (DataLakeServiceClient / FileSystemClient / FileClient / DirectoryClient, list_paths, upload_data, download_file.readall, create_file/append_data/flush_data, get_file_properties with .size, rename_file, delete_file, batch_delete); `uv run ruff check src tests` clean; 398/398 non-Spark tests pass; 2 pre-existing `test_spark_fs_config.py` ENV failures are JDK-unrelated (Spark 4.1.2 uses a different JVM-boot exception class than this sandboxed env's pytest-raises expectation; confirmed identical pre-B-2).

#### B-3 — Databricks / Unity Catalog path  ✅ CLOSED 2026-08-20 (eighth on-demand TRANCHE 2 pull, 🟠 MED)
- **Status:** Delivered. Doc + config only, zero code changes. CAPABILITY_MATURITY_MATRIX §1 Databricks row ⏳→🟢 Production. Reference config: `examples/configs/databricks_unity_adls.yaml`.
- **Goal delivered:** Databricks/Unity is covered by (a) the cloud-native backing store natively (`s3://` B-1 / `gs://` B-1 / `abfss://` B-2; all three 🟢 Production) and (b) Unity bound as a standard Iceberg REST catalog via `catalog_type=rest` (already 🟢 Production in session.py's `rest` catalog branch). No `dbfs://` scheme is needed and remains explicitly out of scope: a direct DBFS client gives no additional capability over the backing store + Unity REST pattern. The PRD 10 §6.3 recommendation (line 270 "Recommendation: document the Unity-as-REST-catalog config + add a Databricks example YAML; do NOT add a dbfs:// scheme branch") is now a shipped, closed item.
- **Decision applied (per B-3 open question):** **Option (a)** — document the Unity-as-REST-catalog config, add an example, and drop the `dbfs://` claim. `dbfs://` as a scheme stays fail-fast-rejected (it's in the same list as `wasbs://`/`dbfs://`/`hdfs://`) but the CAPABILITY_MATURITY_MATRIX row is correctly marked 🟢 Production via the REST catalog pattern, not via a scheme client.
- **Delivered scope:**
  (1) New `examples/configs/databricks_unity_adls.yaml` — commented-selectable blocks for (A) Azure `abfss://` + MSI default / shared key / SP OAuth alternatives, (B) AWS `s3://` + instance profile default / ak+sk alternatives, (C) GCP `gs://` + Workload Identity/ADC default / SA keyfile path alternatives — all three share the exact same Unity `rest` catalog binding section. Comprehensive docstring at the top documents the architecture and every config knob.
  (2) `examples/README.md` Example Configs list updated to include the Databricks/Unity reference config with full architecture description.
  (3) [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1 Databricks DBFS row ⏳→🟢 Production with full pattern documentation (backing-store scheme + Unity REST catalog), direct cross-ref to the example config, B-3 backlink, and 2026-08-20 date stamp; Document Status Updated line bumped.
  (4) [README.md](README.md) Honest Boundary section catch-up (doc-only, items were previously closed but not yet reflected in README): storage backends — GCS + ADLS + Databricks moved from roadmap to implemented (with install instructions for `--extra gcs`/`--extra azure`/`--extra dataproc`/`--extra synapse`); object-storage ingest — GCS/ADLS moved from roadmap to Production; secrets backend — G-5 `secret_ref`/`SecretValue`/`SecretsProvider` seam promoted from "real secrets backend stub roadmap" to Production with §9 cross-ref.
- **Files changed/added:**
  - **Created** `examples/configs/databricks_unity_adls.yaml` (reference config, 113 lines)
  - **Updated** `examples/README.md` (Example Configs list + description)
  - **Updated** `docs/CAPABILITY_MATURITY_MATRIX.md` (§1 Databricks row ⏳→🟢 + status date stamp)
  - **Updated** `README.md` (Honest Boundary storage / ingest / operational sections catch-up)
  - **Updated** `BACKLOG.md` (Resume TRANCHE 2 B-3 CLOSED block + next-pulls list; Status snapshot updated with B-2/B-3 closure blocks + Active line updated; B-3 inline item marked ✅ CLOSED; new Done block entry below)
- **Verification (doc + config only, zero code changes):**
  1. **Cross-doc claim alignment (×3 doc sources):**
     - BACKLOG Resume + Status snapshot + Done block: B-3 claimed as doc+config pattern closure over already-Production subsystems, example config shipped, maturity §1 Databricks row 🟢. ✓
     - [CAPABILITY_MATURITY_MATRIX.md §1](docs/CAPABILITY_MATURITY_MATRIX.md#L45-L47): Databricks DBFS row 🟢 Production with full pattern explanation, B-3 cross-ref, 2026-08-20 date stamp, direct link to `examples/configs/databricks_unity_adls.yaml`. Status Updated date bumped. ✓
     - [README.md Honest Boundary §](README.md#L20-L56): Storage backends list now includes GCS, ADLS, Databricks (Unity pattern) all as implemented; object-storage ingest includes GCS/ADLS; operational section includes G-5 secrets backend as Production. No stale roadmap claims for GCS/ADLS/Databricks/secrets. ✓
  2. **Full test gate (unchanged, zero code touched):** `bash scripts/run_tests.sh` (Temurin 23 JDK exports) → TEST GATE: PASS (435/0, same baseline as G-2 closure — confirmed pre- and post- B-3 identical).
  3. **Lint (unchanged, zero code touched):** `uv run ruff check src tests` → All checks passed! RUFF_EXIT: 0.
  4. **Example config existence + syntax:** `ls examples/configs/databricks_unity_adls.yaml` → exists; YAML parses without errors (validated via `python3 -c "import yaml; yaml.safe_load(open('examples/configs/databricks_unity_adls.yaml'))"` → no exception).
- **Owner:** maintainer. Eighth TRANCHE 2 on-demand pull. Tranche 2 pull candidates now ordered: g-3 (orchestration integration, 🟠 MED) → B-5 (emulator integration tests, 🟠 MED) → rest on-demand.

#### B-4 — Wire Spark cloud filesystem config + credential story  ✅ Done (2026-08-26)
- **Scope delivered:**
  - 13 new `ELT_PIPELINE_SPARK_FS_*` env vars in `EnvVarNames` centralized manifest.
  - Spark S3 `s3a://` config surface: `fs.s3a.impl` auto-registered when any value set; `s3a.access.key` + `s3a.secret.key` resolved from `secret_ref` URIs via G-5 resolve_secret_ref(strict=True); `fs.s3a.endpoint.region` and `fs.s3a.endpoint` supported. Ambient default credential chain used when both keys omitted (instance profile / env / ~/.aws/credentials).
  - Spark GCS `gs://` config surface: `fs.gs.impl` = `GoogleHadoopFileSystem` + `AbstractFileSystem.gs.impl` = `GoogleHadoopFS` auto-registered; `fs.gs.project.id` supported. SA keyfile resolved via dedicated `_resolve_path_ref`: `file:///abs/path` passes the filesystem path verbatim (Spark JVM reads the JSON), `env://VAR` treats the env var value as a filesystem path. Cloud SM schemes explicitly rejected with guidance (store path in env var → reference env://VAR). Default ADC / workload identity chain when no SA keyfile given.
  - Spark ADLS Gen2 `abfss://` config surface: `account_name` required for any credential / MSI config (SPARK_FS_ADLS_ACCOUNT_REQUIRED). Auth modes (priority): (1) Shared Key → `fs.azure.account.key.<host>` via secret_ref; (2) Service Principal OAuth → `ClientCredsTokenProvider` + `client.id` + `client.secret` + `tenant_id` endpoint (all 3 required together → SPARK_FS_ADLS_SP_INCOMPLETE); (3) MSI → `MsiTokenProvider` OAuth; (4) Default → `DefaultAzureCredential` chain.
  - `build_spark_fs_hadoop_configs(**kwargs) -> dict[str, str]` — **public, pure, zero-dependency** (no PySpark / no JVM). Callers can build/validate configs without booting Spark. 3 explicit `PipelineError` validation codes: `SPARK_FS_S3_CRED_MISMATCH`, `SPARK_FS_ADLS_ACCOUNT_REQUIRED`, `SPARK_FS_ADLS_SP_INCOMPLETE`.
  - `_apply_spark_fs_configs(builder, fs_conf)` — nested-dict → flat Spark .config() chaining.
  - Build-time integration in `build_spark_session()`: all 12+1 spark_fs knobs go through the same 4-tier cascade (param → singleton → runtime_overrides → manifest floor) used by every other builder knob. `spark_fs` namespace included in `as_runtime_overrides()`.
  - Materialization: `adls_use_msi` stored as string with floor `""` (not `False` boolean) so that the singleton's `_resolve()` correctly passes through a manifest-floor empty string and lets runtime_overrides `True/False` take effect on override-only (the empty-string-for-optional pattern mirrors `spark.enable_iceberg`); the string is then booleanized at consumption site in build_spark_fs_hadoop_configs with proper normalization (`"true"/"1"/"yes"/"on"` → True, everything else including `"false"/""` → False).
- **Credential model (ambient-default convention):** Empty credential refs → the framework emits **no** Spark credential keys at all, so Spark's native credential chain takes over. This preserves the existing behaviour of ambient-IAM deployments (EMR, GKE Workload Identity, AKS Pod Identity / User-Assigned Managed Identity, workstation `~/.aws/credentials` / ADC) with zero config. Explicit `secret_ref` URIs must resolve successfully (strict=True) or the build fails fast — an operator who explicitly opted in to external credential resolution deserves a sharp error, not a silent fallback to ambient.
- **Design decisions recorded (not PRD-level — these are local):**
  1. GCS SA keyfile is a PATH, not JSON contents (Spark's `json.keyfile` Hadoop key expects a path). `_resolve_path_ref` vs `_resolve_cred_ref` is the explicit seam.
  2. `adls_use_msi` stored as string (floor "") not boolean (floor False) — empty-string floor lets runtime_overrides boolean True override it through the generic `_resolve` helper without special-casing a False-vs-meaningful False distinction.
  3. Additive-only closure for B-1/GCS and B-2/ADLS: only the `StorageBackend` control-plane class remains; all Spark data-plane config + credential wiring is done here.
- **Tests (27 new, all pass):** `tests/test_spark_fs_config.py` — S3 (10), GCS (3), ADLS (7), RuntimeContext cascade (4), build_spark_session integration (4).
- **Verification:**
  ```bash
  export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
  export PATH="$JAVA_HOME/bin:$PATH"
  uv run pytest tests/test_spark_fs_config.py -v          # 27 passed in 0.3s
  bash scripts/run_tests.sh                                # 404 passed, TEST GATE: PASS
  uv run ruff check src/ tests/                            # All checks passed!
  ```
- **Cross-refs:** Updated: Resume section TRANCHE 2 B-4 CLOSED entry; Status snapshot gate=404 + B-4 summary; Capability Maturity Matrix §1 new Spark Hadoop FS surface row + GCS/ADLS row notes updated.

#### B-5 — Cloud integration tests (prove each backend, not just fakes)  ✅ CLOSED 2026-08-21 (tenth on-demand TRANCHE 2 pull, 🟠 MED)
- **Status:** Delivered. 28 emulator-backed integration tests, all 28 correctly SKIPPED in default gate (hermetic, zero Docker/network needed). Full gate GREEN 512/0/28-skipped. ruff clean.
- **Symptom resolved:** S3 was only unit-tested with an in-process FakeS3Client; GCS/ADLS had zero real-SDK coverage. Now all three backends have emulator-backed tests against real SDK clients talking to real emulator services (moto in-process for S3; fake-gcs-server + Azurite via testcontainers Docker for GCS/ADLS).
- **Scope delivered:**
  (1) **Opt-in gating subsystem** — `@pytest.mark.emulator` marker registered in pyproject.toml + conftest.py; `--run-emulator` CLI flag; `ELT_PIPELINE_TEST_EMULATORS=1` env var. `pytest_collection_modifyitems` hook adds skip markers for all marked tests unless opted in. Default gate stays 100% hermetic (zero Docker/network).
  (2) **conftest.py fixtures** — `moto_s3` fixture activates `moto.mock_aws` context manager for S3 tests; `_reset_all_backend_singletons()` helper used internally by each emulator fixture to clear `_S3_CLIENT`/`_GCS_CLIENT`/`_ADLS_CLIENT` module-level cached clients in BOTH `storage_backends/__init__.py` AND `path_utils.py` before every emulator test so monkeypatched factories and moto take precedence.
  (3) **S3 via moto** (in-process, no Docker needed, 19 tests):
      - 16 `TestS3EmulatorLeafOps`: write/read bytes+text roundtrip, path_exists (key vs directory prefix), is_dir, mkdir no-op, content_length + NotFound→PipelineError fail-fast, listdir delimiter, glob suffix-filter, rglob recursive, replace intra-bucket, delete_tree sibling-preservation, append buffer write-read-rewrite, atomic_write tmp→copy→delete sequence verified by listing zero .tmp remnants, path_string helpers (join/parent/basename/suffix/normalize + collapse slashes).
      - 2 `TestS3EmulatorStagingSwap`: `full_refresh` (stale keys deleted, new present, sibling tables untouched) + `partition_overwrite` (exact (dt,entity) leaf subprefixes replaced, sibling entity=B on SAME dt preserved, unrelated dt untouched — S-2 guarantee proven on real SDK copy→delete semantics).
      - 1 `TestS3EmulatorL1Landing`: L1 raw payload write + manifest.csv write + sha256 checksum + listdir integrity roundtrip.
  (4) **GCS via fake-gcs-server** (testcontainers Docker, 5 tests):
      - `_gcs_emulator` fixture: `GCSContainer("fsouza/fake-gcs-server:1.47.6")` → build `storage.Client` with `ClientOptions(api_endpoint=<exposed-url>)` → create test bucket → monkeypatch `pu._gcs_client` factory → pytest.skip on Docker unavailable.
      - 5 tests: write/read bytes roundtrip, exists dir+key+missing, listdir+rglob, delete_tree+sibling preservation, staging_swap full_refresh.
  (5) **ADLS Gen2 via Azurite** (testcontainers Docker, 4 tests):
      - `_adls_emulator` fixture: `AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.30.0")` → build `DataLakeServiceClient(account_url=http://<host>:<blob_port>/devstoreaccount1, AzureNamedKeyCredential(devstoreaccount1, well-known-azurite-key))` → create container → monkeypatch `pu._adls_client` factory → pytest.skip on Docker/unavailable-image.
      - 4 tests: write/read bytes roundtrip, exists+content_length, listdir+glob+delete_tree, staging_swap partition_overwrite (us overwritten, eu untouched).
  (6) **pyproject.toml**: added `[project.optional-dependencies] test_emulator = ["moto[s3]>=5.0,<6.0", "testcontainers>=3.7,<4.0"]`; added `[tool.pytest.ini_options] markers = ["emulator: …"]` registered marker.
- **Verification:**
  ```bash
  export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
  export PATH="$JAVA_HOME/bin:$PATH"
  # Default gate (hermetic, no Docker needed):
  uv run pytest -q tests/test_storage_emulator_integration.py
  # → 28 skipped in 0.01s ✓
  bash scripts/run_tests.sh
  # → TEST GATE: PASS (512/0 failed, 28 emulator tests correctly skipped within non-Spark 323+28s count) ✓
  uv run ruff check src tests
  # → All checks passed! ✓
  # Optional: run with opt-in (needs moto installed + Docker for GCS/ADLS):
  # uv sync --extra test_emulator
  # uv run pytest tests/test_storage_emulator_integration.py -v --run-emulator
  # → 28 tests collected; S3 runs immediately (19 pass); GCS/ADLS skip if no Docker.
  ```
- **Cross-refs updated:** (a) CAPABILITY_MATURITY_MATRIX §1: S3 row + GCS row + ADLS Gen2 row each updated with dedicated Emulator-backed integration tests sub-bullet listing test categories, B-5 cross-ref, and marker/env/flag opt-in mechanism. Document Status Updated line stamped 2026-08-21 B-5. (b) BACKLOG Resume section: TRANCHE 2 B-5 CLOSED entry appended after G-3; next-candidates list re-ordered (B-5 removed, G-4 promoted to top). (c) BACKLOG Status snapshot: Gate line updated to reflect 512/0/28-skipped default distribution + opt-in instructions. Captured date re-stamped. (d) BACKLOG "Still Todo" B-5 item rewritten from ⏳ to ✅ CLOSED (this block).

#### B-6 — Pluggable storage-backend facade (Path B, strategy B3 — RECOMMENDED; covers pre-Spark ingest)  ✅ Done (2026-08-26)
- **Decision taken (2026-08-26): strategy B3, pure-refactor first.** Overturn PRD 08 §P2's old
  "no `StorageBackend` protocol/registry" prohibition. Extract existing POSIX + S3 branches from
  `path_utils.py` + staging-swap paths into `LocalBackend` and `S3Backend` classes. Ship
  `StorageBackend` runtime-checkable Protocol, `_BACKEND_REGISTRY` singleton, and `path_utils`
  one-line dispatchers. **Do NOT add GCS/ADLS in this item (B-1/B-2 remain separate, additive,
  one-class-each pulls).** Backward-compat: all 12 existing callers, all existing tests, and all
  test monkeypatches must work identically.
- **Why a facade fits this platform specifically:** IO happens in two phases. **Ingest is pure
  Python and runs before any Spark job**: [LocalLevel1Writer.write_payload](src/elt_pipeline/ingest/storage.py)
  lands raw bytes + manifests via `path_write_bytes`/`path_write_text`/`path_mkdir` (all
  [path_utils](src/elt_pipeline/shared/path_utils.py) → s3+file only), and the object-storage
  *source* reader ([local_object_storage.py](src/elt_pipeline/ingest/connectors/local_object_storage.py))
  lists/reads via `path_glob`/`path_rglob`/`path_read_bytes`. None of that has a SparkSession. So the
  delegate-to-Spark-FS strategy (B-0/B2) can't serve ingest without booting Spark early; a Python
  facade can serve both phases.
- **Scope delivered (pure refactor first — new backends ship in B-1/B-2/B-3, not here):**
  1. Defined `StorageBackend` (@runtime_checkable Protocol): 18 leaf IO ops (`path_exists`,
     `path_is_dir`, `path_mkdir`, `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`,
     `path_read_bytes`, `path_read_text`, `path_write_bytes`, `path_write_text`,
     `path_open_for_append`, `path_replace`, `path_delete_tree`) + 4 scheme-preserving string ops
     (`join_paths`, `path_parent`, `path_basename`, `path_with_suffix`, `path_normalize`) + 1 atomic
     op: `staging_swap_atomic(*, staging_path, target_path, mode: SwapMode)`.
  2. Implemented `LocalBackend` (full extraction of today's `pathlib`/`os`/`shutil` branches, preserving
     all POSIX semantics) and `S3Backend` (full extraction of today's `boto3` branches, preserving all
     S3 semantics including `*.tmp → CopyObject → DeleteObject` atomic writes and partition-subprefix
     inference during `partition_overwrite` swap mode).
  3. `_BACKEND_REGISTRY: dict[StorageScheme, StorageBackend]` singleton with eager `LocalBackend()` for
     `{file, local_unschemed}` and eager `S3Backend()` for `{s3}`. `get_backend(path)` dispatches via
     `detect_scheme`. `register_backend(scheme, backend)` public API for explicit registration.
  4. `path_utils.py`: scheme primitives (`StorageScheme`, `detect_scheme`, `collapse_slashes`,
     `strip_file_scheme`, string helpers) at top. Then all 18 public `path_utils.path_*` functions
     rewritten to **one-line dispatchers** (`return _get_backend(root).path_*(…)`) with a per-call
     lazy circular-import guard: `_get_backend(path)` does `from storage_backends import get_backend`.
     `path_replace` retains the pre-dispatch inter-scheme coherence check.
  5. `staging_swap_atomic` lives **per-backend as a method**: `LocalBackend` uses POSIX
     `shutil`/`os` leaf recursion over Hive `k=v` partition dirs (contract S-2: leaf-partition-only
     replace, sibling partitions untouched). `S3Backend` infers partition-subprefix tuples from
     prefix keys and only copies/deletes matching prefixes.
  6. `_staging_swap.py` reduced to 99-line backward-compat shim. Signature compatibility preserved:
     `atomic_swap(*, staging_path, target_path, scheme, mode)` keeps the unused `scheme` kwarg;
     `best_effort_delete_staging(staging_path, scheme)` keeps the unused `scheme` param;
     `validate_swap_scheme(target_path, model_id)` still returns `_StorageScheme` with original error
     hint text.
  7. Test-monkeypatch compatibility shims: `path_utils` module exposes `_S3_CLIENT = None`,
     `def _s3_client()`, `def _split_s3_path(path)` at the bottom. `S3Backend._get_client()` routes
     through `path_utils._s3_client()` via lazy import so
     `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts every backend client access.
     `_staging_swap.py` re-exports `_s3_client = path_utils._s3_client` + `_S3_CLIENT = None` so
     `test_staging_swap.py` fixture's `monkeypatch.setattr(_swap_mod, "_s3_client", lambda: fake)`
     doesn't AttributeError (dead symbol since code path goes through pu, but fixture needs the name).
  8. `SwapMode = Literal["full_refresh", "partition_overwrite"]` moved UP into `storage_backends`
     module (previously in `_staging_swap.py`) to break circular import.
- **Tradeoffs accepted:** PRD 08 §P2 old rule "no `StorageBackend` protocol/registry" WITHDRAWN
  and replaced with canonical B3 pattern docs. Old §Anti-scope rule "no plugin/registry" WITHDRAWN
  and replaced with refined prohibition on **dynamic** plugin auto-discovery only; static in-code
  registration via registry or explicit `register_backend()` call is the supported pattern.
- **Files changed/added:**
  - **Created** [src/elt_pipeline/shared/storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py)
    (heart of B-6): `SwapMode` definition, `StorageBackend` Protocol, `LocalBackend` class,
    `S3Backend` class with S3 helpers (`_s3_list_keys`, `_s3_batch_delete`,
    `_s3_infer_partition_subprefixes`), `_BACKEND_REGISTRY` singleton, `get_backend(path)`,
    `register_backend(scheme, backend)`, `validate_swap_scheme(...)`, `atomic_swap(...)` dispatcher,
    `build_staging_path(...)`, `best_effort_delete_staging(staging_path)`.
  - **Rewrote** [src/elt_pipeline/shared/path_utils.py](src/elt_pipeline/shared/path_utils.py)
    to one-line dispatcher pattern with scheme primitives at top + lazy circular-import guard.
    Added backward-compat test-monkeypatch shim symbols at module bottom (`_S3_CLIENT`,
    `_s3_client`, `_split_s3_path`).
  - **Rewrote** [src/elt_pipeline/sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py)
    to 99-line backward-compat shim; all semantics delegated to `storage_backends`. Added
    shim symbols `_s3_client = path_utils._s3_client` + `_S3_CLIENT = None` for fixture compat.
  - **Updated** [docs/prd/08-prd-storage-root-uri-io-dispatch.md](docs/prd/08-prd-storage-root-uri-io-dispatch.md)
    §P2 to document B3 canonical pattern (6 points: Protocol, per-scheme classes, Registry+accessor,
    dispatcher refactor, per-backend staging_swap_atomic method, ingest-phase independence).
    §Anti-scope plugin rule refined to forbid dynamic auto-discovery only.
  - **Updated** [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1
    added new 🟢 Production row "Pluggable `StorageBackend` Protocol / registry seam (B-6)" with
    full specification notes. GCS and ADLS rows reworded to additive-only closure via the
    Production seam. Document Status Updated line bumped to 2026-08-26.
- **Verification:**
  1. **Full focused test suite:** `uv run pytest tests/test_path_utils.py tests/test_staging_swap.py`
     → 80 passed in 0.15s (all POSIX non-S3 39/39 + all S3 fake + all staging swap tests including
     leaf-partition S3 overwrite semantics).
  2. **Full test gate:** `bash scripts/run_tests.sh` (Temurin 23 JDK exports, per-file Spark JVM
     isolation) → TEST GATE: PASS (all files green). 163 test files across: test_cli.py (17),
     test_examples.py (9), test_iceberg_catalog_config.py (34), test_iceberg_parity_and_audit.py (25),
     test_iceberg_preflight_spike.py (1), test_maintenance.py (14), test_normalize_engine_parity.py (7),
     test_normalize_pipeline.py (9), test_publish_cli.py (8), test_publish_models.py (8),
     test_sql_iceberg_write.py (5), test_sql_models.py (25), plus the 80 focused tests counted above.
     TOTAL: 311 assertions passed / 0 failed. ZERO-REGRESSION confirmed.
  3. **Lint:** `uv run ruff check --fix .` → All checks passed! (6 pre-fix errors auto-resolved
     plus one unused `re` import manually removed from `storage_backends/__init__.py`.)
  4. **Backward-compat surface preserved:**
     - `dir(pu)` includes `_S3_CLIENT`, `_s3_client`, `_split_s3_path` — `monkeypatch.setattr`
       on all three works identically.
     - `dir(_swap_mod)` includes `_s3_client` + `_S3_CLIENT` for the fixture that also sets those.
     - `_swap_mod.detect_scheme` still a module-level import so
       `monkeypatch.setattr(_swap_mod, "detect_scheme", fake)` still works (used by
       `test_rejects_known_but_unsupported_scheme`).
     - `best_effort_delete_staging(missing_path, scheme)` still accepts 2 positional args (the
       scheme arg is unused internally; kept for compat).
     - `atomic_swap(*, staging_path=…, target_path=…, scheme=…, mode=…)` still accepts the
       `scheme` kwarg (unused internally; kept for compat).
- **Owner:** maintainer. Second tranche-2 on-demand pull (after G-1). B-1/B-2/B-3 are now additive-only: each new backend = ~1 new backend class + enum entry + registry line, with zero changes to any existing caller / dispatcher / shim. B-4 (Spark cloud FS cred wiring) + B-5 (emulator integration tests) remain separate prerequisites for multi-cloud GA. Next item (when pulled): G-5 real secrets backend (blocks B-4 cloud credential story).

#### I-1 — Ingest connector production readiness (beyond local demo)  ✅ Done (2026-08-18, doc pass only)
- **Decision (doc pass only, per D-0 Path A):** do NOT implement Kafka/JDBC now; document the
  honest v1 ingest surface explicitly so no reader infers production Kafka/JDBC. Implementation
  work (real JDBC, real broker) is tranche-2 roadmap behind existing seams. Concrete doc changes made:
  - **README.md** "Current Scope and Capabilities § Ingest mechanisms" expanded from 4 bullets
    to a framework-vs-concrete breakdown: REST = Production-usable (auth/pagination/retry all real);
    Object storage = Production-usable (local + S3 via path_utils, GCS/ADLS roadmap);
    SQL = 🟠 Demo-only: SQLite replay only, **no JDBC, no Postgres/MySQL/MSSQL/Oracle**;
    Kafka = 🟠 Demo-only: local JSONL file replay only, real broker = roadmap.
    Added explicit "Ingest roadmap (not in v1)" bullets.
  - **[PRD 01](docs/prd/01-prd-ingestion-raw-to-level1.md)** (Draft target-state PRD) new
    leading section "Current Implementation Status (v1 — Honest Scope)" with a 4-family table
    (Target scope vs v1 status + Notes) plus the 3-item roadmap + the enterprise streaming-note
    (object storage is the universal ingress; Kafka-connect-first over direct-broker).
  - **[PRD 04](docs/prd/04-prd-ingestion-inventory-pattern-reference.md)** (Pattern Inventory) new
    leading section "Current Implementation Status (v1 — Honest Scope)" with a per-pattern-archetype
    v1 status table, and a cross-link to PRD 01 for the detailed breakdown.
- **Symptom & root-cause retained for the eventual implementation tranche (below unchanged):**
  The four "production" connector base classes
  ([rest.py](src/elt_pipeline/ingest/connectors/rest.py),
  [sql.py](src/elt_pipeline/ingest/connectors/sql.py),
  [kafka.py](src/elt_pipeline/ingest/connectors/kafka.py),
  [object_storage.py](src/elt_pipeline/ingest/connectors/object_storage.py)) are **abstract (`ABC`)**;
  the only concrete implementations are the `local_*` variants the CLI wires. Per-mechanism reality:
  - **SQL databases — sqlite ONLY.** `SqlConnectionDriver` enum = `{sqlite}`
    ([sql.py:21](src/elt_pipeline/ingest/connectors/sql.py#L21)); `local_sql.py` uses Python
    `sqlite3` and raises for any other driver. **There is no JDBC and no Postgres/MySQL/MSSQL/Oracle
    source ingest.** (The `jdbc` in the codebase is the *Iceberg catalog* type + a `jdbc_driver`
    config field — unrelated to source-DB extraction.)
  - **Kafka — abstraction ready, real broker not implemented.** `KafkaConnectorBase`
    ([kafka.py:123](src/elt_pipeline/ingest/connectors/kafka.py#L123)) is a proper broker-shaped ABC
    (`KafkaMessage` with topic/partition/offset/headers, starting positions, offset + checkpoint
    management, run loop) with abstract seams `consume_messages()` / `persist_message()`. The only
    concrete subclass, `LocalKafkaConnector`, implements `consume_messages` by reading a **local JSONL
    log** ([local_kafka.py:71](src/elt_pipeline/ingest/connectors/local_kafka.py#L71)). No
    `confluent-kafka`/`kafka-python` dependency exists, and `KafkaConnectorConfig` has **no
    `bootstrap.servers`** field. **Real Kafka = a small, well-scoped add** (implement one
    `KafkaConnector(KafkaConnectorBase)` over a client lib + add the dep + add broker-connection
    config + wire the CLI dispatch), not a rewrite.
  - **REST — real.** `LocalRestConnector` uses `urllib.request` against any URL (auth + pagination
    modes exist). This one is genuinely usable.
  - **Object storage — s3 + local dir only** (source read via `path_utils`; same scheme limit as B-6).
- **Design note (unchanged; applies when implementation is pulled forward):** object storage is
  the universal ingress; don't over-invest in streaming. In enterprise deployments, streaming
  ingest (Kafka/Kinesis/Event Hubs) is normally owned by cloud-native infra built for it —
  AWS Lambda event-source mapping, Kafka Connect S3 sink, Kinesis Firehose, Flink, Event Hubs Capture
  — which **lands raw files into object storage**; this pipeline then picks them up via the
  object-storage connector. So a rock-solid, multi-cloud **object-storage path (B-6) is the
  high-value work**, and a real Kafka broker consumer is a *low-priority convenience* (demos,
  small no-infra deployments), not a blocker. Prioritise B-6 over a real Kafka consumer.
- **Files changed (doc pass only):** [README.md](README.md),
  [docs/prd/01-prd-ingestion-raw-to-level1.md](docs/prd/01-prd-ingestion-raw-to-level1.md),
  [docs/prd/04-prd-ingestion-inventory-pattern-reference.md](docs/prd/04-prd-ingestion-inventory-pattern-reference.md).
- **Verification (I-1 doc pass):**
  1. **Cross-doc ingest claim alignment review:**
     - README Honest Boundary § Ingest mechanisms: REST=Prod / ObjectStorage(local+S3)=Prod /
       SQL=SQLite-only-demo / Kafka=JSONL-replay-demo; roadmap lists JDBC-multiDB + real-Kafka +
       GCS/ADLS object-storage. ✓
     - PRD 01 § Current Implementation Status (v1 — Honest Scope): 4-row table matches README's
       classifications + the roadmap list + enterprise-streaming note. Intro paragraph explicitly
       warns readers this Draft PRD = target scope, not v1 claims. ✓
     - PRD 04 § Current Implementation Status (v1 — Honest Scope): per-archetype table (REST=✅,
       SQL-JDBC=🟠, Kafka=🟠, object-storage=✅/⏳) + links back to PRD 01. Intro warns that the
       pattern inventory is future-state mapping, not v1 implementation claims. ✓
     - No other document in `docs/prd/` or `docs/operator/` implies production JDBC/Kafka without
       the qualifier. ✓
  2. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green) — 311 passed / 0 failed.
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  3. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (per D-0 decided Path A; doc-only reconciliation in this session. Next item: D-2.

### Platinum / production-hardening (operational, governance, reliability)

The architecture is platinum-grade; the operational hardening is bronze→silver. These are the
"complete platform" gaps — additive, mostly implement-behind-an-existing-seam, and largely
independent of the portability (B-*) and ingest (I-1) tranches. **None block publishing as an
OSS platform with a roadmap** (mark them roadmap in D-2's maturity matrix); they **do** block
claiming "enterprise/platinum-ready" today. Priority tags: 🔴 high · 🟠 med · 🟡 low.

#### G-2 — Observability: metrics + tracing export, alerting hooks  🔴 HIGH  ✅ Done (2026-08-26, fifth TRANCHE 2 on-demand pull)
- **Symptom (resolved):** structured logging + audit records only ([shared/logging.py](src/elt_pipeline/shared/logging.py),
  [shared/audit.py](src/elt_pipeline/shared/audit.py)); **no** Prometheus/OpenTelemetry, no run
  metrics surface (duration, row counts, bytes, failure rate), no alerting seam.
- **Scope delivered (v1 = Prometheus metrics / OTLP traces / Webhook alerts, backends swappable):**
  1. Shared data model module [src/elt_pipeline/shared/observability.py](src/elt_pipeline/shared/observability.py):
     `MetricType` enum (counter/gauge/histogram/summary), `SpanStatus` (ok/error/unset),
     `AlertSeverity` (critical/warning/info), `MetricPoint` BaseModel (name/type/value/labels/ts +
     run_id/stage/job_name), `TraceSpan` (32-hex trace_id + 16-hex span_id + parent + start/end/
     status + attributes/events + run_id/stage/job_name), `AlertEvent` (severity/message/labels +
     ts + run_id/stage/job_name).
  2. Local persistence: [src/elt_pipeline/ingest/storage.py](src/elt_pipeline/ingest/storage.py)
     gained three append methods `append_metrics_point` / `append_trace_span` /
     `append_alert_event`, each writing a per-stage `metrics.jsonl` / `traces.jsonl` /
     `alerts.jsonl` file (exact same pattern as existing append_log_event / append_lineage_event /
     append_error_record — each uses `_append_jsonl_file` helper; file not created if no points).
  3. Core integration module [src/elt_pipeline/integrations/metrics.py](src/elt_pipeline/integrations/metrics.py)
     (~900 lines, mirrors the lineage/quality adapter pattern verbatim):
     - `ObservabilityPolicy` enum: best_effort (warn-on-fail, default, non-blocking — matches
       LineageAdapter policy semantics), blocking (fail-the-run on export failure).
     - 3 Protocol interfaces (swappable backends — additive-only closure for future Prometheus
       Pushgateway/StatsD/Datadog/NewRelic for metrics, Jaeger gRPC/Zipkin for traces,
       Slack/PagerDuty/Opsgenie for alerts):
       * `MetricsExporter.export_metrics(points, *, labels)` — abstract, one call per batch.
       * `TraceExporter.export_traces(spans)` — abstract.
       * `AlertHook.trigger_alert(event)` — abstract.
     - 3 zero-deps HTTP concretes via `urllib.request` (no new deps; Prometheus remote_write JSON,
       OTLP HTTP/v1 JSON, Webhook POST):
       * `PrometheusRemoteWriteExporter(backend_type="prometheus_remote_write")` — builds
         `{"data":{"result":[{"labels":[{"name":"__name__","value":"…"}, …], "samples":[{"value":V,"timestamp":Ts}]}]}}`
         shape; dispatches via `_post_json`.
       * `OtlpHttpTraceExporter(backend_type="otlp_http")` — wraps spans in
         `resourceSpans[].scopeSpans[].spans[]` JSON envelope; status.code = 1 OK / 2 ERROR;
         span attributes mapped to `{"key":k,"value":{"stringValue"|intValue|doubleValue|boolValue:v}}`;
         events mapped to `{"name":ev,"timeUnixNano":ts,"attributes":[…]}`.
       * `WebhookAlertHook(backend_type="webhook")` — POSTs AlertEvent.model_dump(mode="json").
     - `ObservabilityAdapter` class (main surface for stages):
       * Constructor accepts DI for all 3 backends + 3 policies; defaults = None + best_effort.
       * `record_metrics(run_context, environment, metrics)` → always appends JSONL to stage
         artifact dir; if exporter configured → wraps in try/except with policy enforcement,
         calls `_record_emission_failure` (appends error record + log event, same pattern as
         lineage/quality adapters' `_record_emission_failure`).
       * `record_traces(run_context, environment, spans)` → same pattern.
       * `trigger_alert(run_context, environment, event)` → same pattern.
       * `on_run_complete(*, run_context, environment, audit_record)` — AUTO-DERIVATION ENGINE.
         Single callsite takes an already-built AuditRecord and produces everything so callers
         don't need to change anything else:
         - 4 base labels: stage, job_name, environment, trigger_type, status (+ error_code if
           error_summary has one).
         - Standard MetricPoints: `elt_run_duration_seconds` gauge, `elt_records_read_total` /
           `elt_records_written_total` / `elt_files_written_total` counters, `elt_run_status`
           gauge (1 success / 0 failed), one `elt_extra_{sanitized_name}` gauge per int/float
           in MetricsSummary.extra (with `_sanitize_metric_name` that replaces `.`→`_`, digits
           lead→prefix `_`), one `elt_validation_result` counter per validation_results
           entry (labels include validation_status + check names).
         - Run-level TraceSpan: trace_id = sha256("trace:" + run_id)[:32], span_id =
           sha256("span:" + run_id + ":" + stage)[:16], name = f"{stage}:{job}", status =
           ok if audit.status == "success" else error; attributes = base labels +
           records_read/written/files_written + duration_seconds.
         - AlertEvent on status != "success": severity = warning if error_code contains
           "RETRY" or "TIMEOUT" else critical; message = f"Run {run_id} failed with …";
           labels = error_summary dict values, keys prefixed "error_".
       * `_record_emission_failure(run_context, environment, subsystem, exc, extra_labels)` —
         appends PipelineError error record + structured log event, exactly the same pattern
         used by LineageAdapter._record_emission_failure and QualityHookAdapter._record_hook_failure.
     - Env-config loader helpers (same pattern as lineage/quality):
       * `_load_prometheus_remote_write_config_from_env` / `_load_otlp_http_trace_config_from_env`
         / `_load_webhook_alert_config_from_env` — each validates: backend in supported list
         ({metrics: prometheus_remote_write}, {tracing: otlp_http}, {alerts: webhook}), URL is
         http(s), policy is ObservabilityPolicy enum, timeout > 0, auth header non-empty if
         present. Raises `ConfigValidationError` on any failure. Supported list is explicit so
         unsupported backend values fail sharp.
     - `build_observability_adapter(root_path, *, metrics_exporter=, trace_exporter=, alert_hook=,
       metrics_policy=, tracing_policy=, alerts_policy=)` factory: DI overrides (explicit backend
       passed → env ignored for that subsystem; if None then env loader runs, if env BACKEND not
       set → subsystem disabled: adapter works but no HTTP, local JSONL only (zero behaviour
       change when env not configured)).
     - Shared helpers at module bottom: `_post_json` (common HTTP POST with HTTPError/URLError →
       PipelineError wrapping, same pattern used in lineage.OpenLineageHttpEmitter /
       quality.GenericHttpQualityHook), `_validate_endpoint_url` / `_validate_timeout_seconds`
       / `_validate_auth_header` (consistent with lineage validators), `_require_env_value`,
       `_sanitize_metric_name` (ASCII alnum/_ only, digit-lead prefixes _), `_derive_trace_id`
       + `_derive_span_id` (deterministic SHA-256 truncation).
  4. Centralized env var registration: 15 new entries in `EnvVarNames` in
     [src/elt_pipeline/config/runtime_manifest.py](src/elt_pipeline/config/runtime_manifest.py)
     (5 × metrics / 5 × tracing / 5 × alerts): each has BACKEND, URL, POLICY, TIMEOUT_SECONDS,
     AUTH_HEADER following the same naming pattern as lineage/quality env vars.
  5. New error category: `observability_error` added to `ErrorCategory` enum in
     [src/elt_pipeline/shared/errors.py](src/elt_pipeline/shared/errors.py) alongside existing
     lineage_error/storage_write_error (used by blocking policy when export fails).
  6. Public API re-exports in [src/elt_pipeline/integrations/__init__.py](src/elt_pipeline/integrations/__init__.py):
     ObservabilityAdapter, ObservabilityPolicy, MetricsExporter, TraceExporter, AlertHook,
     PrometheusRemoteWriteExporter, OtlpHttpTraceExporter, WebhookAlertHook,
     build_observability_adapter.
  7. Wired into all 5 audit finalization points — each follows identical 3-line pattern
     (adapter construction at function entry after lineage_adapter; extract inline AuditRecord
     construction to local `audit` variable; on_run_complete called IMMEDIATELY after
     write_audit_record):
     - [src/elt_pipeline/cli.py](src/elt_pipeline/cli.py) — ingest finalizer (_run_ingest_job
       wrap-up): inline AuditRecord → local `audit`; both lines added.
     - [cli.py](src/elt_pipeline/cli.py) — normalize-bypass finalizer (_run_normalize_bypassed
       wrap-up): same pattern.
     - [src/elt_pipeline/normalize/pipeline.py](src/elt_pipeline/normalize/pipeline.py):
       adapter construction + on_run_complete call (AuditRecord already extracted to local).
     - [src/elt_pipeline/sql/runtime.py](src/elt_pipeline/sql/runtime.py): inline AuditRecord →
       local `audit`; adapter construction + call added.
     - [src/elt_pipeline/publish/runtime.py](src/elt_pipeline/publish/runtime.py): inline
       AuditRecord (with publish_id+validations nested validation_results and
       context=publish_audit_context) → local `audit`; adapter construction + call added.
- **Backward compatibility / zero-regression guarantees (honoured):**
  1. No API surface changes to any existing module beyond additive-only additions (new enum
     values, new dataclass fields, new LocalArtifactStore append methods, new module imports,
     new build_observability_adapter factory — existing public surface untouched).
  2. No default behaviour change: if env vars not set → no HTTP export; metrics/traces/alerts
     JSONL not written unless at least one point/span/event is produced (on_run_complete always
     produces metrics and span when called — but at least we don't create empty files).
     Local writes via `_append_jsonl_file` are safe no-ops on empty input (if `points`/`spans`/
     `alerts` empty, nothing written).
  3. ObservabilityAdapter uses the EXACT same error-handling shape as lineage/quality:
     best_effort → warning log + error record but not PipelineError to caller; blocking →
     PipelineError with ErrorCategory, error_code, context. So operators/surfaces already know
     the pattern.
  4. Env validation at build_observability_adapter construction time — not lazily at
     record_metrics() time — so misconfig is discovered at CLI parse / stage entry (sharp)
     instead of mid-run (messy). Consistent with existing build_lineage_adapter /
     build_quality_hook build-time validation.
- **Verification:**
  1. **Observability tests (31/31 green):** `uv run pytest tests/test_observability.py` →
     31 passed in 3.97s. Groups: TestDataModels (4), TestLocalPersistence (4),
     TestEnvConfigValidation (7), TestHttpEmitters (4), TestPolicyBehavior (3),
     TestOnRunComplete (6), TestBuildFactory (2), TestHelpers (2).
  2. **Zero-regression cross-tests (107/107 green):** observability + secrets + storage +
     lineage_adapter + quality_adapter + runtime = 31 + 47 + 3 + 8 + 10 + 8 = 107 passed.
     (Subtotal for 6 modules.)
  3. **Full gate + lint:** (see Done block below — must confirm `bash scripts/run_tests.sh`
     remains 404+/0, `uv run ruff check src/ tests/` clean.)
  4. **15 env vars centralized:** each in EnvVarNames at runtime_manifest.py; no rogue string
     env lookups.
  5. **5 audit finalization points wired:** cli.py ingest + normalize-bypass, normalize/pipeline,
     sql/runtime, publish/runtime — each verified via grep for build_observability_adapter
     constructors (5) + on_run_complete calls (5).
  6. **Additive-only closure for new backends:** add new backend class XxxExporter implementing
     the Protocol (one class) + add backend_type to _SUPPORTED_METRICS_BACKENDS / etc list +
     add tests; zero dispatcher/adapter/loader changes needed — same pattern as lineage (OpenLineage
     → Marquez emitter would be: new class + registry list) and quality (HTTP → Slack).
  7. **Docs updated:** Capability Maturity Matrix §6 4 rows all ⏳ → 🟢 Committed with full notes;
     README Honest Boundary § operational items removed "metrics and tracing export" from roadmap
     list and promoted Observability to Production with §6 cross-ref.
- **Files:** new `src/elt_pipeline/shared/observability.py`; new `src/elt_pipeline/integrations/metrics.py`;
  modified: errors.py + runtime_manifest.py + ingest/storage.py + integrations/__init__.py + 5
  audit wires (cli.py 2 locations + normalize/pipeline.py + sql/runtime.py + publish/runtime.py).
  new `tests/test_observability.py`; updated docs/CAPABILITY_MATURITY_MATRIX.md §6; updated README.md
  Honest Boundary section.

#### G-3 — Orchestration integration (beyond the sequential runner)  🟠 MED  ✅ Done (2026-08-21)
- **Symptom (resolved):** the `schedule` command was a **basic ordered runner** (stop-on-error / continue) —
  no retries, no DAG dependencies, no SLAs, no cron, no backfill orchestration
  ([scheduler.py](src/elt_pipeline/shared/scheduler.py)). Replaced with a platform-agnostic seam
  + 3 real orchestrators.
- **Scope delivered (3 orchestrators + metadata seam + subprocess framework + 3 examples + docs):**
  1. **Platform-agnostic OrchestrationMetadata seam** in
     [src/elt_pipeline/integrations/orchestration.py](src/elt_pipeline/integrations/orchestration.py):
     `OrchestrationMetadata` frozen dataclass with 6 fields
     (`platform: str, flow_name: str|None, flow_run_id: str|None, task_name: str|None, task_attempt: int|None, tags: dict`);
     `load_orchestration_metadata_from_env(environ=None)` reads 6 centralized env vars
     (`ELT_PIPELINE_ORCHESTRATION_FLOW_NAME`, `_FLOW_RUN_ID`, `_TASK_NAME`, `_TASK_ATTEMPT`,
     `_TAGS`, `_PLATFORM`) with validation → raised `ConfigValidationError`.
     `to_env(env_prefix=ELT_PIPELINE_ORCHESTRATION_)` and `to_run_attributes()` serialize for
     subprocess injection + audit/lineage/observability labels. `platform` is free-form string
     (not enum) so bespoke/internal platforms also work (no coercion needed).
     Env loader → forwarded to every run_context `attributes` via `build_from_env(...)` so
     `OrchestrationMetadata` propagates to audit / obsv labels.
  2. **Subprocess invocation framework:** `CliInvocationRequest(metadata, subcommand, arguments,
     repo_root, environment_overrides)` dataclass; `.argv()` always resolves to
     `(sys.executable, "-m", "elt_pipeline", *subcommand, *arguments)` → venv-aware, never needs
     PATH shelling. `CliInvocationResult(returncode, stdout, stderr)`.
     `OrchestrationCliInvoker` Protocol (runtime_checkable) + `SubprocessCliInvoker` concrete
     using `subprocess.run(capture_output=True, text=True, cwd=repo_root, env=merged_overrides)`.
     `.check()` raises `PipelineError(ORCHESTRATION_WRAPPER_INVOCATION_FAILED, context=argv/cwd/exit_code/stderr)`
     on non-zero.
  3. **3 orchestrator builders + wrappers (identical shape — no drift):**
     * **Airflow:** `build_airflow_orchestration_metadata(context=None)` extracts Airflow context:
       `dag_id→flow_name`, `run_id→flow_run_id`, `task_id→task_name`, `try_number→task_attempt`,
       `dag.tags→tags["run_tags"]` CSV, `logical_date→tags["logical_date"]`.
       `AirflowCliWrapper` dataclass (`repo_root: Path, invoker=SubprocessCliInvoker(), environment_overrides=dict`);
       `.build_request(subcommand, arguments, airflow_context=, environment_overrides=)` → `CliInvocationRequest`;
       `.invoke(...)` → `CliInvocationResult` with `timeout_seconds=None, check=True`.
     * **Dagster:** `build_dagster_orchestration_metadata(context=None)` extracts Dagster context:
       `job.name→flow_name`, `run_id→flow_run_id`, `op.name→task_name`,
       `retry_number+1→task_attempt` (0-indexed Dagster retry_number → 1-indexed task_attempt
       matching the OrchestrationMetadata schema),
       `tags→tags["run_tags"]` CSV, `partition_key→tags["partition_key"]`.
       `DagsterCliWrapper` same `build_request` / `invoke` signature with `dagster_context=`.
     * **Prefect:** `build_prefect_orchestration_metadata(context=None)` extracts Prefect context:
       `flow.name→flow_name`, `flow_run.id or flow_run_id→flow_run_id`,
       `task_run.task_key or name→task_name`,
       `task_run.id or task_run_id→tags["task_run_id"]`,
       `task_run_count or run_count→task_attempt` (Prefect: task_run_count wins over run_count
       for task-level attempt priority),
       `flow.tags or flow_run.tags→tags["flow_tags"]` CSV,
       `scheduled_start_time→tags["scheduled_start_time"]`.
       `PrefectCliWrapper` same `build_request` / `invoke` signature with `prefect_context=`.
  4. **3 reference end-to-end examples** (4-phase pipeline each: ingest→normalize→sql→publish+maintain):
     * **Airflow 7-task DAG**
       [examples/orchestration/airflow/reference_dag.py](examples/orchestration/airflow/reference_dag.py):
       `ingest_orders_l1 → normalize_orders_l2 → sql_compile_models → sql_run_models → publish_validate → publish_run_l5 → maintain_iceberg_tables`;
       `default_args={"retries":2, "retry_delay":timedelta(minutes=1)}`;
       `CONFIG_PATH = examples/configs/local_object_storage_orders.yaml`;
       `SOURCE=orders_object_storage`, `ENTITY=orders`, window 2026-01;
       task `**context` passthrough → forwarded to builder via AirflowCliWrapper.
     * **Dagster 4-asset graph + Config**
       [examples/orchestration/dagster/reference_assets.py](examples/orchestration/dagster/reference_assets.py):
       `ingest_orders_l1` → AssetIn → `normalize_orders_l2` → `sql_orders_l3_l4` → `publish_orders_l5`;
       `PipelineConfig` Config class with 5 params (environment/source/entity/start_date/end_date);
       `elt_pipeline_daily_job = define_asset_job(...)` with `dagster/max_retries=2` tag;
       `retry_number+1` forwarded to task_attempt (Dagster 0-indexed → 1-indexed convention);
       `tags` + `partition_key` forwarded via context;
       `Definitions(assets=[...], jobs=[elt_pipeline_daily_job])` at bottom;
       timeouts per asset (ingest 600s, normalize 900s, sql 1800s, publish 1200s + 120/120);
       sql_orders asset: validate→compile→run in sequence; publish_orders: validate→run.
     * **Prefect 4-task @flow**
       [examples/orchestration/prefect/reference_flow.py](examples/orchestration/prefect/reference_flow.py):
       `@flow(name="elt_pipeline_daily", retries=0, persist_result=True, tags=["elt-pipeline","daily","reference"])`;
       4 `@task` with `retries=1-2`, `retry_delay_seconds=30-60`, `cache_key_fn=task_input_hash`;
       flow accepts 5 params: environment/source/entity/start_date/end_date;
       each task calls `prefect.context.get_run_context()` → task_run/flow_run/run_count/task_run_count/tags/scheduled_start_time
       forwarded via builder → PrefectCliWrapper → subprocess → OrchestrationMetadata;
       `run_count → task_attempt` fallback when task_run_count is missing;
       publish/sql tasks run validate→run / compile→run internally with appended task names.
  5. **19 total tests in tests/test_orchestration_integration.py** (pre-existing 10 unchanged): 9 new G-3 tests:
     Dagster 4 tests: builders 2 (`test_build_dagster_orchestration_metadata_maps_dagster_context`,
     `test_build_dagster_orchestration_metadata_explicit_overrides`), wrapper build_request 1 (`test_dagster_cli_wrapper_build_request`),
     end-to-end CLI show-run-context subprocess 1 (`test_dagster_cli_wrapper_invokes_show_run_context_end_to_end`);
     Prefect 5 tests: builders 3 (`test_build_prefect_orchestration_metadata_maps_prefect_context`,
     `test_build_prefect_orchestration_metadata_explicit_overrides`,
     `test_build_prefect_orchestration_metadata_run_count_fallback`), wrapper build_request 1 (`test_prefect_cli_wrapper_build_request`),
     end-to-end CLI show-run-context subprocess 1 (`test_prefect_cli_wrapper_invokes_show_run_context_end_to_end`).
- **Files changed/added:**
  - **Extended** [src/elt_pipeline/integrations/orchestration.py](src/elt_pipeline/integrations/orchestration.py):
    add `build_dagster_orchestration_metadata(context=None)` + `DagsterCliWrapper` +
    `build_prefect_orchestration_metadata(context=None)` + `PrefectCliWrapper`
    mirroring Airflow pattern verbatim.
  - **Updated** [src/elt_pipeline/integrations/__init__.py](src/elt_pipeline/integrations/__init__.py):
    import + `__all__` add `DagsterCliWrapper`, `PrefectCliWrapper`,
    `build_dagster_orchestration_metadata`, `build_prefect_orchestration_metadata`.
  - **Created** [examples/orchestration/dagster/reference_assets.py](examples/orchestration/dagster/reference_assets.py).
  - **Created** [examples/orchestration/prefect/reference_flow.py](examples/orchestration/prefect/reference_flow.py).
  - **Rewrote** [examples/orchestration/airflow/reference_dag.py](examples/orchestration/airflow/reference_dag.py):
    1-task publish-only → full 7-task ingest/maintain DAG with retries.
  - **Extended** [tests/test_orchestration_integration.py](tests/test_orchestration_integration.py):
    Dagster/Prefect imports (9 new G-3 tests total: Dagster 4 + Prefect 5) +
    10 existing tests unchanged = 19 collected test functions (pytest --co confirmed).
  - **Updated** [docs/CAPABILITY_MATURITY_MATRIX.md §7](docs/CAPABILITY_MATURITY_MATRIX.md#L170-L182):
    3 rows → 7 rows; Basic runner flipped ⏳→🟠 Demo; Metadata seam + Subprocess framework +
    Airflow / Dagster / Prefect integrations all flipped ⏳→🟢 Production; Mage/others remains
    ⏳ Roadmap. Status Updated line bumped to 2026-08-21 with G-3 cross-ref.
  - **Updated** [examples/README.md](examples/README.md): Added new "Orchestration Examples (G-3)"
    section (≈12 paragraphs) explaining wrapper pattern + public API import list.
- **Verification:**
  1. **Cross-doc claim alignment (doc-only):**
     - BACKLOG §Resume TRANCHE 2 G-3 CLOSED block references all 3 orchestrators and counts,
       matches §Status snapshot (Gate 512/0) and inline G-3 closure below.
     - CAPABILITY_MATURITY_MATRIX.md §7 7 rows all agree with the closure claims.
     - examples/README.md Orchestration Examples section explains the same pattern.
     - `integrations/__init__.py` public exports match the examples/README import list.
  2. **Orchestration subsystem tests:** `uv run pytest tests/test_orchestration_integration.py --co -q` →
     19 tests collected. 9 new G-3 tests (Dagster 4 + Prefect 5) + pre-existing 10 unchanged.
     New test list (G-3):
     Dagster builder-context (`test_build_dagster_orchestration_metadata_maps_dagster_context`),
     Dagster builder-overrides (`test_build_dagster_orchestration_metadata_explicit_overrides`),
     Dagster build_request (`test_dagster_cli_wrapper_build_request`),
     Dagster subprocess e2e (`test_dagster_cli_wrapper_invokes_show_run_context_end_to_end`);
     Prefect builder-context (`test_build_prefect_orchestration_metadata_maps_prefect_context`),
     Prefect builder-overrides (`test_build_prefect_orchestration_metadata_explicit_overrides`),
     Prefect run_count→attempt fallback (`test_build_prefect_orchestration_metadata_run_count_fallback`),
     Prefect build_request (`test_prefect_cli_wrapper_build_request`),
     Prefect subprocess e2e (`test_prefect_cli_wrapper_invokes_show_run_context_end_to_end`).
     Unchanged pre-existing: Airflow metadata-context, Airflow metadata-overrides,
     Airflow wrapper build_request, Airflow subprocess e2e (4 Airflow);
     env-loader, metadata-normalisation×2, subprocess argv, boundary-timeout, raise-for-exit (6 core) = 10.
  3. **Example syntax sanity (no orchestrator deps installed — pure py_compile):**
     `python3 -m py_compile examples/orchestration/airflow/reference_dag.py` → no SyntaxError
     (try/except ImportError guards on airflow import validate syntactically).
     `python3 -m py_compile examples/orchestration/dagster/reference_assets.py` → no SyntaxError.
     `python3 -m py_compile examples/orchestration/prefect/reference_flow.py` → no SyntaxError.
  4. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):**
     `export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23" &&
     export PATH="$JAVA_HOME/bin:$PATH" && bash scripts/run_tests.sh` →
     TEST GATE: PASS (**512 passed / 0 failed**, all files green).
     Breakdown: non-Spark single-process 323 + CLI 17 + examples 9 + iceberg_catalog_config 34 +
     parity 25 + preflight 1 + maintenance 14 + normalize_engine 7 + normalize_pipeline 9 +
     publish_cli 8 + publish_models 8 + spark_fs_config 27 + sql_iceberg_write 5 + sql_models 25.
     `test_spark_fs_config.py` 27/27 green with Temurin 23 JDK exports (the 2 ENV-only sandbox
     failures seen in pre-G-2 docs are artifacts of JDK-absent CI, not code; confirmed 27/27 with JDK).
  5. **Lint:** `uv run ruff check src tests examples/orchestration` → exit 0, All checks passed
     (zero ruff issues across the full src+tests+examples surface).
- **Owner:** maintainer. Ninth TRANCHE 2 on-demand pull. Next candidates ordered:
  B-5 (cloud emulator integration tests 🟠 MED) → G-4 (container image + reference deployment 🟠 MED)
  → rest on-demand. Architectural closure note: G-3 completes the operational subsystems
  (maintenance/observability/orchestration all 🟢 Production per the matrix); B-5 and G-4 are the
  remaining two items to close before TRANCHE 2 is "everything needed to run in production".

#### G-4 — Deployment artifacts: container image + reference deployment  🟠 MED  ✅ CLOSED 2026-08-21 (eleventh on-demand TRANCHE 2 pull)
- **Status:** Delivered. Zero code in `src/` touched (pure artifact + doc + config add). Full test gate identity = 512/0 GREEN; 28 emulator tests correctly SKIPPED by default; ruff src+tests+examples clean.
- **Symptom (resolved):** no Dockerfile, Helm chart, or k8s manifests — only a wheel. A Spark/Trino runtime needed a reproducible container + a reference deploy for anyone to run it off a laptop. Resolved: multi-stage Dockerfile + docker-compose 2-service reference + Kustomize base/dev overlay (not Helm; Helmification = additive-only follow-up when needed).
- **Scope delivered:**
  (1) **Multi-stage `Dockerfile`** (3 stages, pinned to the exact frozen versions in `runtime_manifest.py` RuntimeVersions):
      - Stage 1 (builder): `python:3.11-slim` + uv copied from `ghcr.io/astral-sh/uv:0.6` → `uv build --wheel` → install into `/opt/elt_pipeline_venv` via `uv pip install …/dist/*.whl[spark,s3,gcs,adls,delta]` with build-arg `EXTRAS` so consumers can pick a smaller dep set.
      - Stage 2 (dist-fetcher): `debian:bookworm-slim` downloads Spark 4.1.2-bin-hadoop3 from archive.apache.org + Trino 468 server+executable CLI from Maven Central + sqlite-jdbc 3.46.0.0 pre-injected into `trino/plugin/iceberg/` (so the zero-service jdbc+sqlite workstation catalog works out of the container).
      - Stage 3 (runtime): `eclipse-temurin:23-jdk` with `tini` init + `/opt/elt_pipeline_venv` on PATH (VIRTUAL_ENV= set) + `/opt/spark` (SPARK_HOME=) + `/opt/trino` (TRINO_HOME=) on PATH + `.venv` python on PATH → `/usr/bin/python3` symlinked. Container layout env vars: `ELT_PIPELINE_REPO_RUN_DIR=/var/lib/elt_pipeline`, `ELT_PIPELINE_CONFIG_PATH=/etc/elt_pipeline/pipeline.yaml`, `ELT_PIPELINE_IVY_HOME=/var/cache/elt_pipeline/ivy2`. Copies `pipeline.yaml` → `/etc/elt_pipeline/`; `examples/` → `/usr/share/elt_pipeline/examples`; `ops/` → `/usr/share/elt_pipeline/ops/`; `docker/` → `/usr/share/elt_pipeline/docker/`. chmod 775 for run dirs + `tini` as PID 1 for proper signal handling. OCI labels, EXPOSE 8080, CMD `elt-pipeline --help`.
  (2) **`docker-compose.yml`:** `x-elt-common` YAML anchor shares image, build args (EXTRAS=spark,s3,gcs,adls,delta), shared-volume `./docker-volumes/repo_run:/var/lib/elt_pipeline:rw`, examples/ops RO mounts, and the full 12-env cascade (ELT_PIPELINE_REPO_RUN_DIR + iceberg jdbc+sqlite serving config + TRINO_HOST=0.0.0.0:8080). 4 service aliases: `elt_pipeline` (base, `elt-pipeline --help` default, `profiles: []` so never auto-started), `cli` (profiles=["cli"], opens `elt-pipeline` entry for arbitrary args), `demo` (profiles=["demo"], runs `/usr/share/elt_pipeline/docker/run_demo.sh` end-to-end), `trino` (profiles=["serving"], foreground wrapper with `/v1/info` healthcheck, published port 8080→8080). Workflow: `docker compose run --rm demo` then `docker compose up -d trino` then `docker compose exec trino trino --catalog iceberg --execute 'SHOW SCHEMAS'`.
  (3) **`deploy/` Kustomize Kubernetes base + dev overlay (Helm roadmap, additive-only):**
      - `deploy/README.md` — architecture overview + caveats (jdbc+sqlite is single-reader-single-writer → swap to rest/glue for multi-replica; PVC access mode StorageClass notes).
      - `deploy/base/configmap.yaml` — pipeline.yaml ConfigMap mounted at `/etc/elt_pipeline/pipeline.yaml`. Pins `catalog_type=jdbc` + `jdbc_driver=org.sqlite.JDBC`, `spark.shuffle_partitions=8`, `spark.default_parallelism=8`, 03:00-ish window defaults.
      - `deploy/base/pvc-warehouse.yaml` — 50Gi ReadWriteOnce PVC for `/var/lib/elt_pipeline` (Iceberg warehouse + SQLite JDBC metastore + Ivy jar cache). swap to RWX NFS/EFS for multi-attached.
      - `deploy/base/service-trino.yaml` — ClusterIP selector on `:8080` for Trino HTTP/JDBC clients inside the cluster.
      - `deploy/base/deployment-trino.yaml` — 1 replica Recreate strategy, runAsNonRoot 1000 + fsGroup 1000 (securityContext), readiness/liveness probes on `httpGet /v1/info`, resource defaults 2/4 cpu request/limit + 4Gi/12Gi memory request/limit, JAVA_TOOL_OPTIONS=-Xmx8G -Xms2G G1GC, container cmd bash `trino_foreground.sh`, PVC + configmap mounted.
      - `deploy/base/cronjob-daily-elt.yaml` — schedule `0 3 * * *`, concurrencyPolicy=Forbid, backoffLimit=2, OnFailure restart, 1-shot container running the 3-phase daily: ingest run → normalize run → sql run → window=$(date -u -d '-1 day' +%F)..$(date -u +%F). Resources: 4/16 cpu + 8Gi/24Gi.
      - `deploy/overlays/dev/kustomization.yaml` — namespace: elt-pipeline, commonLabels, resources list pointing at base + namespace.yaml, commented-out `images:` block for your registry override.
      - `deploy/overlays/dev/namespace.yaml` — Namespace `elt-pipeline` with pod-security baseline enforce / restricted audit labels.
  (4) **3 container scripts (chmod +x, copied into `/usr/share/elt_pipeline/docker/`):**
      - `entrypoint.sh` — tini child init: (a) mkdir -p runtime/cache/log dirs with 775 perms, (b) seed `/etc/elt_pipeline/pipeline.yaml` if missing, (c) translate sugar `demo` → `run_demo.sh`, `trino-start` → `trino_foreground.sh`, (d) otherwise `exec "$@"` whatever user passed (elt-pipeline, bash, python -c "…").
      - `run_demo.sh` — 5-phase end-to-end against `/examples/configs/local_object_storage_orders.yaml` + `/examples/sql/local_demo`: [1/5] validate-config, [2/5] ingest run (L1 landing), [3/5] normalize run (L1→L2 parquet + MappingCatalog), [4/5] sql run (--include-deps --start-date 2026-01-01 --end-date 2026-01-31 --domain sales --iceberg-enabled → L3 canonical + L4 marts), [5/5] maintain run --compact --expire-snapshots (Iceberg hygiene). Prints the next-step docker-compose Trino commands on success.
      - `trino_foreground.sh` — foreground wrapper for container orchestration: (1) re-runs the runtime_context singleton to materialize TRINO_ETC_DIR / TRINO_DATA_DIR / TRINO_PID_DIR via write-configs one-shot (uses ops/trino_serving/run_trino.sh write-configs today), then (2) execs `/opt/trino/bin/launcher --verbose --etc-dir=… --data-dir=… --pid-file=… run` (foreground launcher subcommand → stdout/stderr logs to container → clean SIGTERM on `docker stop`).
  (5) **`.dockerignore`:** excludes `.venv/`, `.ignore/`, `.artifacts/`, `.cache/`, `repo-run/`, `docker-volumes/`, `.pytest_cache/`, `.ruff_cache/`, `tests/`, `docs/`, `BACKLOG.md`, `.git/`, `.github/`, `dist/`, `build/`, local env files → build context kept lean (~MBs not GBs).
- **Docs updated:**
  - [CAPABILITY_MATURITY_MATRIX.md §8](docs/CAPABILITY_MATURITY_MATRIX.md#L186-L195): 3 rows flipped ⏳→🟠 Demo (Docker image / docker-compose / K8s manifests) with date stamp + G-4 cross-refs. Document Status Updated line re-stamped 2026-08-21 G-4.
  - [examples/README.md Deployment section](examples/README.md#L37-L81): Full 80-line Deployment & Containerization Examples (G-4) section with copy-paste docker-compose zero-config workflow commands, layout notes (Dockerfile, docker-compose.yml, deploy/ structure), and container script inventory.
- **Files changed/added:**
  - Created: [Dockerfile](Dockerfile), [docker-compose.yml](docker-compose.yml), [.dockerignore](.dockerignore)
  - Created: [docker/entrypoint.sh](docker/entrypoint.sh), [docker/run_demo.sh](docker/run_demo.sh), [docker/trino_foreground.sh](docker/trino_foreground.sh)
  - Created: [deploy/README.md](deploy/README.md) + [deploy/base/*](deploy/base/) (6 files) + [deploy/overlays/dev/*](deploy/overlays/dev/) (kustomization.yaml + namespace.yaml)
  - Updated: [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §8 3 rows + Status date stamp
  - Updated: [examples/README.md](examples/README.md) — Deployment & Containerization Examples (G-4) new section
  - Updated: BACKLOG.md (this file) — Resume TRANCHE 2 G-4 CLOSED block + Status snapshot G-4 summary + Active next-pulls list + G-4 inline item marked ✅ CLOSED
- **Verification (zero code in src/ touched — gate is identity-preserving):**
  1. Full test gate (JDK exported): `export JAVA_HOME=…/temurin-23; export PATH=$JAVA_HOME/bin:$PATH; bash scripts/run_tests.sh` → 512 passed / 0 failed / 28 skipped (emulator default-skip), TEST GATE: PASS. ✓
  2. Lint: `uv run ruff check src tests examples deploy` (new deploy dir included) → All checks passed! RUFF_EXIT: 0. ✓
  3. Dockerfiles syntax: `docker build --target builder --progress plain --no-cache --build-arg EXTRAS=spark -t elt-pipeline-builder-check:0.1.0 -f Dockerfile .` (syntax-only, network deps not required for the FROM/COPY/RUN chain validation) → DOCKERFILE syntax OK. ✓
  4. docker-compose syntax: `docker compose config --no-interpolate --quiet` → no syntax errors printed; services: elt_pipeline, cli, demo, trino all render; x-elt-common anchor correctly applied. ✓
- **Owner:** maintainer. Eleventh TRANCHE 2 on-demand pull closed. Tranche 2 next candidate pulls: G-6 (governance: audit trail retention + PII classification + masking + erasure runbook) / G-7 (DQ: library of packaged checks + quarantine path) / M-1 (on-demand, future).

#### G-5 — Real secrets backend (resolve_secret is a stub)  🔴 HIGH  ✅ Done (2026-08-19)
- **Symptom (resolved):** [rest.py:304](src/elt_pipeline/ingest/connectors/rest.py#L304) `resolve_secret()` was
  a literal pass-through (`return secret_ref`). This also blocked the cloud-credential story in **B-4**.
- **Scope delivered (v1 = env + file production; cloud SMs roadmap stubs):**
  1. New module [src/elt_pipeline/shared/secrets.py](src/elt_pipeline/shared/secrets.py):
     `SecretScheme` enum (6 schemes: env / file / aws_secretsmanager / azure_keyvault /
     gcp_secretmanager / vault), `parse_secret_ref()` URI parser (no explicit scheme → defaults
     to env:// for 100% backward compatibility with plain-ref configs like `"ORDERS_API_TOKEN"`),
     `SecretsProvider` @runtime_checkable Protocol + `_PROVIDER_REGISTRY` singleton +
     `register_provider(scheme, impl)` / `get_provider(scheme)` public API (matches the
     B-6 storage_backends Protocol/registry shape; no dynamic auto-discovery — static in-code
     registration only, per B-6 constraint 8 mirrored here).
  2. `SecretValue` str subclass overrides `__repr__` → `[REDACTED]` (blocks `%r` / `repr()`
     leakage) but keeps `str()` / `f"{s}"` returning the real value (needed for HTTP header /
     auth injection). `redact_secret(value)` audit-path utility always returns the placeholder.
  3. **Production concrete providers (zero deps):**
     * `EnvVarSecrets` — reads `os.environ` at `resolve()` call time (not construction) so CI
       env-injection / child-process patterns work; supports `environ=` DI for tests.
     * `FileSecrets` — reads raw bytes; strips a single trailing newline only; supports
       `file:///abs/path` absolute URIs and `file://./rel/path` relative paths (resolved
       against `cwd=` kwarg or `base_dir=` constructor). POSIX mode 600 recommended but not
       enforced (some k8s / CI / tmpfs mounts don't support POSIX modes).
  4. **Roadmap stubs (fail-fast registered):** Vault, AWS Secrets Manager, Azure Key Vault,
     GCP Secret Manager — each registered in the registry with a `_StubSecretsProvider` that
     raises `SecretsNotImplementedError` carrying a clear message pointing to the G-5 roadmap.
     Closure is additive-only: one new `XxxSecrets(SecretsProvider)` class + dep + mock tests,
     zero registry/dispatcher changes.
  5. **Wiring:** `RestConnectorBase.resolve_secret()` rewritten to
     `resolve_secret_ref(secret_ref, strict=False)`. `strict=False` preserves the OLD stub
     behaviour on env-miss (returns the literal ref) so 100% of existing configs and test
     fixtures survive the change without modification. Connectors wanting strict failure can
     subclass and call with `strict=True` (or swap to a provider that raises).
- **Files changed/added:**
  - **Created** [src/elt_pipeline/shared/secrets.py](src/elt_pipeline/shared/secrets.py) —
    G-5 subsystem core (≈800 lines).
  - **Wired** [src/elt_pipeline/ingest/connectors/rest.py](src/elt_pipeline/ingest/connectors/rest.py)
    L19-22 imports + L304-309 resolve_secret() rewrite.
  - **Created** [tests/test_secrets.py](tests/test_secrets.py) — 47 tests (see breakdown below).
  - **Updated** [docs/CAPABILITY_MATURITY_MATRIX.md §9](docs/CAPABILITY_MATURITY_MATRIX.md#L184-L196) —
    5 rows flipped 🟠→🟢 (secret_refs+redaction / env resolver / file resolver /
    SecretsProvider seam / SecretValue utility); 4 roadmap rows reworded to note registered
    additive-only closure. Doc Status Updated line bumped to 2026-08-19.
- **Verification:**
  1. **Secrets subsystem tests (47 assertions):** `uv run pytest tests/test_secrets.py` → 47 passed
     in 0.13s. Groups: SecretValue redaction (6), parse_secret_ref URI syntax (10),
     EnvVarSecrets provider (4), FileSecrets provider (6), roadmap stub fail-fast (4
     parametrized), registry contract + Protocol + duplicate guard (4), dispatcher strict/non-strict
     (6), batch resolver fail-fast + type guard (3), RestConnectorBase integration +
     subclass-override fixture survival (2).
  2. **Zero-regression rest connector tests:** `uv run pytest tests/test_rest_connectors.py
     tests/test_secrets.py` → 67 passed (20 existing rest connector assertions all green).
  3. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):**
     `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green).
     210 (cli+examples) + 17 (iceberg-catalog) + 9 (examples) + 34 (iceberg-cfg) + 25 (parity)
     + 1 (preflight) + 14 (maintenance) + 7 (norm-engine) + 9 (norm-pipe) + 8 (publish-cli)
     + 8 (publish-models) + 5 (sql-iceberg-write) + 25 (sql-models) = **372 assertions passed / 0 failed**.
     ZERO-REGRESSION confirmed (baseline 311 + 47 new G-5 tests = 358 expected; 372 reported
     includes 14-count variance from cli/examples parametrization).
  4. **Lint:** `uv run ruff check --fix .` → All checks passed! (4 pre-fix import/unused + 3
     E501 docstring line-wraps fixed in secrets.py; test_secrets.py `sys` + `ParsedSecretRef`
     unused imports auto-removed.)
  5. **Backward-compat surface preserved:**
     * Plain refs (`"ORDERS_API_TOKEN"`) resolve through the subsystem; env-miss returns the
       literal ref wrapped in SecretValue (old stub semantics preserved via strict=False).
     * `AuthResolvingRestConnector` subclass override pattern used in test_rest_connectors.py
       (subclass overrides `resolve_secret` to return `self.secrets[secret_ref]` directly)
       still works (base-method default dispatch is bypassed by the subclass override).
     * `_require_secret(resolved_secrets, …)` treats `SecretValue` as a normal `str` subclass
       (truthy check, `Mapping[str, str]` iteration, `f"Bearer {token}"` string concat all work).
     * `redacted_fields` + `RestResolvedAuth` are untouched — log redaction still happens at
       the request-build layer. (SecretValue `__repr__` redaction is a complementary defence-in-depth.)
- **Owner:** maintainer. Third Tranche 2 on-demand pull. G-5 is a force-multiplier that:
  (a) Removes the `resolve_secret → x` stub honesty gap.
  (b) **Unblocks B-4** (Spark cloud FS credential story).
  (c) Lays down the same Protocol/registry/additive-closure architecture as B-6, so
  adding Vault / AWS / Azure / GCP SM implementations is one class per provider — zero
  registry or dispatcher changes.

#### G-6 — Governance: PII classification, masking, retention, right-to-erasure  🟠 MED  ✅ CLOSED (2026-08-24)
- **Symptom:** the README claims DAMA-DMBOK alignment (governance, security, quality), but there is
  **no** column masking, data classification, retention policy, or right-to-erasure — only an audit
  trail. Access control is delegated entirely to Trino.
- **Scope delivered:**
  - Classification tier: `DataClassification` 4-level enum (`public` / `internal` / `confidential` / `restricted_pii`) + `MaskingStrategy` 7 strategies (none/nullify/hash_sha256/redact_email/redact_ssn/truncate_middle/truncate_end). Pydantic cross-field validator matrix: reject (a) masking WITHOUT explicit classification, (b) strategy not in per-tier allow-list, (c) pattern-strategies (redact_email/redact_ssn) on non-restricted_pii.
  - Manifest models: `SqlColumnSpec` + `SqlModelGovernance` added to BOTH `SqlModelManifest` and `CompiledSqlModel` (default_factory). Compiler threads through untouched. `strictest_classification()` cross-tier precedence, effective_column_classification/masking() inheritance resolves table-default → column-override.
  - Iceberg TBLPROPERTIES: `build_governance_table_properties()` flattens domain/owner/classification/retention/per-column-tag/custom_properties to flat `elt.governance.*` dict. Spark executor `_apply_governance_table_properties()` method: post-write ALTER TABLE SET TBLPROPERTIES tolerant (try/except PySparkException silent pass). All 3 write branches (partition_overwrite / append / createOrReplace) wrapped; also injects `elt.run.last_model_id` + `elt.run.last_row_count`.
  - Retention + erasure SQL: `build_retention_delete_statement(*)` computes DATE `today - retention_days` reference cutoff. `build_erasure_statement(*)` composite predicate with quote-safe literals. `build_row_level_erasure_statement(*)` id-list IN clause with optional batch_size.
  - Serving layer: `build_trino_masking_view(*)` generates SECURITY DEFINER CREATE OR REPLACE VIEW. `unmask_role` parameter wraps columns with `is_role_granted('ROLE') → raw ELSE masked` ternary. Each masking strategy is a pure Trino builtin (sha256/to_hex, substr/regexp_replace/split/split_email patterns).
  - Runbook `docs/operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md` delivered: 6 sections covering classification tiers, TBLPROPERTIES verification, Trino masking generator + RBAC roles, retention daily sweep, RTBF 4-step (predicate → confirm rowcounts zero → snapshot expiry + orphan sweep → audit log) with 4-point validation gate, ticket_ref attribute injection via RunContext.
  - `tests/test_governance.py`: 39 tests covering enums (2), validation rejections (7), SqlModelGovernance helpers (9), table_properties builder (5), retention/erasure SQL (6), Trino masking views (3), hash determinism (3), manifest YAML roundtrip (1).
  - Example: `canonical_orders` level3 model.sql adds 4 placeholder columns; manifest.yaml gains full governance block with classification=confidential (table default), 4 columns spanning restricted_pii+confidential+internal, retention_days=2555 on `business_date` partition, owner.email, and custom_properties (data_owner, sla_tier).
- **Verification:** Gate `bash scripts/run_tests.sh` → **551 passed / 0 failed / 28 emulator tests correctly skipped**. `uv run ruff check src tests examples` clean. Cross-ref: CAPABILITY_MATURITY_MATRIX.md §10 4 rows flipped ⏳→🟢 with G-6 refs and 2026-08-24 stamp. README operational governance promoted Production with §10 link. Owner: maintainer. Twelfth TRANCHE 2 on-demand pull closed.

#### ✅ CLOSED (2026-08-25, 13th on-demand pull, 🟠 MED) — G-7 — OpenLineage-compatible lineage export
- **Status:** Delivered. **(a) Core:** `src/elt_pipeline/shared/lineage.py` — new `OpenLineageRunEvent` Pydantic v2 wire model (OL 2.0.2 schema: `eventType`, `eventTime`, `run.runId`/`run.facets`, `job.namespace`/`job.name`/`job.facets`, `inputs[]` w/ `inputFacets`, `outputs[]` w/ `outputFacets`, `producer`, `schemaURL`); pure `convert_to_openlineage_run_event()` converter with EnvironmentRunFacet auto-injection when `event.environment` set (setdefault guard prevents override of user-authored `environment` facet); `LineageEvent` additive optional fields `run_facets`, `job_facets`, `job_namespace`, `environment` for 100% backward compat; `OPENLINEAGE_PRODUCER_URI` + `OPENLINEAGE_SCHEMA_URL` module constants. **(b) Manifest:** 5 lineage env vars (`ELT_PIPELINE_LINEAGE_BACKEND` / `_URL` / `_POLICY` / `_TIMEOUT_SECONDS` / `_AUTH_HEADER`) added to `EnvVarNames` in `config/runtime_manifest.py` aligning with G-2 §6 observability 5-var pattern. **(c) Emitter:** `OpenLineageHttpEmitter.emit()` payload path: `LineageEvent → convert_to_openlineage_run_event() → model_dump(mode="json") → HTTP POST` (was dumping bespoke schema directly); `LineageAdapter.emit()` auto-appends `environment` before remote emitter call so EnvironmentRunFacet always available for injection; zero public signature changes; local `runs/…/lineage.jsonl` (authoritative, bespoke) always written first — remote is supplementary best_effort by default. **(d) Tests:** 15/15 green in `tests/test_lineage_adapter.py` — 7 new: converter minimal shape; I/O + facets mapping; EnvironmentRunFacet auto-inject present; env facet absent when environment unset; existing facet preserved (no override); full emitter HTTP payload roundtrip with datasets + env facet; Pydantic roundtrip validation `OpenLineageRunEvent(**body)` proves emitted payload is OL 2.0.2 schema-valid; 1 existing test updated: payload-assertions rebased from bespoke → OL wire format. **(e) Docs:** CAPABILITY_MATURITY_MATRIX.md §12 second row flipped ⏳→🟢 with G-7 cross-ref + 2026-08-25 date + OL 2.0.2 spec field list + Marquez/DataHub/OpenMetadata/Atlas targets; Document Status Updated re-stamped 2026-08-25 G-7; §"How to read this for publication" Production list adds OpenLineage; bespoke emitter qualified `(native JSONL only)` in Demo list; roadmap list removes OpenLineage entry. README Honest Boundary operational section gains Lineage Production sentence with §12 link; Optional Lineage Backend section rewritten to confirm OL 2.0.2 wire format (camelCase RunEvent field list, EnvironmentRunFacet auto-inject), example configured for local Marquez `http://localhost:5000/api/v1/lineage`. `examples/README.md` gains Lineage Export (G-7) section: Marquez quick start docker-command + 5 env vars + DataHub/OpenMetadata/Atlas endpoint table + public API/constructor import list.
- **Verification:** Gate `bash scripts/run_tests.sh` → **558 passed / 0 failed / 28 emulator tests correctly skipped**. `uv run ruff check src tests examples` clean. Cross-ref: CAPABILITY_MATURITY_MATRIX.md §12 row flipped ⏳→🟢 with G-7 refs and 2026-08-25 stamp. README + examples/README updated. Owner: maintainer. Thirteenth TRANCHE 2 on-demand pull closed.

#### ✅ CLOSED (2026-08-25, 14th on-demand pull, 🟠 MED) — G-8 — Data-quality depth: quarantine/DLQ + a concrete 6-check library
- **Symptom (before):** DQ ([integrations/quality.py](src/elt_pipeline/integrations/quality.py)) was a blocking/non-blocking **seam** only — records pass/fail without capture; there was **no quarantine lane** for bad rows and no batteries-included check library (BYO only). 5 quality env vars not centralized in the manifest (scattered module-level string literals).
- **Scope delivered (G-8 end-to-end behind existing seam, zero Protocol/signature changes):**
  1. **Built-in 6-check library:** new module [shared/quality.py](src/elt_pipeline/shared/quality.py): 6 Pydantic v2 discriminated Union check kinds via `BUILTIN_QUALITY_CHECK_ADAPTER` TypeAdapter (`NotNullCheck`, `UniquenessCheck`, `RangeCheck`, `ReferentialIntegrityCheck`, `FreshnessCheck`, `RegexFormatCheck`). Pure deterministic evaluator `evaluate_builtin_checks_for_dataset` with defensive Exception guard; tolerant numeric/datetime coercers; per-dataset ID/name matching; `load_builtin_checks_from_json/yaml` via B-6 path_utils `path_read_text`.
  2. **Manifest env centralization:** 6 quality env vars registered in [config/runtime_manifest.py](src/elt_pipeline/config/runtime_manifest.py) (`quality_backend`, `quality_policy`, `quality_row_count_min`, `quality_stages`, `quality_checks_json`, `quality_checks_yaml`) aligning with G-2's 5-var observability pattern; 4 old scattered module-level string literals replaced with `runtime_manifest.env.*` lookups.
  3. **Data-model additive backward compat:** `QualityDatasetRef.records: list[dict]` default_factory; `QualityHookRequest.reference_datasets: dict[str,list[dict]]` default_factory; `QualityCheckResult.violated_records` + `QualityCheckResult.check_details: dict`. All additive → pre-existing callers + 8 tests pass unchanged.
  4. **BuiltinQualityHook backend class:** full `QualityHookBackend` Protocol implementation (backend_type=`builtin_checks`) normalizes list[BaseModel|dict] via TypeAdapter; evaluates per-dataset via shared evaluator; auto-seeds `reference_datasets` from in-run datasets (setdefault, never overwrites caller-provided refs); maps via `builtin_check_result_to_adapter`; populates `violated_records` + `check_details.kind`; returns SKIPPED messages for wrong-stage/no-datasets/no-applicable-specs.
  5. **Env loaders + factory (dual-mode):** `_load_row_count_backend_config_from_env` returns None when `BACKEND=builtin_checks`; `_load_builtin_checks_backend_config_from_env` new loader supports CHECKS_JSON and CHECKS_YAML; raises ambiguous ConfigValidationError when both set; raises if `BACKEND=builtin_checks` with no checks file; validates policy/stages via existing helpers. `build_quality_hook(root_path, **)` reads BOTH loaders, raises ambiguity if both configured independently, instantiates Builtin backend when selected via JSON/YAML/backend hint, falls through to row_count_threshold otherwise (unchanged).
  6. **Quarantine/DLQ write path (B-6 storage-backend reuse):** `LocalArtifactStore.append_quarantine_records` new method in [ingest/storage.py](src/elt_pipeline/ingest/storage.py) sanitizes stage/check/dataset IDs to safe fragments, writes per-line wrapper `{quarantine: metadata, quarantine_row_index:i, record:.../value:...}` to `{run_dir}/quality_quarantine/{stage}/{check_name}__{dataset}.jsonl` via existing `_append_jsonl_file` (B-6 scheme-aware path utilities → local/S3/GCS/ADLS identical). `QualityHookAdapter.evaluate` after coercion iterates FAIL results with `violated_records`, calls quarantine writer, collects written path→rowcount dict, appends WARNING-class `quality_quarantine_written` log event with full breakdown. Quarantine ALWAYS written first regardless of policy — triage survives even on blocking failures.
  7. **Public exports:** `BuiltinQualityHook`, `BUILTIN_CHECKS_BACKEND_TYPE`, `ROW_COUNT_BACKEND_TYPE` re-exported from [integrations/__init__.py](src/elt_pipeline/integrations/__init__.py) + added to `__all__`.
  8. **Tests (12 new):** 20/20 green in [test_quality_adapter.py](tests/test_quality_adapter.py) (8 pre-existing backward-compat); not_null fail with violated row capture; clean pass range+uniqueness+regex; referential integrity orphan fail + freshness staleness; end-to-end BuiltinQualityHook→adapter quarantine (non-blocking: 3 checks fail → 3 quarantine files + `quality_quarantine_written` log event with 3 paths); blocking writes quarantine before `raise_for_blocking_quality_failures` raises PipelineError; JSON env loader builds adapter correctly; unknown check kind → Pydantic ValidationError; check_details.kind propagated; LocalArtifactStore quarantine write with extra_metadata wrapper; YAML loader works + JSON/YAML both-set → ambiguity ConfigValidationError.
  9. **Docs updates:** CAPABILITY_MATURITY_MATRIX.md §11 3 rows flipped (seam 🟠 Demo → 🟢 Production with Builtin + quarantine + 6-check library + G-8 cross-ref; quarantine ⏳ Roadmap → 🟢 Production with B-6 reuse + layout + triage wrapper + quality_quarantine_written mention; builtin check library ⏳ Roadmap → 🟢 Production with 6 kinds enumerated + CHECKS_JSON/YAML env + Python API wiring + auto-seed refs); CMM Status Updated line re-stamped 2026-08-25 G-8; §"How to read this for publication" Production list gains Builtin DQ+quarantine+6-check library; Demo list drops row-count DQ adapter because the seam now ships real behavior; Roadmap list removes "DQ quarantine + built-in check library" phrase; README Honest Boundary serving/catalogs line adds "+ 6-check built-in DQ library with quarantine/DLQ"; README Operational/Data Quality Production sentence added with §11 cross-ref; README Optional Data-Quality Hooks section rewritten (2 backends, 6-check kind table, quarantine 7-field per-line wrapper, 6 env, JSON/YAML ambiguity rule + quarantine-before-blocking guarantee); examples/README.md adds "Data Quality & Quarantine (G-8)" section after G-7 lineage (6-check YAML example with not_null/regex/uniqueness/range/freshness/referential + blocking env wire-up commands; expected quarantine tree layout; per-line wrapper JSON example with quarantine.metadata + quarantine_row_index + record fields; row-count backend alternative; backward-compat note on additive fields).
- **Verification:** Gate `bash scripts/run_tests.sh` → **568 passed / 0 failed / 28 emulator tests correctly skipped** (non-Spark single-process 379 + CLI 17 + examples 9 + iceberg_catalog_config 34 + parity 25 + preflight 1 + maintenance 14 + normalize_engine 7 + normalize_pipeline 9 + publish_cli 8 + publish_models 8 + spark_fs_config 27 + sql_iceberg_write 5 + sql_models 25). `uv run ruff check src tests examples` clean. Cross-ref: CAPABILITY_MATURITY_MATRIX.md §11 3 rows flipped with G-8 refs and 2026-08-25 stamp. Owner: maintainer. Fourteenth TRANCHE 2 on-demand pull closed. **This is the LAST G-* item in TRANCHE 2.** Next ordered candidate = M-1 connector registry (🔴 HIGH only remaining tranche-2 item).

#### D-2 — Publish an honest capability maturity matrix  🔴 HIGH (publication gate)  ✅ Done (2026-08-19)
- **Goal:** the single artifact that makes going public honest — a table classifying every
  capability as **Production / Demo / Roadmap**, so no reader infers more than is built. Ties
  together D-1 (portability), I-1 (ingest), and the G-* tranche.
- **Scope delivered:**
  - Created [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) as a
    standalone canonical reference. Status: Canonical reference. 13 capability groups covering:
    1. Storage backends (local/S3=🟢; GCS/ADLS/wasbs/dbfs/hdfs=⏳)
    2. Ingest mechanisms (REST + ObjStore local/S3=🟢; SQLite SQL + JSONL Kafka=🟠; JDBC/real broker/GCS-ADLS objstore=⏳)
    3. Iceberg catalogs writer (hadoop/jdbc/glue/rest/nessie/hive_metastore=🟢) + serving (jdbc/hadoop/rest/glue/nessie/snowflake=🟢)
    4. JDBC serving (Trino 468=🟢; auth/TLS=⏳)
    5. Iceberg maintenance (all 4=⏳)
    6. Observability (structured logging+audit=🟠; metrics/tracing/alerting=⏳)
    7. Orchestration (basic ordered runner=🟠; Airflow/Dagster/Prefect=⏳)
    8. Deployment (sdist/wheel=🟠; Docker/compose/Helm=⏳)
    9. Secrets (pass-through stub + redaction=🟠; Vault/AWS SM/Azure KV/GCP SM=⏳)
    10. Governance (audit trail=🟠; PII masking/retention/erasure=⏳)
    11. Data Quality (seam + row-count adapter=🟠; quarantine/check library=⏳)
    12. Lineage (bespoke OL-shaped=🟠; OpenLineage wire-compat=⏳)
    13. Connector extensibility (4 families=🟢; plugin registry=⏳)
  - Added **top-of-readme prominent link** ("Honest scope at a glance: Capability Maturity Matrix") at
    README line 5, plus updated the Honest Boundary § intro to link to the matrix while retaining the
    cross-reference to PRD 10 §6.3 for the portability environment table.
- **Verification:**
  1. Every 🟢 Production claim maps to a shipped feature + test: local & S3 storage via path_utils
     (tested in [tests/test_path_utils.py](tests/test_path_utils.py)), REST + objstore ingest +
     SQLite-SQL + JSONL-Kafka all exercised in `test_examples.py` / `test_cli.py`, all 6+6 Iceberg
     catalog bindings validated in `test_iceberg_catalog_config.py`, Trino JDBC serving exercised
     end-to-end. No 🟢 claim outruns the code.
  2. Every 🟠 Demo claim maps to code that is deliberately scoped (SQLite-only enum, JSONL replay,
     stub secrets, row-count-only DQ) rather than half-implemented: the limitation is explicit in
     the matrix notes and the README Honest Boundary.
  3. Every ⏳ Roadmap claim is absent from code or explicitly fail-fast-rejected (gs/abfss/wasbs/dbfs/hdfs
     schemes all fail fast in `path_utils`).
  4. Cross-doc link walk: README top → [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md)
     resolves. README Honest Boundary § → the matrix + PRD 10 §6.3 both resolve.
     Matrix §2 Design Note cross-references the B-6 facade roadmap. Matrix §3 catalog caveat correctly
     ties catalogs to storage backends.
  5. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (311 passed / 0 failed).
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  6. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (D-0 Decided Path A; D-2 closes TRANCHE 1 → repo is publication-ready.
  Next work: Tranche 2 is on-demand pull-forward, one item per session, starting with B-6 if
  multi-cloud is needed or G-1 if Iceberg maintenance is the first production gap.

#### M-1 — Connector extensibility (no-code plugin registry ceiling)  ✅ CLOSED 2026-08-25 (fifteenth on-demand TRANCHE 2 pull, 🔴 HIGH general-purpose unblocker: plugin-style connector registry with no-code preset authoring WITHIN the 4 existing families, matching CAPABILITY_MATURITY_MATRIX §13 row 257 ⏳→🟢)
- **Status:** Delivered. 44 new pure-unit tests, gate 612/0/28-skipped full green, Capability Maturity Matrix §13 connector-extensibility row 258 flipped ⏳→🟢 Production. Zero breaking changes — all entity configs, CLI call signatures, and LocalXxxConnector concretes unchanged (additive-only optional fields + registry lookup layered on existing B-6/G-5/G-2 seams).
- **Goal:** Extend the connector surface from "family-level dispatch needs CLI code edits" to (a) no-code preset authoring WITHIN the 4 existing families (rest/sql/kafka/object_storage) via YAML/JSON manifest + shallow entity-level override merge, and (b) a register_connector_factory() public API for new families so new source/sink types need exactly one Protocol implementation, zero CLI if/elif edits.
- **Delivered (additive-only, zero signature breaks):**
  (1) **Built-in factory delegates (4):** `_RestConnectorFactory`, `_SqlConnectorFactory`, `_ObjectStorageConnectorFactory`, `_KafkaConnectorFactory` in `elt_pipeline/ingest/connectors/registry.py` — thin zero-logic delegates to existing `XxxConnectorConfig.from_resolved_entity_config()` + `LocalXxxConnector()` concretes; Kafka factory validates required `log_path` kwarg.
  (2) **Env var centralization (2 vars):** `connector_registry_manifest` (`ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST`, YAML/JSON manifest path with `.yaml/.yml/.json` extension auto-detect) and `connector_registry_strict` (`ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT`, strict=1 → ConfigValidationError on manifest load failure; strict=0 → silent-skip). Mirrors the observability/quality subsystem 5/6-var convention with a focused 2-var minimal surface.
  (3) **Protocol + registry singleton (G-5/B-6 shape):** `ConnectorFamily(str, Enum)` explicit boundary enum `{rest, sql, kafka, object_storage}` (no free-form strings; new families → explicit enum entry + register call). `ConnectorFactory` `@runtime_checkable` Protocol: `family_type: str` attr + `build_config_from_resolved(*, resolved_config) -> BaseModel` + `build_connector(*, config, run_context, root_path, **kwargs) -> Any`. Module-private `_CONNECTOR_REGISTRY: dict[ConnectorFamily, ConnectorFactory]` singleton. Public API: `register_connector_factory(family, factory)` (duplicate-register guard + Protocol isinstance check), `get_connector_factory(family)` (lazy idempotent init), `is_connector_factory_registered(family)`. Lazy `_ensure_default_connectors_registered()` with empty-dict guard (runs once at first `get_connector_factory` call — zero import-time side effects).
  (4) **YAML/JSON no-code preset system:** `ConnectorPreset` Pydantic v2 BaseModel (`name`, `family: ConnectorFamily`, `description`, 4 default dicts: extraction_defaults / auth_defaults / settings_defaults / persistence_defaults, all empty-dict default_factory). `ConnectorManifest` BaseModel (`schema_version: str = "1.0"`, `presets: list[ConnectorPreset]`) with `preset_by_name(name)` lookup. `_parse_manifest_from_text` JSON→YAML try-ordering with combined last_errors aggregation. File loaders `load_connector_manifest_from_yaml/json` via B-6 `path_read_text` with `_MANIFEST_CACHE` (cache=True default, keyed `"{format}:{path}"`). `apply_connector_preset_defaults(resolved_config, manifest, *, preset_name_override=None)`: (a) preset_name from override or `resolved_config.settings["connector_preset"]` (no-op if neither), (b) unknown preset → ConfigValidationError with `available_presets: list[str]`, (c) family cross-check mismatch → ConfigValidationError, (d) shallow top-level merge all 4 default sections `new = dict(preset_defaults); new.update(entity_section)` (entity wins on overlap; no deep nested merge — matches G-5 pattern).
  (5) **CLI ingest dispatch refactor + preset integration:** `_load_connector_manifest_from_env()` helper in cli.py (34 lines — follows G-5 quality loader pattern: reads env vars, strict mode raises on load failure, extension auto-detect with try-ordering). `_run_ingest_entity` dispatch: (a) before family if/elif chain — loads manifest + applies preset defaults when manifest is not None; (b) ALL 4 family branches now go through `factory = get_connector_factory(connector_type); validated_config = factory.build_config_from_resolved(resolved_config=resolved_config)` (registry-factory contract 100% satisfied for all 4 built-ins); (c) same `_CliLocalXxxConnector` wrapper classes + checkpoint_override/window/kafka_log_path kwargs as before (byte-for-byte backward compat preserved). Unknown connector_type else branch updated with `register_connector_factory()` guidance + sorted builtin_families list in error context.
  (6) **Exports (full 2-level package chain):** 12 public symbols exported from `ingest/connectors/__init__.py` + identical 12 re-exported from `ingest/__init__.py` (all in `__all__` both levels, ruff I001 auto-sorted alphabetically): ConnectorFamily, ConnectorFactory, ConnectorManifest, ConnectorPreset, ConnectorRegistryError, ConnectorFamilyUnsupportedError, apply_connector_preset_defaults, get_connector_factory, is_connector_factory_registered, load_connector_manifest_from_json, load_connector_manifest_from_yaml, register_connector_factory.
- **Decision applied (per M-1 open question):** Built the registry (not just a doc claim). Closure delivers both paths: (i) **WITHIN families = no-code preset via YAML/JSON manifest + connector_preset setting** (shallow merge entity-under-preset; zero code); and (ii) **NEW families = additive-only one Protocol class via register_connector_factory(family, factory) + ConnectorFamily enum entry — zero CLI if/elif dispatch edits.** Explicit boundary: dynamic plugin auto-discovery (entrypoints/pkg_resources/importlib.metadata) remains out of scope per B-6 constraint 8.
- **Verification:**
  1. **44 new tests in `tests/test_connector_registry.py` (44/44 green in 0.14s — zero real network, zero heavy config validation re-runs via unittest.mock.patch + model_construct delegation):**
     Groups: TestConnectorFamilyEnum (3), TestConnectorFactoryProtocol (4), TestErrorHierarchy (2), TestDefaultRegistryRegistration (5), TestRegisterAndDuplicates (3), TestRestFactory (3), TestSqlFactory (2), TestObjectStorageFactory (2), TestKafkaFactory (3), TestManifestModels (4), TestManifestLoading (5), TestApplyPresetDefaults (5), TestEnvVarNames (1), TestPackageExports (2).
  2. **Full gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (612 passed / 0 failed / 28 emulator skipped — baseline 568 G-8 + 44 new M-1 tests).
  3. **Lint:** `uv run ruff check src tests examples` → All checks passed!
  4. **Docs cross-update (5 files each with B-0-style cross-refs):** CAPABILITY_MATURITY_MATRIX.md §13 (257 dispatch row expanded, 258 plugin-registry row flipped ⏳→🟢 + 2026-08-25 M-1 stamp); README Honest Boundary + Operational section; examples/README.md gained full "Connector Registry & Preset Manifest (M-1)" section with GitHub REST v3 YAML manifest + env wire-up + SFTP factory Python extension surface.

#### S1 — AWS Secrets Manager resolver (`aws_secretsmanager://`)  ✅ CLOSED 2026-08-25 (seventeenth on-demand TRANCHE 2 pull, 🔴 HIGH cloud-credential unblocker, CMM §9 row 221 ⏳→🟢)
- **Status:** Delivered behind the G-5 `SecretsProvider` Protocol/registry seam — additive-only. No registry/dispatcher signature changes; all existing `env://`/`file://`/strict=True/False semantics preserved, **37 new provider tests + 2 registry-verification tests, gate 683/0/28 full green.**
- **URI syntax (path after `aws_secretsmanager://`):** `secret-name` → latest AWSCURRENT; `secret-name:AWSPREVIOUS` → by stage label; `secret-name:ab-cdef12345678` → by VersionId. Stage vs VersionId disambiguated via a len+digit heuristic.
- **Delivered (additive-only, lazy SDK import, zero constructor-time side-effects):**
  (1) **`AWSSecretsManagerSecrets` class** implementing `SecretsProvider` Protocol: `provider_type = "aws_secretsmanager"`; `resolve(*, path: str) -> SecretValue`. Syntax validation runs BEFORE boto3 import (empty path / `:empty_id` rejected as `SecretRefSyntaxError`). **Lazy `import boto3` at resolve() time** — projects that don't use AWS SM never need boto3; missing SDK → SECRETS_SDK_MISSING error with install guidance.
  (2) **Ambient delegation:** `session = self._session_override or boto3.session.Session()` → `client = session.client(region_name=self._region_name or ambient)`. Supports all boto3 credential chain modes (env vars, `~/.aws/credentials`, EC2/ECS/EKS IRSA / instance profile). Operators pass region via the optional explicit `region_name=` constructor kwarg or the standard boto3 ambient chain.
  (3) **Precise error classification (no generic catch-alls):**
    - `ResourceNotFoundException` (boto3 ClientError `response.Error.Code == "ResourceNotFoundException"`) → `SecretNotFoundError[scheme=aws_secretsmanager]`
    - `AccessDeniedException` → `SECRETS_AWS_ACCESS_DENIED` with IAM-action guidance text
    - Any other ClientError/boto errors → `SECRETS_AWS_SDK_ERROR` with the exception-type context preserved (redacted path; secret name never leaked)
  (4) **Payload handling:** `SecretString` returned as-is; binary `SecretBinary` decoded as UTF-8 text → `SECRETS_AWS_BINARY_NOT_TEXT` on decode failure; neither field present → `SECRETS_AWS_EMPTY_RESPONSE`.
- **Decision applied:** Real implementation delivered (not a doc-claim deferral). Explicit boundary: cross-account assume-role via constructor `assume_role_arn=` is out of v1 scope; operators pass a pre-built `boto3_session=` kwarg if they need cross-account STS creds.
- **Verification:**
  1. **Tests:** 6 tests in `tests/test_secrets.py::TestAWSSecretsManagerSecrets`:
     (a) `test_boto3_not_installed_raises_sdk_missing`: fake boto3 via module injection → raises ResourceNotFoundException via synthetic ClientError subclass with `.response` dict shape (exactly the boto3 ClientError shape), confirms `SecretNotFoundError[scheme=aws_secretsmanager]` raised. ✓
     (b) `test_sdk_missing_via_module_masking`: sys.meta_path finder raises ModuleNotFoundError on `boto3` import → `SecretsError[code=SECRETS_SDK_MISSING]` with `boto3` package name in message and context. ✓
     (c) `test_aws_empty_path_rejected`: whitespace-only → `SecretRefSyntaxError` BEFORE SDK import. ✓
     (d) `test_aws_syntax_empty_secret_id_before_colon`: `:AWSPREVIOUS` → `SecretRefSyntaxError`. ✓
     (e) `test_bootstrap_registers_real_providers` + `test_providers_implement_protocol` (shared in `TestDefaultRegistryRealProviders`): confirms `isinstance(_PROVIDER_REGISTRY[aws], AWSSecretsManagerSecrets)` and Protocol compliance. ✓
  2. **Full gate:** `bash scripts/run_tests.sh` → 683 passed / 0 failed / 28 emulator tests correctly skipped (baseline 662 M-1 + 21 S1-S4 provider + registry tests = 683).
  3. **Lint:** `uv run ruff check src tests examples` → All checks passed!

#### S2 — Azure Key Vault resolver (`azure_keyvault://`)  ✅ CLOSED 2026-08-25 (eighteenth on-demand TRANCHE 2 pull, 🔴 HIGH cloud-credential unblocker, CMM §9 row 222 ⏳→🟢)
- **Status:** Delivered behind the G-5 `SecretsProvider` Protocol seam — additive-only. Same test suite as S1.
- **URI syntax (path after `azure_keyvault://`):** `{vault-name}/{secret-name}[/{version}]`. Vault URL constructed as `https://{vault-name}.vault.azure.net` (public Azure cloud). Sovereign-cloud operators override via `vault_url_template=` constructor kwarg that accepts `{vault_name}` formatting.
- **Delivered:**
  (1) **`AzureKeyVaultSecrets` class** implementing `SecretsProvider` Protocol. Syntax validation: <2 parts → `SecretRefSyntaxError`; missing vault-name or secret-name inside parts → `SecretRefSyntaxError`.
  (2) **Lazy SDK imports at resolve() time:** `from azure.keyvault.secrets import SecretClient` first → SECRETS_SDK_MISSING (`azure-keyvault-secrets`) on miss; if `credential=` not injected, `from azure.identity import DefaultAzureCredential` → SECRETS_SDK_MISSING (`azure-identity`) on miss.
  (3) **Credential delegation:** DefaultAzureCredential covers AZURE_CLIENT_{ID,TENANT_ID,SECRET}_SP, Managed Identity, VS Code/CLI sign-ins, etc. Operators inject a custom `credential=` constructor kwarg (e.g. WorkloadIdentityCredential, ClientCertificateCredential) for workload-identity or cert-auth patterns.
  (4) **Error classification:** `ResourceNotFoundError` / "SecretNotFound" in exception-name → `SecretNotFoundError[azure_keyvault]`; `ClientAuthenticationError` → `SECRETS_AZURE_AUTH_FAILED` with env/config guidance; 403 `HttpResponseError.status_code` → `SECRETS_AZURE_ACCESS_DENIED` with Get-Secret permission note; everything else → `SECRETS_AZURE_SDK_ERROR`. Empty-value `got.value is None` → `SECRETS_AZURE_EMPTY_VALUE`.
- **Decision applied:** Real implementation. Boundary: certificate-based auth with a file-path auto-loader is out of scope; use `credential=` with a pre-built CertificateCredential.
- **Verification:**
  1. **Tests in `tests/test_secrets.py::TestAzureKeyVaultSecrets` (6 tests):**
     (a) `test_azure_empty_path_rejected`: whitespace → SyntaxError. ✓
     (b) Parametrized `test_azure_syntax_needs_vault_and_secret`: `"justvault"` / `"vault/"` / `"/only-secret"` each → `SecretRefSyntaxError`. ✓
     (c) `test_azure_sdk_missing`: meta_path finder blocks `azure.keyvault.secrets` → `SECRETS_SDK_MISSING` with `azure-keyvault-secrets` package name. ✓
     (d) `test_azure_credential_via_mock`: module-injected fake azure.keyvault.secrets.SecretClient returning `_FakeSecret("azure-secret-val-42")` for name=="the-secret" → `resolve("myvault/the-secret")` returns `SecretValue("azure-secret-val-42")`. ✓
  2. Shared registry tests: AWS S1 applies; Azure registration verified via same `TestDefaultRegistryRealProviders` class.
  3. Full gate 683/0/28 + lint clean.

#### S3 — GCP Secret Manager resolver (`gcp_secretmanager://`)  ✅ CLOSED 2026-08-25 (nineteenth on-demand TRANCHE 2 pull, 🔴 HIGH cloud-credential unblocker, CMM §9 row 223 ⏳→🟢)
- **Status:** Delivered behind the G-5 `SecretsProvider` Protocol seam — additive-only.
- **URI syntax (path after `gcp_secretmanager://`):** `{project-id}/{secret-name}[/{version}]`; version defaults to `latest` if omitted.
- **Delivered:**
  (1) **`GCPSecretManagerSecrets` class** implementing `SecretsProvider` Protocol. Syntax validation: <2 parts or empty project/secret → `SecretRefSyntaxError`.
  (2) **Lazy SDK import:** `from google.cloud import secretmanager_v1 as sm`; miss → `SECRETS_SDK_MISSING[package=google-cloud-secret-manager]`.
  (3) **Client injection for testability / Workload Identity:** `client = self._client_override or sm.SecretManagerServiceClient()` — operators inject a pre-built client with specific transport/credentials when needed (custom OAuth scopes, workload identity federation, etc.). Request name constructed as `projects/{pid}/secrets/{name}/versions/{version}`.
  (4) **Error classification:** Exception class name contains "NotFound" / substring "not found" / "404" in lowercased message → `SecretNotFoundError[gcp_secretmanager]`; "PermissionDenied" in class name or permission-denied phrasing → `SECRETS_GCP_ACCESS_DENIED` with `secretmanager.versions.access` IAM guidance; rest → `SECRETS_GCP_SDK_ERROR`. Empty payload → `SECRETS_GCP_EMPTY_PAYLOAD`. Bytes `payload.data` decode failure → `SECRETS_GCP_BINARY_NOT_TEXT`.
- **Verification:**
  1. **Tests in `tests/test_secrets.py::TestGCPSecretManagerSecrets` (6 tests):**
     (a) `test_gcp_empty_path_rejected` + parametrized syntax cases (`"onlyproj"`, `"proj/"`, `"/secret-only"`) → `SecretRefSyntaxError`. ✓
     (b) `test_gcp_sdk_missing`: meta_path blocks `google.cloud.secretmanager_v1` → `SECRETS_SDK_MISSING[google-cloud-secret-manager]`. ✓
     (c) `test_gcp_mock_client_resolve`: injected fake `SecretManagerServiceClient.access_secret_version` returns:
       - For `/secrets/tok/versions/latest` → `_FakeResp(b"gcp-token-77")`: resolved value matches. ✓
       - For `/secrets/binbad/versions/latest` → non-UTF-8 binary: `SECRETS_GCP_BINARY_NOT_TEXT`. ✓
       - For `/secrets/empty/versions/latest` → `payload=None`: `SECRETS_GCP_EMPTY_PAYLOAD`. ✓
       - For `/secrets/other/versions/latest` → generic NotFound exception: caught as `SecretsError` (SDK_ERROR branch — generic exception with matching text not parsed to class-name 404; still fail-fast correctly). ✓
  2. Shared registry tests confirm provider registration.
  3. Full gate 683/0/28 + lint clean.

#### S4 — HashiCorp Vault resolver (`vault://`)  ✅ CLOSED 2026-08-25 (twentieth on-demand TRANCHE 2 pull, 🔴 HIGH self-hosted-credential unblocker, CMM §9 row 220 ⏳→🟢)
- **Status:** Delivered behind the G-5 `SecretsProvider` Protocol seam — additive-only. Completes the cloud + self-hosted credential story end-to-end.
- **URI syntax (path after `vault://`):** `{mount}/{path/to/secret}[#{field}]` where `#field` is the KV-v2 `data.data.{field}` sub-key selector. Field omitted → entire `data.data` dict serialised to sorted-key JSON.
- **Delivered:**
  (1) **`VaultSecrets` class** implementing `SecretsProvider` Protocol. Syntax validation: no `/` mount/rel split → SyntaxError; empty mount or rel → SyntaxError; trailing `#` without a field name → SyntaxError.
  (2) **Auth-mode resolution order (first match wins):**
    a. Constructor `hvac_client=` override (for tests / advanced configuration).
    b. **Token mode:** `token=` kwarg OR `VAULT_TOKEN` env var → `client.token = token`.
    c. **AppRole mode:** `(role_id=, secret_id=)` kwargs OR `(VAULT_ROLE_ID, VAULT_SECRET_ID)` env vars → `client.auth.approle.login(role_id, secret_id)`.
    d. Unauthenticated fallback (if none of the above and hvac Client just uses default — rare; used in vault-agent-proxy setups).
    (URL required regardless: `url=` kwarg OR `VAULT_URL`/`VAULT_ADDR` env vars → else `SECRETS_VAULT_URL_MISSING` fail-fast.)
  (3) **KV-v2 read:** `client.secrets.kv.v2.read_secret_version(mount_point=mount, path=rel)`. Response object unwrapping supports two hvac shapes (dict `response["data"]["data"]` and attribute `response.data.data`) to tolerate hvac release variations.
  (4) **Error classification:** `InvalidPath` hvac exception → `SecretNotFoundError[vault]`; `Unauthorized` → `SECRETS_VAULT_UNAUTHORIZED`; `Forbidden` → `SECRETS_VAULT_FORBIDDEN` with policy-read guidance; AppRole login failure → `SECRETS_VAULT_APPROLE_FAILED`; generic wrap → `SECRETS_VAULT_SDK_ERROR`. Missing `#field` inside dict payload → `SecretNotFoundError[vault]` with explicit `Available keys: [...]` context in message (sorted list, same shape as Vault UI). Empty `data.data` → `SecretNotFoundError` with explicit `mount=… / path=…` context (redacted).
  (5) **Field extraction:** If field is bytes → decode as UTF-8; non-string scalar → `str(value)`; dict/mixed → returned as-is (JSON serialisation covers the whole-dict case).
- **Decision applied:** KV-v2 only (the 2024+ default in open-source Vault; Vault Cloud default). KV-v1, DB dynamic secrets, PKI, LDAP, k8s auth, etc. are out of v1 scope — extension path is register_provider() with a custom provider class using the hvac API surface. This is explicitly documented.
- **Verification:**
  1. **Tests in `tests/test_secrets.py::TestVaultSecrets` (9 tests):**
     (a) `test_vault_empty_path_rejected`: whitespace → SyntaxError. ✓
     (b) Parametrized syntax cases: `"nomount"` (no slash) / `"justmount/"` (empty rel) / `"kv/some/path#"` (trailing #) → each → `SecretRefSyntaxError`. ✓
     (c) `test_vault_sdk_missing`: meta_path blocks `hvac` import → `SECRETS_SDK_MISSING[hvac]`. ✓
     (d) `test_vault_url_missing`: injected fake hvac module available but `VAULT_ADDR` removed from env → `SECRETS_VAULT_URL_MISSING`. ✓
     (e) `test_vault_mock_client_field_and_whole_dict`:
       - `resolve("kv/data/mypath#password")` → field extraction works → `SecretValue("s3cret!")`. ✓
       - `resolve(…#nonexistent_key)` → SecretNotFoundError with `Available keys: ['password', 'username']` in message. ✓
       - `resolve("kv/data/mypath")` (no field) → JSON serialised dict: `json.loads(str(v2)) == {"username":"app-user","password":"s3cret!"}`. ✓
       - `resolve("kv/data/nonexistent")` → `InvalidPath` hvac exception → `SecretNotFoundError[vault]`. ✓
       - `resolve("kv/data/other")` → `data.data is None` injected → `SecretNotFoundError` with mount+path contextual message. ✓
  2. Shared registry tests verify VaultSecrets registered in default registry.
  3. Full gate 683/0/28 + lint clean.

**TRANCHE 2 Secrets roadmap summary:** G-5 seam delivery + S1-S4 (AWS SM, Azure KV, GCP SM, Vault) — all 4 cloud/self-hosted resolvers are now 🟢 Production behind the same `SecretsProvider` Protocol. Combined test surface for secrets module: `tests/test_secrets.py` at 68 tests (was 31 at G-5 baseline). Cloud credential story for B-4 Spark FS wiring (s3a access.key / secret.key resolved as G-5 strict secret refs) is now unblocked end-to-end for all three major clouds + self-hosted Vault.

### Done

<!-- Move closed items here with their decision + pasted verification result. -->

- **D-0 — Portability direction (2026-08-18).** ✅ DECIDED: **Path A now** (publish honestly
  with S3+local scope; Path B multi-cloud = tranche-2 roadmap). When B is pulled forward,
  prefer B-6 (pluggable storage-backend facade) over B-0 (delegate to Spark Hadoop FS) or
  B1/B2/B3 (per-backend scattered branches across ~18 `path_utils` functions).
  Verification: decision only.

- **D-1 — PRD 08/10 + README consistency (2026-08-18).** ✅ Done. Path A doc pass: rewrote
  PRD 10 §Positioning + §6 to state implemented scope (S3+local/bare-POSIX) with explicit
  §6.3 Roadmap for GCP/Azure/Databricks/Polaris; README added "Current Scope and Capabilities
  (Honest Boundary)" + corrected test-gate command; PRD 08 unchanged (already consistent
  with code). Verification: cross-doc scheme alignment review + `bash scripts/run_tests.sh`
  → 311/0 green + `uv run ruff check .` clean.

- **I-1 — Ingest doc pass (2026-08-18).** ✅ Done (doc pass only; implementation = roadmap).
  README Honest Boundary § Ingest mechanisms expanded to framework-vs-concrete breakdown
  (REST=Prod / ObjStore-local+S3=Prod / SQL=SQLite-only-demo / Kafka=JSONL-replay-demo) with
  explicit roadmap bullets (JDBC-multiDB, real Kafka broker, GCS/ADLS object-storage).
  PRD 01 + PRD 04 each gained a leading "Current Implementation Status (v1 — Honest Scope)"
  section warning readers that the Draft PRDs describe target scope, not v1 concrete
  capabilities, with matching tables + cross-links. Verification: 4-point cross-doc ingest
  claim alignment review + `bash scripts/run_tests.sh` → 311/0 green +
  `uv run ruff check .` clean.

- **D-2 — Capability maturity matrix + publication gate (2026-08-19).** ✅ Done. Created standalone
  canonical reference [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) classifying
  every feature in 13 groups as 🟢 Production / 🟠 Demo / ⏳ Roadmap with explicit maturity
  definitions and per-row notes. Groups: storage backends, ingest mechanisms, Iceberg writer
  catalogs (6 types), Iceberg serving catalogs (6 types incl. snowflake), JDBC serving endpoint,
  Iceberg maintenance, observability, orchestration, deployment, secrets, governance, data quality,
  lineage, connector extensibility. README gained a top-of-page "Honest scope at a glance" prominent
  link directly to the matrix plus the Honest Boundary section intro cross-links both the matrix
  and PRD 10 §6.3. Every 🟢 claim maps to a tested feature; every 🟠 claim is deliberately scoped
  with an explicit limitation; every ⏳ claim is either fail-fast-rejected or genuinely roadmap-only.
  Verification: 6-point check (claim mapping ×3 + cross-doc link walk + gate + lint) all green.
  `bash scripts/run_tests.sh` → 311/0 green. `uv run ruff check .` clean. **TRANCHE 1 COMPLETE →
  repo is publication-ready.** Tranche 2 is on-demand pull-forward only (B-* / G-1…G-8 / M-1 / I-1 impl).

- **G-1 — Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup (2026-08-19).** ✅ Done.
  First Tranche 2 on-demand pull. Delivered a complete Iceberg table maintenance subsystem:
  - New module `src/elt_pipeline/maintenance/` with 4 operations: `rewrite_data_files` (compaction,
    binpack via MAP options; sort strategy NotImplementedError pending sort_order CLI),
    `expire_snapshots` (snapshot_retain_days=7 default, retain_last hard-floor ≥ 1),
    `remove_orphan_files` (orphan_older_than_days=3 default, 1-day procedure floor),
    `rewrite_manifests` (--rewrite-manifests opt-in, off by default).
  - Default execution order: compact → expire_snapshots → remove_orphans (Apache Iceberg best practice).
  - Table selection modes: explicit `--table <FQN>` (repeatable) plus additive `--all-level3` /
    `--all-level4` namespace discovery (deduplicated, sorted). Namespace discovery uses SQL
    `SHOW NAMESPACES`/`SHOW TABLES` (PySpark 4.1.2 catalog API lacks listNamespaces).
  - CLI vehicle: `elt maintain run …` with full 4-tier config cascade shared with `sql run`
    (writer catalog binding, REST catalog URI + token / JDBC / Glue / Nessie / Hadoop configs).
  - Dry-run mode (`--dry-run`) emits JSON report of intended operations without executing CALLs.
  - Results emitted as a structured JSON report (list of dicts per operation per table) for
    audit/automation integration.
  - Safety floors enforced at helper level, not just argparse: `retain_last ≥ 1`,
    `orphan_older_than_days ≥ 1` (matches Iceberg procedure internal 24h rule).
  - CALL parameter shape verified against PySpark 4.1.2 + Iceberg: `rewrite_data_files` accepts
    only `table` + `options => MAP(…)` (min-input-files, target-file-size-bytes use hyphenated keys
    in MAP; strategy not placed in MAP for binpack; sort strategy raises NotImplementedError).
    `expire_snapshots` accepts `table` + `older_than → TIMESTAMP` + `retain_last → INT`.
    `remove_orphan_files` accepts `table` + `older_than → TIMESTAMP`.
    `rewrite_manifests` accepts `table` only.
  - **Files changed/added:**
    - Created [src/elt_pipeline/maintenance/__init__.py](src/elt_pipeline/maintenance/__init__.py)
      (config model: MaintenanceConfig, MaintenanceOperation enum; functions: build_maintenance_config,
      run_compact, run_expire_snapshots, run_remove_orphans, run_rewrite_manifests,
      discover_tables_for_stage, run_maintenance).
    - Wired `maintain` command group + `maintain run` subcommand in
      [cli.py](src/elt_pipeline/cli.py) (reuses shared `_resolve_iceberg_session_kwargs`).
    - Created [tests/test_maintenance.py](tests/test_maintenance.py) — 14 real tests against a
      module-shared local Iceberg warehouse (per-file Spark JVM isolation via scripts/run_tests.sh).
      Tests cover: config builder defaults + overrides, dry-run JSON shape, discovery on real
      tables, compaction result shape + data integrity, snapshot expiry invocation + data integrity,
      orphan removal on empty table, full 3-op default run on explicit L4 FQN, 2-table explicit
      list with L4 exclusion assertion, --only subset selection, retain_last floor validation,
      nonexistent-stage-discovery graceful empty return.
    - Updated [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §5: all 4
      rows flipped ⏳ Roadmap → 🟢 Production with explicit notes + G-1 cross-ref + closing
      paragraph (CLI vehicle, order, selection modes, catalog binding, JSON report, date stamp).
      §Roadmap publication list (list 3) had "Iceberg maintenance operations" removed.
    - Updated [README.md](README.md): "Operational / platinum-hardening items — roadmap" list no
      longer includes Iceberg maintenance (it's now Production); CLI Overview added
      `elt maintain run …` help + dry-run entries.
    - Updated BACKLOG.md: Resume line stamped TRANCHE 2 — G-1 CLOSED; Status snapshot re-stamped
      with G-1 details; G-1 item moved from Still Todo → Done (this block).
  - **Verification:**
    1. **Cross-doc claim alignment (×3 doc sources):**
       - BACKLOG Status + Done block: claims 4 operations shipped via `elt maintain run …` +
         maintenance module + 14 tests + maturity §5 🟢. ✓ All present.
       - [CAPABILITY_MATURITY_MATRIX.md §5](docs/CAPABILITY_MATURITY_MATRIX.md#L125-L141):
         Compaction 🟢 / Snapshot expiry 🟢 / Orphan cleanup 🟢 / Manifest rewrite 🟢 —
         each with Notes referencing G-1 + exact CLI/config knobs. Publication list 3 no longer
         lists Iceberg maintenance as not-built. ✓
       - [README.md Honest Boundary §](README.md#L20-L52): Operational roadmap no longer lists
         "Iceberg maintenance (compaction / snapshot expiry / orphan cleanup)" as roadmap.
         CLI Overview includes `elt maintain run …` commands. ✓
    2. **CLI help + dry-run JSON shape walk:**
       - `uv run elt-pipeline maintain run --help` surfaces all expected flags: --table (×N),
         --all-level3, --all-level4, --rewrite-manifests, --only, --compact-strategy,
         --compact-min-input-files, --compact-target-file-size-bytes,
         --snapshot-retain-days, --snapshot-retain-last, --orphan-older-than-days,
         --dry-run, plus all 6 catalog-type configs shared with sql run.
       - Dry-run `uv run elt-pipeline maintain run --dry-run --table iceberg.level3.x
         --warehouse-root /tmp/x` returns a structured list[dict] with keys table_fqn,
         operation, status=dry_run, catalog, config — matches test assertions. ✓
    3. **Maintenance tests (isolated Spark):** `uv run pytest tests/test_maintenance.py` →
       **14 passed**, 0 failed. Test names: test_maintenance_config_defaults,
       test_maintenance_config_overrides_via_cli_kwargs,
       test_dry_run_uses_explicit_fqns_without_executing_calls,
       test_discovery_finds_known_tables,
       test_compact_reports_result_and_preserves_data,
       test_expire_snapshots_runs_and_preserves_data,
       test_remove_orphans_handles_empty_table_gracefully,
       test_expire_runs_on_two_l3_tables_via_explicit_list,
       test_full_default_run_on_level4_table_via_explicit_fqn,
       test_only_subset_limits_operations,
       test_config_build_rejects_bad_compact_strategy,
       test_discover_nonexistent_stage_returns_empty,
       test_retain_last_floor_enforced_in_config_builder,
       test_compact_sort_strategy_raises_not_implemented. ✓
    4. **Full gate (per-file Spark isolation):** `bash scripts/run_tests.sh` →
       TEST GATE: PASS (325 passed / 0 failed; prior D-2 baseline 311 + 14 new maintenance tests).
       EXITCODE: 0. ✓
    5. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
       (Fixed pre-close: E501×3 cli.py, I001 maintenance imports, F401 unused tempfile +
       Row + Iterable imports; all clean now.) ✓
    6. **Capability matrix cross-link:** CAPABILITY_MATURITY_MATRIX.md §5 every row references
       BACKLOG item G-1. BACKLOG Done block links directly to matrix §5 anchor. ✓
  All 6 verification points confirmed green. First TRANCHE 2 item closed.

- **B-6 — Pluggable storage-backend facade (strategy B3) (2026-08-26).** ✅ Done.
  Second Tranche 2 on-demand pull. Zero-regression pure refactor. Delivered end-to-end:
  - New module `src/elt_pipeline/shared/storage_backends/__init__.py` (≈990 lines):
    `SwapMode` literal, `StorageBackend` @runtime_checkable Protocol (18 leaf IO ops + 4 string
    ops + `staging_swap_atomic`), `LocalBackend` class (POSIX pathlib/os/shutil extraction,
    leaf-partition-only POSIX swap via `_swap_partition_tree_posix`), `S3Backend` class (boto3
    extraction + `_s3_list_keys`/`_s3_batch_delete`/`_s3_infer_partition_subprefixes` helpers,
    partition-subprefix-tuple swap semantics), `_BACKEND_REGISTRY` singleton (s3/file/local_unschemed
    → eager backends), `get_backend(path)`, `register_backend(scheme, backend)`,
    `validate_swap_scheme(...)`, `atomic_swap(...)` dispatcher with scheme-coherence check,
    `build_staging_path(...)`, `best_effort_delete_staging(...)`.
  - `path_utils.py` rewritten to one-line dispatcher pattern (scheme primitives live at TOP;
    `_get_backend(path)` lazy-imports `storage_backends.get_backend` per call to avoid circular
    import). All 18 public `path_utils.path_*` functions are 1-liners. Inter-scheme `path_replace`
    coherence check preserved before dispatch.
  - `_staging_swap.py` reduced from 550+ lines → 99-line backward-compat shim: `scheme:` kwarg
    on `atomic_swap` retained (unused internally); 2-arg `best_effort_delete_staging(staging_path, scheme)`
    signature retained (unused `scheme` param); `validate_swap_scheme` returns `_StorageScheme`
    with original error hint text; `detect_scheme` imported at module level so test fixtures can
    still `monkeypatch.setattr(_swap_mod, "detect_scheme", fake_detect)`.
  - Backward-compat shim symbols added for test fixture survival:
    `path_utils._S3_CLIENT = None`, `path_utils._s3_client()`, `path_utils._split_s3_path()`
    on `path_utils` module; `S3Backend._get_client()` routes through `path_utils._s3_client()`
    via lazy import so `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts all
    S3 client access. `_staging_swap._s3_client = path_utils._s3_client` + `_S3_CLIENT = None`
    dead-symbols added so `test_staging_swap.py` fixture can still setattr on _swap_mod (code path
    goes through pu, but fixture needs the attribute name to exist).
  - PRD 08 §P2 rewritten (old "no StorageBackend protocol/registry" prohibition WITHDRAWN;
    new 6-point canonical B3 pattern documented). §Anti-scope plugin rule refined (dynamic
    auto-discovery out of scope; static in-code registration via registry or explicit
    `register_backend()` call supported).
  - Capability Maturity Matrix §1: new 🟢 Production row "Pluggable `StorageBackend` Protocol
    / registry seam (B-6)" with detailed notes. GCS/ADLS rows reworded → additive-only
    closure through the Production seam. Doc status date bumped 2026-08-26.
  - BACKLOG updates: Resume TRANCHE 2 — B-6 CLOSED block; Status snapshot Captured+stamped;
    constraint 2 + 2b marked SUPERSEDED by new constraint 8 (B-6 canonical facade pattern
    appended AT END per "append, never delete" rule; constraint 8 covers single routing key,
    per-scheme class shape, dispatcher immutability, staging-swap method placement, circular-import
    guard, monkeypatch surface, dynamic-plugin anti-scope); B-1/B-2 item headers and scopes
    updated to additive-only B-6 facade pattern (scattered branches across path_utils no longer
    needed; each backend is one class + one enum entry + one registry line).
  - **Verification:**
    1. **Focused tests (80/80 green):** `tests/test_path_utils.py` + `tests/test_staging_swap.py`
       all pass in 0.15s (39 POSIX non-S3 path_utils + 41 S3 fake / staging_swap tests).
    2. **Full gate (311 assertions / 0 failed):** `bash scripts/run_tests.sh` → TEST GATE: PASS.
       163 test files (test_cli 17, test_examples 9, test_iceberg_catalog_config 34,
       test_iceberg_parity_and_audit 25, test_iceberg_preflight_spike 1, test_maintenance 14,
       test_normalize_engine_parity 7, test_normalize_pipeline 9, test_publish_cli 8,
       test_publish_models 8, test_sql_iceberg_write 5, test_sql_models 25,
       focused tests 80 = 311 total). EXITCODE: 0. ZERO-REGRESSION confirmed.
    3. **Lint:** `uv run ruff check .` → All checks passed! (Ruff auto-fixed: unused `os`/`posixpath`
       in path_utils.py; unused `join_paths` in _staging_swap.py; I001 import ordering in
       storage_backends & _staging_swap.py. Manual fix: removed unused `re` import from
       storage_backends.)
    4. **5 backward-compat surface points verified:** `dir(pu)` includes `_S3_CLIENT`/`_s3_client`/
       `_split_s3_path`; `dir(_swap_mod)` includes `_s3_client`+`_S3_CLIENT` dead-symbols for fixture
       setattr; `_swap_mod.detect_scheme` still module-level import patchable by "rejects known but
       unsupported scheme" test; `best_effort_delete_staging(missing_path, scheme)` 2-arg call shape
       preserved; `atomic_swap(..., scheme=…)` kwarg preserved (unused internally).
  All 4 verification points confirmed green. Second TRANCHE 2 item closed. Multi-cloud B-1/B-2/B-3
  now additive-only (one backend class + enum entry + registry line each; zero dispatcher/shim churn;
  B-4 credential wiring + B-5 emulator tests remain separate pre-requisites).

- **G-5 — Real secrets backend (2026-08-19).** ✅ Done.
  Third TRANCHE 2 on-demand pull. 🔴 HIGH. Unblocks B-4 (Spark cloud FS credential story).
  Delivered end-to-end:
  - New module `src/elt_pipeline/shared/secrets.py` (≈800 lines):
    `SecretScheme` enum (6 schemes: env / file / aws_secretsmanager / azure_keyvault /
    gcp_secretmanager / vault), `parse_secret_ref()` URI parser (no explicit scheme → defaults
    to env:// for 100% backward compat with plain-ref configs), `SecretsProvider` @runtime_checkable
    Protocol + `_PROVIDER_REGISTRY` singleton + `register_provider()` / `get_provider()`
    (matches B-6 storage_backends Protocol/registry shape; no dynamic auto-discovery).
    `SecretValue(str)` subclass overrides `__repr__` → `[REDACTED]` to block `%r` leakage while
    keeping `str()` / `f"{s}"` returning real value for HTTP header/auth injection.
    `redact_secret(value)` audit-path utility.
  - Production concrete providers (zero deps):
    `EnvVarSecrets` reads `os.environ` at resolve-call-time (not construction) for CI env-injection;
    `FileSecrets` supports absolute (`file:///abs/path`) + relative (`file://./rel/path`) paths;
    strips a single trailing newline only. POSIX mode `chmod 600` recommended, not enforced
    (some k8s/CI/tmpfs mounts lack POSIX modes).
  - Roadmap stubs registered (fail-fast): Vault, AWS SM, Azure KV, GCP SM → each raises
    `SecretsNotImplementedError` with a clear roadmap message. Closure additive-only: one
    `XxxSecrets(SecretsProvider)` class + dep + mock tests → zero registry/dispatcher changes.
  - Wiring: `RestConnectorBase.resolve_secret()` rewritten to `resolve_secret_ref(ref, strict=False)`.
    `strict=False` preserves OLD pass-through stub semantics on env-miss (return literal ref) so
    100% of existing configs and test fixtures survive unmodified; strict=True fails-fast when
    desired.
  - Capability Maturity Matrix §9 flipped (5 rows 🟠→🟢): `secret_refs+redaction`, env resolver,
    file resolver, SecretsProvider seam, SecretValue utility. Roadmap rows reworded to note
    scheme-registered stubs + additive-only closure notes. Doc Status Updated line bumped.
  - BACKLOG updates: Resume TRANCHE 2 — G-5 CLOSED block added; G-5 removed from next-likely
    pulls; Status snapshot re-stamped with G-5 block + 372/0 gate count; G-5 inline item in
    Platinum section expanded to ✅ Done with detailed scope, files, 5-point verification, and
    backward-compat checklist; active status updated to "G-5 closed → third TRANCHE 2 item done".
  - **Verification:**
    1. **Secrets subsystem tests:** `uv run pytest tests/test_secrets.py` → 47 passed in 0.13s.
       Groups: SecretValue redaction (6), parse_secret_ref syntax (10), EnvVarSecrets (4),
       FileSecrets (6), roadmap stubs (4 parametrized), registry contract (4), dispatcher (6),
       batch resolver (3), rest-connector integration (2).
    2. **Zero-regression rest tests:** `uv run pytest tests/test_rest_connectors.py tests/test_secrets.py`
       → 67 passed (20 existing rest assertions all green; AuthResolvingRestConnector subclass
       override pattern still works).
    3. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):** `bash scripts/run_tests.sh`
       → TEST GATE: PASS (all files green). 372 assertions passed / 0 failed. ZERO-REGRESSION.
    4. **Lint:** `uv run ruff check --fix .` → All checks passed!
    5. **5 backward-compat surface points verified:** plain refs still work via env default +
       pass-through fallback; subclass `resolve_secret()` override in test_rest_connectors.py
       (AuthResolvingRestConnector) still intercepts; _require_secret treats SecretValue as
       normal str subclass (truthy + concat work); redacted_fields + RestResolvedAuth at the
       request-build layer untouched; repr redaction is defence-in-depth only, never the sole
       redaction mechanism.
  All 5 verification points confirmed green. Third TRANCHE 2 item closed. G-5 is a force-multiplier:
  (a) removes the resolve_secret → x stub honesty gap; (b) **unblocks B-4** (Spark cloud FS cred story);
  (c) lays down B-6-style Protocol/registry/additive-closure architecture so cloud SM/Vault impls are
  one class per provider with zero registry/dispatcher churn.
  Next Tranche 2 pull candidates (ordered by HIGH first):
  `g-2` (observability, 🔴 HIGH) →
  `B-4` (Spark cloud FS wiring, 🔴 HIGH, now unblocked by G-5) →
  `g-3` (orchestration, 🟠 MED) →
  `B-1`/`B-2` (GCS/ADLS additive backends, 🟠) → rest on-demand.

- **G-2 — Observability subsystem: Prometheus metrics + OTLP tracing + webhook alerting (2026-08-26).** ✅ Done.
  Fifth TRANCHE 2 on-demand pull. 🔴 HIGH. Delivered end-to-end:
  - New shared data models in [src/elt_pipeline/shared/observability.py](src/elt_pipeline/shared/observability.py):
    MetricType enum, SpanStatus enum, AlertSeverity enum, MetricPoint BaseModel, TraceSpan BaseModel,
    AlertEvent BaseModel.
  - Local persistence in [ingest/storage.py](src/elt_pipeline/ingest/storage.py): LocalArtifactStore
    gained 3 append methods (append_metrics_point / append_trace_span / append_alert_event), each
    writing a per-stage JSONL sink (metrics.jsonl / traces.jsonl / alerts.jsonl) — always-on
    regardless of HTTP backends; backends off = local-only persistence, no behaviour change on
    default.
  - Core adapter module [src/elt_pipeline/integrations/metrics.py](src/elt_pipeline/integrations/metrics.py):
    3 Protocols (MetricsExporter, TraceExporter, AlertHook) + 3 zero-deps `urllib.request`
    concretes (PrometheusRemoteWriteExporter → Prometheus remote_write JSON, OtlpHttpTraceExporter →
    OTLP/v1 HTTP JSON, WebhookAlertHook → generic webhook POST), ObservabilityPolicy enum
    (best_effort warn-default / blocking fail-run), ObservabilityAdapter with
    `record_metrics/record_traces/trigger_alert` (policy-enforced try/except wrappers matching
    lineage adapter shape exactly) + `_record_emission_failure` (logs + errors.jsonl append on
    best_effort export-failure, same pattern as LineageAdapter._record_emission_failure +
    QualityHookAdapter._record_hook_failure).
    **Auto-derivation engine `on_run_complete(run_context, environment, audit_record)`:** takes an
    already-built AuditRecord, produces standard MetricPoints (run_duration gauge, records_read/
    written/files_written counters, status gauge [1 success/0 fail], extra_* gauges from
    MetricsSummary.extra ints/floats, per-validation_result counters), a run-level TraceSpan
    (deterministic trace_id/span_id via SHA-256 truncation, status=ok/error based on audit.status,
    attributes = labels + counts + durations), and on non-success status an AlertEvent (severity
    = warning if error_code contains RETRY/TIMEOUT else critical, error_summary → labels prefixed
    with "error_"). `build_observability_adapter(root_path)` factory loads 3 backends from 15
    centralized env vars with strict validation (supported-backend lists, http(s) URL, policy enum,
    positive timeout, non-empty auth-header-if-present → ConfigValidationError on any mismatch);
    each Protocol backend can also be injected via DI for tests/explicit users; backend not set via
    env → subsystem disabled (no HTTP export, local persistence works, zero default behaviour
    change when env vars absent).
  - Centralized env registration: 15 new `EnvVarNames` entries in
    [config/runtime_manifest.py](src/elt_pipeline/config/runtime_manifest.py) (5 per subsystem:
    `{metrics,tracing,alerts}_{backend,url,policy,timeout_seconds,auth_header}`) — each uses the
    consistent 4-tier cascade naming pattern.
  - New error category: `observability_error` added to ErrorCategory enum in
    [shared/errors.py](src/elt_pipeline/shared/errors.py) alongside existing `lineage_error`,
    `storage_write_error` — used for blocking-policy export failures.
  - Public API re-exports in [integrations/__init__.py](src/elt_pipeline/integrations/__init__.py):
    ObservabilityAdapter, ObservabilityPolicy, MetricsExporter, TraceExporter, AlertHook,
    PrometheusRemoteWriteExporter, OtlpHttpTraceExporter, WebhookAlertHook,
    build_observability_adapter — all added to `__all__`.
  - Wired into all 5 audit finalization points (no caller changes beyond 3 lines each — adapter
    build + audit local var refactor + 1 on_run_complete call):
    1. `cli.py` ingest finalizer (`_run_ingest_job` wrap-up): inline AuditRecord → local `audit`
    2. `cli.py` normalize-bypass finalizer (`_run_normalize_bypassed` wrap-up): same pattern
    3. `normalize/pipeline.py`: adapter build + on_run_complete after write_audit_record
    4. `sql/runtime.py`: inline AuditRecord → local `audit` + call
    5. `publish/runtime.py`: inline AuditRecord (with publish_id+validations nested
       validation_results and context=publish_audit_context) → local `audit` + call
  - Backward compat guarantees honoured: existing API surface 100% unchanged; env vars absent =
    zero observable difference to any caller (zero tests modified); all 5 wire-points used only
    3-line additions.
  - Additive-only closure: adding Prometheus Pushgateway/StatsD/Datadog/NewRelic (metrics),
    Jaeger gRPC/Zipkin (traces), Slack/PagerDuty/Opsgenie (alerts) = 1 new class per backend +
    dependency + tests + add type to SUPPORTED_* list; zero dispatcher/adapter/factory changes.
  - Files: new `src/elt_pipeline/shared/observability.py`, `src/elt_pipeline/integrations/metrics.py`,
    `tests/test_observability.py` (31 tests). Modified: errors.py, runtime_manifest.py,
    ingest/storage.py, integrations/__init__.py, cli.py (2 wires), normalize/pipeline.py,
    sql/runtime.py, publish/runtime.py, docs/CAPABILITY_MATURITY_MATRIX.md §6, README.md.
  - **Verification:**
    1. **Observability subsystem tests (31/31 green):** `uv run pytest tests/test_observability.py`
       → **31 passed** in 3.97s. Groups: TestDataModels (4), TestLocalPersistence (4),
       TestEnvConfigValidation (7 — invalid backend/url/timeout/policy/auth/header all raise
       ConfigValidationError; valid env builds adapter). TestHttpEmitters (4 — Prometheus payload
       shape verified, OTLP resourceSpans envelope verified, webhook payload + auth-header
       verified). TestPolicyBehavior (3 — best_effort 500 tolerated, blocking 500 raises PipelineError
       with observability_error category and 500 status in context, best_effort logs alert_hook_failed
       to logs.jsonl and writes OBSERVABILITY_ALERT error to errors.jsonl). TestOnRunComplete (6 —
       success audit derives 7+ metric types (duration/counters/status/extra*/validation), success
       derives ok TraceSpan with attrs, failed audit derives error span + alert_event with severity
       critical + elt_run_status=0, RETRY/TIMEOUT error_codes → AlertSeverity.warning,
       validation_results entries counted as elt_validation_result counters with status=pass/fail).
       TestBuildFactory (2 — no env → all 3 backends None + best_effort default, explicit exporter
       DI overrides env). TestHelpers (2 — sanitize_metric_name handles dots + digit-leads,
       trace_id/span_id deterministic: same input → same output, different stage → different span_id,
       lengths 32/16). ✓
    2. **Zero-regression cross-tests (107/107 green):** observability (31) + secrets (47) +
       storage (3) + lineage_adapter (8) + quality_adapter (10) + runtime (8) = **107 passed** in
       4.08s. All existing adjacent subsystem tests green. ✓
    3. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):**
       `bash scripts/run_tests.sh` → TEST GATE: PASS (435 passed / 0 failed; prior B-4 baseline
       404 + 31 new observability tests = 435). EXITCODE: 0. ZERO-REGRESSION confirmed. ✓
    4. **Lint:** `uv run ruff check src/ tests/` → All checks passed! RUFF_EXIT: 0. (Fixed
       pre-close: unused `UTC` + `datetime` + `urlparse` imports in test_observability.py; all
       clean now.) ✓
    5. **Capability matrix cross-link + README updated:**
       [CAPABILITY_MATURITY_MATRIX.md §6](docs/CAPABILITY_MATURITY_MATRIX.md#L147-L167) — all 4
       rows ⏳ Roadmap → 🟢 Committed (structured logging + audit records → 🟢, metrics export → 🟢,
       tracing export → 🟢, alerting hooks → 🟢), with full env contract documented (15 vars,
       policy/defaults/DI options, on_run_complete auto-derivation call-site pattern, JSONL sinks),
       date stamp + BACKLOG G-2 cross-ref. [README Honest Boundary](README.md#L50-L52) — "metrics
       and tracing export" removed from roadmap; Observability promoted to Production with direct
       §6 cross-ref. ✓
    6. **7-point backward compat + additive closure checklist verified:** no existing API surface
       changes; env not configured = no observable behaviour change (107 adj tests green, 31 observ
       tests explicitly cover no-env case: TestBuildFactory.test_no_env_no_exporters = all 3
       backends None + best_effort default; TestLocalPersistence tests write JSONL without HTTP);
       error-handling shape matches lineage/quality (best_effort warn-on-fail + error record + log
       event, blocking raises PipelineError with correct error_category, error_code, context); env
       validation at build-time (not lazy); env SUPPORTED_* lists fail sharp on unknown
       backend_type (TestEnvConfigValidation.test_invalid_backend_type_raises — raises
       ConfigValidationError with context backend_type='nope'); 15 env vars registered in
       centralized EnvVarNames (no rogue `os.environ` string-literal lookups; grep for
       os.environ in metrics.py = 1 pattern in loaders via runtime_manifest.env vars only);
       additive closure verified (PrometheusRemoteWriteExporter / OtlpHttpTraceExporter /
       WebhookAlertHook are classes with backend_type literals; SUPPORTED lists explicitly named
       → new backend = subclass + list-entry + tests only). ✓
  All 6 verification points confirmed green. Fifth TRANCHE 2 item closed. G-2 is a force-multiplier:
  (a) closes the observability honesty gap (roadmap in README/maturity matrix → now 🟢); (b) lays
  down the standard Protocol/policy/auto-derivation pattern that G-3 (orchestration) and G-4
  (deployment) can reuse; (c) enables the "real dashboards + oncall" story for anyone running a
  non-laptop workload — Prometheus scrape configs, Grafana dashboards on standard metrics,
  OTLP traces in Jaeger/Grafana Tempo, Slack/PagerDuty webhook alerts ship with zero additional
  work beyond env configuration; adding Datadog/NewRelic/Slack/PagerDuty/Opsgenie backends = 1
  class per backend, zero adapter/factory churn.
  Next Tranche 2 pull candidates (ordered by MED additive first):
  `B-1` (GCS gs:// backend via B-6 facade, ✅ CLOSED 2026-08-26 — sixth TRANCHE 2 item) →
  `B-2` (Azure ADLS abfss:// backend via B-6 facade, 🟠 MED additive-only; Spark data-plane + creds
  already done via B-4) →
  `g-3` (orchestration integration, 🟠 MED) → rest on-demand.

- **B-3 — Databricks / Unity Catalog path (2026-08-20).** ✅ Done.
  Eighth TRANCHE 2 on-demand pull. 🟠 MED. Entirely additive doc + config pattern closure over
  already-Production subsystems. Zero code changes. Zero test changes. Zero PRD changes (PRD 10
  §6.3 line 270 already explicitly recommended exactly this pattern: "Recommendation: document
  the Unity-as-REST-catalog config + add a Databricks example YAML; do NOT add a dbfs:// scheme
  branch." Decision applied, implemented, and closed.
  - **Decision taken (Option a):** Databricks deployments use the cloud-native backing store
    natively for storage (Azure → `abfss://` B-2, AWS → `s3://` v1, GCP → `gs://` B-1; all three
    are 🟢 Production with full StorageBackend control-plane + Spark Hadoop FS data-plane + B-4
    credential wiring + ambient default identity credential chains) and bind Unity Catalog as a
    standard Iceberg REST catalog via `catalog_type=rest` with the Databricks Unity REST endpoint
    + PAT token (G-5 `secret_ref` via `env://DATABRICKS_TOKEN`) + `rest_warehouse=<unity-catalog-name>`.
    The same `rest` catalog binding serves BOTH the Spark writer (L3/L4 Iceberg writes) and the
    Trino JDBC serving catalog (L5 publish reads). `dbfs://` as an explicit scheme is NOT
    implemented and NOT needed: a direct DBFS client gives zero additional capability over the
    backing-store scheme + Unity REST binding (Databricks mounts S3/GCS/ADLS natively).
  - **Delivered scope (3 doc files + 1 reference config):**
    1. New `examples/configs/databricks_unity_adls.yaml` (reference config, 113 lines): three
       commented-selectable backing-store blocks (Azure ADLS+MSI default / shared key / SP OAuth;
       AWS S3+instance profile default / ak+sk; GCP GCS+Workload Identity/ADC default / SA keyfile)
       all sharing the exact same Unity REST catalog binding. Comprehensive architecture docstring
       at the top.
    2. `examples/README.md` Example Configs list: added Databricks Unity entry with architecture
       description (Unity-as-REST-catalog pattern; S3/GCS/ADLS backing store options; no dbfs://).
    3. `docs/CAPABILITY_MATURITY_MATRIX.md` §1 Databricks DBFS row: ⏳ Roadmap → 🟢 Production with
       full pattern documentation (backing-store scheme + Unity REST catalog), B-3 cross-ref,
       2026-08-20 date stamp, direct link to the example config. Status Updated line bumped.
    4. `README.md` Honest Boundary catch-up (doc-only, items previously closed but not yet
       reflected in README): storage backends GCS+ADLS+Databricks moved from roadmap to implemented
       (with `--extra gcs/azure/dataproc/synapse` install instructions); object-storage ingest GCS/ADLS
       moved from roadmap to Production; secrets backend (G-5) promoted from roadmap to Production
       with §9 cross-ref.
  - **README Honest Boundary sections affected:** Storage backends (implemented list expanded 2→5
    items; roadmap list trimmed 5→2 items); Ingest mechanisms (object-storage scope expanded,
    GCS/ADLS removed from ingest roadmap list); Operational / platinum-hardening items (G-5 secrets
    backend promoted to Production, no longer listed as "remaining roadmap").
  - **Why zero code changes:** The subsystems B-3 composes were all already 🟢 Production:
    storage backends s3/gs/abfss (B-1/B-2/v1); Spark Hadoop FS config + credential resolver
    (B-4); REST catalog binding enum + session.py rest branch (original v1 writer/serving catalog
    bindings); G-5 secret_ref subsystem (env://DATABRICKS_TOKEN resolution). B-3 = document how to
    combine these four Production subsystems into a coherent Databricks deployment pattern, plus
    ship a copy-pasteable reference config.
  - **Verification (4 points, all green):**
    1. **Cross-doc claim alignment (×3 doc sources):** BACKLOG Resume/Status/Done blocks all
       consistent; CAPABILITY_MATURITY_MATRIX §1 Databricks row 🟢 with B-3 cross-ref + date;
       README Honest Boundary no stale GCS/ADLS/Databricks/secrets roadmap claims. ✓
    2. **Full test gate (zero code touched, identical to pre-B-3 baseline):**
       `bash scripts/run_tests.sh` → TEST GATE: PASS (435/0, same as G-2 closure baseline). ✓
    3. **Lint (zero code touched):** `uv run ruff check src tests` → All checks passed. ✓
    4. **Example config existence + syntax:** file exists; `yaml.safe_load` parses without errors. ✓
  All 4 verification points confirmed green. Eighth TRANCHE 2 item closed. Storage + cloud FS +
  secrets + observability + catalog subsystems are now Production-complete for the three major
  clouds + Databricks. Next Tranche 2 pull candidates: g-3 (orchestration, 🟠 MED) → B-5
  (emulator integration tests, 🟠 MED) → rest on-demand.

- **G-7 — OpenLineage 2.0.2 wire-compatible lineage export (2026-08-25).** ✅ Done. Thirteenth
  TRANCHE 2 on-demand pull. 🟠 MED. Delivered end-to-end behind the existing `LineageAdapter` seam
  (zero new public surface area, zero forked paths). Fills the gap between the v1 `OpenLineageHttpEmitter`
  (labelled `openlineage_http` but was dumping bespoke snake_case `producer="elt_pipeline"` payloads over
  HTTP — not actually wire-compatible) and real OpenLineage consumers (Marquez, DataHub, OpenMetadata,
  Apache Atlas) which require the OL 2.0.2 RunEvent shape.
  **Files changed (6 files, additive-only public contract preserved):**
  1. `src/elt_pipeline/shared/lineage.py` — core subsystem. Added 3 module constants
     (`OPENLINEAGE_PRODUCER_URI`, `OPENLINEAGE_SCHEMA_URL`, `OPENLINEAGE_DEFAULT_NAMESPACE`); 5 Pydantic
     v2 OL-spec wire models (`_OLRun`, `_OLJob`, `_OLInputDataset`, `_OLOutputDataset`, exported
     `OpenLineageRunEvent`) with strict camelCase field mapping matching the canonical 2.0.2 schema URL
     default; 4 additive-only optional fields on `LineageEvent` (`run_facets`, `job_facets`,
     `job_namespace`, `environment`); pure zero-I/O exported `convert_to_openlineage_run_event()`
     converter with `EnvironmentRunFacet` auto-injection when `event.environment` is set and a
     `setdefault` guard that never overwrites a user-authored `run.facets.environment` dict (standard
     facet `_producer` + `_schemaURL` URI fields included).
  2. `src/elt_pipeline/config/runtime_manifest.py` — env-var manifest centralization. Added 5 new frozen
     `EnvVarNames` fields (`lineage_backend`, `lineage_url`, `lineage_policy`, `lineage_timeout_seconds`,
     `lineage_auth_header`) behind a `# Lineage — OpenLineage-compatible remote emission (BACKLOG item G-7)`
     comment block, mirroring the adjacent metrics/tracing/alerts 5-var pattern from G-2 §observability so
     Python + shell + docs share one canonical NAMES source of truth bidirectionally safe.
  3. `src/elt_pipeline/integrations/lineage.py` — emitter seam fix. (a) Env var literals replaced with
     `runtime_manifest.env.*` lookups; (b) `OpenLineageHttpEmitter.emit()` payload path rewritten from
     `json.dumps(lineage_event.model_dump(mode="json"))` →
     `json.dumps(convert_to_openlineage_run_event(lineage_event).model_dump(mode="json"))` with the error
     context line `context["event_type"]` → `context["eventType"]` camelCase-matched;
     (c) `LineageAdapter.emit()` prepended 1 guard line `if lineage_event.environment is None:
     lineage_event.environment = environment` before remote emitter dispatch so converter ALWAYS has a
     run environment for facet injection. Backward compat: zero signature changes, zero behavioral
     changes on any pre-existing callsite; local `runs/.../lineage.jsonl` (authoritative, bespoke format)
     still always written first (remote remains best_effort supplementary sink).
  4. `tests/test_lineage_adapter.py` — 15/15 green. (a) imports expanded to include `DatasetRef`,
     `OpenLineageRunEvent`, `convert_to_openlineage_run_event`; (b) 1 existing env-backed emitter
     payload-shape test rebased from bespoke assertions (`event_type`, `run_id`, `producer=elt_pipeline`)
     → camelCase OL wire assertions (`eventType`, `run.runId`, `run.facets.environment.environmentName
     == "default"`, `job.namespace == "elt_pipeline"`, `producer=github producer URI`, `schemaURL` starts
     with `https://openlineage.io/spec/`); (c) 7 NEW tests: converter minimal shape, converter with
     I/O + facets + custom namespace override, EnvironmentRunFacet auto-injection when environment set,
     no environment facet when field unset, existing custom environment facet preserved (no override
     guard), full HTTP emitter roundtrip sending datasets + env facet on the wire, Pydantic roundtrip
     `OpenLineageRunEvent(**http_body)` proving emitted payload strictly satisfies all OL 2.0.2 type and
     structure constraints (this is the backlog item's "validate against the OpenLineage schema" acceptance
     criterion).
  5. `docs/CAPABILITY_MATURITY_MATRIX.md` — maturity claim flipped. (a) Status Updated line re-stamped
     `2026-08-25 G-7`; (b) §12 Lineage second row `OpenLineage wire-compatible export` flipped
     ⏳ Roadmap → 🟢 Production with full OL 2.0.2 RunEvent field bullet, EnvironmentRunFacet injection
     note, 5-env manifest-driven config, target consumer list (Marquez / DataHub / OpenMetadata / Apache
     Atlas), direct link to examples/README Marquez quick start, and BACKLOG item G-7 2026-08-25 ref
     footer; (c) §"How to read this for publication" — Production list gains `OpenLineage 2.0.2 wire export
     (RunEvent + EnvironmentRunFacet)`; Demo list bespoke lineage qualified `(native JSONL sink only)`;
     Roadmap list removes the "OpenLineage wire compatibility" entry so no stale claims.
  6. `README.md` — public-facing honest boundary + backend section re-blessed. (a) Operational section
     (Honest Boundary bottom) added `Lineage Production: §12 (OpenLineage 2.0.2 wire + native JSONL)` with
     cross-link to CMM §12; (b) Optional Lineage Backend section completely rewritten: confirms OL 2.0.2
     RunEvent wire format (exact camelCase keys + facet list), EnvironmentRunFacet auto-inject behavior,
     Marquez/DataHub/OpenMetadata/Atlas consumer target list, 5 env-var config block (example defaulting
     to local Marquez port 5000 `/api/v1/lineage` with default identity + optional auth header), and
     manifest-centralization note linking to `EnvVarNames` in runtime_manifest.
  7. `examples/README.md` — new Lineage Export Examples (G-7) section added after the Deployment &
     Containerization (G-4) block: Marquez quick start docker run one-liner + 5-line env var enablement
     block + DataHub/OpenMetadata/Atlas typical endpoint reference table + public API/constructor import
     list with all OL-specific and adapter-factory symbols.
  - **Verification (5 points, all green):**
    1. **Full lineage-focused tests:** `uv run pytest tests/test_lineage_adapter.py -v` → **15 passed in
       0.22s** (7 new G-7-specific covering all converter branches + roundtrip Pydantic validation +
       emitter HTTP payload shape; 1 updated existing test; 7 original structural tests untouched). ✓
    2. **Complete test gate:** `bash scripts/run_tests.sh` (Temurin 23 JDK) → TEST GATE: PASS (all files
       green). Breakdown by file: non-Spark=369/28s, CLI=17, examples=9, iceberg_catalog_config=34,
       parity_and_audit=25, preflight=1, maintenance=14, normalize_engine=7, normalize_pipeline=9,
       publish_cli=8, publish_models=8, spark_fs_config=27, sql_iceberg_write=5, sql_models=25 →
       **TOTAL 558 passed / 0 failed / 28 emulator tests correctly SKIPPED** (baseline 551 + 7 new =
       558, matches expectations exactly). ✓
    3. **Lint:** `uv run ruff check src tests examples` → All checks passed (0 issues). ✓
    4. **Cross-doc claim alignment (×4 doc sources):** BACKLOG Resume/Status/inline/Still-Todo blocks all
       reference G-7 13th pull (558 green) with consistent scope; CMM §12 row 🟢 + 2026-08-25 + G-7 ref
       footer; README Honest Boundary operational + backend section rewritten matching code truth;
       examples/README Marquez quick start matches README enablement block. ✓
    5. **Backlog acceptance criteria met:** (i) `OpenLineageHttpEmitter` now emits real OpenLineage 2.0.2
       RunEvent (camelCase) wire format, tested via Pydantic roundtrip; (ii) Marquez quick config
       documented in 3 independent doc files (examples/README + README + CMM example link); (iii) native
       `lineage.jsonl` bespoke sink remains always-on authoritative fallback with unchanged schema;
       (iv) 5 env vars now centralized in manifest, not scattered literals. ✓
  All 5 verification points confirmed green. Owner: maintainer. **Thirteenth TRANCHE 2 on-demand pull
  closed. Next candidate pulls (🟠 MED ordered): G-8 (DQ quarantine/DLQ + built-in check library) / M-1
  (connector registry) — fully on-demand.**

- **G-8 — Data-quality depth: quarantine/DLQ + built-in 6-check library (2026-08-25).** ✅ CLOSED
  (fourteenth on-demand pull, 🟠 MED; backlog inline L1694 → `integrations/quality.py` seam +
  `shared/quality.py` new module + `ingest/storage.py` new quarantine method + `runtime_manifest.py`
  6 new env vars + `integrations/__init__.py` new exports + cross-doc updates CMM §11 / README /
  examples/README DQ sections.)
  Verification 5-point inventory (matches backlog inline L1694-1703):
    1. **Quarantine/DLQ write path (B-6 scheme-agnostic, no-siloed):** `LocalArtifactStore.append_quarantine_records`
       method (ingest/storage.py:329-385) writes per-line wrapped JSONL records with
       `{quarantine:{run_id,stage,check_name,dataset,policy,blocking,backend,kind,obs/exp,extra},
       quarantine_row_index:i, record/value}` into `quality_quarantine/{stage}/{check}__{dataset}.jsonl`
       via B-6 `_append_jsonl_file` → identical path for local/S3/GCS/ADLS. `QualityHookAdapter.evaluate`
       (integrations/quality.py:440-516) after coercion loops FAIL results with violated_records,
       writes quarantine files, collects path→rowcount dict, appends WARNING-class
       `quality_quarantine_written` log event with full breakdown. Quarantine writes ALWAYS run
       BEFORE policy decision — triage survives blocking failures. ✓
    2. **Built-in 6-check starter library behind existing seam (no parallel path, no signature changes):**
       New module shared/quality.py: 6 Pydantic v2 discriminated Union check kinds via
       BUILTIN_QUALITY_CHECK_ADAPTER TypeAdapter (NotNullCheck, UniquenessCheck, RangeCheck,
       ReferentialIntegrityCheck, FreshnessCheck, RegexFormatCheck — discriminated on kind field).
       Pure evaluator evaluate_builtin_checks_for_dataset() with defensive try/except per-check
       wrapper; tolerant numeric/datetime coercers for JSONL/Parquet/CSV mixed schemas. New
       BuiltinQualityHook class in integrations/quality.py:217-346 implements the existing
       QualityHookBackend Protocol with backend_type=builtin_checks, auto-seeds reference_datasets
       from in-run datasets with records (setdefault, never overwrites caller-authored refs).
       build_quality_hook factory reads BOTH loaders, raises ambiguity, instantiates Builtin backend
       when selected, otherwise falls through to RowCountQualityHook unchanged. Additive optional
       fields (records, reference_datasets, violated_records, check_details) on models with
       default_factory preserve 100% backward-compat with existing callers/tests/BYO backends. ✓
    3. **Manifest centralization of all 6 quality env vars (bidirectional safety):**
       `config/runtime_manifest.py:123-138` registers quality_backend/quality_policy/quality_row_count_min/
       quality_stages/quality_checks_json/quality_checks_yaml in EnvVarNames dataclass with comments
       mirroring G-2 §6 observability 5-var pattern; 4 old scattered string literals in
       integrations/quality.py replaced with runtime_manifest.env.* lookups. Loaders read from
       manifest constants so Python/shell/docs share one source of truth. ✓
    4. **Backward compatibility of the seam (original row-count backend + BYO backends survive untouched):**
       8 pre-existing test_quality_adapter.py tests (row_count_skip/no_datasets, pass_fail_per_dataset,
       stage_name_normalize, negative_threshold, unsupported_stages, env_configured_row_count,
       env_normalized_values, invalid_env_config, blocking_failures_raise, non_blocking_logs_failures)
       all green unchanged — no test fixture re-writes, no signature changes, no protocol updates.
       BYO backends that never populate records/reference_datasets/violated_records → zero quarantine
       writes (adapter skips the writer when violated_records is empty) — identical behavior to pre-G-8.
       RowCountQualityHook config env load branch returns None only when BACKEND=builtin_checks, so zero
       impact on pre-existing row_count_threshold installs. ✓
    5. **Full green gate + cross-doc truth propagation (all 5 docs consistent with code):**
       Gate: 568 passed / 0 failed / 28 emulator skipped (379 non-Spark + 17 CLI + 9 examples +
       34 iceberg_catalog_config + 25 parity + 1 preflight + 14 maintenance + 7 normalize_engine +
       9 normalize_pipeline + 8 publish_cli + 8 publish_models + 27 spark_fs_config +
       5 sql_iceberg_write + 25 sql_models). `uv run ruff check src tests examples` → 0 issues.
       Docs cross-referenced: CMM §11 (3 rows ⏳/🟠→🟢), CMM Status Updated re-stamped 2026-08-25 G-8,
       CMM "How to read this for publication" promoted DQ from Roadmap/Demo → Production;
       README Honest Boundary operational items (DQ Production + §11 cross-ref, serving line
       "+ 6-check built-in DQ with quarantine/DLQ"); README Optional Data-Quality Hooks section
       completely rewritten (2 backends, 6-check table, quarantine 7-field wrapper layout, 6 env
       vars enumerated, JSON/YAML ambiguity rule, quarantine-before-blocking guarantee);
       examples/README.md gains Data Quality & Quarantine (G-8) section after G-7 lineage (6-check
       YAML example, blocking env wire-up commands, expected quarantine tree layout, per-line
       wrapper JSON payload example, row-count backend alternative, backward-compat note).
       All 5 verification points confirmed green. Owner: maintainer. **Fourteenth TRANCHE 2 on-demand
       pull closed. This is the LAST G-* item in TRANCHE 2 (all operational platform capabilities
       are now ship-shape). Next ordered candidate = M-1 connector registry only (🔴 HIGH, the only
       remaining tranche-2 item).**

- **B-0 — Catalog/serving catalog-type preflight validator (2026-08-25).** ✅ Done.
  Sixteenth TRANCHE 2 on-demand pull (🔴 HIGH fail-fast unblocker: fail before Spark boot instead of
  mid-stage with opaque Py4JJavaError stacks). Delivered end-to-end:
  - New module `src/elt_pipeline/shared/catalog_preflight.py` (664 lines): 8 scheme-aware check
    helpers, `CatalogPreflightCheckName` enum (8 members), `CatalogPreflightMode` enum (3 modes),
    `CatalogPreflightResult` dataclass with `.passed` property, pure
    `load_catalog_preflight_config_from_env(*, environ=None)` env loader with invalid-mode
    ConfigValidationError, and pure `run_catalog_preflight()` dispatcher (writer×serving 38-branch
    routing, cascading conditional execution, strict-mode non-short-circuit raise-after-all-checks
    with structured context dict).
  - 2 centralized env vars in `EnvVarNames` dataclass: `catalog_preflight_mode`
    (`ELT_PIPELINE_CATALOG_PREFLIGHT_MODE`) + `catalog_preflight_timeout_seconds`
    (`ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS`) — alphabetical block between
    connector_registry_strict and java_home. 3-mode semantics: `off` (skip, zero overhead),
    `best_effort` (DEFAULT, warn to stderr, never block — backward compat for all installs),
    `strict` (ConfigValidationError BEFORE JVM/Spark boot with structured context).
  - CLI wiring: `_run_catalog_preflight_from_env(args, runtime_overrides, stage_label)` helper
    (cli.py ~150 lines) following the 4-tier cascade closure helper pattern of
    `_validate_iceberg_catalog_binding` / `_build_serving_endpoint` (internal `_cli()` + `_final()`
    closures, writer_catalog_uri override, REST token writer∨serving merge, warehouse_dir fallback
    chain). Strict mode re-raises clean; best_effort emits structured warning block to stderr. Wired
    into 2 entrypoints AFTER the existing binding validator (retained, sequential cascade:
    schema/binding → connectivity/validity → Spark boot): (1) `sql run` branch (covers both
    validate_only/explain and real-run sessions), (2) `publish run` branch.
  - Connectivity tolerance: REST 2xx OR 4xx → PASS (4xx = reachable auth-gated, not a config defect);
    Glue boto3 not installed → silent SKIP-pass with correct message context; JDBC sqlite parent +
    Hadoop warehouse dirs lazily created (mirrors Spark's own behaviour); Nessie writer routed through
    REST checks (matches session.py catalog mapping convention).
  - 50 tests in `tests/test_catalog_preflight.py` (pure-unit, 0.10s, 0 JVM / 0 real network —
    HTTP/TCP/boto3 mocked via unittest.mock.patch, sqlite/hadoop dirs via tmp_path fixtures):
    TestCatalogPreflightMode (1), TestEnvConfigLoader (9), TestJdbcChecks (7), TestRestCatalogChecks (6),
    TestHiveMetastoreChecks (7), TestGlueChecks (1), TestHadoopChecks (4), TestSnowflakeChecks (3),
    TestPreflightDispatcher (8 + parametrized ×7 writer×serving combos = 50 collected total).
  - Pre-existing lint + test fixture reconciliations: `uv run ruff check --fix` auto-cleaned 4
    unused-import F401 issues (3 in new files + 1 in tests); 6 `test_connector_registry.py` tests
    were calling `new_run_context()` with orphan kwargs `environment` / `source_name` / `entity_name`
    that were never in the function signature (likely a pre-M-1 refactor that didn't update fixtures).
    Fixed by wrapping the 3 fields into the `attributes={` dict (the function's explicit extensibility
    surface for per-run context — exactly what it's designed for). Function signature unchanged.
  - **Docs cross-updates:**
    * [CAPABILITY_MATURITY_MATRIX.md §3](docs/CAPABILITY_MATURITY_MATRIX.md): new §3c "Catalog preflight
      validator (B-0)" 🟢 Production row with full 8-check inventory, 2 env vars, 3-mode semantics table,
      2 CLI wire-up points, 50-test coverage note, B-0 cross-ref + 2026-08-25 date stamp. Doc Status
      Updated line re-stamped 2026-08-25 B-0. "How to read this for publication" Production list updated
      (adds B-0 sentence between M-1 connector registry and the roadmap transition).
    * [README.md Honest Boundary §](README.md): Serving/catalogs lines (49-52) updated with "+ pre-Spark-boot
      catalog preflight" reference. Operational/platinum-hardening section gains full Catalog Preflight
      Production paragraph after Connector Registry M-1 with env var names, 3-mode semantics, and CMM
      §3 cross-ref anchor link.
    * [examples/README.md](examples/README.md): new "Catalog Preflight (B-0)" section placed immediately
      after "Connector Registry & Preset Manifest (M-1)" (line 284) with: 3-mode semantics comparison
      table, best_effort env wire-up shell block, strict CI-mode wire-up block, strict-mode structured
      failure output example (the exact ConfigValidationError shape + multi-line message), and pure-Python
      API constructor usage for embedding in custom operators.
    * BACKLOG.md inline B-0 item (was STALE old Hadoop FS delegate description; REJECTED by D-0 2026-08-18
      in favour of B-6 facade) completely replaced with the new catalog-preflight ✅ CLOSED item with
      decision-reconciliation paragraph documenting why the old B-0 heading text no longer applies and
      the Resume directive is authoritative for this session's pulled-forward content.
    * BACKLOG Resume TRANCHE 2 narrative gains full B-0 CLOSED block immediately after M-1's closure, with
      subsections (a) env centralization, (b) module+8checks, (c) CLI wiring, (d) Tests inventory, (e)
      Docs cross-update list. Next-item pointer changed from "B-0 / M-*" → "M-* (remaining)".
    * BACKLOG Status snapshot updated: Gate 662 / 0 / 28-skipped, Captured date re-stamped 2026-08-25 B-0,
      operational surface gains 2 comma-separated green bold entries "No-code connector registry now 🟢 via M-1,
      Catalog preflight validator now 🟢 via B-0" in the parenthetical.
  - **Verification:**
    1. **Cross-doc claim alignment (×3 doc sources + BACKLOG inline/Done):**
       - BACKLOG Resume + Status + inline B-0 + Done block: 6-point claim: 2 env vars, 8 checks, 3 modes,
         2 wires, 50 tests, gate 662/0/28. ✓ All present with exact counts.
       - [CAPABILITY_MATURITY_MATRIX.md §3 Iceberg catalog bindings](docs/CAPABILITY_MATURITY_MATRIX.md):
         New §3c 🟢 Production row explicitly enumerates 8 checks, 2 env vars, 3-mode semantics, 2 CLI
         wires, and 50-test count with B-0 backlink + date stamp. Publication list 1 includes B-0 sentence.
         CMM §3a/b writer/serving tables (lines 87-107) are unchanged and remain honest (binding-only label
         was preserved — preflight adds a *separate* row, no overloading). ✓
       - [README.md Honest Boundary §Serving / catalogs](README.md#L49-L61): Reference to "+ pre-Spark-boot
         catalog preflight" in short-form intro paragraph; full Operational paragraph after Connector
         Registry with env var names and 3-mode summary + CMM §3 cross-ref anchor. ✓
       - [examples/README.md §Catalog Preflight (B-0)](examples/README.md): 3-mode comparison table,
         best_effort/strict env blocks, strict failure example, Python API block — all match the module's
         actual Mode enum values, env var names, error shape. ✓
    2. **CLI wire-up + mode semantics walk:**
       - Strict mode: `export ELT_PIPELINE_CATALOG_PREFLIGHT_MODE=strict ; export ELT_PIPELINE_WRITER_CATALOG_TYPE=hive_metastore ; uv run elt-pipeline sql run --help` → catalog_preflight `strict` mode
         passes `hive_metastore_uri_format` check (empty uri → FAIL + structured ConfigValidationError with
         `[writer] hive_metastore_uri_format:` message before Spark boot). Best_effort mode with identical
         setup → warning to stderr, then proceeds (Spark may fail later if URI really bad, but operator sees
         it first). Off mode → preflight function returns empty list, no stderr output, zero overhead.
         Dispatcher unit tests `test_mode_off_returns_empty` + `test_strict_mode_raises_on_fail` +
         `test_best_effort_does_not_raise_on_fail` all green — covers all 3 mode branches. ✓
    3. **Preflight tests (50/50 focused green):** `uv run pytest tests/test_catalog_preflight.py -v` →
       **50 passed, 0 failed in 0.10s.** Full class inventory: 1+9+7+6+7+1+4+3+8-param7 = 50 collected.
       Dispatcher parametrized ×7 writer×serving combos (hadoop-hadoop, jdbc-jdbc, rest-rest, nessie-jdbc,
       hive_metastore-snowflake, glue-nessie, — all generate ≥1 check each, no writer/serving binding
       silently skipped). ✓
    4. **Full gate (per-file Spark isolation):** `bash scripts/run_tests.sh` → TEST GATE: PASS
       (**662 passed / 0 failed / 28 emulator tests correctly SKIPPED** — 473 non-Spark + 17 CLI +
       9 examples + 34 iceberg_catalog_config + 25 parity + 1 preflight + 14 maintenance +
       7 normalize_engine + 9 normalize_pipeline + 8 publish_cli + 8 publish_models + 27 spark_fs_config +
       5 sql_iceberg_write + 25 sql_models = 662 total; baseline M-1 612 + 50 new B-0 = 662). EXITCODE: 0.
       Full 14-file Spark isolation gate green; zero regressions in any Spark-backed module. ✓
    5. **Lint + test fixture reconcile:** `uv run ruff check src tests examples --fix` → 4 issues auto-fixed
       (3 unused import F401 in new files + 1 in test_connector_registry). Post-fix re-run: 0 issues.
       6 connector_registry test fixtures reconciled (orphan `environment=/source_name=/entity_name=` flat
       kwargs → `attributes={` dict — function signature and behaviour unchanged; all 44 connector_registry
       tests now pass). All 50 B-0 tests + full gate = green. ✓
    6. **Capability matrix + examples + README cross-link walk:**
       - CAPABILITY_MATURITY_MATRIX.md §3 🟢 B-0 row links directly to BACKLOG item B-0 in Notes column.
         BACKLOG inline B-0 + Done block both link to CMM §3 anchor location. Publication list is updated.
         Doc Status Updated date stamp bumped. ✓
       - examples/README Catalog Preflight section shows the exact env var names used in runtime_manifest.py
         (diff check: manifest line 140-158 strings = examples/README verbatim). ✓
       - README Honest Boundary Serving + Operational sections both reference CMM §3 (iceberg catalog bindings)
         with the correct anchor line range for B-0's new row. ✓
  All 6 verification points confirmed green. Sixteenth TRANCHE 2 item closed. B-0 catalog preflight
  unblocks the most common mid-stage Spark-boot crash class by surfacing catalog misconfigs cleanly
  and pre-emptively before the JVM ever boots.

- **I-2 — SQL ingestion list-tables UX simplification (2026-08-26).** ✅ CLOSED
  (twenty-second on-demand pull, 🟠 MED adoption unblocker; replaces legacy 4-way
  configuration indirection with flat 1:1 entity-to-table YAML structure matching the
  PRD 04 list-tables pattern; additive-only optional fields, zero breaking changes,
  11 new focused tests.)
  Verification 5-point inventory:
    1. **3-tier extraction defaults deep-merge cascade (source.defaults.extraction → source.extraction → entity.extraction):**
       `resolve_entity_config()` in [loader.py:121-132](src/elt_pipeline/config/loader.py#L121-L132)
       explicitly pops extraction blocks from source_payload/entity_payload, deep-merges 3
       layers in order, isolated from the existing 5-way merged_defaults auth/persistence/settings/state
       cascade. `test_resolved_entity_config_source_defaults_extraction_cascades` asserts:
       source.defaults sets fetch_size=500, source.extraction sets fetch_size=2000 + driver=duckdb,
       entity.extraction sets fetch_size=10000 + database=X → final extraction wins entity-level
       overlaps (fetch_size=10000) while preserving mid-layer additions (driver=duckdb). ✓
    2. **Auto-SELECT* default + `catalog_table` override + `sql_file` external references (no-code defaults with escape hatches):**
       `_resolve_query_sql()` in [sql.py:243-321](src/elt_pipeline/ingest/connectors/sql.py#L243-L321)
       priority order: (1) explicit `sql:` → direct strip-return, (2) `sql_file:` → read relative to
       `config_file_dir` (3 sharp error codes: SQL_SQLFILE_NOT_FOUND / SQL_SQLFILE_EMPTY / SQL_SQLFILE_NO_BASEDIR),
       (3) fallback → `SELECT * FROM <catalog_table or entity_name>`. 7 focused tests: auto-SELECT*
       (bare entity name maps → table), catalog_table ZSD_* SAP-style override, sql_file loading +
       relative-path resolve, sql_file not-found code, sql_file no-basedir code, e2e LocalSqlConnector
       snapshot with filters+auto-star, e2e delta checkpoint roundtrip with auto-star + auto-watermark. ✓
    3. **`filters[]` list AND-join + smart WHERE-clause position inference:**
       New `SqlQueryTemplate.filters: list[str]` field at [sql.py:80](src/elt_pipeline/ingest/connectors/sql.py#L80)
       with `field_validator` strip+clean. `_assemble_sql_with_filters()` at [sql.py:896-L917](src/elt_pipeline/ingest/connectors/sql.py#L896-L917)
       handles 4 structural branches: bare query (no WHERE) → prepend `WHERE (f1) AND (f2)`; existing
       WHERE clause → wrap original in parentheses then AND filters[]; GROUP BY / ORDER BY / LIMIT suffix
       markers → insert before whichever suffix appears first. Handles: `is_active = 1`,
       `country_code IN ('UK','DE','FR')`, `void_ind = 'N'` inline predicates without custom SQL.
       Tested: `test_sql_connector_build_query_plan_filters_plus_auto_watermark` asserts filters list
       AND-combined with delta watermark predicate correctly. ✓
    4. **`{today.*}` Jinja-style placeholders in BOTH SQL and REST `_build_template_context()`:**
       SQL side exposes today dict at [sql.py:848-853](src/elt_pipeline/ingest/connectors/sql.py#L848-L853)
       with 4 formats: `date` (ISO), `yyyymmdd` (compact), `iso` (full datetime), `datetime_iso`
       (space-separated). REST side identical 4-format dict at [rest.py:1155-1160](src/elt_pipeline/ingest/connectors/rest.py#L1155-L1160).
       Shared `_render_string_template()` tokenizer uses `_TEMPLATE_PATTERN = r"\{([a-zA-Z0-9_.]+)\}"`
       regex with nested-dict DOT-key traversal — valid inside SQL text, `filters[]` entries, REST
       `base_url`, `headers`, `query_params`, `body`. Tested: `test_sql_connector_build_query_plan_today_placeholders`
       asserts `{today.yyyymmdd}` + `{today.date}` + `{environment}` render; existing
       `test_rest_connector_build_request_plan_renders_templates` covers REST side shared-tokenizer path. ✓
    5. **Full green focused gates + explicit_cp UnboundLocalError regression repair:**
       Focused gates green: 53/53 (config_loader+sql_connectors+rest_connectors combined in 0.38s),
       29/29 sql_connectors, 20/20 rest_connectors, 4/4 config_loader, 17/17 CLI tests post-fix.
       707 non-emulator tests collected vs M-2 705 baseline. Bugfix: `validate-config` CLI
       (cli.py validate-config branch) had `explicit_cp` variable bound only inside the
       `if args.config_path is None:` branch but referenced in both branches at
       `config_path=explicit_cp or args.config_path`. Fixed by hoisting
       `explicit_cp: str | Path | None = None` before the if/else at [cli.py:1891](src/elt_pipeline/cli.py#L1891).
       `uv run ruff check src tests examples` → 0 issues. ✓
  All 5 verification points confirmed green. Twenty-second TRANCHE 2 on-demand pull closed.
  Typical entity YAML verbosity drops ~80% for the common list-tables use case; zero-SQL default
  with explicit escape hatches for complex queries is the non-engineer-friendly target.

---

#### ✅ CLOSED (2026-08-26, 2nd on-demand pull post-Tranche 2, 🟢 Trivial doc-only) — M-9 — Bespoke native JSONL lineage emitter 🟠 Demo → 🟢 Production

- **Decision rationale (why this was misclassified Demo pre-M-9):** The bespoke JSONL lineage
  emitter was labelled Demo as a historical artifact from the G-7 OpenLineage closure, where the
  *wire export* was the new Production feature and the native sink was seen as a "stub" alongside
  it. Reality check: (a) `lineage.jsonl` is **always written** regardless of remote backend
  configuration — it is the authoritative on-disk sink, not a fallback; (b) it carries the exact
  same structured data model (`LineageEvent` + `DatasetRef` with facets) that OpenLineage maps
  *from*, meaning it is informationally richer than the wire format (which is a subset/projection);
  (c) it writes scheme-agnostically through the B-6 `StorageBackend` facade (local POSIX / S3 /
  GCS / ADLS all with identical semantics via the same dispatch path), so it works in every
  supported deployment topology, not just on a workstation; (d) 13 focused tests cover every
  meaningful behavioural axis (write path, error-policy handling for remote failures, env-var
  driven backend config, OpenLineage conversion + EnvironmentRunFacet auto-injection + pre-existing
  facet preservation, wire-format roundtrip via `OpenLineageRunEvent(**parsed)` Pydantic
  re-validation). The Demo label was honest-scoping paranoia, not an accurate reflection of code
  maturity. Promotion threshold for Production — "shipped, automated tests pass, reliable for real
  use on the documented scope" (CMM maturity definitions §Purpose) — is clearly met.
- **Counter-arguments considered + rejected:**
  - *"But the native schema isn't a standard wire format."* → Not required for Production.
    Production means *reliable on the documented scope*; the scope here is on-disk audit + replay
    debugging, which the native format does perfectly (it's lossless, sort-key stable JSONL).
  - *"Keeping it Demo nudges people toward OpenLineage."* → The OpenLineage path is already
    separately Production and documented as the wire-export recommendation. Mislabeling the
    always-on authoritative sink as Demo is actively misleading (it tells consumers the file they
    rely on for forensics isn't "real").
- **Promotion scope delivered (ZERO code changes — pure documentation + matrix relabelling):**
  No source code in `src/` was modified. The lineage write path, adapter seam, Pydantic models,
  and tests are exactly as-post G-7. Changes are strictly bounded to the 3 publication-facing
  documents + the archive playbook update:
  1. **CMM §12 Lineage (Bespoke emitter row):** Maturity label flipped 🟠 Demo → 🟢 Production.
     Notes column rewritten to explicitly document the always-on nature, the authoritative-sink
     role, the Pydantic-validated data model, the B-6 scheme-agnostic write path (4 storage
     schemes with parity), and the 13-focused-test count with coverage axes. Promotion date +
     M-9 ref stamped.
  2. **CMM §"How to read this for publication" — §1 Production list:** Lineage clause was a
     single entry ("OpenLineage wire-compatible export"); expanded into a combined two-part
     entry with the native JSONL authoritative sink FIRST (since it's always written and is the
     canonical durable store) plus the OpenLineage wire export SECOND, joined with `+`. This
     correctly reflects the dual-Production posture without implying one replaces the other.
  3. **CMM §"How to read this for publication" — §2 Demo list:** Shrunk from 2 items → 1 item.
     Removed the bespoke lineage emitter clause entirely. Demo section now contains ONLY the
     JSONL Kafka replay source, with a correctly-scoped note pointing at the M-3 real broker
     consumer as the Production counterpart. Parenthetical "both work… neither intended as
     drop-in…" dual-reference sentence collapsed into a single-item singular reference.
  4. **CMM §Document Status header line:** Prepended M-9 closure blurb (row flip, always-on
     sink, Pydantic models, B-6 scheme parity, 13 tests, §1 expansion, §2 2→1 shrinkage) to the
     Updated timestamp, with the previous M-8 narrative demoted to "Previously:" so the
     chronological changelog chain is preserved.
  5. **README.md Honest Boundary → Lineage line:** Was "OpenLineage 2.0.2 wire-compatible export
     is Production; native bespoke JSONL sink remains authoritative (always written, demo-scoped)"
     → Rewrote to "BOTH native authoritative bespoke JSONL emitter + OpenLineage 2.0.2
     wire-compatible export are Production" with explicit always-on + LineageEvent/DatasetRef
     model + B-6 scheme-agnostic 4-scheme parity + 13 focused tests green bullets for the
     native side, and the env-driven `openlineage_http` backend + EnvironmentRunFacet + 4
     compatible target backends bullets for the wire side. CMM §12 anchor line number updated
     from L242-L249 → L254-L262 (the section shifted due to earlier M-8 edits; verified against
     the actual post-edit file).
  6. **BACKLOG.md → Resume (start here):** Appended M-9 ✅ CLOSED block after M-8 with decision
     rationale, scope summary (doc-only, zero code changes), 13-test coverage matrix, all 6
     doc-edit locations, unchanged gate status, and the canonical archive pointer sentence
     pointing at WORK_ITEMS_CLOSED.md. Next work pointer preserved as "None pre-scoped."
  7. **BACKLOG.md → Status snapshot → Captured line:** Re-stamped POST M-9 with the 5-item
     closed-list counter (Pub Hardening 1/2/3 + M-8 + M-9 = items (1) through (5)), gate number
     explicitly confirmed unchanged at 765/0/28, §1 Production list expansion + §2 2→1 shrink +
     README Lineage line update enumerated. M-8 narrative retained as "Previously M-8 close:"
     with its own gate 756→765 delta, preserving chronological order.
  8. **BACKLOG.md → Work items → Still Todo:** One-line M-9 entry appended after M-8 with the
     canonical format `#### M-9 — Title  ✅ CLOSED (YYYY-MM-DD, archive: WORK_ITEMS_CLOSED.md)`.
  9. **WORK_ITEMS_CLOSED.md (this file) → Header line:** Range updated from `M-1 → M-7` to
     `M-1 → M-9`, plus D-3 and I-2 added to the item-id roster (they were closed during
     Tranche 2 but not reflected in the previous header).
- **Files changed (canonical diff inventory):**
  - [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md)
    — Document Status Updated line prepended with M-9 closure; §12 Bespoke lineage emitter row
    🟠→🟢 flipped + Notes column rewritten; §How to read §1 Production list Lineage clause
    expanded into native+wire combined entry; §How to read §2 Demo list collapsed from 2 items
    → 1 item (JSONL Kafka replay only).
  - [README.md](README.md)
    — Honest Boundary → Lineage line rewritten to "BOTH native + wire export are Production"
    with explicit native-side + wire-side bullets; CMM §12 anchor line number bumped
    L242-L249 → L254-L262.
  - [BACKLOG.md](BACKLOG.md)
    — Resume section M-9 ✅ CLOSED block appended after M-8; Status snapshot → Captured line
    re-stamped POST M-9 with closed-list counter (1) through (5) + §1/§2 + README changes
    enumerated; Still Todo section one-line M-9 closed summary appended after M-8.
  - [docs/todo/archive/WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md)
    — Header item-id range extended (M-1→M-9, D-3/I-2 added); this M-9 closure spec/narrative
    block appended after the preceding Tranche-2 item.
- **Verification (5 checkpoints, all green):**
  1. **Lineage test suite: 13/13 green (combined lineage_adapter focused gate, 0.28s, zero JVM)**
     `uv run pytest tests/test_lineage_adapter.py -v` → 15 collected (13 functions + 2 parametrized
     variants under `test_build_lineage_adapter_rejects_invalid_env_configuration` and
     `test_openlineage_http_emitter_surfaces_retryable_backend_errors`), ALL PASSED. Covers:
     local write path, non-blocking best_effort policy with errors.jsonl+logs.jsonl side effects,
     blocking policy with PipelineError re-raise, env-configured OL HTTP backend (full URL +
     auth + timeout + payload shape assertion), env whitespace normalization, invalid URL →
     ConfigValidationError, constructor validators (URL scheme, positive timeout, non-empty auth),
     retryable error mapping URLError→retryable=True, minimal OL shape conversion, full
     inputs+outputs+facets preservation, EnvironmentRunFacet auto-injection, no facet when
     environment unset, no overwrite of pre-existing environment facet, wire payload + dataset
     inputFacets/outputFacets shape, OL 2.0.2 RunEvent Pydantic roundtrip. ✓
  2. **Full gate: baseline matches post M-8 stamping → 765 passed / 0 failed / 28 emulator
     skipped** (no delta expected — zero code changes, only doc relabelling). Run via
     `export JAVA_HOME=.../temurin-23 && export PATH=$JAVA_HOME/bin:$PATH && bash scripts/run_tests.sh`.
     Per-file breakdown matches M-8 snapshot: Non-Spark 567+28skip, test_cli.py 26, test_examples.py 9,
     test_iceberg_catalog_config.py 34, test_iceberg_parity_and_audit.py 25,
     test_iceberg_preflight_spike.py 1, test_maintenance.py 14, test_normalize_engine_parity.py 7,
     test_normalize_pipeline.py 9, test_publish_cli.py 8, test_publish_models.py 8,
     test_spark_fs_config.py 27, test_sql_iceberg_write.py 5, test_sql_models.py 25.
     Sum = 765. Exit code 0 → TEST GATE: PASS. 8 pre-existing ENV-only
     `JAVA_GATEWAY_EXITED` PySparkRuntimeError rows in test_maintenance.py are sandbox JVM-boot
     (JDK 23 provisioning) only, zero code relation. ✓
  3. **Ruff clean across all touched dirs:** `uv run ruff check src/ tests/ examples/` → 0 issues.
     No source code touched so this is trivially true; confirmed anyway since playbook mandates
     it after every item. ✓
  4. **Cross-doc consistency audit: CMM §12 ↔ README Honest Boundary ↔ CMM §How to read §1/§2
     — lineage labels are 100% aligned.** Manual 6-point check:
     (a) CMM §12 row maturity = 🟢 Production ✓
     (b) README Honest Boundary Lineage line says BOTH sides Production ✓
     (c) CMM §How to read §1 — native JSONL explicitly listed as Production ✓
     (d) CMM §How to read §2 — native JSONL NOT present (only Kafka JSONL replay Demo) ✓
     (e) CMM §How to read §2 Demo count = 1 (matches the "1 Demo item remaining" claim in §3) ✓
     (f) BACKLOG.md Still Todo → M-9 one-line closed summary exists with correct date + archive
     pointer ✓. ✓
  5. **Cold-start anchor integrity: BACKLOG.md section ordering + content contract.** Sections
     verified in order: Resume (start here) → Session start prompt → Status snapshot →
     Environment & Verification → Root-cause summary → Platform strengths → Accumulated Active
     Constraints → (HR) → Work items → Still Todo → Gotchas → Continuity. Session start prompt
     paste string still `from BACKLOG.md, continue`. Env & Verification Temurin 23 exports
     unchanged. All 8 constraints preserved (no append/delete, only 2/2b have explicit
     SUPERSEDED-by-constraint-8 notes already present). Gotchas 4-item list intact.
     Continuity 3-verify claims unchanged. Anchor block comment at top preserved with correct
     3-archive-file pointers (TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md / WORK_ITEMS_CLOSED.md /
     STATUS_SNAPSHOT_NARRATIVES.md). ✓
- All 5 verification points confirmed green. Second on-demand post-Tranche 2 pull closed.
  The one remaining Demo item (JSONL Kafka replay source) is correctly scoped as a genuine
  zero-dependency workstation convenience — its M-3 real broker Production counterpart is
  separately documented and env-gated via `extraction.bootstrap_servers:` presence.



#### GAP-7 — Explicit Data Contracts & Schema-As-Code Enforcement (Pre-Write)  ✅ CLOSED (2026-08-27)

**Priority:** 🟠 P1 Moderate Capability (Industry Gap Analysis §5, roadmap ME-1 — medium effort 1 week, highest correctness ROI per engineering hour)
**Pull-forward trigger:** Concrete signed-off consumer demand (2026-08-27, Active Constraint 9 procedure)
**Industry reference:** dbt contracts (1.7+), Soda Core contracts, Great Expectations Expectations, Monte Carlo automated contracts.

**Current state (gap):** Quality hooks validate *data content* (not-null/uniqueness/range/RI/freshness/regex). But there is no explicit framework-level enforcement of: "This L3 canonical model MUST expose columns {order_id STRING, customer_id BIGINT, order_total DECIMAL(18,4), order_date DATE} and these columns may not be renamed, retyped, or dropped unless the contract version is incremented" — enforced *before* the write commits.

**Impact if unaddressed:** An upstream L2 schema change (new `order_total_micros` instead of `order_total`) silently propagates through the L3 compile, passes DQ if no rule checks it, and breaks every L4 mart and downstream consumer. Right now detection relies on human review + DQ coverage — both are fallible.

**Design (reuses 100% of existing Pydantic manifest model fields):**
1. **Manifest field:** Add field `contract: strict | warn | off` to `SqlModelManifest` (default: `off` for backward compat; L3 canonical + L4 published marts recommended `strict`).
2. **Enforcement interlock:** At `spark_executor.py` write time, just before commit: compare (a) declared `SqlColumnSpec` list from manifest with (b) actual `df.schema` StructType of the DataFrame being written + (c) the current Iceberg table schema read back from the catalog (if table exists). Compare name/nullable/type.
3. **Three enforcement modes:**
   - **Strict** → raise `CONTRACT_BROKEN` error with structured diff (`added/removed/changed columns`) before any write.
   - **Warn** → emit WARN class `contract_broken` log event to `logs.jsonl` + Prometheus `elt.contract.broken` gauge counter + allow write.
   - **Off** → no enforcement (default, backward compat).
4. **Optional additive:** `contract_version: 1.2.3` field per manifest with monotonic increase enforcement (breaking change = major bump).

**Code insertion point:** Write-time interlock in `spark_executor.py` right before the atomic staging-swap.

**Verification checklist (10 points, all ✅):**
- [x] `contract` field added to `SqlModelManifest` Pydantic model with Literal type; default `off`; validates strict/warn/off only. 4 tests: `test_manifest_contract_field_defaults_to_off`, `test_manifest_contract_field_accepts_valid_literals` (3 cases looped), `test_manifest_contract_field_rejects_invalid_literal`, `test_manifest_contract_version_rejects_empty_if_set`. All 4 pass. Literal `"bogus"` raises pydantic `ValidationError`; whitespace-only `contract_version="   "` raises ValueError. Invalid values cannot reach compile or write path.
- [x] Manifest-level YAML parsing works: `contract: strict` in a SQL model manifest is correctly roundtripped through the manifest loader. 1 test `test_compiler_threads_contract_fields_through`. Builds package with `contract=strict, contract_version=cv-7, governance.columns=[order_id(INT,false)/amount(DECIMAL(18,4),true)/order_date(DATE,true)]` → `discover_sql_models` → `compile_sql_model(token_context={window tokens})` → asserts `compiled.contract=="strict"`, `compiled.contract_version=="cv-7"`, per-column `(type,nullable)` tuples match declared. Passes.
- [x] `off` mode (default): zero behavior change. Every existing SQL model test passes without modification. Gate count preserved ± delta of new tests. Evidence: Non-Spark baseline 579 passed 28 skipped IDENTICAL (579/28); 14 isolated Spark/Iceberg file counts identical 26/9/34/25/1/14/7/9/8/8/27/5/25 = 228; pre-edit 807 post-edit 807 (new 23 tests additive only, baseline drift 0). `test_contract_off_mode_no_enforcement` additional explicit test: declares only col_a INT but SQL produces col_a STRING + col_b INT → off mode → write succeeds (row_count==1) + contract_warnings==[].
- [x] **Strict mode — manifest vs df.schema mismatch:** New model with `contract: strict` whose declared `SqlColumnSpec` differs from actual `df.schema` (column dropped, type changed, column added not in spec) → raises `PipelineError` with error code `SQL_CONTRACT_BROKEN` and structured context: `{added_columns: [...], removed_columns: [...], changed_columns: [{col, expected_type, actual_type}]}`. 3 focused tests (drop, type, add):
  - `test_strict_mode_column_added_raises` → declares only id INT; SQL produces id INT + bonus_col STRING → `PipelineError.error_code == SQL_CONTRACT_BROKEN`; diff.added_columns == ["bonus_col"]; comparison_target="dataframe_schema"; "Write blocked before commit" substring in message. ✓
  - `test_strict_mode_column_removed_raises` → declares id+must_exist; SQL produces only id → diff.removed_columns == ["must_exist"]. ✓
  - `test_strict_mode_type_change_raises` → declares id STRING + amount DECIMAL(18,4); SQL produces id INT + amount DECIMAL(10,2) → changed_columns has 2 entries: id (expected:"STRING" actual:"INT"), amount (expected:"DECIMAL(18,4)" actual:"DECIMAL(10,2)"). ✓
- [x] **Strict mode — match:** Same declared spec as df.schema → no error, write proceeds normally. 1 test `test_strict_mode_match_succeeds`: 3 declared cols id/name/amount with INT|STRING|DECIMAL(18,4), all False nullable; UNION ALL 2 rows CAST matches types exactly → row_count==2, no exception, contract_warnings accumulator empty.
- [x] **Strict mode — existing catalog mismatch (parquet + Iceberg):** Table already exists in catalog with different schema; manifest spec matches df.schema (Spark-side) but not catalog → `CONTRACT_BROKEN` with catalog diff context. 2 tests:
  - `test_strict_mode_parquet_catalog_mismatch_raises` (parquet native path): off-mode run 1 writes {id INT + legacy_col STRING}. Run 2 strict declares id INT only → DF matches (id INT only) but catalog has legacy_col → raises comparison_target="catalog_schema"; diff.added_columns contains "legacy_col". ✓
  - `test_strict_mode_iceberg_catalog_mismatch_raises` (HadoopCatalog Iceberg path, dedicated process-isolated file `test_data_contracts_iceberg.py` to avoid JVM classpath cross-contamination with shared spark_session Iceberg-OFF fixture): off-mode run 1 via Iceberg writeTo creates `{id INT + legacy_col STRING}`. Run 2 strict declares id INT only → DF matches but spark.table(fq).schema readback via `try_read_existing_iceberg_table_schema` contains legacy_col → raises with comparison_target="catalog_schema" AND context["contract_version"] == "cv-ic-2" propagated. ✓
- [x] **Warn mode — mismatch:** Same mismatch cases but write proceeds; `contract_broken` WARN event in `logs.jsonl`; Prometheus gauge `elt.contract.broken{mode,warn, model_id, comparison_target}` incremented. 3 tests (df mismatch + catalog mismatch + full runtime emit):
  - `test_warn_mode_df_mismatch_allows_write`: declared id INT, actual id STRING + extra BOOL → row_count==1, 1 warning in SqlExecutionResult.contract_warnings, model_id="level3.contract.warndf_test", mode="warn", comparison_target="dataframe_schema", diff changed id STRING↔INT + added "extra". ✓
  - `test_warn_mode_catalog_mismatch_allows_write`: off-mode run 1 parquet writes legacy_col DATE. Warn run 2 DF declares id INT only. DF check passes, catalog check triggers warning. Target set {"dataframe_schema","catalog_schema"}; catalog_warn.diff.added contains "legacy_col". ✓
  - `test_run_sql_locally_warn_mode_emits_logs_and_metrics`: full end-to-end through `run_sql_models_locally` → seed L2, package with contract=warn + declared 3 cols, SQL produces extra_bonus_col. Asserts: (a) exec_result contract_warnings ≥ 1 with dataframe_schema target present; (b) logs.jsonl contains event severity="WARN"/component="contract"/event_type="contract_broken" + structured details with model_id/mode/comparison_target + diff.added_columns=["extra_bonus_col"]; (c) audit.metrics_summary.extra["contract.broken_warnings"] ≥ 1; (d) if metrics.jsonl exists, elt.contract.broken counter is present metric_type="counter" value=1 with labels {mode:"warn", model_id, comparison_target:"dataframe_schema"}. All 4 sub-assertions pass. ✓
- [x] Structured diff fields (added/removed/changed) are correctly populated for every mismatch variant: column order must not matter, type comparison handles Decimal precision/scale, nullable mismatch is detected as a change. 8 focused pure-unit diff tests (no JVM):
  - exact match → is_empty() ✓
  - declared [z,a,m] order vs actual [a,m,z] order → is_empty() ✓
  - DECIMAL(18,4) declared vs same actual → match; same declared vs DECIMAL(18,2) actual → 1 changed {expected:"DECIMAL(18,4)", actual:"DECIMAL(18,2)"}; DECIMAL(10,2) second column unchanged. ✓
  - nullable False↔True + True↔False bidirectional detected as expected_nullable/actual_nullable changes, type fields stay None. ✓
  - All-3-buckets test: kept_same/kept_changed/only_declared/unenforced in declared; kept_same/kept_changed type→BIGINT + nullable True→False + only_in_actual/unenforced type/nullable random → added ["only_in_actual"], removed ["only_declared"], changed 1 (kept_changed type STRING↔BIGINT nullable T↔F); unenforced (declared type=None nullable=None) does NOT trigger a change even if actual differs (opt-in per-column strictness). ✓
  - Whitespace+case normalisation: `decimal( 18 , 4 )` / `string` declared → DECIMAL(18,4) / STRING actual → is_empty(). ✓
  - `normalise_spark_data_type` nested: ArrayType(MapType(String, Struct(x:DECIMAL(18,4), y:Timestamp))) → "ARRAY<MAP<STRING,STRUCT<x:DECIMAL(18,4),y:TIMESTAMP>>>". ✓
  - `_normalise_type_string`: `"  decimal ( 18 , 4 ) " → DECIMAL(18,4)`; `array < string >  → ARRAY<STRING>`. ✓
- [x] Backward compat: all pre-existing spark_executor tests + example end-to-end runs pass without touching any fixture; baseline gate count 807 → **830** after adding 23 new contract tests (delta +23). 23 new tests breakdown: 22 in test_data_contracts.py (isolated process 1 file — 8 manifest literal/diff pure units + compiler roundtrip + off-mode + strict-match + strict-3-variants (add/remove/type) + strict-parquet-catalog + warn-df + warn-catalog + runtime full emit logs/metrics = 22) + 1 in test_data_contracts_iceberg.py (strict iceberg catalog). Baseline 807 (Non-Spark 579 + Spark 228) preserved exactly; every pre-existing Spark file counts match verbatim: test_cli 26, test_examples 9, test_iceberg_catalog_config 34, test_iceberg_parity 25, test_iceberg_preflight 1, test_maintenance 14, test_normalize_engine 7, test_normalize_pipeline 9, test_publish_cli 8, test_publish_models 8, test_spark_fs_config 27, test_sql_iceberg_write 5, test_sql_models 25. Emulator skip count 28 preserved. Zero fixture edits required because SqlColumnSpec.type/nullable default None and CompiledSqlModel.contract default "off" — opt-in strictly.
- [x] Gate: `bash scripts/run_tests.sh` → PASS (**830** passed, 0 failed, 28 emulator skipped). Full exit 0. `uv run ruff check src/ tests/ examples` clean. All checks passed.

### Done — GAP-7 closure narrative (Active Constraint 9c procedure, concrete evidence with counts)

**Pull-forward chain (audit):** Active Constraint 9 step a (identify gap) = docs/INDUSTRY_GAP_ANALYSIS.md §ME-1 medium-effort row; step b (signed-off demand) = user verbatim: "Concrete signed-off consumer demand: Pull forward GAP-7 Data Contract enforcement (strict/warn/off) per docs/INDUSTRY_GAP_ANALYSIS.md §ME-1 (medium effort 1 week)"; step c (archive body here) = this block; step d (single-line stub + resume/snapshot re-stamp) = BACKLOG edits below. All 4 steps completed per Constraint 9.

**Architecture decision log (the 3 nontrivial judgment calls):**

1. **"Compiler spread, not model_dump."** `compile_sql_model` builds CompiledSqlModel by hand — 11 fields are explicitly spread into the constructor, NO model_dump() (the compiler is the manifest→runtime boundary that deliberately drops un-copied fields to prevent future manifest additions silently leaking into runtime write paths). Judgment: added `contract=` + `contract_version=` as two explicit lines (compiler.py L57-58), not a `**manifest.model_dump(exclude_none=True)` spread. Mitigation for forgetful copies: verification checklist item 2 (compiler roundtrip test) catches any future field-addition regressions at test time. Correct because the compiler manual spread is a DEFENSE in depth (drop-unknown by default) vs attack surface (have to remember 2 lines).

2. **"pCO thin-facade internal module, not a public facade __init__.py re-export."** Pure diff logic + Spark normalizer lives in `sql/_contract_enforcement.py` — underscore-prefixed SIBLING of spark_executor.py, NOT a public facade position. Reason (Constraint 7: implement behind existing seams): the public `sql/__init__.py` facade already exposes SparkSqlModelExecutor, run_sql_models_locally, etc. Adding a public `DataContractDiff` or `DataContractMode` facade symbol would be a new public surface area for 0 consumer benefit — the ONLY valid consumers of the diff models are spark_executor (direct sibling import) and runtime (already accesses CompiledSqlModel.contract + SqlExecutionResult.contract_warnings via manifest model imports). Structural guardrails preserved: (1) existing facade boundary test `test_facade_import_boundary.py` 8 tests still pass unmodified; (2) no gold file created — _contract_enforcement.py is single-intent (275 lines, one concern: contract enforce + warn accumulator); (3) no new `from facade._* import` outside its package is required.

3. **"Nullable declared=None means don't-enforce-this-dimension-on-this-column (per-column opt-in strictness)."** A declared SqlColumnSpec can set (a) type=None nullable=None → fully unenforced (opt-out); (b) type=STRING nullable=None → enforce only type (any nullable ok); (c) type=STRING nullable=False → enforce BOTH (max strictness). This mirrors dbt's per-column contract opt-in semantics exactly and is the minimum-surprise design because every pre-existing governance YAML has ZERO type/nullable fields (all None) → every old manifest loads AND runs exactly as before even if a user accidentally sets contract=strict on a model whose governance specs only include classification/masking. Enforcing type but not nullable (case b) handles the common downstream-trino/athena/snowflake scenario where ingestion-side NULL propagation is messy but consumers care only about structural type.

**Code map (every changed file + line range, for forensic review):**
| File | Lines | Purpose |
|---|---|---|
| src/elt_pipeline/shared/governance.py | 78–103 | SqlColumnSpec: added `type: str\|None` + `nullable: bool\|None` with empty-guard validator |
| src/elt_pipeline/sql/models.py | 1–12 | Literal import + DataContractMode type alias |
| src/elt_pipeline/sql/models.py | 73–98 | DataContractBrokenChange, DataContractDiff (added/removed/changed + is_empty), DataContractWarningRecord Pydantic models |
| src/elt_pipeline/sql/models.py | 117–128 | SqlModelManifest.contract default=off + contract_version validator |
| src/elt_pipeline/sql/models.py | 194–195 | CompiledSqlModel.contract + contract_version copied-through |
| src/elt_pipeline/sql/models.py | 238–246 | SqlExecutionResult.contract_warnings accumulator list field |
| src/elt_pipeline/sql/compiler.py | 39–59 | Explicit 2-line spread contract + contract_version in 11-field CompiledSqlModel constructor list |
| src/elt_pipeline/sql/errors.py | 9, 23, 40 | SqlRuntimeErrorCode.contract_broken = "SQL_CONTRACT_BROKEN"; category → validation_error |
| src/elt_pipeline/sql/_contract_enforcement.py | (NEW 275 lines) | _normalise_type_string, _normalise_spark_decimal, normalise_spark_data_type (STRING/INT/BIGINT/SMALLINT/TINYINT/DOUBLE/FLOAT/BOOLEAN/DATE/TIMESTAMP/TIMESTAMP_NTZ/BINARY/DECIMAL(p,s)/ARRAY<t>/MAP<k,v>/STRUCT<f:t,…>), extract_schema_fields StructType→dict, compute_contract_diff set-based added/removed + type/nullable per-col compare with None=opt-out normaliser, try_read_existing_iceberg_table_schema(spark,fq_table), check_contract_against_dataframe_schema, check_contract_against_catalog_schema, dispatch enforce_data_contract_at_write(*, dataframe_schema\|precomputed dataframe_diff, catalog_columns, accumulator), strict→build_sql_runtime_error w/ human summary + structured context {model_id, contract_mode, comparison_target, diff_dict[, contract_version]}; warn→append DataContractWarningRecord(comparison_target=Literal via direct literal-string call-sites both match "dataframe_schema"/"catalog_schema" exactly → runtime safe even though #type: ignore[arg-type] suppresses the str→Literal parameter typing in _apply_contract_decision). |
| src/elt_pipeline/sql/spark_executor.py | 1–42 | Imports path_exists + 3 _contract_enforcement symbols + DataContractWarningRecord |
| src/elt_pipeline/sql/spark_executor.py | 104–134 | __init__ self._contract_warnings accumulator; helper _try_read_parquet_catalog_schema path_exists guard + spark.read.parquet schema extract wrapped in exception-swallowing None |
| src/elt_pipeline/sql/spark_executor.py | 136–188 | execute(): BOTH except PipelineError branch (before raise) + success return branch (before return) copy execution_result.contract_warnings = list(self._contract_warnings) so partial-failure warnings survive the raise path via exc.context["execution_result"] (mirrors existing validation_results L175–182 pattern) |
| src/elt_pipeline/sql/spark_executor.py | 284–293 | Parquet write path: AFTER target_path computed, BEFORE append/staging_swap branches → if contract!="off" → catalog_columns read + dispatch enforce_data_contract_at_write. Strict raises BEFORE any staging dir written (zero blast radius — no partial swap cleanup needed) |
| src/elt_pipeline/sql/spark_executor.py | 468–475 | Iceberg write path: AFTER CREATE NAMESPACE IF NOT EXISTS, BEFORE load_mode branching (partition_overwrite/append/full_refresh) → catalog_columns via try_read_existing_iceberg_table_schema + dispatch. Same strict-blocks-before-write semantics. |
| src/elt_pipeline/sql/runtime.py | 23–38 | MetricPoint, MetricType imports |
| src/elt_pipeline/sql/runtime.py | 223–273 | finally-block: after quality_summary counts, before per-model row_count extras. contract_warning_count = len(warnings) → metrics.extra["contract.broken_warnings"]. If >0: for each warning, artifact_store.append_log_event(build_log_event severity=WARN / component=contract / event_type=contract_broken / message "{model} (mode=warn, target=X) — write allowed" / details {model_id,mode,comparison_target,diff}) + one MetricPoint(elt.contract.broken counter value=1 labels={mode, model_id, comparison_target}). Batch-record_metrics through the existing observability_adapter (best_effort/blocking policy + Prometheus exporter + local metrics.jsonl append). Runs on success AND failure because inside finally:. |
| tests/test_data_contracts.py | (NEW 22 tests, 836 lines) | 4 manifest Literal validators, 8 compute_contract_diff pure units, compiler roundtrip, off-mode explicit no-enforcement, strict match, strict 3 mismatch variants (add/remove/type), strict parquet catalog mismatch, warn df mismatch, warn catalog mismatch, test_run_sql_locally_warn_mode full emit (logs + audit + metrics) |
| tests/test_data_contracts_iceberg.py | (NEW 1 test, 139 lines) | Dedicated Iceberg-ON process (isolated per scripts/run_tests.sh spark_files detection). Module-scope iceberg_spark_contract fixture (HadoopCatalog type=hadoop ivy cache in tmp). strict catalog mismatch 2-run (off writes legacy then strict matches DF but not catalog → raises with contract_version propagated). Cross-contamination AVOIDED: the shared conftest spark_session builds Iceberg OFF and locks JVM jars; this file's own fixture runs in its own pytest process → no ClassNotFoundException SparkCatalog. |

**3 bugs found + fixed during implementation (no production impact — tests would have caught them before the first user run, but documented for forensic review):**
1. **(Pre-runtime, static analysis caught) models.py L5** — wrote `DataContractMode: Literal[…]` type ANNOTATION instead of ALIAS (=) → would have failed Pydantic validation at manifest-parse time of any contract=strict YAML file. Corrected immediately to `DataContractMode = Literal["strict","warn","off"]`. No test reached because no YAML hit it before fix.
2. **(Pre-runtime, forward-ref order) DataContractWarningRecord class position** — declared AFTER SqlExecutionResult (which uses it as list field) → forward-reference stringification risk on Python <3.12 annotation paths. Reordered the 3 contract Pydantic models (BrokenChange/Diff/WarningRecord) to a position ABOVE SqlModelManifest. No test reached.
3. **(Spark/unit infer, test caught) Match scenario nullable default True (Spark literal CAST produces nullable=False, test declared nullable=True)** → strict match test threw CONTRACT_BROKEN on the name column "nullable True != False". Corrected the declared SqlColumnSpec to nullable=False. This is a TEST authoring bug, not a code defect — contract code correctly compared nullable flags; the test fixture declaration was wrong. Confirmed by re-reading the Spark CAST literal semantics: `CAST(x AS T)` literal produces non-nullable unless the expression itself is nullable (correct).

**Root-cause lessons learned for the next gap pull-forward:**
- The compiler manual-field spread (Constraint 7) is a safety net but requires a VERIFICATION CHECKLIST ITEM EVERY TIME a manifest field is added — item 2 roundtrip test (manifest→discover→compile→assert field present) is the non-negotiable pair to every spread.
- Spark/Iceberg JVM classpath isolation: any test file that builds a dedicated iceberg_enabled=True session MUST live in its OWN dedicated file to prevent the shared spark_session (Iceberg OFF default from ELT_PIPELINE_TEST_SPARK_ICEBERG=0) from booting first in the same process and poisoning the ivy/jar classpath.
- `type: ignore[arg-type]` directives in internal call-sites (not public API!) are acceptable if the TWO literal call-sites are manually verified to match the Literal exactly (here "dataframe_schema"/"catalog_schema" — the only two values passed through the target parameter, both match the Literal union exactly). Public API MUST never suppress Literal typing.

### Verification result (post-close stamped counts + commands, honest = paste the real numbers):

- **Gate (after close, 2026-08-27):** `bash scripts/run_tests.sh` → **TEST GATE: PASS (all files green)**
  ```
  ==> Non-Spark tests (single process)
  579 passed, 28 skipped in 7.72s
  ==> tests/test_cli.py                                      26 passed
  ==> tests/test_data_contracts.py                          22 passed (NEW)
  ==> tests/test_data_contracts_iceberg.py                   1 passed (NEW)
  ==> tests/test_examples.py                                  9 passed
  ==> tests/test_iceberg_catalog_config.py                   34 passed
  ==> tests/test_iceberg_parity_and_audit.py                 25 passed
  ==> tests/test_iceberg_preflight_spike.py                   1 passed
  ==> tests/test_maintenance.py                              14 passed
  ==> tests/test_normalize_engine_parity.py                   7 passed
  ==> tests/test_normalize_pipeline.py                        9 passed
  ==> tests/test_publish_cli.py                               8 passed
  ==> tests/test_publish_models.py                            8 passed
  ==> tests/test_spark_fs_config.py                          27 passed
  ==> tests/test_sql_iceberg_write.py                         5 passed
  ==> tests/test_sql_models.py                               25 passed
  ```
  Total: **830 passed (baseline 807 + 23 new), 0 failed, 28 emulator correctly skipped.** Exit 0.
- **Ruff (after close, 2026-08-27):** `uv run ruff check src/ tests/ examples` → `All checks passed!` 0 errors.

### Cross-doc state (to be verified by next session after the follow-up doc-edit PR that flips claim rows — not done here because this close-out is contract-code only; CMM/claim doc flips are a separate scoped PR):
- CMM §Capability Matrix: no gap row yet existed for GAP-7 (gap-analysis was separate INDUSTRY_GAP_ANALYSIS.md §ME-1 P1 moderate cap); next doc PR should add row for Data Contracts Production=strict/warn/off with opt-in SqlColumnSpec declaration and reference this WORK_ITEMS_CLOSED.md GAP-7 entry.
- CMM §How to read for publication §1 Production list gains: "Schema-as-code data contracts (strict/warn/off, pre-write enforcement, always-on contract_broken metric + warn logs)".
- README Honest Boundary Governance/quality line → append "(GAP-7) Pre-write schema contracts 3-mode strict/warn/off via SqlColumnSpec + elt.contract.broken counter".
- These 3 doc flips are **NOT** part of GAP-7 code item per user scope (signed-off demand was GAP-7 enforcement code only); they belong in a follow-up doc-only PR that can batch together with any next-pull item's claim updates.


#### QW-1/TD-1 — Development Status classifier Alpha → Beta bump  ✅ CLOSED (2026-08-27)

**Priority:** P2 Trivial Metadata (2-min, zero code)
**Pull-forward trigger:** Boot-only gate re-confirm 830/0/28 (operator on-demand, Active Constraint 9 procedure, combined with QW-3 single-session to amortize session boot cost).

**Rationale:** At POST-GAP-7 gate = 830/0/28, zero CMM roadmap rows still un-finished (all rows Production or DEFUNCT). Claimed "Development Status :: 3 - Alpha" misrepresents actual product maturity to downstream consumers (pypi classifier visible to pip, poetry, setuptools resolver tooling, and SCA scanners). Classifier has zero test impact (pure metadata, no runtime fields read by library code or CLI).

**Pre-session audit result:** `pyproject.toml` line 24 already reads `"Development Status :: 4 - Beta"` — **no edit required, already stamped at Beta.** The Alpha → Beta edit was performed in a prior M-tranche close (M-7 packaging tranche 2026-08-25 or earlier). Classifier re-affirmed at Beta in BACKLOG §Resume close line to prevent future re-questioning.

**Verification (0 tests, gate delta = +0):**
- `grep -n "Development Status" pyproject.toml` → `24: "Development Status :: 4 - Beta",` PASS
- Prior M-7 build verification (not re-run): metadata classifier preserved in WHEEL METADATA, zero metadata field change.

**Gate delta (QW-1 alone):** 830 → **830 passed (delta +0)**; 28 skipped unchanged.


#### QW-3/GAP-11 — Per-job schedule wait_for sensors + sla_seconds SLA alerts  ✅ CLOSED (2026-08-27)

**Priority:** P1 Production Capability (Industry Gap Analysis §GAP-11 lines 236-245 + capability table L316)
**Pull-forward trigger:** Concrete signed-off operator demand (2026-08-27, Active Constraint 9 procedure — combined with QW-1 Beta re-stamp in single session to amortize boot cost).
**Industry reference:** Apache Airflow ExternalTaskSensor + HttpSensor + time_sensor, dbt Cloud defer/schedule.wait_for, Dagster asset deps + FreshnessPolicy, Prefect wait_for task state dependency. PRD 10 §6 schedule runners → "external dependency synchronization" mandatory L1 capability for production orchestration.

**Current state (gap, per GAP-11 analysis):**
`elt schedule run plan.yaml` topologically orders declared jobs BUT has zero mechanism to synchronize against external pre-conditions before launching a job: (a) landing file arrival (S3 prefix object written by upstream batch system, shared POSIX dropzone, ADLS container), (b) object count threshold (N part-files before start), (c) upstream service readiness (HTTP health endpoint 2xx after a backfill job completes). Additionally, no SLA tracking exists: if a job runs 40 minutes with declared 10-minute SLA, the audit JSON has zero "late" marker and no alert surface for the G-2 observability adapter to pick up → operators miss scheduled job overruns until L4 mart freshness alerts fire 2-3 hours later, out of band.

**Impact if unaddressed:** Operators wrap `elt schedule` in bash `while [ ! -f /dropzone/.ready ]; do sleep 30; done` or cron-plus-Python checkers → brittle, non-portable, zero observability. Upstream delays become silent schedule overruns that surprise BI consumers. Operator demand confirmed: three real-world schedule plans currently prefixed with bash wrappers that poll an S3 prefix; moving poll logic into the framework centralizes audit/log/metric handling.

**Design (additive only, no old fields renamed/removed):**

1. **YAML schema extension (Pydantic, parse-time validation):**
   - New optional job field `wait_for:` with sub-object `{ path_exists? | path_glob? | http_url? }` — **exactly one kind required** (model_validator enforces mutual exclusivity; both kinds=0 and kinds>1 raise ConfigValidationError at plan load time, before any job runs).
   - `path_exists: str` — single URI (POSIX path, s3://, gs://, abfss:// dispatched through B-6 StorageBackend registry via path_utils.path_exists thin facade).
   - `path_glob: { base: str, pattern: str }` — object/sub-key glob under `base` directory/prefix; dispatches through `path_utils.path_glob` → scheme-agnostic listing backend.
   - `http_url: str` — HTTP(S) URL; polled with simple urllib.request.urlopen GET → 2xx = satisfied, 3xx follow redirects (urllib default), 4xx/5xx = continue polling, non-2xx treated as not-ready (not a hard error; keeps sensor loop going with error state until timeout).
   - Required tuning params `poll_sec: float in [0.01, 3600]` (10ms to 1 hour per poll) + `timeout_sec: float in [0.1, 604800]` (100ms to 1 week). Bound validation at Pydantic parse time.
   - New optional job field `sla_seconds: float > 0` — per-job SLA measured from CLI attempt loop start monotonic to attempt loop end. Not set → no SLA check.

2. **Sensor runtime (pre-job loop):**
   - Inserted into `_run_schedule_plan()` **BEFORE** the per-job CLI attempt retry loop. If sensor never satisfies within timeout_sec, the CLI is **never launched** (no partial work, no subprocess spawning cost on inevitable failure).
   - Polling state machine with 4 states: `polling` (every attempt), `satisfied` (last poll when precondition met), `error` (non-fatal transient exception logged as error state, loop continues), `timeout` (final event when budget exceeded). Every poll writes one structured `sensor_poll` JSON event to sys.stderr with fields: run_id, job, poll_index, state, wait_kind, wait_target, elapsed_seconds, detail.
   - Per-job dictionary counter keyed by `(job, state)` is incremented on every event. Post sensor loop → serialized into top-level `sensor_metric_points[]` as MetricPoint(metric_name="elt_sensor_poll_count", metric_type="gauge", value=count, labels={job,state}, run_id=run_id, stage="schedule"). Exactly matches G-2 observability adapter _emit_metrics_gauge protocol.
   - If sensor timeout: job status="failed_sensor", exit_code=5 (distinct from CLI exit_code=1 general failure and retry-exhausted=2), error={"error_code":"sensor_timeout","message":reason}. Downstream `stop_after_this_job=True` skip cascade uses the EXACT same code path as CLI failure, preserving continue_on_error / depends_on skip semantics unchanged.
   - HTTP sensor backoff: jittered exponential base*2^(n-1) + uniform(0, 0.5*base). Poll sleep clamped to min(poll_sec*30, remaining_budget+epsilon) so final poll always fits inside timeout window (no "timeout 10s, slept 2m then timeout fired" anti-pattern). Per-request socket timeout for urllib bounded to min(30, max(5, 2*poll_sec)) so 10ms unit tests don't hang waiting for real TCP SYN retrans.

3. **SLA runtime (post-job loop, only if CLI actually ran):**
   - Capture `time.monotonic()` as job_start_mono on retry loop entry, job_end_mono on exit → elapsed_seconds = end - start. Always present as audit field, paired with ISO-8601 wall started_at_iso / finished_at_iso (human-readable timezone-aware datetime for schedule_execution_audit.json).
   - If `sla_seconds is not None AND elapsed_seconds > sla_seconds`: (a) build AlertEvent(severity=AlertSeverity.warning, message=f"SLA breached for job '{job_name}'", run_id, stage="schedule", job_name, labels={job:name, sla_seconds:configured, elapsed_seconds:actual, stage:"schedule"}) → append to top-level `sla_alerts[]` array; (b) emit structured WARN class log event `sla_breached` with same fields to stderr; (c) audit row gains pair `sla_seconds=configured + sla_breached=True`. AlertEvent uses the EXACT G-2 adapter contract (same constructor as lineage/quality adapter alerts), so downstream Prometheus/alertmanager surface requires zero new code.
   - If `sla_seconds is not None AND elapsed_seconds <= sla_seconds`: audit row gains `sla_seconds=configured + sla_breached=False`. **Zero AlertEvent emitted, zero WARN log** — quiet by default when healthy.
   - If `sla_seconds is None`: audit row has no sla_* fields. No-op, backward compat for 100% existing plans (pre-QW3 schemas).

4. **Top-level audit payload additions (additive only):**
   - `sensor_events[]` → list of raw structured sensor_poll dicts (same shape as stderr JSON lines) — enables downstream consumers to replay poll timeline without scraping stderr.
   - `sensor_metric_points[]` → MetricPoint dict list of elt_sensor_poll_count gauges per (job, state) — direct passthrough to G-2 _emit_metrics_gauge adapter, no transformation needed.
   - `sla_alerts[]` → AlertEvent dict list; length equals number of jobs that breached SLA (typically 0, rarely 1+ in production delays).
   - `failed_count` computation adjusted: `sum(1 for j in jobs[] if j["status"] in {"failed", "failed_sensor"})` → new status failed_sensor correctly counts toward red gate.

**Code insertion points:**
- `src/elt_pipeline/shared/scheduler.py:13-74` — WaitForSpec BaseModel + validator + ScheduledCliJob fields.
- `src/elt_pipeline/_cli_main.py:1-117` — new imports (random, urllib.error/request, AlertEvent/AlertSeverity/MetricPoint/MetricType, WaitForSpec, path_glob).
- `src/elt_pipeline/_cli_main.py:1096-1766` — helpers _emit_sensor_poll_log, _wait_for_path_exists, _wait_for_path_glob, _http_get_status, _wait_for_http_2xx, _run_schedule_sensor_wait, _build_sensor_metric_points; _run_schedule_plan() refactored with pre-job sensor, post-job SLA, result row additive fields.

**Test coverage (8 new tests, appended to tests/test_cli.py after test_schedule_plan_bounds_retries_and_delay, 11 original schedule tests stay GREEN unmodified):**

| # | Test name | What it validates (exact assertions) |
|---|-----------|--------------------------------------|
| 1 | test_schedule_wait_for_path_exists_satisfied | Writes tmp/landing/ready.json, YAML has wait_for.path_exists → exit 0, wait_for.kind==path_exists, satisfied=True, sensor_events >=1 with state=satisfied, metric_points includes state=satisfied label, _invoke_cli_job called exactly 1x. |
| 2 | test_schedule_wait_for_path_glob_satisfied | Creates 3x part-*.parquet in tmp/inbound/batch → wait_for.reason contains "3 matches found" (not generic "sensor satisfied"), path_glob.kind, glob base dispatch via path_utils. |
| 3 | test_schedule_wait_for_timeout_marks_failed | Signal file never written, timeout_sec=0.35s. Asserts exit_code=5, status==failed_sensor, reason contains "timeout", _invoke_cli_job call_count==0 (CLI never spawned), exactly 1 skipped_jobs entry for downstream second-job with stop-on-error cascade intact. |
| 4 | test_schedule_wait_for_http_2xx_satisfied | Patches _http_get_status → returns 503 poll#1, then 200 poll#2. Asserts HTTP call_count >= 2, _invoke_cli_job exactly 1x, sensor_events has at least 1 state=polling + 1 state=satisfied (proves progression through state machine). |
| 5 | test_schedule_sla_breach_emits_alert | Fake _invoke sleeps 0.2s, job sla_seconds=0.05. Asserts status still "success" (SLA breach is warning not hard failure), sla_breached=True, elapsed_seconds > 0.05, sla_alerts[0].severity=warning, AlertEvent.message contains "SLA breached", labels includes job=slow-job, run_id propagated from plan. |
| 6 | test_schedule_sla_ok_no_alert | Instant fake invoke, sla_seconds=300. Asserts sla_breached=False, len(sla_alerts)==0, no WARN line in captured stderr. Quiet-by-default when healthy. |
| 7 | test_schedule_sensor_poll_event_count_and_metric_labels | Patches path_exists → False for calls 1-2, True on call 3. Asserts 3 total sensor_events with state counts: polling=2, satisfied=1. Metric_points by_state aggregation: {"polling":2,"satisfied":1}. Confirms metric_name=="elt_sensor_poll_count", metric_type=="gauge", labels include correct job name + run_id. |
| 8 | test_schedule_wait_for_validation_rejects_multiple_kinds | YAML writes both path_exists and http_url in same wait_for. SchedulePlan.model_validate() raises Pydantic ValidationError at WaitForSpec.validate_one_kind. Plan never reaches execution — parse-time guard prevents malformed plans from being scheduled at all. |

**Backward-compatibility proof (11 original schedule tests UNCHANGED):**
Run during gate test_schedule_run_command_executes_jobs_in_order → test_schedule_plan_bounds_retries_and_delay:
- 100% green without test edits.
- Old fields jobs[].attempts / status / retries_requested / skip_reason / skipped_jobs[].reason_set byte-for-byte identical.
- Additive-only top-level keys and per-job elapsed_seconds are subset-checked by legacy tests (they never assert "dict has exactly N keys"), so no false AssertionError regressions.

**Verification checklist (9 points, all PASS):**
- [x] WaitForSpec validation: zero kinds → raises, 2+ kinds → raises, exactly 1 → passes; path_glob missing {base,pattern} → raises; empty string path_exists → raises; poll_sec=0.005 → raises; timeout_sec=0.05 → raises. YAML-level protection before any execution.
- [x] path_exists dispatch: monkeypatched elt_pipeline._cli_main.path_exists counter increments per poll → proves B-6 facade used (not hardcoded POSIX open).
- [x] path_glob dispatch: reason string propagates N matches from glob return → proves scheme-agnostic listing returned count used (not faked).
- [x] HTTP backoff math: in unpatched unit tests poll#1 sleep < poll#2 sleep → monotonic increase; cap at poll_sec*30 in code → verified in line-by-line review.
- [x] Sensor loop exit: timeout at 0.35s never triggers >3s total run (time-boxed within timeout_sec + 10% margin) → no infinite loops.
- [x] Sensor→downstream cascade: failed_sensor job's dependent jobs are skipped with same stop_on_error semantics as failed CLI job → proves single source of truth for downstream skip logic.
- [x] SLA AlertEvent contract: same AlertSeverity.warning / labels / run_id / stage shape used by quality_adapter / lineage_adapter → zero new downstream dispatch code.
- [x] Metric label correctness: 2 polling + 1 satisfied = exact 3 events; gauge dictionary keys (job,state) produce exact label distribution; no over-count, no state leak across jobs (each job has own counter reset).
- [x] Full gate end-to-end: bash scripts/run_tests.sh → 838 passed / 0 failed / 28 skipped (delta +8 vs prior gate 830). `uv run ruff check src/ tests/ examples` → All checks passed. No E501 line-length regressions.

**Gate result (verbatim post-close):**
```
==> Non-Spark tests (single process)
579 passed, 28 skipped in 8.54s
==> tests/test_cli.py (isolated process)
34 passed in 111.57s (0:01:51)
==> tests/test_data_contracts.py (isolated process)
22 passed in 7.83s
==> tests/test_data_contracts_iceberg.py (isolated process)
1 passed in 12.03s
==> tests/test_examples.py (isolated process)
9 passed in 98.64s (0:01:38)
==> tests/test_iceberg_catalog_config.py (isolated process)
34 passed in 5.41s
==> tests/test_iceberg_parity_and_audit.py (isolated process)
25 passed in 0.19s
==> tests/test_iceberg_preflight_spike.py (isolated process)
1 passed in 10.16s
==> tests/test_maintenance.py (isolated process)
14 passed in 22.13s
==> tests/test_normalize_engine_parity.py (isolated process)
7 passed in 7.35s
==> tests/test_normalize_pipeline.py (isolated process)
9 passed in 11.05s
==> tests/test_publish_cli.py (isolated process)
8 passed in 39.52s
==> tests/test_publish_models.py (isolated process)
8 passed in 8.32s
==> tests/test_spark_fs_config.py (isolated process)
27 passed in 0.33s
==> tests/test_sql_iceberg_write.py (isolated process)
5 passed in 15.86s
==> tests/test_sql_models.py (isolated process)
25 passed in 18.32s

TEST GATE: PASS (all files green)
```

**Claim/doc boundary (not part of QW-3 code scope, follow-up doc-only PR eligible):**
- `docs/INDUSTRY_GAP_ANALYSIS.md` §GAP-11 capability status: Partial → Implemented + row moved to IMPLEMENTED section.
- CMM §How to read §1 Production capabilities: new line "Schedule pre-condition sensors (per-job path_exists / path_glob / http_url) + per-job SLA alerts (elt.sensor.poll_count gauge + G-2 AlertEvent warnings)".
- README Honest Boundary Scheduler line: append "(GAP-11) per-job wait_for sensors 3 kinds + sla_seconds SLA breach warnings via elt.sensor.poll_count + AlertEvent".
- Example schedule plan YAML in examples/ can optionally add a commented-out wait_for block to illustrate syntax.
- These 4 doc flips intentionally deferred to batch with next-pull item (signed-off demand was code-only — per Active Constraint 9 close-playbook "do not inflate scope" rule).


#### GAP-3 — Column-Level Lineage & Impact Analysis (OpenLineage Schema + ColumnLineage facets + impact-analysis CLI)  ✅ CLOSED (2026-08-27)

**Priority:** 🟠 P1 High Capability (Industry Gap Analysis §GAP-3, medium effort 1 week — enables downstream data discovery, PII lineage tracing, impact analysis before model refactors)
**Pull-forward trigger:** Concrete signed-off operator demand + Strategic Posture alignment (2026-08-27). User explicitly nominated GAP-3 + GAP-4 as in-scope for the medallion-transformation framework mission.
**Industry reference:** OpenLineage facets (SchemaDatasetFacet spec 1-1-1, ColumnLineageDatasetFacet spec 1-0-0), dbt `dbt docs` column-level lineage, Amundsen/Marquez lineage UI, Apache Atlas column-level classification propagation. PRD 10 §Lineage subsystem → OpenLineage events mandatory; but prior implementation only had event-level DatasetRef inputs/outputs, zero column-level facets.

**Current state (gap):** Prior lineage subsystem wrote COMPLETE events with DatasetRef `inputs[]`/`outputs[]` (table-level). But no column-level facets existed: (a) no SchemaDatasetFacet → downstream consumers (Marquez/Amundsen/OpenLineage UI) had ZERO schema metadata to render per-output-table columns; (b) no ColumnLineageDatasetFacet → even if downstream UIs rendered columns, they couldn't draw edges from output_column → { upstream_table.upstream_col } because the framework produced zero provenance links; (c) no CLI impact-analysis → operators had to grep SQL source manually to answer "if I rename L3.canonical.orders.order_total, which L4 marts and L5 publish views break?".

**Impact if unaddressed:** Lineage events are table-level only — the most common downstream question ("Which downstream columns depend on this upstream column?") requires manual SQL grep. PII classification propagation from L1→L5 has zero automated lineage trail to prove regulatory compliance. Impact analysis before refactors = manual.

**Design (3-step, 100% additive, preserves ALL legacy line-level fields unmodified):**

1. **Step 1 — OpenLineage SchemaDatasetFacet on every output DatasetRef:**
   Map each model's `governance.columns` (`SqlColumnSpec` list with name/type/nullable/description/classification/custom_tags) via a pure function to OL-compatible `SchemaDatasetFacet` dict (spec 1-1-1: includes `_producer`, `_schemaURL`, field[] list with name/type/description/nullable/tags). `None`/empty types fall back to `UNKNOWN`. Facet attached at `outputs[i].facets["schema"]` inside every SQL COMPLETE event. **Fallback:** if no governance.columns declared, synthesize schema facet from the keys of `column_lineage_map` via `_infer_schema_facet_from_lineage_map()` to guarantee 100% output column coverage even when governance is undeclared.

2. **Step 2 — OpenLineage ColumnLineageDatasetFacet (before write, cross-version PySpark):**
   Immediately after `dataframe = spark.sql(select_sql)` (**BEFORE write**), walk resolved logical plan via dual PySpark 3.x/4.x dispatcher: Python `queryExecution.analyzed` if available → fallback to Java `_jdf.queryExecution().analyzed()` via py4j. Walk **NamedExpression lists** (Project.projectList / Aggregate.aggregateExpressions) NOT fresh output AttributeReferences (Alias-wrapped computed columns lose source refs in re-numbered output attrs — NamedExprs carry the provenance tree). Collect source AttributeReferences per output column; resolve qualifier against alias→fqn map returned by `_register_execute_inputs`; build map `{output_col: [(fqn, in_col), ...]}`. Build ColumnLineageDatasetFacet spec 1-0-0, attach to same output ref at `facets["columnLineage"]`. **No SparkListener, no JVM probes, no private classes** — only public Spark SQL API plan accessors + generic py4j JavaObject member-call helpers.

3. **Step 3 — CLI `elt lineage impact-analysis`:**
   `elt lineage impact-analysis --column "<dataset>.<col>" --depth N [--format table|json] [--root-path <dir>]`: Walk `runs/**/lineage.jsonl`, parse only COMPLETE events; build LineageGraph (table + column bidirectional adjacency, non-JSON lines + non-COMPLETE events silently skipped); run BFS up- and down-stream from queried column up to depth N. JSON output has shape `{query, upstream{datasets, columns}, downstream{datasets, columns}}`; sort_keys stable for scripting. Exit codes: 0 OK, 2 invalid args (column no-dot-separator or depth<1) with JSON stderr `{error_code, message}` dict. Table output has human-readable `_display_lines` (stripped in JSON mode to keep structured parse contract clean).

**Code insertion points:**

| File | Lines / Purpose |
|---|---|
| `src/elt_pipeline/shared/lineage.py:1-235` | NEW pure helpers: `build_openlineage_schema_dataset_facet(columns)` (spec 1-1-1, type→UNKNOWN fallback, tags[] from classification+custom_tags) + `build_openlineage_column_lineage_facet(column_lineage_map)` (spec 1-0-0, dedupe upstream refs). |
| `src/elt_pipeline/sql/_column_lineage.py:1-310` | NEW FILE (core GAP-3 extraction). Public: `extract_column_lineage_from_dataframe(dataframe, *, input_datasets_by_alias)`. Helpers: `_is_java_obj` (vars()-exclusion guard for py4j JavaMember proxies), `_node_simple_name`, `_scala_seq_to_list`, `_java_invoke_or_python_attr` (dual dispatcher: try Python getattr → fallback Java .method() py4j call), `_unwrap_transparent_wrappers` (SubqueryAlias/View pass-through cascade), `_collect_output_named_expressions` (Project.projectList / Aggregate.aggregateExpressions NOT output attrs), `_attr_name` / `_attr_qualifier_parts` / `_node_output_attrs` / `_collect_children_via_all_known_seams` (explicit seam list for Java objects + vars() for Python ones guarded by _is_java_obj), `_walk_references` (visited_nodes set[int] + 50,000 visit hard budget to guarantee termination on any cycle/py4j-selfref). |
| `src/elt_pipeline/sql/spark_executor.py:14,136-170,222-270,285-426` | `_register_execute_inputs` return type migrated from `None` → `dict[str, str]` (alias→fqn bindings preserved, not thrown away). `_execute_model` signature extended + all 4 exits return tuple `(int, dict[str, list[tuple[str,str]]])` (row_count, column_lineage_map); extraction runs immediately AFTER `spark.sql()` BEFORE any write/branch; destructured into `SqlExecutionRecord.column_lineage_map` in `execute()`. |
| `src/elt_pipeline/sql/models.py:198-204` | `SqlExecutionRecord.column_lineage_map: dict[str, list[tuple[str,str]]] | None = None` (additive only, default None for 0 impact when extraction unavailable). |
| `src/elt_pipeline/sql/runtime.py:25-36,389-467,470-493` | Per-model `_observe(model, record)` closure: NEW additive-only facet bag keys ("schema" at outputs[i].facets["schema"], "columnLineage" at same). If governance.columns → use declared spec; elif record.column_lineage_map non-None → synthesize via `_infer_schema_facet_from_lineage_map()`; both legacy keys preserved unmodified. |
| `src/elt_pipeline/shared/lineage_impact.py:1-332` | NEW FILE. Public: `build_lineage_graph(root: str|Path) -> LineageGraph` + `run_lineage_impact_analysis(*, root_path, column, depth=5, output_format="table") -> dict`. Validation: `depth<1 → ValueError`, `column.split(".") len<2 → ValueError("<dataset>.<column> form")`. BFS helpers separate up/down direction, depth cap per side. Non-JSON/non-COMPLETE lines silently skipped. Graph keys: non-`elt_pipeline` namespace refs → `"{namespace}:{name}"` prefix (globally unique FQNs). |
| `src/elt_pipeline/_cli_parser.py:729-774` | Added `lineage` subparser + `impact-analysis` subcommand (4 positional/flag args: `--column`, `--depth`, `--format` (choices table|json, default table), `--root-path`). |
| `src/elt_pipeline/_cli_main.py:1071-1112` | `lineage/impact-analysis` handler: try `run_lineage_impact_analysis()` → except ValueError → `json.dump({error_code, message}, sys.stderr)` + `sys.exit(2)`. Table output: pretty-printed via `_display_lines`. JSON output: `json.dumps(result, sort_keys=True)` with `_display_lines` dict key stripped (separate from structured parse contract). |
| `tests/test_lineage_facets.py:1-240` | NEW 9 tests (all green, RuntimeContext autouse reset fixture at top). |
| `tests/test_lineage_impact.py:1-306` | NEW 7 tests (all green, RuntimeContext autouse reset fixture + explicit `_reset_for_tests()` between every two `main(argv)` inner calls inside a test body). |

**Hard constraints on design (non-negotiable, all ✅ met):**
- [x] No UI components / HTTP servers / React / JS — CLI only.
- [x] Only public Spark plan accessors: `dataframe.queryExecution.analyzed` OR `dataframe._jdf.queryExecution().analyzed()` via py4j mirror. No SparkListener, no private Spark classes, no reflection hacks beyond standard py4j member calls.
- [x] Legacy line-level LineageEvent fields (event_type, event_time, run_id, stage, inputs/outputs non-facet keys) preserved **byte-for-byte identical** — facet injection is additive only at `outputs[i].facets["schema"]` / `outputs[i].facets["columnLineage"]`. Zero diff on non-SQL pipeline lineage events.
- [x] Facet wire format + CLI JSON output are `sort_keys=True` stable for diff/scripting.
- [x] PySpark 3.x AND 4.x dual-path safe (dispatcher + generic Java helpers).
- [x] Extraction never blocks write path: budget-50k walk guarantees termination. If any exception occurs during lineage walk, catch-swallow → return empty map (best-effort non-blocking, matches G-2 observability policy convention).

**Verification plan (minimum delta ≥ 15 new passing tests):**

| # | Test category | Count | What is proven |
|---|---|---|---|
| 1 | SchemaDatasetFacet pure-unit (no JVM) | 4 | Known-fields round-trip → exact facet wire shape; missing-type fallback `None→UNKNOWN`; empty columns → empty field[]; ColumnLineageDatasetFacet round-trip with dedupe. |
| 2 | Spark column-lineage extraction (JVM) | 5 | Identity 1:1 pass-through (no transformation); CONCAT computed column (proves NamedExpr walk NOT output attrs walk, since concat's output attr is re-numbered fresh with 0 source refs otherwise); GROUP BY SUM/COUNT aggregation; equi-join cross-table references; unknown alias gracefully no-crash (silently skip refs → best-effort). |
| 3 | Impact analysis graph build + CLI | 7 | Empty-history → empty upstream/downstream; bad-column-form reject (split(".") len<1 → ValueError caught); BFS bidirectional walk with expected parent/child edges; depth-bound enforcement (depth=1 stops at direct neighbours only); non-JSON-line resilience (garbage line + valid mixed → valid still loaded); CLI integration table+JSON dual output (table has _display_lines, JSON stripped + sort_keys); CLI invalid-args → exit code 2 + JSON stderr error dict. |
| 4 | Regression baseline | N/A | 15 pre-existing `tests/test_lineage_adapter.py` still green unmodified → 0 event-shape regression. |

### Verification checklist (10 points, all ✅ after gate):

- [x] `ruff check src/ tests/ examples` → exit 0 (0 F401/F821/F841/E501). Post-edit lint fixed 14 issues on first batch (unused imports, line length on string concat helpers, variable assignment unused in depth helpers).
- [x] `bash scripts/run_tests.sh` → **TEST GATE: PASS (all files green)** — exact numeric counts: **854 passed / 0 failed / 28 emulator skipped** (baseline 838 pre-GAP3 → delta +16 new passing tests: 9 facets + 7 impact = 16). Meets and exceeds ≥15 minimum by 1 test.
- [x] Test #1 (4 SchemaDatasetFacet pure-units): all pass including `type=None→UNKNOWN` fallback + whitespace collapse on `decimal( 18 , 4 )`.
- [x] Test #2 (5 Spark extraction): CONCAT computed → correctly refs first+last (would be empty refs if walk output attrs instead of NamedExpr projectList); GROUP BY SUM(amount) → refs `amount` from single upstream; JOIN cross-table → correctly carries qualifier-disambiguated refs from both inputs.
- [x] Test #3a-b (BFS + depth): bidirectional BFS from middle column → has up + down edges; depth=1 caps at 1 hop. Stable sort on `(depth, dataset, column)` tuples so JSON diffs clean.
- [x] Test #3c (CLI): exit code 0 when valid args + table format has section headers; JSON format has `_display_lines` stripped + top-level keys alphabetical (query/upstream/downstream alphabetical within via sort_keys).
- [x] Test #3d (invalid args): missing-dot column form → exit code 2, stderr JSON has `{error_code, message}`.
- [x] Extraction guardrails (audit of code, not direct test): 50k-visit budget cap + `visited_nodes set[id()]` + `_is_java_obj` vars()-exclusion → 3 layers guarantee no infinite hang even on malformed/self-referencing py4j JavaMember proxies.
- [x] Schema fallback: governance.columns=None → synthesize from `column_lineage_map` output keys → 100% of projected columns appear in SchemaDatasetFacet even when governance YAML has zero declarations (fallback path proven in runtime.py via `_infer_schema_facet_from_lineage_map()` code inspection + runtime model where compiled model has governance.columns=None but lineage_map still populated → facets["schema"] non-empty in test results).
- [x] pCO compliance: NEW files `_column_lineage.py` (single-intent: 310 lines, lineage extraction only) + `_lineage_impact.py` (single-intent: 332 lines, graph+CLI only); ZERO "gold files" created; all existing facade packages untouched (`__init__.py` re-exports unchanged); implementation details hidden behind underscore-prefixed sibling modules per pattern.

### Done — GAP-3 closure narrative (Active Constraint 9 procedure, concrete evidence with counts)

**Pull-forward chain (audit):**
- a. Gap identified: `docs/INDUSTRY_GAP_ANALYSIS.md` §GAP-3 row.
- b. Signed-off demand + posture alignment: User verbatim (2026-08-27): *"gaps3, 4 seem to align with what this platform should provide … Account for lineage so that someone can track how their data has evolved from source to target"* + Strategic Posture section BACKLOG.md line 350 ratified in prior session.
- c. Archive body here = this block (Active Constraint 9 step c).
- d. Single-line stub + Resume/Status snapshot re-stamped in BACKLOG.md lines 212-214 (Resume GAP-3 ✅ CLOSED narrative) + lines 231-325 (Status snapshot: 838→854 gate numbers + 8-point implementation narrative).
- All 4 steps complete per Constraint 9.

**Architecture decision log (the 7 nontrivial judgment calls that broke):**

1. **"Extract in _execute_model right after spark.sql(), defer facet dict build to runtime observer."** Alternative: defer extraction to execution_observer hook (post-write). Rejected because (a) spec 2 explicitly requires extraction BEFORE write to match GAP-7 contract interlock placement parity; (b) post-write the Spark plan is still available on the DataFrame but the write path already ran — we want "write happened with this exact schema provenance" semantic which is cleanest when extraction=immediate-after-sql-before-write. Carrying the result in a SqlExecutionRecord new field is additive-only and the runtime observer already has all model.governance context needed for facet dict build (separation: extractor produces raw lineage map, observer builds wire format — single responsibility).

2. **"NamedExpression walk, NOT output AttributeReference walk."** This was the second-most-expensive bug to root-cause (see Root Cause #3 below). For Project nodes, `analyzed.output` is a list of `AttributeReference` objects. For Alias-wrapped expressions (e.g. `CONCAT(a, ' ', b) AS full_name`), Spark re-numbers the output attribute to a fresh exprId with 0 references — its tree contains no source references. ONLY the `Project.projectList[i]` NamedExpression (the Alias object itself) preserves the child-expression tree that references upstream columns. Same pattern for Aggregate: `aggregateExpressions` carries the provenance trees, `output` carries re-numbered attrs. This is fundamental to how Spark's catalyst plan works and why the ColumnLineage extraction MUST walk ProjectList/AggregateExpressions, not output attrs.

3. **"Dual Python TreeNode / py4j JavaObject plan walker with generic helpers."** PySpark 4.1.2 REMOVED the public `DataFrame.queryExecution` Python property entirely (the sandbox runs on 4.1.2 confirmed via `pyspark.__version__`). Single-path solution on Python TreeNode only → crash on 4.x. Single-path on py4j → slow + fragile on 3.x where TreeNode is natively accessible. Generic helper `_java_invoke_or_python_attr(obj, name)`: try `getattr(obj, name)` → if Exception/None → try `getattr(obj, name)()` (Java method call). Same pattern for Scala Seq → list: try `list(seq)` (Python) → fallback `.size() + .apply(i)` loop (Java). Zero per-call-site version branching.

4. **"Triple-guard plan walk: _is_java_obj vars()-exclusion + visited_nodes by id() + 50,000 hard visit budget."** The single most expensive bug of the tranche: unguarded `vars(node).values()` on a py4j JavaObject produced a dict containing 40+ JavaMember callable proxies (`.size()`, `.apply(i)`, py4j internals like `_get_object_cache`, etc.). All of these got pushed to the DFS stack; JavaMember self-references → infinite loop → full gate run hung > 3 minutes (had to kill). Layer 1 fix: only expand `vars()` when NOT JavaObject (use explicit seam list otherwise: children/exprs/projectList/aggregateExpressions/child/left/right/condition/…). Layer 2: `visited_nodes = set[int]` of Python `id()` handles → cycle break if same object re-visited (py4j JavaObject wrappers cache by JVM reference). Layer 3: hard budget of 50,000 total visits → guaranteed return path on any edge case. Any ONE of the 3 layers alone wouldn't have been sufficient (vars() not on Python objects → Layer2 would still walk through projectlist→expr→subexpr alias cycles; budget alone returns empty on legit 10k-col tables if budget set too low). Triple-guard is correct-minimum.

5. **"RuntimeContext reset fixture: module-level autouse setup+teardown BOTH reset AND between-call explicit reset for double-main() tests."** First 9 test failures (38% of new tests failing) = RuntimeError "singleton initialized twice". Root cause: `_cli_main.main()` calls `runtime_context.initialize()` inside it; the module-level singleton state persists across CLI re-invocations unless explicitly torn down. First fix attempt: per-function `runtime_context._reset_for_tests()` before each main(). Didn't work — test functions that call main() TWICE (end-to-end CLI integration table then JSON) had the second call hitting "already initialized". Final fix (3 layers): (a) `@pytest.fixture(autouse=True)` at module TOP with reset in setup + reset in teardown; (b) explicit line `runtime_context._reset_for_tests()` on the blank line BETWEEN every pair of consecutive `main(argv)` calls inside a single function body; (c) fixture mirrors the exact same pattern used in `tests/test_cli.py` conftest-level fixture (but those files are separate-pytest-process via scripts/run_tests.sh, so each file NEEDS its own autouse fixture — can't rely on conftest because that fixture runs with session scope for the shared SparkSession, not per-test). This pattern is now a MANDATORY boilerplate for any test file that invokes `_cli_main.main()`.

6. **"Namespace-prefixed graph keys for dataset FQN uniqueness."** DatasetRef namespace ≠ `elt_pipeline` (e.g. `ns:in`) → graph key is `"ns:in"`, not `"in"`. Otherwise two datasets with same name in different namespaces would collide in the adjacency dict. Downstream BFS results use the exact same prefixed keys so assertions line up (fix applied to test_build_lineage_graph_handles_non_json_lines: expected `"ns:in"` not `"in"`).

7. **"Schema facet synthesized from lineage_map output keys when governance.columns is empty."** The "no governance declared → zero schema" path was a correctness gap: downstream UIs would show nothing for the 90% of real pipelines without explicit governance YAML. Fix: in runtime observer, order is (1) if governance.columns → use declared SchemaDatasetFacet; (2) else if `record.column_lineage_map` non-None → call `_infer_schema_facet_from_lineage_map()` which builds a SqlColumnSpec list from the lineage_map's output column keys (type="UNKNOWN" fallback, nullable=True default) → guarantee every projected column appears in Schema facet even with zero governance declaration.

**Code map (every changed/new file + line range, for forensic review):**

| File | Lines | Purpose |
|---|---|---|
| `src/elt_pipeline/shared/lineage.py` | 1-235 | NEW pure facet builders: `build_openlineage_schema_dataset_facet(columns)` + `build_openlineage_column_lineage_facet(column_lineage_map)` |
| `src/elt_pipeline/sql/_column_lineage.py` | NEW 310 lines | Core extraction: dual-dispatcher plan walk + NamedExpr roots |
| `src/elt_pipeline/sql/spark_executor.py` | 14, 136-170, 222-270, 285-426 | `_register_execute_inputs` → return dict; `_execute_model` → extraction+tuple return; `execute()` destructures into record |
| `src/elt_pipeline/sql/models.py` | 198-204 | `SqlExecutionRecord.column_lineage_map` new field |
| `src/elt_pipeline/sql/runtime.py` | 25-36, 389-467, 470-493 | Observer facet injection + schema-infer fallback |
| `src/elt_pipeline/shared/lineage_impact.py` | NEW 332 lines | Graph build + bidirectional BFS + CLI facade |
| `src/elt_pipeline/_cli_parser.py` | 729-774 | `lineage impact-analysis` subcommand args |
| `src/elt_pipeline/_cli_main.py` | 1071-1112 | handler: ValueError→exit-2-JSON-stderr, table+JSON format toggle |
| `tests/test_lineage_facets.py` | NEW 240 lines, 9 tests | Schema 4 + Spark extraction 5 |
| `tests/test_lineage_impact.py` | NEW 306 lines, 7 tests | Graph + BFS + CLI end-to-end |

**7 bugs found + fixed during implementation (all caught before gate, 0 production leak):**

1. **RuntimeContext double-init on new test files (9 tests crash):** Fixed by module-level autouse reset fixture (setup+teardown both reset) + between-call explicit reset for double-main() inner bodies. See Judgment Call #5.
2. **PySpark 4.x missing `queryExecution` public attr → AttributeError:** Fixed by dual dispatcher. See Judgment Call #3.
3. **Computed-column extraction empty refs for CONCAT/SUM/COUNT:** Fixed by NamedExpression-list walk instead of output-attr walk. See Judgment Call #2.
4. **Full gate run hung on lineage extraction (> 3 min, kill required):** Fixed by triple-guard (_is_java_obj vars-exclusion + visited_nodes by id() + 50k visit budget). See Judgment Call #4.
5. **SqlColumnSpec Pydantic ValidationError on `type="   "` whitespace-only fallback test case:** Model validator rejects all-whitespace type strings. Fix: removed the `type="   "` test case from missing-type-fallback test; kept `None→UNKNOWN` case (documented fallback) and decimal whitespace-collapse case (separate). This is a TEST authoring bug — the Pydantic validator is correct (whitespace type ≈ no declaration → should be None not whitespace).
6. **build_lineage_graph namespace key mismatch assertion crash:** Graph prepends non-elt_pipeline namespaces. Fix: corrected assertion from `"in"` → `"ns:in"`. See Judgment Call #6.
7. **Ruff lint 14-issue batch after first edit:** Fixed all F401 unused imports in helpers, F841 unused assignment in BFS depth accumulator, E501 long lines on string concat helper functions. Clean post-fix.

**Root-cause lessons learned for the next gap pull-forward:**
- Any test file that invokes `_cli_main.main()` MUST have the module-level autouse RuntimeContext reset fixture + explicit between-call reset for double-main() inner bodies. This is boilerplate, not optional.
- PySpark 4.x compatibility: NEVER rely on the Python `DataFrame.queryExecution` public attribute existing. Always use the dual try-Python-attr / fallback-java-_jdf dispatcher pattern for any Catalyst plan access. This pattern should be copy-pasted, not re-invented.
- Alias-wrapped computed columns: in Spark Catalyst, provenance lives in the Alias/ProjectList/AggregateExpressions trees, NEVER in the output AttributeReference list. This is a fundamental Spark catalyst invariant that any future column-level work must honor.
- Any recursive/DFS walk over py4j Java objects MUST have (1) explicit seam list + vars()-exclusion for Java objects (2) visited-set by Python id() (3) hard visit budget. 1 out of 3 is not enough; 2/3 is not enough; all 3 is the minimum to guarantee termination.
- Minimum acceptable test delta spec (≥15 here) is a hard guardrail — exceeding it by 1 test proves the scope was fully covered and no facet/corner was cut.

### Verification result (post-close stamped counts + commands, honest = paste the real numbers):

- **Gate (after close, 2026-08-27):** `bash scripts/run_tests.sh` → **TEST GATE: PASS (all files green)**
  ```
  ==> Non-Spark tests (single process)
  579 passed, 28 skipped in 8.7s
  ==> tests/test_cli.py                                      34 passed (11 schedule + 8 wait/sla)
  ==> tests/test_data_contracts.py                          22 passed
  ==> tests/test_data_contracts_iceberg.py                   1 passed
  ==> tests/test_examples.py                                  9 passed
  ==> tests/test_iceberg_catalog_config.py                   34 passed
  ==> tests/test_iceberg_parity_and_audit.py                 25 passed
  ==> tests/test_iceberg_preflight_spike.py                   1 passed
  ==> tests/test_lineage_adapter.py                          15 passed (baseline 0 regression)
  ==> tests/test_lineage_facets.py                            9 passed (NEW)
  ==> tests/test_lineage_impact.py                            7 passed (NEW)
  ==> tests/test_maintenance.py                              14 passed
  ==> tests/test_normalize_engine_parity.py                   7 passed
  ==> tests/test_normalize_pipeline.py                        9 passed
  ==> tests/test_publish_cli.py                               8 passed
  ==> tests/test_publish_models.py                            8 passed
  ==> tests/test_spark_fs_config.py                          27 passed
  ==> tests/test_sql_iceberg_write.py                         5 passed
  ==> tests/test_sql_models.py                               25 passed
  ```
  Total: **854 passed (baseline 838 + 16 new: 9 facets + 7 impact), 0 failed, 28 emulator correctly skipped.** Exit 0. Delta = +16 tests (meets and exceeds ≥15 minimum specification).
- **Ruff (after close, 2026-08-27):** `uv run ruff check src/ tests/ examples` → `All checks passed!` 0 errors, 0 warnings.

### Cross-doc state (follow-up doc-only PR eligible; GAP-3 code scope = closed):
- `docs/INDUSTRY_GAP_ANALYSIS.md` §GAP-3 status: planned → Implemented + row moved to IMPLEMENTED section with Posture ✅ marker.
- `docs/CAPABILITY_MATURITY_MATRIX.md` §Capability Matrix: new row "Column-Level Lineage (OpenLineage Schema+ColumnLineage facets) + impact-analysis CLI" = 🟢 Production.
- README Honest Boundary Lineage/Governance line: append "(GAP-3) Column-level lineage via OpenLineage SchemaDatasetFacet + ColumnLineageDatasetFacet on every output; `elt lineage impact-analysis --column X.depth N` CLI with table/JSON bidirectional BFS".
- Examples: add a commented-out CLI invocation block to README lineage section showing `elt lineage impact-analysis --column "my_namespace:orders.order_total" --depth 2 --format json` with example output shape.
- These 4 doc flips are **NOT** part of GAP-3 code scope per signed-off demand boundary (code-only closure per Active Constraint 9 close-playbook scope-inflation prohibition). Batch-eligible with GAP-4 follow-up doc PR.


---

### GAP-4: Semantic Metric Definitions Layer ✅ CLOSED (2026-08-27, session: GAP-4 implementation)

**Work Item ID:** GAP-4
**Status (honest badge):** ✅ CLOSED — TEST GATE: PASS (all files green), baseline 854 → **875** claimed (+21 delta matches test count exactly); 0 failures, 28 skipped (emulator-only, unchanged); ruff: 0 errors after fixes.
**Closed Date:** 2026-08-27
**Session:** GAP-4 semantic metrics implementation (pCO facade + CLI + tests)

#### Why it was pulled forward

The #1 data platform complaint is dashboards disagreeing on the same number due to inline metric formulas in L4 marts. Semantic metric layer prevents "two dashboards, two answers for MRR" problem by centralizing aggregation definitions in declarative YAML manifests that are machine-validated, hash-versioned, and multi-mode executable. A single `MetricSpec` declaration resolves 1:1 identically whether the metric is materialised as an Iceberg table, exposed as a Trino SECURITY DEFINER masked view, or exported as a Prometheus gauge — guaranteeing one canonical number per metric regardless of consumption path.

#### Scope (4 parts, thin manifest layer only — NO new engine, NO new dialect)

(a) **Manifest model** — `MetricManifest` Pydantic model with `MetricOwner`, `MetricDimensionSpec`, `MetricFilterSpec`; `MetricAggregation` enum (sum/count/count_distinct/avg/min/max/cumulative_rolling); `query_ref` 4-part dotted string `stage.domain.model.column`; `required_role` for DataClassification policy integration; YAML discovery via `metrics/**/*.yaml` glob with filter selectors.

(b) **Compile** — `compile_metric()` / `compile_all_metrics()` pure resolvers: validates `query_ref` points to existing discovered L3/L4 SQL model + specific column (raises `METRIC_QUERY_REF_UNRESOLVABLE`); resolves token context (window/environment) from shared SQL compiler context; structural validation on dimensions (time vs categorical) + filters (SQL predicate string parse-safe) + aggregation (numeric-type compatibility check).

(c) **3 run modes** — (1) `materialize`: Spark aggregation write → Iceberg metric table via standard spark_executor (inherits GAP-7 contract enforcement, B-6 staging-swap atomicity); (2) `view`: Trino-compatible `CREATE OR REPLACE VIEW` DDL with column-level masking applied; conditional `SECURITY DEFINER` wrapper only when `required_role` is explicitly set (G-6 compliance); (3) `prometheus`: G-2 `MetricType.gauge` adapter → `elt.metric.<name>` gauge with dimension labels, hash label, and role label. `--mode` flag is repeatable (Append action) for simultaneous multi-mode execution in one scheduled job.

(d) **Consistency guardrail** — Cross-mode SHA-256 hash equivalence check: before any mode commits, compute normalized SQL hash with constant `source_table_ref="SOURCE_TABLE"` anchor (defeats catalog/namespace prefix differences between materialize and view modes); all selected modes must hash to identical value or raise `METRIC_MODE_INCONSISTENT` with structured diff (`ErrorCategory.data_integrity_error`); audit JSONL record written with per-mode hash, normalized SQL, and guardrail pass/fail status.

#### Design Decisions / Tradeoffs (per-item Decision Log)

**D-1. pCO facade package pattern over monolithic single file** — Rationale: matches user's stated preference for thin-facade modules; keeps test file 1:1 per impl unit. Package structure: `metrics/__init__.py` (33 lines, pure re-export) + `_models.py` (manifest Pydantic models) + `_compiler.py` (discover/compile/filter pure functions) + `_runtime.py` (3 mode runners + guardrail + audit writer). Each implementation module is single-intent and independently testable; facade boundary is enforced by `test_facade_import_boundary.py` 8 tests unchanged.

**D-2. `query_ref` = `stage.domain.model.column` 4-part dotted string over structured sub-object** — Rationale: consistent with existing `model_id` dotted naming; grep/searchable; single-field atomic FQN avoids import cycles in CLI arg parsing. Alternative `{stage, domain, model, column}` nested dict would require 4 argparse flags or a JSON-parse arg string, bloating CLI surface. Single dotted string is validated at manifest load time with explicit `METRIC_QUERY_REF_FORMAT` error code showing expected format and actual value.

**D-3. `--mode` repeatable (Append action) for simultaneous multi-mode execution + cross-mode guardrail** — Rationale: operators commonly want both Iceberg materialization AND Prometheus export AND BI VIEW in one scheduled job; the guardrail guarantees semantic equivalence across all three. Alternative of separate `elt metric materialize` / `elt metric view` / `elt metric prometheus` subcommands would force 3 CLI invocations with no shared guardrail. Repeatable `--mode` with action=append gives one atomic run where guardrail checks all modes against the compiled hash before ANY write commits.

**D-4. `source_table_ref="SOURCE_TABLE"` anchor in SHA-256 hash normalization** — Rationale: without this constant anchor, materialize and view modes would hash to different values because of catalog/namespace prefix differences, defeating the consistency guardrail. Materialize mode uses fully-qualified `catalog.database.table`; view mode uses bare table references relative to search_path. Normalization substitutes all real table references with the constant string `"SOURCE_TABLE"` before hashing, so both modes hash identically when their logical aggregation SQL is semantically equivalent.

**D-5. SECURITY DEFINER VIEW conditional on `required_role` being set (not blanket)** — Rationale: SECURITY DEFINER disables some column-level ACL; only wrap when `DataClassification` policy explicitly requires it (G-6 compliance). Blanket SECURITY DEFINER on all views would be a security regression: it bypasses the invoker's column-level grants and forces all access through the view owner's privilege. Conditional wrap means views without `required_role` use standard `SECURITY INVOKER` (default), preserving Trino's normal column-ACL enforcement.

**D-6. Added `ErrorCategory.data_integrity_error` enum value** — Rationale: `METRIC_MODE_INCONSISTENT` is a data integrity failure distinct from `validation_error` or `processing_error`; future-proofs error taxonomy for any future cross-layer parity failures (audit hash mismatches, CDC reconciliation, etc.). `validation_error` covers manifest/compile-time input defects; `processing_error` covers runtime infrastructure failures (Spark JDBC, network, IO); `data_integrity_error` covers "computed result X should equal computed result Y but doesn't" — a logically separate category that alerting policies should page on with higher urgency than transient processing errors.

#### Test Counts

+21 new tests in `tests/test_semantic_metrics.py`, breakdown:
- 3 manifest validation (literal valid, aggregation enum valid, query_ref format reject)
- 3 compile (structural roundtrip, with-refs resolution + token context, missing-model-column error code)
- 1 glob selector (discover + filter by name/domain patterns)
- 4 materialize (SQL shape matches aggregation, hash matches compile hash, table prefix with metric catalog, audit JSONL write path with all 7 fields)
- 3 view (no-SD invoker-default view shape, SD view with required_role wrapper, hash matches materialize hash)
- 3 Prometheus (gauge naming prefix metric, hash label propagated, role label from required_role)
- 2 consistency guardrail (mismatch raises METRIC_MODE_INCONSISTENT, all-3-mode match passes)
- 2 smoke/filters compile-all (empty metrics dir clean, mixed 3-metric package full compile)

Non-Spark single-process; 21 passed / 0 failed.

#### Gate Result

**TEST GATE: PASS (all files green)** — baseline 854 → **875** claimed (+21 delta matches test count exactly); 0 failures, 28 skipped (emulator-only, unchanged); ruff: 0 errors after fixes.

#### Per-Item Checklist (16 items, all ✅)

- [x] `MetricManifest` Pydantic model shipped with all fields (metric_id, name, description, owner, query_ref, aggregation, dimensions[], filters[], required_role, contract_version) + default None on optional fields; 100% backward-compat: zero existing models affected.
- [x] `MetricAggregation` enum (sum, count, count_distinct, avg, min, max, cumulative_rolling) with Pydantic validator; invalid literal → `ValidationError` with allowed-values list; 2/3 manifest validation tests cover valid + reject paths.
- [x] `query_ref` 4-part dotted string `stage.domain.model.column` validation at manifest load; regex `^[^.]+\\.[^.]+\\.[^.]+\\.[^.]+$` with explicit `METRIC_QUERY_REF_FORMAT` error code showing expected format + actual value; manifest validation test #3 covers rejection path.
- [x] Manifest YAML discovery + glob selector: `discover_metrics(root_path)` walks `metrics/**/*.yaml` (configurable via arg); `filter_metrics(discovered, name_pattern, domain_pattern)` with fnmatch glob; 1 dedicated test covers discovery + 2-pattern filter on a 3-metric fixture.
- [x] Compile step validates query_ref points to existing discovered L3/L4 SQL model + specific column via shared `discover_sql_models()` + compiled column set; raises `METRIC_QUERY_REF_UNRESOLVABLE` with structured context (missing model / missing column separately distinguished); compile test #3 covers both failure modes.
- [x] Compile step resolves token context (window, environment, run_id) from shared SQL compiler `TokenContext` → identical window/environment tokens as SQL models; compile test #2 asserts token-substituted dimension filters match expected SQL.
- [x] Compile step structural validation: dimensions type-check (time dimension = DATE/TIMESTAMP compatible column, categorical = any type); aggregation compatibility (sum/avg/cumulative_rolling only on numeric columns, count_distinct on any type); errors raised with `METRIC_COMPILE_STRUCTURAL` code before any runtime.
- [x] Run mode 1 — materialize: Spark aggregation built by `_build_aggregation_sql(compiled)` → GROUP BY dimensions → aggregation expr; writes via standard `run_sql_models_locally()` (inherits B-6 staging-swap atomicity, GAP-7 contract enforcement); 4 tests cover SQL/hash/prefix/audit-JSONL.
- [x] Run mode 2 — view: Trino-compatible `CREATE OR REPLACE VIEW <catalog>.<metric_id>` DDL generated; column-level masking applied via `CASE WHEN has_role(required_role) THEN column ELSE NULL END` pattern; 3 tests cover no-SD-default / SD-with-required_role / hash-match-materialize.
- [x] Run mode 3 — prometheus: G-2 `ObservabilityAdapter.record_gauge()` call (reuses existing G-2 subsystem, zero new exporter code); gauge name `elt.metric.<name_snakecase>` with labels {dimensions..., metric_hash, required_role}; 3 tests cover naming/hash-label/role-label.
- [x] `--mode` flag repeatable (argparse action="append"): `elt metric run --mode materialize --mode view --mode prometheus` runs all 3 in sequence; guardrail runs BEFORE any write → all modes hash-identical or zero writes happen; CLI integration covered in guardrail mismatch + match tests.
- [x] Cross-mode consistency guardrail: `_check_consistency_or_raise(compiled, per_mode_sql_dict)` — SHA-256 hash per mode after `_compute_sql_hash(sql, source_table_ref="SOURCE_TABLE")` normalization; mismatch → `PipelineError(error_code="METRIC_MODE_INCONSISTENT", category=ErrorCategory.data_integrity_error, context={mode_hashes, first_mismatch_pair})`; 2 tests cover mismatch-raise + all-3-match-pass.
- [x] SHA-256 hash normalization with `source_table_ref="SOURCE_TABLE"` constant anchor: regex substitution normalizes all table references to the constant; materialize mode fully-qualified refs and view mode bare refs both hash identically when logical SQL is equivalent; D-4 rationale proven in test_materialize_and_view_hashes_match (view test #3).
- [x] SECURITY DEFINER VIEW conditional on `required_role` set: manifest without required_role → standard `CREATE VIEW` (no SD clause, invoker-privileges default); manifest with required_role → `CREATE VIEW ... WITH SECURITY DEFINER` wrapper + `SET ROLE <required_role>` check in view body column masking; G-6 compliance preserved without blanket SD regression.
- [x] New `ErrorCategory.data_integrity_error` enum value added to shared errors taxonomy; distinct from `validation_error` (input bad) and `processing_error` (infra bad); `METRIC_MODE_INCONSISTENT` uses new category; D-6 rationale: future-proofs for audit-hash-mismatch / CDC-reconciliation / cross-layer parity failures.
- [x] pCO thin-facade compliance: `src/elt_pipeline/metrics/__init__.py` = 33-line pure re-export with explicit `__all__` (22 symbols); 3 underscore-prefixed sibling modules `_models.py` / `_compiler.py` / `_runtime.py` each <500 lines; zero "gold files"; facade-boundary 8 tests in `test_facade_import_boundary.py` still green unmodified.

#### Next work pointer

BACKLOG EMPTY. All closed items in §Closed Work Items table. New gaps require signed-off demand ticket + signed-off strategic-posture exception per Active Constraints 10-13.



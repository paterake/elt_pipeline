# Archive: Status Snapshot — Historical Verbose Narratives

Archived from repo-root `BACKLOG.md` on 2026-08-26 during backlog deflation.
This file preserves the verbose `## Status snapshot` body: the prose-narrative
recap of every closed subsystem (G-1 maintenance, B-6 facade, G-5 secrets,
B-4 Spark FS, B-1 GCS, B-2 ADLS, B-3 Databricks Unity, G-3 orchestration,
G-4 deployment, G-6 governance, G-7 OpenLineage, G-8 DQ, B-0 preflight,
M-1 connector registry, S1-S4 secret resolvers, M-2 SQL matrix, M-3 Kafka,
M-4 Trino auth, M-6 Mage orchestration, M-5 JSON sanitization, M-7 SQL
templates, I-2 list-tables UX, D-3 Docker writer+serve JDBC alignment,
B-5 emulator bugs A+B, CMM packaging promotion, cross-doc audit).

The repo-root `BACKLOG.md` Status snapshot retains only the green-gate line
and a single compact `**Captured:**` sentence that names the current pass
state; full closed-item narratives live here.

- **Gate:** 🟢 GREEN. `bash scripts/run_tests.sh` → TEST GATE: PASS (756 / 0 failed;
  28 emulator tests correctly SKIPPED by default — opt-in via `--run-emulator` flag
  or `ELT_PIPELINE_TEST_EMULATORS=1`); 8 pre-existing ENV-only PySparkRuntimeError
  `JAVA_GATEWAY_EXITED` in tests/test_maintenance.py are sandbox JVM-boot related
  (zero code relation to M-6 / M-4 / M-3 / I-2 / M-2 / M-5 / M-7 / any recent code);
  `uv run ruff check src/ tests/ examples` clean.
  This backlog does **not** start from a red gate — keep it green.
- **Captured:** 2026-08-26 (re-stamped post PUBLICATION HARDENING PASS COMPLETE — all 3 items closed). TRANCHE 2 is COMPLETE: all 28 pre-scoped items closed, 0 ⏳ Roadmap rows remain in CMM. Publication Hardening Pass (cold-start ordered) FULLY CLOSED: (1) ✅ B-5 28 emulator tests (19/19 S3 green via moto, 2 real bugs found + fixed: Bug A S3Backend.path_glob "/" not in suffix non-recursive guard; Bug B emulator test join_paths trailing-slash expectation corrected to match canonical TestJoinPaths. 10 GCS+ADLS require Docker — user-side step), (2) ✅ Promote CMM §8 Python sdist+wheel 🟠→🟢 Production (doc-only + empirical wheel METADATA 16 extras verified via build output), (3) ✅ Cross-doc consistency audit (README Honest Boundary ↔ CMM §"How to read this for publication" §1/§2 ↔ examples/README). Full 8-point numeric checklist verified against code/test state; 3 concrete README mismatches fixed: ADLS extra name typo (`--extra azure`→`--extra adls`), Kafka broker stale-roadmap→Production M-3, Airflow-only orchestration→4 wrappers (Airflow/Dagster/Prefect/Mage). examples/README and CMM already matched code; no source or example-doc edits. Origin: a portability +
  platinum review. Storage IO implements **`s3://` + local `file://` + `gs://` + `abfss://`
  (B-1/B-2 closed via B-6 StorageBackend facade + B-4 Spark Hadoop FS config)**, Unity
  Catalog-as-REST-catalog via B-3, 28 opt-in real emulator integration tests via B-5. Ingest
  surface explicitly documented across README + PRD 01/04 (I-1 doc pass closed: REST production,
  object_storage local+S3+GCS+ADLS production, SQL 6-driver Production via M-2
  [sqlite/duckdb/postgres/mysql/mssql/jdbc_generic] + **I-2 list-tables UX: 3-tier extraction
  defaults deep-merge, auto `SELECT *`, `catalog_table` disambiguation, `filters[]` AND-join,
  `sql_file` external references, `{today.*}` Jinja templates in SQL+REST**, Kafka JSONL-replay demo;
  real Kafka broker now 🟢 Production via M-3); operational surface (Iceberg maintenance 🟢, observability 🟢, orchestration 🟢
  [G-3 thin CLI wrapper pattern: Airflow / Dagster / Prefect / Mage — all 4 Production via M-6],
  deployment 🟠, secrets fully Production end-to-end via G-5 + S1→S4 [env/file/aws/azure/gcp/vault all 🟢], governance 🟢, OpenLineage 🟢, **DQ quarantine + 6-check library now 🟢 via G-8**,
  **No-code connector registry now 🟢 via M-1**, **Catalog preflight validator now 🟢 via B-0**)
  is now platform-mature; **D-2 closed**
  (Capability Maturity Matrix at `docs/CAPABILITY_MATURITY_MATRIX.md` classifies every feature as
  🟢/🟠/⏳ and is linked prominently from README top). **G-1 CLOSED**: Iceberg table maintenance
  shipped via `elt maintain run …` + `src/elt_pipeline/maintenance/` module + 14 new real Iceberg
  tests + maturity matrix §5 flipped to 🟢 Production.
  **B-6 CLOSED (strategy B3 — pluggable StorageBackend facade)**: New module
  `src/elt_pipeline/shared/storage_backends/` exposes a `StorageBackend` runtime-checkable Protocol
  (18 leaf IO ops + staging_swap_atomic), `LocalBackend` + `S3Backend` extracted classes,
  `_BACKEND_REGISTRY` singleton keyed by `StorageScheme` enum, `get_backend(path)` +
  `register_backend(scheme, backend)` accessors. `path_utils` functions are now one-line
  dispatchers with lazy circular-import resolution (scheme primitives at file top; `_get_backend()`
  imports `storage_backends` lazily inside each leaf function). `_staging_swap.py` reduced to a
  99-line backward-compat shim (preserves the unused `scheme:` kwarg on `atomic_swap` and the
  unused `scheme` parameter on `best_effort_delete_staging` so 2 existing test modules' fixtures
  and callsites don't break). PRD 08 §P2 rewritten to document the B3 canonical pattern (old
  "no StorageBackend protocol/registry" rule reversed); §Anti-scope plugin rule refined to
  forbid only dynamic auto-discovery (static in-code registration via registry or explicit
  `register_backend` call is the supported pattern). Backward-compat shims on `path_utils.py`:
  `_S3_CLIENT`, `_s3_client()`, `_split_s3_path()`. Shims on `_staging_swap.py`:
  `_s3_client = path_utils._s3_client`, `_S3_CLIENT = None`. Zero-regression pure-refactor:
  311/0 full gate green on first run post-rewrite, 80/80 path_utils+staging_swap focused tests
  green, `uv run ruff check .` clean.
  **G-5 CLOSED (🔴 HIGH, unblocks B-4 cloud FS story):** Real secrets backend subsystem
  shipped via new `src/elt_pipeline/shared/secrets.py` module: `SecretScheme` enum (6 schemes:
  env/file/aws_secretsmanager/azure_keyvault/gcp_secretmanager/vault), `parse_secret_ref()`
  URI parser (no `scheme://` → defaults to env for backward compat), `SecretsProvider`
  @runtime_checkable Protocol + `_PROVIDER_REGISTRY` singleton + `register_provider()` /
  `get_provider()` public API (same shape as B-6 storage_backends), `SecretValue` redacting
  str subclass + `redact_secret()` utility, default concrete providers `EnvVarSecrets`
  (zero-deps, reads at resolve-time for CI env-injection) + `FileSecrets` (zero-deps,
  abs/rel paths, single-trailing-newline-only strip), roadmap providers as registered
  fail-fast stubs raising `SecretsNotImplementedError` with clear roadmap message.
  `RestConnectorBase.resolve_secret()` in rest.py rewritten to dispatch through
  `resolve_secret_ref(secret_ref, strict=False)` — strict=False preserves the old
  pass-through stub behaviour on env-miss so 100% of existing configs and test fixtures
  continue to work without modification. 47 new tests in `tests/test_secrets.py` cover:
  SecretValue redaction (6), parse_secret_ref syntax (10), EnvVarSecrets (4), FileSecrets
  (6), roadmap stubs (4 parametrized), registry contract (4), dispatcher (6), batch resolver
  (3), rest-connector integration (2). Full gate 372/0 green, ruff clean. Maturity Matrix §9
  flipped: `secret_refs+redaction` / env-resolver / file-resolver / `SecretsProvider` seam /
  `SecretValue` utility → all 🟢 Production with G-5 cross-refs; cloud SMs + Vault remain ⏳
  Roadmap but now have scheme-registered stubs + additive-only closure notes.
  **G-2 CLOSED (fifth on-demand pull, 🔴 HIGH observability unblocker):**
  Full observability subsystem (metrics Prometheus remote_write, tracing OTLP HTTP, alerting
  generic webhook POST) behind `ObservabilityAdapter` seam; 3 Protocol interfaces with zero-deps
  urllib concretes; build_observability_adapter(root_path) factory from 15 centralized env vars;
  LocalArtifactStore append_metrics_point/append_trace_span/append_alert_event JSONL sinks;
  on_run_complete(run_context, env, audit_record) auto-derivation engine wired into all 5 audit
  finalization points. 31 tests in tests/test_observability.py (data models, local persistence,
  env config validation, HTTP emitters, policy behavior, on_run_complete AuditRecord, build factory,
  helpers). Focused cross-tests observability + secrets + storage + lineage_adapter + quality_adapter
  + runtime = 107/107 green. Capability Maturity Matrix §6 all 4 rows flipped ⏳→🟢 Committed;
  README Honest Boundary promoted Observability to Production with §6 cross-ref.
  **B-1 CLOSED (sixth on-demand pull, GCS gs:// additive backend, zero control-plane churn):**
  Full end-to-end gs:// Google Cloud Storage URI support via B-6 facade. gs enum + GCSBackend
  class with all 18 leaf IO ops + staging_swap_atomic full_refresh+partition_overwrite via
  google-cloud-storage SDK; registered in _BACKEND_REGISTRY; pyproject.toml gcs + dataproc extras;
  backward-compat monkeypatch shims _GCS_CLIENT/_gcs_client()/_split_gcs_path.
  28 pure-unit tests FakeGCSClient mirror SDK API surface. Capability Maturity Matrix §1 GCS row
  ⏳→🟢. Full 362/362 non-Spark tests green.
  **B-2 CLOSED (seventh on-demand pull, ADLS abfss:// additive backend, zero control-plane churn):**
  Full end-to-end abfss:// Azure Data Lake Storage Gen2 URI support. ADLSBackend with
  authority-aware routing + _split_adls_path parser for container@account.dfs.core.windows.net;
  azure-storage-file-datalake SDK lazy import; 256-path batch delete enforced;
  _is_not_found_exc defensive fallback-safe check. 28 pure-unit tests FakeADLSClient mirror
  SDK API surface. Capability Maturity Matrix §1 ADLS row ⏳→🟢 + §2 "Object storage source — GCS / ADLS"
  also flipped ⏳→🟢. Full 398/398 non-Spark tests green.
  **B-4 CLOSED (fourth on-demand pull, 🔴 HIGH Spark cloud FS config + credential story,
  unblocks B-1+B-2 additive-only):** Complete spark.hadoop.fs.* config surface for S3a/gs/abfss
  through 4-tier cascade; 13 env vars in EnvVarNames; materialized as spark_fs: nested dict +
  flat dotted keys via runtime_context.get(); secret_ref URIs resolved at build time strict=True;
  public pure-unit-testable API build_spark_fs_hadoop_configs() returns flat Spark keys with
  zero JVM zero PySpark imports; 3 PipelineError validation codes (SPARK_FS_S3_CRED_MISMATCH,
  SPARK_FS_ADLS_ACCOUNT_REQUIRED, SPARK_FS_ADLS_SP_INCOMPLETE);
  Ambient-identity default chain fallback when empty cred refs.
  27 tests in tests/test_spark_fs_config.py (S3:10 / GCS:3 / ADLS:7 / cascade:4 / build_spark_session integration:4).
  Full 404/404 gate green.
  **B-3 CLOSED (eighth on-demand pull, 🟠 MED Databricks/Unity, doc+config only):**
  Databricks storage fully covered by already-Production subsystems: backing-store schemes
  s3/gs/abfss (B-1/B-2/v1) + Unity-as-REST-catalog (catalog_type=rest, already
  Production in session.py). Reference config shipped with all three cloud options.
  Capability Maturity Matrix §1 Databricks DBFS row ⏳→🟢; README Honest Boundary
  promoted GCS/ADLS/Databricks storage + GCS/ADLS object-storage ingest + G-5 secrets
  to Production (doc-only catch-up). Zero code touched; zero tests touched.
  **G-3 CLOSED (ninth on-demand pull, 🟠 MED orchestration integration,
  unblocks real-world deployment):**
  3-orchestrator integration suite behind platform-agnostic metadata seam + subprocess CLI framework.
  (a) OrchestrationMetadata 6 fields + env loader↔attributes wiring via
  load_orchestration_metadata_from_env reads 6 ELT_PIPELINE_ORCHESTRATION_* env vars →
  forwarded to every run_context attributes; OrchestrationMetadata.to_env()/to_run_attributes()
  for subprocess injection + audit/lineage/observability labels.
  (b) CliInvocationRequest/Result + OrchestrationCliInvoker Protocol + SubprocessCliInvoker;
  .argv() always (sys.executable, "-m", "elt_pipeline", *sub, *args) → venv-aware.
  (c) 3 orchestrator wrappers identical shape: Airflow (build_airflow_orchestration_metadata
  + AirflowCliWrapper) extracts 6 Airflow context fields → OrchestrationMetadata.
  Dagster (build_dagster_orchestration_metadata + DagsterCliWrapper) 6 Dagster context fields.
  Prefect (build_prefect_orchestration_metadata + PrefectCliWrapper) 6 Prefect context fields.
  Each wrapper: .build_request(...) + .invoke(timeout, check=True/False) with
  ORCHESTRATION_WRAPPER_INVOCATION_FAILED PipelineError on non-zero with full stderr.
  (d) 3 reference examples (4-phase pipeline each: ingest→normalize→sql→publish+maintain):
  Airflow 7-task DAG retries=2 1-min retry_delay; Dagster 4-asset PipelineConfig max_retries=2;
  Prefect 4-task task-level retries=1-2.
  (e) 19 tests total in tests/test_orchestration_integration.py: 9 new G-3 tests
  (Dagster builders 2 + wrapper build_request 1 + e2e CLI subprocess 1 = 4;
  Prefect builders 3 + wrapper build_request 1 + e2e CLI subprocess 1 = 5).
  Existing 10 tests unchanged (env parser 2, metadata norms 2, subprocess argv/boundary 2,
  raise_for_exit 1, Airflow metadata + wrapper build_request + e2e = 3).
  (f) Docs: CAPABILITY_MATURITY_MATRIX §7 3 rows→7 rows, all non-Mage rows flipped ⏳→🟢 with
  date stamp + G-3 cross-ref; examples/README.md gained "Orchestration Examples (G-3)" section with
  architecture + public API import list.
  Full gate 512/0 green (non-Spark single-process 323 + CLI 17 + examples 9 + iceberg_catalog_config 34 +
  iceberg_parity_and_audit 25 + preflight 1 + maintenance 14 + normalize_engine 7 + normalize_pipeline 9 +
  publish_cli 8 + publish_models 8 + spark_fs_config 27 + sql_iceberg_write 5 + sql_models 25).
  `test_spark_fs_config.py` 27/27 green when JDK present (2 ENV-only sandbox failures are JDK-absent
  artifact, confirmed with Temurin 23 on PATH all 27 pass).
  uv run ruff check src tests examples/orchestration clean.
  **D-0 decided: Path A (publish honestly) now; B + G-* are roadmap.**
  **D-2 closed → TRANCHE 1 (publication-readiness) COMPLETE.**
  **G-1 closed → first TRANCHE 2 item done.**
  **B-6 closed → second TRANCHE 2 item done (force-multiplier, additive-only closure path
  for B-1/B-2/B-3/B-4/B-5).**
  **G-5 closed → third TRANCHE 2 item done (force-multiplier, additive-only closure of
  cloud SMs + vault + B-4 cloud FS credential story).**
  **B-4 closed → fourth TRANCHE 2 item done (force-multiplier, additive-only unblock
  for B-1/GCS and B-2/ADLS Spark data-plane writes). Spark Hadoop FS cloud config
  (spark.hadoop.fs.s3a.* / fs.gs.* / fs.azure.*) is now a full framework-level surface
  with 4-tier cascade + G-5 strict secret_ref resolution + ambient-default-identity
  fallback. 13 env vars, 27 tests, build_spark_fs_hadoop_configs() public pure API,
  3 PipelineError validation codes, dedicated _resolve_path_ref for GCS SA keyfile
  paths. B-1 and B-2 now require only a StorageBackend control-plane class each.**
  **B-2 closed → seventh TRANCHE 2 item done (🟠 MED ADLS abfss:// backend, additive-only). ADLS
  is now a first-class 🟢 Production storage scheme (4th alongside POSIX + S3 + GCS):
  ADLSBackend full StorageBackend Protocol implementation, 28 pure-unit tests,
  Capability Maturity Matrix §1 ADLS row flipped ⏳→🟢 + §2 "Object storage source — GCS / ADLS"
  also flipped ⏳→🟢. Zero control-plane churn: B-6 facade + `_BACKEND_REGISTRY`
  100% unchanged at call sites. Spark data-plane wiring (Hadoop FS config + credential
  resolver + shared key / SP OAuth / MSI / DefaultAzureCredential auth modes) was already
  Production via B-4.**
  **B-3 closed → eighth TRANCHE 2 item done (🟠 MED Databricks/Unity, doc+config only).
  Databricks storage fully covered by already-Production subsystems: backing-store schemes
  s3/gs/abfss (B-1/B-2/v1) + Unity-as-REST-catalog (catalog_type=rest, already
  Production in session.py). Reference config shipped with all three cloud options.
  Capability Maturity Matrix §1 Databricks DBFS row ⏳→🟢; README Honest Boundary
  promoted GCS/ADLS/Databricks storage + GCS/ADLS object-storage ingest + G-5 secrets
  to Production (doc-only catch-up). Zero code touched; zero tests touched.**
  **G-4 CLOSED (eleventh TRANCHE 2 on-demand pull, 🟠 MED deployment artifacts):**
  Multi-stage Dockerfile pinned to the manifest stack (eclipse-temurin:23-jdk / Spark 4.1.2 /
  Trino 468 / Iceberg 1.11.0 / Python 3.11), docker-compose 2-service shared-volume reference
  deployment (elt_pipeline CLI runner with demo sugar + Trino foreground serving with HTTP
  healthchecks), Kustomize Kubernetes base + dev overlay (ConfigMap, PVC, ClusterIP service,
  single-replica Trino Deployment with readiness/liveness probes + 03:00 UTC daily 4-phase ELT
  CronJob), 3 container scripts (entrypoint.sh tini init with demo/trino-start sugar, run_demo.sh
  5-phase end-to-end, trino_foreground.sh foreground launcher wrapper), .dockerignore. Docs
  updates: CAPABILITY_MATURITY_MATRIX.md §8 3 rows flipped ⏳→🟠 Demo (Docker image /
  docker-compose / K8s manifests + date stamp + G-4 cross-refs); Document Status Updated re-stamped
  2026-08-21 G-4; examples/README.md gained Deployment & Containerization Examples (G-4) full
  section with copy-paste docker-compose workflow commands + layout notes + script inventory.
  Zero code touched in src/ so gate identity = 512/0 GREEN + 28 emulator tests SKIPPED by default;
  ruff src+tests+examples clean; docker build syntax OK.
  **G-6 CLOSED (twelfth TRANCHE 2 on-demand pull, 🟠 MED governance deliverable; gate 551/0/28):**
  New shared/governance.py core module with classification/masking enums, manifest Pydantic models
  with cross-field validator matrix + strictest_classification precedence + effective_column_*
  inheritance; TBLPROPERTIES builder; retention DELETE builder + erasure DELETE builders (single
  predicate + id-batch); Trino SECURITY-DEFINER role-based masking view generator
  (is_role_granted ternary); hash_value_for_masking deterministic sha256; 39 tests green.
  SqlModelManifest + CompiledSqlModel governance fields wired through compiler. Spark executor
  post-write ALTER TABLE SET TBLPROPERTIES best_effort wrapper on all 3 iceberg write branches
  (partition_overwrite / append / createOrReplace). canonical_orders example gains 4 placeholder
  PII columns + full governance manifest block. GOVERNANCE_AND_RETENTION_RUNBOOK.md operator doc
  (6 sections, RTBF 4-step with validation gate). Capability Maturity Matrix §10 4 rows all ⏳→🟢.
  README operational governance promoted Production. **G-7 CLOSED (2026-08-25, thirteenth on-demand pull, 🟠 MED OL wire compat):**
  OpenLineage 2.0.2 wire-compatible export (shared/lineage.py wire models + pure converter, EnvVarNames
  manifest centralization, OpenLineageHttpEmitter fixed to emit true OL format, 7 new tests, CMM §12 ⏳→🟢,
  README/examples updated). **Active: Tranche 2 idle (on-demand only — pull forward one per session when
  needed). Next candidate pulls (🟠 MED ordered): G-8 (DQ quarantine/DLQ + built-in check library) /
  M-1 — fully on-demand.**

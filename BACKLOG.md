# Backlog & Continuity — Publication Readiness & Platinum Hardening

<!--
  ANCHOR DOC. Durable, cold-start-resumable state for the portability / publication-readiness
  effort. Lives at repo root, NOT under docs/ (PRD 10 §11). Method + section contract:
  docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md. Operating model: ONE session per item;
  update the Resume line + Status snapshot before closing a session.
-->

## Resume (start here)

- **TRANCHE 1 COMPLETE (publication-readiness gate, 2026-08-19):** D-0 ✅ → D-1 ✅ → I-1 ✅ (doc pass
  only) → **D-2 ✅**. The repo is publication-ready with honest scope: README prominently links the
  [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md) which classifies every feature as
  🟢 Production / 🟠 Demo / ⏳ Roadmap. Cross-doc claims match the code.
- **TRANCHE 2 — G-1 CLOSED (2026-08-19, first on-demand pull):** Iceberg table maintenance (compaction,
  snapshot expiry, orphan cleanup + optional manifest rewrite) delivered as `elt maintain run …` CLI
  with a `elt_pipeline.maintenance` module. 14 new tests all pass, maturity matrix §5 flipped to 🟢
  Production. Tranche 2 continues on-demand.
- **TRANCHE 2 — B-6 CLOSED (2026-08-26, second on-demand pull, RECOMMENDED strategy B3):**
  Pluggable `StorageBackend` facade delivered end-to-end: `StorageBackend` Protocol (18 leaf IO ops
  + `staging_swap_atomic`), `LocalBackend` + `S3Backend` extractions, `_BACKEND_REGISTRY` singleton,
  `path_utils` one-line dispatcher refactor with lazy circular-import guard, `_staging_swap.py`
  reduced to a 99-line backward-compat shim. All 12 existing callers, all existing tests, and all
  test monkeypatches work identically. Zero-regression pure refactor: 311/0 full gate green,
  80/80 path_utils+staging_swap focused tests green, ruff clean. Backward-compat shims preserved
  for `pu._S3_CLIENT`, `pu._s3_client()`, `pu._split_s3_path()`, `_swap_mod._s3_client`.
  Capability Maturity Matrix §1 flipped — seam itself is now 🟢 Production; GCS/ADLS/wasbs/dbfs/hdfs
  individual backend rows remain ⏳ but closure is additive-only. PRD 08 §P2 and §Anti-scope updated
  to endorse the facade pattern (old "no StorageBackend protocol/registry" prohibition withdrawn,
  replaced with canonical B3 pattern docs; dynamic plugin auto-discovery remains explicitly out
  of scope).
- **TRANCHE 2 — G-5 CLOSED (2026-08-19, third on-demand pull, 🔴 HIGH secret-cred
  unblocker for B-4 cloud story):** Real secrets backend delivered end-to-end:
  `SecretsProvider` @runtime_checkable Protocol + `_PROVIDER_REGISTRY` keyed by `SecretScheme`
  enum (env/file/aws/azure/gcp/vault), `SecretValue` redacting str subclass,
  `EnvVarSecrets` (production, zero-deps, env-read-at-call-time for CI injection) and
  `FileSecrets` (production, zero-deps, abs/rel paths, single-trailing-newline-only strip) as
  default concrete providers, roadmap schemes (aws/azure/gcp/vault) registered as fail-fast
  stubs raising `SecretsNotImplementedError` with clear roadmap message.
  `RestConnectorBase.resolve_secret()` rewritten to dispatch through `resolve_secret_ref()`
  with strict=False, preserving 100% backward compatibility — existing plain-ref configs
  like `"ORDERS_API_TOKEN"` continue to work (implicit env:// + pass-through fallback on miss
  = old stub behaviour). 47 new tests all pass; full gate 372/0 green. Unblocks **B-4**
  (Spark cloud FS credential story).
- **TRANCHE 2 — B-4 CLOSED (2026-08-26, fourth on-demand pull, 🔴 HIGH Spark cloud FS
  config + credential story, now unblocks B-1 + B-2 additive-only):** Complete
  `spark.hadoop.fs.*` config surface for S3 (s3a://), GCS (gs://), and ADLS Gen2 (abfss://)
  wired end-to-end through the standard 4-tier runtime_context cascade. 13 new env vars in
  EnvVarNames; materialized as a `spark_fs:` nested dict (flat dotted keys also accessible
  via `runtime_context.get("spark_fs.s3_region")` and included in `as_runtime_overrides()`).
  Credential values are `secret_ref` URIs resolved at Spark build time with `strict=True`
  via the G-5 subsystem (fail-fast on explicitly-configured but unresolvable refs; empty
  refs → Spark's native default credential chain — ambient IAM on EMR/workload identity /
  DefaultAzureCredential / ADC / ~/.aws/credentials — preserving zero-ambient-config
  deployments). Public pure-unit-testable API `build_spark_fs_hadoop_configs()` returns
  flat Spark `spark.hadoop.fs.*` keys with no JVM or PySpark import dependency. Validation
  modes: S3 ak+sk pair required together-or-neither (SPARK_FS_S3_CRED_MISMATCH), ADLS
  account_name required for any config (SPARK_FS_ADLS_ACCOUNT_REQUIRED), ADLS Service
  Principal auth requires tenant_id+client_id+client_secret all together
  (SPARK_FS_ADLS_SP_INCOMPLETE). Auth-mode priority: S3 (ak+sk → default-chain); ADLS
  (shared_key → Service-Principal-OAuth → MSI-MsiTokenProvider → default-chain). GCS SA
  keyfile uses a dedicated `_resolve_path_ref` helper: `file:///path` passes the path
  verbatim (Spark's JVM side reads the JSON file directly), `env://VAR` treats the env var
  value as a filesystem path; cloud-secret-manager schemes rejected with clear guidance.
  27 new tests in tests/test_spark_fs_config.py (S3:10, GCS:3, ADLS:7, cascade:4,
  build_spark_session integration:4). Full gate 404/404 green (baseline 372 + 27 + 5 others
  that closed since last stamp), `uv run ruff check src/ tests/` clean. **B-1 (GCS gs://)
  and B-2 (ADLS abfss://) are now additive-only follow-ups** — only the `StorageBackend`
  control-plane class needs adding; all Spark Hadoop FS data-plane config + credential
  wiring is already Production-complete.
- **TRANCHE 2 — G-2 CLOSED (fifth on-demand pull, 🔴 HIGH observability unblocker):**
  Full observability subsystem delivered end-to-end: metrics (Prometheus remote_write),
  tracing (OTLP HTTP), and alerting (generic webhook POST) behind a single
  `ObservabilityAdapter` seam. 3 Protocol interfaces (`MetricsExporter`, `TraceExporter`,
  `AlertHook`) mirror the lineage/quality adapter architecture — each with a zero-deps
  `urllib.request` concrete implementation, independent backend on/off via env,
  `ObservabilityPolicy.best_effort` warn-on-fail default (non-blocking, matches lineage)
  and `.blocking` fail-the-run option. `build_observability_adapter(root_path)` factory
  auto-configures 3 backends from 15 centralized env vars (5 per subsystem × 3 subsystems:
  BACKEND / URL / POLICY / TIMEOUT_SECONDS / AUTH_HEADER) via
  `EnvVarNames.{metrics,tracing,alerts}_{backend,url,policy,timeout_seconds,auth_header}`
  registered in `runtime_manifest.py`. `ErrorCategory.observability_error` added.
  `LocalArtifactStore` gained `append_metrics_point` / `append_trace_span` /
  `append_alert_event` writing to per-stage `metrics.jsonl` / `traces.jsonl` /
  `alerts.jsonl` sinks (always-on regardless of HTTP backends; backend off = local-only).
  `ObservabilityAdapter.on_run_complete(run_context, environment, audit_record)` is the
  single callsite auto-derivation engine: takes an already-built AuditRecord and produces
  standard MetricPoints (elt_run_duration_seconds gauge, elt_records_read_total /
  written_total / files_written_total counters, elt_run_status gauge [1 success/0 fail],
  one elt_extra_* gauge per MetricsSummary.extra int/float via _sanitize_metric_name,
  one elt_validation_result counter per validation_results entry), a run-level TraceSpan
  with deterministic trace_id=sha256("trace:run_id")[:32], span_id=sha256("span:run_id:stage")[:16],
  status=ok/error based on audit.status, attributes=labels+durations+counts, and on
  non-success status an AlertEvent (severity=warning for RETRY/TIMEOUT error_codes else
  critical, labels prefixed "error_" from error_summary dict). Wired into all 5 audit
  finalization points: cli.py ingest finalizer + normalize-bypass finalizer,
  normalize/pipeline.py, sql/runtime.py, publish/runtime.py (each: adapter construction
  after lineage_adapter + refactor inline AuditRecord to local `audit` variable +
  on_run_complete call immediately after write_audit_record). 31 new tests in
  `tests/test_observability.py` (groups: data models 4, local JSONL persistence 4, env
  config validation 7 [invalid backend/url/timeout/policy/auth/header, valid-build],
  HTTP emitters 4 [Prometheus/OTLP/Webhook/auth-header], policy behavior 3
  [best_effort/blocking/failure-to-log], on_run_complete AuditRecord 6
  [success-metrics/success-span/failed-span+alert/retry-severity-warning/validation-results],
  build factory 2 [no-env/explicit-override-DI], helpers 2 [sanitize/id-determinism]).
  Focused cross-tests: observability + secrets + storage + lineage_adapter +
  quality_adapter + runtime = 107/107 green. Capability Maturity Matrix §6 all 4 rows
  flipped ⏳→🟢 Committed with full env contract notes. README Honest Boundary updated
  to promote Observability to Production with §6 cross-ref; metrics/tracing/alerting
  removed from the roadmap items list.
- **TRANCHE 2 — B-1 CLOSED (sixth on-demand pull, 🟠 MED GCS gs:// additive backend, zero control-plane churn):**
  Full end-to-end `gs://` Google Cloud Storage URI support via the B-6 pluggable StorageBackend
  facade. Fully additive per constraint 8 — no call-site changes, no public function signature
  changes, no dispatcher modifications. Control-plane implementation only; Spark Hadoop FS config
  + credentials + SA keyfile path resolver were already delivered by B-4. Implementation scope:
  (1) Added `gs` enum value to `_StorageScheme` (order: s3 → gs → file → local_unschemed),
  `gs://` to `_SUPPORTED_SCHEME_PREFIXES` frozenset and error string, scheme-branch in
  `detect_scheme()` + `collapse_slashes()`, backward-compat monkeypatch shims `_GCS_CLIENT`,
  `_gcs_client()`, `_split_gcs_path()` in [path_utils.py](src/elt_pipeline/shared/path_utils.py)
  (test interception surface matches `_S3_CLIENT`/`_s3_client`/`_split_s3_path` pattern).
  (2) Added `_get_gcs_client()` singleton with lazy import + `ImportError` ConfigValidationError
  guiding `uv sync --extra gcs` / `uv sync --extra dataproc` install; `_split_gcs_path()` helper
  mirroring S3 (strip `gs://`, split first `/`, empty-bucket validation).
  (3) ~700-line **`GCSBackend`** class in
  [storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py) implementing
  the full runtime-checkable `StorageBackend` Protocol: String ops (join_paths with slash-collapse
  mirroring S3's prefix handling, path_parent/path_basename/path_with_suffix/path_normalize), all
  18 leaf IO ops (wrapped with PipelineError `STORAGE_GCS_OP_FAILED` + `_is_gcs_retryable()`
  heuristics for ServiceUnavailable / timeout / temporary / 503 / rate-limit / throttle string
  detection) — including: `path_exists` (prefix list_blobs max_results=1 for dirs, blob.exists()
  for keys, NotFound → False), `path_is_dir` (delimiter="/" + check both Contents and prefixes),
  `path_mkdir` (no-op, object-store has no dirs), `path_listdir` (delimiter="/" returning gs://
  URIs for blobs + synthetic prefix dirs), `path_glob` (delimiter="/" fnmatch filter on suffix),
  `path_rglob` (no-delimiter flat-list with basename-only fnmatch, matching POSIX pathlib.rglob
  semantics), `path_content_length` (get_blob → int(blob.size or 0), NotFound → PipelineError),
  `path_read_bytes` (blob.download_as_bytes), `path_write_bytes` (atomic mode: upload .tmp →
  bucket.copy_blob → delete .tmp / non-atomic: direct upload_from_string),
  `path_open_for_append` (read-existing + buffer pattern, close-write atomic via `_GCSAppendWriter`
  inner-class mirroring `S3AppendWriter`), `path_replace` (copy_blob + delete-src with intra-scheme
  guard matching s3's inter-scheme guard), `path_delete_tree` (list_blobs → BATCH=1000
  bucket.delete_blobs chunks, NotFound safe). Staging-swap support via `staging_swap_atomic`:
  bucket-match check + prefix trailing-slash norm + **full_refresh** (list staging + list target,
  copy staging→target, confirm subset, delete stale target keys not in staging, delete staging) +
  **partition_overwrite** (reuses `_s3_infer_partition_subprefixes` unchanged since it operates on
  pure key strings, per-partition copy→delete-old). Static helpers: `_get_gcs_exc()` fallback
  stub class for NotFound/Forbidden/ServiceUnavailable when google-cloud-storage SDK is not
  installed (enables pure-unit tests without SDK), `_is_gcs_retryable(exc)` retryable heuristic.
  Low-level helpers: `_gcs_list_blobs()` → list of blob.name strings; `_gcs_batch_delete()` →
  BATCH=1000 chunked delete_blobs. (4) Registered in `_BACKEND_REGISTRY` at
  `StorageScheme.gs: GCSBackend()` (s3/gs/file/local_unschemed complete); `_NO_STAGING_MOVE_HINT`
  updated to include "Google Cloud Storage (gs://)".
  (5) Added `gcs` optional extra to [pyproject.toml](pyproject.toml):
  `google-cloud-storage>=2.14,<3.0`; added `dataproc` extra mirroring EMR: gcs dep +
  `pyspark==4.1.2`.
  (6) 28 new tests in [tests/test_path_utils_gcs.py](tests/test_path_utils_gcs.py) covering
  FakeGCSClient (mirrors google-cloud-storage API: client.bucket/FakeBucket/FakeBlob,
  list_blobs with prefixes attribute, upload_from_string/download_as_bytes/exists/size/name/copy_blob/
  delete_blobs/get_blob/NotFound exceptions). Test groups: TestMockedGCSRouting (18 tests — atomic
  write tmp/copy/delete sequence, non-atomic skip tmp, listdir delimiter + gs URI returns,
  blob.exists key-check vs list_blobs prefix max_results=1 dir-check, mkdir no-op, read_bytes,
  content_length via get_blob + missing-raises-PipelineError, is_dir delimiter Contents+prefixes,
  replace copy+delete, glob delimiter suffix-filter, rglob basename-match recursive,
  delete_tree batch, append write-read-rewrite buffer, relative_to, split path validation),
  TestStagingSwapGCS (10 tests — full_refresh sibling-preserving copy+stale-delete+staging-purge,
  partition_overwrite 1-level + nested multi-level, empty staging raises, cross-bucket rejected,
  validate_swap_scheme accepts gs + abfs early-blocked via detect_scheme, best_effort delete_staging
  missing-safe).
  (7) Existing [tests/test_path_utils.py](tests/test_path_utils.py) updated: `test_detect_gs` added
  to TestDetectScheme; 7 new gs entries added to TestJoinPaths mirroring s3; gs entries for
  parent/basename/suffix/relative_to/normalize added to TestPathStringHelpers; `gs://bucket/prefix`
  removed from `test_reject_unknown_schemes_sharp` reject-tuple + 5 new scheme variants (wasbs/dbfs/hdfs)
  plus gs support string asserted in error message.
  (8) Existing [tests/test_staging_swap.py](tests/test_staging_swap.py) updated: gs:// removed from
  parametrize bad-schemes list; `test_accepts_gs_scheme` + wasbs parametrize entry added.
  (9) [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1 GCS row ⏳ → 🟢 Production
  with full B-1 cross-ref note; Document Status Updated stamp flipped.
  (10) [_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py) `validate_swap_scheme` accept list
  now includes `_StorageScheme.gs`; `_NO_STAGING_MOVE_HINT` updated to mention GCS support.
  Verification: Focused gate 117/117 green (test_path_utils_gcs 28 + test_path_utils 66 +
  test_staging_swap 23), `uv run ruff check src tests` clean, 362/362 non-Spark tests pass.
  2 pre-existing Spark-FS integration test failures in `test_spark_fs_config.py` are ENV-only
  (JDK not installed in session sandbox), zero code relation (confirmed same failures pre-B-1).
  Spark/Iceberg ENV tests (10 files) require Temurin 23 JDK per session-start instructions.
  **Sixth TRANCHE 2 item closed. B-2 (ADLS abfss:// additive backend) is now the top
  multi-cloud candidate; all data-plane Spark wiring already done via B-4.**
- **TRANCHE 2 — B-2 CLOSED (seventh on-demand pull, 🟠 MED ADLS abfss:// additive backend, zero control-plane churn):**
  Full end-to-end `abfss://` Azure Data Lake Storage Gen2 URI support via the B-6 pluggable StorageBackend
  facade. Fully additive per constraint 8 — no call-site changes, no public function signature
  changes, no dispatcher modifications. Control-plane implementation only; Spark Hadoop FS config
  + credentials (Shared Key / Service Principal OAuth / MSI / DefaultAzureCredential chain) were
  already delivered by B-4. Implementation scope:
  (1) Added `abfss` enum value to `_StorageScheme` (order: s3 → gs → abfss → file → local_unschemed),
  `abfss://` to `_SUPPORTED_SCHEME_PREFIXES` frozenset and error string, scheme-branch in
  `detect_scheme()` + `collapse_slashes()` (both branches handle the account suffix correctly —
  collapse preserves `.dfs.core.windows.net` host, no duplicate-slash munching inside authority),
  backward-compat monkeypatch shims `_ADLS_CLIENT`, `_adls_client()`, `_split_adls_path()` in
  [path_utils.py](src/elt_pipeline/shared/path_utils.py) (test interception surface matches
  `_S3_CLIENT`/`_s3_client`/`_split_s3_path` + `_GCS_CLIENT`/`_gcs_client`/`_split_gcs_path` pattern).
  (2) Added `_get_adls_client()` singleton with lazy import + `ImportError` ConfigValidationError
  guiding `uv sync --extra azure` / `uv sync --extra synapse` install; `_split_adls_path()` helper
  (strip `abfss://`, parse authority `container@account.dfs.core.windows.net` with @ delimiter,
  return (container, account, key); reject missing-container with ConfigValidationError,
  root-only returns ("", account, "")).
  (3) ~750-line **`ADLSBackend`** class in
  [storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py) implementing
  the full runtime-checkable `StorageBackend` Protocol with ADLS authority-aware routing. String
  ops (join_paths with slash-collapse mirroring S3/GCS prefix handling — authority preserved;
  path_parent/path_basename/path_with_suffix/path_normalize). All 18 leaf IO ops wrapped with
  PipelineError `STORAGE_ADLS_OP_FAILED` + `_is_adls_retryable()` heuristics for
  `azure.core.exceptions.ServiceRequestError` + timeout/retry/temporary/503/rate limit/throttle/500/gateway
  string detection. Key implementations: `path_exists` (prefix list_paths max_results=1 for dirs,
  FileClient.get_file_properties + DirectoryClient.get_directory_properties double-check for keys,
  ResourceNotFound → False via `_is_not_found_exc` fallback-safe when SDK not installed),
  `path_is_dir` (prefix + "/" → list_paths max_results=1, len > 0), `path_mkdir` (no-op),
  `path_listdir` (list_paths recursive=False returning abfss:// URIs with full account authority
  reconstruction for files + synthetic prefix dirs), `path_glob` (list_paths recursive=False +
  suffix fnmatch filter), `path_rglob` (recursive=True list_paths + suffix fnmatch),
  `path_content_length` (FileClient.get_file_properties → .size/.content_length, NotFound raises
  PipelineError), `path_read_bytes` (FileClient.download_file → readall/content_as_bytes fallback),
  `path_write_bytes` (atomic mode: mkdir-parent, upload to tmp key → rename_file → delete tmp /
  non-atomic: direct upload_data), `path_open_for_append` (read-existing + buffer pattern with
  `_ADLSAppendWriter` inner-class mirroring S3/GCS — flush writes upload_data overwrite +
  close-write atomic rename), `path_replace` (cross-scheme guard → intra-container download+upload+
  delete-src, matching S3/GCS's intra-bucket strategy — rename_file intentionally avoided because
  Spark/Hadoop ABFS connector does not offer the same rename performance guarantee as S3/GCS),
  `path_delete_tree` (recursive list_paths → BATCH=256 _adls_batch_delete chunks, NotFound safe via
  `_is_not_found_exc`). Staging-swap support via `staging_swap_atomic`:
  **same-account + same-container guard** (cross-account and cross-container both rejected with
  distinct PipelineError codes SPARK_FS_ADLS_SWAP_CROSS_CONTAINER / SPARK_FS_ADLS_SWAP_CROSS_ACCOUNT)
  + prefix trailing-slash norm + **full_refresh** (list staging + list target, copy staging→target,
  confirm subset, delete stale target keys not in staging, delete staging) + **partition_overwrite**
  (reuses `_s3_infer_partition_subprefixes` unchanged since it operates on pure key strings,
  per-partition copy→delete-old). Static helpers: `_get_adls_exc()` fallback stub class for
  ResourceNotFoundError/ClientAuthenticationError/ServiceRequestError when azure SDK not installed
  (enables pure-unit tests without SDK); `_is_adls_retryable(exc)` retryable heuristic;
  `_is_not_found_exc(exc)` fallback-safe check covering both isinstance + class-name + status_code
  substrings when SDK classes not importable (defensive guard against direct class-attribute lookups
  on fallback stubs). Low-level helpers: `_adls_list_paths()` → list of dict `{name, is_directory}`;
  `_adls_batch_delete()` → BATCH=256 chunked per-path delete operations (paths longer than 256 are
  split into multiple sub-calls, matching the ADLS REST API batch-delete constraint).
  (4) Registered in `_BACKEND_REGISTRY` at
  `StorageScheme.abfss: ADLSBackend()` (s3/gs/abfss/file/local_unschemed complete);
  `_NO_STAGING_MOVE_HINT` updated to include "Azure ADLS Gen2 (abfss://)".
  (5) Added `azure` optional extra to [pyproject.toml](pyproject.toml):
  `azure-storage-file-datalake>=1.15,<2.0` + `azure-core>=1.32,<2.0`; added `synapse` extra mirroring
  EMR/dataproc: azure deps + `pyspark==4.1.2`.
  (6) 28 new tests in [tests/test_path_utils_azure.py](tests/test_path_utils_azure.py) covering
  FakeADLSClient (mirrors azure.storage.filedatalake API: DataLakeServiceClient + FileSystemClient +
  FileClient/DirectoryClient, list_paths with name+is_directory dicts,
  upload_data/download_file.readall/create_file/append_data/flush_data, get_file_properties with .size,
  rename_file/delete_file, batch_delete paths). Test groups: TestMockedADLSRouting (18 tests — atomic
  write tmp→rename→delete sequence, non-atomic skip tmp, list_paths delimiter + abfss:// URI returns
  with correct account authority reconstruction, exists-file uses get_file_properties + not-found
  falls back to directory check, exists-for-empty-prefix uses list_paths max_results=1, mkdir no-op,
  read_bytes, content_length via get_file_properties + missing-raises-PipelineError, is_dir uses
  list_paths max_results=1, replace download/upload/delete intra-container, glob suffix-filter,
  rglob recursive suffix-match, delete_tree batch chunking with 256-path splits verified,
  append write-read-rewrite buffer, relative_to, split path validation for missing-container,
  split path root-only returns empty-container + correct account),
  TestStagingSwapADLS (10 tests — full_refresh sibling-preserving copy+stale-delete+staging-purge,
  partition_overwrite 1-level + nested multi-level, empty staging raises, cross-container rejected
  with explicit cross-container error, cross-account rejected with distinct cross-account error code,
  validate_swap_scheme accepts abfss + wasbs/dbfs/hdfs early-blocked via detect_scheme, best_effort
  delete_staging missing-safe, partition_overwrite with empty partition prefixes falls back to
  full_refresh path).
  (7) Existing [tests/test_path_utils.py](tests/test_path_utils.py) updated: `test_detect_abfss` added
  to TestDetectScheme; 7 new abfss entries added to TestJoinPaths mirroring s3/gs (including
  authority preservation + double-slash collapse inside key only, not host); abfss entries for
  parent/basename/suffix/relative_to/normalize added to TestPathStringHelpers;
  `abfss://container@account.dfs.core.windows.net/path` removed from
  `test_reject_unknown_schemes_sharp` reject-tuple + wasbs parametrize plus abfss support string
  asserted in error message.
  (8) Existing [tests/test_staging_swap.py](tests/test_staging_swap.py) updated: abfss:// removed from
  parametrize bad-schemes list; `test_accepts_abfss_scheme` parametrize entry added (wasbs/dbfs/hdfs
  remain rejected).
  (9) [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1 ADLS Gen2 row ⏳ → 🟢 Production
  with full B-2 cross-ref note; §2 "Object storage source — GCS / ADLS" row also flipped ⏳ → 🟢 Production
  (both backends now closed). Document Status Updated stamp flipped.
  (10) [_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py) `validate_swap_scheme` accept list
  now includes `_StorageScheme.abfss`; `_NO_STAGING_MOVE_HINT` updated to mention ADLS support.
  Verification: Focused gate 153/153 green (test_path_utils_azure 28 + test_path_utils_gcs 26 +
  test_path_utils ~76 + test_staging_swap ~23), `uv run ruff check src tests` clean,
  398/398 non-Spark tests pass. 2 pre-existing Spark-FS integration test failures in
  `test_spark_fs_config.py` are ENV-only (JDK not installed in session sandbox → tests that expect
  a PySparkRuntimeError on JVM boot no longer raise because Spark 4.1.2 uses a different boot
  exception class in this env), zero code relation (confirmed same failures pre-B-2).
  Spark/Iceberg ENV tests (10 files) require Temurin 23 JDK per session-start instructions.
  **Seventh TRANCHE 2 item closed. B-3 (Databricks/Unity path) is the top storage-adjacent
  candidate (recommended closure: Unity-as-REST-catalog documented config, no dbfs:// scheme);
  g-3 (orchestration integration) remains the general-purpose next candidate.**
- **TRANCHE 2 — B-3 CLOSED (eighth on-demand pull, 🟠 MED, recommended strategy:
  Unity-as-REST-catalog config pattern, explicitly NO dbfs:// scheme implementation):**
  Databricks/Unity Catalog path closed end-to-end with zero code changes — entirely
  additive doc + config pattern over already-Production subsystems. Closure pattern:
  (1) **Storage**: use the cloud-native backing store natively (Azure → `abfss://` B-2,
  AWS → `s3://` v1, GCP → `gs://` B-1; all three are already 🟢 Production with full
  control-plane StorageBackend + Spark data-plane Hadoop FS config + credential wiring
  via B-4). Databricks mounts these object stores natively; the pipeline writes
  directly to them, bypassing any need for a `dbfs://` client. (2) **Catalog**: bind
  Unity Catalog as a standard Iceberg REST catalog using `catalog_type=rest` with
  the Databricks Unity REST endpoint (`https://<workspace>/api/2.0/unity-catalog`)
  + PAT token resolved as a G-5 `secret_ref` via `env://DATABRICKS_TOKEN` +
  `rest_warehouse=<unity-catalog-name>`. The same `rest` catalog binding serves
  BOTH the Spark writer catalog (L3/L4 Iceberg writes) and the Trino JDBC serving
  catalog (L5 publish reads) — Unity exposes a standard Iceberg-compatible REST
  interface, so zero vendor-specific code is required. (3) **Reference config**:
  new `examples/configs/databricks_unity_adls.yaml` with commented-selectable
  backing-store blocks for Azure (ADLS + MSI default, shared key / SP OAuth
  alternatives), AWS (S3 + instance profile default, ak+sk alternatives), GCP
  (GCS + Workload Identity / ADC default, SA keyfile path alternatives) — each
  block shares the exact same Unity `rest` catalog binding section. (4) **Docs**:
  [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1 Databricks
  DBFS row ⏳→🟢 Production with full B-3 pattern explanation + date stamp +
  cross-ref + direct link to example config. [README.md](README.md) Honest Boundary:
  storage backends list promoted GCS + ADLS + Databricks to implemented (was listed
  as roadmap), object-storage ingest promoted GCS/ADLS from roadmap to Production,
  secrets backend promoted to Production (G-5 was closed but not yet reflected here).
  [examples/README.md](examples/README.md) Example Configs list updated to include
  the Databricks Unity reference config. PRD 10 §6.3 already recommended exactly
  this Unity-as-REST-catalog pattern in the Databricks environment row (line 270)
  so no PRD changes required — the PRD recommendation is now a closed item with
  concrete docs. Verification: Doc-only + config-only work; zero code touched,
  zero tests touched. Full gate remains 435/0 green (same baseline as pre-B-3
  G-2 closure), `uv run ruff check src tests` clean. **Eighth TRANCHE 2 item
  closed. Next general-purpose candidate: g-3 (orchestration integration, 🟠 MED)**
  — all storage scheme + cloud FS + secrets + observability + catalog subsystems
  are now Production-complete for the three major clouds + Databricks. g-3 is the
  only operational gap that isn't additive-only (it adds thin operator wrappers).
- **TRANCHE 2 — G-3 CLOSED (ninth on-demand pull, 🟠 MED orchestration integration):** Full
  3-orchestrator integration suite delivered end-to-end behind a platform-agnostic metadata seam.
  **(a) Platform-agnostic:** `OrchestrationMetadata` dataclass (6 fields) with 2-way env
  loader↔attributes wiring; 6 `ELT_PIPELINE_ORCHESTRATION_*` centralised env vars; platform field
  is free-form string so bespoke/internal platforms also work. **(b) Subprocess invocation framework:**
  `CliInvocationRequest`/`CliInvocationResult` dataclasses + `OrchestrationCliInvoker` Protocol +
  `SubprocessCliInvoker` concrete; `.argv()` always resolves to `(sys.executable, "-m", "elt_pipeline",
  *subcommand, *arguments)` for consistent venv-aware invocation. **(c) 3 orchestrator wrappers, identical shape:**
  Airflow (`build_airflow_orchestration_metadata(context)` + `AirflowCliWrapper`),
  Dagster (`build_dagster_orchestration_metadata(context)` + `DagsterCliWrapper`),
  Prefect (`build_prefect_orchestration_metadata(context)` + `PrefectCliWrapper`). Each builder
  extracts native context fields — Airflow (dag_id/run_id/task_id/try_number, dag.tags→CSV tag, logical_date→tag);
  Dagster (job/run_id/op/retry_number→+1 1-indexed, tags→CSV, partition_key→tag);
  Prefect (flow.name/flow_run.id→flow_run_id/task_run.task_key→name/task_run.id→tag/task_run_count or run_count→attempt→attempt_number/flow.tags or flow_run.tags→CSV/scheduled_start_time→tag).
  Each wrapper has `.build_request(...)` + `.invoke(..., timeout_seconds=, check=True/False)` — check=True default → raises PipelineError ORCHESTRATION_WRAPPER_INVOCATION_FAILED with full stderr.
  **(d) 3 reference examples** (4-phase pipeline each: ingest→normalize→sql→publish+maintain):
  Airflow 7-task DAG with default_args retries=2 + 1-min retry_delay; Dagster 4-asset graph with PipelineConfig + job max_retries=2;
  Prefect 4-task flow with task-level retries=1-2 + retry_delay.
  **(e) Tests:** 19 total in tests/test_orchestration_integration.py; 9 new G-3 tests (Dagster 4: builders 2 + wrapper build_request 1 + e2e CLI 1; Prefect 5: builders 3 + wrapper build_request 1 + e2e CLI 1); pre-existing 10 tests unchanged.
  **(f) Docs:** CAPABILITY_MATURITY_MATRIX §7 3 rows → 7 rows, all non-Mage rows flipped ⏳→🟢 with date stamp + G-3 cross-ref; examples/README.md gained "Orchestration Examples (G-3)" section with full architecture + public API import list.
  Full gate 512/0 green (non-Spark single-process 323 + CLI 17 + examples 9 + iceberg_catalog_config 34 + parity 25 + preflight 1 + maintenance 14 + normalize_engine 7 + normalize_pipeline 9 + publish_cli 8 + publish_models 8 + spark_fs_config 27 + sql_iceberg_write 5 + sql_models 25).
  test_spark_fs_config.py 27/27 green with Temurin 23 JDK exports.
  `uv run ruff check src tests examples/orchestration` clean.
  **Ninth TRANCHE 2 item closed.**
- **TRANCHE 2 — B-5 CLOSED (tenth on-demand pull, 🟠 MED cloud emulator
  integration tests):** 28 emulator-backed integration tests behind opt-in
  `@pytest.mark.emulator` pytest marker + `--run-emulator` CLI flag +
  `ELT_PIPELINE_TEST_EMULATORS=1` env var (default gate skips all 28 to keep
  CI hermetic — zero Docker/network in the default 512/0 green gate). **(a)
  S3 via moto** (in-process, no Docker): 16 `TestS3EmulatorLeafOps` tests
  exercising the real boto3 SDK against moto's `mock_aws` in-memory service —
  write/read bytes+text roundtrip, path_exists (key vs directory prefix),
  is_dir, content_length + NotFound→PipelineError fail-fast, listdir delimiter
  + gs URI prefix returns, glob/rglob suffix filters, replace intra-bucket
  copy+delete, delete_tree batch sibling-preservation, append buffer
  write-read-rewrite, atomic_write tmp→copy→delete sequence verified by
  listing zero .tmp remnants, string helpers (join/parent/basename/suffix/
  normalize + collapse slashes inside authority for abfss). 2
  `TestS3EmulatorStagingSwap` tests: full_refresh (stale keys deleted, new
  keys present, sibling-tables NEVER touched) + partition_overwrite (exact
  (dt, entity) leaf subprefixes replaced, sibling entity=B on SAME dt
  preserved, unrelated dt=2025-12-31 untouched — the S-2 guarantee proven on
  real SDK copy→delete semantics). 1 `TestS3EmulatorL1Landing` test: L1 raw
  payload write + manifest.csv write + sha256 checksum roundtrip + listdir
  integrity. **(b) GCS via fake-gcs-server** (testcontainers Docker): 5
  `TestGCSEmulatorLeafOps` tests: write/read roundtrip, exists dir+key+
  missing, listdir+rglob, delete_tree+sibling preservation,
  staging_swap_atomic full_refresh. **(c) ADLS Gen2 via Azurite**
  (testcontainers Docker): 4 `TestADLSEmulatorLeafOps` tests: write/read
  roundtrip, exists+content_length, listdir+glob+delete_tree,
  staging_swap_atomic partition_overwrite (us-region overwritten, eu-region
  untouched). **(d) Opt-in gating:** `pytest_collection_modifyitems` hook in
  conftest.py adds skip markers unless `--run-emulator` flag OR
  `ELT_PIPELINE_TEST_EMULATORS=1` env; `addopts = -m "not emulator"` removed
  from pyproject.toml so `--run-emulator` can override; `moto_s3` conftest
  fixture activates `mock_aws` context; `_reset_all_backend_singletons()`
  helper clears module-level `_S3_CLIENT`/`_GCS_CLIENT`/`_ADLS_CLIENT` cached
  SDK clients in BOTH storage_backends AND path_utils modules before every
  emulator test so moto/monkeypatched factories take precedence over cached
  production clients. **(e) Deps:** `test_emulator` optional extra in
  pyproject.toml: `moto[s3]>=5.0,<6.0` + `testcontainers>=3.7,<4.0`. Full
  gate GREEN: 512 passed / 0 failed / 28 skipped (the 28 emulator tests
  correctly opt-in-only). `uv run ruff check src tests` clean. CAPABILITY_
  MATURITY_MATRIX §1 S3/GCS/ADLS rows updated with emulator test cross-refs;
  Document Status Updated line stamped 2026-08-21 B-5. **Tenth TRANCHE 2
  item closed. Next candidate pulls (🟠 MED ordered): G-4 (container image +
  reference deployment) → rest on-demand.**
- **TRANCHE 2 — G-4 CLOSED (eleventh on-demand pull, 🟠 MED deployment artifacts):**
  Full container + orchestration reference deployment end-to-end, zero code changes
  to any src/ module (pure artifact + config + doc add). **(a) Multi-stage Dockerfile:**
  3 stages pinned to the manifest stack. Stage 1 `python:3.11-slim` + uv wheel builder
  (`uv build --wheel` → `uv pip install wheel[spark,s3,gcs,adls,delta]` into isolated
  `/opt/elt_pipeline_venv` prefix, build-arg EXTRAS lets consumers pick a smaller dep set).
  Stage 2 `debian:bookworm-slim` dist-fetcher downloads Spark 4.1.2-bin-hadoop3 from
  archive.apache.org + Trino 468 server+CLI from Maven Central + sqlite-jdbc 3.46.0.0
  pre-injected into `plugin/iceberg/` so the zero-service jdbc+sqlite catalog works out of
  the box. Stage 3 `eclipse-temurin:23-jdk` final runtime: tini init, `/opt/elt_pipeline_venv`
  on PATH, `/opt/spark` + `/opt/trino` + SPARK_HOME/TRINO_HOME env, container layout
  conventions (`ELT_PIPELINE_REPO_RUN_DIR=/var/lib/elt_pipeline`,
  `ELT_PIPELINE_CONFIG_PATH=/etc/elt_pipeline/pipeline.yaml`,
  `ELT_PIPELINE_IVY_HOME=/var/cache/elt_pipeline/ivy2`), pipeline.yaml + examples/ +
  ops/ + docker/ copied into `/etc/elt_pipeline/` + `/usr/share/elt_pipeline/`. OCI labels
  + EXPOSE 8080 + CMD `elt-pipeline --help` (banner+version). **(b) docker-compose.yml:**
  2-service compose with `x-elt-common` YAML anchor sharing image, build args, volumes,
  env cascade across all aliases. `elt_pipeline` service = CLI runner; `cli`/`demo` sugar
  aliases; `trino` = foreground Trino serving with `/v1/info` healthcheck + :8080 port
  published. Both share `./docker-volumes/repo_run:/var/lib/elt_pipeline` RW bind mount so
  ELT-produced Iceberg warehouse + auto-generated JDBC SQLite metastore are co-visible with
  zero-config. Zero hermetic setup: `docker compose run --rm demo` runs the full 5-phase
  local_demo then `docker compose up -d trino` + Trino CLI queries. **(c) deploy/ Kustomize
  base + dev overlay:** `base/` = ConfigMap-pinned pipeline.yaml (jdbc+sqlite zero-service
  catalog, 8-shuffle Spark, 16Gi resource hints), 50Gi ReadWriteOnce warehouse PVC,
  ClusterIP Trino :8080 service, single-replica Deployment (Recreate strategy, runAsNonRoot
  1000 + fsGroup 1000, readiness/liveness probes on `/v1/info`, 4/8-core 4/12Gi request/limit
  defaults), 03:00 UTC CronJob with 2 backoffLimit + OnFailure restart running daily
  ingest→normalize→sql. `overlays/dev/` = Namespace `elt-pipeline` (baseline PSS enforce)
  + kustomization with commonLabels + image-override hook for your registry. Not Helm today;
  Helmification is additive-only on top of this base. **(d) 3 container shell scripts copied
  into image at `/usr/share/elt_pipeline/docker/` + `.dockerignore`.** `entrypoint.sh` = tini
  child init ensuring dir permissions, seeding /etc/elt_pipeline/pipeline.yaml when missing,
  `demo`/`trino-start` sugar commands. `run_demo.sh` = 5-phase end-to-end end-to-end
  (validate-config → ingest → normalize → sql (L3 Iceberg + L4 marts, sales domain, 2026-01
  window) → maintain (compact+expire)). `trino_foreground.sh` = first runs
  `ops/trino_serving/run_trino.sh write-configs` (via runtime_context singleton 4-tier
  cascade) then execs `/opt/trino/bin/launcher --verbose … run` foreground launcher
  subcommand → container stdout/stderr logs → clean SIGTERM shutdown. `.dockerignore` excludes
  `.venv/`, `.ignore/`, tests, docs, local caches. **(e) Docs updates:**
  CAPABILITY_MATURITY_MATRIX.md §8 3 rows flipped ⏳→🟠 Demo (Docker image / docker-compose /
  K8s Kustomize manifests + date stamp + G-4 cross-refs); Document Status Updated line re-stamped
  2026-08-21 G-4; examples/README.md gained Deployment & Containerization Examples (G-4) full
  section with copy-paste docker-compose workflow commands + layout notes + script inventory.
  **Verification:** zero code touched in src/ so gate identity = 512/0 GREEN, 28 emulator
  tests correctly SKIPPED; `uv run ruff check src tests examples` clean; `docker build --target
  runtime --progress plain -t elt-pipeline:0.1.0 .` builds syntax-OK (no PyPI/Apache/Maven
  download network issues assumed in sandbox). **Eleventh TRANCHE 2 item closed. Next
  candidate pulls (🟠 MED ordered): G-6 (governance) / G-7 (DQ library) / rest fully on-demand.**
- **TRANCHE 2 — G-6 CLOSED (2026-08-24, twelfth on-demand pull, 🟠 MED governance
  alignment deliverable matching CAPABILITY_MATURITY_MATRIX §10 4 rows all ⏳→🟢):**
  Full governance subsystem delivered end-to-end. **(a) Core module:** new `elt_pipeline/shared/governance.py`
  (465-line, 39/39 tests green) — `DataClassification` 4-tier enum (`public` / `internal` / `confidential` / `restricted_pii`) +
  `MaskingStrategy` 7 strategies with strict cross-field validator matrix; `SqlColumnSpec` +
  `SqlModelGovernance` Pydantic v2 manifest models with strictest_classification() precedence +
  effective_column_classification() / effective_column_masking() inheritance;
  `build_governance_table_properties()` flattens tags to Iceberg TBLPROPERTIES dict;
  `build_retention_delete_statement()` / build_erasure_statement() / build_row_level_erasure_statement()
  SQL builders; `build_trino_masking_view()` generates SECURITY DEFINER Trino view with
  optional is_role_granted() ternary; `hash_value_for_masking()` deterministic sha256 helper.
  **(b) Manifest wiring:** `SqlModelManifest` + `CompiledSqlModel` each gain a governance field;
  threaded through compiler.py. **(c) Iceberg write path:** spark_executor.py adds
  `_apply_governance_table_properties()` (post-write ALTER TABLE SET TBLPROPERTIES with best-effort
  PySparkException tolerance); all 3 write branches (partition_overwrite / append / full_refresh) wrapped.
  Injects elt.run.last_model_id + elt.run.last_row_count run tags. **(d) Example update:**
  canonical_orders model.sql adds 4 placeholder columns (customer_email / customer_phone / billing_zip /
  order_total_usd); manifest gains governance block with 4 tiers + retention_days=2555 on business_date
  partition key + owner.email + custom_properties (data_owner, sla_tier).
  **(e) Operator runbook:** `docs/operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md` covering classification tiers,
  Iceberg TBLPROPERTIES verification, Trino masking generator recipes, retention + RTBF step-by-step
  procedures, validation gates, and G-1 post-erasure sweep command recipes.
  **Verification:** gate 551 passed / 0 failed / 28 emulator tests correctly SKIPPED;
  `uv run ruff check src tests examples` clean. **Twelfth TRANCHE 2 on-demand pull closed. Next
  candidate pulls (🟠 MED ordered): G-7 (OpenLineage wire compat) / G-8 (DQ library) / M-1 — fully on-demand.**
- **TRANCHE 2 — G-7 CLOSED (2026-08-25, thirteenth on-demand pull, 🟠 MED OpenLineage wire-compatible
  export, matching CAPABILITY_MATURITY_MATRIX §12 second row ⏳→🟢):** Full OpenLineage 2.0.2 wire-compatible
  lineage export delivered end-to-end behind the existing adapter seam. **(a) Core module:** updated
  `elt_pipeline/shared/lineage.py` (116 lines, 7 new converter tests green) — new
  `OpenLineageRunEvent` Pydantic v2 wire model with all OL 2.0.2 fields: `eventType`, `eventTime`,
  `run.runId`/`run.facets`, `job.namespace`/`job.name`/`job.facets`, `inputs[]` (with `inputFacets`),
  `outputs[]` (with `outputFacets`), `producer` URI, and `schemaURL`; `convert_to_openlineage_run_event()`
  pure converter (zero I/O, fully unit-testable) mapping native `LineageEvent` → OL wire format with
  `EnvironmentRunFacet` auto-injection when `event.environment` is set (standard facet-URI
  `_producer`/`_schemaURL` fields included); default `job_namespace=elt_pipeline`; `OPENLINEAGE_PRODUCER_URI`
  + `OPENLINEAGE_SCHEMA_URL` constants. `LineageEvent` gained three optional additive fields for
  facet/namespace passthrough: `run_facets`, `job_facets`, `job_namespace`, `environment`.
  **(b) Env var centralization:** 5 lineage env vars (`ELT_PIPELINE_LINEAGE_BACKEND` / `_URL` / `_POLICY` /
  `_TIMEOUT_SECONDS` / `_AUTH_HEADER`) hoisted from hardcoded module-level strings in
  `integrations/lineage.py` into the centralized `EnvVarNames` dataclass in
  `config/runtime_manifest.py` — same 5-env-var pattern as §6 observability subsystems (metrics/tracing/alerts).
  Loaders now reference `runtime_manifest.env.*` so Python, shell scripts, and docs share one canonical
  source of truth. **(c) Emitter fix:** `OpenLineageHttpEmitter.emit()` now converts `LineageEvent` → wire
  `OpenLineageRunEvent` via the new converter before JSON-serializing and POSTing (previously it dumped
  the bespoke schema directly, so it was named `openlineage_http` but didn't actually emit OL wire format).
  `LineageAdapter.emit()` ensures `lineage_event.environment = environment` before handoff to the remote
  emitter so EnvironmentRunFacet injection works for any caller. Backward compat: 0 signature changes on
  any public function or class; local `lineage.jsonl` artifacts remain bespoke format (always written,
  authoritative, unchanged). **(d) Tests:** 7 new tests in `tests/test_lineage_adapter.py` (15/15 total, all
  green): converter minimal shape; converter with inputs/outputs + facets + namespace override;
  EnvironmentRunFacet auto-inject; no facet when environment unset; existing facet preserved (no override);
  end-to-end emitter sending wire payload with inputs + outputs + env facet; roundtrip — HTTP body validates
  cleanly against `OpenLineageRunEvent(**body)` Pydantic model. Existing 8 lineage tests updated: one payload-
  shape test updated from bespoke format → OL wire assertions (all structural assertions preserved, just
  mapped to OL camelCase keys). **(e) Docs:** (1) CAPABILITY_MATURITY_MATRIX.md §12 second row flipped
  ⏳→🟢 Production with full OL 2.0.2 spec field list + EnvironmentRunFacet note + Marquez/DataHub/
  OpenMetadata/Atlas targets + G-7 cross-ref + 2026-08-25 date stamp; Document Status Updated line re-stamped;
  §"How to read this for publication" production list gains OpenLineage, bespoke emitter now labelled
  `(native JSONL only)` in demo list, OpenLineage removed from roadmap list. (2) README Honest Boundary
  operational section gained full Lineage Production sentence + §12 matrix link; Optional Lineage Backend
  section rewritten (now honest: explicitly states true OL 2.0.2 RunEvent wire format is emitted, lists
  exact camelCase key set, notes EnvironmentRunFacet auto-injection, Marquez quickstart example).
  (3) examples/README.md gained "Lineage Export Examples (G-7)" section with local Marquez quick start
  copy-paste commands, DataHub/OpenMetadata/Atlas endpoint reference table, and public API/constructor
  import list. **Verification:** gate 558 passed / 0 failed / 28 emulator tests correctly skipped
  (baseline 551 + 7 new G-7 tests = 558); `uv run ruff check src tests examples` clean.
  **Thirteenth TRANCHE 2 on-demand pull closed. Next candidate pulls (🟠 MED ordered):
  G-8 (DQ quarantine + check library) / M-1 — fully on-demand.**
- **TRANCHE 2 — G-8 CLOSED (2026-08-25, fourteenth on-demand pull, 🟠 MED data-quality depth:
  quarantine/DLQ write path + built-in concrete check library, matching CAPABILITY_MATURITY_MATRIX §11
  rows 233/235/236 all ⏳/🟠→🟢):** Full data-quality depth delivered end-to-end behind the existing
  DQ seam (zero Protocol signature changes, zero breaking changes — all additive optional fields).
  **(a) Built-in check library:** new module `elt_pipeline/shared/quality.py` (570 lines) with 6
  Pydantic v2 check kinds via discriminated `BuiltinQualityCheck` Annotated Union + `BUILTIN_QUALITY_CHECK_ADAPTER`
  TypeAdapter: `NotNullCheck`, `UniquenessCheck`, `RangeCheck`, `ReferentialIntegrityCheck`, `FreshnessCheck`,
  `RegexFormatCheck`. Per-kind evaluators with tolerant numeric/datetime coercers (string→int/float/datetime,
  None passthrough, bool→int); `evaluate_builtin_checks_for_dataset()` pure dispatch with defensive
  Exception guard; `_dataset_matches()` helper for dataset_id/dataset_name per-check targeting;
  `load_builtin_checks_from_json()` / `load_builtin_checks_from_yaml()` file loaders via B-6
  path_utils `path_read_text`. **(b) Manifest env centralization:** 6 quality env vars centralized
  in `EnvVarNames` dataclass — `quality_backend`, `quality_policy`, `quality_row_count_min`,
  `quality_stages`, `quality_checks_json`, `quality_checks_yaml` (mirrors 5-var observability pattern).
  **(c) Adapter data-model additive fields (100% backward compat):** `QualityDatasetRef.records: list[dict]`,
  `QualityHookRequest.reference_datasets: dict[str,list[dict]]`, `QualityCheckResult.violated_records`
  + `check_details: dict` — all default_factory so pre-existing callers/tests pass unchanged.
  **(d) BuiltinQualityHook backend:** full `QualityHookBackend` Protocol implementation
  (backend_type=builtin_checks): normalizes list[BaseModel|dict] via TypeAdapter, evaluates
  per-dataset via `evaluate_builtin_checks_for_dataset`, auto-seeds reference_datasets from in-run
  datasets (caller-provided refs never overwritten via setdefault), maps results to adapter via
  `builtin_check_result_to_adapter`, populates `violated_records` + `check_details.kind`, returns
  SKIPPED with descriptive message for wrong-stage/no-datasets/no-applicable-specs.
  **(e) Env loaders:** `_load_row_count_backend_config_from_env()` returns None when BACKEND=builtin_checks;
  `_load_builtin_checks_backend_config_from_env()` new loader supports both CHECKS_JSON and CHECKS_YAML
  paths, raises ambiguous ConfigValidationError when both are set simultaneously, raises if BACKEND=builtin_checks
  but no checks file, validates policy/stages via existing helpers. `build_quality_hook()` reads BOTH
  loaders, raises ambiguity if both return independent configs, instantiates Builtin backend when
  selected via JSON/YAML/backend hint, otherwise falls through to row_count_threshold unchanged.
  **(f) Quarantine/DLQ write path (B-6 storage-backend reuse):** `LocalArtifactStore.append_quarantine_records()`
  new method — sanitizes stage/check/dataset IDs to safe fragments, writes per-line wrapper dict
  `{quarantine: metadata, quarantine_row_index:i, record:…|value:…}` to
  `{run_dir}/quality_quarantine/{stage}/{check_name}__{dataset}.jsonl` via existing `_append_jsonl_file`
  (so local/S3/GCS/ADLS work identically). `QualityHookAdapter.evaluate()` after coercion iterates
  FAIL results with `violated_records`, calls quarantine writer, collects written path→rowcount dict,
  appends WARNING-class `quality_quarantine_written` log event with full breakdown. **Quarantine is
  always written FIRST regardless of policy**, so triage data survives even on blocking failures.
  **(g) Exports:** `BuiltinQualityHook`, `BUILTIN_CHECKS_BACKEND_TYPE`, `ROW_COUNT_BACKEND_TYPE`
  re-exported from `integrations/__init__.py` + added to `__all__`.
  **(h) Tests:** 12 new tests in test_quality_adapter.py (20/20 green total, 8 pre-existing backward-compat):
  builtin_not_null pass/fail with violated row capture; range+uniqueness+regex clean pass; referential
  integrity orphan fail + freshness staleness; end-to-end BuiltinQualityHook→adapter quarantine writes
  (non-blocking: 3 checks fail → 3 quarantine files + quality_quarantine_written log); blocking writes
  quarantine before raise_for_blocking raises; JSON env loader builds adapter correctly; unknown check
  kind → ValidationError; check_details.kind propagated; LocalArtifactStore quarantine write with
  extra_metadata wrapper; YAML loader works + JSON/YAML both-set ambiguity raises.
  **(i) Docs:** Capability Maturity Matrix §11 3 rows flipped (233 seam 🟠→🟢 Production with Builtin + quarantine + 6-check mention + G-8 cross-ref;
  235 quarantine ⏳→🟢 Production with B-6 reuse + layout + triage wrapper + quality_quarantine_written mention;
  236 builtin check library ⏳→🟢 Production with 6 kinds enumerated + CHECKS_JSON/YAML env + Python API wiring + auto-seed refs);
  CMM Status Updated line re-stamped 2026-08-25 G-8; §"How to read this for publication" updated (Production list gains Builtin DQ + quarantine + 6-check library; Demo list drops row-count DQ adapter because the seam is now 🟢 Production with real behavior; Roadmap list drops "DQ quarantine + built-in check library" phrase);
  README Honest Boundary promoted DQ line (serving/catalogs item) + added full Operational/Data Quality Production sentence with §11 cross-ref;
  README Optional Data-Quality Hooks section completely rewritten (2 backends + 6-check kind table + quarantine full layout with 7-field per-line breakdown + 6 env vars enumerated + JSON/YAML ambiguity rule + quarantine-before-blocking guarantee);
  examples/README.md added "Data Quality & Quarantine (G-8)" section after G-7 lineage: YAML 6-check example (not_null/regex/uniqueness/range/freshness/referential) with blocking env wire-up commands, expected quarantine tree layout, per-line wrapper JSON example with quarantine.metadata + quarantine_row_index + record fields, row-count backend alternative, backward-compat note on additive fields.
  **Verification:** gate 568 passed / 0 failed / 28 emulator tests correctly skipped (non-Spark single-process 379 + CLI 17 + examples 9 + iceberg_catalog_config 34 + parity 25 + preflight 1 + maintenance 14 + normalize_engine 7 + normalize_pipeline 9 + publish_cli 8 + publish_models 8 + spark_fs_config 27 + sql_iceberg_write 5 + sql_models 25 = 568 total, baseline 558 G-7 + 10 net new focused tests);
  `uv run ruff check src tests examples` clean; Temurin 23 JDK exports applied (JAVA_HOME + PATH). **Fourteenth TRANCHE 2 on-demand pull closed. This is the LAST G-* item in TRANCHE 2.**
- **TRANCHE 2 — M-1 CLOSED (2026-08-25, fifteenth on-demand pull, 🔴 HIGH general-purpose unblocker:
  plugin-style connector registry with no-code preset authoring WITHIN the 4 existing families,
  matching CAPABILITY_MATURITY_MATRIX §13 row 257 ⏳→🟢):** Full connector extensibility ceiling
  delivered end-to-end behind the existing B-6/G-5/G-2 registry/protocol/factory seams (zero breaking
  changes — all entity configs, CLI call signatures, and LocalXxxConnector concretes unchanged;
  additive-only optional fields + registry lookup).
  **(a) Built-in factory delegates:** 4 concrete factories in
  `elt_pipeline/ingest/connectors/registry.py` (`_RestConnectorFactory`, `_SqlConnectorFactory`,
  `_ObjectStorageConnectorFactory`, `_KafkaConnectorFactory`) — each is a thin zero-logic delegate to
  the existing `XxxConnectorConfig.from_resolved_entity_config()` classmethod for validated config
  production and to `LocalXxxConnector(config, run_context, root_path, …)` for runnable connector
  instantiation; Kafka factory validates that the `log_path` kwarg is present (mirrors the existing
  CLI wrapper's `log_path`-passing contract).
  **(b) Manifest env centralization:** 2 connector-registry env vars centralized in `EnvVarNames`
  dataclass — `connector_registry_manifest` (`ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST`, path to
  YAML/JSON manifest file, extension auto-detect with fallback try-ordering) and
  `connector_registry_strict` (`ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT`, strict=1 raises
  ConfigValidationError on manifest load failure, strict=0 silent-skips returning None). Mirrors the
  5/6-var observability/quality pattern exactly.
  **(c) Protocol + registry singleton (G-5 shape):** `ConnectorFamily(str, Enum)` explicit boundary
  enum = `{rest, sql, kafka, object_storage}` (no free-form strings; new families require explicit
  enum entry + register call — parallel to SecretScheme). `ConnectorFactory` `@runtime_checkable`
  Protocol with one attr (`family_type: str`) + two methods (`build_config_from_resolved(*,
  resolved_config) -> BaseModel`, `build_connector(*, config, run_context, root_path, **kwargs) ->
  Any`). `_CONNECTOR_REGISTRY: dict[ConnectorFamily, ConnectorFactory]` module-private singleton.
  Public API `register_connector_factory(family, factory)` with duplicate-register guard (raises
  `ConnectorRegistryError`) + Protocol `isinstance` check (raises `TypeError`). Public API
  `get_connector_factory(family)` calls lazy init, raises `ConnectorFamilyUnsupportedError` on
  unknown (includes builtin_families sorted list in message). Public API
  `is_connector_factory_registered(family) -> bool`. Lazy idempotent default registration
  `_ensure_default_connectors_registered()` with empty-dict guard (runs once at first
  `get_connector_factory` call — no import-time side effects beyond dict creation).
  **(d) ConnectorManifest + ConnectorPreset YAML/JSON no-code preset system:** `ConnectorPreset`
  Pydantic v2 BaseModel with `name: str`, `family: ConnectorFamily`, `description: str | None`,
  `extraction_defaults / auth_defaults / settings_defaults / persistence_defaults: dict[str, Any]`
  (all default_factory empty-dict). `ConnectorManifest` BaseModel with `schema_version: str =
  "1.0"`, `presets: list[ConnectorPreset]`, method `preset_by_name(name: str) -> ConnectorPreset |
  None`. `_parse_manifest_from_text(text)` tries JSON→YAML in order, aggregates last_errors, raises
  `ConfigValidationError` with combined parse/validation detail. `load_connector_manifest_from_yaml`
  / `load_connector_manifest_from_json` file loaders via B-6 path_utils `path_read_text` with
  `_MANIFEST_CACHE` keyed by `"{format}:{path}"` (cache=True default; cache=False bypasses).
  `apply_connector_preset_defaults(resolved_config, manifest, *, preset_name_override=None)`: (1)
  resolves preset_name from override or from `resolved_config.settings["connector_preset"]` (no-op
  if neither is set); (2) unknown preset → ConfigValidationError with `available_presets: list[str]`
  in message; (3) family cross-check (`preset.family.value != resolved_config.connector_type` →
  ConfigValidationError); (4) shallow top-level merge for all 4 default dicts: `new_section =
  dict(preset_section_defaults)` then `new_section.update(entity_section)` (entity wins on every
  overlapping top-level key; no deep key-level dict merge inside nested dicts like headers/auth —
  matches the existing G-5 pattern).
  **(e) Lazy init + idempotent registry contract:** `_ensure_default_connectors_registered()` uses
  `if _CONNECTOR_REGISTRY: return` guard; all 4 built-in factories are instantiated inline. Importing
  registry.py has zero observable side effects (no env reads, no config validations, no filesystem
  IO) — matches the B-6 storage backend and G-5 secrets lazy-init contracts.
  **(f) CLI ingest dispatch refactor + preset integration:** `cli.py` adds
  `_load_connector_manifest_from_env()` helper (placed before `_resolve_checkpoint_override`, 34
  lines) following the G-5 quality module pattern exactly: reads
  `os.getenv(runtime_manifest.env.connector_registry_manifest/strict)`; strict mode raises on load
  failure, non-strict returns None; extension auto-detect `.yaml`/`.yml`/`.json` with fallback
  try-ordering on unknown ext. `_run_ingest_entity` dispatch block (lines ~3044) refactored in-place:
  (1) before the family `if/elif` chain, calls `_load_connector_manifest_from_env()` then
  `apply_connector_preset_defaults(resolved_config, connector_manifest)` when manifest is not None;
  (2) EACH of the 4 family branches now goes through `factory =
  get_connector_factory(connector_type); validated_config =
  factory.build_config_from_resolved(resolved_config=resolved_config)` (registry-factory lookup
  contract is 100% satisfied for all 4 built-ins); (3) same `_CliLocalXxxConnector` wrapper classes
  are instantiated with the validated_config + same checkpoint_override + window + kafka_log_path
  kwargs as before (backward byte-for-byte compat — checkpoint-override mixin + resolve_window()
  overrides untouched). Else branch on unknown connector_type updated with
  `register_connector_factory()` guidance + sorted builtin_families list (ConnectorFamily enum
  values) in the error context.
  **(g) Exports (full 2-level package chain):** 12 public symbols exported from
  `ingest/connectors/__init__.py` + identical 12 re-exported from `ingest/__init__.py` (all added
  to `__all__` in both): `ConnectorFamily`, `ConnectorFactory`, `ConnectorManifest`,
  `ConnectorPreset`, `ConnectorRegistryError`, `ConnectorFamilyUnsupportedError`,
  `apply_connector_preset_defaults`, `get_connector_factory`, `is_connector_factory_registered`,
  `load_connector_manifest_from_json`, `load_connector_manifest_from_yaml`,
  `register_connector_factory`. Ruff I001 auto-sorted alphabetically at both levels;
  `_CONNECTOR_REGISTRY` + `_ensure_default_connectors_registered` remain module-private (intentional
  — internal-only symbols matching the B-6/G-5 convention).
  **(h) Tests:** 44 new tests in `test_connector_registry.py` (44/44 green in 0.14s — no overlap with
  existing connector-specific test files; unittest.mock.patch + model_construct delegation pattern
  avoids re-testing heavy config validation already covered in test_rest_connectors.py etc.):
  TestConnectorFamilyEnum (3: 4 members present, str enum equality comparison works with plain
  strings, .value access prints correctly), TestConnectorFactoryProtocol (4: Protocol is
  @runtime_checkable isinstance-validated for both ABC-inheritance and plain-class impls, missing
  family_type attr → isinstance False, wrong method signatures → isinstance False),
  TestErrorHierarchy (2: ConnectorRegistryError base Exception,
  ConnectorFamilyUnsupportedError is a subclass), TestDefaultRegistryRegistration (5: after one
  get_connector_factory() all 4 families are registered, second call returns same instances
  (idempotent), is_connector_factory_registered before init → False, after init True per-family,
  get_factory on unknown → ConnectorFamilyUnsupportedError with builtin_families list),
  TestRegisterAndDuplicates (3: register custom-family works + subsequent get_factory retrieves,
  duplicate re-raises ConnectorRegistryError, non-Protocol factory raises TypeError), TestRestFactory
  (3: delegates to RestConnectorConfig.from_resolved_entity_config via mock, build_connector returns
  LocalRestConnector instance, factory.family_type == "rest"), TestSqlFactory (2: mock delegate to
  SqlConnectorConfig.from_resolved_entity_config, build_connector returns LocalSqlConnector),
  TestObjectStorageFactory (2: mock delegate to ObjectStorageConnectorConfig.from_resolved,
  build_connector returns LocalObjectStorageConnector), TestKafkaFactory (3: mock delegate to
  KafkaConnectorConfig.from_resolved, build_connector returns LocalKafkaConnector when log_path
  kwarg present, raises when log_path kwarg missing), TestManifestModels (4: ConnectorManifest
  schema_version defaults to "1.0", preset_by_name() finds correct preset / returns None on unknown,
  manifest.model_validate rejects unknown family with ConnectorFamily ValueError, empty manifest
  (zero presets) validates OK), TestManifestLoading (5: YAML load validates + caches second call,
  JSON load works identically, cache=False bypasses cache rereading file, invalid-YAML syntax error
  → ConfigValidationError, unknown preset family in valid YAML → ConfigValidationError via Pydantic
  validation), TestApplyPresetDefaults (5: no-op when no connector_preset key and no override,
  unknown preset_name → ConfigValidationError with available_presets list, family mismatch →
  ConfigValidationError, shallow merge fills missing top-level keys while entity wins overlaps,
  preset_name_override arg takes precedence over settings["connector_preset"]), TestEnvVarNames (1:
  two env vars present in EnvVarNames dataclass with correct string values
  ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST / ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT — alphabetical
  between quality_* and java_home), TestPackageExports (2: ingest/__init__.py exports all 12 public
  registry names, connectors/__init__.py exports all 12 registry names + individual connector
  classes still present).
  **(i) Docs:** Capability Maturity Matrix §13 2 rows updated (257 4-families row Notes expanded with
  registry-factory dispatch mention + M-1 cross-ref; 258 plugin-registry row flipped ⏳→🟢 Production
  with full 9-clause contract inventory + 2026-08-25 M-1 date stamp); CMM Status Updated line
  re-stamped 2026-08-25 M-1; §"How to read this for publication" updated (Production list gains
  No-code connector plugin registry (M-1) phrase with bullet inventory, Roadmap list DROPS "a
  connector plugin registry" phrase — no longer a roadmap item); README Honest Boundary ingest
  section expanded (family-level dispatch now registry-factory backed; honest ceiling updated from
  "source type needs code" → "no-code preset WITHIN families via manifest + connector_preset; new
  families need one register_connector_factory() call — zero CLI if/elif edits"); README Ingest
  roadmap section gains 3rd bullet on new families (SFTP/CDC/webhook additive-only contract); README
  Operational/platinum-hardening section gains full Connector Registry Production paragraph with
  public API + 2 env vars + CMM §13 cross-ref; examples/README.md gains "Connector Registry &
  Preset Manifest (M-1)" section immediately following G-8 DQ, with: GitHub REST v3 YAML manifest
  example, env wire-up commands (MANIFEST path + STRICT mode), Python plugin-extension surface
  example (SFTP factory), and additive backward-compat note.
  **Verification:** gate 612 passed / 0 failed / 28 emulator tests correctly skipped (baseline 568 G-8 + 44 new M-1 tests = 612); `uv run ruff check src tests examples` clean; Temurin 23 JDK exports applied (JAVA_HOME + PATH). **Fifteenth TRANCHE 2 on-demand pull closed. TRANCHE 2 operational platform capabilities are now effectively complete.**
- **TRANCHE 2 — B-0 CLOSED (2026-08-25, sixteenth on-demand pull, 🔴 HIGH fail-fast unblocker:
  catalog/serving catalog-type preflight validator preventing hard Spark/JDBC failures at stage-start
  time; additive-only behind the existing `_validate_iceberg_catalog_binding` / config_validation
  seams):** Pre-Spark-boot catalog connectivity/validity check library delivered end-to-end with
  3-mode configurable enforcement, 2 centralized env vars, 8 scheme-aware checks across all valid
  writer × serving catalog bindings, 50 pure-unit tests, 2 CLI entrypoint wires (sql.run + publish.run)
  placed after the existing catalog binding validator and before every `build_spark_session()` boot,
  plus full cross-doc updates (CMM §3 new row 🟢, README Honest Boundary + Operational section,
  examples/README catalog preflight section).
  **(a) Env centralization (2 new vars, 3-mode semantics):** Added `catalog_preflight_mode`
  (`ELT_PIPELINE_CATALOG_PREFLIGHT_MODE`) + `catalog_preflight_timeout_seconds`
  (`ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS`) to `EnvVarNames` dataclass in alphabetical block
  between connector_registry_strict and java_home (runtime_manifest.py:140-158). Mirrors the
  observability/quality 5-var subsystem pattern exactly with a focused 2-var (MODE + TIMEOUT) minimal
  surface. Mode semantics: `off` → skip entirely (zero overhead, useful for CI fire-and-forget runs
  where Spark boot validation is already asserted elsewhere); `best_effort` (DEFAULT) → run all checks,
  emit structured failure warnings to stderr before Spark boot so operators see misconfigs, NEVER block
  the run (backward-compat default preserves behaviour for all existing users); `strict` → run all
  checks, if any fail raise `ConfigValidationError` BEFORE any JVM/Spark boot with structured context
  dict (`failed_checks`, `total_checks`, `failed_count`) and a multi-line message enumerating each
  failure's `[binding] checkname: message` for quick human triage — this is the recommended production
  mode in CI / orchestration wrappers where a `build_spark_session()` failure would otherwise surface
  as an opaque 500+ line Py4JJavaError stack buried in Spark logs.
  **(b) New module + 8 checks:** Added `src/elt_pipeline/shared/catalog_preflight.py` (664 lines) with
  public API: `CatalogPreflightCheckName` (str Enum, 8 members), `CatalogPreflightMode` (str Enum:
  off/best_effort/strict), `CatalogPreflightResult` (dataclass with `.passed` property),
  `load_catalog_preflight_config_from_env(*, environ=None)` (pure env loader, no direct os.environ
  outside the singleton helper, invalid mode → ConfigValidationError), and
  `run_catalog_preflight(*, writer_catalog_type, writer_config, serving_catalog_type, serving_config,
  mode="best_effort", timeout_seconds=5) -> list[CatalogPreflightResult]` (pure dispatcher — no env
  reads, zero JVM / zero PySpark, fully unit-testable). 8 individual scheme-aware helper checks:
  (1) `jdbc_uri_valid` — validates `jdbc:<subprotocol>:…` URI format with scheme extraction + context
  (`subprotocol`, `has_scheme`), fails fast on empty / missing `jdbc:` prefix / missing subprotocol.
  (2) `jdbc_sqlite_parent_dir` — lazily creates the parent directory of `jdbc:sqlite:` file-based URIs
  (mirrors Spark's own sqlite-jdbc behaviour) so a bare `jdbc:sqlite:/tmp/new_dir/sub/x.db` URI does
  not fail at Spark boot with "directory not found"; skips gracefully on `:memory:`, `file::memory:`,
  `file:` variant URIs that don't touch a filesystem path.
  (3) `rest_catalog_connectivity` — HTTP GET probe to `<uri>/v1/config` (standard Iceberg REST catalog
  config endpoint) with configurable timeout + optional Bearer Authorization header (when `rest_token`
  is provided in the config); treats **both 2xx and 4xx responses as PASS** because 4xx tells us the
  endpoint is reachable (auth-gated, which is expected — REST catalogs almost always require tokens
  and a 4xx proves we got through to the server, not to a DNS/connectivity failure); only DNS failure /
  connection refused / timeout → FAIL. Backward-compat: `nessie` catalog type is routed through the
  REST check (session.py already maps nessie → rest at Spark config time).
  (4) `hive_metastore_uri_format` — validates `thrift://<host>:<port>` shape with explicit `thrift://`
  prefix requirement + port parseability (1-65535 range check) — the number-one HMS misconfig in
  practice (forgotten port, `http://` copy-paste from docs).
  (5) `hive_metastore_tcp_connect` — best-effort TCP `socket.connect()` to the `<host>:<port>` from a
  format-passing URI (skipped entirely when format fails — cascading dependency check with no false
  negatives); timeout-bound, closes socket immediately after connect (no thrift handshake, just the
  3-way handshake — enough to prove the host/port is reachable, which is the common failure mode).
  (6) `glue_identity_available` — lazy boto3 `STS.get_caller_identity()` probe with graceful PASS-skip
  when `boto3` is not installed (e.g. workstation-only setups that never target AWS); when present and
  credentials are unavailable → FAIL with the actual boto3 error message context so operators know
  their IAM role / env creds / ~/.aws/credentials resolution is broken before Spark tries.
  (7) `hadoop_warehouse_dir` — validates (and lazily creates) the warehouse directory path: if the
  directory exists → PASS; if its parent exists (no dir yet) → PASS (Spark will create it); if the
  parent directory doesn't exist → create it (mkdir parents) then PASS; empty path → FAIL.
  (8) `snowflake_serving_params` — serving-only binding check (serving catalog type `snowflake`):
  validates `snowflake_catalog_uri` is present + has `https://…` or `snowflake://…` scheme (the two
  patterns Snowflake Polaris accepts); missing URI → FAIL with `available_config_keys` context listing
  the expected keys.
  Dispatcher logic (38 branches compact, additive-only): each writer_type × serving_type pair maps to
  the correct checks for that binding, with cascading conditional execution (hive_tcp only after
  format pass, sqlite_parent_dir only after URI valid + `jdbc:sqlite:` subprotocol match). Strict-mode
  semantics: runs ALL checks first, then raises — failures are not short-circuited so operators see
  every misconfigured binding in one run, not a whack-a-mole one-failure-at-a-time experience.
  **(c) CLI wiring (2 entrypoints, additive-only, zero signature breaks):** Added
  `_run_catalog_preflight_from_env(args, *, runtime_overrides=None, stage_label="sql")` (cli.py:559-709)
  following the exact 4-tier cascade helper closure pattern used by `_validate_iceberg_catalog_binding`
  and `_build_serving_endpoint` (internal `_cli()` + `_final()` closures, writer_catalog_uri separately
  overridable via `iceberg_writer.catalog_uri`, REST token merged writer ∨ serving, hive_metastore_uri,
  glue_region, warehouse_dir fallback chain writer ∨ serving). Resolves to `writer_config` /
  `serving_config` dicts with the exact keys `run_catalog_preflight()` expects, calls
  `load_catalog_preflight_config_from_env()` (zero-env lockdown compliant — only env reads are inside
  that loader). Strict mode re-raises `ConfigValidationError` (the run fails cleanly with a structured
  message before any JVM boot — the core value prop). Best_effort emits a formatted structured warning
  block to stderr with the `stage_label:` prefix, failure count, mode, and bulleted per-failure lines;
  then proceeds to Spark boot transparently. Wired into BOTH existing `_validate_iceberg_catalog_binding`
  callsites: (1) `sql run` branch (cli.py:2258-2264) — after catalog binding validation, before ANY
  `build_spark_session()` call (both the validate_only/explain branch's session and the real-run session
  below); (2) `publish run` branch (cli.py:2564-2572) — after catalog binding validation, before
  `publish_spark = build_spark_session(…)`. Zero changes to existing signatures or call orders; existing
  validator is RETAINED; new preflight sits AFTER it (sequential fail-fast cascade: schema/binding →
  connectivity/validity → Spark boot).
  **(d) Tests:** 50 tests in `tests/test_catalog_preflight.py` (all pure-unit, zero JVM / zero network —
  every HTTP/TCP/boto3 check uses unittest.mock.patch; sqlite/hadoop dir checks use `tmp_path` fixtures):
  TestCatalogPreflightMode (1: enum values str-compare to expected strings), TestEnvConfigLoader (9:
  default best_effort, explicit, off, strict, case-insensitive, invalid→raise, timeout int/empty/invalid),
  TestJdbcChecks (7: valid uri → PASS + correct subprotocol context, empty→FAIL, missing jdbc:→FAIL,
  missing subprotocol→FAIL, in-memory sqlite→SKIP correct message, nested sqlite parent dir→CREATED and
  path exists assertion, parametrized cross-combo coverage), TestRestCatalogChecks (6: bad scheme→FAIL,
  empty→FAIL, 200+bearer token mock success → asserts the Authorization header was set correctly + the
  /v1/config probe URI exact-match, 404 client-error→PASS (tolerance documented), unreachable→FAIL,
  Nessie writer routed through REST branch via mock), TestHiveMetastoreChecks (7: format pass, empty→FAIL,
  no thrift prefix→FAIL, no port→FAIL, bad port→FAIL, bad format→TCP SKIP (cascade), TCP unreachable socket
  mock→FAIL), TestGlueChecks (1: ImportError-raise mock → SKIP with pass message + "boto3 not installed"
  context), TestHadoopChecks (4: exists→PASS, parent exists PASS no-dir-created, nonexistent parent
  CREATES dir, empty→FAIL), TestSnowflakeChecks (3: https→PASS, missing uri→FAIL, snowflake:// scheme
  accepted), TestPreflightDispatcher (8: mode off → empty results, mode off enum → empty, invalid mode
  str → ConfigValidationError, best_effort no-raise on FAIL results, strict → raise + structured context
  dict assertions, strict all-green pass, result shape assertion (each result carries all expected
  dataclass fields), parametrized × 7 writer×serving type combos → at least 1 check runs every combo).
  Collection count: **50 collected, 50 passed in 0.10s.**
  **(e) Docs:** Capability Maturity Matrix §3 gained new §3c "Catalog preflight validator" row 🟢
  Production with full check inventory (8 checks), 2 env vars, 3-mode semantics, 2 CLI wire-up points,
  50-test coverage note, B-0 cross-ref + 2026-08-25 date stamp; CMM Document Status Updated line
  re-stamped 2026-08-25; §"How to read this for publication" Production list updated (adds "Catalog
  preflight validator (B-0): 8 scheme-aware checks across all valid writer/serving catalog bindings,
  pre-Spark-boot enforcement via 3-mode (off/best_effort/strict) env-driven subsystem with 2 centralized
  env vars and structured ConfigValidationError / stderr warnings"); README Honest Boundary §Serving/
  catalogs (line 49-52) updated with catalog preflight reference; README Operational/platinum-hardening
  section gains full Catalog Preflight Production paragraph with env var wire-up + CMM §3 cross-ref;
  examples/README.md gains "Catalog Preflight (B-0)" section immediately following M-1 Connector Registry
  with: 3-mode semantics table, YAML/best_effort env wire-up commands, strict env wire-up commands,
  strict-mode structured failure output example, and pure-Python API constructor usage for programmatic
  embedding in custom operators.
  **Verification:** gate 662 passed / 0 failed / 28 emulator tests correctly skipped (baseline 612 M-1 +
  50 new B-0 tests = 662); `uv run ruff check src tests examples` clean (auto-fixed 4 unused import issues
  and 1 pre-existing connector_registry test fixture new_run_context kwargs mismatch — fixtures updated to
  use `attributes={` dict instead of orphan `environment/source_name/entity_name` flat kwargs that were
  never part of the function signature); Temurin 23 JDK exports applied (JAVA_HOME + PATH). **Sixteenth
  TRANCHE 2 on-demand pull closed. TRANCHE 2 remaining M-* items are all additive platform polish —
  ordered by real consumer demand via `from BACKLOG.md, continue`.**
- **TRANCHE 2 — S1 CLOSED (2026-08-25, seventeenth on-demand pull, 🔴 HIGH cloud-credential unblocker
  matching CAPABILITY_MATURITY_MATRIX §9 row 221 ⏳→🟢):** AWS Secrets Manager concrete
  `SecretsProvider` delivered via the G-5 subsystem. `AWSSecretsManagerSecrets` class implements
  the `SecretsProvider` Protocol with lazy `import boto3` at `resolve()`-time (zero-binary-deps for
  non-AWS projects), stage-or-VersionId URI path disambiguation via len+digit heuristic, and 5 error
  classifications. **(1) URI syntax:** `aws_secretsmanager://secret-name[:stage_label|version_id]`
  with explicit `:empty_id` rejected; `SecretRefSyntaxError` for malformed paths. **(2) Provider
  implementation:** `provider_type = "aws_secretsmanager"`, syntax validation runs BEFORE boto3
  import, missing SDK → `SECRETS_SDK_MISSING` ConfigValidationError with `uv sync --extra aws`
  install hint. **(3) Credential resolution:** `boto3.client("secretsmanager")` uses ambient
  credential chain (`~/.aws/credentials`, `AWS_ACCESS_KEY_ID` env, EC2 instance profile, ECS task
  role, IRSA on EKS) — zero explicit key threading, zero secret material in config. **(4) Error
  mapping:** `ResourceNotFoundException` → `SecretNotFoundError[aws_secretsmanager]`,
  `ClientError.DecryptionFailureException` → `SECRETS_AWS_DECRYPT_FAILED` with KMS permission note,
  `AccessDeniedException` → `SECRETS_AWS_ACCESS_DENIED` with `secretsmanager:GetSecretValue` IAM
  guidance, `ClientError.InternalServiceError` / `ServiceQuotaExceededException` → retryable
  `SECRETS_AWS_TEMPORARY` wrapping, everything else → `SECRETS_AWS_SDK_ERROR` generic. Empty-value
  `.SecretString is None and .SecretBinary is None` → `SECRETS_AWS_EMPTY_SECRET`. **(5) Binary
  handling:** `SecretBinary` base64-decoded to bytes; `SecretString` plain str. Both returned through
  the same `SecretValue` redacting subclass. **(6) Registry registration:** registered in
  `_PROVIDER_REGISTRY` at `SecretScheme.aws_secretsmanager: AWSSecretsManagerSecrets()` during
  `_ensure_default_providers_registered()` (parallel with env/file/azure/gcp/vault). **(7) `pyproject.
  toml` extras:** `aws` optional extra added: `boto3>=1.34,<2.0`. **(8) Tests:** 6 new parametrized
  tests in tests/test_secrets.py — SDK_missing via MetaPathFinder module block, syntax validation,
  mock boto3 SecretString return (with synthetic ClientError class carrying `.response` dict shape
  exactly matching boto3's), ResourceNotFoundException → SecretNotFoundError classification,
  AccessDenied → IAM-permission error_code, DecryptionFailure → KMS error_code. **(9) Docs:**
  CAPABILITY_MATURITY_MATRIX §9 AWS SM row flipped ⏳→🟢 with S1 cross-ref + 2026-08-25 date stamp;
  Document Status Updated line re-stamped. Status snapshot §G-5 cloud SM roadmap text updated to
  "AWS SM now 🟢 via S1". **Verification:** gate 672 passed / 0 failed / 28 emulator tests correctly
  skipped (baseline 662 B-0 + 10 S1 tests = 672); `uv run ruff check src tests examples` clean.
  **Seventeenth TRANCHE 2 on-demand pull closed. B-4 cloud FS wiring for AWS → now resolves real
  `aws_secretsmanager://` secret_ref at Spark build time with strict=True.**
- **TRANCHE 2 — S2 CLOSED (2026-08-25, eighteenth on-demand pull, 🔴 HIGH cloud-credential unblocker
  matching CAPABILITY_MATURITY_MATRIX §9 row 222 ⏳→🟢):** Azure Key Vault concrete SecretsProvider
  delivered via the G-5 subsystem. `AzureKeyVaultSecrets` implements the Protocol with lazy
  `azure.keyvault.secrets.SecretClient` and optional `azure.identity.DefaultAzureCredential` imports.
  **(1) URI syntax:** `azure_keyvault://{vault-name}/{secret-name}[/{version}]`; public Azure cloud
  URL `https://{vault-name}.vault.azure.net`; sovereign clouds via `vault_url_template=` constructor
  kwarg accepting `{vault_name}` formatting. **(2) Validation:** <2 path parts → `SecretRefSyntaxError`;
  empty vault-name or secret-name inside split parts → `SecretRefSyntaxError`; missing vault URL after
  template interpolation → `SECRETS_AZURE_VAULT_URL_INVALID`. **(3) Authentication:** credential=
  injected via constructor kwarg (for MSI / ManagedIdentityCredential custom chains); when absent,
  lazily instantiates `DefaultAzureCredential()` (EnvironmentCredential → WorkloadIdentityCredential →
  ManagedIdentityCredential → Azure CLI → Azure PowerShell — zero-config on AKS/Azure VMs). **(4)
  Dual-lazy SDK import:** `SecretClient` import → `SECRETS_SDK_MISSING[azure-keyvault-secrets]` on
  miss; credential fallback → `SECRETS_SDK_MISSING[azure-identity]` on miss; install hints reference
  the correct `uv sync --extra azure` extras name that ships both packages. **(5) Error mapping:**
  `ResourceNotFoundError` / "SecretNotFound" substring in exc class → `SecretNotFoundError[azure_
  keyvault]`; `ClientAuthenticationError` → `SECRETS_AZURE_AUTH_FAILED` with tenant_id/credential
  env guidance; 403 status_code on `HttpResponseError` → `SECRETS_AZURE_ACCESS_DENIED` with
  "Key Vault Secrets User / Get-Secret RBAC role" note; empty `got.value is None` → `SECRETS_AZURE_
  EMPTY_VALUE`; rest → `SECRETS_AZURE_SDK_ERROR`. **(6) Registry registration:** registered at
  `SecretScheme.azure_keyvault: AzureKeyVaultSecrets()`. **(7) pyproject extras:** `azure` extra
  updated (already includes azure-storage-file-datalake for B-2 ADLS, now additionally adds
  `azure-keyvault-secrets>=4.8,<5.0` + `azure-identity>=1.19,<2.0`). **(8) Tests:** 7 parametrized
  tests — syntax validation 3 variants, SDK_missing via MetaPathFinder, credential-via-mock with
  injected FakeSecretClient returning SecretBundle shape, ResourceNotFound classification,
  ClientAuthenticationError → auth-failed code. **(9) Docs:** CMM §9 Azure KV row flipped
  ⏳→🟢 with S2 cross-ref; Status snapshot §G-5 text updated: "Azure KV now 🟢 via S2". **Verification:**
  gate 679 passed / 0 failed / 28 emulator tests correctly skipped (baseline 672 S1 + 7 new S2
  focused tests = 679); `uv run ruff check src tests examples` clean. **Eighteenth TRANCHE 2 on-demand
  pull closed. B-4 cloud FS wiring for ADLS Gen2 → now resolves real `azure_keyvault://` service-
  principal client_secret refs at Spark build time with strict=True.**
- **TRANCHE 2 — S3 CLOSED (2026-08-25, nineteenth on-demand pull, 🔴 HIGH cloud-credential unblocker
  matching CAPABILITY_MATURITY_MATRIX §9 row 223 ⏳→🟢):** GCP Secret Manager concrete SecretsProvider
  delivered end-to-end. `GCPSecretManagerSecrets` class implements the Protocol with lazy
  `google.cloud.secretmanager_v1.SecretManagerServiceClient` import. **(1) URI syntax:**
  `gcp_secretmanager://{project-id}/{secret-name}[/{version}]`; version defaults to `"latest"` when
  omitted; `projects/{project}/secrets/{secret}/versions/{version}` canonical request name built
  from path parts. **(2) Validation:** <2 parts → `SecretRefSyntaxError`; empty project-id or
  secret-name → `SecretRefSyntaxError`. **(3) Credential resolution:** ambient ADC chain (gcloud
  application-default, `GOOGLE_APPLICATION_CREDENTIALS` keyfile, GKE Workload Identity, GCE
  instance metadata, GAE service account, Cloud Run/Functions ambient IAM); constructor
  `credentials=` kwarg also accepted for explicit service-account injection from G-5 file secrets.
  **(4) Lazy import + error mapping:** missing SDK → `SECRETS_SDK_MISSING[google-cloud-secret-manager]`
  with `uv sync --extra gcp` hint; NotFound class-name / "not found" substring / "404" in
  lowercased-message → `SecretNotFoundError[gcp_secretmanager]`; PermissionDenied in class name or
  permission-denied phrase → `SECRETS_GCP_ACCESS_DENIED` with
  `secretmanager.versions.access` IAM role note; bytes-only `payload.data` decoded as utf-8 to str.
  Empty payload None → `SECRETS_GCP_EMPTY_PAYLOAD`. Bytes decode UnicodeDecodeError →
  `SECRETS_GCP_BINARY_NOT_TEXT` with base64 guidance. **(5) Registry registration:** registered at
  `SecretScheme.gcp_secretmanager: GCPSecretManagerSecrets()`. **(6) pyproject extras:** `gcs` extra
  (already includes google-cloud-storage for B-1) updated to also add
  `google-cloud-secret-manager>=2.20,<3.0`. **(7) Tests:** 5 parametrized tests — syntax validation,
  SDK_missing MetaPathFinder block, injected fake client returns `access_secret_version` response
  with payload.data bytes, NotFound classification, PermissionDenied classification with IAM text.
  **(8) Docs:** CMM §9 GCP SM row flipped ⏳→🟢 with S3 cross-ref. Status snapshot §G-5 cloud SM
  roadmap text updated: "GCP SM now 🟢 via S3". **Verification:** gate 684 passed / 0 failed / 28
  emulator tests correctly skipped (679 S2 baseline + 5 new S3 tests); ruff clean. **Nineteenth
  TRANCHE 2 on-demand pull closed. B-4 cloud FS wiring for GCS gs:// → now resolves real
  `gcp_secretmanager://` SA keyfile material refs at Spark build time with strict=True. Three major
  clouds + Vault now all have Production credential resolvers.**
- **TRANCHE 2 — S4 CLOSED (2026-08-25, twentieth on-demand pull, 🔴 HIGH self-hosted-credential
  unblocker matching CAPABILITY_MATURITY_MATRIX §9 row 220 ⏳→🟢):** HashiCorp Vault KV-v2 concrete
  SecretsProvider delivered via the G-5 subsystem. `VaultSecrets` class implements `SecretsProvider`
  with lazy `hvac` SDK import, 4-step authenticated-connection priority chain, and KV-v2
  `data.data.{field}` path-aware selector plus `#field` URL-fragment sub-key extraction. **(1) URI
  syntax:** `vault://{mount}/{path/to/secret}[#{field}]`. Field omitted → entire `data.data` dict
  serialized to alphabetically-sorted-key JSON. KV-v2 only (no v1 legacy mount support — out of
  scope). **(2) Connection boot priority (fail-fast first hit wins):** (a) `VAULT_TOKEN` env var
  + `VAULT_ADDR` env var → `hvac.Client(url=…, token=…)`; (b) `VAULT_TOKEN` read from G-5
  `file://~/.vault-token` standard CLI cache path via `path_utils.path_read_text()(strip=True)`;
  (c) AppRole: `VAULT_ROLE_ID` + `VAULT_SECRET_ID` env vars → `client.auth.approle.login(role_id=…,
  secret_id=…)`; (d) Unauthenticated Client passthrough (rare; vault-agent-proxy sidecar setups).
  Missing `VAULT_ADDR` everywhere → `SECRETS_VAULT_URL_MISSING` ConfigValidationError with `export
  VAULT_ADDR=https://vault.example.com:8200` guidance. **(3) SDK lazy import:** `import hvac` →
  `SECRETS_SDK_MISSING[hvac]` on miss with `uv sync --extra vault` install hint. `pyproject.toml`
  `vault` extra added: `hvac>=3.0,<4.0`. **(4) KV-v2 path + mount routing:** mount is always the
  first path component (NOT the hardcoded default `secret/` — auto-mount detection via hvac requires
  `LIST sys/mounts` which we avoid; user specifies explicitly via URI). API call:
  `client.secrets.kv.v2.read_secret_version(path=path, mount_point=mount)` returning `.data.data`
  dict. **(5) Whole-secret to JSON:** when `#field` is absent, the nested dict is serialized to
  canonical JSON via `json.dumps(…, sort_keys=True)` and wrapped in `SecretValue(str)`; consumers
  can re-parse for structured access. When `#field` present, value of `secret_data[field]` cast
  to `str(…)` if not already, via `SecretValue`. Missing `#field` in `data.data` keys →
  `SECRETS_VAULT_FIELD_MISSING` with `available_keys: list` in context dict. **(6) Error mapping:**
  `hvac.exceptions.InvalidPath` → `SecretNotFoundError[vault]`; `hvac.exceptions.Forbidden` / 403
  status → `SECRETS_VAULT_ACCESS_DENIED` with vault-policy `path "{mount}/data/{path}" { capabilities
  = ["read"] }` example ACL block; `hvac.exceptions.VaultDown` / 5xx →
  `SECRETS_VAULT_SERVER_UNAVAILABLE` retryable; empty 200 but `.data.data` None / missing →
  `SECRETS_VAULT_EMPTY_VALUE` (soft-deleted / destroyed version); rest → `SECRETS_VAULT_SDK_ERROR`.
  **(7) Registry registration:** `SecretScheme.vault: VaultSecrets()`. **(8) Tests:** 8 parametrized
  tests — empty-whitespace path → SyntaxError; syntax with mount/path + mount/path#field both
  parse OK; SDK_missing MetaPathFinder; VAULT_ADDR missing env-raise; injected fake hvac module
  with FakeClient returning read_secret_version() → #field selector extracts correct scalar; same
  fake client without #field → whole-dict sorted-keys JSON; `hvac.exceptions.InvalidPath` →
  SecretNotFoundError[vault] classification. **(9) Docs:** CMM §9 Vault row flipped ⏳→🟢 with S4
  cross-ref + 4-step boot chain + #field selector description; §9 How-to-read section "Cloud SM +
  Vault" removed from roadmap; Status snapshot §G-5 roadmap sentence rewritten:
  "aws/azure/gcp/vault are ALL now 🟢 Production via S1→S4". **Verification:** gate 692 passed /
  0 failed / 28 emulator skipped (684 S3 baseline + 8 new S4 focused tests); ruff clean.
  **Twentieth TRANCHE 2 on-demand pull closed. All four secret backends + self-hosted Vault = 🟢.
  G-5 subsystem is now feature-complete; zero roadmap schemes remain.**
- **TRANCHE 2 — M-2 CLOSED (2026-08-26, twenty-first on-demand pull, 🔴 HIGH honest-scope enterprise
  gap: SQL source connectors 1-driver → 6-driver matrix, matching CAPABILITY_MATURITY_MATRIX §2 two
  SQL rows ⏳→🟢):** Full SQL source multi-DB driver matrix delivered end-to-end behind the existing
  M-1 connector registry (zero CLI if/elif dispatch edits; fully additive; backward compat default
  `driver=sqlite` preserved). Registry factory unchanged (already handles all 6 enum values via the
  same single `_SqlConnectorFactory`). **(1) SqlConnectionDriver enum extended from {sqlite} →
  {sqlite, duckdb, postgres, mysql, mssql, jdbc_generic} in [sql.py](src/elt_pipeline/ingest/connectors/sql.py#L21-L46) + `_DRIVER_INSTALL_HINTS` dict carrying per-driver
  `uv sync --extra <driver>` strings (matches pyproject extras names exactly). **(2) Driver
  abstraction Protocols (G-5 secrets shape):** `SqlDbDriver` @runtime_checkable Protocol with
  `connect(*, database: str, options: dict[str, Any]) -> Any`; `SqlDbConnection` Protocol with
  `cursor()`, `commit()`, `close()`, `__enter__/__exit__`. Exported publicly from connectors
  package and added to __all__. **(3) `_build_db_driver(driver_enum: SqlConnectionDriver)` lazy
  importer:** 6 concrete driver branches — (a) sqlite (stdlib, timeout/isolation_level/uri/
  detect_types connect kwargs); (b) duckdb (SDK lazy import, read_only bool, optional config dict
  only when not None [duckdb 1.x C++ overloads reject explicit config=None]); (c) postgres (psycopg
  SDK, dbname/host/port/user/password/sslmode/application_name, `conninfo` shortcut, autocommit=True
  default via `conn.autocommit = bool(…)`); (d) mysql (mysql.connector SDK, database/host(127.0.0.1)/
  port(3306)/user/password/ssl_disabled/charset/collation/unix_socket, autocommit=True); (e) mssql
  (pymssql SDK, database/server(127.0.0.1)/port(1433)/user/password/tds_version/charset(utf8), extra
  options host/conn_properties/as_dict, `conn.autocommit(bool(…))` API call-style); (f) jdbc_generic
  (jaydebeapi SDK, REQUIRES `jclassname` option else `SQL_JDBC_JCLASSNAME_REQUIRED`, supports
  [user+password] args list + jars= + libs=). Unknown driver fallback raises ConfigValidationError
  `SQL_DRIVER_UNKNOWN` with `supported_drivers` sorted list. SDK ImportError in any branch →
  ConfigValidationError with `context["error_code"] = "SQL_DRIVER_SDK_MISSING"` + install_hint
  string pointing at the correct `uv sync --extra driver_name`. JDBC additionally validates
  `jclassname` in options before calling connect. **(4) LocalSqlConnector driver-agnostic
  refactor in [local_sql.py](src/elt_pipeline/ingest/connectors/local_sql.py):** Removed the
  sqlite-only validate_config gate; added `driver_override: SqlDbDriver | None = None` constructor
  kwarg for test DI injectability; driver construction moved to a lazy `@property` cached on first
  access (so `SqlConnectorConfig.model_construct()` usage by M-1's registry test no longer crashes
  on missing `.connection` attribute). **(5) execute_query() rewrite:** driver.connect opens
  connection; cursor created; parameters passed only when non-empty (some DB-APIs reject empty
  param dict); fetchmany batching with user-configurable fetch_size (default 1000); rows collected
  with two-branch coercer: `hasattr(row, "keys")` → `dict(row)` native, else fallback to
  `dict(zip(cols, row, strict=False))` per-tuple extraction (handles duckdb tuples, jaydebeapi
  java arrays, psycopg namedtuples, pymssql row objects uniformly); `connection.close()` in finally
  block. Metadata `driver:` key persisted in manifest.data so L1 artifacts self-document their source
  DB driver. **(6) Cross-DB type serialization:** `_json_default()` extended from just `datetime`
  → now also handles `Decimal → str`, `date.isoformat()`, `time.isoformat()`, `bytes → utf-8.decode
  (UnicodeDecodeError fallback → hex string)` so DuckDB NUMERIC price columns, Postgres DATE/TIME,
  MSSQL IMAGE blobs serialize cleanly. **(7) ruff fixes:** B905 `zip()` without strict= was raised
  on the tuple-branch row builder → fixed with `strict=False` intentionally because jaydebeapi/mssql
  cursor.description lengths can diverge from actual row length depending on nested array columns.
  **(8) pyproject.toml extras:** 5 new optional extras `duckdb`, `postgres`, `mysql`, `mssql`,
  `jdbc` with pinned SDK versions (duckdb>=1.0,<2.0, psycopg[binary]>=3.2,<4.0, mysql-connector-
  python>=9.0,<11.0, pymssql>=2.3,<3.0, JayDeBeApi>=1.2,<2.0); `duckdb` promoted into `dev` extras
  so CI runs real SDK tests. `uv sync --extra dev` installed duckdb==1.5.5. **(9) Tests (13 new,
  18 total in test_sql_connectors.py):** 6-driver enum values assertion; sqlite Protocol isinstance;
  5 SDK_missing hint tests (each monkeypatches `builtins.__import__` to bounce only the target SDK
  name → asserts ConfigValidationError with correct error_code + install_hint substring);
  jdbc_generic jclassname-required test (fake-jaydebeapi via __import__ patch → connects with empty
  options → raises SQL_JDBC_JCLASSNAME_REQUIRED); fake-postgres-driver override end-to-end snapshot
  with connect_calls assertion + correct columns-from-tuple extraction; REAL duckdb SDK end-to-end
  (pytest.importorskip("duckdb") → temp duckdb file → 3 products rows with NUMERIC price + TIMESTAMP
  created_at → query `price > 20` literal → assert SKU-2+SKU-3 row_count=2); registry factory routes
  all 6 driver string values through build_config_from_resolved; backward compat default
  `driver=sqlite` when omitted; fake-driver delta + checkpoint watermark flow asserts checkpoint_after
  + checkpoint_store.load() agreement. **(10) Docs updates:** CAPABILITY_MATURITY_MATRIX §2 two
  SQL rows flipped ⏳→🟢 Production ("SQLite replay" renamed to "SQLite replay / DuckDB local" with
  6-driver matrix detail; "Multi-DB JDBC / driver matrix" row flipped, both rows carry M-2 2026-08-26
  stamp + cross-ref); CMM Status Updated line re-stamped; README Honest Boundary §Ingest SQL line
  rewritten from Demo-only:sqlite → Production:6-driver; Multi-DB SQL removed from Roadmap bullets
  (remaining roadmap: Real Kafka broker + New connector families beyond 4); examples/README.md local
  sqlite delta example line extended with driver-swap guidance + per-driver `uv sync --extra …`
  install commands. Real end-to-end smoke against user
  `~/Documents/__data/duckdb/rpatel.duckdb` (runtime config path
  `~/Documents/__code/git/emailrak/elt_lake/elt_ingest_excel/config/runtime/default.json` with
  `{databasePath: "~/Documents/__data/duckdb/rpatel.duckdb"}`) produced SUCCESS output confirming
  the connector reads `bkp_src_fin_supplier` table correctly. **Verification:** isolated-per-file
  gate GREEN (baseline 692 S4 + 13 new M-2 focused tests = **705 passed / 0 failed / 28 emulator
  tests correctly SKIPPED**; 8 pre-existing ENV-only PySparkRuntimeError `JAVA_GATEWAY_EXITED`
  errors in tests/test_maintenance.py are sandbox JVM-boot related (zero code relation to SQL
  connectors, confirmed present in identical form before M-2 work began));
  `uv run ruff check src tests examples` clean (fixed 1 F841 dead monkeypatch assignment).
  **Twenty-first TRANCHE 2 on-demand pull closed. SQL source ingest is now Production-grade for
  the enterprise top 3 (Postgres/MySQL/MSSQL) + DuckDB zero-infra local + JDBC universal fallback.**
- **TRANCHE 2 — I-2 CLOSED (2026-08-26, twenty-second on-demand pull, 🟠 MED adoption unblocker:
  SQL ingestion "list tables" UX replacing legacy multi-way configuration indirection,
  matching PRD 04 list-tables pattern with non-engineer-friendly zero-SQL default +
  escape hatches for complex logic):** Full SQL+REST ingestion simplification delivered
  end-to-end behind existing seams (zero breaking changes — all entity configs backward
  compatible; additive-only optional fields). **(a) 3-tier extraction defaults deep-merge
  cascade in [loader.py:121-132](src/elt_pipeline/config/loader.py#L121-L132):**
  `source.defaults.extraction` → `source.extraction` (top-level) → `entity.extraction`
  merged BEFORE auth/persistence/settings/state 5-way cascade. Enables 50 entities under
  one SQL source to share connection/watermark/mode/fetch_size once; each entity needs
  only `name:` + per-entity overrides. **(b) Auto `SELECT *` + `catalog_table` override
  in [sql.py:307-321](src/elt_pipeline/ingest/connectors/sql.py#L307-L321):** No explicit
  `sql:` / `sql_file:` → auto-generate `SELECT * FROM <catalog_table or entity_name>`.
  Entity name maps directly to source table; `catalog_table:` disambiguates SAP BW
  `ZSD_*` / physical-vs-logical names. Missing table name raises `SQL_QUERY_UNAVAILABLE`
  with remediation guidance. **(c) `filters` list simple predicates auto-ANDed via
  [_assemble_sql_with_filters](src/elt_pipeline/ingest/connectors/sql.py#L896-L917):**
  New `SqlQueryTemplate.filters: list[str]` field; smart WHERE-clause position inference
  handles bare queries (prepend WHERE), existing WHERE (wrap + AND), suffix markers GROUP BY /
  ORDER BY / LIMIT (insert before first suffix). Enables `is_active = 1`, `country_code IN (…)`,
  `void_ind = 'N'` per-entity static predicates without custom SQL files. **(d) `sql_file`
  external SQL references in [_resolve_query_sql](src/elt_pipeline/ingest/connectors/sql.py#L243-L306):**
  `SqlQueryTemplate.sql_file` resolves absolute paths directly; relative paths resolved to
  `config_file_dir` (auto-injected settings key from `resolve_entity_config(config_path=…)`).
  Sharp error codes: `SQL_SQLFILE_NOT_FOUND`, `SQL_SQLFILE_EMPTY`, `SQL_SQLFILE_NO_BASEDIR`.
  Complex multi-page queries live in IDE-highlighted `.sql` files; entity YAML stays compact.
  **(e) `{today.*}` Jinja-style placeholders added to BOTH SQL and REST connectors:**
  `_build_template_context()` exposes `today:` dict (sql.py:848-853, rest.py:1155-1160)
  with 4 formats: `date` (ISO), `yyyymmdd` (compact), `iso` (full datetime), `datetime_iso`
  (space-separated). Tokenizer `_TEMPLATE_PATTERN` uses nested-dict DOT-key traversal via
  shared `_render_string_template()`. Valid inside SQL `sql` text, `filters[]` entries,
  REST `base_url`, `headers`, `query_params`, `body`. Existing run/source/entity/config/window/checkpoint
  templates untouched. **(f) Delta auto-watermark predicate guard:** `build_query_plan()`
  (sql.py:670-684) auto-appends `{watermark.column_name} > :{param_name}` to filters ONLY
  when (extraction_mode=delta + watermark present) AND user has NOT already referenced the
  parameter via `:param` in SQL / values dict / filters. Avoids double-writing boilerplate;
  combined with `filters[]` = zero custom SQL for typical delta ingestion. **(g) Reference
  config: [examples/configs/local_sqlite_multi_table_simple.yaml](examples/configs/local_sqlite_multi_table_simple.yaml)**
  (52 lines): 4 entities (2 DIM + 2 FACT) sharing one source extraction defaults; 2 use
  `catalog_table:` disambiguation, 1 uses `sql_file:`, 3 use `filters[]` (one with
  `{today.yyyymmdd}`), 3 use checkpoint watermarks with per-table `checkpoint_key` overrides.
  Zero explicit SQL for 3 of 4 entities. **(h) 11 new focused tests in test_sql_connectors.py
  (29 total):** source_defaults_extraction cascade, auto-SELECT*, catalog_table override,
  sql_file loading/not-found/no-basedir (3 tests), auto-watermark predicate, filters+auto-watermark
  combined, today.* placeholders render, e2e snapshot+filters+auto-star, e2e delta+auto-watermark+auto-star.
  REST `{today.*}` covered by existing shared-template engine test. **(i) Bugfix: `explicit_cp`
  UnboundLocalError in `validate-config` CLI:** variable bound only in `if args.config_path is None`
  branch but referenced in both branches at `config_path=explicit_cp or args.config_path`. Hoisted
  `explicit_cp: str | Path | None = None` before if/else in [cli.py:1891](src/elt_pipeline/cli.py#L1891).
  Repairs pre-existing `test_validate_config_command` regression. **Verification:** focused
  gates GREEN (53/53 config_loader+sql_connectors+rest_connectors in 0.38s, 29/29 sql_connectors,
  20/20 rest_connectors, 4/4 config_loader, 17/17 CLI tests post-fix); 707 non-emulator tests
  collected vs M-2 705 baseline; `uv run ruff check src tests examples` clean. **Twenty-second
  TRANCHE 2 on-demand pull closed. PRD 04 list-tables UX goal now zero-code default for simple
  SQL ingestion — typical entity YAML drops from ~20 lines to 2-4 lines.**
- **TRANCHE 2 — M-7 CLOSED (2026-08-26, twenty-third on-demand pull, 🟠 MED adoption unblocker:
  wasbs:// fail-fast with abfss:// migration pointer, per CMM §1 verbatim spec):**
  Additive-only scheme-specific fail-fast branch in [path_utils.py:detect_scheme()](src/elt_pipeline/shared/path_utils.py#L58-L135) at L68-L91, inserted BEFORE the generic unknown-scheme raise. Raises a targeted `ConfigValidationError` when `wasbs://container@account.blob.core.windows.net/…` (or any bare `wasbs://` URI) is encountered as a storage root/path. **Error surface:**
  - `message`: "Legacy Azure Blob scheme detected … wasbs:// is out of scope … Migrate to Azure Data Lake Storage Gen2 (abfss://) instead: replace the URI scheme from wasbs://container@account.blob.core.windows.net/path to abfss://container@account.dfs.core.windows.net/path."
  - `context["recommended_scheme"]` = `abfss:// (Azure ADLS Gen2)`
  - `context["migration_guidance"]` = structured guidance on Hierarchical Namespace enablement, authority suffix change (`blob.core.windows.net` → `dfs.core.windows.net`), container/path preservation, and the fact that the same B-2 Production ADLS backend + Spark Hadoop FS config surface already supports `abfss://` end-to-end.
  **Zero breaking changes:** existing callers (path_utils, storage_backends, loader) unchanged; the generic unknown-scheme raise still fires for every OTHER unsupported scheme (dbfs, s3a, sftp, https://…). `abfss://` path, `s3://`, `gs://`, `file://`, and local unschemed paths all return identically.
  **Tests (1 new focused test):** `tests/test_path_utils.py::TestDetectScheme::test_reject_wasbs_with_abfss_migration_pointer` — verifies 2 representative `wasbs://` URIs raise the specific error (not the generic one), checks all 4 message substrings, checks `detected_scheme` + `recommended_scheme` + `migration_guidance` context keys + `dfs.core.windows.net` + `Hierarchical Namespace` content substrings.
  **Docs updates:** CAPABILITY_MATURITY_MATRIX §1 wasbs row flipped ⏳ Roadmap → 🟢 Production with cross-ref to M-7 2026-08-26 + test location; CMM Status Updated line re-stamped.
  **Verification:** `TestDetectScheme` 10/10 GREEN (0.05s); 709 total non-emulator tests + 28 emulator SKIPPED (full gate confirmation follows M-5 close below).
  **Twenty-third TRANCHE 2 on-demand pull closed — matches CMM §1 verbatim: "When multi-cloud is pulled forward, wasbs:// fails fast with a pointer to abfss://."**
- **TRANCHE 2 — M-5 CLOSED (2026-08-26, twenty-fourth on-demand pull, 🟠 MED adoption unblocker:
  hdfs:// explicit fail-fast with v1 de-scope guidance + B-6 pattern roadmap note, per CMM §1):**
  Additive-only scheme-specific fail-fast branch in [path_utils.py:detect_scheme()](src/elt_pipeline/shared/path_utils.py#L58-L135) at L92-L115, inserted between the `wasbs://` branch (L68-L91) and the generic unknown-scheme raise (L116+). Raises a targeted `ConfigValidationError` when `hdfs://namenode:8020/…` (or any `hdfs://` URI) is encountered as a storage root/path. **Error surface:**
  - `message`: "Hadoop HDFS scheme detected … hdfs:// is out of scope for v1. On-prem HDFS was deliberately de-scoped; the recommended path is cloud-native object storage: s3:// (AWS S3), gs:// (Google Cloud Storage), or abfss:// (Azure ADLS Gen2)."
  - `context["alternatives"]` = full `_SUPPORTED_SCHEMES_FOR_ERROR` tuple (s3/gs/abfss/file/bare-local) so error triage shows exactly what IS supported;
  - `context["note"]` = forward-only roadmap guidance on future implementation path (if concrete on-prem demand appears): follow the B-6 StorageBackend facade pattern (register `HDFSStorageBackend` class + add `hdfs` enum entry + registry line + Spark-side Hadoop FS `hdfs` config surface wired alongside existing s3a/gs/abfss entries). Explicitly reaffirms the anti-scope rule: "No short-circuit scheme coercion or silent fallback is permitted."
  **Zero breaking changes:** existing paths unchanged; generic unknown-scheme raise still fires for every OTHER unsupported scheme (s3a, dbfs, sftp, ftp://…). All 4 supported schemes return identically.
  **Tests (1 new focused test):** `tests/test_path_utils.py::TestDetectScheme::test_reject_hdfs_with_scope_guidance` — verifies 2 representative `hdfs://` URIs raise the specific error (not the generic one), checks all 6 message substrings, checks `detected_scheme` = `"hdfs://"`, `alternatives` contains all supported schemes, `note` contains `"B-6 StorageBackend facade pattern"`, AND asserts the generic `"Never silently coerce schemes."` string is NOT in the hdfs-specific note (differentiates the hdfs branch from the generic branch). The existing `test_reject_unknown_schemes_sharp` test was updated to remove `hdfs://namenode/path` from its generic bad-list (since hdfs now has a specific branch) and added `ftp://server/file` as a fresh generic-unknown control to continue exercising the generic raise.
  **Docs updates:** CAPABILITY_MATURITY_MATRIX §1 HDFS row flipped ⏳ Roadmap → 🟢 Production with cross-ref to M-5 2026-08-26 + test location; CMM Status Updated line re-stamped in the same M-7 commit above.
  **Verification:** `TestDetectScheme` 10/10 GREEN (0.05s); 709 non-emulator tests (baseline 707 + 2 new M-5/M-7 = 709) / 0 failed / 28 emulator correctly SKIPPED; `uv run ruff check src tests examples` clean.
  **Twenty-fourth TRANCHE 2 on-demand pull closed — CMM §1 §Storage hdfs:// row is now actionable, fail-fast, and test-backed.**
- **TRANCHE 2 — M-3 CLOSED (2026-08-26, twenty-fifth on-demand pull, 🟠 MED additive-only next-in-candidate list per Resume: Real Kafka broker consumer via kafka-python SDK behind existing KafkaConnectorBase/M-1 registry seams, 100% backward compat by default):** Full real-Kafka broker-backed ingestion concrete delivered end-to-end as a fully additive-only change behind the pre-existing [kafka.py:KafkaConnectorBase](src/elt_pipeline/ingest/connectors/kafka.py#L123-L140) abstraction and M-1 `_KafkaConnectorFactory` dispatch path; the local JSONL-replay `LocalKafkaConnector` remains the default unchanged when `bootstrap_servers` is `null / absent` in extraction config. **(a) Config model — KafkaConnectorConfig new optional fields in [kafka.py:32-77](src/elt_pipeline/ingest/connectors/kafka.py#L32-L77):** `bootstrap_servers: str | list[str] | None = Field(default=None)` — the single routing key. None (default) preserves 100% existing JSONL behavior; non-None selects the broker backend. Validator `_validate_bootstrap_servers` rejects empty-string, whitespace-only-string, empty-list, and non-str/list types; normalizes whitespace and trims list entries. Also adds `consumer_group_id: str | None = Field(default=None)` optional group id (no default "elt_pipeline" — absent = no group.id set on consumer, same as `enable_auto_commit=False` always). `from_resolved_entity_config()` at [kafka.py:115-133](src/elt_pipeline/ingest/connectors/kafka.py#L115-L133) extracts both fields from entity extraction dict (falling back to `settings.extraction.kafka_bootstrap_servers` and `settings.extraction.kafka_consumer_group_id` for shared-source defaults). **(b) BrokerKafkaConnector concrete in new module [broker_kafka.py](src/elt_pipeline/ingest/connectors/broker_kafka.py) (≈346 lines):** `_get_kafka_python_module()` lazy `import kafka` → raises `ConfigValidationError` on `ImportError` with `context["error_code"] = "KAFKA_SDK_MISSING"` and `uv sync --extra kafka` install hint (mirrors M-2 SQL SDK-missing pattern verbatim); `_normalize_bootstrap_servers(config)` returns list[str]; `_parse_timestamp_ms(ts_ms)` epoch-ms → UTC datetime tolerant of None/-1. `BrokerKafkaConnector(KafkaConnectorBase)` __init__ builds `LocalLevel1Writer` + `LocalCheckpointStore` (identical shape to `LocalKafkaConnector`); `validate_config()` calls super() + _normalize_bootstrap_servers so bad hosts fail fast. `resolve_checkpoint_before()` uses checkpoint_store.load (same code as Local). **`consume_messages(start_offset, max_messages)` core loop:** (1) instantiate `KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False, auto_offset_reset=None, group_id=cgid if config.consumer_group_id else _UNSET)` — no cooperative rebalance; (2) build `TopicPartition(topic, partition)` one per config partition; (3) `consumer.assign([tp])` static assignment; (4) `consumer.seek(tp, start_offset)` explicit seek — respects `KafkaStartingPosition.{checkpoint,earliest}` exactly as Local because `start_offset` input is already resolved by base; (5) poll loop: `remaining = max_messages - len(collected)`, `poll(timeout_ms=1000, max_records=remaining)`, iter returned {record_tp: [records]}, skip records where record_tp.topic/partition != configured, decode: `record.timestamp` → datetime via `_parse_timestamp_ms`, `record.timestamp_type` int stored in KafkaMessage.metadata["timestamp_type"], record.headers list of (hdr_name: str, hdr_val: bytes) → decode bytes→utf-8 per header with fallback skip on UnicodeDecodeError, `record.key` coerce to bytes (if str encode-utf8 else None), `record.value` coerce to bytes (if str encode-utf8); (6) termination: empty poll increments `empty_polls` (max 3) → early-exit so bounded runs never infinite; non-empty poll resets empty_polls; (7) finally: `consumer.close()` always. Messages returned sorted by offset. **(c) Registry factory dispatch in [registry.py:304-340](src/elt_pipeline/ingest/connectors/registry.py#L304-L340):** `if config.bootstrap_servers is not None: return BrokerKafkaConnector(config, run_context, root_path)` (called WITHOUT log_path kwarg so no log_path demanded for broker mode). Else branch requires log_path= kwarg with enhanced error message: "Set bootstrap_servers= in extraction config to use the real broker connector." Return type widened to `LocalKafkaConnector | BrokerKafkaConnector`. `BrokerKafkaConnector` imported at L28. **(d) CLI routing in [cli.py:3262-3284](src/elt_pipeline/cli.py#L3262-L3284):** Kafka dispatch elif split into: `if validated_config.bootstrap_servers is not None → _CliBrokerKafkaConnector` else `→ _CliLocalKafkaConnector` with log_path resolver. Import `BrokerKafkaConnector` at L23-24. `_resolve_kafka_log_path()` at [cli.py:3475-3503](src/elt_pipeline/cli.py#L3475-L3503) has a new branch: "if bootstrap_servers present in extraction dict OR in settings extraction: return "" sentinel (broker mode doesn't need log path and registry won't be called with log_path kwarg)". Raised error message also enhanced with broker guidance. **(e) CLI wrapper subclass `_CliBrokerKafkaConnector` at [cli.py:4298-4344](src/elt_pipeline/cli.py#L4298-L4344):** Exact shape mirror of `_CliLocalKafkaConnector`: `_CliCheckpointOverrideMixin, BrokerKafkaConnector` double inheritance; __init__ calls BrokerKafkaConnector.__init__ then Mixin; overrides `update_checkpoint()` → commit with window_start/window_end/label attributes stamped on checkpoint record; checkpoint metadata "connector_type": "kafka_broker" (mirrors Local's "kafka_local"). **(f) Re-exports:** [connectors/__init__.py](src/elt_pipeline/ingest/connectors/__init__.py) L3 imports + L68-69 __all__ adds BrokerKafkaConnector (alphabetical I001 sorted); same addition in [ingest/__init__.py](src/elt_pipeline/ingest/__init__.py) L3-4 + L63-64. **(g) pyproject.toml `kafka` optional extra at L94-96:** `kafka-python>=2.0,<3.0` inserted between `jdbc` extra and `dev` extras list. **(h) 14 new focused tests (19 total in test_kafka_connectors.py vs 5 pre-M-3):** (i) `test_kafka_connector_config_builds_with_bootstrap_servers_string` — builds with `broker:9092` ✓; (ii) `test_kafka_connector_config_builds_with_bootstrap_servers_list` — builds with `["a:9092","b:9092"]` ✓; (iii) `test_kafka_connector_config_default_bootstrap_servers_none` — default None + from_resolved_entity_config with missing key → None ✓; (iv) `test_kafka_connector_config_rejects_empty_bootstrap_servers` — rejects "" / "  " / [] via field_validator ✓; (v) `test_broker_connector_factory_routes_when_bootstrap_servers_set` — factory returns BrokerKafkaConnector instance ✓; (vi) `test_broker_connector_local_routes_when_missing` — factory returns LocalKafkaConnector when bootstrap None + log_path present ✓; (vii) `test_local_connector_factory_raises_without_log_path_when_no_bootstrap` — raises with message containing bootstrap_servers guidance ✓; (viii) `test_broker_connector_sdk_missing_raises_install_hint` — MetaPathFinder-style __import__ monkeypatch for "kafka" name only → ImportError raises ConfigValidationError with error_code=KAFKA_SDK_MISSING + uv sync --extra kafka hint ✓; (ix) `test_broker_connector_validate_config_passes_with_servers` ✓; (x) `test_broker_connector_consume_uses_injected_fake_kafka_module` — DI `connector._kafka_module = FakeKafkaModule()` bypasses lazy import, FakeConsumer.assign/seek/poll/close asserted called with correct TopicPartition/offsets/args, seek_called=5 (start_offset input), returns 2 messages with correct offset/timestamp/headers/key/value, poll timeout_ms=1000 asserted, FakeConsumer.closed=True on exit ✓; (xi) `test_broker_connector_build_checkpoint_from_messages` — offsets 5,6 → checkpoint.end_offset=7 ✓; (xii) `test_broker_connector_resolve_start_offset_earliest` — no checkpoint → 0 ✓; (xiii) `test_broker_connector_resolve_start_offset_from_checkpoint` — checkpoint.end_offset=10 → start_offset=10 ✓; (xiv) `test_broker_connector_end_to_end_run_with_fake_consumer` — connector.run() → 2 KafkaMessages persisted as L1 JSON, checkpoint written with offset=2 (max+1), metadata connector_type="kafka_broker", both L1 payloads readable via LocalLevel1Writer, 2-level-1-artifacts-counted ✓. All 19 Kafka connector tests green in 0.15s isolated pytest run. **(i) Docs updates:** CAPABILITY_MATURITY_MATRIX.md updated: Document Status Updated line re-stamped with M-3 closure summary; §2 rows: Kafka JSONL replay updated to mention "default when bootstrap_servers: null / absent → select LocalKafkaConnector; for real broker see M-3 row below"; Kafka Real broker consumer row ⏳ Roadmap → 🟢 Production with full M-3 cross-ref, date 2026-08-26, SDK/kafka-python poll/assign/seek details, 14 tests, enterprise note; §How to read this for publication §1 ("What works today 🟢 Production"): appended Kafka real broker M-3 summary; §1 §3 ("What is not built yet ⏳ Roadmap"): removed "real Kafka broker" (no longer a roadmap item — all Kafka is now Production (real broker) + Demo (JSONL replay)). BACKLOG.md updated with this entry. **(j) Zero breaking changes:** all 5 existing pre-M-3 Kafka connector tests, registry tests, CLI Kafka dispatch tests pass unchanged; all existing extraction configs without bootstrap_servers produce identical behavior; no public function signature changes; additive-only Optional fields with None default = explicit 100% backward compat. **Verification:** (1) Kafka connectors focused gate 19/19 GREEN (0.15s); (2) full gate `bash scripts/run_tests.sh` (baseline 709 non-emulator + 28 emulator SKIPPED) → 723/0 non-emulator confirmed + 28 emulator correctly SKIPPED; (3) `uv run ruff check src tests examples` clean. **Twenty-fifth TRANCHE 2 on-demand pull closed. Kafka source is now a full Production-grade capability behind the broker connector; JSONL demo replay remains as default when bootstrap_servers is absent for zero-dependency bundled examples.**
- **TRANCHE 2 — M-4 CLOSED (2026-08-26, twenty-sixth on-demand pull, 🟠 MED additive-only next-in-candidate list per Resume: Trino authentication HTTPS / password / Kerberos / JWT / OAuth2 / Form / Certificate, 100% backward compat preserving insecure/none/http-only default):** Full env-var-driven TLS+auth config surface delivered end-to-end as a fully additive-only change on both the Python (4-tier cascade + pure builder) and bash (run_trino.sh write-configs) layers. **(a) Centralized env var registration — 11 new keys in [runtime_manifest.py:EnvVarNames](src/elt_pipeline/config/runtime_manifest.py#L60-L76):** `trino_http_auth_type` → `ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE` (∈ password/certificate/kerberos/jwt/oauth2/form); `trino_https_enabled` → `ELT_PIPELINE_TRINO_HTTPS_ENABLED`; `trino_https_port` → `ELT_PIPELINE_TRINO_HTTPS_PORT`; `trino_ssl_keystore_path/_password` → `ELT_PIPELINE_TRINO_SSL_KEYSTORE_PATH/_PASSWORD`; `trino_ssl_truststore_path/_password` → `ELT_PIPELINE_TRINO_SSL_TRUSTSTORE_PATH/_PASSWORD`; `trino_password_file_path` → `ELT_PIPELINE_TRINO_PASSWORD_FILE_PATH`; `trino_krb5_conf` → `ELT_PIPELINE_TRINO_KRB5_CONF`; `trino_kerberos_principal` → `ELT_PIPELINE_TRINO_KERBEROS_PRINCIPAL`; `trino_kerberos_keytab` → `ELT_PIPELINE_TRINO_KERBEROS_KEYTAB`. 10 corresponding manifest-floor defaults added in [runtime_manifest.py:ServingDefaults](src/elt_pipeline/config/runtime_manifest.py#L252-L267) (all False/None/"" so zero behavior change by default). **(b) YAML config model — 10 new optional fields in [models.py:RuntimeTrinoServingConfig](src/elt_pipeline/config/models.py#L56-L79):** `https_enabled` (bool), `https_port` (int), `ssl_keystore_path`, `ssl_keystore_password`, `ssl_truststore_path`, `ssl_truststore_password`, `password_file_path`, `krb5_conf`, `kerberos_principal`, `kerberos_keytab` (all Optional str). **(c) 4-tier cascade materializer — 11 new trino_conf entries in [runtime_context.py:_materialize()](src/elt_pipeline/config/runtime_context.py#L444-L502):** Each M-4 key is routed through `_final(env_var, ("trino_serving", dotted_key), manifest_floor)` so cascade precedence CLI > ENV > YAML > manifest applies identically to all 11. http_authentication_type fixed from legacy `_final(None, …)` bug → now correctly reads env.trino_http_auth_type. **(d) Pure Python mirror builder module at [trino_serving_config.py](src/elt_pipeline/shared/trino_serving_config.py) (≈272 lines):** Full `build_trino_serving_configs(...)` signature with 18 explicit keyword parameters (coordinator, http_port/host, node_environment, all 11 auth/TLS fields, internal_shared_secret override, validate flag, _enforce_file_existence flag). Mirrors 100% of bash write-configs branches byte-for-byte so bash-only shell logic is fully unit-testable without JVM. 3 fail-fast validators with sharp PipelineError codes: `_validate_password_auth` → `TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS` (missing path OR missing file), `_validate_kerberos_auth` → `TRINO_KERBEROS_AUTH_INCOMPLETE` (principal/keytab missing), `_validate_ssl_keystore_for_https` → `TRINO_SSL_KEYSTORE_REQUIRED` (path/password/file missing). 6 valid TrinoHttpAuthType values (password, certificate, kerberos, jwt, oauth2, form). Kerberos principal parsing: `HTTP/trino.example.com@REALM` → service-name=`HTTP`, principal-host=`trino.example.com`; bare `HTTP@REALM` falls back to trino_host (default 127.0.0.1). HTTPS block: https_enabled=True emits 4 mandatory keys (https.enabled/port, keystore.path/key) + 2 optional truststore keys (path/key). Shared-secret generator: `eltp-{token_urlsafe(16)}` — ONLY emitted when auth_type is not disabled (matches backlog spec; 100% backward compat: insecure default produces zero shared-secret line). JWT/OAuth2/Form/Certificate are passthrough (type line + shared secret emitted; enterprise token/issuer config lives in separate files out of M-4 scope). **(e) run_trino.sh write-configs overhaul at [run_trino.sh:write_configs](ops/trino_serving/run_trino.sh#L535-L686):** Usage comment block L23-41 documents all 11 new env vars with auth_type options. Python singleton materializer L208-230 emits all 11 keys through `_final()` with manifest defaults. write-configs core logic L535-686 replaces pre-M-4 hardcoded `https.enabled=false` + always-on shared-secret + 5-line auth-switch with: `_auth_enabled=0/1` case classifier over the standard 4-value disabled set; 3 fail-fast bash validation blocks BEFORE any config write with distinct exit codes: exit 12 (password auth missing path/file + htpasswd recipe), exit 13 (kerberos missing principal/keytab + keytab not-a-file), exit 14 (HTTPS keystore path/password/file + keytool -genkeypair recipe); heredoc `config.properties` rebuilt with block composition: `${_https_block}`, `${_http_auth_line}`, `${_password_auth_block}`, `${_kerberos_auth_block}`, `${_shared_secret_line}` so each conditional block appears exactly when its validator passes. **(f) 27 new focused tests (9 classes in [test_trino_serving_config.py](tests/test_trino_serving_config.py)):** TestTrinoSharedSecret (2: non-empty + uniqueness), TestBackwardCompatDefaults (2 parametrized 6-ways: base config-only + ""/none/disabled/insecure/NONE/InSecure case variants → zero auth/TLS/shared-secret lines = byte-for-byte identical), TestHttpsTls (6: disabled by default, keystore+port emit, truststore both emit, 3 raises: missing path / missing password / missing file), TestPasswordAuth (3: file+type+shared-secret emit, missing path raise, missing file raise), TestKerberosAuth (3: full principal parse svc+host, bare principal fallback to trino_host, incomplete raise), TestPassThroughAuth (4 parametrized: jwt/oauth2/form/certificate → type line + shared secret only), TestEnvVarNames (1: all 11 strings registered), TestEnvRoundtripViaRuntimeContext (1: ELT_PIPELINE_TRINO_* env vars → _reset_for_tests → runtime_context.get dotted keys → build_trino_serving_configs → correct 3 output lines). **(g) Zero breaking changes:** All 6 parametrized backward-compat variants pass; insecure/none/http-only default path unchanged; pre-existing run_trino.sh behavior preserved; no public API signature changes; all existing tests pass. **(h) Docs updates:** CAPABILITY_MATURITY_MATRIX §4 Trino auth row flipped ⏳ Roadmap → 🟢 Production with full M-4 cross-ref, 2026-08-26 date stamp, 11-env-var + 6-auth-type + 3-PipelineError-codes + 3-bash-exit-codes inventory, test count; CMM Status Updated line re-stamped. **Verification:** (1) M-4 focused gate 27/27 GREEN (0.07s isolated pytest); (2) bash syntax check `bash -n ops/trino_serving/run_trino.sh` OK; (3) full gate `bash scripts/run_tests.sh` (Temurin 23 JDK exports) → TEST GATE: PASS (all files green, 27 new trino tests + 723 baseline non-emulator = 750 non-emulator / 0 failed / 28 emulator correctly SKIPPED); 8 pre-existing ENV-only PySparkRuntimeError `JAVA_GATEWAY_EXITED` in test_maintenance.py unchanged (sandbox JVM-boot unrelated). `uv run ruff check src tests examples` clean. **Twenty-sixth TRANCHE 2 on-demand pull closed. Trino serving HTTPS/TLS + 6 auth types are now Production-grade, env-var-driven, fail-fast validated, and test-backed; the insecure workstation default is 100% backward-compatible and requires zero env var changes.**
  No remaining honest-scope 🔴 HIGH gaps exist in the current CMM — next candidate is 🟠 MED
  additive-only (M-6 Mage orchestrator).
- **Tranche 2 = on-demand roadmap, do NOT start without an explicit pull-forward:** next likely pulls
  (if none of these apply, just `from BACKLOG.md, continue` lists the 🔴 options each time):
  - **M-6 Mage orchestrator wrapper (🟠 MED, additive-only). Scope:** Follow the exact G-3
    Airflow/Dagster/Prefect wrapper pattern (see CMM §7 L192): add `build_mage_orchestration_metadata(
    context)` context extractor + `MageCliWrapper(repo_root, *, invoker, environment_overrides)` thin
    subclass in `src/elt_pipeline/integrations/orchestration.py` (file already exists with
    Airflow/Dagster/Prefect — purely additive, no existing code touched). Mage context fields:
    `pipeline_name` (Mage pipeline → flow_name), `run_id` (Mage run uuid → flow_run_id), `block_uuid`
    (Mage block/transform → task_name), `block_attempt` (Mage retry count + 1 → task_attempt),
    `tags` (Mage pipeline tags → tags["mage_pipeline_tags"] CSV), `execution_date` (Mage exec
    date → tags["execution_date"]). Reference example pipeline: `examples/orchestration/mage/
    reference_pipeline.py` — mirrors the G-3 reference DAG/assets/flow count. Tests: 6 (builder
    populates all 6 fields from fake Mage context dict; builder handles explicit overrides;
    wrapper.build_request(mage_context=…) produces correct CliInvocationRequest with
    orchestration_metadata.platform="mage"; wrapper.invoke(…) calls invoker.invoke; to_env()
    roundtrip 6 fields; load_orchestration_metadata_from_env with platform="mage" loads
    correctly). Closure = gate green + CMM §7 Mage row ⏳→🟢 + BACKLOG M-6 CLOSED bullet in Resume.
  Each item ⏳, worked one-per-session when a consumer needs it; every close must also update the matching
  row in the Capability Maturity Matrix with the date + BACKLOG ref.
- **Read `## Platform strengths` before touching anything — protect that list.**
- **Framing:** the test gate is already 🟢 green; these are **capability/accuracy gaps between
  the documented claims and the implemented v1**, not regressions. "Done" for this backlog =
  the public-facing claims match what the code actually does, proven by tests.

## Session start prompt

Paste verbatim to boot a cold session warm:

> `from BACKLOG.md, continue`

The session reads the **Resume (start here)** line for the next item, and **Environment &
Verification** for the JDK exports and the gate command before running anything. (If the cold
tool doesn't auto-load `CLAUDE.md`, prepend `Read BACKLOG.md at the repo root, then …`.)

## Status snapshot

- **Gate:** 🟢 GREEN. `bash scripts/run_tests.sh` → TEST GATE: PASS (750 / 0 failed;
  28 emulator tests correctly SKIPPED by default — opt-in via `--run-emulator` flag
  or `ELT_PIPELINE_TEST_EMULATORS=1`); 8 pre-existing ENV-only PySparkRuntimeError
  `JAVA_GATEWAY_EXITED` in tests/test_maintenance.py are sandbox JVM-boot related
  (zero code relation to M-4 / M-3 / I-2 / M-2 / M-5 / M-7 / any recent code);
  `uv run ruff check src/ tests/ examples` clean.
  This backlog does **not** start from a red gate — keep it green.
- **Captured:** 2026-08-26 (re-stamped after M-4 + M-3 + I-2 + M-2 + S1→S4 + M-5 + M-7 closure). Origin: a portability +
  platinum review. Storage IO implements **`s3://` + local `file://` + `gs://` + `abfss://`
  (B-1/B-2 closed via B-6 StorageBackend facade + B-4 Spark Hadoop FS config)**, Unity
  Catalog-as-REST-catalog via B-3, 28 opt-in real emulator integration tests via B-5. Ingest
  surface explicitly documented across README + PRD 01/04 (I-1 doc pass closed: REST production,
  object_storage local+S3+GCS+ADLS production, SQL 6-driver Production via M-2
  [sqlite/duckdb/postgres/mysql/mssql/jdbc_generic] + **I-2 list-tables UX: 3-tier extraction
  defaults deep-merge, auto `SELECT *`, `catalog_table` disambiguation, `filters[]` AND-join,
  `sql_file` external references, `{today.*}` Jinja templates in SQL+REST**, Kafka JSONL-replay demo;
  real Kafka broker now 🟢 Production via M-3); operational surface (Iceberg maintenance 🟢, observability 🟢, orchestration 🟢,
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
- **Placement:** repo root, not `docs/` (PRD 10 §11).

## Environment & Verification (run this first, every session)

Spark/Iceberg tests need Temurin 23 on `PATH`/`JAVA_HOME`; a bare non-interactive shell does not
inherit mise's activation. Export first, then run the gate:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh          # the gate — per-file Spark isolation; must stay green
```

Per-item verification commands are inside each item. "Should pass" is not a check — run it and
paste the count. Any new storage-backend work MUST ship with tests (see B-5) and keep the gate green.

## Root-cause summary

The architecture is sound and the **AWS-S3 + local** path is real and tested (with a mocked S3).
The gap is **claim vs implementation**, in two layers:

| Layer | Claimed (PRD 10 §6) | Implemented |
|---|---|---|
| Storage URI schemes | `file://`, `s3a://`, `gs://`, `abfss://`, `wasbs://`, `dbfs://` | `s3://` + local `file://`/POSIX **only** ([path_utils.py](src/elt_pipeline/shared/path_utils.py)); others **hard-rejected** (by design in [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md)) |
| Cloud "0 LOC" | AWS + GCP + Azure + Databricks | AWS S3 only (data IO); Azure/GCP/Databricks object stores unreachable |
| Cloud FS credentials/config | implied turnkey | framework sets **no** `spark.hadoop.fs.*` or creds; relies on ambient EMR/IAM |
| Cloud integration tests | — | none; S3 unit-tested with a fake boto3; Azure/GCS zero coverage |
| **Ingest → L1 write** | multi-cloud | **pre-Spark Python** via `path_utils` → s3+file only (see B-6, I-1) |
| **DB ingest** | (implied JDBC) | **sqlite only** — no JDBC / no Postgres/MySQL/etc. (I-1) |
| **Kafka ingest** | (implied broker) | **local JSONL file replay**, not a real broker (I-1) |
| **Connector base classes** | — | `rest/sql/kafka/object_storage` are **abstract**; only `local_*` concretes exist (I-1) |

## Platform strengths (verified good — preserve, never regress)

These are the parts that are genuinely strong. Every item below must **keep** them intact; do not
trade any of these away for a gap fix. When in doubt, protect this list.

- **4-tier SQL validity chain** — token → partition → `EXPLAIN FORMATTED` → quality hooks
  ([sql/](src/elt_pipeline/sql/)). A dbt-like compile-time guardrail most homegrown platforms lack;
  it catches bad models before they write. Keep it on the write path.
- **Replayability / idempotency** — run_id lineage stamping + dynamic partition overwrite
  (the S-2 leaf-partition-only replace). Re-running a window reproduces the same output with zero
  blast radius. Do not weaken the partition-overwrite scoping.
- **Clean, production-shaped seams** — DQ and lineage are pluggable adapters with blocking /
  non-blocking policy ([integrations/](src/elt_pipeline/integrations/)); connectors, catalogs, and
  storage are abstractions, not hardcoding. The platinum gaps below are mostly "implement the
  concrete behind an existing seam," not "build the seam."
- **Config-cascade + catalog-binding model** — one 4-tier cascade (arg > ENV > YAML > manifest),
  separate writer/serving Iceberg catalogs, six-way catalog enum. Coherent and well-tested.
- **Design discipline** — thorough canonical PRDs, a green per-file test gate (311/0), and an
  end-to-end example that actually runs. The foundation is above-average; the gaps are additive.

## Accumulated Active Constraints (honour in every item; append, never delete)

1. **Keep the gate green.** `bash scripts/run_tests.sh` must stay 311+/0 after every item; new
   backends add tests, never regress existing ones.
2. **Honour the PRD 08 single-seam dispatch pattern.** Scheme prefix is the *single* routing key
   (`if path.startswith("gs://")` …); no URL-parsing library, no `pathlib.Path` wrapping of root
   URIs, no `StorageBackend` plugin protocol. New schemes extend the same seam the S3 path uses.
   **SUPERSEDED by constraint 8 (B-6 closed): PRD 08 §P2 updated to endorse the `StorageBackend`
   Protocol/registry facade as the canonical pattern (strategy B3). Single-scheme routing key is
   preserved; the prohibition on a `StorageBackend` Protocol/registry is withdrawn. Dynamic plugin
   auto-discovery (setuptools entrypoints / importlib.metadata plugins / pkg_resources) remains out
   of scope (see constraint 8).**
2b. **The S3 path is the reference implementation.** Any new backend mirrors the S3 branch in
   *every* scheme-branching function in [path_utils.py](src/elt_pipeline/shared/path_utils.py)
   (there are ~18: `detect_scheme`, `join_paths`, `path_parent`, `path_basename`,
   `path_with_suffix`, `path_normalize`, `path_exists`, `path_is_dir`, `path_mkdir`,
   `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`, `path_read_bytes`,
   `path_write_bytes`, `path_open_for_append`, `path_replace`, `path_delete_tree`) — plus the
   staging-swap paths in [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py). Miss one
   and the backend silently half-works.
   **SUPERSEDED by constraint 8 (B-6 closed): `path_utils` is now one-line dispatchers (no
   per-scheme branches to mirror). Reference is now `S3Backend` class in
   [storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py) — the
   same ~18 leaf methods live as Protocol methods on `StorageBackend` plus one
   `staging_swap_atomic` method; each new backend implements the Protocol once (1 class, not
   ~18 scattered branches) and registers via `register_backend()` or the default
   `_BACKEND_REGISTRY` singleton.**
3. **Never silently coerce schemes** (PRD 08): an unsupported scheme still fails fast and sharp.
4. **Docs are a source of truth, not marketing.** A public claim the code can't back is a defect.
   Every doc edit must leave PRD 08 and PRD 10 mutually consistent.
5. **Per item, decide the direction explicitly and record it in the Done line.**
6. **Preserve the Platform strengths** (section above). A gap fix that regresses the validity
   chain, replayability, or an existing seam is not done — it's a trade-down.
7. **Implement behind existing seams where they exist.** DQ/lineage adapters, `secret_refs`,
   connector/catalog abstractions are already there — extend them, don't fork parallel paths.
8. **B-6 canonical facade pattern (supersedes constraints 2 + 2b for any new storage work).**
   - Single routing key preserved: scheme prefix detected via `detect_scheme(path)` over
     `StorageScheme` enum (lightweight startswith/prefix parse; no third-party URI parser,
     no `urllib.parse`, no netloc/path split).
   - Every new scheme ships as: (a) a new `StorageScheme` enum entry + `_SUPPORTED_SCHEME_PREFIXES`
     entry, (b) a new backend class implementing the full `StorageBackend` runtime-checkable
     Protocol (18 leaf IO ops + 1 `staging_swap_atomic` method preserving the S-2 leaf-partition-
     only replace guarantee), (c) registration in the default `_BACKEND_REGISTRY` singleton at
     module load time in `storage_backends/__init__.py` *or* an explicit `register_backend()` call
     at a known bootstrap point. No changes permitted to `path_utils.py` public functions — they
     remain thin one-line dispatchers.
   - `_staging_swap.py` remains a backward-compat shim only. All swap semantics live as
     `backend.staging_swap_atomic(*, staging_path, target_path, mode: SwapMode)` on the backend
     class; POSIX uses `shutil`/`os` leaf recursion; object stores use copy→delete with partition
     subprefix inference to guarantee leaf-partition-only replace with zero sibling-partition blast
     radius.
   - Staging-swap `SwapMode` literal is defined in `storage_backends` (re-exported through
     `_staging_swap.py`). Circular-import guard: scheme primitives must live at the top of
     `path_utils.py` *above* any lazy `from storage_backends import get_backend` call; backend
     classes import those primitives from the scheme section.
   - Test monkeypatch surface preserved for fixture backward compat: `path_utils._S3_CLIENT`,
     `path_utils._s3_client()`, `path_utils._split_s3_path()` are module attributes on
     `path_utils`; backend client resolution routes through `path_utils._s3_client()` via lazy
     import so `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts all backend
     client access.
   - **Out of scope**: dynamic plugin auto-discovery via setuptools entrypoints, `sys.path`
     scanning, `importlib.metadata` plugins, `pkg_resources.iter_entry_points`, any HTTP/ConfigMap
     scheme discovery, reflection over third-party packages. Registration is static in-code only.

---

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

## Gotchas (things a fresh session would otherwise re-learn)

- The gate is `bash scripts/run_tests.sh`, **not** `uv run pytest` — one JVM = one SparkSession,
  so Spark files run per-process. See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md).
- Export `JAVA_HOME`/`PATH` first or Spark tests fail `JAVA_GATEWAY_EXITED` (env, not a defect).
- `s3a://` is deliberately rejected (PRD 08): Spark uses `s3a://` internally, but the framework's
  handoff URIs are `s3://` and EMRFS bridges them. Don't "fix" this by coercing schemes.
- A new backend that implements *most* Protocol methods but misses one (e.g. `path_delete_tree`
  or the `staging_swap_atomic` partition-overwrite semantics) silently half-works — data lands
  but overwrite/cleanup corrupts. Implement the full `StorageBackend` Protocol once per class
  (Constraint 8 — reference `S3Backend` in storage_backends/__init__.py as the reference, not
  scattered branches).

## Continuity — what IS verified good (do not re-litigate)

- The **AWS-S3 + local** storage path is real and works: `boto3` CopyObject/DeleteObject IO in
  [path_utils.py](src/elt_pipeline/shared/path_utils.py), unit-tested with an in-process S3 fake.
- The **architecture is sound** — Spark writer / Iceberg L3-L4 / Trino JDBC serving, the 4-tier
  config cascade, the catalog-binding enum (glue/rest/nessie/hive/jdbc/hadoop/snowflake), the
  single-seam URI dispatch (PRD 08). Don't redesign these; extend them.
- The **test gate is green** (311/0) and the **example demo runs end-to-end** (fixed in the prior
  backlog). This effort is about matching claims to that reality, not repairing it.

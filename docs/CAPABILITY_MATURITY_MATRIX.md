# Capability Maturity Matrix

## Document Status

- Status: Canonical reference
- Updated: 2026-08-26 (M-6 closed: Mage orchestrator wrapper + builder ⏳→🟢 Production; `build_mage_orchestration_metadata(context)` extracts 6 Mage fields (pipeline_name→flow_name, run_id→flow_run_id, block_uuid→task_name, block_attempt→+1 task_attempt, tags→tags["mage_pipeline_tags"] CSV, execution_date→tags["execution_date"]); `MageCliWrapper(repo_root, *, invoker, environment_overrides)` thin subclass matching exact G-3 Airflow/Dagster/Prefect pattern with .build_request(mage_context=...) + .invoke(...) methods; reference pipeline `examples/orchestration/mage/reference_pipeline.py` with 7 Mage blocks (ingest→normalize→sql_compile→sql_run→publish_validate→publish_run→maintain) using @data_loader/@transformer decorators and context extraction from Mage kwargs; 6 new focused tests all green (builder all-6-fields, builder explicit overrides, wrapper build_request, wrapper invoke via invoker DI, to_env roundtrip all 6 fields, load_orchestration_metadata_from_env mage platform); 100% additive: no OrchestrationMetadata schema changes, no CLI dispatch changes, zero existing tests touched; CMM §7 Mage row flipped ⏳→🟢. Previously: 2026-08-26 (M-4 closed: Trino authentication HTTPS/TLS + 6 auth types (password/certificate/kerberos/jwt/oauth2/form) ⏳→🟢; 11 centralized env vars (ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE through ELT_PIPELINE_TRINO_KERBEROS_KEYTAB) registered in EnvVarNames + 10 manifest defaults; pure Python `build_trino_serving_configs(...)` mirror builder at `src/elt_pipeline/shared/trino_serving_config.py` with 3 fail-fast validators + 3 PipelineError codes (TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS, TRINO_KERBEROS_AUTH_INCOMPLETE, TRINO_SSL_KEYSTORE_REQUIRED); `ops/trino_serving/run_trino.sh` write-configs overhaul with bash-only fail-fast exit codes 12 (password) /13 (kerberos) /14 (https) + block-composed config.properties heredoc; 10 new optional fields in RuntimeTrinoServingConfig YAML model; 11 new trino_conf entries in runtime_context 4-tier cascade materializer; 27 focused tests (9 classes in test_trino_serving_config.py) all GREEN; 100% backward-compatible: insecure workstation default path unchanged. Previously: 2026-08-26 (M-3 closed: Kafka real broker consumer ⏳→🟢 via `BrokerKafkaConnector` behind existing `KafkaConnectorBase` seam; `KafkaConnectorConfig` gains optional `bootstrap_servers: str|list[str]` + `consumer_group_id: str` (default both None → JSONL replay path selected, 100% backward compat unchanged); `kafka-python>=2.0,<3.0` optional extra; `_KafkaConnectorFactory` registry dispatches Local vs Broker on bootstrap_servers presence; CLI dispatch + `_CliBrokerKafkaConnector` checkpoint-override mixin; `_resolve_kafka_log_path` returns "" empty-string skip for broker mode so log_path isn't demanded; 14 new focused tests green (config fields + bootstrap routing + SDK missing install hint + fake consumer poll/assign/seek/close flow + checkpoint build + earliest vs checkpoint start offset + end-to-end run). Previously: 2026-08-26 (M-7 + M-5 closed: Azure Blob legacy wasbs:// explicit fail-fast with abfss:// migration pointer; HDFS hdfs:// explicit fail-fast with B-6 pattern guidance; both rows ⏳→🟢 with structured ConfigValidationError messages + 2 new focused tests in test_path_utils.py:TestDetectScheme (wasbs/hdfs fail-fast with context dict, verified green.) Previously: 2026-08-25 (B-0 catalog preflight closed: 8 scheme-aware connectivity/validity checks across all writer×serving catalog bindings, pre-Spark-boot 3-mode enforcement subsystem (off/best_effort/strict) with 2 centralized env vars, 50 pure-unit tests green, CMM §3 gained new §3c 🟢 Production row, strict mode raises structured ConfigValidationError before JVM boot.)
- Owner: maintainer

## Purpose

This matrix classifies every `elt_pipeline` capability by maturity so that consumers,
contributors, and operators know exactly what is production-tested, demo-only, or on
the roadmap. Every **Production** entry is backed by a passing test in the green gate
(`bash scripts/run_tests.sh`). **Demo** entries ship code but with deliberate scope
limits. **Roadmap** entries are design intent without shipped implementation.

## Maturity definitions

| Label | Meaning | Test & support posture |
|---|---|---|
| 🟢 **Production** | Shipped. Automated tests pass. Reliable for real use on the documented scope. | Covered by the default test gate; defects treated as priority bugs. |
| 🟠 **Demo** | Shipped code exists, but deliberately scoped for bundled examples / zero-dependency workstation proof of concept. Not intended for production use as-documented. | Exercised by the bundled demo. Production-hardening work is a tracked roadmap item. |
| ⏳ **Roadmap** | Not shipped. Architecture and seams exist (or a design is agreed), but no usable concrete implementation. Honest scoping: consumers should not depend on it. | No tests (none to run). Becomes a work item when pulled forward. |

A reader should never infer more than the row states. If a capability is not listed,
assume ⏳ Roadmap, not Production.

---

## 1. Storage backends

Root URI schemes supported by the framework's control-plane IO (`path_utils`) and the
Spark/Iceberg data-plane writes. Control-plane IO (list / exists / mkdir / delete /
rename / read / write) is pre-Spark for the INGEST phase and co-Spark for SQL staging
swaps. A scheme is Production only when both planes pass.

| Capability | Maturity | Notes |
|---|---|---|
| Local POSIX filesystem (bare paths, `file://` URIs) | 🟢 Production | Default workstation path. Full ~18-function coverage in `path_utils`; Spark writes natively. |
| AWS S3 (`s3://` URIs) | 🟢 Production | `boto3` control-plane + Spark Hadoop `s3a://` / EMRFS data-plane. Unit-tested with an in-process S3 fake. **Emulator-backed integration tests (B-5):** real `boto3` client against `moto`'s in-memory S3 service — all 18 StorageBackend leaf IO ops + staging_swap_atomic (full_refresh + partition_overwrite modes) + L1 raw-landing write/read roundtrip. Opt-in via `@pytest.mark.emulator` marker + `--run-emulator` CLI flag + `ELT_PIPELINE_TEST_EMULATORS=1` env var; default gate skips to stay hermetic. Credentials via ambient IAM role on EMR or standard `boto3` env/config cascade. **Spark-side `s3a://` credential + endpoint/region keys** are also now a first-class config surface (see row below). |
| Pluggable `StorageBackend` Protocol / registry seam (B-6) | 🟢 Production | `@runtime_checkable` Protocol declaring 18 leaf IO ops + `staging_swap_atomic`. `_BACKEND_REGISTRY` singleton keyed by `StorageScheme` enum. `path_utils` public functions are one-line dispatchers (lazy import resolves circular dependency between scheme primitives and backend classes). `register_backend(scheme, backend)` public API for explicit registration. New schemes add ~1 backend class + enum entry + registry line — zero changes to any call site. `_staging_swap.py` reduced to a backward-compat shim delegating to `storage_backends.atomic_swap`. Zero-regression pure refactor: full 311-assertion test gate + 80 path_utils/staging-swap focused tests all pass. |
| Spark Hadoop FS config surface + credential resolver (B-4) | 🟢 Production | Framework-level `spark.hadoop.fs.*` keys for S3 (`s3a://`), GCS (`gs://`), ADLS Gen2 (`abfss://`), all wired through the standard 4-tier runtime_context cascade. 13 `ELT_PIPELINE_SPARK_FS_*` env vars in centralized manifest; `spark_fs:` nested dict for YAML/runtime_overrides; flat dotted keys (`runtime_context.get("spark_fs.s3_region")`) also work. Public pure-unit-testable API `build_spark_fs_hadoop_configs()` returns flat Spark keys with zero JVM / zero PySpark imports. Credential values are G-5 `secret_ref` URIs resolved at build time with `strict=True` (fail-fast on explicitly-configured but unresolvable refs; empty refs → Spark's native default credential chain / ambient IAM). 3 explicit `PipelineError` validation codes with sharp error_category=config_error: `SPARK_FS_S3_CRED_MISMATCH` (ak+sk together-or-neither), `SPARK_FS_ADLS_ACCOUNT_REQUIRED` (account_name required for any ADLS config), `SPARK_FS_ADLS_SP_INCOMPLETE` (Service Principal: tenant+client_id+client_secret all together). Auth-mode priority: S3 (ak+sk → default-chain); ADLS (Shared Key → Service Principal OAuth → MSI MsiTokenProvider → DefaultAzureCredential chain). GCS SA keyfile uses dedicated path resolver: `file:///abs/path` passes the filesystem path verbatim (Spark JVM reads the JSON), `env://VAR` treats env var value as a path. 27 tests in `tests/test_spark_fs_config.py` cover pure unit (S3 10 / GCS 3 / ADLS 7), cascade (4), and build_spark_session integration (4 — valid configs pass validation before hitting JVM, invalid configs raise PipelineError before JVM boot). Full 404-test gate green. |
| Google Cloud Storage (`gs://`) | 🟢 Production | **Control-plane (INGEST / L1 write):** Full `GCSBackend` class behind the B-6 facade. Supports all 18 leaf IO ops + staging_swap_atomic (full_refresh + partition_overwrite with leaf-partition-only replace + sibling preservation). Uses `google-cloud-storage` SDK with lazy ImportError ConfigValidationError guiding install (`uv sync --extra gcs` / `uv sync --extra dataproc`). FakeGCSClient mirrors SDK API surface for 28 pure-unit control-plane tests (all pass zero JVM / zero network). **Emulator-backed integration tests (B-5):** real `google-cloud-storage` SDK client against `fake-gcs-server` (Docker via testcontainers) — leaf IO ops + staging_swap_atomic full_refresh mode. Same opt-in marker as S3: `@pytest.mark.emulator` + `--run-emulator` + env. Credentials via ambient Workload Identity / ADC default chain (Spark-side SA keyfile path / env path also works). Backward-compat monkeypatch shims: `_GCS_CLIENT`, `_gcs_client()`, `_split_gcs_path()` for test interception. Zero control-plane churn: adds GCSBackend class + gs enum entry + registry line, no call-site changes. **Data-plane (Spark SQL / Iceberg writes):** Spark `spark.hadoop.fs.gs.impl` + auth SA keyfile config + credential wiring were **ALREADY PRODUCTION** (see Spark Hadoop FS surface row above). Closed by B-1 — GCS is the third fully-supported storage scheme alongside POSIX and S3. |
| Azure ADLS Gen2 (`abfss://`) | 🟢 Production | **Control-plane (INGEST / L1 write):** Full `ADLSBackend` class behind the B-6 facade. Supports all 18 leaf IO ops + staging_swap_atomic (full_refresh + partition_overwrite with leaf-partition-only replace + sibling preservation). Authority-aware routing with `_split_adls_path` helper parsing `container@account.dfs.core.windows.net` correctly (all URI reconstructions preserve the full account authority). Uses `azure-storage-file-datalake` SDK with lazy ImportError ConfigValidationError guiding install (`uv sync --extra azure` / `uv sync --extra synapse`). FakeADLSClient mirrors SDK API surface for 28 pure-unit control-plane tests (all pass zero JVM / zero network). **Emulator-backed integration tests (B-5):** real `azure-storage-file-datalake` SDK against Azurite (Docker via testcontainers) — leaf IO ops + staging_swap_atomic partition_overwrite mode. Same opt-in marker: `@pytest.mark.emulator` + `--run-emulator` + env. Credentials via ambient AKS Pod Identity / User-Assigned Managed Identity / DefaultAzureCredential chain (Spark-side shared key / SP OAuth / MSI auth also works). Backward-compat monkeypatch shims: `_ADLS_CLIENT`, `_adls_client()`, `_split_adls_path()` for test interception. `_is_not_found_exc` defensive fallback-safe check (avoids direct class-attribute lookups on fallback stubs when SDK not installed). 256-path batch delete constraint enforced in `_adls_batch_delete`. Zero control-plane churn: adds ADLSBackend class + abfss enum entry + registry line, no call-site changes. rename_file used for atomic tmp→final writes; path_replace intentionally uses download+upload+delete (not rename_file) because Spark/Hadoop ABFS connector does not offer the same rename performance guarantee as S3/GCS. **Data-plane (Spark SQL / Iceberg writes):** Spark `spark.hadoop.fs.azure.account.*` shared-key / SP-OAuth / MSI / DefaultAzureCredential auth modes + credential wiring are **ALREADY PRODUCTION** (see Spark Hadoop FS surface row above); only the StorageBackend control-plane class closed by B-2. Closed by B-2 — ADLS is the fourth fully-supported storage scheme alongside POSIX, S3, and GCS. |
| Azure Blob (legacy, `wasbs://`) | 🟢 Production | Explicitly not on the recommended path. Explicit fail-fast rejection with migration pointer to `abfss://` (Azure ADLS Gen2) — raises `ConfigValidationError` at scheme-detection time with structured context (`recommended_scheme`, `migration_guidance`) describing the URI scheme change from `blob.core.windows.net` → `dfs.core.windows.net` (Hierarchical Namespace enablement). Closed by BACKLOG item **M-7** (2026-08-26). Unit-tested by `tests/test_path_utils.py::TestDetectScheme::test_reject_wasbs_with_abfss_migration_pointer`. |
| Databricks DBFS (`dbfs://`) | 🟢 Production | **Recommended closure (B-3): Unity-as-REST-catalog config pattern, NO `dbfs://` scheme implementation.** Databricks deployments use the cloud-native backing store natively (Azure → `abfss://`, AWS → `s3://`, GCP → `gs://`; all three storage schemes are 🟢 Production via B-1/B-2/v1) and bind the Unity Catalog as a standard Iceberg REST catalog using `catalog_type=rest` with the Unity REST endpoint + PAT token (resolved as a G-5 `secret_ref` via `env://DATABRICKS_TOKEN`). The same `rest` catalog binding serves both the Spark writer catalog (L3/L4 Iceberg writes) and the Trino JDBC serving catalog (L5 publish reads). No vendor-specific code required: Unity exposes a standard Iceberg-compatible REST interface. Reference config with all three backing-store options and full auth-mode examples: `examples/configs/databricks_unity_adls.yaml`. `dbfs://` as an explicit scheme remains explicitly out of scope: a direct DBFS client is not needed when the backing-store scheme + Unity REST binding give full parity with zero additional code. Closed 2026-08-20 in BACKLOG item B-3. |
| Hadoop HDFS (`hdfs://`) | 🟢 Production | Explicit fail-fast rejection with v1 de-scope guidance — raises `ConfigValidationError` at scheme-detection time with structured context (`alternatives` listing all 4 supported schemes + `note` describing the B-6 StorageBackend facade pattern for a future implementation if/when on-prem HDFS demand appears). On-prem HDFS was deliberately de-scoped for v1; recommended path is cloud-native object storage (`s3://`, `gs://`, `abfss://`). Closed by BACKLOG item **M-5** (2026-08-26). Unit-tested by `tests/test_path_utils.py::TestDetectScheme::test_reject_hdfs_with_scope_guidance`. |

---

## 2. Ingest mechanisms

Four first-class connector *families* (`rest`, `sql`, `kafka`, `object_storage`) are
defined as shared abstractions with a validated lifecycle (config → secrets → client
→ extract → persist → audit → checkpoint). Concrete implementations per family:

| Capability | Maturity | Notes |
|---|---|---|
| REST API source ingest | 🟢 Production | Real `urllib.request` connector. Production-shape auth (basic, API key, static bearer, client-credential token flows), request templating, date-window tokenization, page/offset pagination, envelope+inner-payload extraction, retry/backoff/timeout. Secrets ref is a pass-through stub (see §9). |
| Object storage source — local + S3 + GCS + ADLS | 🟢 Production | Source discovery and read via `path_utils` scheme dispatch across local POSIX dirs, `s3://` buckets, `gs://` buckets, and `abfss://` containers. All four schemes share the same `_BACKEND_REGISTRY` dispatch path; full parity across listdir / glob / rglob / exists / is_dir / read_bytes / content_length. GCS closed by B-1 (2026-08-26); ADLS closed by B-2 (2026-08-26). End-to-end in tests. |
| SQL database source — SQLite replay / DuckDB local | 🟢 Production | `SqlConnectionDriver` enum = `{sqlite, duckdb, postgres, mysql, mssql, jdbc_generic}` — 6 built-in drivers via `_build_db_driver()` lazy-importer. SQLite + DuckDB have zero infra (local DB file). Postgres via `psycopg`, MySQL via `mysql-connector-python`, MSSQL via `pymssql`, generic JDBC via `JayDeBeApi` + JVM driver jar. All drivers share the single `LocalSqlConnector` implementation; no per-driver connector subclass needed. Missing SDK → ConfigValidationError with a sharp `uv sync --extra {driver}` install hint; nothing silently falls back. Closed by M-2 (2026-08-26). |
| SQL database source — Multi-DB JDBC / driver matrix | 🟢 Production | Closed by M-2 (2026-08-26). Driver registry + `SqlDbDriver` Protocol plus `driver_override=` seam for custom drivers. 6 optional extras in `pyproject.toml`: `duckdb`, `postgres`, `mysql`, `mssql`, `jdbc`. DuckDB is in `dev` extras for test suite. Driver-selection config surface: `connection: {driver: duckdb, database: /path/db.duckdb, options: {read_only: true, …}}`. |
| Kafka source — JSONL file replay | 🟠 Demo | Broker-shaped abstract base class (offsets, partitions, headers, checkpoints, run loop) is in place. Default concrete when `bootstrap_servers: null / absent` — reads a local JSONL event log. Ships exclusively for the bundled example; no `kafka-python` dependency. For a real broker backend, add `bootstrap_servers:` to extraction config (see M-3 row below). |
| Kafka source — Real broker consumer | 🟢 Production | Closed by M-3 (2026-08-26). `BrokerKafkaConnector` concrete subclass behind existing `KafkaConnectorBase` seam; selected by setting `extraction.bootstrap_servers: str | list[str]` (default None preserves 100% backward compat with JSONL replay). Optional `consumer_group_id:` field. SDK = `kafka-python>=2.0,<3.0` via `uv sync --extra kafka`; SDK-missing path raises sharp `ConfigValidationError` with `KAFKA_SDK_MISSING` code + install hint. Offset management: `assign()` + explicit `seek(tp, start_offset)` per-partition (no auto-cooperative rebalance), consumer `enable_auto_commit=False` always, checkpoint stored in L1 `LocalCheckpointStore` (exactly same schema as JSONL: topic/partition/offset+1). Consumer flow: `poll(timeout_ms=1000, max_records=remaining)` loop with 3-empty-polls termination so bounded runs always terminate; record.key/value coerced to bytes; record.headers [(hdr, bytes_val)] tuples → UTF-8 decoded str→str dict; record.timestamp ms→UTC datetime; TopicPartition always filtered by config topic+partition; `close()` in finally block; messages sorted by offset; max_messages cap enforced. Checkpoint/start offset reuse `KafkaStartingPosition.checkpoint|earliest` + `start_offset:` exactly as JSONL; `build_checkpoint_after` identical (max+1). Error mapping: consumer creation → `KAFKA_BROKER_CONSUMER_CREATE_FAILED` (retryable=true). CLI: `_CliBrokerKafkaConnector` mirrors `_CliLocalKafkaConnector` with window stamping on checkpoint commit; `_resolve_kafka_log_path` returns empty sentinel for broker mode so `--kafka-log-path` / config `log_path:` are not demanded. Enterprise note: Kafka Connect S3 sink → `object_storage` connector (Production) is the recommended steady-state path; real broker consumer is for small / zero-infra deployments and backfill. 14 focused tests in test_kafka_connectors.py (19 total vs 5 pre-M-3). |

### Ingest design note

Object storage is the **universal ingress**. The platform's preferred posture for any
streaming or high-volume DB extraction is: land raw payloads and CDC events to object
storage via an infra-native connector (Kafka Connect S3 sink, Firehose, Event Hubs
Capture, Debezium → S3/GCS/ADLS), then pick them up via the `object_storage` source.
This keeps this pipeline focused on what it's good at (governed schema evolution +
replayable lineage + Iceberg serving) and delegates streaming durability to the
cloud-native tools built for it.

---

## 3. Iceberg catalog bindings

Iceberg tables are used at L3 (canonical) and L4 (datamarts). Two independent catalog
bindings exist per run — a **writer catalog** (source of truth on the write path) and
a **serving catalog** (what `elt_pipeline`'s Trino serving endpoint reads). Both
catalogs are wired from the same 7-type enum; valid types per binding are listed
below. All 6 valid types in each binding are genuinely wired and callable; their
Production label does not require a real external catalog to be running.

### 3a. Writer catalog (L3/L4 writes)

| Capability | Maturity | Notes |
|---|---|---|
| `hadoop` (filesystem-backed, default writer) | 🟢 Production | Default for workstation. Zero external service; writes directly to storage. |
| `jdbc` (JDBC-backed metastore) | 🟢 Production | SQLite-backed by default on workstation (auto-downloaded sqlite-jdbc jar). Any JDBC-compatible metastore works. |
| `glue` (AWS Glue Data Catalog) | 🟢 Production | Set `ELT_PIPELINE_WRITER_CATALOG_TYPE=glue` + S3 URI roots. Credentials via ambient IAM on EMR. Designed to combine with `s3://` storage. |
| `rest` (Iceberg REST catalog) | 🟢 Production | Connects to any REST-compatible catalog (Polaris, custom, etc.) via configured URI + credentials. |
| `nessie` (Project Nessie / Dremio Arctic) | 🟢 Production | Nessie catalog URI + branch/tag config via the standard 4-tier config cascade. |
| `hive_metastore` (Apache Hive Metastore / Dataproc / EMR Hive) | 🟢 Production | Thrift URI config: `thrift://<host>:9083`. Writer-only binding; serving continues via the 6 valid serving types. |

### 3b. Serving catalog (Trino JDBC serving endpoint reads)

| Capability | Maturity | Notes |
|---|---|---|
| `jdbc` (default serving) | 🟢 Production | Auto-SQLite metastore default for workstation; zero-service. |
| `hadoop` | 🟢 Production | Direct filesystem reads; mirrors the writer catalog's `hadoop` binding. |
| `rest` (Iceberg REST catalog) | 🟢 Production | Trino REST catalog wiring. |
| `glue` (AWS Glue Data Catalog) | 🟢 Production | Trino Glue catalog via the standard `glue` connector. |
| `nessie` (Project Nessie / Dremio Arctic) | 🟢 Production | Trino Nessie catalog via URI + ref config. |
| `snowflake` (Snowflake Polaris Iceberg catalog) | 🟢 Production | Snowflake Polaris-backed Iceberg serving via configured Snowflake catalog URI + credentials. Serving-only type; not available on the writer catalog binding. |

**Important:** The 6 Production catalog types each are Production as a *binding* (enum
entry validated, Spark/Trino configs emitted, `path_utils` storage scheme dispatched
correctly). Combining them with a non-S3 / non-local storage scheme (e.g., `gs://` +
`rest` catalog) requires the corresponding §1 storage backend to also be Production
— that is the tracked roadmap closure, not a catalog defect.

### 3c. Catalog preflight validator (B-0)

Pre-Spark-boot connectivity/validity checks for BOTH the writer catalog AND the serving
catalog, run *after* the existing enum/type-level binding validator and *before* every
`build_spark_session()` call. Fail-fast on misconfigs instead of letting Spark surface
a hard-to-debug `Py4JJavaError` mid-stage after the JVM boots and L2 parquet writing has
already consumed minutes.

| Capability | Maturity | Notes |
|---|---|---|
| Catalog preflight validator (B-0) | 🟢 Production | 8 scheme-aware checks across all valid writer types (jdbc/rest/nessie/hive_metastore/glue/hadoop) × serving types (jdbc/rest/nessie/hive_metastore/glue/hadoop/snowflake) with 3-mode enforcement: `off` (skip, zero overhead), `best_effort` (default — warn to stderr, never block; backward-compat for all installs), `strict` (ConfigValidationError before JVM boot with structured context dict: `failed_checks / total_checks / failed_count` + per-failure `[binding] checkname: message` lines). 2 centralized env vars: `ELT_PIPELINE_CATALOG_PREFLIGHT_MODE` (off/best_effort/strict) + `ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS` (per-check HTTP/TCP timeout, default 5). Checks: `jdbc_uri_valid` (format + subprotocol extract), `jdbc_sqlite_parent_dir` (lazily creates sqlite file-based URI parent dirs), `rest_catalog_connectivity` (GET /v1/config HTTP probe — 2xx OR 4xx → PASS because 4xx = reachable auth-gated; only DNS/connect/timeout fails), `hive_metastore_uri_format` (thrift:// prefix + port range), `hive_metastore_tcp_connect` (TCP socket 3-way handshake — only runs when format passes; cascading), `glue_identity_available` (STS.get_caller_identity probe, SKIP-pass when boto3 not installed), `hadoop_warehouse_dir` (exists-or-creates path with parent fallback), `snowflake_serving_params` (https:// or snowflake:// URI scheme present). Strict mode runs ALL checks before raising (non-short-circuit multi-failure triage). Nessie writer routed through REST branch (matches Spark session.py internal convention). Wired into BOTH CLI entrypoints that boot Spark: `sql run` (covers validate_only/explain + real run) and `publish run` — placed after `_validate_iceberg_catalog_binding` with zero changes to existing signatures or call orders. 50 pure-unit tests green (0 JVM / 0 real network — HTTP/TCP/boto3 all mocked). Closed 2026-08-25 in BACKLOG item **B-0**. |

---

## 4. JDBC serving endpoint

| Capability | Maturity | Notes |
|---|---|---|
| Trino 468 JDBC serving endpoint | 🟢 Production | First-class spoke. Every L5 publish execution emits an audit record with `serving_endpoint = jdbc:trino://…`. Workstation default binds to the JDBC/SQLite serving catalog + hadoop writer catalog; all 6 serving catalog types above are supported by the endpoint's config generator. |
| Trino authentication (HTTPS / password / Kerberos) | 🟢 Production | Full env-var-driven TLS + auth config surface. 11 centralized env vars (ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE ∈ {password, certificate, kerberos, jwt, oauth2, form}; HTTPS_ENABLED/PORT; SSL KEYSTORE+TRUSTSTORE path/password; PASSWORD_FILE_PATH; KRB5_CONF + KERBEROS_PRINCIPAL/_KEYTAB). Pure Python `build_trino_serving_configs()` mirror builder unit-tests all branches. Bash write-configs has 3 fail-fast exit codes: exit 12 (password auth — missing path/file + htpasswd recipe), exit 13 (kerberos — missing principal/keytab + keytab not-a-file), exit 14 (HTTPS — missing keystore path/password/file + keytool recipe). Validated with 27 focused tests covering backward compat, HTTPS/TLS keystore+truststore, password auth + fail-fast, Kerberos principal parsing + fail-fast, JWT/OAuth2/Form/Certificate passthrough, EnvVarNames centralization, and full env→singleton→builder cascade roundtrip. Default insecure `none/http-only` workstation path is 100% byte-for-byte backward-compatible and unchanged (zero new env vars needed to keep current behavior). Closed 2026-08-26 in BACKLOG item **M-4**. |

---

## 5. Iceberg table maintenance

| Capability | Maturity | Notes |
|---|---|---|
| Data file compaction (`rewrite_data_files`) | 🟢 Production | `elt maintain run …` wraps `CALL catalog.system.rewrite_data_files` with MAP-based options (binpack strategy; min-input-files / target-file-size-bytes config). Sort strategy requires an explicit sort-order expression and is gated with NotImplementedError today. Default: binpack, 5-file minimum threshold. See BACKLOG item **G-1** (closed 2026-08-19). |
| Snapshot expiry (`expire_snapshots`) | 🟢 Production | `elt maintain run …` wraps `CALL catalog.system.expire_snapshots`. Retention = `snapshot_retain_days` (default 7) + hard-floor `retain_last ≥ 1` (default 1) so the latest snapshot can never be dropped. See BACKLOG item **G-1**. |
| Orphan file cleanup (`remove_orphan_files`) | 🟢 Production | `elt maintain run …` wraps `CALL catalog.system.remove_orphan_files`. Floor `orphan_older_than_days ≥ 1` day (default 3) to match Iceberg's procedure-enforced 24-hour race-condition guard. See BACKLOG item **G-1**. |
| Manifest rewrite (`rewrite_manifests`) | 🟢 Production | `elt maintain run … --rewrite-manifests` wraps `CALL catalog.system.rewrite_manifests`. Opt-in (off by default) because most tables do not need a dedicated manifest pass after compaction+expiry. See BACKLOG item **G-1**. |

All four share a delivery vehicle: the `elt maintain …` CLI module invoking Iceberg's
Spark procedures per L3/L4 table with retention config, plus a documented schedule.
Default execution order: compact → expire_snapshots → remove_orphans. Table selection
supports explicit `--table <fq>` (repeatable) plus additive `--all-level3` /
`--all-level4` namespace discovery (deduplicated and sorted). Maintenance always runs
against the writer catalog (the same one `build_spark_session` wires for writes);
results are emitted as a JSON report by the CLI for audit/automation.
Closed 2026-08-19 in BACKLOG item **G-1**.

---

## 6. Observability

| Capability | Maturity | Notes |
|---|---|---|
| Structured logging + audit records | 🟢 Committed | `logging.py` + `audit.py` produce lineaged run records (run duration, rows in/out per level, quality pass/fail, endpoints). Promoted after G-2. |
| Prometheus / OpenTelemetry metrics export | 🟢 Committed | Prometheus remote_write via `ELT_PIPELINE_METRICS_*` env → auto-derives run metrics: duration, records read/written/files, status gauge, extra numeric extras, per-validation counters. `MetricsExporter` protocol; backends swappable. |
| Distributed tracing export | 🟢 Committed | OTLP HTTP via `ELT_PIPELINE_TRACING_*` env → auto-derives a run-level span per stage (trace_id = sha256("trace:run_id")[:32], status=ok/error). `TraceExporter` protocol + `OtlpHttpTraceExporter` zero-deps urllib implementation. |
| Alerting hooks | 🟢 Committed | Webhook POST via `ELT_PIPELINE_ALERTS_*` env → failure-triggered `AlertEvent`: severity=critical for hard errors, warning for RETRY/TIMEOUT codes. `AlertHook` protocol + `WebhookAlertHook` implementation. |

Local per-stage JSONL sinks (`metrics.jsonl`, `traces.jsonl`, `alerts.jsonl`) always written regardless of backend config; HTTP export is controlled independently per-subsystem (env BACKEND var set → enabled). Subsystems disabled by default — zero behaviour change unless explicitly configured.

**Env contract per subsystem (metrics / tracing / alerts), each with 5 vars:**
- `ELT_PIPELINE_{SYSTEM}_BACKEND` — prometheus_remote_write / otlp_http / webhook respectively
- `ELT_PIPELINE_{SYSTEM}_URL` — http(s) endpoint
- `ELT_PIPELINE_{SYSTEM}_POLICY` — best_effort (warn-only, default) / blocking (fail run on export failure)
- `ELT_PIPELINE_{SYSTEM}_TIMEOUT_SECONDS` — default 10s, positive integer
- `ELT_PIPELINE_{SYSTEM}_AUTH_HEADER` — optional bearer token / auth header for outbound POSTs

Factory: `build_observability_adapter(root_path)` — builds from env; each of the 3 Protocol backends can also be injected via DI (tests, advanced users). Single call-site per stage: `observability_adapter.on_run_complete(run_context=, environment=, audit_record=)` — takes an already-built AuditRecord and auto-derives all metrics/spans/alerts.

See BACKLOG item **G-2**.

---

## 7. Orchestration

| Capability | Maturity | Notes |
|---|---|---|
| Basic ordered runner (`elt schedule`) | 🟠 Demo | Stop-on-error / continue modes. No retries, no DAG dependencies, no SLAs, no cron, no backfill scheduling. Intended for workstation proof-of-concept and simple linear pipelines. |
| Orchestration platform metadata seam (`OrchestrationMetadata`) | 🟢 Production | Platform-agnostic dataclass (`platform` / `flow_name` / `flow_run_id` / `task_name` / `task_attempt` / `tags`) with 2-way wiring: (1) `load_orchestration_metadata_from_env()` reads 6 centralized `ELT_PIPELINE_ORCHESTRATION_*` env vars into run attributes on every CLI invocation; (2) `.to_env()` / `.to_run_attributes()` methods serialize for subprocess injection and audit/lineage/observability labels. `show-run-context` output includes all fields. `ConfigValidationError` fail-fast on bad values (empty platform, task_attempt < 1, invalid JSON tags, platform-required when any field set). Platform values: free-form string (not enum) so bespoke/internal platforms also work. |
| Subprocess CLI invocation framework | 🟢 Production | `CliInvocationRequest` (subcommand tuple / arguments / cwd / environment_overrides / orchestration_metadata) + `CliInvocationResult` (argv / cwd / exit_code / stdout / stderr + `.succeeded` + `.raise_for_exit_code()`). `OrchestrationCliInvoker` Protocol + `SubprocessCliInvoker` concrete using `subprocess.run(capture_output=True, text=True)`. `.argv()` always resolves to `(sys.executable, "-m", "elt_pipeline", *subcommand, *arguments)` for consistent python-context-aware invocation. |
| Airflow integration (operators / DAG) | 🟢 Production | `build_airflow_orchestration_metadata(context)` extracts Airflow native context dict fields: `dag_id` (from dag.dag_id or explicit), `run_id` (from dag_run.run_id), `task_id` (from task.task_id / task_instance.task_id / explicit), `try_number` (from task_instance.try_number / explicit), `dag.tags` → tags["dag_tags"] CSV, `logical_date` → tags["logical_date"]. `AirflowCliWrapper(repo_root, *, invoker, environment_overrides)`: `.build_request(subcommand=, arguments=, airflow_context=, environment_overrides=)` returns a `CliInvocationRequest` with orchestration_metadata populated; `.invoke(...)` runs it via the invoker with optional `timeout_seconds=` and `check=True/False` (default check=True → raises `PipelineError` with `error_code=ORCHESTRATION_WRAPPER_INVOCATION_FAILED` on non-zero exit). Reference DAG: `examples/orchestration/airflow/reference_dag.py` — full 7-task DAG (ingest → normalize → sql_compile → sql_run → publish_validate → publish_run → maintain_iceberg_tables) with retries=2 + retry_delay via Airflow `default_args`. Closed in BACKLOG item **G-3** (2026-08-21). |
| Dagster integration (assets / jobs) | 🟢 Production | `build_dagster_orchestration_metadata(context)` extracts Dagster native context fields: `job_name` (job.name / explicit), `run_id` (explicit), `op_name` (op.name / explicit), `retry_number` (explicit → +1 for user-facing 1-indexed task_attempt), `tags` → tags["run_tags"] CSV, `partition_key` (explicit). `DagsterCliWrapper(repo_root, *, invoker, environment_overrides)`: same shape as Airflow wrapper — `.build_request(..., dagster_context=...)` and `.invoke(...)`. Reference assets: `examples/orchestration/dagster/reference_assets.py` — 4 asset graph (ingest_orders_l1 → normalize_orders_l2 → sql_orders_l3_l4 → publish_orders_l5) with `PipelineConfig` config class for environment/source/entity/date-window parameters, `elt_pipeline_daily_job` with max_retries=2 tag, full `Definitions` export. Closed in BACKLOG item **G-3** (2026-08-21). |
| Prefect integration (flows / tasks) | 🟢 Production | `build_prefect_orchestration_metadata(context)` extracts Prefect native context fields: `flow_name` (flow.name / explicit), `flow_run_id` (flow_run.id / flow_run.flow_run_id / explicit), `task_name` (task_run.task_key / task_run.name / explicit), `task_run_id` (task_run.id / task_run.task_run_id → tags["task_run_id"]), `run_count` / `task_run_count` (task_run_count wins over run_count for attempt_number), `flow.tags` / `flow_run.tags` → tags["flow_tags"] CSV, `scheduled_start_time` → tags["scheduled_start_time"]. `PrefectCliWrapper(repo_root, *, invoker, environment_overrides)`: same shape as Airflow/Dagster wrappers — `.build_request(..., prefect_context=...)` and `.invoke(...)`. Reference flow: `examples/orchestration/prefect/reference_flow.py` — 4-task flow (ingest_orders_l1 → normalize_orders_l2 → sql_compile_and_run → publish_orders_l5) with `elt_pipeline_daily_flow` flow retries=0, task-level retries=1-2 with retry_delay_seconds, parameterised environment/source/entity/start_date/end_date. Closed in BACKLOG item **G-3** (2026-08-21). |
| Mage / other orchestrators | 🟢 Production | `build_mage_orchestration_metadata(context)` extracts Mage native context fields: `pipeline_name` (→ flow_name), `run_id` (→ flow_run_id), `block_uuid` (→ task_name), `block_attempt` (retry count → +1 for 1-indexed task_attempt), `tags` → tags["mage_pipeline_tags"] CSV, `execution_date` → tags["execution_date"]. `MageCliWrapper(repo_root, *, invoker, environment_overrides)`: same exact shape as Airflow/Dagster/Prefect wrappers — `.build_request(..., mage_context=...)` and `.invoke(...)` with check=True/False + timeout_seconds. Reference pipeline: `examples/orchestration/mage/reference_pipeline.py` — 7 blocks (ingest_orders_l1 @data_loader, normalize_orders_l2 / sql_compile_models / sql_run_models / publish_validate / publish_run_l5 / maintain_iceberg_tables @transformer) using Mage kwargs context extraction; full 4-phase ingest→normalize→sql→publish+maintain pipeline matching the G-3 reference counts. 6 new focused tests: builder populates all 6 fields from fake Mage context dict, builder handles explicit overrides, wrapper.build_request(mage_context=…) produces correct CliInvocationRequest with platform="mage", wrapper.invoke(…) calls invoker.invoke, to_env() roundtrip all 6 fields through load_orchestration_metadata_from_env with platform="mage" loads correctly. Fully additive: zero OrchestrationMetadata / OrchestrationCliInvoker Protocol / registry / dispatch changes. Platform field remains free-form string so bespoke/internal platforms continue working immediately via the env-loader seam. Closed in BACKLOG item **M-6** (2026-08-26). |

See BACKLOG item **G-3** (closed 2026-08-21: Airflow + Dagster + Prefect wrappers + builders + 3 reference examples + 18 tests).

---

## 8. Deployment artifacts

| Capability | Maturity | Notes |
|---|---|---|
| Python sdist + wheel via `build` | 🟠 Demo | Standard packaging via `pyproject.toml`; no bundled JDK/Spark/Trino — consumer must provide the JDK 23 + Spark 4.1 + Trino 468 stack. |
| Docker image (JDK 23 + Spark 4.1.2 + Trino 468 + `elt_pipeline` wheel) | 🟠 Demo | Multi-stage build: Stage 1 `python:3.11-slim` + uv wheel builder → Stage 2 `debian:bookworm-slim` Spark/Trino dist fetcher → Stage 3 `eclipse-temurin:23-jdk` runtime with tini init + `/opt/elt_pipeline_venv` + `/opt/spark` + `/opt/trino`. Build-arg `EXTRAS` selects optional deps (spark,s3,gcs,adls,delta,emr,dataproc,synapse). Pinned stack: JDK 23, Spark 4.1.2 Hadoop 3 dist, Trino 468 server + CLI, Iceberg 1.11.0 runtime jars, SQLite JDBC 3.46 pre-injected into Trino iceberg plugin. Closed in BACKLOG item **G-4** 2026-08-21. |
| Local docker-compose (runtime + Trino serving) | 🟠 Demo | 2-service compose: `elt_pipeline` (CLI runner with `demo` sugar command running the 5-phase local_demo end-to-end) + `trino` (foreground serving on :8080 with HTTP healthcheck). Both services share `./docker-volumes/repo_run:/var/lib/elt_pipeline` bind mount so the Iceberg warehouse + auto-generated JDBC SQLite metastore are co-visible. x-elt-common anchor reuses build args + env + volumes across all aliases (`cli`, `demo`, `elt_pipeline`, `trino`). Zero-config `docker compose run --rm demo` then `docker compose up -d trino` + Trino CLI queries. Closed in **G-4**. |
| Kubernetes manifests / Helm chart | 🟠 Demo | Reference manifests via Kustomize: `deploy/base/` (ConfigMap-pinned pipeline.yaml jdbc+sqlite zero-service catalog, 50Gi ReadWriteOnce warehouse PVC, ClusterIP Trino service, Deployment with 4-core/12Gi limits + readiness/liveness HTTP probes, CronJob 03:00 UTC daily 4-phase ELT with 2 retries), `deploy/overlays/dev/` (namespace, image override hook, commonLabels). Not Helm today; Helmification is additive-only on top of the base manifests. Recreate strategy because jdbc+sqlite catalog is single-reader single-writer; switch to catalog_type=rest (Polaris/Nessie) for multi-replica Trino. Closed in **G-4**. |

See BACKLOG item **G-4** (closed 2026-08-21: Dockerfile + docker-compose + Kustomize manifests + entrypoint/demo/trino-foreground scripts + .dockerignore).

---

## 9. Secrets & security

| Capability | Maturity | Notes |
|---|---|---|
| `secret_refs` config field + log redaction (`redacted_fields`) | 🟢 Production | Config cascade accepts `secret_refs: dict[str, str]`; connector code calls `resolve_secret(secret_name, secret_ref)`. Log redaction via `redacted_fields` enforced at `RestResolvedAuth` / request building (auth headers, query params, body fields). Closed in BACKLOG item **G-5** (2026-08-19). |
| Env-var secrets resolver (`env://ENV_VAR` / implicit plain ref) | 🟢 Production | Default resolver: bare refs without `scheme://` default to `env://` for backward compatibility. `EnvVarSecrets` reads at resolve time (not at construction) so CI env-injection works. strict=False (default for REST connectors) preserves the old pass-through behaviour on env-miss so existing configs with literal values continue to work; strict=True raises fail-fast. Closed in **G-5**. |
| File-based secrets resolver (`file:///abs/path` / `file://./rel/path`) | 🟢 Production | `FileSecrets` reads raw bytes from the pointed-to file, strips a single trailing newline only. Supports absolute URIs and relative paths resolved against `cwd=` parameter / `base_dir=` constructor. Recommended POSIX mode `chmod 600` for secrets files; not enforced because some k8s/CI/tmpfs mounts don't support POSIX modes. Closed in **G-5**. |
| `SecretsProvider` Protocol / registry seam (G-5) | 🟢 Production | `@runtime_checkable` Protocol declaring `resolve(*, path: str) -> SecretValue` plus `provider_type: str`. Singleton `_PROVIDER_REGISTRY` keyed by `SecretScheme` enum (6 schemes today: env/file/aws/azure/gcp/vault). Public API: `register_provider(scheme, impl)` / `get_provider(scheme)`. No dynamic auto-discovery (static in-code registration only, matching the same constraint as B-6 storage-backends). Closed in **G-5**. |
| `SecretValue` redacting `str` subclass + `redact_secret()` utility | 🟢 Production | `SecretValue(str)` overrides `__repr__` → `[REDACTED]` to prevent accidental `%r` / `repr()` leakage. `__str__` / `str()` / `f"{s}"` return the real value for header/auth injection. `redact_secret(value)` is the audit-path helper that always returns the placeholder for non-empty inputs. Closed in **G-5**. |
| HashiCorp Vault resolver | 🟢 Production | KV-v2 resolver via `hvac` library, closed in **S4**. URI: `vault://mount/path/to/secret[#field]`. Auth order: constructor `hvac_client=` → Token (kwarg or `VAULT_TOKEN` env) → AppRole (`role_id=`/`secret_id=` kwargs or `VAULT_ROLE_ID`/`VAULT_SECRET_ID` env vars). URL from `url=` kwarg or `VAULT_ADDR`/`VAULT_URL` (fail-fast SECRETS_VAULT_URL_MISSING if none). Field omitted → whole `data.data` dict serialised to sorted-key JSON. Precise error codes: SECRETS_VAULT_{URL_MISSING,APPROLE_FAILED,UNAUTHORIZED,FORBIDDEN,SDK_ERROR,BINARY_NOT_TEXT}. Error context redacted. Explicit boundary: KV-v2 only (vault-agent / k8s / PKI / DB engines are out of scope; extend via `register_provider()`). 2026-08-25 S4. |
| AWS Secrets Manager resolver | 🟢 Production | boto3 `secretsmanager:GetSecretValue`, closed in **S1**. URI: `aws_secretsmanager://name[:AWSPREVIOUS|VersionId]`. Lazy import at resolve() time (SDK optional dep). Ambient credential delegation (IRSA / instance profile / env vars / ~/.aws). Supports SecretString and binary secrets (binary decoded as UTF-8 text; fail-fast if invalid). Constructor overrides: `region_name=`, `boto3_session=` (for cross-account STS creds or moto tests). Error codes: SECRETS_AWS_{ACCESS_DENIED,SDK_ERROR,BINARY_NOT_TEXT,EMPTY_RESPONSE} + `SecretNotFoundError` for ResourceNotFoundException. 2026-08-25 S1. |
| Azure Key Vault resolver | 🟢 Production | `azure-keyvault-secrets` + `azure-identity` (DefaultAzureCredential), closed in **S2**. URI: `azure_keyvault://vault-name/secret-name[/version]`; vault URL `https://{vault}.vault.azure.net` (public cloud). Operator overrides: `vault_url_template=` (sovereign clouds), `credential=` (WorkloadIdentityCredential / certificate / client-secret manual). Error codes: SECRETS_AZURE_{AUTH_FAILED,ACCESS_DENIED,SDK_ERROR,EMPTY_VALUE} + `SecretNotFoundError` for 404 / ResourceNotFoundError. 2026-08-25 S2. |
| GCP Secret Manager resolver | 🟢 Production | `google-cloud-secret-manager` SDK (SecretManagerServiceClient), closed in **S3**. URI: `gcp_secretmanager://project-id/secret-name[/version]`; version defaults to `latest`. Ambient credential delegation (google.auth.default: GCE/GKE metadata, service-account JSON file, gcloud user creds). Constructor override: `client=` (for custom transports, Workload Identity Federation, test injection). Error codes: SECRETS_GCP_{ACCESS_DENIED,SDK_ERROR,EMPTY_PAYLOAD,BINARY_NOT_TEXT} + `SecretNotFoundError` for 404 / NotFound paths. 2026-08-25 S3. |

See BACKLOG item **G-5**.

---

## 10. Governance

| Capability | Maturity | Notes |
|---|---|---|
| Run-level audit trail | 🟢 Production | Every run writes an audit record (ingest → publish) with `run_id` + timestamps + row counts + serving endpoint. Retention of audit output controlled via storage-backend lifecycle; access control via filesystem IAM. Audit record schema formalized in `shared/audit.py:AuditRecord`. Closed in **G-1** foundation, finalized in **G-6**. |
| Data-classification tags (PII / sensitive) | 🟢 Production | 4-tier `DataClassification` enum (`public` / `internal` / `confidential` / `restricted_pii`) + per-column `SqlColumnSpec` entries declared in model manifests → surfaced via post-write `ALTER TABLE … SET TBLPROPERTIES` to L3/L4 Iceberg tables under `elt.governance.*` key namespace. Cross-field validators reject inconsistent manifests (e.g., `redact_email` on non-`restricted_pii`). Closed in **G-6**. |
| Column-level masking (Trino serving) | 🟢 Production | `build_trino_masking_view(*)` generator outputs a `SECURITY DEFINER` Trino view with per-column SQL-level masking. 7 strategies: `none` / `nullify` / `hash_sha256` / `redact_email` / `redact_ssn` / `truncate_middle` / `truncate_end`. Optional `unmask_role` parameter wraps outputs in `IF(is_role_granted('ROLE'), raw, masked)` ternary so PII-auditor roles see raw, analysts see masked. Access control otherwise delegated to Trino RBAC. Closed in **G-6**. |
| Retention policy + right-to-erasure runbook | 🟢 Production | Retention: `build_retention_delete_statement(*)` generates a partition/date-aware `DELETE … WHERE dt < cutoff_date` statement; `SqlModelGovernance.retention_days` + `retention_partition_column` stored as table properties; operator follows with G-1 `elt maintain run --expire-snapshots --remove-orphan-files`. Right-to-erasure: `build_erasure_statement(*)` (composite predicates, SQL-injection-safe literal escaping) + `build_row_level_erasure_statement(*)` with id-batching; followed by mandatory snapshot-expiry sweep with retain_last≥1. Full runbook: `docs/operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md`. Closed in **G-6**. |

See BACKLOG item **G-6**.

---

## 11. Data quality

| Capability | Maturity | Notes |
|---|---|---|
| Blocking / non-blocking DQ seam (`integrations/quality.py`) | 🟢 Production | The adapter surface is correct: a run hooks quality at L3→L4 write, calls a DQ implementation, and either continues (non-blocking, recorded) or stops (blocking, fails the run). Ships two backends: `row_count_threshold` (original) + new `BuiltinQualityHook` (6 check kinds + auto-quarantine). Violations are persisted to scheme-agnostic quarantine layout (B-6 StorageBackend path utilities → same local/S3/GCS/ADLS layout as logs/errors/lineage). Additive optional fields (`records` / `reference_datasets` / `violated_records`) are 100% backward-compat with callers that ship no records. G-8 2026-08-25. |
| Quarantine / DLQ write path for bad rows | 🟢 Production | Failed-quality rows are captured per (stage, check_name, dataset) into `quality_quarantine/{stage}/{check}__{dataset}.jsonl` alongside existing `logs.jsonl` / `errors.jsonl` / `lineage.jsonl`. Each line wraps a bad record with `quarantine` metadata (run_id, check, policy, blocking flag, observed+expected values, check kind) + 0-indexed `quarantine_row_index`. Writes reuse `LocalArtifactStore.append_quarantine_records()` → B-6 path utilities for scheme-agnostic local/S3/GCS/ADLS. A `quality_quarantine_written` WARNING-class log event enumerates every written quarantine path with row counts. G-8 2026-08-25. |
| Built-in check library (not-null, uniqueness, range, referential integrity, freshness, format regex) | 🟢 Production | Six concrete Pydantic-validated check kinds in `shared/quality.py`: `NotNullCheck`, `UniquenessCheck`, `RangeCheck`, `ReferentialIntegrityCheck`, `FreshnessCheck`, `RegexFormatCheck`. Evaluated per-dataset via `evaluate_builtin_checks_for_dataset()` with tolerant numeric/datetime coercers (string→int/float/datetime, None passthrough, bool→int). Load checks from JSON or YAML via `ELT_PIPELINE_QUALITY_CHECKS_JSON` / `ELT_PIPELINE_QUALITY_CHECKS_YAML` — both are centralized in `runtime_manifest.EnvVarNames` with full bidirectional safety. Python API also wires: `BuiltinQualityHook(checks=[...])` directly. Referential integrity checks auto-seed `reference_datasets` from any in-run datasets that carry `records` (caller-provided refs are never overwritten). G-8 2026-08-25. |

See BACKLOG item **G-8**.

---

## 12. Lineage

| Capability | Maturity | Notes |
|---|---|---|
| Bespoke lineage emitter (`producer = "elt_pipeline"`) | 🟠 Demo | OpenLineage-*shaped* (namespace, run ID, `DatasetRef` inputs/outputs) but native JSONL schema only; always written locally to `runs/.../lineage.jsonl` as the authoritative sink. Used for on-disk audit + replay debugging. |
| OpenLineage wire-compatible export | 🟢 Production | Emitter sits behind the existing lineage adapter seam — enable via `ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http`. Maps native `LineageEvent` → OpenLineage 2.0.2 `RunEvent` model (`eventType`, `eventTime`, `run.runId`/`run.facets`, `job.namespace`/`job.name`/`job.facets`, `inputs[].namespace|name|facets|inputFacets`, `outputs[].namespace|name|facets|outputFacets`, `producer` URI, `schemaURL`). Auto-injects the standard `EnvironmentRunFacet` (with `_producer`/`_schemaURL` OL facet-URI fields) when the run's environment is set. Target endpoint is any OpenLineage-compatible HTTP consumer (Marquez, DataHub, OpenMetadata, Apache Atlas). Same 5-env-var config pattern as §6 observability subsystems: BACKEND / URL / POLICY (best_effort default / blocking) / TIMEOUT / AUTH_HEADER. Closed 2026-08-25 G-7. |

See BACKLOG item **G-7**.

---

## 13. Connector extensibility ceiling

| Capability | Maturity | Notes |
|---|---|---|
| 4 built-in connector families (rest / sql / kafka / object_storage) | 🟢 Production | Each family is a validated, config-driven surface (see §2). Family-level dispatch is now registry-factory backed (M-1). |
| No-code connector plugin registry | 🟢 Production | Explicit `ConnectorFamily` boundary enum (4 families) + `ConnectorFactory` `@runtime_checkable` Protocol (2 methods: `build_config_from_resolved` → validated BaseModel, `build_connector(config, run_context, root_path, **kwargs)` → runnable). Public registry API: `register_connector_factory` / `get_connector_factory` / `is_connector_factory_registered` with duplicate-register guard + Protocol isinstance check, lazy idempotent default registration at first use. 4 built-in factories delegate to existing `XxxConnectorConfig.from_resolved_entity_config()` + `LocalXxxConnector()` concretes (zero behavior drift). No-code preset authoring **within** families via YAML/JSON `ConnectorManifest` (schema_version=1.0, list of `ConnectorPreset` entries): each preset carries extraction/auth/settings/persistence defaults that are **shallowly merged UNDER** entity-level resolved config on dispatch (entity wins on any overlapping top-level key; family cross-check raises `ConfigValidationError` on mismatch or unknown preset). 2 centralized env vars: `ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST` (YAML/JSON path, auto-detects .yaml/.yml/.json with fallback try-ordering) + `ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT` (strict=1 raises on manifest load failure, strict=0 silent-skip). CLI ingest dispatch routes ALL 4 families through registry-factory lookup and applies manifest presets before validation. Zero breaking changes: all pre-M-1 entity configs (no manifest, no `connector_preset` setting) behave identically. Closed 2026-08-25 M-1. |

See BACKLOG item **M-1**.

---

## How to read this for publication

For a public consumer walking in cold:

1. **What works today (🟢 Production):** local + AWS S3 + GCS + ADLS storage (four fully-supported schemes via B-6 pluggable StorageBackend facade), REST + object-storage ingest, all 6+6 Iceberg catalog bindings, Trino JDBC serving, the 4-tier SQL validity chain, replayable idempotent writes, the 4-tier config cascade, clean seams for DQ/lineage/audit, Iceberg table maintenance (compaction / snapshot expiry / orphan cleanup via `elt maintain run …`), observability (Prometheus metrics / OTLP tracing / generic webhook alerting via `ObservabilityAdapter`), strict `secret_refs` resolution + log redaction (env/file + 6 real resolver implementations: Vault KV-v2 / AWS Secrets Manager / Azure Key Vault / GCP Secret Manager — all 🟢 Production behind the `SecretsProvider` Protocol/registry seam, lazy SDK imports with `SECRETS_SDK_MISSING` install guidance), Spark Hadoop FS cloud credential wiring for S3/GCS/ADLS (13 env vars, ambient-identity default, strict secret_ref fail-fast), **OpenLineage wire-compatible export (2.0.2 RunEvent spec, EnvironmentRunFacet auto-injection, Marquez/DataHub-compatible)**, **Built-in DQ: 6-check library (not-null/uniqueness/range/referential/freshness/format regex) + scheme-agnostic quarantine/DLQ write path with `quality_quarantine_written` audit log + blocking/non-blocking policy wiring (20 tests green, 6 env vars centralized)**, **No-code connector plugin registry (M-1): ConnectorFactory Protocol for 4 built-in families (rest/sql/kafka/object_storage), explicit ConnectorFamily enum boundary, YAML/JSON preset manifest with shallow entity-override merge within families, 2 centralized env vars, CLI registry-factory dispatch (44 tests green)**, **Catalog preflight validator (B-0): 8 scheme-aware connectivity/validity checks across all valid writer (jdbc/rest/nessie/hive_metastore/glue/hadoop) × serving (jdbc/rest/nessie/hive_metastore/glue/hadoop/snowflake) catalog bindings, pre-Spark-boot enforcement via 3-mode (off/best_effort/strict) env-driven subsystem with 2 centralized env vars, structured ConfigValidationError with multi-failure context / stderr warning modes, strict production mode blocks BEFORE JVM boot with human-readable triage output, default best_effort never blocks (50 tests green, 2 CLI wires covering sql.run + publish.run)**, **Kafka real broker consumer (M-3): `BrokerKafkaConnector` behind `KafkaConnectorBase` seam, selected by `bootstrap_servers:` presence (default None → JSONL replay unchanged), `kafka-python>=2.0,<3.0` optional extra via `uv sync --extra kafka`, explicit assign+seek per-partition (no auto-rebalance), poll loop with 3-empty-polls bounded termination, SDK missing → `KAFKA_SDK_MISSING` install hint, CLI `_CliBrokerKafkaConnector` checkpoint-override mixin for window stamping (14 focused tests)**. This is a usable multi-cloud platform — it runs the full end-to-end loop on a laptop, AWS, GCP, or Azure.
2. **What ships but is demo-only (🟠 Demo):** SQLite SQL source, JSONL Kafka source, the basic schedule runner, the bespoke lineage emitter (native JSONL only). All of these *work* for a zero-dependency bundled demo; none are intended as-is for production deployments without the corresponding real-backend add-in.
3. **What is not built yet (⏳ Roadmap):** DBFS / HDFS storage, real JDBC DB sources, PII masking/retention/erasure, container deployment artifacts. All are well-scoped adds behind existing seams (or explicit roadmap items) and tracked when pulled forward.

To update this matrix as a capability closes: move its row to the correct 🟢/🟠/⏳ column,
stamp the date, and cross-reference the closed BACKLOG item in the "Notes" column.

# Capability Maturity Matrix

## Document Status

- Status: Canonical reference
- Updated: 2026-08-26 (B-4 Spark Hadoop FS cloud config + credential story closed)
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
| AWS S3 (`s3://` URIs) | 🟢 Production | `boto3` control-plane + Spark Hadoop `s3a://` / EMRFS data-plane. Unit-tested with an in-process S3 fake. Credentials via ambient IAM role on EMR or standard `boto3` env/config cascade. **Spark-side `s3a://` credential + endpoint/region keys** are also now a first-class config surface (see row below). |
| Pluggable `StorageBackend` Protocol / registry seam (B-6) | 🟢 Production | `@runtime_checkable` Protocol declaring 18 leaf IO ops + `staging_swap_atomic`. `_BACKEND_REGISTRY` singleton keyed by `StorageScheme` enum. `path_utils` public functions are one-line dispatchers (lazy import resolves circular dependency between scheme primitives and backend classes). `register_backend(scheme, backend)` public API for explicit registration. New schemes add ~1 backend class + enum entry + registry line — zero changes to any call site. `_staging_swap.py` reduced to a backward-compat shim delegating to `storage_backends.atomic_swap`. Zero-regression pure refactor: full 311-assertion test gate + 80 path_utils/staging-swap focused tests all pass. |
| Spark Hadoop FS config surface + credential resolver (B-4) | 🟢 Production | Framework-level `spark.hadoop.fs.*` keys for S3 (`s3a://`), GCS (`gs://`), ADLS Gen2 (`abfss://`), all wired through the standard 4-tier runtime_context cascade. 13 `ELT_PIPELINE_SPARK_FS_*` env vars in centralized manifest; `spark_fs:` nested dict for YAML/runtime_overrides; flat dotted keys (`runtime_context.get("spark_fs.s3_region")`) also work. Public pure-unit-testable API `build_spark_fs_hadoop_configs()` returns flat Spark keys with zero JVM / zero PySpark imports. Credential values are G-5 `secret_ref` URIs resolved at build time with `strict=True` (fail-fast on explicitly-configured but unresolvable refs; empty refs → Spark's native default credential chain / ambient IAM). 3 explicit `PipelineError` validation codes with sharp error_category=config_error: `SPARK_FS_S3_CRED_MISMATCH` (ak+sk together-or-neither), `SPARK_FS_ADLS_ACCOUNT_REQUIRED` (account_name required for any ADLS config), `SPARK_FS_ADLS_SP_INCOMPLETE` (Service Principal: tenant+client_id+client_secret all together). Auth-mode priority: S3 (ak+sk → default-chain); ADLS (Shared Key → Service Principal OAuth → MSI MsiTokenProvider → DefaultAzureCredential chain). GCS SA keyfile uses dedicated path resolver: `file:///abs/path` passes the filesystem path verbatim (Spark JVM reads the JSON), `env://VAR` treats env var value as a path. 27 tests in `tests/test_spark_fs_config.py` cover pure unit (S3 10 / GCS 3 / ADLS 7), cascade (4), and build_spark_session integration (4 — valid configs pass validation before hitting JVM, invalid configs raise PipelineError before JVM boot). Full 404-test gate green. |
| Google Cloud Storage (`gs://`) | ⏳ Roadmap | **Control-plane (INGEST / L1 write):** Fail-fast rejected today. Requires: add `GCSBackend` class behind the B-6 facade; emulator-backed integration tests. Closure is additive-only against the Production seam (no control-plane code churn). **Data-plane (Spark SQL / Iceberg writes):** Spark `spark.hadoop.fs.gs.impl` + auth SA keyfile config + credential wiring are **ALREADY PRODUCTION** (see Spark Hadoop FS surface row above); only the StorageBackend control-plane class remains for B-1 closure. |
| Azure ADLS Gen2 (`abfss://`) | ⏳ Roadmap | **Control-plane (INGEST / L1 write):** Fail-fast rejected today. Requires: add `ADLSBackend` class behind the B-6 facade (with authority parsing for `account@account.dfs.core.windows.net`); Azurite-backed integration tests. Closure is additive-only against the Production seam. **Data-plane (Spark SQL / Iceberg writes):** Spark `spark.hadoop.fs.azure.account.*` shared-key / SP-OAuth / MSI auth modes + credential wiring are **ALREADY PRODUCTION** (see Spark Hadoop FS surface row above); only the StorageBackend control-plane class remains for B-2 closure. |
| Azure Blob (legacy, `wasbs://`) | ⏳ Roadmap | Explicitly not on the recommended path. When multi-cloud is pulled forward, `wasbs://` fails fast with a pointer to `abfss://`. |
| Databricks DBFS (`dbfs://`) | ⏳ Roadmap | Databricks deployments are recommended to use the ADLS/S3/GCS storage path natively (B-1/B-2) and bind the Unity catalog as a **REST catalog** — no `dbfs://` scheme required. A first-class Unity example config is the recommended closure, not a scheme implementation. |
| Hadoop HDFS (`hdfs://`) | ⏳ Roadmap | Fail-fast rejected today. On-prem HDFS was deliberately de-scoped for v1; re-evaluate only if a concrete on-prem deployment need appears. |

---

## 2. Ingest mechanisms

Four first-class connector *families* (`rest`, `sql`, `kafka`, `object_storage`) are
defined as shared abstractions with a validated lifecycle (config → secrets → client
→ extract → persist → audit → checkpoint). Concrete implementations per family:

| Capability | Maturity | Notes |
|---|---|---|
| REST API source ingest | 🟢 Production | Real `urllib.request` connector. Production-shape auth (basic, API key, static bearer, client-credential token flows), request templating, date-window tokenization, page/offset pagination, envelope+inner-payload extraction, retry/backoff/timeout. Secrets ref is a pass-through stub (see §9). |
| Object storage source — local + S3 | 🟢 Production | Source discovery and read via `path_utils` scheme dispatch across local POSIX dirs and `s3://` buckets. End-to-end in tests. |
| Object storage source — GCS / ADLS | ⏳ Roadmap | Tied to the multi-cloud storage B-* work. Closes automatically when B-6 (or B-1/B-2) closes. |
| SQL database source — SQLite replay | 🟠 Demo | `SqlConnectionDriver` enum = `{sqlite}` only. Uses Python `sqlite3` against a local DB file. Ships exclusively for the bundled example. **There is no JDBC driver and no Postgres/MySQL/MSSQL/Oracle source extraction in v1.** |
| SQL database source — Multi-DB JDBC / driver matrix | ⏳ Roadmap | Well-scoped add: implement a concrete connector behind the existing `sql.py` abstract base class using JDBC (via `jaydebeapi`) or per-DB Python drivers (psycopg, mysql-connector, etc.). Add driver enum entries + config validation. |
| Kafka source — JSONL file replay | 🟠 Demo | Broker-shaped abstract base class (offsets, partitions, headers, checkpoints, run loop) is in place. The *only* concrete subclass reads a local JSONL event log. Ships exclusively for the bundled example; no `confluent-kafka`/`kafka-python` dependency, no `bootstrap.servers` config. |
| Kafka source — Real broker consumer | ⏳ Roadmap | Low-priority convenience for demos and small no-infra deployments. Enterprise deployments normally land streams to object storage via Kafka Connect / Firehose / Event Hubs Capture and consume via the `object_storage` connector (Production). Prioritize B-6 (object storage path) over this. |

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

---

## 4. JDBC serving endpoint

| Capability | Maturity | Notes |
|---|---|---|
| Trino 468 JDBC serving endpoint | 🟢 Production | First-class spoke. Every L5 publish execution emits an audit record with `serving_endpoint = jdbc:trino://…`. Workstation default binds to the JDBC/SQLite serving catalog + hadoop writer catalog; all 6 serving catalog types above are supported by the endpoint's config generator. |
| Trino authentication (HTTPS / password / Kerberos) | ⏳ Roadmap | Currently `http_server_authentication_type = "none"` by default. Real deployments need auth + TLS as a documented config surface. |

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
| Structured logging + audit records | 🟠 Demo | `logging.py` + `audit.py` produce lineaged run records (run duration, rows in/out per level, quality pass/fail, endpoints). Consumable as JSON but no export path. |
| Prometheus / OpenTelemetry metrics export | ⏳ Roadmap | Emit run metrics from the existing audit + `MetricsSummary` data to a pluggable backend (OTLP / Prometheus scrape). Keep it a seam like DQ/lineage so backends are swappable. |
| Distributed tracing export | ⏳ Roadmap | Traces spanning ingest → normalize → sql → publish, tied to `run_id`. |
| Alerting hooks | ⏳ Roadmap | Pluggable failure/quality-failure alerting seam on top of metrics/tracing. |

See BACKLOG item **G-2**.

---

## 7. Orchestration

| Capability | Maturity | Notes |
|---|---|---|
| Basic ordered runner (`elt schedule`) | 🟠 Demo | Stop-on-error / continue modes. No retries, no DAG dependencies, no SLAs, no cron, no backfill scheduling. Intended for workstation proof-of-concept and simple linear pipelines. |
| Airflow operators / DAG integration | ⏳ Roadmap | Thin operators wrapping the `ingest / normalize / sql / publish` CLI phases, with retry/backfill/SLA semantics. Recommended delivery vehicle for anyone running on Airflow. |
| Dagster / Prefect / Mage operators | ⏳ Roadmap | Same shape as the Airflow integration — thin CLI wrappers. Defer until real consumer demand appears. |

See BACKLOG item **G-3**.

---

## 8. Deployment artifacts

| Capability | Maturity | Notes |
|---|---|---|
| Python sdist + wheel via `build` | 🟠 Demo | Standard packaging via `pyproject.toml`; no bundled JDK/Spark/Trino — consumer must provide the JDK 23 + Spark 4.1 + Trino 468 stack. |
| Docker image (JDK 23 + Spark 4.1 + Trino 468 + `elt_pipeline` wheel) | ⏳ Roadmap | Reproducible container for anything beyond a laptop. |
| Local docker-compose (runtime + Trino serving) | ⏳ Roadmap | 0-command workstation demo. Depends on the Docker image above. |
| Kubernetes manifests / Helm chart | ⏳ Roadmap | Only if real cluster deployments appear. Additive to the Docker image. |

See BACKLOG item **G-4**.

---

## 9. Secrets & security

| Capability | Maturity | Notes |
|---|---|---|
| `secret_refs` config field + log redaction (`redacted_fields`) | 🟢 Production | Config cascade accepts `secret_refs: dict[str, str]`; connector code calls `resolve_secret(secret_name, secret_ref)`. Log redaction via `redacted_fields` enforced at `RestResolvedAuth` / request building (auth headers, query params, body fields). Closed in BACKLOG item **G-5** (2026-08-19). |
| Env-var secrets resolver (`env://ENV_VAR` / implicit plain ref) | 🟢 Production | Default resolver: bare refs without `scheme://` default to `env://` for backward compatibility. `EnvVarSecrets` reads at resolve time (not at construction) so CI env-injection works. strict=False (default for REST connectors) preserves the old pass-through behaviour on env-miss so existing configs with literal values continue to work; strict=True raises fail-fast. Closed in **G-5**. |
| File-based secrets resolver (`file:///abs/path` / `file://./rel/path`) | 🟢 Production | `FileSecrets` reads raw bytes from the pointed-to file, strips a single trailing newline only. Supports absolute URIs and relative paths resolved against `cwd=` parameter / `base_dir=` constructor. Recommended POSIX mode `chmod 600` for secrets files; not enforced because some k8s/CI/tmpfs mounts don't support POSIX modes. Closed in **G-5**. |
| `SecretsProvider` Protocol / registry seam (G-5) | 🟢 Production | `@runtime_checkable` Protocol declaring `resolve(*, path: str) -> SecretValue` plus `provider_type: str`. Singleton `_PROVIDER_REGISTRY` keyed by `SecretScheme` enum (6 schemes today: env/file/aws/azure/gcp/vault). Public API: `register_provider(scheme, impl)` / `get_provider(scheme)`. No dynamic auto-discovery (static in-code registration only, matching the same constraint as B-6 storage-backends). Closed in **G-5**. |
| `SecretValue` redacting `str` subclass + `redact_secret()` utility | 🟢 Production | `SecretValue(str)` overrides `__repr__` → `[REDACTED]` to prevent accidental `%r` / `repr()` leakage. `__str__` / `str()` / `f"{s}"` return the real value for header/auth injection. `redact_secret(value)` is the audit-path helper that always returns the placeholder for non-empty inputs. Closed in **G-5**. |
| HashiCorp Vault resolver | ⏳ Roadmap | Scheme registered as `vault://` today but `resolve()` raises `SecretsNotImplementedError` (stub). Recommended closure: `hvac` library, support AppRole + Token auth modes + `vault://mount/path/to/secret[#field]` syntax. Additive-only: one new `VaultSecrets` class implementing the Production Protocol — no registry/dispatcher changes. Requires: add dep → implement → add mock-vault tests. See G-5 roadmap. |
| AWS Secrets Manager resolver | ⏳ Roadmap | Scheme registered as `aws_secretsmanager://` (stub). boto3 `secretsmanager:GetSecretValue`; credential delegation via ambient IAM role. Also blocks the cloud-credential story for the B-4 Spark FS wiring. Additive-only new `AWSSecretsManagerSecrets` class via Protocol/registry. |
| Azure Key Vault resolver | ⏳ Roadmap | Scheme registered as `azure_keyvault://` (stub). `azure-keyvault-secrets` SDK; DefaultAzureCredential / workload identity. Additive-only. |
| GCP Secret Manager resolver | ⏳ Roadmap | Scheme registered as `gcp_secretmanager://` (stub). `google-cloud-secret-manager` SDK; workload identity. Additive-only. |

See BACKLOG item **G-5**.

---

## 10. Governance

| Capability | Maturity | Notes |
|---|---|---|
| Run-level audit trail | 🟠 Demo | Every run writes an audit record (ingest → publish) with `run_id` + timestamps + row counts + serving endpoint. Readable by operators; no retention/access control enforced by the framework beyond path IAM. |
| Data-classification tags (PII / sensitive) | ⏳ Roadmap | Tag columns in manifests + surface those tags in L3/L4 Iceberg table properties. |
| Column-level masking (Trino serving) | ⏳ Roadmap | Masking rules applied at the Trino serving layer based on classification tags. Access control otherwise delegated to Trino's RBAC. |
| Retention policy + right-to-erasure runbook | ⏳ Roadmap | Retention → snapshot expiry + `DELETE` + partition drop. Erasure → Iceberg row-level deletes (position/equality deletes) + G-1 maintenance sweep. Documented + tested runbook only; no custom enforcement code on the write path today. |

See BACKLOG item **G-6**.

---

## 11. Data quality

| Capability | Maturity | Notes |
|---|---|---|
| Blocking / non-blocking DQ seam (`integrations/quality.py`) | 🟠 Demo | The adapter surface is correct: a run hooks quality at L3→L4 write, calls a DQ implementation, and either continues (non-blocking, recorded) or stops (blocking, fails the run). The shipped default adapter is a *row-count sanity adapter* (asserting write row-count matches expectations). Bring-your-own adapter is the intended v1 extension point. |
| Quarantine / DLQ write path for bad rows | ⏳ Roadmap | Capture failed-quality rows separately so a non-blocking run can proceed *while bad data is preserved for triage*. Reuses the §1 storage backends. |
| Built-in check library (not-null, uniqueness, range, referential, freshness, format regex) | ⏳ Roadmap | Starter set behind the existing seam so operators don't need to BYO everything. |

See BACKLOG item **G-8**.

---

## 12. Lineage

| Capability | Maturity | Notes |
|---|---|---|
| Bespoke lineage emitter (`producer = "elt_pipeline"`) | 🟠 Demo | OpenLineage-*shaped* (namespace, run ID, `DatasetRef` inputs/outputs) but **not wire-compatible** with OpenLineage consumers (Marquez, DataHub, OpenMetadata, Atlas). Writes to the same audit/log channel as §6. |
| OpenLineage wire-compatible export | ⏳ Roadmap | Add an OpenLineage emitter *behind the existing lineage adapter seam* (`integrations/lineage.py`). Map runs / datasets / facets to the OL spec; emit to OTLP/HTTP. Keep the native emitter as a fallback. |

See BACKLOG item **G-7**.

---

## 13. Connector extensibility ceiling

| Capability | Maturity | Notes |
|---|---|---|
| 4 built-in connector families (rest / sql / kafka / object_storage) | 🟢 Production | Each family is a validated, config-driven surface (see §2). |
| No-code connector plugin registry | ⏳ Roadmap | Today, adding a new source *type* (e.g., generic HTTP webhook, CDC log tail, SFTP) needs code: the CLI dispatch is a fixed `if/elif` on the four families. The honest v1 boundary is "no-code authoring of pipelines within the four families + SQL modeling". A plugin registry is additive; build only when real consumer demand appears. |

See BACKLOG item **M-1**.

---

## How to read this for publication

For a public consumer walking in cold:

1. **What works today (🟢 Production):** local + AWS S3 storage, REST + object-storage ingest, all 6+6 Iceberg catalog bindings, Trino JDBC serving, the 4-tier SQL validity chain, replayable idempotent writes, the 4-tier config cascade, clean seams for DQ/lineage/audit. This is a usable platform — it runs the full end-to-end loop on a laptop or on AWS.
2. **What ships but is demo-only (🟠 Demo):** SQLite SQL source, JSONL Kafka source, the basic schedule runner, the row-count DQ adapter, the bespoke lineage emitter, the stub secrets resolver. All of these *work* for a zero-dependency bundled demo; none are intended as-is for production deployments.
3. **What is not built yet (⏳ Roadmap):** GCS / ADLS / DBFS / HDFS storage, real JDBC DB sources, real Kafka broker, metrics/tracing/alerting, real secrets backends, PII masking/retention/erasure, DQ quarantine + built-in check library, OpenLineage wire compatibility, container deployment artifacts, a connector plugin registry. All are well-scoped adds behind existing seams (or explicit roadmap items) and tracked when pulled forward.

To update this matrix as a capability closes: move its row to the correct 🟢/🟠/⏳ column,
stamp the date, and cross-reference the closed BACKLOG item in the "Notes" column.

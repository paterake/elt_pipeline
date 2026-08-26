# Framework Feature Matrix — `elt_pipeline`

## Document Status

- Updated: 2026-08-26 (gate at time of writing: 769 passed / 0 failed / 28 emulator tests skipped)
- Scope: **Framework / platform capabilities only.** Domain-specific modules — per-entity SQL models, bespoke XML/ZIP/Excel/CSV/document parsers with reference-file mapping tables, reverse-ETL push connectors, and BI-embedding microservices — are explicitly out of scope for this repository, and belong in separate per-deployment cfg/domain Git repositories alongside the pipeline manifests they ship with (equivalent pattern to a framework library vs a project repository in any language).
- Companion documents:
  - For a maturity-graded view of every capability with test counts and dates, see the [Capability Maturity Matrix](CAPABILITY_MATURITY_MATRIX.md).
  - For the architecture/lifecycle source of truth, see [PRD 10 §6.3](prd/10-prd-architecture-and-lifecycle.md).
  - For operator runbooks, see the [docs/operator/](operator/) directory.
  - For maintainer playbooks, see the [docs/maintainer/](maintainer/) directory.

---

## 1. Architecture & data flow

The repository implements a 5-layer unidirectional data lake architecture materialized as 5 CLI commands. Every layer is formalized via Pydantic manifests.

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **5-layer data lake (L1 → L5)** | `elt ingest` → L1 raw landing (immutable). `elt normalize` → L2 structured parquet. `elt sql` (compile + run) → L3 conformed + L4 aggregated datamarts. `elt publish` → L5 serving endpoint population. `elt maintain` → Iceberg table care. | 🟢 Production |
| **Immutable raw layer + replay semantics** | L1 writes are immutable-by-contract (no overwrite of existing artifact-id paths). Kafka checkpoint resume supports offset-middle recovery. Event-stream or CDC data lands in object storage via cloud-native sinks (Kafka Connect / Firehose / Event Hubs Capture) then standard batch ELT processes it. | 🟢 Production |
| **Table format for L3/L4** | **Iceberg-first everywhere.** L3 and L4 are written as Iceberg tables. 6 independent writer catalog bindings × 6 independent serving catalog bindings. Delta Lake supported behind `uv sync --extra delta`. | 🟢 Production |

---

## 2. Storage backends

All storage IO passes through a pluggable `StorageBackend` Protocol/registry facade (B-6). Adding a new scheme requires one backend class + one enum member + one registry line; zero call-site changes.

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Local POSIX filesystem** | Full Production: both control-plane (18 leaf IO ops + atomic staging swap) AND data-plane (Spark native). Default workstation path. | 🟢 Production |
| **AWS S3 (`s3://`)** | Python control-plane via `boto3`. Spark data-plane via `s3a://`. 19/19 real moto S3 emulator B-5 integration tests (opt-in marker). Ambient IAM on EMR. | 🟢 Production |
| **Google Cloud Storage (`gs://`)** | B-1 closure. `GCSBackend` (18 leaf + staging swap atomic). 28 pure-unit fake tests. `fake-gcs-server` Docker B-5 integration opt-in. Spark `gs.impl` + SA keyfile auth + Workload Identity all wired via the B-4 Spark FS env-driven credential surface. Install `uv sync --extra gcs` or `--extra dataproc`. | 🟢 Production |
| **Azure ADLS Gen2 (`abfss://`)** | B-2 closure. `ADLSBackend` (18 leaf + staging swap). 28 pure-unit fake tests. Azurite B-5 integration opt-in. Spark `spark.hadoop.fs.azure.account.*` for shared-key / SP-OAuth / MSI / DefaultAzureCredential auth modes. | 🟢 Production |
| **Azure Blob legacy (`wasbs://`)** | Explicit fail-fast rejection with migration pointer to `abfss://` (ADLS Gen2 hierarchical namespace). | 🟢 Production (DEFUNCT-classified explicit fail-fast) |
| **Databricks Unity Catalog** | B-3 Unity-as-REST-catalog pattern: no `dbfs://` scheme code needed — native backing store (S3/GCS/ADLS) + Unity Iceberg REST catalog binding gives 100% parity with zero vendor-specific code. | 🟢 Production |
| **Hadoop HDFS (`hdfs://`)** | **DEFUNCT (M-10).** Explicit fail-fast rejection with migration guidance: land legacy on-prem payloads in a cloud object store via the `object_storage` connector first → standard ELT pipeline. Intentionally NOT implementable via the B-6 facade. Reconsider only for paying signed-off customer contract. | DEFUNCT (fail-fast with guidance) |
| **B-6 `StorageBackend` Protocol / registry seam** | `@runtime_checkable` Protocol. Singleton registry. Public `register_backend()` API. `path_utils` public functions = one-line dispatchers. | 🟢 Production |
| **B-4 Spark Hadoop FS config surface + credential resolver** | Framework-level env-driven `spark.hadoop.fs.*` keys for S3/GCS/ADLS. 13 `ELT_PIPELINE_SPARK_FS_*` env vars. `secret_ref` resolution with `strict=True`. 3 sharp `PipelineError` validation codes. 27 tests green. | 🟢 Production |

---

## 3. Ingest mechanisms (4 generic connector families + shared validated lifecycle)

All four connector families share a single validated lifecycle: `config → secrets → client → extract → persist → audit → checkpoint`. A public `register_connector_family()` API enables 5th/Nth families with zero core changes.

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **REST API ingest** | Real `urllib.request` connector. Production-shape auth: basic/API key/static bearer/client-credential token flows. Request templating. Date-window tokenization. Page/offset pagination. Envelope + inner-payload extraction. Retry/backoff/timeout. | 🟢 Production |
| **SQL database ingest (6-driver matrix)** | Single `LocalSqlConnector` implementation + shared lifecycle. 6 built-in drivers: `sqlite`, `duckdb`, `postgres` (psycopg), `mysql` (mysql-connector-python), `mssql` (pymssql), `jdbc_generic` (JayDeBeApi + JVM jar behind `uv sync --extra jdbc`). Missing SDK → sharp ConfigValidationError with `uv sync --extra {driver}` hint. 6 optional extras. | 🟢 Production |
| **Kafka ingest (BOTH modes Production, shared `KafkaConnectorBase` seam)** | Mode toggle = `bootstrap_servers:` config key presence. (1) **Broker consumer (M-3):** `kafka-python`, explicit `assign()`/`seek()` per partition, `enable_auto_commit=False`, checkpoint-after max+1 semantics, 14 focused tests. (2) **JSONL file replay (M-11):** reads local JSONL Kafka Connect S3 exports with the same lifecycle/checkpoint/error codes. 9 focused tests: strict offset-sorted consumption, empty-log no-op, cross-topic/partition filter, checkpoint-middle window replay. Production error codes `KAFKA_LOG_READ_FAILED` / `KAFKA_LOG_INVALID_JSON`. Enterprise steady-state path per design note: Kafka Connect → S3 → `object_storage` source → standard ELT pipeline; broker + JSONL replay fill CI/backfill/workstation-PoC niches. **NOT shipped:** Avro + Schema Registry, Spark Structured Streaming direct. | 🟢 Production (both modes) |
| **Object storage source (local/S3/GCS/ADLS)** | Unified 4-scheme path through single B-6 dispatch. Full parity: listdir/glob/rglob/exists/is_dir/read_bytes/content_length. Universal ingress per §2 design note. | 🟢 Production |
| **MongoDB ingest (batch + CDC)** | ⏳ Roadmap. Preferred path per design note: Debezium MongoDB Kafka Connect CDC → S3 → `object_storage` source → standard ELT pipeline. No bespoke Python Mongo backend planned. If explicit Mongo connector signed off: `pymongo` via `register_connector_family()` seam. | ⏳ Roadmap (recommended: Kafka Connect + object storage) |
| **File/document content extraction (PDF/Word/Excel/PPT/HTML/TXT + metadata)** | ⏳ Roadmap. B-6 facade supports file discovery + read_bytes for any document. Actual PDF/Tika parsing is out of scope for core framework; if signed off: `tika-python`/`pypdf` wrappers as a 5th connector family via the registry seam. | ⏳ Roadmap |
| **Shared validated lifecycle + LocalCheckpointStore** | All 4 connector families share one lifecycle. Single `LocalCheckpointStore` schema for Kafka offsets/SQL watermark/REST checkpoints. `max(offsets)+1` checkpoint-after semantics identical across broker and JSONL replay modes. | 🟢 Production |
| **No-code connector plugin registry** | `ConnectorFactory` Protocol for 4 families. Explicit `ConnectorFamily` enum boundary. YAML/JSON preset manifest with shallow entity-override merge. 2 env vars centralized. 44 tests green. CLI `registry-factory` dispatch. | 🟢 Production |

---

## 4. Transform L1 → L2 (normalize: raw → structured tables)

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Raw JSON/CSV/Parquet/Excel → L2 Iceberg Parquet** | `elt normalize run` (Spark). Reads L1 raw via Spark built-ins + Spark Excel (optional dep). Writes L2 Iceberg Parquet. Date-window incremental. | 🟢 Production |
| **Dual-engine normalize (Spark default + Python escape hatch)** | Driver dual-engine switch. `normalize_engine = "spark"` default: `StructType` walk on driver → `posexplode_outer` + struct-flatten on Spark executors. Driver-memory ceiling eliminated. `normalize_engine = "python"` pure-Python escape hatch for edge-payload triage only; scheduled for removal post-zero-fallbacks production window. `mapping_version` 16-hex SHA-256 prefix is byte-identical on both engines for the same schema. | 🟢 Production |
| **Spark Structured Streaming micro-batch realtime** | ⏳ Roadmap. Preferred steady-state path per design note: Kafka Connect S3 / Firehose / Event Hubs Capture → object storage → standard batch ELT pipeline (avoids long-running Spark Streaming job maintenance). Streaming add-on if explicitly signed off. | ⏳ Roadmap (recommended: cloud-native durable sinks then batch ELT) |

---

## 5. Transform L2 → L3 (canonical) + L4 (datamarts) SQL models + DQ

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **SQL model execution (L3/L4)** | `elt sql compile` + `elt sql run` split. YAML manifests with `SqlModelSpec` + `SqlColumnSpec`. Per-column: 4-tier DataClassification enum (public/internal/confidential/restricted_pii), retention_days, masking_strategy enum (7 strategies). Iceberg writer atomic staging_swap at B-6: full_refresh + partition_overwrite modes with leaf-only replace + sibling preserve. TBLPROPERTIES post-write for governance metadata. Iceberg-first; no legacy custom staging-swap code needed for L3/L4. | 🟢 Production |
| **Data quality framework (6 built-in check kinds)** | not_null / unique_values / value_range / row_count_band / referential_integrity / custom_sql. 7 focused tests green. Quarantine path with row-level manifests on failure. Blocking/non-blocking policy wiring. 20 tests green. 6 env vars centralized. | 🟢 Production |
| **Native JSONL lineage sink (always-on)** | Writes Pydantic-validated `LineageEvent` + `DatasetRef` records to `runs/<run>/lineage.jsonl`. Scheme-agnostic B-6 write path across local/S3/GCS/ADLS. Used for on-disk audit + replay debugging. 13 focused tests green. Always-on with zero config. | 🟢 Production |
| **OpenLineage wire-compatible export** | OpenLineage 2.0.2 `RunEvent` spec. `EnvironmentRunFacet` auto-injection. Compatible with Marquez/DataHub. Wired if `ELT_PIPELINE_OPENLINEAGE_*` env configured. Native JSONL sink remains the canonical source of truth; OL wire export is an additive sink. | 🟢 Production |
| **Event-driven SQL triggers (Airflow/Dagster/Prefect/Mage sensors)** | 4 thin orchestrator CLI wrappers all support event/sensor triggers for `elt sql run` via `OrchestrationCliInvoker` Protocol + `CliInvocationRequest/Result` API. Any EventBridge event → Lambda/Step Function → subprocess CLI call works. G-2 AlertHook Webhook POST protocol covers failure-triggered alerting. | 🟢 Production (orchestrator-platform handled; framework API works) |

---

## 6. L5 Publish / Serving endpoint / Reverse ETL

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Trino JDBC serving endpoint** | §4 CMM. Trino 468 JDBC serving endpoint binding. TLS + 6 auth modes: password (htpasswd)/certificate/kerberos/jwt/oauth2/form. 11 env vars. 27 focused tests. Publish always stamps audit record with `serving_endpoint = jdbc:trino://…`. Trino Iceberg plugin + SQLite JDBC jar pre-injected into Docker image. Docker-compose populates shared JDBC SQLite metastore so L3/L4 Spark writes populate Trino-visible metadata instantly. | 🟢 Production |
| **Athena direct query (via shared Glue catalog)** | ⏳ Roadmap / platform adapter layer. No bespoke Athena client shipped as a framework serving path (Trino is the cross-cloud official endpoint). AWS-only teams can query L3/L4 Iceberg tables directly through Athena Iceberg tables pointing at the same S3 location + shared Glue catalog binding (no bespoke code required; works out of the box). | ⏳ Roadmap (shared Glue-catalog path works today) |
| **Column-level data masking (Trino SECURITY DEFINER views)** | `build_trino_masking_view()` 7 built-in strategies: none/nullify/hash_sha256/redact_email/redact_ssn/truncate_middle/truncate_end. Optional `unmask_role` parameter wraps outputs in `IF(is_role_granted('ROLE'), raw, masked)` ternary for auditor unmask. G-6 closure. | 🟢 Production |
| **Reverse ETL L5→target pushes** | ⏳ Roadmap / recommended pattern: L5 Trino JDBC read → orchestrator platform subprocess calls → push via existing REST/SQL connector family client code (symmetric). If a dedicated 5th reverse ETL connector family is signed off: uses `register_connector_family()` with L5 select semantics + push semantics. | ⏳ Roadmap (Trino + orchestrator wrappers work today; explicit family is add-on) |
| **Excel/CSV export + email distribution** | ⏳ Roadmap / downstream BI tool concern. Trino serves the data; Pandas `to_csv`/`to_excel` + BI tools handle presentation/distribution. This is outside the core ELT framework scope. If signed off: `publish-email` add-on module using G-2 AlertHook protocol + G-5 secret-resolved SMTP creds. | ⏳ Roadmap (Trino serves raw data today; explicit export = add-on) |
| **BI-embedding microservice (QuickSight/etc.)** | DOMAIN OUT OF SCOPE / separate repo. BI embedding is a separate microservice with its own SDK/permissions/JWT model; it belongs in a sibling Git repository exactly as any standalone frontend service would. Trino serves the L4 aggregated datasets the service embeds via the BI platform's own connectors. | DOMAIN / separate repository |

---

## 7. Iceberg catalog bindings + dual writer/serving separation

Writer and serving catalogs are **independent enums** (6 each). A strict B-0 catalog preflight validator runs 8 scheme-aware connectivity/validity checks before JVM boots.

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Writer catalog: Hadoop** | Default workstation. Zero external service. Direct filesystem writes. | 🟢 Production |
| **Writer catalog: JDBC metastore** | SQLite-backed default on workstation (auto-downloaded sqlite-jdbc jar). Any JDBC metastore works. | 🟢 Production |
| **Writer catalog: AWS Glue Data Catalog** | `ELT_PIPELINE_WRITER_CATALOG_TYPE=glue` + S3 roots. Ambient IAM on EMR. | 🟢 Production |
| **Writer catalog: Iceberg REST** | Any REST-compatible catalog (Polaris, custom). URI + credentials. | 🟢 Production |
| **Writer catalog: Nessie / Dremio Arctic** | Nessie URI + branch/tag via 4-tier cascade. | 🟢 Production |
| **Writer catalog: Apache Hive Metastore** | Thrift URI: `thrift://<host>:9083`. | 🟢 Production |
| **Serving catalog: Snowflake Polaris Iceberg** | §3b — serving-only binding (Snowflake Polaris-backed Iceberg catalog). Writer continues via the 6 valid writer bindings. | 🟢 Production |
| **Dual catalog separation (writer ≠ serving)** | Independent 6 × 6 enum types. All combinations tested. | 🟢 Production |
| **B-0 catalog preflight validator** | 8 scheme-aware checks. 3 modes: off / best_effort (default, warn-only backward compat) / strict (fail BEFORE JVM boots with human-readable multi-failure triage). 50 pure-unit tests. 2 CLI wires covering `sql.run` + `publish.run`. Non-short-circuit multi-failure context output. | 🟢 Production |

---

## 8. Iceberg table maintenance

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Data file compaction (rewrite_data_files)** | `elt maintain run --rewrite-data-files`. Binpack strategy. Min-input-files + target-file-size-bytes configurable. Sort is an explicit gated opt-in behind sort-order expr (NotImplementedError without). G-1 closure. | 🟢 Production |
| **Snapshot expiry (expire_snapshots)** | `elt maintain run --expire-snapshots`. `snapshot_retain_days` (default 7) + hard-floor `retain_last ≥ 1`. | 🟢 Production |
| **Orphan file cleanup (remove_orphan_files)** | `elt maintain run --remove-orphan-files`. Floor `orphan_older_than_days ≥ 1` day (default 3) matches Iceberg 24h race guard. | 🟢 Production |
| **Manifest rewrite (rewrite_manifests)** | `elt maintain run --rewrite-manifests`. Opt-in flag. | 🟢 Production |

---

## 9. Observability: metrics, tracing, alerts

All three Protocol backends (metrics / tracing / alerts) share:
- Always-on local JSONL sink written to `runs/<run>/metrics.jsonl` / `traces.jsonl` / `alerts.jsonl` — no config needed.
- Env-driven remote backends via 5 per-subsystem env vars.
- Best_effort / blocking policy.
- Auth header injection + timeout caps.

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Structured logging + audit records** | `logging.py` + `audit.py`. Run-level per-stage lineaged audit records (duration, rows in/out per level, quality pass/fail, endpoints). AuditRecord Pydantic schema formalized. | 🟢 Committed |
| **Metrics export** | `MetricsExporter` Protocol. Prometheus remote_write via `ELT_PIPELINE_METRICS_*` env. Local metrics.jsonl always written. G-2 closure. | 🟢 Committed |
| **Distributed tracing export** | `TraceExporter` Protocol. OTLP HTTP. Run-level span per stage. Trace_id = sha256 of run_id. Local traces.jsonl always written. G-2 closure. | 🟢 Committed |
| **Alerting hooks** | `AlertHook` Protocol. Webhook POST on failure (severity=critical hard errors, warning RETRY/TIMEOUT). Local alerts.jsonl always written. Sensu/Tivoli is an external HTTP adapter on top of the Webhook protocol; not framework code. G-2 closure. | 🟢 Committed |
| **SNS alerting + SES emailing** | ⏳ Roadmap / not framework-level. G-2 Webhook protocol supports any HTTP endpoint; an SNS SES adapter is a tiny external Lambda/process, not core framework. If signed off: add new alerting backend via the Protocol. | ⏳ Roadmap |

---

## 10. Orchestration

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Built-in `elt schedule` DAG runner** | DAG-aware execution. Per-job `depends_on:` stable declaration-order topological sort. Cyclic/unknown-dep fail-fast at YAML validation. Per-job `retries:` 0–100 + `retry_delay_seconds:` with per-attempt audit. `schedule_execution_audit.json` artifact with run_id + ISO timestamps + execution_order counters. Backward-compatible payloads: `executed_count`/`jobs[]`/`skipped_jobs[]` separation. `--continue-on-error` mode with 3 distinct skip-reason codes. 11 tests green. | 🟢 Production |
| **Orchestration platform metadata seam** | `OrchestrationMetadata` dataclass: 6 fields (platform/flow_name/flow_run_id/task_name/task_attempt/tags). `load_orchestration_metadata_from_env()` reads 6 `ELT_PIPELINE_ORCHESTRATION_*` envs. `.to_env()` + `.to_run_attributes()` serialize for subprocess injection. Platform field is free-form string so any internal bespoke platform works via the env seam. | 🟢 Production |
| **Subprocess CLI invocation framework** | `CliInvocationRequest`/`CliInvocationResult`/`OrchestrationCliInvoker` Protocol/`SubprocessCliInvoker`. `.argv()` always resolves to `sys.executable -m elt_pipeline ...` for consistent Python-context invocations. | 🟢 Production |
| **Airflow integration (operators/DAG + reference DAG)** | G-3. `build_airflow_orchestration_metadata(context)` extracts native Airflow context (dag_id/run_id/task_id/try_number/dag_tags/logical_date). `AirflowCliWrapper` (build_request + invoke). Reference DAG at `examples/orchestration/airflow/reference_dag.py` (7 tasks: ingest→normalize→sql_compile→sql_run→publish_validate→publish_run→maintain). Max retries=2 + retry_delay. | 🟢 Production |
| **Dagster integration (assets/jobs + reference assets)** | G-3. `build_dagster_orchestration_metadata(context)`. `DagsterCliWrapper`. Reference assets: `examples/orchestration/dagster/reference_assets.py` (4-asset graph: ingest_orders_l1→normalize→sql→publish). Config class. 4-phase pipeline. max_retries=2 tag. | 🟢 Production |
| **Prefect integration (flows/tasks + reference flow)** | G-3. `build_prefect_orchestration_metadata(context)`. `PrefectCliWrapper`. Reference flow: `examples/orchestration/prefect/reference_flow.py` 4-task flow with parameterised env/source/entity/dates, flow-level retries=0 task-level retries=1-2. | 🟢 Production |
| **Mage / other orchestrators** | M-6 closure. `build_mage_orchestration_metadata(context)`. `MageCliWrapper`. Reference pipeline: `examples/orchestration/mage/reference_pipeline.py` 7 blocks @data_loader/@transformer. 6 new focused tests green. Platform is free-form string so internal bespoke platforms work via the env loader seam. | 🟢 Production |

---

## 11. Secrets & security

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **G-5 unified `SecretsProvider` Protocol + public registry** | `@runtime_checkable` Protocol. Singleton registry. Public `register_provider()` API. 6 built-in registered providers. Strict mode: any bare `$VAR` secret_ref without `provider://` prefix → fail BEFORE code runs. Default strict=False backward compat. | 🟢 Production |
| **Env + File providers (zero-config always-on)** | `env://VAR_NAME` direct read, `file:///path/to/secret` file read. Zero SDK. | 🟢 Production |
| **AWS Secrets Manager resolver (S1)** | `aws_secretsmanager://name[:AWSPREVIOUS|VersionId]`. Lazy boto3 import. Ambient IAM (IRSA/instance profile/env vars/~/.aws). String+binary secrets. 4 sharp error codes. Constructor: region_name/boto3_session override. | 🟢 Production |
| **Azure Key Vault resolver (S2)** | `azure_keyvault://vault-name/secret-name[/version]`. azure-keyvault-secrets + DefaultAzureCredential. WorkloadIdentityCredential override. 4 error codes. | 🟢 Production |
| **GCP Secret Manager resolver (S3)** | `gcp_secretmanager://project-id/secret-name[/version]`. GCE/GKE metadata/SA keyfile/gcloud creds. 4 error codes. | 🟢 Production |
| **HashiCorp Vault KV-v2 resolver (S4)** | `vault://mount/path/to/secret[#field]`. Auth priority order: injected client → Token env → AppRole. URL from env/kwargs. Field omitted → full `data.data` dict serialized. 7 error codes. | 🟢 Production |
| **AWS Systems Manager Parameter Store** | ⏳ Roadmap if signed off. AWS Secrets Manager (S1) is the recommended AWS-native path. SSM works today if an external process materialises SSM values into env vars at task start (standard pattern). If an explicit `ssm_parameter:` provider is required: register via the public `register_provider()` API. | ⏳ Roadmap (Secrets Manager shipped; SSM add-on if signed off) |
| **`SecretValue` repr-redacting str subclass** | Overrides `__repr__` → `[REDACTED]`. `__str__`/`f"{s}"` returns real value. `redact_secret()` audit-path helper always returns placeholder for non-empty inputs. Blocks accidental secret leakage into logs/tracebacks/audit records. | 🟢 Production |
| **RestConnector auth + request-body/query-param log redaction** | `redacted_fields` enforced at `RestResolvedAuth` + request building (auth headers, query params, body fields). | 🟢 Production |
| **S-0 Spark test isolation enforcement** | `scripts/run_tests.sh` gate runs 14 Spark-backed test files in **isolated processes** (S-0 one JVM = one SparkSession Iceberg on/off cross-contamination constraint). Gate is single source of truth; bare `uv run pytest` will fail on Spark isolation. Documented in [docs/maintainer/JVM_TOOLCHAIN_SETUP.md](maintainer/JVM_TOOLCHAIN_SETUP.md). | 🟢 Production (enforced) |

---

## 12. Governance: classification / masking / retention / erasure

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Run-level audit trail** | G-6. AuditRecord schema + per-run write. Retention via storage lifecycle policies. | 🟢 Production |
| **4-tier DataClassification enum + column-level TBLPROPERTIES** | public/internal/confidential/restricted_pii. Per-column `SqlColumnSpec`. Post-write `ALTER TABLE SET TBLPROPERTIES elt.governance.*`. Cross-field validators. G-6 closure. | 🟢 Production |
| **Column-level Trino masking views + role-based unmask** | 7 built-in strategies per column. Optional `unmask_role` for auditors. G-6 closure. See §6 Publish above. | 🟢 Production |
| **Retention policy SQL builders + Right-to-erasure runbook** | `build_retention_delete_statement(...)` / `build_erasure_statement(...)` / `build_row_level_erasure_statement(...)` — SQL-injection-safe literal escaping + batched id pagination. Follow with G-1 `elt maintain run --expire-snapshots --remove-orphan-files` for post-delete Iceberg snapshot isolation sweep. Full operator runbook with step-by-step cleanup + audit: [docs/operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md](operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md). | 🟢 Production |

---

## 13. Deployment / packaging / containerization / K8s

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **PEP 517 Hatchling sdist + wheel** | `pyproject.toml` declares 19 extras (driver, auth, cloud, spark, trino, jdbc, delta, kafka, etc.). `uv sync` resolves zero-install pure-Python. sdist: 747 KB. Pure-Python wheel: 242 KB. JDK/Spark/Trino are consumer-provided (pinned versions documented). Publication Hardening Pass (item 2) validated. | 🟢 Production |
| **Docker multi-stage image** | §8 CMM. Multi-stage (uv wheel builder → Spark/Trino dist fetcher → Temurin 23 runtime). Pinned stack: JDK 23 / Spark 4.1.2 / Trino 468 / Iceberg 1.11.0 / SQLite JDBC 3.46. Build arg `EXTRAS=` for optional extras. `demo` sugar command. Docker entrypoint seed fallback fixes + env var + dead-code removal (D-3 fixes: shared driver file copied both locations + Trino SHOW SCHEMAS works instantly with shared SQLite JDBC metastore). | 🟢 Production |
| **docker-compose 2-service reference deployment** | Service 1: `elt_pipeline` CLI runner with shared bind mount + build args/env vars/pinned image. Service 2: `trino` foreground (shared JDBC SQLite metastore file on 50Gi Docker volume so writer writes → Trino reads instantly, zero extra catalog config). Shared x-elt-common anchor reduces duplication. Zero-config: `docker compose run --rm demo` then `docker compose up -d trino` to browse. | 🟢 Production |
| **Kubernetes Kustomize base/overlays manifests** | Kustomize base: ConfigMap pipeline.yaml jdbc+sqlite default. 50Gi RWO PVC. ClusterIP Trino. Deployment: 4-core/12Gi resource requests, readiness/liveness probes (Trino 8080 / CLI healthy). CronJob: 03:00 UTC daily 4-phase ELT, restartPolicy OnFailure, backoffLimit 2, ttlSecondsAfterFinished 3600. dev overlay: namespace, image override commonLabels, recreate strategy (single-reader jdbc+sqlite constraint). Switch `catalog_type=rest` for multi-replica. Helm is additive-only on top if explicitly signed off. | 🟢 Production |
| **CloudFormation YAML / Terraform for VPC/IAM/RBAC** | Out of this framework repo's scope (equivalent modern paths shipped: Docker + Kustomize + env-driven IAM surface). Terraform/CF for VPC/IAM/RBAC belongs in separate per-deployment infrastructure Git repositories. | OUT OF SCOPE per framework/domain boundary |

---

## 14. Validation discipline & publication safety guardrails

| Capability | What ships | Maturity / Notes |
|---|---|---|
| **Green gate: 769/0/28 published + stamped in 3 places** | Gate at time of writing: 769 passed / 0 failed / 28 emulator tests correctly skipped. Baseline (non-Spark): 571 passed. 14 isolated Spark-backed files: 198 passed. Gate command: `bash scripts/run_tests.sh` (NOT bare `uv run pytest` — S-0 isolation required). Number is always stamped into (a) BACKLOG §Status snapshot, (b) BACKLOG §Resume close narratives, (c) CAPABILITY_MATURITY_MATRIX.md Document Status header. | 🟢 Production (enforced) |
| **Bidirectional doc↔code↔gate↔test safety guardrails** | Every numeric claim (test count, maturity badge, gate delta) written into any doc change is mechanically cross-checked against the actual code/test output before close. CMM rows ↔ README Honest Boundary ↔ BACKLOG Resume/Status/StillTodo are mechanically synced. No claim drift tolerated. | 🟢 Production (enforced) |
| **Ruff static analysis** | `uv run ruff check src/ tests/ examples` required clean at close. Ruff E741 enforced (no `l`/`O`/`I` ambiguous variable names). Zero rule suppressions in `pyproject.toml` ruff config. | 🟢 Production (enforced) |
| **Bash syntax validation** | All `.sh` files `bash -n` clean (Docker entrypoint, demo scripts, gate, Trino foreground). D-3 item validated. | 🟢 Production (enforced) |
| **Publication safety: no client/company/organisation identifiers** | Audited 2026-08-26: zero client-name / internal programme-name strings anywhere in tree. | 🟢 Production (enforced) |
| **Publication safety: no absolute internal file paths** | Audited 2026-08-26: all markdown code reference links use standard GitHub-compatible relative paths + `#Lstart-Lend` fragment anchors. Zero `file:///Users/…` absolute links. Zero home-dir `~/Documents/…` narrative path references outside context of generic user-local wording. | 🟢 Production (enforced) |
| **Harness/assistant context wiring excluded from public repo** | No private assistant-context directories, no `.claude/`/`.qwen/`/`.trae/` harness symlinks, no private toolchain wiring in this repo. TRAE.md + CLAUDE.md are tiny self-contained routers pointing at this repo's own public docs only. | 🟢 Production (enforced at repo boundary) |

---

## 15. Architectural highlights (non-exhaustive — why this platform stands on its own)

1. **Multi-cloud storage (S3/GCS/ADLS + Databricks Unity REST) unified behind B-6 Protocol facade.** No cloud-vendor lock-in by design.
2. **Dual independent catalogs (writer 6 × serving 6 types) with strict B-0 preflight validation.** Strict separation of where data is written from how it is served.
3. **4 orchestrator wrappers + platform-agnostic metadata seam + built-in DAG runner.** Works as a standalone scheduler or drops into any orchestration platform with zero adapter churn.
4. **6 secrets providers + unified SecretsProvider Protocol + SecretValue repr-redaction.** Credential leakage from logs/tracebacks is structurally blocked at the string-subclass level.
5. **Iceberg-first + 4 maintenance procedures + end-to-end governance (4-tier classification, per-column masking, retention + erasure SQL generators with operator runbook).** Data governance is baked in, not bolted on.
6. **Bidirectional doc↔code↔gate safety guardrails + published green gate (769/0/28 at time of writing) + S-0 enforced Spark test isolation + Ruff clean.** Documentation claims are mechanically verified against tests.
7. **Single-language single-repo Python + uv + YAML.** The Python sdist and pure-Python wheel are sub-megabyte artifacts; JDK/Spark/Trino are consumer-provided with pinned versions documented.
8. **Kafka BOTH modes Production (broker consumer + JSONL file replay) sharing one `KafkaConnectorBase` seam.** Broker mode for direct small-scale ingest, JSONL replay for CI pipelines, offline backfill from Kafka Connect S3 exports, and workstation PoC.

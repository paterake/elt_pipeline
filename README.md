# elt_pipeline

> A 5-layer configuration-driven ELT runtime for a governed, replayable,
> zero-service Apache Iceberg lakehouse on your laptop.
> **L1 raw → L2 normalized → L3 canonical → L4 marts → L5 exports → Trino SQL.**
>
> Paste ≤5 commands and you have a queryable modern lakehouse.
> No Hive metastore, no Glue crawler, no Nessie, no Kyuubi, no Docker Compose
> if you don't want it — just a shared SQLite JDBC metastore, Spark 4.1, and
> Trino 468, all driven from a single YAML file.

**Maturity at a glance:** [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md) — every feature is classified 🟢 Production / 🟠 Demo. **No pre-scoped ⏳ roadmap rows remain.**

---

## Architecture (30 seconds)

```
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                     CONFIG-DRIVEN ELT PIPELINE                          │
 │                                                                        │
 │  Ingest (CLI)    Normalize (Spark)    SQL (Spark Iceberg)   Maintain  │
 │  ──────────     ─────────────────     ──────────────────    ────────   │
 │  REST / SQL /   │  L1 raw → L2  │     │  L2 → L3 canonical │  compact │
 │  object_sto. /  │  parquet +    │────▶│  L2 → L4 marts     │  expire  │
 │  Kafka          │  MappingCat.   │     │  Apache Iceberg    │  orphan  │
 │                 └────────────────┘     └─────────┬──────────┘  rewrite │
 │                                                  │ Iceberg metadata     │
 │                                                  ▼                      │
 │  Publish (CLI)  ────────────  L5 exports (CSV / JSONL / TSV / ZIP)     │
 │                                                  ▲                      │
 │                                                  │ shared SQLite        │
 │                         6 writer catalogs        │ JDBC metastore       │
 │                    (hadoop/jdbc/rest/nessie/     │ + warehouse/ dir     │
 │                     hive_metastore/glue)         │                      │
 └──────────────────────────────────────────────────┼──────────────────────┘
                                                    │
                         Trino 468 JDBC serving    │
                         (7 catalog bindings)      ▼
                            ──────────────      SHOW SCHEMAS;
                            SELECT ... FROM   SELECT * FROM
                            iceberg.sales.    sales.order_summary;
                            canonical_orders
```

Every arrow above is a **CLI subcommand**. No orchestrator, no scheduler, no
broker, no metastore service required on a workstation. Swap the catalog
binding via one env var and the same pipeline runs against Unity Catalog /
Nessie / Glue / Hive Metastore / any JDBC database in production.

---

## Quick Start — Two Paths

Pick **one**. Docker is fewer steps; no-Docker is for Python-native engineers
who already have `uv` and a JDK.

### Path A — Docker (3 commands → queryable Iceberg + Trino)

```bash
# 1. Build the image (Spark 4.1.2, Trino 468, Temurin 23, Python 3.11)
docker compose build

# 2. Run the end-to-end demo: ingest → normalize → sql (Iceberg) → maintain
docker compose run --rm demo

# 3. Boot Trino in the background and wait ~30s for the healthcheck
docker compose up -d trino
```

**Query it.** Paste one command to get a Trino shell, then open
[examples/queries/trino_medium_article.sql](examples/queries/trino_medium_article.sql)
and copy-paste queries one-by-one:

```bash
docker compose exec trino trino --catalog iceberg
```

Expected first output (you should see `sales` and `inventory` immediately —
no `register_table()` required):

```
trino:iceberg> SHOW SCHEMAS;
   Schema
-------------
 inventory
 sales
(2 rows)
```

### Path B — No-Docker (uv + Temurin 23 JDK)

Requires:
- [`uv`](https://github.com/astral-sh/uv) for Python package management
- [Temurin 23 JDK](https://adoptium.net/temurin/releases/?version=23) with
  `JAVA_HOME` set (Spark + Trino both need a full JDK)

```bash
# 1. Install Python deps + Spark + optional drivers
uv sync --extra dev --extra spark

# 2. Point at the JDK
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"

# 3. L1 (raw landing) + L2 (parquet + MappingCatalog)
uv run elt-pipeline ingest run \
    examples/configs/local_object_storage_orders.yaml
uv run elt-pipeline normalize run \
    examples/configs/local_object_storage_orders.yaml

# 4. L3 canonical Iceberg + L4 marts (2026-01 window, sales domain)
uv run elt-pipeline sql run \
    --include-deps --environment workstation \
    --start-date 2026-01-01 --end-date 2026-01-31 --domain sales \
    --iceberg-enabled examples/sql/local_demo

# 5. Start Trino serving in the foreground (new terminal)
uv run elt-pipeline trino start-foreground
```

**Query it** from a third terminal using any Trino 468 CLI:

```bash
trino --catalog iceberg --server http://127.0.0.1:8080
```

Then open
[examples/queries/trino_medium_article.sql](examples/queries/trino_medium_article.sql)
— all 6 query groups work against both Docker and no-Docker paths.

---

## 6 Copy-Paste Trino Queries (the "Medium article" section)

Open [examples/queries/trino_medium_article.sql](examples/queries/trino_medium_article.sql)
for the fully-commented version. The short version — paste these into your
Trino shell after the Quick Start:

| # | What it shows | Query to paste |
|---|---|---|
| 1 | **Discovery** — what schemas exist? | `SHOW SCHEMAS; SHOW TABLES FROM sales;` |
| 2 | **L4 mart** — the BI-ready aggregation | `SELECT * FROM sales.order_summary ORDER BY order_date;` |
| 3 | **L3 canonical** — row-level orders + top customer | `SELECT customer_id, customer_name, SUM(order_total_usd) FROM sales.canonical_orders GROUP BY 1,2 ORDER BY 3 DESC;` |
| 4 | **Cross-domain** — orders ⋈ shipments fulfillment view | `SELECT o.order_id, o.customer_name, s.carrier, s.tracking_number, s.ship_date FROM sales.canonical_orders o LEFT JOIN inventory.canonical_shipments s ON s.order_id = o.order_id;` |
| 5 | **Iceberg metadata** — versioned snapshot audit | `SELECT committed_at, operation, snapshot_id FROM iceberg.sales."canonical_orders$snapshots" ORDER BY 1 DESC;` |
| 6 | **Gate check** — row counts across all layers | `… UNION ALL …` (see file) → expects 2/2/2 rows |

Expected counts for the bundled demo:
- `sales.canonical_orders`: **2 rows** (Alice A-100 $10 / Bob A-200 $25)
- `inventory.canonical_shipments`: **2 rows** (S-100 / S-200 via `acme_freight`)
- `sales.order_summary`: **2 rows** (2026-01-01 $10 / 2026-01-02 $25)

---

`elt_pipeline` is not only an ingestion and transformation tool. It is a governed data platform runtime for moving data through explicit architectural levels with strong auditability, lineage, metadata discipline, replayability, and access-control boundaries.

The platform is designed to align with DAMA-DMBOK v2 principles for:

- data architecture
- data integration and interoperability
- metadata management
- data quality
- governance and security
- operational auditability

The repository does not claim that DAMA-DMBOK v2 prescribes the exact `level1` through `level5` naming used here. Instead, those levels are the platform's chosen architecture model for operationalizing DMBOK-aligned concerns in a concrete implementation.

---


## Current Scope and Capabilities (Honest Boundary)

This section states what the code actually ships, so no reader infers more than is built. For a condensed **what-ships feature-matrix overview by capability area** (15 sections, tabular, maturity-graded) see [Framework Feature Matrix](docs/FRAMEWORK_FEATURE_MATRIX.md). For the formal, per-capability classification with full maturity definitions, dates, and per-row test-count notes, see [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md). The cross-doc roadmap and portability environment breakdown also lives in [PRD 10 §6.3](docs/prd/10-prd-architecture-and-lifecycle.md). Domain-specific modules — per-entity SQL models, bespoke XML/ZIP/Excel/CSV/document parsers with reference-file mapping tables, reverse-ETL push connectors, and BI-embedding microservices — are explicitly out of this framework repo's scope, and belong in separate per-deployment cfg/domain Git repositories alongside the pipeline manifests they ship with (equivalent pattern to a framework library vs a project repository in any language).

**Storage backends — implemented and tested:**
- Local POSIX filesystem (bare paths or `file://` URIs) — fully implemented, default on a laptop.
- AWS S3 (`s3://` URIs) — Python control plane via `boto3`, Spark data plane via Spark's native S3 / EMRFS; unit-tested with an in-process S3 fake.
- Google Cloud Storage (`gs://` URIs) — `GCSBackend` class behind the pluggable `StorageBackend` Protocol (B-6 facade), 28 pure-unit control-plane tests, Spark data plane via `fs.gs.impl` + SA keyfile / workload identity config. Install with `uv sync --extra gcs` or `uv sync --extra dataproc`.
- Azure ADLS Gen2 (`abfss://` URIs) — `ADLSBackend` class behind the pluggable `StorageBackend` Protocol (B-6 facade), 28 pure-unit control-plane tests with authority-aware routing (`container@account.dfs.core.windows.net`), Spark data plane via shared key / Service Principal OAuth / MSI / DefaultAzureCredential auth modes. Install with `uv sync --extra adls` or `uv sync --extra synapse`.
- Databricks (Unity Catalog) — **backing store + REST catalog pattern, no `dbfs://` scheme**: use your cloud's native object store (S3/GCS/ADLS, all above) for storage and bind Unity Catalog as a standard Iceberg REST catalog via `catalog_type=rest` with the Unity endpoint + PAT token. Reference config: `examples/configs/databricks_unity_adls.yaml` with all three backing-store options and full auth-mode examples.

**Storage backends — not yet implemented (roadmap):**
- Azure Blob legacy (`wasbs://`) — explicitly not on the recommended path; fail-fast with a pointer to `abfss://`.
- Hadoop HDFS (`hdfs://`) — **DEFUNCT (M-10, 2026-08-26):** Industry reality has displaced on-prem Hadoop/HDFS clusters with cloud-native object storage. Fail-fast with migration guidance: land legacy on-prem payloads in a cloud object store (local/S3/GCS/ADLS — all 4 Production) via the `object_storage` connector first, then run the standard ELT pipeline. Intentionally NOT implementable via the B-6 StorageBackend facade for this project unless a paying signed-off customer contract explicitly demands on-prem HDFS AND object storage is genuinely unavailable at that site.

**Ingest mechanisms — honest v1 surface (framework abstractions vs. concrete implementations):**

The platform defines four first-class connector *families* (`rest`, `sql`, `kafka`, `object_storage`) as shared abstractions — each with a validated lifecycle (config → secrets → client → extract → persist → audit → checkpoint). Family-level dispatch is now **registry-factory backed** (M-1): a public `ConnectorFactory` Protocol + `register_connector_factory` / `get_connector_factory` API with lazy init, 4 built-in factories that delegate to the existing concretes with zero behavior drift, and **no-code preset authoring WITHIN families** via YAML/JSON `ConnectorManifest` loaded from `ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST` (strict mode via `ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT=1`). Adding a *new family* still needs Python code (one `register_connector_factory()` call — no CLI `if/elif` edits required thanks to registry lookup). Their concrete v1 implementations vary by readiness:

- **REST — Production-usable.** Real `urllib.request`-based connector with authentication (basic, API key, static bearer, client-credential token flows), request templating, date-window tokenization, page/offset pagination, envelope+inner-payload extraction, retry/backoff/timeout controls.
- **Object storage — Production-usable (local + S3 + GCS + ADLS).** Source discovery and read via `path_utils` scheme dispatch across local POSIX dirs, `s3://` buckets, `gs://` buckets, and `abfss://` containers. All four schemes share the same `_BACKEND_REGISTRY` dispatch path with full parity.
- **SQL — Production: 6-driver matrix (SQLite, DuckDB, Postgres, MySQL, MSSQL, JDBC generic).** `SqlConnectionDriver` enum = `{sqlite, duckdb, postgres, mysql, mssql, jdbc_generic}` behind the single `LocalSqlConnector` plus `_build_db_driver()` lazy-importer. SQLite + DuckDB are zero-infra local files; Postgres uses `psycopg`; MySQL uses `mysql-connector-python`; MSSQL uses `pymssql`; JDBC uses `JayDeBeApi` + JVM driver jar. Each driver has an optional `uv sync --extra {driver}` install; missing SDK → ConfigValidationError with a sharp install hint. Backward compat: `driver` defaults to `sqlite` when omitted. Closed by M-2 (2026-08-26).
- **Kafka — BOTH modes Production (M-3 real broker + M-11 JSONL file replay).** The `KafkaConnectorBase` abstraction is broker-shaped and in place (offsets, partitions, headers, checkpoints, run loop). Two concrete subclasses share the same lifecycle and are a config-only toggle: (1) `BrokerKafkaConnector` — 🟢 Production real broker consumer via `kafka-python>=2.0,<3.0` (install with `uv sync --extra kafka`), selected by `bootstrap_servers:` presence in entity config, explicit assign+seek per-partition (no auto-rebalance), poll loop with 3-empty-polls bounded termination, SDK missing → `KAFKA_SDK_MISSING` install hint, 14 focused tests; (2) JSONL file replay — 🟢 Production fallback (default when `bootstrap_servers:` is absent), reads a local JSONL event log, Production guarantees: strict offset-sorted consumption, empty-log zero-message no-op, cross-topic/cross-partition filter, checkpoint-middle window replay idempotency identical to broker mode, Production error codes `KAFKA_LOG_READ_FAILED`/`KAFKA_LOG_INVALID_JSON`, 9 focused tests (5 pre-existing + 4 new M-11). Enterprise deployments normally land streams to object storage via Kafka Connect/Firehose/Event Hubs Capture and use the `object_storage` connector to pick them up, so a rock-solid multi-cloud object-storage path is the higher-value ingress work; the direct broker consumer covers low-latency / replay-from-timestamp CI pipelines; the JSONL replay path is for offline backfill from Kafka Connect S3 JSONL exports plus workstation PoC / CI testing. Closed in BACKLOG items M-3 + M-11 (2026-08-26).

**Ingest roadmap (not in v1, tracked for later tranches):**
- New connector *families* beyond the current 4 (generic HTTP webhook, CDC log tail, SFTP) — each is additive: one `ConnectorFactory` Protocol impl + one `register_connector_factory()` call; no CLI dispatch edits.

**Serving / catalogs — implemented:**
- Iceberg L3/L4 tables with a 6-way catalog enum: `hadoop`, `jdbc`, `rest`, `nessie`, `hive_metastore`, `glue`.
- Trino 468 JDBC serving endpoint (first-class spoke; SQLite-backed metastore default for workstation). **Trino authentication & TLS is Production via M-4:** 6 HTTP auth types (password / certificate / kerberos / jwt / oauth2 / form), HTTPS/TLS keystore+truststore, Kerberos principal+keytab, all 11 tunables env-var-driven through the centralized EnvVarNames cascade with bash fail-fast validators (exit 12/13/14) and Python pure-builder unit tests; default insecure `none/http-only` workstation path is 100% backward-compatible. See [Capability Maturity Matrix §4 JDBC serving endpoint](docs/CAPABILITY_MATURITY_MATRIX.md#L129-L135).
- **Orchestration: `elt schedule` DAG-aware runner (M-8) + 4 thin CLI wrappers (G-3 / M-6) — all 🟢 Production.** Schedule runner: per-job `depends_on:` declaration-order-stable topological sort, cyclic/unknown-dep fail-fast at YAML validation time, per-job `retries:`+`retry_delay_seconds:` with per-attempt structured output, `schedule_execution_audit.json` artifact with run_id + timestamps, backward-compat payload shapes; 11 tests (2 legacy backward-compat green + 9 new). 4 orchestrator wrappers = Airflow / Dagster / Prefect / Mage — each follows the same thin-wrapper pattern: a Python callable invokes the standard `elt-pipeline` CLI via `subprocess`, with the orchestrator's native context forwarded as `ELT_PIPELINE_ORCHESTRATION_*` env vars that appear in every run's audit record, lineage events, metrics labels, and observability spans. Reference examples live at `examples/orchestration/`; helpers exported from `elt_pipeline.integrations`. Closed in BACKLOG items G-3 + M-6 + M-8 (2026-08-26).
- OpenLineage-compatible lineage adapter; row-count DQ adapter + 6-check built-in DQ library with quarantine/DLQ.
- **Pre-Spark-boot catalog preflight (B-0):** 8 scheme-aware connectivity/validity checks across writer × serving catalog bindings before every SparkSession boot, preventing opaque Py4JJavaError crashes mid-stage. Configurable 3-mode enforcement (off / best_effort default / strict). See [Capability Maturity Matrix §3 Iceberg catalog bindings](docs/CAPABILITY_MATURITY_MATRIX.md#L78-L127).

**Operational / platinum-hardening items:**
Iceberg table maintenance (compaction / snapshot expiry / orphan cleanup / manifest rewrite) is **Production** (via `elt maintain run …`; see [Capability Maturity Matrix §5](docs/CAPABILITY_MATURITY_MATRIX.md#L125-L141)).
Observability (Prometheus metrics, OTLP tracing, webhook alerting) is **Production** via env-driven backends behind the `ObservabilityAdapter` seam; see [Capability Maturity Matrix §6](docs/CAPABILITY_MATURITY_MATRIX.md#L147-L167).
Secrets resolution (env vars + files + **4 real implementations**: HashiCorp Vault KV-v2 / AWS Secrets Manager / Azure Key Vault / GCP Secret Manager — all lazy SDK import with `SECRETS_SDK_MISSING` install guidance) is **Production** via the G-5 subsystem: `secret_ref` URIs, `SecretValue` redaction, and the pluggable `SecretsProvider` Protocol/registry; see [Capability Maturity Matrix §9](docs/CAPABILITY_MATURITY_MATRIX.md#L211-L225). Closed in G-5 + S1–S4 (2026-08-25).
Governance — classification tags (4 tiers: public/internal/confidential/restricted_pii), column-level Trino masking (7 strategies, role-based), retention sweeps, and right-to-erasure runbook + SQL helpers is **Production** via the G-6 subsystem; see [Capability Maturity Matrix §10](docs/CAPABILITY_MATURITY_MATRIX.md#L217-L227).
Lineage — **BOTH native authoritative bespoke JSONL emitter + OpenLineage 2.0.2 wire-compatible export are Production**. Native JSONL always-on authoritative sink written to runs/.../lineage.jsonl (Pydantic-validated LineageEvent+DatasetRef, scheme-agnostic B-6 write path across local/S3/GCS/ADLS, used for on-disk audit + replay debugging; 13 focused tests green). OpenLineage wire export is env-driven `openlineage_http` backend behind existing adapter seam. Auto-injects standard `EnvironmentRunFacet`. Targets Marquez, DataHub, OpenMetadata, Apache Atlas out of the box. See [Capability Maturity Matrix §12](docs/CAPABILITY_MATURITY_MATRIX.md#L254-L262).
Data Quality — **Built-in 6-check library + scheme-agnostic quarantine/DLQ is Production** behind the existing adapter seam. Ships `BuiltinQualityHook` (not-null, uniqueness, range, referential integrity, freshness, format regex checks) loaded from JSON/YAML env, plus the original `RowCountQualityHook`. Failed rows are written to a quarantine JSONL layout via the same B-6 pluggable storage backend (local/S3/GCS/ADLS) as logs/errors/lineage, with a `quality_quarantine_written` audit log event listing every path. Blocking and non-blocking policies supported; quarantine is always written first. See [Capability Maturity Matrix §11](docs/CAPABILITY_MATURITY_MATRIX.md#L230-L238).
Connector Registry — **No-code preset authoring within families is Production** behind the M-1 plugin registry. Public registry API (`register_connector_factory` / `get_connector_factory` / `is_connector_factory_registered`), explicit `ConnectorFamily` enum boundary, `ConnectorFactory` Protocol (2 methods) with 4 built-in factories, and YAML/JSON `ConnectorManifest` preset system (shallow merge UNDER entity config via `ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST` + strict-mode env var). CLI ingest dispatch for ALL 4 families routes through the registry. Adding new families is one `register_connector_factory()` call with zero CLI edits. See [Capability Maturity Matrix §13](docs/CAPABILITY_MATURITY_MATRIX.md#L253-L260).
Catalog Preflight (B-0) — **Pre-Spark-boot catalog connectivity/validity validator is Production** behind the existing catalog binding seam. Runs AFTER the existing `_validate_iceberg_catalog_binding()` (enum/type checks) and BEFORE every `build_spark_session()` boot in both `sql run` and `publish run` CLI branches. 8 scheme-aware checks across all valid writer × serving catalog bindings (JDBC format+sqlite-parent-dir, REST /v1/config HTTP probe with 4xx-tolerance reachability semantics, Hive metastore thrift-format+TCP-3way-handshake, Glue STS boto3 identity probe with SDK-not-installed SKIP-pass, Hadoop warehouse exists-or-creates dir, Snowflake serving URI scheme). 2 centralized env vars: `ELT_PIPELINE_CATALOG_PREFLIGHT_MODE` (`off` skip / `best_effort` warn-to-stderr-never-block DEFAULT / `strict` ConfigValidationError pre-JVM-boot with structured multi-failure context dict) + `ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS` (per-check HTTP/TCP timeout, default 5). Strict mode is the recommended CI / orchestration production mode to fail fast on catalog misconfigs instead of letting Spark surface a multi-hundred-line Py4JJavaError mid-stage. See [Capability Maturity Matrix §3 Iceberg catalog bindings](docs/CAPABILITY_MATURITY_MATRIX.md#L78-L127).
Trino Authentication & TLS (M-4) — **HTTPS/TLS + 6 HTTP auth types for Trino JDBC serving are Production** behind the `ops/trino_serving/run_trino.sh write-configs` entrypoint. 11 env vars (all centralized in EnvVarNames, routed through the 4-tier runtime_context cascade with matching YAML fields in RuntimeTrinoServingConfig + manifest-floor ServingDefaults): `ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE` ∈ {password, certificate, kerberos, jwt, oauth2, form}; `ELT_PIPELINE_TRINO_HTTPS_ENABLED` (bool) + `ELT_PIPELINE_TRINO_HTTPS_PORT` (int, default 8443); `ELT_PIPELINE_TRINO_SSL_KEYSTORE_PATH` + `ELT_PIPELINE_TRINO_SSL_KEYSTORE_PASSWORD` (mandatory when HTTPS=on); optional `ELT_PIPELINE_TRINO_SSL_TRUSTSTORE_PATH` + `ELT_PIPELINE_TRINO_SSL_TRUSTSTORE_PASSWORD` (enterprise PKI); `ELT_PIPELINE_TRINO_PASSWORD_FILE_PATH` (for password auth, htpasswd format); `ELT_PIPELINE_TRINO_KRB5_CONF` + `ELT_PIPELINE_TRINO_KERBEROS_PRINCIPAL` + `ELT_PIPELINE_TRINO_KERBEROS_KEYTAB` (for Kerberos auth). Bash write-configs has 3 pre-config-write fail-fast validators with distinct exit codes: exit 12 (password auth missing file + `htpasswd` creation recipe), exit 13 (kerberos missing principal/keytab + keytab-not-file error), exit 14 (HTTPS missing keystore path/password/file + `keytool -genkeypair` PKCS#12 JKS recipe). Default workstation path is explicitly 100% backward-compatible: all M-4 env vars default to None/False/empty → `http-server.authentication.type` line not emitted, `https.enabled=false` line emitted, shared-secret NOT generated → byte-for-byte identical config.properties to pre-M-4. Pure-Python mirror builder `build_trino_serving_configs(...)` unit-tests every branch without JVM/shell invocation: 27 focused tests in 9 classes cover backward-compat 6-value disabled parametrized set, HTTPS/TLS keystore+truststore+raises, password auth+raises, Kerberos principal parsing+host fallback+raises, JWT/OAuth2/Form/Certificate passthrough, EnvVarNames registry, and full env→singleton→builder roundtrip. See [Capability Maturity Matrix §4 JDBC serving endpoint](docs/CAPABILITY_MATURITY_MATRIX.md#L129-L135).

## Source Code references

**Architecture & lifecycle overview (canonical):**
- See [10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md) for the authoritative architecture, four-phase lifecycle, four-tier config cascade, four-tier SQL validity chain, schema evolution, portability, catalog bindings, and JDBC serving endpoint model.

## Platform Model

Within `elt_pipeline`, the levels mean:

- `level1`: raw landed source data
- `level2`: relationalized source-aligned structured data, typically persisted in parquet form for local workflows
- `level3`: canonical and standardized warehouse-style data
- `level4`: consumer-facing datamarts for direct analytical use
- `level5`: transformed static outputs or canned deliverables for consumer pickup

Consumers may either:

- analyze queryable `level4` datamarts directly, or
- consume static `level5` outputs when a file-based handoff is preferred.

The `level5` publish/export contract is defined in [docs/prd/06-prd-level4-to-level5-publish-and-export.md](docs/prd/06-prd-level4-to-level5-publish-and-export.md).

## Client Neutrality

This repository must remain client-neutral.

- do not include client names or vendor names in product contracts, examples, or defaults
- use generic and non-identifying configuration examples

## Docs

PRDs live in `docs/prd/` and define the target-state architecture.
The canonical architecture, 4-phase lifecycle, config cascade, SQL validity chain, portability matrix, and next-cycle operator triggers are consolidated in the top-level PRD reference [docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md) — start there.

Recommended starting points:

- `docs/prd/10-prd-architecture-and-lifecycle.md`: canonical architecture, four-phase lifecycle, four-tier config, validity chain, portability, serving endpoint model
- `docs/prd/00-prd-platform-principles.md`: product positioning and DAMA-DMBOK v2 alignment
- `docs/prd/00-prd-architecture-levels-and-governance.md`: level model and governance boundaries
- `docs/prd/06-prd-level4-to-level5-publish-and-export.md`: approved `level5` publish/export contract

## Install

This project uses `uv` for environment management.

`level2` through `level5` execute on Apache Spark (`pyspark`), and the reference JDBC serving endpoint runs on Trino 468. Both require a **Temurin 23** JDK with `JAVA_HOME` set before running any command that touches `normalize`, `sql`, or `publish`. See [docs/maintainer/JVM_TOOLCHAIN_SETUP.md](docs/maintainer/JVM_TOOLCHAIN_SETUP.md) for a clean workstation install.

```bash
uv sync --extra dev --extra spark
```

Run the test suite:

The quality gate is **`bash scripts/run_tests.sh`** — Spark-backed test files each run in their own process (one JVM = one SparkSession). A bare single-file test is fine locally with `uv run pytest tests/<file>.py`:

```bash
# Full gate (311+ tests, per-file Spark isolation)
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh

# Individual file (e.g. non-Spark)
uv run pytest tests/test_path_utils.py
```

Run the CLI:

```bash
uv run elt-pipeline --help
```

## Repository Layout

- `src/elt_pipeline/`: runtime implementation
- `tests/`: automated coverage for connectors, normalization, SQL, and CLI flows
- `examples/configs/`: runnable local connector configs for object storage, SQL, Kafka, and REST demos
- `examples/data/`: bundled sample inputs for local connector workflows
- `examples/orchestration/airflow/reference_dag.py`: reference Airflow wrapper calling the authoritative CLI
- `examples/publish/local_demo/`: example `level5` publish definitions for local file-based exports
- `examples/sql/local_demo/`: example SQL model package for local execution
- `examples/schedules/local_demo.yaml`: example schedule plan wiring the CLI stages together
- `examples/README.md`: setup and command sequences for the bundled examples
- `docs/maintainer/`: maintainer local development, CI, and release workflow guidance
- `docs/operator/`: operator runbook and troubleshooting guidance for local execution
- `docs/prd/`: approved product and architecture requirements

## CLI Overview

Validate and inspect configuration:

```bash
uv run elt-pipeline validate-config path/to/pipeline.yaml
uv run elt-pipeline validate-config path/to/pipeline.yaml --source my_source --entity my_entity
uv run elt-pipeline show-run-context --stage ingest --job-name demo-ingest
```

Run ingestion into local `level1` storage:

```bash
uv run elt-pipeline ingest run path/to/pipeline.yaml --root-path path/to/runtime
uv run elt-pipeline ingest run path/to/pipeline.yaml --source my_source --entity my_entity --window-start 2026-01-01T00:00:00+00:00 --window-end 2026-01-31T23:59:59+00:00
```

Run local `level1 -> level2` normalization:

```bash
uv run elt-pipeline normalize run path/to/pipeline.yaml --root-path path/to/runtime
uv run elt-pipeline normalize run path/to/pipeline.yaml --source my_source --entity my_entity --rerun-run-id prior-run-id
```

Compile and execute SQL models against a Spark-backed local parquet warehouse:

```bash
uv run elt-pipeline sql compile examples/sql/local_demo --environment default --start-date 2026-01-01 --end-date 2026-01-31
uv run elt-pipeline sql run examples/sql/local_demo --root-path path/to/runtime --warehouse-root path/to/warehouse --include-deps --start-date 2026-01-01 --end-date 2026-01-31
uv run elt-pipeline sql run examples/sql/local_demo --root-path path/to/runtime --warehouse-root path/to/warehouse --validate-only --stage level3
uv run elt-pipeline sql run examples/sql/local_demo --root-path path/to/runtime --warehouse-root path/to/warehouse --explain --stage level4
```

Validate, explain, and run `level5` publish definitions:

```bash
uv run elt-pipeline publish validate path/to/publish_defs
uv run elt-pipeline publish explain path/to/publish_defs --root-path path/to/runtime
uv run elt-pipeline publish run path/to/publish_defs --root-path path/to/runtime --warehouse-root path/to/warehouse
uv run elt-pipeline publish run path/to/publish_defs --root-path path/to/runtime --warehouse-root path/to/warehouse --rerun-run-id prior-publish-run-id
```

Run a deterministic local schedule plan:

```bash
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml
```

Run Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup):

```bash
uv run elt-pipeline maintain run --help
# Dry run — preview what will run on specific tables
uv run elt-pipeline maintain run --dry-run \
  --warehouse-root path/to/warehouse \
  --table iceberg.level3.orders --table iceberg.level4.daily_orders
# Run against all L3 tables + specific L4, with custom retention
uv run elt-pipeline maintain run --all-level3 \
  --table iceberg.level4.daily_orders \
  --warehouse-root path/to/warehouse \
  --snapshot-retain-days 14 --orphan-older-than-days 7
# Opt-in manifest rewrite + only compaction+expire (skip orphans)
uv run elt-pipeline maintain run --all-level4 \
  --warehouse-root path/to/warehouse \
  --rewrite-manifests --only compact,expire_snapshots
```

## Local Workflow

The local runtime is organized as a staged filesystem workflow:

1. `ingest run` writes durable source-aligned artifacts into `level1/`
2. `normalize run` turns `level1` manifests into source-aligned `level2/` tables or records a bypass
3. `sql compile` resolves tokens and validates model selection
4. `sql run` materializes downstream `level3/` and `level4/` models
5. `publish run` exports approved `level4` datasets into run-scoped `level5` delivery artifacts
6. `schedule run` orchestrates the above commands in a deterministic local sequence

This reflects the currently implemented runtime path through `level5`. `level2` through `level4` are Spark-backed parquet datasets under a local warehouse root: `normalize run` writes `level2` via Spark, `sql run` materializes `level3`/`level4` via Spark SQL reading `level2` sources declared on `level3` model manifests, and `publish run` reads `level4` parquet via Spark to produce local file-based `level5` deliveries. The current publish implementation supports run-scoped manifests, publish definition discovery/validation, explain-mode, CSV, `jsonl`, and `tsv` execution, optional zip packaging for file deliveries, the `versioned_delivery`, `overwrite_in_place`, and `append_new_artifact` replacement behaviors, and audit-driven publish reruns plus windowed backfill tagging. Broader delivery patterns remain follow-on work.

Runtime metadata is persisted under the selected root path, including:

- `level1/`: raw landed artifacts and manifests
- `level2/`: Spark-written parquet datasets (source-aligned tables) and mapping catalogs; the `source=`, `entity=`, and `ingest_date=` path segments are addressing metadata, not queryable Spark partition columns
- `runs/`: audit, structured logs, lineage, and stage-scoped rerun artifacts
- `state/`: local checkpoint history

`sql run` and `publish run` take a separate `--warehouse-root`, distinct from `--root-path`, containing the Spark-written `level3/` and `level4/` parquet tables (one directory per table, flat-namespaced by `target.table_name`).

When omitted, `--root-path` defaults to `.ignore/runtime` and `--warehouse-root` defaults to `.ignore/warehouse` — both under a single gitignored `.ignore/` directory, so bare commands run from the repo do not pollute the working tree. Pass explicit paths for real runs.

## Optional Lineage Backend

The runtime ships with **true OpenLineage 2.0.2 wire-compatible lineage export** behind the existing lineage adapter seam.

- Local `runs/.../lineage.jsonl` artifacts remain authoritative and are always written first (bespoke schema, always-on regardless of remote backend).
- Remote emission is optional and configured entirely through environment variables (same 5-env-var pattern as observability subsystems).
- The remote backend `openlineage_http` emits **standard OpenLineage `RunEvent`** payloads (not bespoke-shaped): `eventType`, `eventTime`, `run.runId` + `run.facets`, `job.namespace` + `job.name` + `job.facets`, `inputs[]`/`outputs[]` with full `namespace|name|facets|inputFacets/outputFacets`, a `producer` URI, and an OpenLineage 2.0.2 `schemaURL`.
- The framework automatically injects the standard `EnvironmentRunFacet` (with OL-spec facet-URI `_producer`/`_schemaURL` fields) when a run's `environment` is set.
- Compatible out of the box with Marquez, DataHub, OpenMetadata, Apache Atlas, and any other consumer that accepts OpenLineage 1.x/2.x HTTP events.
- Remote emission failures are recorded in local `logs.jsonl` and `errors.jsonl`; use `ELT_PIPELINE_LINEAGE_POLICY=blocking` only when a remote backend must fail the stage.

Example enablement (local Marquez default endpoint):

```bash
export ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http
export ELT_PIPELINE_LINEAGE_URL=http://localhost:5000/api/v1/lineage
export ELT_PIPELINE_LINEAGE_POLICY=best_effort
export ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS=10
# Optional when the backend expects an Authorization header.
export ELT_PIPELINE_LINEAGE_AUTH_HEADER="Bearer <token>"
```

Supported variables (all centralized in `EnvVarNames` in `config/runtime_manifest.py`):

- `ELT_PIPELINE_LINEAGE_BACKEND`: set to `openlineage_http` to enable remote emission
- `ELT_PIPELINE_LINEAGE_URL`: full `http` or `https` endpoint URL for OpenLineage event submission
- `ELT_PIPELINE_LINEAGE_POLICY`: `best_effort` (default, warn + record; non-blocking) or `blocking` (fail the run)
- `ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS`: positive request timeout in seconds; default 10
- `ELT_PIPELINE_LINEAGE_AUTH_HEADER`: optional `Authorization` header value sent with requests (e.g. `Bearer <token>` / `Basic <base64>`)

All `ELT_PIPELINE_LINEAGE_*` values are trimmed before validation.
`ELT_PIPELINE_LINEAGE_BACKEND` and `ELT_PIPELINE_LINEAGE_POLICY` are accepted
case-insensitively, but the normalized lowercase values shown above remain the
recommended form in scripts and documentation.

## Optional Orchestration Wrapper

The runtime now includes one reference orchestration integration for Airflow in addition to the generic subprocess boundary under `elt_pipeline.integrations.orchestration`.

- The CLI remains authoritative; the wrapper still invokes `python -m elt_pipeline ...`.
- Local `runs/.../audit.json`, `logs.jsonl`, `lineage.jsonl`, and checkpoint artifacts remain authoritative.
- Airflow metadata is attached as supplemental run attributes through environment variables, so downstream audit records keep the same platform-owned `run_id`.
- Airflow is not a base dependency of this project; the bundled example DAG is only for environments that install Airflow separately.

Reference files:

- `examples/orchestration/airflow/reference_dag.py`
- `src/elt_pipeline/integrations/orchestration.py`

Reference usage inside an Airflow task:

```python
from pathlib import Path

from elt_pipeline.integrations import AirflowCliWrapper

wrapper = AirflowCliWrapper(repo_root=Path("/path/to/elt_pipeline"))


def run_publish(**context) -> None:
    wrapper.invoke(
        subcommand=("publish", "run"),
        arguments=(
            "/path/to/publish_defs",
            "--warehouse-root",
            "/path/to/warehouse",
            "--root-path",
            "/path/to/runtime",
            "--job-name",
            "airflow-publish-run",
        ),
        airflow_context=context,
        timeout_seconds=300.0,
    )
```

Disablement and failure behavior:

- Do nothing to disable it; omit the wrapper and run the CLI directly.
- If the wrapped CLI command fails, the wrapper raises the same structured runtime error used elsewhere in the platform.
- Even when an Airflow-managed run fails, operators should inspect local run artifacts first because they remain the source of truth for replay and investigation.

## Optional Data-Quality Hooks

The runtime now includes **two built-in data-quality backends** behind the `QualityHookAdapter` seam, plus a full quarantine/DLQ write path for failed rows:

### 1. Row-count sanity backend (`row_count_threshold`)
Original reference backend: asserts each output dataset's row count meets a configured minimum. See supported variables below.

### 2. Built-in check library backend (`builtin_checks`)
Production 6-check library loaded from JSON or YAML file:

| Check kind | Purpose |
|---|---|
| `not_null` | No nulls in target column |
| `uniqueness` | Unique key across one or more columns |
| `range` | Numeric min/max (inclusive) bounds for a column |
| `referential_integrity` | Source key exists in target dataset's key column (in-run datasets auto-seeded as refs) |
| `freshness` | Max age for a timestamp column (seconds) |
| `regex_format` | Values in a column match a compiled Python regex pattern |

### Quarantine / DLQ write path
Any check failure (regardless of blocking policy) writes failed records to a scheme-agnostic JSONL layout:
```
{runs_dir}/quality_quarantine/{stage}/{check_name}__{dataset_id}.jsonl
```
Each line wraps the bad record with:
- `quarantine.run_id`, `quarantine.stage`, `quarantine.check_name`, `quarantine.dataset_id`
- `quarantine.policy`, `quarantine.blocking`, `quarantine.backend_type`
- `quarantine.observed_value`, `quarantine.expected_value`, `quarantine.kind`
- `quarantine.extra_metadata` (backend-specific)
- `quarantine_row_index` (0-based within this quarantine file)
- `record` or `value` (the offending row / offending scalar)

A WARNING-class `quality_quarantine_written` log event is emitted with every written path + row count, so audit streams can forward quarantine locations downstream for triage.

- Quality hooks run only after `normalize run` and `sql run`; publish-stage quality is still out of scope unless a later PRD extends it.
- Local stage artifacts remain authoritative whether the quality backend is enabled or disabled.
- Quality outcomes are recorded in stage audit `validation_results`, structured `logs.jsonl`, and stage metrics such as `quality.pass`, `quality.warn`, `quality.fail`, and `quality.skipped`.

Example enablement (row-count backend):

```bash
export ELT_PIPELINE_QUALITY_BACKEND=row_count_threshold
export ELT_PIPELINE_QUALITY_ROW_COUNT_MIN=1
export ELT_PIPELINE_QUALITY_POLICY=best_effort
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql
```

Example enablement (builtin check library via YAML):

```bash
export ELT_PIPELINE_QUALITY_BACKEND=builtin_checks
export ELT_PIPELINE_QUALITY_CHECKS_YAML=./config/builtin_quality_checks.yaml
export ELT_PIPELINE_QUALITY_POLICY=blocking
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql
```

Supported variables:

- `ELT_PIPELINE_QUALITY_BACKEND`: `row_count_threshold` OR `builtin_checks` (enable a backend)
- `ELT_PIPELINE_QUALITY_POLICY`: `best_effort` (continue on failure) or `blocking` (fail run on failure)
- `ELT_PIPELINE_QUALITY_STAGES`: comma-separated subset of `normalize` and `sql`
- `ELT_PIPELINE_QUALITY_ROW_COUNT_MIN`: minimum allowed row count per dataset (row_count_threshold backend only)
- `ELT_PIPELINE_QUALITY_CHECKS_JSON`: absolute or relative path to builtin checks JSON file (`builtin_checks` backend only)
- `ELT_PIPELINE_QUALITY_CHECKS_YAML`: absolute or relative path to builtin checks YAML file (`builtin_checks` backend only)

All `ELT_PIPELINE_QUALITY_*` values are trimmed before validation.
`ELT_PIPELINE_QUALITY_BACKEND`, `ELT_PIPELINE_QUALITY_POLICY`, and
`ELT_PIPELINE_QUALITY_STAGES` are accepted case-insensitively, but the
normalized lowercase values shown above remain the recommended form in scripts
and documentation.

Disablement and failure behavior:

- Leave `ELT_PIPELINE_QUALITY_BACKEND` unset to disable the integration entirely.
- Do NOT set both `ELT_PIPELINE_QUALITY_CHECKS_JSON` and `ELT_PIPELINE_QUALITY_CHECKS_YAML` simultaneously (raises `ConfigValidationError` with `ambiguous_builtin_dq_config`).
- Use `best_effort` when quality evidence should be captured without failing an otherwise successful stage.
- Use `blocking` when any failed quality result must fail the stage with `QUALITY_CHECK_FAILED`.
- **Quarantine files are ALWAYS written before the run outcome is decided**: even a blocking stage failure leaves the triage dataset behind (never silently dropped).
- Backend execution failures are recorded locally with `QUALITY_BACKEND_EXECUTION_FAILED`, so operators can distinguish core stage success from optional quality integration failure.

## Runnable Examples

The repository now includes runnable local connector configs under `examples/configs/`:

- `local_object_storage_orders.yaml`: JSON object storage ingest plus normalization
- `local_object_storage_orders_csv_bypass.yaml`: CSV ingest with `bypass_level2`
- `local_sqlite_orders_delta.yaml`: sqlite delta ingest after seeding `.ignore/example-source.db`
- `local_kafka_orders_replay.yaml`: Kafka replay ingest from `examples/data/kafka/orders-events.jsonl`
- `local_rest_orders.yaml`: REST ingest against a local static HTTP endpoint served from `examples/data/rest_api/`

It also includes a runnable publish package under `examples/publish/local_demo/` for local `level4 -> level5` CSV, `jsonl`, `tsv`, and zip-bundled export workflows against the bundled SQL demo warehouse.

See `examples/README.md` for setup commands and stage-by-stage usage. See `docs/operator/LOCAL_OPERATOR_RUNBOOK.md` and `docs/operator/TROUBLESHOOTING.md` for reruns, backfills, schedule execution, and artifact inspection guidance.

Maintainers should also use `docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md` for the local quality gates, smoke checks, packaging steps, and CI expectations.

## End-to-End Local Demo

The repository includes an example SQL package under `examples/sql/local_demo/` and a matching publish package under `examples/publish/local_demo/`. A typical local workflow looks like this:

1. Create or point to a pipeline YAML file for a local source.
2. Run `ingest run` into a writable runtime root.
3. Run `normalize run` against the same runtime root, producing `level2` parquet tables under it.
4. Declare the resulting `level2` table(s) as `sources` on the relevant `level3` model manifest(s) in `examples/sql/local_demo/`.
5. Run `sql compile` or `sql run` against `examples/sql/local_demo/`, pointing `--root-path` at the same runtime root and `--warehouse-root` at where `level3`/`level4` should be written.
6. Run `publish validate`, `publish explain`, or `publish run` against `examples/publish/local_demo/`, pointing `--warehouse-root` at the same warehouse used by `sql run`.

Example command sequence:

```bash
uv run elt-pipeline ingest run path/to/pipeline.yaml \
  --root-path path/to/runtime \
  --source my_source \
  --entity my_entity

uv run elt-pipeline normalize run path/to/pipeline.yaml \
  --root-path path/to/runtime \
  --source my_source \
  --entity my_entity

uv run elt-pipeline sql compile examples/sql/local_demo \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

uv run elt-pipeline sql run examples/sql/local_demo \
  --root-path path/to/runtime \
  --warehouse-root path/to/warehouse \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

uv run elt-pipeline publish validate examples/publish/local_demo

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path path/to/runtime \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path path/to/runtime \
  --warehouse-root path/to/warehouse \
  --publish daily_order_export \
  --window-label 2026-01
```

## Schedule Plans

Schedule plans are YAML documents that invoke existing CLI commands in order.

See `examples/schedules/local_demo.yaml` for a full example. The high-level shape is:

```yaml
jobs:
  - name: validate
    argv:
      - validate-config
      - /absolute/path/to/pipeline.yaml
  - name: ingest
    argv:
      - ingest
      - run
      - /absolute/path/to/pipeline.yaml
      - --root-path
      - /absolute/path/to/runtime
continue_on_error: false
```

Each job omits the program name and passes the same arguments you would provide after `elt-pipeline`.

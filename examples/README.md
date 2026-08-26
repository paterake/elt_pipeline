# Examples

Run all example commands from the repository root so relative paths resolve correctly.

## Example Configs

- `examples/configs/local_object_storage_orders.yaml`: object storage ingest using bundled JSON sample data
- `examples/configs/local_object_storage_orders_csv_bypass.yaml`: object storage ingest with CSV payloads and `bypass_level2`
- `examples/configs/local_sqlite_orders_delta.yaml`: sqlite delta ingest after seeding `.ignore/example-source.db`
- `examples/configs/local_kafka_orders_replay.yaml`: Kafka replay ingest from a bundled JSONL log
- `examples/configs/local_rest_orders.yaml`: REST ingest against a local static HTTP endpoint
- `examples/configs/databricks_unity_adls.yaml`: **Databricks + Unity Catalog (B-3)** — reference deployment using Unity as an Iceberg REST catalog (catalog_type=rest) with your cloud's native object store (Azure ADLS Gen2 / AWS S3 / GCP GCS). All three backing stores are commented selectable. No `dbfs://` scheme required (and intentionally not supported): Databricks storage is S3/GCS/ADLS underneath.
- `examples/publish/local_demo/`: runnable `level5` publish definitions for CSV, `jsonl`, `tsv`, and zip-bundled local exports
- `examples/sql/local_demo/`: local SQL model package that prepares example `level4` tables for publish runs

## Orchestration Examples (G-3)

All orchestration examples follow the **thin CLI wrapper** pattern: each orchestrator task is a python callable that invokes the standard `elt-pipeline` CLI via `subprocess` with the orchestrator's native context (dag_id/run_id/task_id/try_number / job/op / flow/task_run) forwarded as `ELT_PIPELINE_ORCHESTRATION_*` env vars that appear in every run's audit record, lineage events, metrics labels, and observability spans. Each wrapper supports DI for the subprocess invoker (so you can test without a real orchestrator installed) plus per-task retries, back-pressure, and backoff semantics come from the orchestrator's native retry knobs (you don't need bespoke scheduler code in the pipeline library).

- `examples/orchestration/airflow/reference_dag.py`: **Apache Airflow** — full 7-task DAG (ingest → normalize → sql_compile → sql_run → publish_validate → publish_run → maintain_iceberg_tables) using `AirflowCliWrapper` + `PythonOperator`. Sets retries=2 with 1-min retry_delay via Airflow `default_args`. Uses `**context` auto-passthrough so `dag_id`/`run_id`/`task_id`/`try_number`/`dag.tags`/`logical_date` are all forwarded via `build_airflow_orchestration_metadata(context)` — all 6 Airflow context extraction fields supported.
- `examples/orchestration/dagster/reference_assets.py`: **Dagster** — 4-asset graph (`ingest_orders_l1 → normalize_orders_l2 → sql_orders_l3_l4 → publish_orders_l5) using `DagsterCliWrapper` + `@asset` decorators with `PipelineConfig` config schema for environment/source/entity/start_date/end_date parameters plus `elt_pipeline_daily_job` with `max_retries=2`. Forward job name, run_id, op name, retry_number (+1 for 1-indexed), run tags, and partition key via `build_dagster_orchestration_metadata(context)`.
- `examples/orchestration/prefect/reference_flow.py`: **Prefect** — 4-task flow (`ingest_orders_l1 → normalize_orders_l2 → sql_compile_and_run → publish_orders_l5`) using `PrefectCliWrapper` + `@flow`/`@task` decorators with `retries=2` and `retry_delay_seconds=30` on tasks. Flow name/run_id, task key/run id, run_count/task_run_count (→attempt_number), flow tags, and scheduled_start_time are all forwarded via `build_prefect_orchestration_metadata(context)` with context extracted from `prefect.context.get_run_context()`.

Orchestration helpers are exported from `elt_pipeline.integrations`:

```python
from elt_pipeline.integrations import (
    AirflowCliWrapper, DagsterCliWrapper, PrefectCliWrapper,
    build_airflow_orchestration_metadata,
    build_dagster_orchestration_metadata,
    build_prefect_orchestration_metadata,
    OrchestrationMetadata, load_orchestration_metadata_from_env,
    CliInvocationRequest, SubprocessCliInvoker,
)
```

## Deployment & Containerization Examples (G-4)

Reference deployment artifacts live at the repo root. All share the same pinned
stack (JDK 23 / Spark 4.1.2 Hadoop 3 / Trino 468 / Iceberg 1.11.0 / Python 3.11)
via a single multi-stage Dockerfile.

**Local workstation (docker-compose):** zero-service local Iceberg warehouse
shared between the ELT CLI runner and a foreground Trino serving container:

```bash
# 1. Build image + run the 5-phase demo (ingest→normalize→sql→maintain)
docker compose run --rm demo

# 2. Bring up Trino serving on http://localhost:8080 (reads the same Iceberg warehouse)
docker compose up -d trino
sleep 30

# 3. Query via Trino CLI inside the serving container
docker compose exec trino trino --catalog iceberg --execute 'SHOW SCHEMAS'
docker compose exec trino trino --catalog iceberg --execute 'SELECT COUNT(*) FROM sales.order_summary'
```

Layout:
- `Dockerfile` — 3 stages: uv wheel-builder → Spark/Trino dist-fetcher → `eclipse-temurin:23-jdk` runtime with `tini` init + `/opt/elt_pipeline_venv` (wheel install) + `/opt/spark` + `/opt/trino`. Build-arg `EXTRAS=spark,s3,gcs,adls,delta` selects optional deps.
- `docker-compose.yml` — 2 services (`elt_pipeline`, `trino`) with shared `./docker-volumes/repo_run:/var/lib/elt_pipeline` bind mount; `x-elt-common` YAML anchor reuses build args + env + volumes; `cli`/`demo` convenience aliases.
- `.dockerignore` — excludes `.venv`, `.ignore/`, tests, docs, local caches.

**Kubernetes (Kustomize base + dev overlay):** reference manifests for a real cluster:

```bash
kubectl apply -k deploy/overlays/dev
```

Layout (see `deploy/README.md` for the full contract):
- `deploy/base/configmap.yaml` — pipeline.yaml mounted at `/etc/elt_pipeline/pipeline.yaml` (pins zero-service jdbc+sqlite serving catalog for the demo; swap to `rest`/`glue` for multi-replica).
- `deploy/base/pvc-warehouse.yaml` — 50Gi ReadWriteOnce PVC for the shared Iceberg warehouse; swap StorageClass/RWX for your cluster.
- `deploy/base/service-trino.yaml` — ClusterIP :8080 for Trino HTTP/JDBC clients inside the cluster.
- `deploy/base/deployment-trino.yaml` — single-replica Deployment with Recreate strategy, readiness/liveness probes on `/v1/info`, 4-core/12Gi default resource limits, runAsNonRoot + fsGroup 1000.
- `deploy/base/cronjob-daily-elt.yaml` — 03:00 UTC CronJob (2 backoffLimit retries, OnFailure restart) running daily ingest→normalize→sql end-to-end.
- `deploy/overlays/dev/namespace.yaml` + `kustomization.yaml` — commonLabels + image-override hook for your registry.

Container scripts (copied into image at `/usr/share/elt_pipeline/docker/`):
- `entrypoint.sh` — `tini` child init with `demo` / `trino-start` sugar commands → run `/usr/share/elt_pipeline/docker/run_demo.sh` or `trino_foreground.sh`.
- `run_demo.sh` — 5-phase local_demo end-to-end: validate-config → ingest run → normalize run → sql run (L3 Iceberg + L4 marts, sales domain, 2026-01 window) → `maintain run` compaction + snapshot expiry.
- `trino_foreground.sh` — foreground wrapper for Trino: first runs `ops/trino_serving/run_trino.sh write-configs` then execs `/opt/trino/bin/launcher --verbose … run` (foreground launcher subcommand → container stdout/stderr logs → clean SIGTERM shutdown).

## Lineage Export Examples (G-7)

OpenLineage 2.0.2 wire-compatible export is enabled by env vars (same 5-var
pattern as §6 observability subsystems). Native `runs/.../lineage.jsonl`
artifacts are **always** written locally first regardless of remote backend
configuration; the remote emitter is a best-effort supplementary sink by
default.

### Local Marquez quick start

[Marquez](https://marquezproject.github.io/) is the reference OpenLineage
server; its default HTTP endpoint matches the canonical path used by all OL
clients:

```bash
# 1. Enable lineage export (best_effort non-blocking by default)
export ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http
export ELT_PIPELINE_LINEAGE_URL=http://localhost:5000/api/v1/lineage
export ELT_PIPELINE_LINEAGE_POLICY=best_effort
export ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS=10
# If your Marquez instance is behind an auth-gated proxy or API gateway:
# export ELT_PIPELINE_LINEAGE_AUTH_HEADER="Bearer marquez-api-token"

# 2. Run any pipeline stage — all four phases emit START/COMPLETE/FAIL events
#    with EnvironmentRunFacet auto-injected, DatasetRef inputs/outputs, and
#    full facet passthrough.
uv run elt-pipeline sql run \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/warehouse-publish \
  examples/configs/local_demo_pipeline.yaml

# 3. Browse in Marquez UI at http://localhost:3000 — the job namespace is
#    `elt_pipeline` (override per-event via `LineageEvent.job_namespace`), and
#    each run is keyed by the platform's deterministic `run_id`.
```

### DataHub / OpenMetadata / Apache Atlas

All three accept OpenLineage 1.x/2.x `RunEvent` payloads natively through
their respective OpenLineage HTTP ingress endpoints. Point
`ELT_PIPELINE_LINEAGE_URL` at your deployment's OL endpoint and configure
`ELT_PIPELINE_LINEAGE_AUTH_HEADER` with the required token:

| Platform | Typical endpoint |
|---|---|
| DataHub | `https://<datahub-host>/api/v2/lineage/openlineage` |
| OpenMetadata | `https://<om-host>/api/v1/lineage/openlineage` |
| Apache Atlas (with OL plugin) | `https://<atlas-host>/api/atlas/v2/openlineage/events` |

**Public API & constructor entry points:**

```python
from elt_pipeline.shared.lineage import (
    LineageEvent,
    DatasetRef,
    OpenLineageRunEvent,
    convert_to_openlineage_run_event,  # pure converter, zero I/O
)

from elt_pipeline.integrations import (
    OpenLineageHttpEmitter,   # standalone emitter with URL + timeout + auth
    LineageAdapter,
    LineageEmissionPolicy,
    build_lineage_adapter,    # env-driven factory (ELT_PIPELINE_LINEAGE_*)
)
```

## Data Quality & Quarantine (G-8)

The platform ships two built-in data-quality backends behind the existing
adapter seam. **Any failing quality check writes a quarantine artifact** —
even blocking failures persist triage data before failing the run. Use
`ELT_PIPELINE_QUALITY_CHECKS_YAML` (or JSON) to configure the built-in
library; the classic `row_count_threshold` backend remains unchanged.

### Example 1: 6-check builtin library via YAML + blocking policy

Create `./config/builtin_quality_checks.yaml`:

```yaml
checks:
  # Users table: enforce not-null email + valid format + user_id unique
  - kind: not_null
    check_name: users.email.not_null
    column: email
    dataset_id: level2.users

  - kind: regex_format
    check_name: users.email.format_match
    column: email
    pattern: "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
    dataset_id: level2.users

  - kind: uniqueness
    check_name: users.user_id.unique
    columns: [user_id]
    dataset_id: level2.users

  # Orders table: amount in band; placed_at <= 7 days stale (pipeline freshness)
  - kind: range
    check_name: orders.amount.in_band
    column: amount
    min_value: 0.0
    max_value: 1_000_000.0
    dataset_id: level2.orders

  - kind: freshness
    check_name: orders.placed_at.freshness_7d
    timestamp_column: placed_at
    max_age_seconds: 604800.0   # 7 days
    dataset_id: level2.orders

  # Referential integrity: every order.user_id exists in users.user_id
  # (No need to seed reference_datasets manually — in-run datasets are
  #  auto-seeded from same-stage outputs that carry records, so orders→users
  #  works out of the box when both appear in QualityHookRequest.datasets.)
  - kind: referential_integrity
    check_name: orders.users.fk
    source_column: user_id
    target_dataset_id: level2.users
    target_column: user_id
    dataset_id: level2.orders
```

Enable via env vars and run a normalize or SQL stage:

```bash
export ELT_PIPELINE_QUALITY_BACKEND=builtin_checks
export ELT_PIPELINE_QUALITY_CHECKS_YAML=./config/builtin_quality_checks.yaml
export ELT_PIPELINE_QUALITY_POLICY=blocking
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql

uv run elt-pipeline sql run \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/warehouse-publish \
  examples/configs/local_demo_pipeline.yaml
```

### Expected quarantine layout on check failure

When a check fails, failed rows land in the B-6 storage-backend path
utils-driven quarantine layout alongside logs/errors/lineage artifacts:

```
{root_path}/runs/stage={stage}/job={job}/run_id={run_id}/
├── logs.jsonl
├── errors.jsonl
├── lineage.jsonl
└── quality_quarantine/
    └── {stage}/
        ├── users.email.not_null__level2.users.jsonl
        ├── users.email.format_match__level2.users.jsonl
        ├── orders.amount.in_band__level2.orders.jsonl
        └── orders.placed_at.freshness_7d__level2.orders.jsonl
```

Each JSONL line wraps the bad record with metadata so a triage job can
re-issue corrections:

```json
{
  "quarantine": {
    "run_id": "run-abc…",
    "stage": "sql",
    "check_name": "orders.amount.in_band",
    "dataset_id": "level2.orders",
    "policy": "blocking",
    "blocking": true,
    "backend_type": "builtin_checks",
    "kind": "range",
    "observed_value": 2,
    "expected_value": "[0.0, 1000000.0]",
    "extra": {}
  },
  "quarantine_row_index": 0,
  "record": {"order_id": 99, "user_id": 3, "amount": -50.0}
}
```

A `quality_quarantine_written` WARNING event also surfaces in `logs.jsonl`
with the full set of written paths + row counts for downstream audit /
alerting integrations to pick up the triage artifact locations without
scanning the filesystem.

### Example 2: row-count sanity backend (unchanged; for quick setup)

For ultra-minimal sanity checks (zero config, just assert "we wrote rows"):

```bash
export ELT_PIPELINE_QUALITY_BACKEND=row_count_threshold
export ELT_PIPELINE_QUALITY_ROW_COUNT_MIN=1
export ELT_PIPELINE_QUALITY_POLICY=best_effort
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql
```

Backward compatibility note: `QualityDatasetRef.records`,
`QualityHookRequest.reference_datasets`, and `QualityCheckResult.violated_records / check_details`
are all additive with default factories; existing BYO backends that never
populated them continue to behave identically (zero quarantine writes for
BYO backends that don't surface `violated_records`).

## Connector Registry & Preset Manifest (M-1)

The M-1 connector registry enables two things: (1) **no-code preset authoring
WITHIN the four built-in families** (rest / sql / kafka / object_storage) via
a declarative YAML/JSON manifest — extraction/auth/settings/persistence
defaults layered under your entity config; (2) **plugin-style Python extension
with zero CLI edits** — a new connector family needs just one
`register_connector_factory()` call (no `if/elif` dispatch changes).

### Example: GitHub REST v3 preset via YAML manifest

Create `./config/connector_registry_presets.yaml` (one manifest can carry
many presets; preset names are free strings scoped to the manifest):

```yaml
schema_version: "1.0"
presets:
  - name: github_rest_v3
    family: rest
    description: >
      Sensible defaults for GitHub public REST API v3: json envelope,
      page-based pagination, Accept + User-Agent headers, 30s timeout,
      static bearer token auth via GITHUB_TOKEN env passthrough.
    extraction_defaults:
      base_url: "https://api.github.com"
      headers:
        Accept: "application/vnd.github+json"
        X-GitHub-Api-Version: "2022-11-28"
        User-Agent: "elt-pipeline-m1"
      timeout_seconds: 30
      retry:
        max_attempts: 5
        backoff_base_seconds: 1.5
      pagination:
        strategy: page_offset
        page_param: page
        per_page_param: per_page
        per_page: 100
      envelope:
        payload_path: "$"
    auth_defaults:
      strategy: api_key_header
      header_name: Authorization
      # Entity config still supplies the real value via secret_refs / env.
      # Presets only carry *shapes* and defaults, never plaintext secrets.
      value_template: "Bearer ${GITHUB_TOKEN}"
    settings_defaults:
      connector_preset: github_rest_v3
    persistence_defaults:
      landing_format: json
```

In your entity config, set `settings.connector_preset: github_rest_v3` to
opt in. The preset's extraction/auth/settings/persistence keys are
**shallowly merged UNDER** your entity's top-level values (entity wins on
any overlap — no deep key-level dict merge; use presets for *base configs*,
entities for per-pipeline overrides).

Wire it up and run:

```bash
# Path to the manifest (YAML or JSON; .yaml/.yml/.json auto-detected with
# fallback try-ordering on unknown extensions).
export ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST=./config/connector_registry_presets.yaml

# Strict mode (1): raise ConfigValidationError if the manifest can't be
# loaded / parsed / validated — safe for CI.  Non-strict (0, default):
# silently skip the manifest and run the entity config as-is.
export ELT_PIPELINE_CONNECTOR_REGISTRY_STRICT=1

uv run elt-pipeline ingest run \
  examples/configs/local_rest_orders.yaml \
  --root-path .ignore/runtime-rest-preset
```

### Python plugin-extension surface (new families without CLI edits)

To add a *new* family (e.g. a generic SFTP source), implement the
`ConnectorFactory` Protocol (2 methods + one attribute) and register it —
the CLI ingest dispatch already routes through `get_connector_factory()`,
so no `if/elif` source edits are required:

```python
from elt_pipeline.ingest import (
    ConnectorFamily,         # explicit boundary enum; rest/sql/kafka/object_storage built in
    ConnectorFactory,        # @runtime_checkable Protocol
    ConnectorManifest, ConnectorPreset,
    ConnectorRegistryError, ConnectorFamilyUnsupportedError,
    register_connector_factory, get_connector_factory, is_connector_factory_registered,
    load_connector_manifest_from_yaml, load_connector_manifest_from_json,
    apply_connector_preset_defaults,
)
from pydantic import BaseModel

class SftpConnectorConfig(BaseModel): ...        # your validated config
class LocalSftpConnector: ...                     # your runnable concrete

class _SftpConnectorFactory:
    family_type: str = "sftp"
    def build_config_from_resolved(self, *, resolved_config) -> BaseModel:
        return SftpConnectorConfig.from_resolved_entity_config(resolved_config)
    def build_connector(self, *, config, run_context, root_path, **kwargs):
        return LocalSftpConnector(config=config, run_context=run_context, root_path=root_path)

# Register BEFORE ingest dispatch (e.g. in a sitecustomize.py or your own
# CLI wrapper that invokes elt-pipeline ingest … as a subprocess import).
register_connector_factory("sftp", _SftpConnectorFactory())
assert is_connector_factory_registered("sftp")
factory = get_connector_factory("sftp")
```

Backward compatibility note: `ConnectorFamily`, `ConnectorFactory`, the
registry singleton, the manifest/preset loaders, and the 2 env vars are
all 100% additive. Existing pipelines that never set
`ELT_PIPELINE_CONNECTOR_REGISTRY_MANIFEST` or `settings.connector_preset`
run identically to pre-M-1 (the manifest loader returns `None` on unset
path in non-strict mode, and `apply_connector_preset_defaults` is a no-op
when `connector_preset` is absent).

## Secrets Resolver Examples (S1–S4, built on G-5 seam)

The G-5 `secrets` subsystem supports **6 secret_ref URI schemes** — all are
now 🟢 Production. Bare refs (no `://` scheme) default to `env://` for
backward compatibility. All cloud/Vault resolvers use **lazy SDK imports** at
`resolve()` time with a `SECRETS_SDK_MISSING` error (message + context) that
directly names the exact Python package to install; missing a cloud SDK won't
break projects that only use `env://`/`file://`.

| Scheme | URI example | Required package |
|---|---|---|
| `env://` (default) | `ORDERS_API_TOKEN` or `env://ORDERS_API_TOKEN` | (stdlib) |
| `file://` | `file:///var/run/secrets/orders-api-token` | (stdlib) |
| `aws_secretsmanager://` | `aws_secretsmanager://orders/api-token` or `aws_secretsmanager://orders/api-token:AWSPREVIOUS` | `boto3` |
| `azure_keyvault://` | `azure_keyvault://prod-vault/orders-token` or `azure_keyvault://prod-vault/orders-token/abcdef1234` | `azure-keyvault-secrets` + `azure-identity` |
| `gcp_secretmanager://` | `gcp_secretmanager://my-gcp-proj/orders-token` or `gcp_secretmanager://my-gcp-proj/orders-token/5` | `google-cloud-secret-manager` |
| `vault://` | `vault://kv/data/orders#api_token` (KV-v2, field selector) or `vault://kv/data/orders` (whole payload as JSON) | `hvac` |

### How to use in entity config (REST connector example)

```yaml
schema_version: v1
environment: prod
source_name: payments
entity_name: invoices
connector_type: rest
extraction:
  base_url: https://api.payments.example.com
  method: GET
  headers:
    Authorization: "Bearer {resolved_token}"
auth:
  # No literal secrets in YAML! This placeholder tells the connector to
  # call resolve_secret("token", secret_ref) at runtime. The actual
  # `resolved_token` above comes from RestConnectorBase resolve_secret.
  token:
    # Option A: AWS Secrets Manager (requires boto3 installed)
    secret_ref: aws_secretsmanager://payments/prod/api-key

    # Option B: Azure Key Vault (AKV)
    # secret_ref: azure_keyvault://payments-vault/payments-api-key

    # Option C: GCP Secret Manager
    # secret_ref: gcp_secretmanager://acme-prod-42/payments-api-key

    # Option D: HashiCorp Vault (KV-v2) — selects field "key" inside data.data
    # secret_ref: vault://kv/data/payments#key

    # Option E: env var (default when no :// present — existing configs work!)
    # secret_ref: PAYMENTS_API_TOKEN
```

> **Operator install cheat sheet** (pick the ones you actually use):
> ```bash
> # AWS SM
> uv add boto3
> # Azure KV
> uv add azure-keyvault-secrets azure-identity
> # GCP SM
> uv add google-cloud-secret-manager
> # HashiCorp Vault (any auth)
> uv add hvac
> ```

### Operator-friendly error map (all paths fail-fast with structured `error_code`)

| Secret scheme | `SecretNotFoundError` | Access denied | SDK missing | Other |
|---|---|---|---|---|
| AWS SM | ✓ (ResourceNotFoundException) | SECRETS_AWS_ACCESS_DENIED | SECRETS_SDK_MISSING (boto3) | SECRETS_AWS_SDK_ERROR / BINARY_NOT_TEXT / EMPTY_RESPONSE |
| Azure KV | ✓ (ResourceNotFound / SecretNotFound) | SECRETS_AZURE_AUTH_FAILED / ACCESS_DENIED (403) | SECRETS_SDK_MISSING (azure-keyvault-secrets / azure-identity) | SECRETS_AZURE_SDK_ERROR / EMPTY_VALUE |
| GCP SM | ✓ (NotFound / 404 text match) | SECRETS_GCP_ACCESS_DENIED | SECRETS_SDK_MISSING (google-cloud-secret-manager) | SECRETS_GCP_SDK_ERROR / EMPTY_PAYLOAD / BINARY_NOT_TEXT |
| Vault | ✓ (InvalidPath / data.data None / missing #field w/ Available keys) | SECRETS_VAULT_UNAUTHORIZED / FORBIDDEN / APPROLE_FAILED / URL_MISSING | SECRETS_SDK_MISSING (hvac) | SECRETS_VAULT_SDK_ERROR / BINARY_NOT_TEXT |

### Extending: adding a custom secret provider

New secret schemes land via the same `register_provider()` public API the
default registry uses. No SDK coupling to the core:

```python
from elt_pipeline.shared.secrets import register_provider, SecretValue, SecretScheme

class DummySecrets:
    provider_type = "dummy"
    def resolve(self, *, path: str) -> SecretValue:
        return SecretValue("dummy-secret-for-" + path)

# Option 1: register into an existing SecretScheme enum slot (overrides the default for custom deployments)
# You must pop from the internal registry first — register_provider has duplicate-install guard
from elt_pipeline.shared.secrets import _PROVIDER_REGISTRY
_PROVIDER_REGISTRY.pop(SecretScheme.env, None)  # remove default EnvVarSecrets first
register_provider(SecretScheme.env, DummySecrets())
```

## Catalog Preflight (B-0)

The B-0 catalog preflight validator runs **before every SparkSession boot**
in both `sql run` and `publish run` branches, failing fast on catalog
misconfiguration / connectivity issues instead of letting Spark surface
an opaque multi-hundred-line `Py4JJavaError` stack trace mid-stage (after
waiting for JVM boot and potentially minutes of prior L1/L2 work). It
covers ALL valid catalog-type bindings: writer types `{jdbc, rest, nessie,
hive_metastore, glue, hadoop}` × serving types `{jdbc, rest, nessie,
hive_metastore, glue, hadoop, snowflake}` with scheme-aware checks per
binding.

### Mode semantics (3 modes, additive-only, backward-compat default)

| Mode | Failure behaviour | Operator output | Intended use |
|---|---|---|---|
| `off` | No checks run. NONE. Zero overhead. | Nothing. | CI fire-and-forget runs where Spark-boot success is already asserted by a separate job, or sandboxed workstation invocations where the default SQLite/hadoop binding is already known-good. |
| `best_effort` (**DEFAULT**) | Check failures **NEVER block the run**. Spark boot still proceeds. | Structured warning block to STDERR: `[sql] catalog preflight: best_effort — 2 failures before Spark boot` + one bulleted line per failure: `- [writer] hive_metastore_uri_format: missing thrift:// prefix`. The operator sees the misconfig immediately; Spark still runs (may fail later if catalog is truly unreachable, but triage signal is preserved in logs). | Workstation default. Existing pipelines behave EXACTLY as pre-B-0 — this is why it's the default. |
| `strict` | Check failures **raise `ConfigValidationError` BEFORE any JVM/Spark boot**. Exitcode non-zero. | Structured multi-line error message: one `[binding] checkname: message` per failure, plus a machine-readable `context` dict with `failed_checks` (list of dicts), `total_checks`, `failed_count`. | **Recommended CI / orchestration production mode.** Fail the run at stage-start time, cleanly, with operator-readable triage. |

Two env vars (centralized in `EnvVarNames` dataclass):

```
ELT_PIPELINE_CATALOG_PREFLIGHT_MODE     = off | best_effort | strict
ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS = 3 | 5 | 10 ... (default 5)
```

### Example 1: best_effort workstation default (no env changes needed)

The default is already `best_effort`. Existing pipelines run with zero
env changes and see transparent warning output on misconfigured catalogs,
never blocked:

```bash
# Default behaviour (no env vars set) → best_effort mode.
# Write a broken hive_metastore URI to trigger a preflight warning.
export ELT_PIPELINE_WRITER_CATALOG_TYPE=hive_metastore
export ELT_PIPELINE_HIVE_METASTORE_URI="http://wrong-scheme.example.com:9083"

# Runs sql run → emits structured stderr WARNING, then proceeds to Spark
# boot transparently (Spark may fail later, but you already see the
# preflight warning in the logs FIRST).
uv run elt-pipeline sql run examples/configs/local_orders.yaml \
  --root-path .ignore/runtime-preflight-best-effort
```

### Example 2: strict CI / orchestration (recommended production mode)

For CI pipelines and orchestration wrappers (Airflow / Dagster / Prefect
/ custom k8s jobs), enable strict mode to **fail cleanly before JVM boot**:

```bash
export ELT_PIPELINE_CATALOG_PREFLIGHT_MODE=strict
export ELT_PIPELINE_CATALOG_PREFLIGHT_TIMEOUT_SECONDS=3

# Same broken hive_metastore URI as above.
export ELT_PIPELINE_WRITER_CATALOG_TYPE=hive_metastore
export ELT_PIPELINE_HIVE_METASTORE_URI="http://wrong-scheme.example.com:9083"

# sql run → EXITS cleanly with a structured ConfigValidationError
# BEFORE any Spark boot happens. Exitcode != 0 → CI step fails at
# stage-start time, not after 2+ minutes of waiting for Spark + parquet.
uv run elt-pipeline sql run examples/configs/local_orders.yaml \
  --root-path .ignore/runtime-preflight-strict
```

**Strict-mode output shape (the exact ConfigValidationError context):**

```
ConfigValidationError: catalog preflight (strict): 2 check(s) failed before Spark boot
  [writer] hive_metastore_uri_format: missing thrift:// prefix — expected thrift://<host>:<port>
  [writer] hive_metastore_tcp_connect: skipped because URI format check failed; fix format first

failed_checks:
  - binding: writer
    check_name: hive_metastore_uri_format
    message: missing thrift:// prefix — expected thrift://<host>:<port>
  - binding: writer
    check_name: hive_metastore_tcp_connect
    message: skipped because URI format check failed; fix format first
total_checks: 4
failed_count: 2
```

### Example 3: Pure-Python API (embedding in custom operators)

The preflight library is also usable as a pure-Python module with no env
reads and no CLI dependency — ideal for embedding in custom orchestration
operators that want to check catalog bindings before launching an
elt-pipeline subprocess:

```python
from elt_pipeline.shared.catalog_preflight import (
    CatalogPreflightMode,
    CatalogPreflightCheckName,
    run_catalog_preflight,
)

results = run_catalog_preflight(
    writer_catalog_type="hive_metastore",
    writer_config={
        "hive_metastore_uri": "thrift://hive.example.com:9083",
    },
    serving_catalog_type="snowflake",
    serving_config={
        "snowflake_catalog_uri": "https://myorg.snowflakecomputing.com",
    },
    mode=CatalogPreflightMode.best_effort,     # or CatalogPreflightMode.strict
    timeout_seconds=3,
)

failed = [r for r in results if not r.passed]
if failed:
    for r in failed:
        print(f"[{r.binding}] {r.check_name.value}: {r.message}")
    # Or: mode=strict above raises ConfigValidationError for you.
```

### 8 checks included (scheme-aware)

1. **`jdbc_uri_valid`** — validates `jdbc:<subprotocol>:…` URI format with subprotocol extraction + context.
2. **`jdbc_sqlite_parent_dir`** — lazily creates the parent directory of `jdbc:sqlite:file://` paths (mirrors Spark's sqlite-jdbc). Gracefully skips `:memory:` / in-memory variants.
3. **`rest_catalog_connectivity`** — GET `/v1/config` probe. Treats BOTH 2xx AND 4xx as PASS (4xx = reachable, just auth-gated — the common case for REST catalogs). Only DNS / connection refused / timeout → FAIL. Optional Bearer token Authorization header when `rest_token` is provided.
4. **`hive_metastore_uri_format`** — validates `thrift://<host>:<port>` shape with explicit `thrift://` prefix + port parseability (1-65535).
5. **`hive_metastore_tcp_connect`** — TCP socket connect (3-way handshake only, no thrift handshake) — cascaded (only runs if format check passes). Timeout-bound, socket closed immediately after success.
6. **`glue_identity_available`** — `STS.get_caller_identity()` probe. SKIPs with PASS when `boto3` is not installed (workstation-only setups). FAILs with actual boto3 error message when credentials are unresolvable.
7. **`hadoop_warehouse_dir`** — exists-or-creates path with parent fallback: dir exists → PASS; parent exists (no dir yet) → PASS; parent missing → mkdir parents then PASS; empty path → FAIL.
8. **`snowflake_serving_params`** (serving-only, `snowflake` catalog type) — validates `snowflake_catalog_uri` is present with `https://…` or `snowflake://…` scheme (the two Snowflake Polaris patterns).

All checks are pure-unit-testable (no JVM / no real network): HTTP/TCP/boto3 are fully mocked with `unittest.mock.patch` in the 50-test suite.

## Object Storage JSON

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-object-storage

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-object-storage
```

## Object Storage CSV With Level2 Bypass

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders_csv_bypass.yaml \
  --root-path .ignore/runtime-object-storage-csv

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders_csv_bypass.yaml \
  --root-path .ignore/runtime-object-storage-csv
```

## SQLite Delta Ingest

Seed the example database once:

```bash
rm -f .ignore/example-source.db
sqlite3 .ignore/example-source.db < examples/data/sql/source_init.sql
```

Run the connector:

```bash
uv run elt-pipeline ingest run examples/configs/local_sqlite_orders_delta.yaml \
  --root-path .ignore/runtime-sql
```

## Kafka Replay Ingest

```bash
uv run elt-pipeline ingest run examples/configs/local_kafka_orders_replay.yaml \
  --root-path .ignore/runtime-kafka
```

## REST Ingest

Start a local static server in one terminal:

```bash
python3 -m http.server 8000 --directory examples/data/rest_api
```

Run the connector in another terminal:

```bash
uv run elt-pipeline ingest run examples/configs/local_rest_orders.yaml \
  --root-path .ignore/runtime-rest
```

## SQL Models

The downstream SQL model package lives under `examples/sql/local_demo/`. It builds five models from the `local_files` source produced by the Object Storage JSON example — four `sales` models over the `orders` entity (`base_orders`, `canonical_orders`, `orders_ingest_snapshot`, and the `level4.sales.order_summary` datamart) plus `level3.inventory.canonical_shipments` over the `shipments` entity. Both entities are declared in `local_object_storage_orders.yaml`, so run the ingest/normalize pair (no `--entity` flag processes both) first to produce real `level2` data before running the SQL package.

## Publish / Export Happy Path

The bundled publish package under `examples/publish/local_demo/` expects the example `level4` table produced by the SQL demo package.

Produce the `level2` input expected by `examples/sql/local_demo/`:

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-publish

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .ignore/runtime-publish
```

Materialize the example `level4` datamart:

```bash
uv run elt-pipeline sql run examples/sql/local_demo \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/example-warehouse \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Validate and explain the bundled publish definitions:

```bash
uv run elt-pipeline publish validate examples/publish/local_demo

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --environment default \
  --window-label 2026-01
```

Notes:

- `publish explain` reports `stable_delivery_path` only for definitions that use `overwrite_in_place` or `append_new_artifact`.
- The bundled zip example also reports `archive_run_scoped_path` and, when applicable, `archive_stable_delivery_path`.

Run one CSV publish definition against the same warehouse:

```bash
uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/example-warehouse \
  --environment default \
  --publish daily_order_export \
  --window-label 2026-01
```

Expected outputs:

- run-scoped CSV artifacts under `.ignore/runtime-publish/artifacts/level5/.../run_id=<...>/`
- a publish manifest next to the exported file
- stage audit, logs, and lineage under `.ignore/runtime-publish/runs/stage=publish/`

Run the bundled query-based `jsonl` publish definition:

```bash
uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/example-warehouse \
  --environment default \
  --publish daily_order_export_windowed \
  --window-label 2026-01
```

Expected outputs:

- run-scoped `jsonl` artifacts under `.ignore/runtime-publish/artifacts/level5/.../run_id=<...>/`
- no stable delivery path because `daily_order_export_windowed` uses `versioned_delivery`
- a publish manifest next to the exported file

Run the bundled direct `tsv` publish definition:

```bash
uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/example-warehouse \
  --environment default \
  --publish daily_order_export_tsv \
  --window-label 2026-01
```

Expected outputs:

- run-scoped `tsv` artifacts under `.ignore/runtime-publish/artifacts/level5/.../run_id=<...>/`
- an append-only stable delivery copy whose filename includes `run_id=<...>`
- a publish manifest next to the exported file

Run the bundled CSV plus zip-bundle publish definition:

```bash
uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .ignore/runtime-publish \
  --warehouse-root .ignore/example-warehouse \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01
```

Expected outputs:

- a run-scoped CSV artifact and a sibling run-scoped `.zip` bundle under `.ignore/runtime-publish/artifacts/level5/.../run_id=<...>/`
- stable delivery copies for both the `.csv` file and the `.zip` bundle because `daily_order_export_bundle` uses `overwrite_in_place`
- `publish explain` output that includes both `run_scoped_path` and `archive_run_scoped_path`

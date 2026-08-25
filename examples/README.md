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

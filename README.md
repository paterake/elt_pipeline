# elt_pipeline

Client-neutral, configuration-driven runtime for a governed data platform.

**Honest scope at a glance:** [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md) — classifies every feature as 🟢 Production / 🟠 Demo / ⏳ Roadmap.

`elt_pipeline` is not only an ingestion and transformation tool. It is a governed data platform runtime for moving data through explicit architectural levels with strong auditability, lineage, metadata discipline, replayability, and access-control boundaries.

The platform is designed to align with DAMA-DMBOK v2 principles for:

- data architecture
- data integration and interoperability
- metadata management
- data quality
- governance and security
- operational auditability

The repository does not claim that DAMA-DMBOK v2 prescribes the exact `level1` through `level5` naming used here. Instead, those levels are the platform's chosen architecture model for operationalizing DMBOK-aligned concerns in a concrete implementation.

## Current Scope and Capabilities (Honest Boundary)

This section states what the code actually ships, so no reader infers more than is built. For the formal, per-capability classification with maturity definitions and notes, see [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md). The cross-doc roadmap and portability environment breakdown also lives in [PRD 10 §6.3](docs/prd/10-prd-architecture-and-lifecycle.md).

**Storage backends — implemented and tested:**
- Local POSIX filesystem (bare paths or `file://` URIs) — fully implemented, default on a laptop.
- AWS S3 (`s3://` URIs) — Python control plane via `boto3`, Spark data plane via Spark's native S3 / EMRFS; unit-tested with an in-process S3 fake.

**Storage backends — not yet implemented (roadmap):**
- GCS (`gs://`), ADLS Gen2 (`abfss://`), Azure Blob (`wasbs://`), Databricks DBFS (`dbfs://`), HDFS (`hdfs://`) — these schemes are hard-rejected with a clear error. Adding them requires per-scheme branches in `src/elt_pipeline/shared/path_utils.py`, Spark Hadoop FS credential wiring, and emulator-backed integration tests.

**Ingest mechanisms — honest v1 surface (framework abstractions vs. concrete implementations):**

The platform defines four first-class connector *families* (`rest`, `sql`, `kafka`, `object_storage`) as shared abstractions — each with a validated lifecycle (config → secrets → client → extract → persist → audit → checkpoint). Their concrete v1 implementations vary by readiness:

- **REST — Production-usable.** Real `urllib.request`-based connector with authentication (basic, API key, static bearer, client-credential token flows), request templating, date-window tokenization, page/offset pagination, envelope+inner-payload extraction, retry/backoff/timeout controls.
- **Object storage — Production-usable (local + S3 only).** Source discovery and read via `path_utils` scheme dispatch across local POSIX dirs and `s3://` buckets. GCS/ADLS object-storage sources are roadmap and tied to the multi-cloud storage B-* items.
- **SQL — Demo-only: SQLite replay.** `SqlConnectionDriver` enum = `{sqlite}` only. Uses Python `sqlite3` against a local DB file for the bundled example. There is **no JDBC** and **no Postgres/MySQL/MSSQL/Oracle source extraction** in v1. The `sql.py` connector base class is abstract and JDBC-capable extraction is roadmap (a well-scoped add behind the existing seam).
- **Kafka — Demo-only: local JSONL file replay.** The `KafkaConnectorBase` abstraction is broker-shaped and in place (offsets, partitions, headers, checkpoints, run loop). The **only** concrete subclass reads a local JSONL event log for the bundled example. A real broker consumer over `confluent-kafka`/`kafka-python` with `bootstrap.servers` config is roadmap. Enterprise deployments normally land streams to object storage via Kafka Connect/Firehose/Event Hubs Capture and use the `object_storage` connector to pick them up, so a rock-solid multi-cloud object-storage path is the higher-value ingress work.

**Ingest roadmap (not in v1, tracked for later tranches):**
- Multi-DB SQL ingest via JDBC or a Python driver matrix (Postgres, MySQL, MSSQL, Oracle, …)
- Real Kafka broker consumer (basic offset-based streaming)
- GCS / ADLS object-storage source read (tied to storage B-* multi-cloud work)

**Serving / catalogs — implemented:**
- Iceberg L3/L4 tables with a 6-way catalog enum: `hadoop`, `jdbc`, `rest`, `nessie`, `hive_metastore`, `glue`.
- Trino 468 JDBC serving endpoint (first-class spoke; SQLite-backed metastore default for workstation).
- Airflow reference orchestration wrapper; OpenLineage-compatible lineage adapter; row-count DQ adapter.

**Operational / platinum-hardening items:**
Iceberg table maintenance (compaction / snapshot expiry / orphan cleanup / manifest rewrite) is **Production** (via `elt maintain run …`; see [Capability Maturity Matrix §5](docs/CAPABILITY_MATURITY_MATRIX.md#L125-L141)).
Observability (Prometheus metrics, OTLP tracing, webhook alerting) is **Production** via env-driven backends behind the `ObservabilityAdapter` seam; see [Capability Maturity Matrix §6](docs/CAPABILITY_MATURITY_MATRIX.md#L147-L167).  Remaining roadmap items (not blocking publication): a real secrets backend (Vault / cloud SM), PII classification and masking, a deeper DQ library with quarantine/DLQ, and container / Helm deployment artifacts — all additive behind existing seams, tracked in the Roadmap matrix in [PRD 10 §6.3](docs/prd/10-prd-architecture-and-lifecycle.md).

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

The runtime now supports one optional reference lineage backend integration through the existing internal adapter boundary.

- Local `runs/.../lineage.jsonl` artifacts remain authoritative and are always written first.
- Remote emission is optional and is configured entirely through environment variables.
- The current reference backend is `openlineage_http`, which can target Marquez or another OpenLineage-compatible HTTP endpoint.
- Remote emission failures are recorded in local `logs.jsonl` and `errors.jsonl`; use `ELT_PIPELINE_LINEAGE_POLICY=blocking` only when a remote backend must fail the stage.

Example enablement:

```bash
export ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http
export ELT_PIPELINE_LINEAGE_URL=http://localhost:5000/api/v1/lineage
export ELT_PIPELINE_LINEAGE_POLICY=best_effort
export ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS=10
# Optional when the backend expects an Authorization header.
export ELT_PIPELINE_LINEAGE_AUTH_HEADER="Bearer <token>"
```

Supported variables:

- `ELT_PIPELINE_LINEAGE_BACKEND`: set to `openlineage_http` to enable remote emission
- `ELT_PIPELINE_LINEAGE_URL`: full `http` or `https` endpoint URL for OpenLineage event submission
- `ELT_PIPELINE_LINEAGE_POLICY`: `best_effort` or `blocking`
- `ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS`: positive request timeout in seconds
- `ELT_PIPELINE_LINEAGE_AUTH_HEADER`: optional `Authorization` header value sent with requests

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

The runtime now includes one optional reference data-quality integration for normalization and SQL outputs.

- The current reference backend is `row_count_threshold`.
- Quality hooks run only after `normalize run` and `sql run`; publish-stage quality is still out of scope unless a later PRD extends it.
- Local stage artifacts remain authoritative whether the quality backend is enabled or disabled.
- Quality outcomes are recorded in stage audit `validation_results`, structured `logs.jsonl`, and stage metrics such as `quality.pass`, `quality.warn`, `quality.fail`, and `quality.skipped`.

Example enablement:

```bash
export ELT_PIPELINE_QUALITY_BACKEND=row_count_threshold
export ELT_PIPELINE_QUALITY_ROW_COUNT_MIN=1
export ELT_PIPELINE_QUALITY_POLICY=best_effort
export ELT_PIPELINE_QUALITY_STAGES=normalize,sql
```

Supported variables:

- `ELT_PIPELINE_QUALITY_BACKEND`: set to `row_count_threshold` to enable the reference backend
- `ELT_PIPELINE_QUALITY_ROW_COUNT_MIN`: minimum allowed row count for each evaluated output dataset
- `ELT_PIPELINE_QUALITY_POLICY`: `best_effort` or `blocking`
- `ELT_PIPELINE_QUALITY_STAGES`: comma-separated subset of `normalize` and `sql`

All `ELT_PIPELINE_QUALITY_*` values are trimmed before validation.
`ELT_PIPELINE_QUALITY_BACKEND`, `ELT_PIPELINE_QUALITY_POLICY`, and
`ELT_PIPELINE_QUALITY_STAGES` are accepted case-insensitively, but the
normalized lowercase values shown above remain the recommended form in scripts
and documentation.

Disablement and failure behavior:

- Leave `ELT_PIPELINE_QUALITY_BACKEND` unset to disable the integration entirely.
- Use `best_effort` when quality evidence should be captured without failing an otherwise successful stage.
- Use `blocking` when any failed quality result must fail the stage with `QUALITY_CHECK_FAILED`.
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

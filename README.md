# elt_pipeline

Client-neutral, configuration-driven runtime for a governed data platform.

`elt_pipeline` is not only an ingestion and transformation tool. It is a governed data platform runtime for moving data through explicit architectural levels with strong auditability, lineage, metadata discipline, replayability, and access-control boundaries.

The platform is designed to align with DAMA-DMBOK v2 principles for:

- data architecture
- data integration and interoperability
- metadata management
- data quality
- governance and security
- operational auditability

The repository does not claim that DAMA-DMBOK v2 prescribes the exact `level1` through `level5` naming used here. Instead, those levels are the platform's chosen architecture model for operationalizing DMBOK-aligned concerns in a concrete implementation.

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

At the current repository state, implementation includes the first local `level5` publish/export slice described in `docs/prd/06-prd-level4-to-level5-publish-and-export.md`.

## Client Neutrality

This repository must remain client-neutral.

- do not include client names, vendor names, or legacy repository names
- do not copy code or configuration verbatim from any archived codebase
- use legacy implementations only as an internal baseline to derive a generic superset of ingestion patterns

## Docs

PRDs live in `docs/prd/` and define the target-state architecture.

The implementation continuity backlog lives in `docs/todo/IMPLEMENTATION_BACKLOG.md`.

Recommended starting points:

- `docs/prd/00-prd-platform-principles.md`: product positioning and DAMA-DMBOK v2 alignment
- `docs/prd/00-prd-architecture-levels-and-governance.md`: level model and governance boundaries
- `docs/prd/06-prd-level4-to-level5-publish-and-export.md`: approved `level5` publish/export contract

## Install

This project uses `uv` for environment management.

```bash
uv sync --extra dev
```

Run the test suite:

```bash
uv run pytest
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

Compile and execute SQL models:

```bash
uv run elt-pipeline sql compile examples/sql/local_demo --environment default --start-date 2026-01-01 --end-date 2026-01-31
uv run elt-pipeline sql run examples/sql/local_demo --database path/to/warehouse.db --include-deps --start-date 2026-01-01 --end-date 2026-01-31
uv run elt-pipeline sql run examples/sql/local_demo --database path/to/warehouse.db --validate-only --stage level3
uv run elt-pipeline sql run examples/sql/local_demo --database path/to/warehouse.db --explain --stage level4
```

Validate, explain, and run `level5` publish definitions:

```bash
uv run elt-pipeline publish validate path/to/publish_defs
uv run elt-pipeline publish explain path/to/publish_defs --root-path path/to/runtime
uv run elt-pipeline publish run path/to/publish_defs --root-path path/to/runtime --database path/to/warehouse.db
```

Run a deterministic local schedule plan:

```bash
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml
```

## Local Workflow

The local runtime is organized as a staged filesystem workflow:

1. `ingest run` writes durable source-aligned artifacts into `level1/`
2. `normalize run` turns `level1` manifests into source-aligned `level2/` tables or records a bypass
3. `sql compile` resolves tokens and validates model selection
4. `sql run` materializes downstream `level3/` and `level4/` models
5. `publish run` exports approved `level4` datasets into run-scoped `level5` delivery artifacts
6. `schedule run` orchestrates the above commands in a deterministic local sequence

This reflects the currently implemented runtime path through `level5`. The first publish implementation slice supports local file-based delivery with run-scoped manifests, publish definition discovery/validation, explain-mode, and CSV execution against sqlite-backed `level4` tables. The approved contract still reserves `jsonl`, broader replacement-mode enforcement, and additional delivery patterns for follow-on work.

Runtime metadata is persisted under the selected root path, including:

- `level1/`: raw landed artifacts and manifests
- `level2/`: normalized output tables and mapping catalogs
- `runs/`: audit, structured logs, lineage, and stage-scoped rerun artifacts
- `state/`: local checkpoint history

## Runnable Examples

The repository now includes runnable local connector configs under `examples/configs/`:

- `local_object_storage_orders.yaml`: JSON object storage ingest plus normalization
- `local_object_storage_orders_csv_bypass.yaml`: CSV ingest with `bypass_level2`
- `local_sqlite_orders_delta.yaml`: sqlite delta ingest after seeding `examples/data/sql/source.db`
- `local_kafka_orders_replay.yaml`: Kafka replay ingest from `examples/data/kafka/orders-events.jsonl`
- `local_rest_orders.yaml`: REST ingest against a local static HTTP endpoint served from `examples/data/rest_api/`

It also includes a runnable publish package under `examples/publish/local_demo/` for local `level4 -> level5` CSV export workflows against the bundled SQL demo warehouse.

See `examples/README.md` for setup commands and stage-by-stage usage. See `docs/operator/LOCAL_OPERATOR_RUNBOOK.md` and `docs/operator/TROUBLESHOOTING.md` for reruns, backfills, schedule execution, and artifact inspection guidance.

Maintainers should also use `docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md` for the local quality gates, smoke checks, packaging steps, and CI expectations.

## End-to-End Local Demo

The repository includes an example SQL package under `examples/sql/local_demo/` and a matching publish package under `examples/publish/local_demo/`. A typical local workflow looks like this:

1. Create or point to a pipeline YAML file for a local source.
2. Run `ingest run` into a writable runtime root.
3. Run `normalize run` against the same runtime root.
4. Load or expose `level2` outputs to a local sqlite database when preparing SQL-stage inputs.
5. Run `sql compile` or `sql run` against `examples/sql/local_demo/`.
6. Run `publish validate`, `publish explain`, or `publish run` against `examples/publish/local_demo/`.

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
  --database path/to/warehouse.db \
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
  --database path/to/warehouse.db \
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

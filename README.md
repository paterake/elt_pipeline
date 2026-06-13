# elt_pipeline

Shared runtime for a configuration-driven ELT pipeline.

## Client Neutrality

This repository must remain client-neutral.

- do not include client names, vendor names, or legacy repository names
- do not copy code or configuration verbatim from any archived codebase
- use legacy implementations only as an internal baseline to derive a generic superset of ingestion patterns

## Docs

PRDs live in `docs/prd/` and define the target-state architecture.

The implementation continuity backlog lives in `docs/todo/IMPLEMENTATION_BACKLOG.md`.

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
- `examples/sql/local_demo/`: example SQL model package for local execution
- `examples/schedules/local_demo.yaml`: example schedule plan wiring the CLI stages together
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
5. `schedule run` orchestrates the above commands in a deterministic local sequence

Runtime metadata is persisted under the selected root path, including:

- `level1/`: raw landed artifacts and manifests
- `level2/`: normalized output tables and mapping catalogs
- `runs/`: audit, structured logs, lineage, and stage-scoped rerun artifacts
- `state/`: local checkpoint history

## End-to-End Local Demo

The repository includes an example SQL package under `examples/sql/local_demo/`. A typical local workflow looks like this:

1. Create or point to a pipeline YAML file for a local source.
2. Run `ingest run` into a writable runtime root.
3. Run `normalize run` against the same runtime root.
4. Load or expose `level2` outputs to a local sqlite database when preparing SQL-stage inputs.
5. Run `sql compile` or `sql run` against `examples/sql/local_demo/`.

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

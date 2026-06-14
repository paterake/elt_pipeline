# Local Operator Runbook

This runbook covers the local v1 operator flows for on-demand execution, targeted reruns, backfills, and deterministic schedule execution.

## Prerequisites

- Run commands from the repository root.
- Install dependencies with `uv sync --extra dev`.
- Use a writable runtime root such as `.tmp/runtime-demo`.
- Validate configs before running jobs.

```bash
uv run elt-pipeline validate-config examples/configs/local_object_storage_orders.yaml
```

## Standard Stage Runs

Run ingest:

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-demo
```

Run normalization against the same root:

```bash
uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-demo
```

Compile or run downstream SQL models:

```bash
uv run elt-pipeline sql compile examples/sql/local_demo \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

uv run elt-pipeline sql run examples/sql/local_demo \
  --database .tmp/warehouse.db \
  --environment default \
  --include-deps \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Validate, explain, or run downstream `level5` publish definitions:

```bash
uv run elt-pipeline publish validate examples/publish/local_demo

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --environment default \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --database .tmp/warehouse.db \
  --environment default \
  --publish daily_order_export \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --database .tmp/warehouse.db \
  --environment default \
  --publish daily_order_export_tsv \
  --window-label 2026-01

uv run elt-pipeline publish explain examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --database .tmp/warehouse.db \
  --environment default \
  --publish daily_order_export_bundle \
  --window-label 2026-01

uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --database .tmp/warehouse.db \
  --window-start 2026-01-01T00:00:00+00:00 \
  --window-end 2026-01-31T23:59:59+00:00 \
  --window-label jan-2026 \
  --backfill
```

## Backfill Runs

Use backfill mode when you want a historical ingest or normalization run to seed checkpoint state from prior history.

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-demo \
  --window-start 2026-01-01T00:00:00+00:00 \
  --window-end 2026-01-31T23:59:59+00:00 \
  --window-label jan-2026 \
  --backfill
```

Notes:

- `--backfill` requires `--window-start`.
- Use the same runtime root so prior checkpoints and artifacts remain discoverable.
- Pair the same window selection with `normalize run` when replaying historical landed data.

## Targeted Normalize Reruns

Normalize reruns reuse the exact input manifest captured by a prior normalize run.

1. Find the prior normalize `run_id` under `runs/stage=normalize/`.
2. Re-run with `--rerun-run-id`.

```bash
uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-demo \
  --rerun-run-id <prior-normalize-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--source`, `--entity`, `--manifest-path`, or window filters.
- The CLI resolves the original manifest path from the stored audit artifact.

## Targeted SQL Reruns

SQL reruns restore the prior model and window selection from SQL audit artifacts.

1. Find the prior SQL `run_id` under `runs/stage=sql/`.
2. Re-run with the same package path and database path.

```bash
uv run elt-pipeline sql run examples/sql/local_demo \
  --database .tmp/warehouse.db \
  --rerun-run-id <prior-sql-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--stage`, `--domain`, `--model`, `--include-deps`, date filters, partitions, or vars.
- Use `--validate-only` or `--explain` first if you want to confirm the plan before writing tables.

## Targeted Publish Reruns

Publish reruns restore the prior publish selection and execution window from publish audit artifacts.

1. Find the prior publish `run_id` under `runs/stage=publish/`.
2. Re-run with the same runtime root, publish package, and database path.

```bash
uv run elt-pipeline publish run examples/publish/local_demo \
  --root-path .tmp/runtime-demo \
  --database .tmp/warehouse.db \
  --rerun-run-id <prior-publish-run-id>
```

Notes:

- Do not combine `--rerun-run-id` with `--domain`, `--publish`, window filters, `--backfill`, or a non-default `--environment`.
- The CLI restores the original publish IDs, environment, and window selection from the stored publish audit record.
- The rerun manifest and audit artifacts record `rerun_of_run_id` so operators can trace replayed deliveries.

## Schedule-Driven Execution

Schedule plans call existing CLI commands in a fixed order.

```bash
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml
uv run elt-pipeline schedule run examples/schedules/local_demo.yaml --continue-on-error
```

Operator guidance:

- Keep paths in the schedule plan aligned with the config, SQL package, and runtime root you intend to use.
- Use the plan-level `continue_on_error` setting for deterministic defaults.
- Use the CLI flag `--continue-on-error` only when you intentionally want to override the plan.

## Optional Lineage Backend Operations

Optional remote lineage emission is available without changing the CLI contract for ingest, normalize, SQL, publish, or schedule-driven runs.

```bash
export ELT_PIPELINE_LINEAGE_BACKEND=openlineage_http
export ELT_PIPELINE_LINEAGE_URL=http://localhost:5000/api/v1/lineage
export ELT_PIPELINE_LINEAGE_POLICY=best_effort
export ELT_PIPELINE_LINEAGE_TIMEOUT_SECONDS=10
```

Optional authentication:

```bash
export ELT_PIPELINE_LINEAGE_AUTH_HEADER="Bearer <token>"
```

Operator guidance:

- Keep local `runs/.../lineage.jsonl` artifacts as the system of record even when remote emission is enabled.
- Use `best_effort` for normal local-first operation; remote failures are then captured locally in `logs.jsonl` and `errors.jsonl` without changing stage success.
- Use `blocking` only when an environment explicitly requires remote lineage submission to succeed.
- Set `ELT_PIPELINE_LINEAGE_URL` to the full OpenLineage-compatible HTTP endpoint, such as Marquez at `http://localhost:5000/api/v1/lineage`.
- Validate connectivity with a small CLI run first; if the backend is unavailable, the stage audit still lets operators confirm whether the core stage itself succeeded.

## Optional Airflow Wrapper Operations

The repository now includes one reference Airflow wrapper that preserves the authoritative CLI contract.

- Reference helper: `elt_pipeline.integrations.AirflowCliWrapper`
- Reference example DAG: `examples/orchestration/airflow/reference_dag.py`
- Execution model: Airflow calls `python -m elt_pipeline ...` through the wrapper; it does not replace the platform CLI with an Airflow-native runtime API.

Operator guidance:

- Keep local `runs/.../audit.json`, `logs.jsonl`, and `lineage.jsonl` as the system of record even when runs are launched from Airflow.
- Pass the same `--root-path`, package paths, selection flags, and window arguments you would use in a direct CLI run; the wrapper is intentionally thin.
- Expect supplemental audit attributes such as `orchestration_platform`, `orchestration_flow_name`, `orchestration_flow_run_id`, and `orchestration_task_name` when the wrapper injects Airflow context.
- Disable the integration by running the CLI directly or by removing the wrapper from the Airflow DAG; no platform config change is required.
- If an Airflow task fails, inspect the local run artifacts first to determine whether the core stage failed or whether the failure happened at the wrapper/orchestrator layer.

## Publish Operator Guidance

Use `publish validate` when reviewing new or changed publish packages before touching runtime outputs.

- It confirms discovery, manifest schema, directory naming, and query-file presence.
- It is safe to run in CI or before a release because it does not read the warehouse or write artifacts.

Use `publish explain` when you want a dry preview of the paths a publish run would target.

- Pass the same `--root-path`, selection filters, and optional `--window-label` that you expect to use in `publish run`.
- Review `run_scoped_path` for the immutable history location and `stable_delivery_path` when the publish definition uses `overwrite_in_place` or `append_new_artifact`.
- Review `archive_run_scoped_path` and `archive_stable_delivery_path` when a publish definition declares `delivery.packaging.archive_format: zip`.

Use `publish run` only after the upstream `level4` table already exists in the target sqlite database.

- The current implementation supports CSV, `jsonl`, and `tsv` outputs.
- The current implementation supports `versioned_delivery`, `overwrite_in_place`, and `append_new_artifact`.
- A publish definition may also request a zip archive bundle in addition to the primary file output.
- `append_new_artifact` writes the immutable run-scoped artifact and also copies a uniquely named delivery file into the consumer-facing artifact path without mutating prior deliveries.
- The bundled `daily_order_export_tsv` example demonstrates `tsv` plus `append_new_artifact`; the stable delivery filename includes the originating `run_id`.
- The bundled `daily_order_export_bundle` example demonstrates a CSV delivery with an additional stable `.zip` bundle produced from the same run-scoped output.
- Use `--backfill` with an explicit publish window when replaying a historical delivery slice; it records the run as a backfill in stage audit artifacts.
- A successful run writes the exported file and `manifest.json` under `artifacts/level5/`, and writes stage audit/log/lineage records under `runs/stage=publish/`.
- Reuse the same runtime root for repeatable operator workflows so historical run artifacts remain available for inspection.

## Audit And State Locations

A local runtime root persists these operator-visible directories:

- `level1/`: landed payloads and manifest metadata
- `level2/`: normalized outputs and mapping catalogs
- `artifacts/level5/`: publish/export delivery artifacts and run-scoped manifests
- `runs/`: stage-scoped audit, logs, lineage, and rerun metadata
- `state/`: checkpoint history used for incremental runs and backfills

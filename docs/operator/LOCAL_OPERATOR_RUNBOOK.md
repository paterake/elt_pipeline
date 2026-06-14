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

## Publish Operator Guidance

Use `publish validate` when reviewing new or changed publish packages before touching runtime outputs.

- It confirms discovery, manifest schema, directory naming, and query-file presence.
- It is safe to run in CI or before a release because it does not read the warehouse or write artifacts.

Use `publish explain` when you want a dry preview of the paths a publish run would target.

- Pass the same `--root-path`, selection filters, and optional `--window-label` that you expect to use in `publish run`.
- Review `run_scoped_path` for the immutable history location and `stable_delivery_path` when the publish definition uses `overwrite_in_place` or `append_new_artifact`.

Use `publish run` only after the upstream `level4` table already exists in the target sqlite database.

- The current implementation supports CSV and `jsonl` outputs.
- The current implementation supports `versioned_delivery`, `overwrite_in_place`, and `append_new_artifact`.
- `append_new_artifact` writes the immutable run-scoped artifact and also copies a uniquely named delivery file into the consumer-facing artifact path without mutating prior deliveries.
- A successful run writes the exported file and `manifest.json` under `artifacts/level5/`, and writes stage audit/log/lineage records under `runs/stage=publish/`.
- Reuse the same runtime root for repeatable operator workflows so historical run artifacts remain available for inspection.

## Audit And State Locations

A local runtime root persists these operator-visible directories:

- `level1/`: landed payloads and manifest metadata
- `level2/`: normalized outputs and mapping catalogs
- `artifacts/level5/`: publish/export delivery artifacts and run-scoped manifests
- `runs/`: stage-scoped audit, logs, lineage, and rerun metadata
- `state/`: checkpoint history used for incremental runs and backfills

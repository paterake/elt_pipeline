# Examples

Run all example commands from the repository root so relative paths resolve correctly.

## Example Configs

- `examples/configs/local_object_storage_orders.yaml`: object storage ingest using bundled JSON sample data
- `examples/configs/local_object_storage_orders_csv_bypass.yaml`: object storage ingest with CSV payloads and `bypass_level2`
- `examples/configs/local_sqlite_orders_delta.yaml`: sqlite delta ingest after seeding `examples/data/sql/source.db`
- `examples/configs/local_kafka_orders_replay.yaml`: Kafka replay ingest from a bundled JSONL log
- `examples/configs/local_rest_orders.yaml`: REST ingest against a local static HTTP endpoint
- `examples/publish/local_demo/`: runnable `level5` publish definitions for CSV, `jsonl`, `tsv`, and zip-bundled local exports
- `examples/sql/local_demo/`: local SQL model package that prepares example `level4` tables for publish runs

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
rm -f examples/data/sql/source.db
sqlite3 examples/data/sql/source.db < examples/data/sql/source_init.sql
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

The downstream SQL model package lives under `examples/sql/local_demo/`. Its `level3.sales.base_orders` model declares a `sources` entry pointing at the `orders` table produced by the Object Storage JSON example (`source_name: local_files`, `entity_name: orders`), so run that ingest/normalize pair first to produce real `level2` data before running the SQL package.

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

# Examples

Run all example commands from the repository root so relative paths resolve correctly.

## Example Configs

- `examples/configs/local_object_storage_orders.yaml`: object storage ingest using bundled JSON sample data
- `examples/configs/local_object_storage_orders_csv_bypass.yaml`: object storage ingest with CSV payloads and `bypass_level2`
- `examples/configs/local_sqlite_orders_delta.yaml`: sqlite delta ingest after seeding `examples/data/sql/source.db`
- `examples/configs/local_kafka_orders_replay.yaml`: Kafka replay ingest from a bundled JSONL log
- `examples/configs/local_rest_orders.yaml`: REST ingest against a local static HTTP endpoint

## Object Storage JSON

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-object-storage

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders.yaml \
  --root-path .tmp/runtime-object-storage
```

## Object Storage CSV With Level2 Bypass

```bash
uv run elt-pipeline ingest run examples/configs/local_object_storage_orders_csv_bypass.yaml \
  --root-path .tmp/runtime-object-storage-csv

uv run elt-pipeline normalize run examples/configs/local_object_storage_orders_csv_bypass.yaml \
  --root-path .tmp/runtime-object-storage-csv
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
  --root-path .tmp/runtime-sql
```

## Kafka Replay Ingest

```bash
uv run elt-pipeline ingest run examples/configs/local_kafka_orders_replay.yaml \
  --root-path .tmp/runtime-kafka
```

## REST Ingest

Start a local static server in one terminal:

```bash
python3 -m http.server 8000 --directory examples/data/rest_api
```

Run the connector in another terminal:

```bash
uv run elt-pipeline ingest run examples/configs/local_rest_orders.yaml \
  --root-path .tmp/runtime-rest
```

## SQL Models

The downstream SQL model package remains under `examples/sql/local_demo/`. Use it after preparing a local sqlite warehouse for `sql compile` or `sql run` commands.

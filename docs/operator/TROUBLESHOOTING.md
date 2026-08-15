# Troubleshooting

Use this guide when local runs fail or when outputs do not appear where expected.

## First Checks

- Confirm you are running commands from the repository root when using bundled example configs.
- Re-run `validate-config` against the same YAML before investigating runtime behavior.
- Inspect the most recent audit artifact under `runs/stage=<stage>/.../run_id=<run_id>/audit.json`.
- Inspect stage logs and error artifacts in the same run directory.

## Config And Selection Failures

### `Configuration file does not exist`

- Confirm the YAML path passed to the CLI is correct.
- Prefer repository-relative paths such as `examples/configs/local_object_storage_orders.yaml`.

### `Unknown environment overlay`

- Use an environment defined under `environments:` in the config.
- The bundled examples use `default`.

### `No level1 manifests matched the requested selection`

- Confirm ingest completed successfully under the same `--root-path`.
- Check that `--source`, `--entity`, and window filters match the landed manifests.
- Remove overly narrow filters and retry.

### `No prior run artifacts matched --rerun-run-id`

- Verify the `run_id` exists under the matching stage in `runs/`.
- Reuse the same runtime root that contains the original audit artifacts.
- Do not point reruns at a different root path from the original execution history.

## Local Connector Issues

### `object_storage bucket_path does not exist`

- Ensure the configured directory exists.
- For bundled examples, keep the working directory at the repo root so `examples/data/object_storage` resolves correctly.

### `Local Kafka connector requires an existing log_path`

- Confirm the configured JSONL file exists.
- If you override with `--kafka-log-path`, ensure the file is readable and each line is valid JSON.

### `Local SQL connector only supports sqlite for v1`

- Use a sqlite database file in the SQL connector example.
- Seed the example database with:

```bash
rm -f .ignore/example-source.db
sqlite3 .ignore/example-source.db < examples/data/sql/source_init.sql
```

### REST connector failures against the local example

- Start the local HTTP server before running ingest:

```bash
python3 -m http.server 8000 --directory examples/data/rest_api
```

- Confirm the config still points at `http://127.0.0.1:8000`.
- Open `http://127.0.0.1:8000/v1/orders.json` in a browser to confirm the payload is reachable.

## Missing Outputs

### No new `level2/` tables after normalization

- Check whether the entity is configured with `level2_mode: bypass_level2`.
- For bypass runs, confirm the normalize audit includes `"bypassed": "true"`.
- For tabular CSV payloads, verify the input file has a header row.

### SQL run succeeded but tables are missing or empty

- Use `sql compile` or `sql run --validate-only` to confirm model selection and token values.
- Check the target warehouse root passed with `--warehouse-root`; `level3`/`level4` tables land under `<warehouse-root>/level3/<table_name>/` and `<warehouse-root>/level4/<table_name>/`.
- Verify `--root-path` points at the same runtime root that `normalize run` wrote `level2/` under.

### `SQL_LEVEL2_SOURCE_NOT_FOUND`

- A `level3` model's `sources` entry (`source_name`/`entity_name`/optional `table_name`) does not match any data under `<root-path>/level2/environment=<env>/source=<source_name>/entity=<entity_name>/`.
- Confirm `normalize run` completed successfully for that source/entity against the same `--root-path` passed to `sql run`.
- Confirm the `sources.table_name` (or `logical_name` when `table_name` is omitted) matches the physical table name normalize produced, visible in the normalize run's `table_manifests[].table_name`.

### `SQL_DEPENDENCY_NOT_MATERIALIZED`

- A model's `depends_on` entry was not included in the current run's model selection, so its output could not be read back from the warehouse.
- Rerun with `--include-deps`, or explicitly select the dependency model as well.

### Spark fails to start / `JAVA_HOME` errors

- `normalize`, `sql`, and `publish` require a local JVM. Install Java 17+ and set `JAVA_HOME`.
- Confirm `pyspark` is installed: `uv sync --extra dev --extra spark`.
- Override the Spark master with `ELT_PIPELINE_SPARK_MASTER` if `local[*]` is unsuitable for the environment.

### Driver out-of-memory during `normalize run` or `publish run`

- `publish run` still collects the full result set to the driver to write one file
  per publish definition; narrow the publish selection/window so fewer rows are
  collected.
- `normalize run` driver OOM depends on which engine is in use:
  - **`normalize_engine = "python"` (current CLI default):** the relationalizer
    flattens each nested payload in the driver process. Reduce per-artifact
    payload size at ingest so normalize processes smaller units.
  - **`normalize_engine = "spark"` (native-Spark alternative):** relationalizer
    work is offloaded to Spark executors; driver holds only schema + plan
    metadata (KB-scale). Switching to the Spark engine is the recommended fix
    for normalize driver OOM on large or deeply nested payloads. See
    `LOCAL_OPERATOR_RUNBOOK.md` → **Normalize Engine Selection** for the
    programmatic access pattern.
- Raising driver memory (Spark driver-memory settings, or `ELT_PIPELINE_SPARK_MASTER`
  with a suitable local config) helps only up to the single-node limit; genuinely
  large workloads need the `"spark"` normalize engine or ingest-side payload
  splitting, not a config tweak.

### SQL overwrite staging-swap failures

When a SQL model runs under `full_refresh` or `partition_overwrite`, the
execution now follows the **Mercell/Camelot staging-swap protocol** (see
`LOCAL_OPERATOR_RUNBOOK.md` → **SQL Overwrite Protocol**). Three new runtime
error codes can appear in the SQL audit `errors.jsonl` or in CLI output:

| Error code | Root cause | Operator action |
|---|---|---|
| `staging_scheme_unsupported` | The `--warehouse-root` uses a scheme outside the supported set `{file, local_unschemed, s3}`. The swap layer has no known-atomic semantics for any other scheme (e.g. `s3a`, `hdfs`, `gs`, `abfss`). | Point the warehouse root at a supported scheme; or switch the model to `load_mode: append` (which bypasses the swap layer). For new scheme support, open a PRD review against `_StorageScheme` in `shared/path_utils.py`. |
| `staging_write_failed` | The `writer.parquet(staging_path)` call returned but the resulting staging parquet could not be read back for the mandatory `validate_stage` step. Typically indicates a partial/corrupt staging write or executor loss during the write job. | The error context records `staging_path`; inspect that path for 0-byte or missing part files. The swap layer best-effort deletes staging on this failure, but on S3 the delete batch can silently miss keys — manually remove the staging run directory before retrying. No canonical `target_path` state was touched by this run. |
| `atomic_swap_failed` | The scheme-dispatched swap step (`rename` on POSIX, `CopyObject` → `DeleteObjects` on S3) raised mid-operation. On POSIX this leaves either all-old or all-new state (rename is atomic on a single FS). On S3 this can leave a mix of old target keys already deleted + new keys already copied — the error context preserves **both** `staging_path` and `target_path` for recovery. | Inspect `target_path` partition directories for partial state. If the S3 copy batch completed but the old-key delete batch failed, readers already see the new post-swap data under `target_path`; the failed delete batch only leaks old orphan part files, which are invisible to consumers with a predicate on `_row_id`/business date. Re-run the model; a second overwrite will overwrite the partial state cleanly. On S3, verify the EMR execution role has `s3:DeleteObject`, `s3:PutObject`, and `s3:ListBucket` scoped to **both** the runtime root **and** the warehouse root buckets — a missing `DeleteObject` grant on the warehouse root is the single most common root cause of `atomic_swap_failed` in the S3 path. |

### `level3/level4` tables are unchanged after a successful SQL run

- Check whether the model uses `load_mode: partition_overwrite` combined with a
  narrow `--start-date` / `--end-date` window that did not touch the partition
  you are inspecting. Dynamic partition overwrite (preserved through the
  staging-swap layer) only replaces partitions present in the incoming
  DataFrame; other partitions survive untouched. Replay a wider ingest_date
  window using the Late-Arriving Data Recovery procedure in the runbook.
- Confirm the SQL audit row's `load_mode` and `row_count` match expectations.
  `row_count = 0` means the model wrote an empty incoming DataFrame — the
  swap step then replaces (for `full_refresh`) or deletes-only-touched-partitions
  (for `partition_overwrite`) with empty content, which can appear as "tables
  unchanged" if you were looking for old data.

## Useful Inspection Commands

Pretty-print an audit artifact:

```bash
python3 -m json.tool .ignore/runtime-demo/runs/stage=ingest/environment=default/job=ingest-run/run_id=<run_id>/audit.json
```

List recent run artifacts:

```bash
find .ignore/runtime-demo/runs -maxdepth 5 -type f | sort
```

List checkpoint files:

```bash
find .ignore/runtime-demo/state -type f | sort
```

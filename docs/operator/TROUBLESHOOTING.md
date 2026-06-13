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
rm -f examples/data/sql/source.db
sqlite3 examples/data/sql/source.db < examples/data/sql/source_init.sql
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
- Check the target sqlite database path passed with `--database`.
- Verify the upstream inputs expected by the SQL models were loaded into that sqlite database.

## Useful Inspection Commands

Pretty-print an audit artifact:

```bash
python3 -m json.tool .tmp/runtime-demo/runs/stage=ingest/environment=default/job=ingest-run/run_id=<run_id>/audit.json
```

List recent run artifacts:

```bash
find .tmp/runtime-demo/runs -maxdepth 5 -type f | sort
```

List checkpoint files:

```bash
find .tmp/runtime-demo/state -type f | sort
```

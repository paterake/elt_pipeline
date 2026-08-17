# Maintainer Workflow

This repository uses `uv` for local development, test execution, and packaging.

## Local Development

`level2` through `level5` execute on Apache Spark (`pyspark`), which requires a local JVM (**Java 23 Temurin**, required by the Trino 468 serving engine) with `JAVA_HOME` set. The `dev` extra already pulls in `pyspark`; only the JVM itself is an external prerequisite. To provision that JVM cleanly (without polluting the OS), see [JVM_TOOLCHAIN_SETUP.md](JVM_TOOLCHAIN_SETUP.md).

Sync the locked development environment from the repository root:

```bash
uv sync --extra dev
```

Run the local quality gates before opening or updating a pull request:

```bash
uv run ruff check .
uv run pytest
```

Use the CLI entrypoint in the managed environment when verifying examples or making manual runtime checks:

```bash
uv run elt-pipeline --help
```

## Example Smoke Checks

The repository bundles runnable examples under `examples/`.

The automated smoke coverage validates:

- example pipeline configs still parse and resolve
- local connector happy paths still execute for object storage, SQL, Kafka, and REST
- the bundled SQL package still compiles and runs against a local Spark-backed parquet warehouse
- the example schedule plan still executes after replacing its placeholder paths with real local values

When changing example files or CLI behavior, run:

```bash
uv run pytest tests/test_examples.py
```

## Packaging

Build both the source distribution and wheel from the repository root:

```bash
uv build
```

Artifacts are written to `dist/`.

The CI workflow validates that both a `.whl` and a `.tar.gz` artifact are produced for every change.

## CI

GitHub Actions runs the repository checks defined in `.github/workflows/ci.yml`.

The workflow currently enforces:

- a Temurin JDK 23 install (required for `pyspark` + Trino 468 serving)
- `uv sync --extra dev`
- `uv run ruff check .`
- `uv run pytest`
- `uv build`

Keep local commands aligned with CI so maintainers can reproduce failures quickly.

## Release Notes

Before cutting a release:

1. Confirm the working tree only contains intentional changes.
2. Run the full local quality gates and example smoke checks.
3. Build artifacts with `uv build`.
4. Review `README.md`, `examples/README.md`, and operator docs for any command or workflow changes that need documentation updates.

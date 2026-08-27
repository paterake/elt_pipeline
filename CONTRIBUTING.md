# Contributing to elt_pipeline

This project is a governed, configuration-driven framework with explicit architectural levels and zero-pre-scoped capability expansion rules. This document describes how to work on it locally, how tests are structured, and what a PR needs to pass review.

If you are a new contributor looking to **add a capability that isn't already in the [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md)** (e.g., a new connector family, a new platform level, a new feature), **please open an issue first** describing the concrete use case, the expected 3+ hrs/week of operator toil it removes, and why none of the 6 existing extensibility APIs below can express it. Do NOT open cold PRs against the Strategic Posture (see [BACKLOG.md §Scoping Policy](BACKLOG.md) Active Constraints 10–13). For everything else — bug fixes, docs, example manifests, internal refactors that keep the public API stable — PRs are welcome directly.

## 1. Environment setup (≤15 minutes)

External prerequisites:
- [`uv`](https://github.com/astral-sh/uv) (Python & dependency manager; owns the entire Python side)
- [Temurin 23 JDK](https://adoptium.net/temurin/releases/?version=23) with `JAVA_HOME` exported (Spark + Trino both need it; this is the only external tool not managed by `uv`)

For a clean, zero-os-pollution JVM install via `mise`, see [docs/maintainer/JVM_TOOLCHAIN_SETUP.md](docs/maintainer/JVM_TOOLCHAIN_SETUP.md). 4-core Apple Silicon laptop users also see the troubleshooting row there for `ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g` to avoid the 8-way concurrent maintenance sandbox OOM.

Then, from the repo root:

```bash
# 1. Locked Python env (every extra — spark, dev, kafka, gcs, adls, dataproc, synapse)
uv sync --extra dev --extra spark

# 2. Confirm JVM visibility to Spark
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"   # or your real path
uv run python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[1]').appName('ok').getOrCreate().version)"
```

Expected output: `4.1.2` with no `JAVA_HOME` errors.

## 2. Working locally

Run the CLI entrypoint in the managed venv:

```bash
uv run elt-pipeline --help
uv run elt-pipeline metric compile examples/sql/local_demo --with-sql-refs --format summary
```

For the full local end-to-end demo (Docker or no-Docker), follow the [Quick Start in README.md](README.md#quick-start--two-paths).

### Running tests — the ONE rule that matters (S-0: per-Spark-file isolation)

DO NOT run a bare `uv run pytest` on the whole suite. The reason is structural: one Python process = one JVM = one `SparkSession`. Spark/Iceberg mode is frozen on the first `spark_session` fixture build, and some Spark teardowns call `spark.stop()` — which tears the JVM down for every subsequent test in the same process, producing spurious `JAVA_GATEWAY_EXITED` dead-session failures.

Instead:

```bash
# Development: single test file during active work (pytest per-file is fine)
uv run pytest tests/test_semantic_metrics.py -v

# Before a PR: the OFFICIAL gate (matches CI exactly). Runs non-Spark tests together,
# then every Spark/Iceberg-backed test file in ITS OWN subprocess.
bash scripts/run_tests.sh
```

See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md) for the full explanation. CI runs exactly `bash scripts/run_tests.sh`. If it's green locally it's green in CI.

### Linting / style

```bash
uv run ruff check src/ tests/ examples/
# Optional: auto-fix what can be auto-fixed (sorting imports, line-length-friendly rewrites)
uv run ruff check --fix src/ tests/ examples/
```

Ruff is the only linter/formatter. PRs need 0 ruff errors across the 3 directories.

## 3. Adding code — the pCO pattern (do this, not a gold file)

Every source package follows the "public-contract oriented" (pCO) pattern in this repo. It's simple:

```
src/elt_pipeline/<package>/
  __init__.py          ← THIN FACADE ONLY: alphabetical imports from _* submodules
                         + public `__all__` list. No implementation.
  _models.py           ← Pydantic models / Enums / validation
  _compiler.py         ← Pure compile-time logic (YAML → typed objects, no IO)
  _runtime.py          ← Runtime side-effects: Spark calls, write-audit, HTTP calls
  _discovery.py        ← Filesystem walking / registry bootstrap (if any)
  _<single-concern>.py ← Exactly one intent per file. Never a file with "14 things".
```

**No "gold files" with multiple intents.** If a file's docstring says "models + compiler + runtime", split it. This is the #1 review failure reason for new-contributor PRs (not style, not bugs — package layout clarity).

Concrete reference implementation you can copy-paste the structure from: the [src/elt_pipeline/metrics/](src/elt_pipeline/metrics/) package (GAP-4, 2026-08-27).

## 4. Adding tests

```
tests/
  test_<pCO-package-name>.py      ← 1:1 with a package. Single-file name matches the package.
```

No nested test directories, no per-subpackage test files. This keeps S-0 isolation simple: one Spark-backed file → one subprocess. If you add a new source module that imports Spark, its tests go in the same `tests/test_<existing>.py` as the other Spark tests for that subsystem.

Minimum test counts (enforced by review, not by CI script):
- **Bug fixes:** ≥1 regression test that fails WITHOUT the fix.
- **New feature:** ≥+16 individual tests (not test functions that loop and assert 16 things). This is a deliberate floor, not a target — the semantic metrics layer shipped with +21 against a +16 minimum.
- **Internal refactor:** No new tests required if existing coverage already passes and API is byte-identical (preferred form: add a `test_…_api_byte_identical` comparing before/after JSON payloads for public functions).

## 5. Extending the framework WITHOUT core edits (preferred first step)

The framework has 6 independently-tested Protocol/registry extension points. 9 times out of 10 what you want is an additive plugin, not a core PR. Try these IN ORDER before opening a core code PR:

1. **New connector family (e.g., SFTP, webhook, CDC) →** `from elt_pipeline.ingest import register_connector_factory` — implement the `ConnectorFactory` Protocol at [src/elt_pipeline/ingest/connectors/registry.py L113](src/elt_pipeline/ingest/connectors/registry.py#L113-L132), call `register_connector_factory("myscheme", MyFactory())` once at process startup via your project's entrypoint, and `connector_type: myscheme` YAML just works. No core edits.

2. **New storage backend (e.g., OCI object storage, SMB share) →** `from elt_pipeline.shared.storage_backends import register_backend` — implement `StorageBackend` Protocol at [src/elt_pipeline/shared/storage_backends/_protocol.py L9](src/elt_pipeline/shared/storage_backends/_protocol.py#L9-L23), register once. All `uri://` paths route to your backend transparently. No core edits.

3. **New secrets vault (e.g., 1Password, Infisical) →** `from elt_pipeline.shared.secrets import register_provider` — implement `SecretsProvider` Protocol at [src/elt_pipeline/shared/secrets/_protocol.py L9](src/elt_pipeline/shared/secrets/_protocol.py#L9-L13), register once, `secret_ref://myvault/path/to/key` URIs work everywhere (pipeline configs, manifests, connectors). No core edits.

4. **New data quality engine (e.g., Soda Core, Great Expectations) →** implement `QualityHookBackend` Protocol at [src/elt_pipeline/integrations/quality/_models.py L108](src/elt_pipeline/integrations/quality/_models.py#L108-L111) and register via the existing env-driven hook selector.

5. **New SQL driver (e.g., SAP HANA, Snowflake dialect-specific connection) →** implement `SqlDbDriver` Protocol at [src/elt_pipeline/ingest/connectors/sql.py L350](src/elt_pipeline/ingest/connectors/sql.py#L350-L388) with a lazy-importer wrapper entry in `_build_db_driver()`. Only this one function changes; the rest of the SQL connector path is driver-agnostic.

6. **New metric aggregation type (e.g., P95, count_distinct_approx) →** add a value to `MetricAggregation` Enum at [src/elt_pipeline/metrics/_models.py L13](src/elt_pipeline/metrics/_models.py#L13-L19) + one arm in `_build_aggregation_sql` dispatcher. Purely additive. The CLI parser, compile step, all 3 run modes, audit JSONL, and guardrail work with zero other edits.

## 6. Pull request checklist

Every PR MUST have all `[x]` marks on the template below (GitHub template lives at [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md)):

- [ ] **Ruff 0 errors:** `uv run ruff check src/ tests/ examples/`
- [ ] **Official gate green locally:** `bash scripts/run_tests.sh` (NOT bare pytest). If the full gate takes too long locally, at minimum run the gate-subset for changed files and confirm CI passes once opened.
- [ ] **Tests:** ≥1 test per behavior change. No "only docs" exception for docs that describe new CLI behavior — a structural CLI parse test or compile smoke test is required.
- [ ] **pCO package layout:** New source code follows thin-facade `__init__.py` + underscore-prefixed submodule pattern (§3 above). No gold files.
- [ ] **Docs:** README, [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md), [Industry Gap Analysis](docs/INDUSTRY_GAP_ANALYSIS.md), and [BACKLOG.md](BACKLOG.md) updated ONLY IF this changes a capability. Publishing-hygiene docs (CONTRIBUTING, SECURITY, tutorials) don't require a CMM row.
- [ ] **API compatibility note:** Any Protocol change, `__all__` list edit, Enum value removal, or CLI subcommand flag rename must be labeled `[BREAKING]` in the PR title and explained in the body. Default rule: no breaking changes without explicit signed-off deprecation window ≥2 releases.
- [ ] **No secrets, no PII:** Review diff for `sk-`, `ghp_`, `AKIA` non-example AWS keys, personal emails, internal hostnames. `.gitignore` covers most of this by policy; double-check new example configs.

For capability PRs (new green row in CMM): additionally include the 4-part closure narrative (rationale / 6 Design Decisions D-1… / exact test counts by category / final gate exit numbers) so maintainers can archive it verbatim to [docs/todo/archive/WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md). The GAP-4 entry in that archive is the canonical template.

## 7. Release process (maintainers only)

Tagged releases use `uv build` + PyPI upload with a maintainer-scoped PyPI API token. See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md §Packaging and CI](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md#L61-L82) for the build + artifact-validation steps that run in CI on every push. Releases are annotated tags: `git tag -a v0.5.0 -m "Backlog Empty / Semantic Metrics"`; maintainers then upload manually via `uv publish` using their own PyPI token (no org-wide tokens in this repo).

# elt_pipeline — session entry

Auto-loaded into every session. Keep it tiny; it is a router, not documentation.

## Backlog continuity (capability — no backlog currently active)

No active recovery backlog right now. If a bounded, multi-session recovery/migration effort
is needed, follow **[docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md)**:
create a repo-root `BACKLOG.md` anchor doc from its skeleton, add a tiny router section here,
work one session per item, and delete both when the backlog is exhausted and the gate is green.

## Running tests (required environment + the gate)

Spark/Iceberg tests need Temurin 23 on `PATH`/`JAVA_HOME`; a non-interactive shell does not inherit mise's activation. Export first, then run the gate:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh
```

The gate is **`scripts/run_tests.sh`**, not a bare `uv run pytest`: one JVM holds one `SparkSession`, so each Spark-backed test file must run in its own process (see [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md)). A single per-file run is fine locally (`uv run pytest tests/<file>.py`); the shared `spark_session` fixture defaults to Iceberg off. Without the JDK exports, Spark tests fail with `JAVA_GATEWAY_EXITED` (environment, not a code defect).

## Canonical docs

Architecture/lifecycle source of truth: **[docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md)**.
Per PRD 10 §11, **no backlog/tracker files belong inside `docs/`** — any `BACKLOG.md` tracker lives at repo root by design (the reusable method is [docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md)).

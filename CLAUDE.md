# elt_pipeline — session entry

Auto-loaded into every session. Keep it tiny; it is a router, not documentation.

## Active backlog (cold-start resume)

There is durable, resumable backlog state in **[docs/todo/BACKLOG.md](docs/todo/BACKLOG.md)** (anchor doc, maintainer choice 2026-08-27 to keep repo-root clean) —
the **publication-readiness & platinum-hardening** effort: two tranches — (1) publish honestly
(reconcile the multi-cloud/ingest claims with the S3+local demo-grade reality + a maturity matrix),
and (2) platinum hardening (Iceberg maintenance, observability, orchestration, deployment, secrets,
governance, OpenLineage, DQ quarantine, multi-cloud storage). If the user asks to continue/progress
the backlog, open it and follow its **"Resume (start here)"** line (and read its **Platform
strengths** section first). Method: [docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md).
Operating model: **one session per backlog item**; update the Resume line + Status snapshot before ending a session.
When the backlog is exhausted and the gate is green, keep the anchor doc in place with its
"Backlog Status: EMPTY" banner — future RFC pull-forward work items land here. The process
constitution (Active Constraints 10-13) outlives the current tranche's items.

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
Repository-specific layout note (2026-08-27): This repo overrides PRD 10 §11's default anchor location
(repo root) for cleaner root directory. The BACKLOG anchor lives at **[docs/todo/BACKLOG.md](docs/todo/BACKLOG.md)**
by maintainer choice; no code or CI consequence. Reusable process method:
[docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md).

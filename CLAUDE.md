# elt_pipeline — session entry

Auto-loaded into every session. Keep it tiny; it is a router, not documentation.

## Active backlog (cold-start resume)

There is durable, resumable backlog state in **[BACKLOG.md](BACKLOG.md)** (repo-root anchor doc).
If the user asks to continue/progress the backlog, open it and follow its **"Resume (start here)"** line.
Operating model: **one session per backlog item**; update the Resume line + Status snapshot before ending a session.
When the backlog is exhausted and the gate is green, delete `BACKLOG.md` and this section.

## Running tests (required environment)

Spark/Iceberg tests need Temurin 23 on `PATH`/`JAVA_HOME`; a non-interactive shell does not inherit mise's activation. Export first, then run:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
uv run pytest -q
```

Without this, Spark tests fail with `JAVA_GATEWAY_EXITED` (environment, not a code defect).

## Canonical docs

Architecture/lifecycle source of truth: **[docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md)**.
Per PRD 10 §11, **no backlog/tracker files belong inside `docs/`** — `BACKLOG.md` lives at repo root by design.
</content>

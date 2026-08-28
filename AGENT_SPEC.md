# elt_pipeline — AGENT SPEC (Consolidated Framework Map)

> **Purpose of this file:** START HERE. This is the ONE consolidated index-card a coding agent
> (human or AI) reads first when starting a cold session on this repo. It contains the distilled
> summaries you need to not violate the architecture in one sitting: purpose, non-goals, file map,
> 6 extension APIs, invariants, gate commands, the 4-branch new-work playbook, architecture tables,
> and durable-state pointers.
>
> **Hierarchy (strictly NO CYCLES — one-directional links):**
> - **Tier 1 (SUMMARIES, editable here):** Cheat-sheet tables, contract tables, playbook text,
>   naming conventions, and exit-code lists. These live ONLY in this file and are maintained HERE.
>   They are NOT duplicated anywhere else. TRAE.md / CLAUDE.md thin routers are the ONLY docs
>   that link to this file.
> - **Tier 2 (FULL NARRATIVE + CANONICAL PROVENANCE, live in the deep docs):** Design rationale,
>   per-item D-1…D-6 decisions, verbose closure narratives, PRD requirements prose, maturity test
>   counts, Strategic Posture gap rationale, historical status snapshots. These live ONLY in their
>   respective canonical files listed in §9 deep-links. Tier-2 canonical files MUST NEVER link
>   back to AGENT_SPEC.md — if they need the distilled summary, tell readers to start from
>   AGENT_SPEC.md manually, not via a cross-link.
>
> **Lifetime rule:** If a summary fact in this file disagrees with the canonical Tier-2 file,
> the Tier-2 file wins and this file's summary must be updated to match. NEVER update a Tier-2
> canonical file to match a stale AGENT_SPEC summary. If in doubt, update the summary in THIS
> file to reflect the canonical provenance truth, not the reverse.

---

## 0. Session start essentials (run THESE first, copied verbatim from prior session routers)

These are the three most actionable lines for ANY session — the old TRAE/CLAUDE routers only
held these plus the AGENT_SPEC pointer. Now consolidated here as the first thing to read.

**1. Backlog anchor (live next-work + empty-state banner + Scoping Policy rules):**
Read [docs/todo/BACKLOG.md](docs/todo/BACKLOG.md) first when continuing any outstanding work.
Session start verbatim prompt: `from docs/todo/BACKLOG.md, continue`

**2. Required JVM environment (no exports → Spark tests fail with JAVA_GATEWAY_EXITED):**
```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
# 4-core Apple Silicon laptop (optional — avoids 8-way maintenance sandbox OOM)
# export ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g
```

**3. Authoritative gate (matches CI exactly — S-0 one-Spark-file-one-subprocess):**
```bash
bash scripts/run_tests.sh
```
Fast local alternative: `uv run pytest tests/<file>.py -v` (per-file only; NOT whole suite bare).
Lint/style: `uv run ruff check src/ tests/ examples/` → 0 errors required.
Smoke: `uv run elt-pipeline metric compile examples/sql/local_demo --with-sql-refs --format summary` → EXIT=0.

**Tier-1 drift anchor smoke (run this once per session; 8 key AGENT_SPEC §9 deep-links; no JVM; ~50ms):**
```bash
uv run python -c "
import pathlib
for p in ['docs/todo/BACKLOG.md','docs/prd/10-prd-architecture-and-lifecycle.md',
          'docs/CAPABILITY_MATURITY_MATRIX.md','docs/INDUSTRY_GAP_ANALYSIS.md',
          'docs/todo/archive/WORK_ITEMS_CLOSED.md','docs/operator/BYOD_TUTORIAL.md',
          'CONTRIBUTING.md','SECURITY.md']:
    assert pathlib.Path(p).exists(), f'AGENT_SPEC anchor MISSING (rename/moved?): {p}'
print('8 AGENT_SPEC anchor links OK')
"
```

---

## 1. Repo purpose & hard non-goals (5 sentences, memorize these)

**WHAT THIS FRAMEWORK IS:** A 5-level configuration-driven Apache Iceberg ELT runtime that runs on a
laptop with zero managed services (no AWS account, no Snowflake, no dbt Cloud). The pipeline is
100% YAML-driven: SQL model manifests, semantic metric manifests, connector configs. It builds
Iceberg tables (CREATE OR REPLACE TABLE … USING iceberg), maintains them (compaction / expire
snapshots / orphan removal / rewrite all), and serves them via Trino + Prometheus + OpenLineage.

**WHAT THIS FRAMEWORK IS NOT (hard non-goals, non-negotiable per Strategic Posture §2):** It is
NOT a BI semantic layer, NOT a reverse-ETL tool, NOT an ML feature store, NOT a data discovery
catalog UI, NOT a Grafana/Prometheus Alertmanager competitor (observability dashboards/alerts),
NOT a visual Airflow/Dagster workflow canvas, NOT a data diff/automated testing engine, NOT an
RBAC/ABAC access control plane, NOT a FinOps optimizer, NOT a multi-cloud replication engine,
NOT a policy-as-code executor, NOT a Flink/Kafka streaming analytics engine, NOT a data contracts
API gateway, NOT a data clean room host.

**DEFAULT ANSWER TO A NEW CAPABILITY REQUEST IS: NO** (zero-pre-scoped capability expansion,
Active Constraints 10-13). 9/10 of new feature requests either (a) already fit into one of the
6 Protocol/registry extension APIs (§3) with zero core code edits, or (b) compete with a mature
external OSS/cloud product that should be delegated to instead. See §6 for the full decision tree.

**Coding philosophy (Active Constraint 11: bidirectional guardrails):** Every cross-mode,
cross-layer, or YAML↔Python pair MUST be byte-identical or fail-closed. Examples: semantic
metric 3 run modes (materialize / view / prometheus) compare identical SHA-256 of a
normalized aggregation SQL string; governance column classifications flow from L3 YAML manifest
all the way to Trino masking DDL with no drift. Never "best effort approximate match".

**Fail-closed taxonomy:** User/operator errors are NEVER Python tracebacks as the primary
surface. Raise a structured `PipelineError` with a stable `error_code` enum string, a
retryable boolean, and a context dict. Exit codes are an API (§5).

---

## 2. File & code map ("where things live")

### Root-level .md files (6 files total, all justified):

| File | Role | Auto-read by |
|---|---|---|
| **README.md** | Navigation readme. 5-copy-paste-command quickstart, capability descriptions, honest boundary, architecture diagrams, CLI surface overview. | GitHub landing page, humans, agents |
| **CONTRIBUTING.md** | Environment setup, pCO layout rules, test minimums, 6 extension APIs (copied here in §3), PR checklist. | GitHub "please read before contributing" prompt |
| **SECURITY.md** | 90-day + 14-day-compression disclosure policy, scope list, supported versions, email/GH private-advisory contact. | GitHub security banner |
| **TRAE.md** | Session router (≤ 40 lines): backlog pointer + JVM exports + gate command. Lean, no prose. | Trae IDE at session start |
| **CLAUDE.md** | Mirror of TRAE.md for Claude Agents (≤ 40 lines). | Claude Agents at session start |
| **AGENT_SPEC.md** (this file) | Consolidated framework map. 1-file "index card" for cold coding agents. No prompt-injection-sensitive content. | Human/agent first-read after session router |

### Source code tree (src/elt_pipeline/):

```
src/elt_pipeline/
├── __init__.py                 (thin facade, __all__ = public API)
├── cli.py, _cli_parser.py,
│   _cli_main.py                (CLI entry: elt-pipeline — 8 commands: ingest/normalize/
│                                 sql compile/run/validate, maintain run, metric compile/run)
├── ingest/                     (ingest side: connector Protocol registry + implementations)
│   ├── connectors/
│   │   ├── registry.py         (ConnectorFactory Protocol + register_connector_factory)
│   │   ├── base.py, csv.py, kafka.py, rest.py, sql.py, file.py
│   │   └── sql_drivers/        (SqlDbDriver Protocol — postgres/mysql/snowflake/bigquery/synapse mssql)
│   ├── kafka_replay/
│   └── spark_loading/
├── normalize/                  (L2 MappingCatalog + normalize bypass)
│   ├── mapping_catalog.py
│   └── bypass.py
├── sql/                        (SQL model layer: YAML manifest → compiled → Spark.run)
│   ├── discovery.py, models.py, compiler.py, runner.py
│   ├── filters.py, lineage.py  (lineage emitter facets per GAP-3)
│   └── serve_trino_view.py     (Trino SECURITY DEFINER VIEWs per G-6)
├── metrics/                    (Semantic Metrics Layer, GAP-4 closed 2026-08-27)
│   ├── __init__.py             (pCO thin facade — 19-item __all__)
│   ├── _models.py              (8 Pydantic classes: MetricManifest → MetricAuditRecord)
│   ├── _compiler.py            (discover/filter/compile + --with-sql-refs governance check)
│   └── _runtime.py             (3 run modes: materialize/view/prometheus + guardrail + audit JSONL)
├── maintenance/                (Iceberg maintenance: compact/expire/orphan/rewrite + 8 sandbox envs)
├── schedule/                   (elt schedule runner: sla / wait_for sensors / retries)
├── shared/                     (cross-cutting shared concerns — MOST stable contracts)
│   ├── runtime.py              (RunContext class + StageName enum)
│   ├── errors.py               (PipelineError + ErrorCategory enum + build_error_record)
│   ├── governance.py           (DataClassification enum + SqlColumnSpec + SqlModelGovernance)
│   ├── config.py, schema_registry.py, retention.py
│   ├── storage_backends/       (StorageBackend Protocol + file/s3/gcs/adls + register_backend)
│   ├── secrets/                (SecretsProvider Protocol + env/aws/gcp/azure/hashivault/gsm
│   │                             + connector_scheme lookup + register_provider)
│   └── lineage/                (OpenLineage HTTP adapter)
├── integrations/               (6 facade packages following pCO pattern)
│   ├── metrics/                (Prometheus Gauge adapter + MetricPoint/MetricType)
│   ├── lineage/                (OpenLineage 2.x event builder)
│   ├── secrets/                (backwards-compat thin re-export of shared/secrets)
│   ├── storage/                (backwards-compat thin re-export of shared/storage_backends)
│   ├── quality/                (QualityHookBackend Protocol + 2 built-in hooks)
│   └── orchestration/          (schedule plan + SLA sensor + retry helper)
└── _version.py
```

### Other important directories:

| Path | Purpose |
|---|---|
| `tests/` | 1 flat directory. 1 file per source package. S-0 subprocess isolation. |
| `docs/prd/` | PRD corpus (PRD-00 platform principles, PRD-08 storage dispatch, PRD-10 architecture/lifecycle). Requirements doc, not changelog. |
| `docs/maintainer/` | Maintainer-facing: BACKLOG_CONTINUITY_PLAYBOOK, LOCAL_DEVELOPMENT_AND_RELEASE, JVM_TOOLCHAIN_SETUP. Operational. |
| `docs/operator/` | Operator-facing: LOCAL_OPERATOR_RUNBOOK, TROUBLESHOOTING, GOVERNANCE_AND_RETENTION_RUNBOOK, BYOD_TUTORIAL (30-min end-to-end new domain). |
| `docs/todo/` | Live anchor doc: `BACKLOG.md` (currently EMPTY banner). 3 archives (work items, tranche completions, status snapshots). |
| `docs/` root level | CMM, INDUSTRY_GAP_ANALYSIS, FRAMEWORK_FEATURE_MATRIX, 7× operator guides = the strategy docs. |
| `examples/` | `sql/` for SQL+metric YAML examples, `orchestration/` for Airflow ref DAG + wrappers, `secrets/` for vault URI example configs, `schedules/` for YAML plans. |
| `.github/` | `workflows/ci.yml` (uv sync + ruff + bash scripts/run_tests.sh + uv build), `ISSUE_TEMPLATE/` (bug/feature/RFC yml forms), `PULL_REQUEST_TEMPLATE.md` (6-section checklist). |
| `scripts/` | `run_tests.sh` (AUTHORITATIVE full gate — S-0 subprocess isolation). |

---

## 3. Extension-point contract table (6 zero-core-edits APIs — try THESE first)

90% of feature requests already fit here WITHOUT modifying src/elt_pipeline/* core files.
Work through the list top to bottom before opening a code PR that touches src/elt_pipeline/*
and before asking for an RFC to pull a gap forward.

| # | What you want | Register call name | Protocol file location | Implementation shape | Additive-only changes allowed |
|---|---|---|---|---|---|
| 1 | **New connector family** (SFTP, webhook, CDC, custom) | `register_connector_factory("myscheme", MyFactory())` | [registry.py L113](src/elt_pipeline/ingest/connectors/registry.py#L113-L132) | 1 class implements 5 methods: `discover`, `extract_records`, `validate_config`, `supports`, `from_config` | Yes. No core edits. Register at process start from YOUR plugin package entrypoint. |
| 2 | **New storage backend** (OCI, SMB, on-prem SAN) | `register_backend("oci://", OCIBackend())` | [_protocol.py L9](src/elt_pipeline/shared/storage_backends/_protocol.py#L9-L23) | 1 class implements 7 methods: `path_exists`, `path_read_bytes`, `path_write_bytes`, `path_delete_tree`, `path_list_dir`, `path_normalize`, `path_open_for_append` | Yes. No core edits. |
| 3 | **New secrets vault** (1Password, Infisical, your org's internal secret store) | `register_provider("myvault", MyVaultProvider())` | [_protocol.py L9](src/elt_pipeline/shared/secrets/_protocol.py#L9-L13) | 1 class implements `resolve_secret(uri) -> str` | Yes. No core edits. All `secret_ref://myvault/...` URIs work globally in configs/manifests. |
| 4 | **New data quality engine** (Soda, GX, Monte Carlo) | Register via `ELT_PIPELINE_QUALITY_HOOK_BACKENDS=env.soda:hook_impl_name` env var + Protocol impl file | [_models.py L108](src/elt_pipeline/integrations/quality/_models.py#L108-L111) | 1 class implements 1 method `run_hook(ctx, model, before_or_after) -> HookResult` | Yes. Zero core code. Env-driven, pattern matches the 2 built-in hooks. |
| 5 | **New SQL driver** (SAP HANA, Snowflake connect-native, MariaDB) | Modify 1 switch case in lazy-importer helper `_build_db_driver(scheme)` in `sql.py` | [sql.py L350](src/elt_pipeline/ingest/connectors/sql.py#L350-L388) | 1 class implements `connect`, `discover_tables`, `query`, `execute`, mapping handlers | Minor: only the dispatch switch in sql.py. Protocol + method signatures stable. |
| 6 | **New semantic metric aggregation** (p95, count_distinct_approx, quantiles) | Add enum value + 1 switch arm — NO register function needed (Enum switch) | Enum: [_models.py L13](src/elt_pipeline/metrics/_models.py#L13-L19); Dispatcher: `_build_aggregation_sql` in [_runtime.py](src/elt_pipeline/metrics/_runtime.py) | 1 enum value + 1 line in dispatcher returning correct SQL | Tiny core change. All 3 run modes, all audit, guardrail hash work ZERO other edits. |

When ALL 6 fail to fit the request → open an RFC-type issue per §6 below. Do NOT skip this list.

---

## 4. Coding-style invariants (non-negotiables — review will REJECT if violated)

### 4a. Architecture invariants

| Invariant name | Rule | Where to see violation |
|---|---|---|
| **pCO package layout (no gold files)** | 1 concern per file. Thin facade `__init__.py` + underscore-prefixed `_models.py / _compiler.py / _runtime.py`. If a file's docstring says "models + compiler" split it. | Reference impl: [metrics package](src/elt_pipeline/metrics/). Review rejects god files. |
| **Active Constraint 11 (Bidirectional guardrails)** | Cross-mode/layer/YAML↔Python values MUST be byte-identical or fail closed with a PipelineError. NEVER "approximate match" / "best effort" / "warn and continue" for a mismatch. | Canonical impl: `_check_consistency_or_raise` in [metrics/_runtime.py L43-L65](src/elt_pipeline/metrics/_runtime.py#L43-L65) — mismatched generated_sql_hash → METRIC_MODE_INCONSISTENT PipelineError. |
| **Fail-closed taxonomy, no tracebacks first** | User/operator-facing errors are structured PipelineError with `error_code` string enum, `retryable: bool`, context dict. Exit codes are API (§5). Raw Python exceptions leak only to debug-level local artifact logs. | [shared/errors.py L9-L18](src/elt_pipeline/shared/errors.py) ErrorCategory enum + PipelineError constructor + build_error_record helper. |
| **S-0 (One Spark/Iceberg file, one subprocess)** | Never put Spark/Iceberg-backed tests for TWO different subsystems into ONE test_ file. Each file gets its own JVM in the gate. This is structural, not convenience. | Enforced in `scripts/run_tests.sh`. |
| **Zero-pre-scoped capability expansion** (Active Constraints 10-13) | Do NOT code-speculatively implement any 🔴/⏳/Out-of-Scope row from the Capability Maturity Matrix. Backlog items are NOT created from a roadmap; they are pulled forward ONLY from concrete operator demand + RFC with 3+ hrs/week toil proof + Strategic Posture sign-off. | Full rule in [BACKLOG.md §Scoping Policy](docs/todo/BACKLOG.md). |
| **No new dependencies for small wins** | If a new capability fits in stdlib or re-uses the existing dependency closure (PySpark, PyYAML, pydantic, riprova, prometheus_client, trino, click), do that. New dep = explicit line-item justification in PR body with "why stdlib/existing didn't work". | `pyproject.toml` [project.dependencies] block is the audited list. |
| **Security disclosure path** | Vulnerabilities do NOT become issues. Follow SECURITY.md. 90-day disclosure window from private report → public issue, 14-day compression if active in the wild. | SECURITY.md L100 at repo root. |

### 4b. Naming & contract conventions you should copy

- **Directory symmetry:** Metrics YAML mirrors SQL YAML structure. `<package>/metrics/<domain>/<metric>/metric.yaml` is symmetric to `<package>/sql/<stage>/<domain>/<model>/manifest.yaml`. Keep this pattern going for future layers (not asymmetric new paths).
- **4-part dotted query_ref format:** `stage.domain.model.column` enforced at Pydantic + compile.
- **Exit codes = API:** 0 success, 1 generic runtime failure (metric run any metric failed; sql run pipeline error; etc.), 2 ConfigValidationError (manifest/compile/validate), 3 reserved future, 4 CLI argparse parse fail, 5 reserved future, 6+ maintainer-only internal. Keep consistent.
- **Audit artifacts:** `runs/<run_id>/` is the canonical output path. JSONL is append-mode. Metric audit file: `metrics/metric_audit.jsonl` (MetricAuditRecord Pydantic shape). Lineage: `lineage.jsonl`. Error records: `error_records.jsonl`.

---

## 5. Gate & verification contract (the 4 ways to run tests + exit codes)

### Authoritative gate (matches CI)

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"   # Temurin 23
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh
```

- CI at `.github/workflows/ci.yml` runs EXACTLY this. Green locally → green in CI.
- 4-core Apple Silicon: export `ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g` to avoid the
  8-way maintenance sandbox OOM (see JVM_TOOLCHAIN_SETUP troubleshooting row TD-4).

### Fast local development (per-file)

```bash
uv run pytest tests/test_semantic_metrics.py -v
```

- Fine for 95% of active work. Spark off by default; just the `spark_session` fixture frozen per session.
- **Required before PR:** Switch back to `bash scripts/run_tests.sh` once.

### Lint & style

```bash
uv run ruff check src/ tests/ examples/   # 0 errors required
uv run ruff check --fix ...               # auto-fixes sort-imports, line-length, etc.
```

### Smoke CLI parse

```bash
uv run elt-pipeline --help
uv run elt-pipeline metric compile examples/sql/local_demo --with-sql-refs --format summary
# Expected EXIT=0, 2 metrics named sales.monthly_sales and sales.order_count with 12-char hash suffixes.
```

### Exit codes as API (keep stable forever)

| Exit code | Meaning | Used by |
|---|---|---|
| 0 | All good | All commands |
| 1 | Runtime failure (1+ metric failed, sql run pipeline error, maintain run stage failed) | `metric run`, `sql run`, `maintain run` |
| 2 | Config/validation/compile error — bad manifest, wrong query_ref, governance column missing, etc. Retryable only after editing YAML. | `metric compile`, `sql validate`, `sql compile` |
| 4 | CLI argparse parse failure (bad flags, unknown subcommand) | Parser in cli parser module |

---

## 6. New work item playbook (backlog empty by default, 4 branches)

**Backlog is EMPTY.** The empty banner is a deliberate signal. Do NOT add items to BACKLOG
from "good ideas". Do NOT speculate. Add them ONLY following this exact 4-branch decision:

```
I want to change something.
├─► Idea fits one of the 6 extension APIs (§3)?
│   ├─ YES → Write a plugin with zero src/elt_pipeline/* core edits. Ship it as a separate package
│   │         OR commit it in examples/ as a documented plugin example. No BACKLOG entry. No RFC.
│   └─ NO → Continue below.
│
├─► Incremental improvement to existing green row? (bug fix / better error message / new CLI flag
│   on existing subcommand / new optional flag for an existing capability)
│   ├─ YES → Open a feature/bug issue with the template. PR directly.
│   │         No RFC. No BACKLOG entry unless the change is >1 day and >1 commit, in which case
│   │         BACKLOG §Resume gets a single bullet for that incremental item.
│   └─ NO → Continue below.
│
├─► New platform capability showing as 🔴 or ⏳ in the CMM / OOS in the Strategic Posture?
│   ├─ YES → Open RFC-type issue with the rfc-capability.yml template. MUST include:
│   │         (a) concrete use case
│   │         (b) MEASURABLE operator toil ≥ 3 hrs/week eliminated
│   │         (c) EXPLICIT answer to the "Strategic Posture Check" (last section of template):
│   │             "why isn't delegating to the standard mature external OSS/cloud product better?"
│   │         (d) file layout list mirroring pCO pattern
│   │         (e) ≥+16 test plan by category
│   │         Once maintainers sign off AND a signed-off strategic-posture exception exists:
│   │         → copy the RFC into BACKLOG §Still Todo as a work item.
│   │         → Work 1 item per session per BACKLOG_CONTINUITY_PLAYBOOK.
│   └─ NO → You're in branch 1 or 2. Go back.
│
└─► Security vulnerability:
    └─ DO NOT OPEN ISSUE. Follow SECURITY.md process. Private email / GH private advisory.
       90-day window, 14-day compression if active in wild.
```

The 4 templates that force the correct information from the submitter are at:
- Bug: [.github/ISSUE_TEMPLATE/bug_report.yml](.github/ISSUE_TEMPLATE/bug_report.yml)
- Feature: [.github/ISSUE_TEMPLATE/feature_request.yml](.github/ISSUE_TEMPLATE/feature_request.yml)
- RFC-capability: [.github/ISSUE_TEMPLATE/rfc-capability.yml](.github/ISSUE_TEMPLATE/rfc-capability.yml)
- PR checklist: [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md)

---

## 7. Architecture cheat sheet (at-a-glance tables)

### 7a. 5 layer model at the heart of everything

| # | Level name | In | Out | Key file or subsystem |
|---|---|---|---|---|
| 1 | L1 — Raw Landing | Connector extract (CSV/Kafka/REST/SQL/file) | L1 raw files JSONL/CSV/Parquet at `<root>/data/level1/` | `ingest/*` |
| 2 | L2 — Structured Parquet | L1 files + MappingCatalog YAML | L2 Parquet files at `<root>/data/level2/<domain>/<model>/` + `_MANIFEST.json` | `normalize/` (MappingCatalog, bypass) |
| 3 | L3 — Canonical Base Tables | L2 files + L3 YAML manifest (`sql/level3/<domain>/<model>/manifest.yaml`) + governance.columns[] | Apache Iceberg base tables. Column-level masking (hash_sha256) per DataClassification. | `sql/` (discover → compile → Spark CTAS USING iceberg) |
| 4 | L4 — Business Marts | L3 + L4 YAML manifest, required approved_only=True filter + explicit dependencies[] | Iceberg marts (joins / window / aggregations). Retention + classification inherited + overridable. | `sql/` |
| 5 | L5 — Serving Exports | L4 + Semantic Metric YAML (`metrics/<domain>/<metric>/metric.yaml`) | 3 export modes: (a) Iceberg metric tables `metric_<domain>_<name>`, (b) Trino plain VIEW or SECURITY DEFINER VIEW if `required_role != None`, (c) Prometheus gauge metrics named `elt.metric.<domain>.<name>` | `metrics/_runtime.py` 3 run_mode_* functions |

### 7b. 4-phase runtime lifecycle per session or orchestrator trigger

| # | Phase | Output | Failure |
|---|---|---|---|
| 1 | Validate | compile + governance ref checks + parse OK or exit 2 | ConfigValidationError, exit 2 |
| 2 | Execute | L1→L2→L3→L4 Spark run; semantic metrics 3-mode run | PipelineError caught per-model / per-metric; exit 1 if any failure |
| 3 | Audit | `runs/<run_id>/error_records.jsonl`, `metrics/metric_audit.jsonl`, lineage events JSONL | Always append. Audit write failures = P0 (never silent). |
| 4 | Maintain | Compaction / expire_snapshots / remove_orphan_files / rewrite_all (8 sandbox envs) | Retryable transient IO → riprova 3x. |

### 7c. Support matrix (Storage × Secrets × Connectors)

| Category | Built-in options | All extensible via Protocol? |
|---|---|---|
| Storage backends | file://, s3://, gcs://, az://, abfss://, adl:// | YES (register_backend — §3 row 2) |
| Secrets providers | env://, aws://, gcp://, azure://, hashivault://, gcp_gsm://, connector_scheme:// (inherit from same scheme as connector) | YES (register_provider — §3 row 3) |
| Connector types | csv, kafka (replay from JSONL), rest (HTTP), sql (postgres/mysql/snowflake/bigquery/synapse_mssql), file (generic file listing) | YES (register_connector_factory — §3 row 1) |
| Downstream sinks | Trino (JDBC), Prometheus (Gauge metrics), OpenLineage (HTTP events) | Via protocols. Dashboarding/alerting is OOS (delegated to Grafana/Alertmanager). |

### 7d. Tiered Governance Classifications (G-6)

| Enum value (DataClassification) | Applies to | Behavior |
|---|---|---|
| public | Columns, models → metric required_role | No masking. Public VIEWs OK. |
| internal | Columns, models, metric required_role | Hash_sha256 if set on column; SECURITY DEFINER VIEW wrapper when used as metric required_role; internal-only lineage tags. |
| confidential | Columns (e.g. customer_id) | Stronger hash_sha256; retention default < 90 days; surfaced only in explicitly-namespaced mart layers. |
| restricted | Columns (PII / PCI / HIPAA identifiers) | Fail-closed at validation time (ConfigValidationError) if any L3 model tries to expose a restricted column through L4 without explicit retention_partition + classification override sign-off. |

---

## 8. Durable-state pointers (where an agent reads/writes continuity between sessions)

| Artifact type | File | What lives there | When agent updates |
|---|---|---|---|
| Cold-session router (LEAN) | TRAE.md + CLAUDE.md (root, 37-line mirrors) | Backlog anchor path + gate command + JVM exports | Never unless the gate command itself or JVM toolchain changes. |
| Live work anchor | [docs/todo/BACKLOG.md](docs/todo/BACKLOG.md) | Empty banner (today), §Resume pointer, §Scoping Policy constraints, §Status snapshot gate numbers, §Still Todo block, §Closed Work Items table, §Gotchas. | **EACH session end** — if a work item was worked: update Resume line + 1-line status snapshot delta + Still Todo + (if closed) Closed Work Items 1-row summary. |
| Verbose closure narratives | [docs/todo/archive/WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md) | Per-item D-1…D-6 decision log, test counts by category, exact gate numbers, files-changed list, diff size | On close of a capability work item (RFC-closed). This is the canonical archival format (12 sections, see GAP-4 template). |
| Historical tranche summaries | [docs/todo/archive/TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md](docs/todo/archive/TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md) | Per-bullet narrative that used to live inside BACKLOG §Resume before deflation. | Append-only (no edits) at backlog deflation points. |
| Historical gate-status snapshots | [docs/todo/archive/STATUS_SNAPSHOT_NARRATIVES.md](docs/todo/archive/STATUS_SNAPSHOT_NARRATIVES.md) | Verbose status-snapshot sections from prior BACKLOGs | Append-only at deflation points. |
| Session-start verbatim prompt | BACKLOG.md §Session start prompt | `from docs/todo/BACKLOG.md, continue` | Update only when the anchor file path changes (changed 2026-08-27 moving from root). |
| Process playbook methodology | [docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md) | Reusable method for bounded AI-assisted sessions. Repository-specific override block at TOP of this repo's copy pointing to `docs/todo/BACKLOG.md` as the anchor location. | Update only when the methodology itself changes (extremely rare). |
| CI contract | [.github/workflows/ci.yml](.github/workflows/ci.yml) | `uv sync --extra dev --extra spark` → `ruff check` → `bash scripts/run_tests.sh` → `uv build`. | Mirror changes when Python/Java versions or the gate script changes. |

---

## 9. Deep-links for full stories (this file is an INDEX; go HERE for narrative)

| If you need to understand: | Start here (canonical source of truth) | Why this source, not AGENT_SPEC |
|---|---|---|
| **Why a parked gap is parked (Strategic Posture rationale, 16 OOS gaps delegated to which external tool)** | [INDUSTRY_GAP_ANALYSIS.md §2 Strategic Posture + §5 per-gap "Correct Documented Posture"](docs/INDUSTRY_GAP_ANALYSIS.md#L11-L22) | AGENT_SPEC says "default answer NO"; the Gap Analysis explains WHY for each gap. |
| **What ships + maturity grades (🟢/🟠/⏳/DEFUNCT per row, test counts, date closed)** | [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) | AGENT_SPEC §1 is a 5-sentence summary; the matrix has exact numbers. |
| **15-section condensed "what ships" by capability area** | [docs/FRAMEWORK_FEATURE_MATRIX.md](docs/FRAMEWORK_FEATURE_MATRIX.md) | Use this for proposal writing / executive summaries. |
| **Architecture deep-dive: 5 layers, 4 phases, 4-tier cascade, 4-tier SQL validity chain, catalog matrix** | [docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md) | The PRD. Requirements + constraints, not changelog. Read this BEFORE a large refactor of layer boundaries. |
| **Platform principles (DAMA-DMBOK alignment, non-goals, engineering values)** | [docs/prd/00-prd-platform-principles.md](docs/prd/00-prd-platform-principles.md) | Tie-breaker for "is this change aligned?" disputes. |
| **Storage URI dispatch contract (s3:// vs s3a://, why handoff URIs are always s3:// not s3a://)** | [docs/prd/08-prd-storage-root-uri-io-dispatch.md](docs/prd/08-prd-storage-root-uri-io-dispatch.md) | Gotchas section in BACKLOG §Gotchas L668 references this. |
| **Full spec + design decisions for a past closed capability work item (D-0 through M-11)** | [WORK_ITEMS_CLOSED.md](docs/todo/archive/WORK_ITEMS_CLOSED.md) — scroll to the 12-section entry for the item ID. Reference template = GAP-4 closed 2026-08-27 entry. | Each item has full D-1…D-6 design decisions, 16-item checklists, gate numbers, tradeoffs. AGENT_SPEC §3/§4 summarizes. |
| **Historical backlog structure + old Resume bullet narratives from TRANCHE 1/2 days** | [TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md](docs/todo/archive/TRANCHE_1_AND_TRANCHE_2_COMPLETIONS.md) | Don't re-open closed tranche items. Use for continuity archaeological digs only. |
| **Status snapshot narrative recap (large closed groups of items)** | [STATUS_SNAPSHOT_NARRATIVES.md](docs/todo/archive/STATUS_SNAPSHOT_NARRATIVES.md) | Use to avoid re-summarizing the same deltas twice. |
| **End-to-end operator tutorial: 30 minutes → new refunds domain with L3 + L4 + metric + SECURITY DEFINER VIEW** | [docs/operator/BYOD_TUTORIAL.md](docs/operator/BYOD_TUTORIAL.md) | New-user onboarding walkthrough. If adding a new example domain, mirror this. |
| **Runbooks for live operation** | [docs/operator/LOCAL_OPERATOR_RUNBOOK.md](docs/operator/LOCAL_OPERATOR_RUNBOOK.md), [TROUBLESHOOTING.md](docs/operator/TROUBLESHOOTING.md), [GOVERNANCE_AND_RETENTION_RUNBOOK.md](docs/operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md) | Day-to-day "where is my data", "why did my retention delete things", "why is Trino mask not working". |
| **Local development + release packaging + CI validation play-by-play** | [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md) | Maintainer-only, not operator. Covers `uv build` output, why gate uses subprocess. |
| **JVM toolchain setup (mise, Temurin 23 install, troubleshooting sandbox OOM)** | [docs/maintainer/JVM_TOOLCHAIN_SETUP.md](docs/maintainer/JVM_TOOLCHAIN_SETUP.md) | The TD-4 troubleshooting row for 4-core Apple Silicon `ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g` lives here. |
| **Session continuity method (AI-assisted session methodology, 1 session per backlog item, anchor doc structure)** | [docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md](docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md) with repository-specific override block at top of this repo's copy. | Read before multi-session continuity work on a new RFC item. |
| **Contributor-facing rules (env setup, pCO pattern, tests, 6 extensions, PR checklist)** | [CONTRIBUTING.md](CONTRIBUTING.md) | Human contributor onboarding, §5 extension APIs copied verbatim into AGENT_SPEC §3. |
| **Security disclosure process** | [SECURITY.md](SECURITY.md) | Mandatory stop before any "I found a security issue" → issue filing. |
| **Human-readable README quickstart + CLI overview + honest boundary** | [README.md](README.md) | Landing page. Links to all the docs above. |

---

## Maintenance note for THIS file (AGENT_SPEC.md)

This file holds **Tier 1 SUMMARIES ONLY** (per the top-of-file hierarchy definition). When
Tier-2 canonical docs change, only update the CORRESPONDING SECTION POINTERS and SUMMARY TABLES
here. Do NOT copy paste narrative from Tier 2 into this file — narrative lives in Tier 2.

**Cycle prevention rule:**
- DO NOT add links TO this file from any Tier-2 canonical doc.
- The only docs allowed to link TO AGENT_SPEC.md are: TRAE.md, CLAUDE.md (thin routers), and
  AGENT_SPEC.md internal self-references (§2 file map table listing itself, Maintenance note).
- If you find a Tier-2 doc with a link to AGENT_SPEC.md, remove that link and tell the reader
  to start from the thin routers or this file directly in prose text, not via a clickable link.

Updates you SHOULD make to this file (Tier-1 summary edits, no narrative duplication):
- New 7th Protocol/registry extension API is added → add row 7 to §3 table.
- Exit codes list changes → update §5 exit code table.
- New architectural level (L6) added → update §7a. Very rare; requires an RFC.
- Durable state pointers move (happened once: BACKLOG.md root → docs/todo/ 2026-08-27).
- §9 deep-links table: change the Tier-2 filename or line reference if a canonical doc moves.
- **pCO package layout refactor (e.g. CLI flat _cli_* → cli/ subpackage, 20-atomic-step green-gate split):**
  Update §2 File & code map table rows. Add/remove package directory rows with the correct
  thin-facade + _* submodule pattern. Tier-2 canonical plan for this refactor lives in
  BACKLOG.md §Still Todo (do NOT link to it here — cycle prevention rule).


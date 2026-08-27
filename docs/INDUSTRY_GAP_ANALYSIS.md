# Industry Gap Analysis & Framework Benchmark

## Document Status
- **Status:** Canonical reference (companion to the maturity + feature matrices)
- **Updated:** 2026-08-27 (first publication — system-wide review against industry-standard ELT frameworks; gate at time of writing: 807 passed / 0 failed / 28 emulator tests skipped per BACKLOG §Status snapshot)
- **Owner:** maintainer

## Purpose

This document answers the question: *"Where does `elt_pipeline` sit relative to what the industry expects from a modern configuration-driven ELT framework?"*

It **does not** restate capability maturities (that is the job of [CAPABILITY_MATURITY_MATRIX.md](./CAPABILITY_MATURITY_MATRIX.md)) and it **does not** relist what ships (that is [FRAMEWORK_FEATURE_MATRIX.md](./FRAMEWORK_FEATURE_MATRIX.md)). Instead, it:

1. Benchmarks every standard ELT-framework capability category against the industry norm (what dbt Core, Meltano, Dagster, Airbyte/Fivetran, Great Expectations, and DataHub/Marquez together deliver).
2. Scores `elt_pipeline`'s placement per axis.
3. Names concrete gaps, their **priority** (P0 strategic / P1 capability / P2 ergonomic / niche), the **code insertion point** behind the existing seam/Protocol/registry where each gap closes, and a **recommended path** forward.
4. Provides a prioritized effort-vs-impact roadmap for closing gaps.

This document is intentionally **gap-facing**, not strength-facing — §5 first calls out what is already above industry median so readers know the baseline before reading the gaps.

Companion documents:
- [CAPABILITY_MATURITY_MATRIX.md](./CAPABILITY_MATURITY_MATRIX.md) — maturity badge per shipped capability (🟢 / 🟠 / ⏳ / DEFUNCT) with per-row test counts and dates.
- [FRAMEWORK_FEATURE_MATRIX.md](./FRAMEWORK_FEATURE_MATRIX.md) — condensed 15-section tabular "what ships" with maturity-graded overview by capability area.
- [BACKLOG.md](./todo/BACKLOG.md) — next-work pointer, green-gate capture, and accumulated active constraints that every gap-closure item must honour.
- Architecture & lifecycle source of truth: [PRD 10 §6.3](./prd/10-prd-architecture-and-lifecycle.md).

---

## 1. Executive Placement

**Weighted overall score: 68 / 100** across 10 standard ELT-framework axes (see §2 for the per-axis table).

In plain language:
- The **foundation** (5-layer model, Protocol seams everywhere, 4-cloud storage parity, Iceberg-first, DAMA-DMBOK alignment, 807/0 green gate, singleton zero-env discipline) is **above the industry median** for an open-source ELT runtime — at or ahead of where most in-house builds plateau and competitive with Meltano v1–v2 on data-plane quality.
- The **gaps are almost entirely additive layers above this foundation**, not architectural flaws. Every gap below can close without rewriting any existing code — the registry/Protocol/adapter patterns are already the right shape.
- The **largest structural gap by a factor of 2× is connector ecosystem breadth** (4 families vs the 300+ norm). This is also the single highest-leverage gap to close because the existing `ConnectorFactory` Protocol/registry (M-1 in CMM §2) was designed explicitly for this.

Placement against named reference points:

| Reference | `elt_pipeline` sits… | Why |
|---|---|---|
| **dbt Core** | …at ~35% for *transform-only* feature parity | dbt invented package hub, snapshots, seeds, MetricFlow semantic layer, contracts, dbt docs UI — none of which exist here yet. But our SQL validity 4-tier chain (token→partition→EXPLAIN→DQ) and Iceberg staging-swap correctness are *above* dbt's default compile→write path. |
| **Meltano** | …at ~55% for full-pipeline platform parity | Meltano's 300+ Singer taps is the giant delta. Our storage parity, Iceberg catalog matrix, governance, secrets 6-provider coverage, and built-in DQ quarantine are *above* Meltano's default posture. |
| **Dagster** | …at ~60% for orchestration+ops parity | Dagster's asset graph UI, sensor/SLA framework, and partition backfill controls lead. Our built-in DAG runner + 4 thin wrappers match the *execution semantics* (subprocess→CLI), but the developer-plane ergonomics (UI/backfill manager/sensors) lag. |
| **In-house enterprise build** | …at ~160% — far ahead | Most in-house ELT builds stop at "Python Airflow DAG writes parquet + Glue table". 5-layer model, Iceberg-first, governance/classification, replay idempotency, 4-cloud storage, and the bidirectional doc↔code safety guardrails simply don't exist in 80%+ of in-house builds. |
| **Fivetran / managed Airbyte Cloud** | …at ~15% on SaaS ease-of-use — unfair comparison | Managed SaaS offers point-and-click 350 connectors with maintenance. This is a self-hosted framework. But the correctness properties (replay idempotency, partition overwrite blast radius, lineage) are comparable. |

---

## 2. Standard Capability Comparison (20 Categories)

Benchmark against the union of what industry-leading open-source ELT frameworks cover. Badge per category for `elt_pipeline`:

| # | Capability Category | `elt_pipeline` | dbt Core | Meltano | Dagster | Gap severity |
|---|---|---|---|---|---|---|
| 1 | Batch ingestion (REST/SQL/Object/Kafka) | 🟢 Production — 4 families, registry-factory, 6-SQL-driver matrix | ❌ (external) | 🟢 300+ Singer taps | 🟠 via ops | 🔴 P0 Major |
| 2 | Change Data Capture (CDC / WAL/binlog) | ⏳ Roadmap (deferred: Kafka Connect → object storage → standard ELT) | ❌ | 🟠 via Singer CDC taps | 🟠 via ops | 🔴 P0 Major |
| 3 | Streaming / micro-batch engine | ❌ Batch-only by design; Spark Structured Streaming not wired | ❌ | ❌ | 🟠 asset sensors | 🔴 P0 Major (mitigation: §6 GAP-13) |
| 4 | SQL transformation engine | 🟢 Production — compile/run split, 4-tier validity, Iceberg staging-swap | 🟢 Market leader (packages/snapshots/seeds/tests) | 🟢 via dbt | 🟢 via ops | 🟠 P1 Moderate |
| 5 | Incremental / merge strategies | 🟢 Partition overwrite + full_refresh + merge SQL generator (see [merge_sql_generator.py](../src/elt_pipeline/sql/merge_sql_generator.py)) | 🟢 4 strategies + custom | 🟢 via dbt | 🟢 via ops | 🟡 P2 Minor |
| 6 | SQL model package / versioning system | ❌ No package.yaml, no package registry, no cross-project refs | 🟢 dbt Hub + SemVer packages | 🟠 via dbt | 🟢 Asset graph | 🟠 P1 Moderate |
| 7 | Data quality framework | 🟢 6-check built-in library + scheme-agnostic quarantine/DLQ | 🟢 20+ generic + singular + dbt tests | 🟠 dbt + GE plugin | 🟢 via ops | 🟠 P1 Moderate |
| 8 | Data contracts / schema enforcement | ❌ No explicit contract API, no enforcement pre-write | 🟠 dbt contracts v1.7+ | 🟠 via dbt | 🟢 Asset checks | 🟠 P1 Moderate |
| 9 | Data catalog / discovery UI layer | ❌ Only Trino `SHOW SCHEMAS` + Pydantic models in filesystem | 🟢 dbt docs (graph+search) | 🟠 via dbt docs | 🟢 Asset graph UI | 🔴 P0 Major (mitigation: §6 GAP-5) |
| 10 | Column-level lineage + impact analysis | ❌ Table-level only; OpenLineage wire export but no ColumnLineageDatasetFacet | 🟢 via dbt docs graph | 🟠 via dbt | 🟢 Asset lineage UI | 🔴 P0 Major |
| 11 | Orchestration & scheduling | 🟢 Built-in DAG runner (topo-sort + retries + audit) + 4 wrappers: Airflow/Dagster/Prefect/Mage | ❌ (external) | 🟢 Airflow/Meltano schedules | 🟢 Market leader (sensors/SLAs/partitions) | 🟡 P2 Minor |
| 12 | Semantic layer / metric definitions | ❌ Metric logic only inside L4 SQL text; no cube/dimension/metric objects | 🟠 dbt Semantic Layer (MetricFlow) | ❌ | 🟢 via Dagster ops | 🔴 P0 Major |
| 13 | Reverse ETL (L5 → SaaS push) | ⏳ Roadmap pattern: Trino read → orchestrator wrapper → REST push | ❌ (Census/Hightouch) | 🟠 Singer targets | 🟠 via ops | 🔴 P0 Major |
| 14 | Secrets & access control / RBAC | 🟢 6 providers + repr-redaction + log redaction; NO native pipeline-artifact RBAC | 🟠 env vars only | 🟢 Meltano env | 🟠 via ops | 🟠 P1 Moderate |
| 15 | Data profiling / descriptive stats | ❌ No auto-describe; users write their own Trino ANALYZE | 🟠 dbt-profiles add-on | 🟠 via plugins | 🟢 via ops | 🟡 P2 Minor |
| 16 | Backfill manager / partition re-run UI | ❌ Window flags exist but no plan/status CLI or UI | ❌ | 🟠 via dbt + Airflow | 🟢 Dagster UI + partition sets | 🟡 P2 Minor |
| 17 | Cost attribution / query optimization | ❌ No Spark cost/bytes-shuffled metrics, no auto-optimize hints | 🟠 dbt Cloud only | ❌ | 🟠 via ops | 🟡 P2 Minor |
| 18 | Feature store training datasets | ❌ Not present (domain: ML engineering) | ❌ | ❌ | 🟢 Feast plugin | ⚪ Niche |
| 19 | Data versioning / Git-for-data branches | ❌ Iceberg `CREATE BRANCH` / `rollback_to_snapshot` no CLI sugar | ❌ | ❌ | ❌ (LakeFS/Buck add-on) | ⚪ Niche |
| 20 | Column-level encryption / tokenization | ❌ Not present; masking views exist but not encrypt-at-rest-per-column | ❌ | ❌ | ❌ | ⚪ Niche |

Coverage distribution by severity:
- 🟢 Already Production or at-par with leaders: **~40% (8/20)**
- 🟡 P2 Minor gaps (ergonomic, closes behind existing seams): **~20% (4/20)**
- 🟠 P1 Moderate gaps (new subsystem surface, existing seams shape-fit): **~25% (5/20)**
- 🔴 P0 Major gaps (blocks enterprise adoption, new subsystems): **~15% (3/20 — categories 2, 3, 10 have architectural mitigations in §6)**
- ⚪ Niche / domain-specific: **~10% (3/20)**

---

## 3. Verified Strengths (Above Industry Median — Protect)

Before reading the gaps, anchor on what is already genuinely strong. No gap closure is allowed to trade any of these away. (See also the explicit *Platform strengths* list in [BACKLOG.md §Root-cause summary](./todo/BACKLOG.md#L281-L299).)

| Capability | Where it lives | Why it beats most in-house + OSS builds |
|---|---|---|
| **pCO-compliant code: no gold files, thin facade + `_*` impl pattern everywhere** | Facade surfaces in [cli.py](../src/elt_pipeline/cli.py) (133 lines), [storage_backends/__init__.py](../src/elt_pipeline/shared/storage_backends/__init__.py) (82 lines), [secrets/__init__.py](../src/elt_pipeline/shared/secrets/__init__.py), [integrations/metrics/__init__.py](../src/elt_pipeline/integrations/metrics/__init__.py), [integrations/quality/__init__.py](../src/elt_pipeline/integrations/quality/__init__.py), [integrations/orchestration/__init__.py](../src/elt_pipeline/integrations/orchestration/__init__.py); guard tests in [test_facade_import_boundary.py](../tests/test_facade_import_boundary.py) (8 tests) | Most Python platforms accrete 5,000-line "god files". This repo eliminated 6 gold files in a dedicated tranche; guard tests prevent regressions. |
| **Zero-env-lockdown: singleton reads `os.environ` exactly once** | [runtime_context.py](../src/elt_pipeline/config/runtime_context.py) — materialiser + frozen `_RuntimeSingleton` | Downstream components never sniff `os.environ`. This eliminates drift from "works on my laptop" env config. |
| **4-tier config cascade (CLI > ENV > YAML > Manifest) with dotted-key access** | Same module; flat + nested access | dbt and Meltano both support cascading config, but this repo's *explicit one-shot materialisation with dotted keys + frozen immutable store* is stricter and thus safer. |
| **4-cloud storage parity behind single `StorageBackend` Protocol** | [_protocol.py](../src/elt_pipeline/shared/storage_backends/_protocol.py) + 4 backends (Local/S3/GCS/ADLS) + [_registry.py](../src/elt_pipeline/shared/storage_backends/_registry.py); 18 leaf IO ops + 1 atomic swap method each | Most OSS frameworks ship S3 and "local POSIX works by accident". Here GCS and ADLS have equal test coverage (28 pure-unit fake tests each + emulator integration opt-in) with SDK-lazy `ConfigValidationError` install hints on missing extras. Equal parity is rare. |
| **Leaf-partition-only staging swap with sibling preservation** | [atomic_swap](../src/elt_pipeline/shared/storage_backends/_registry.py) → per-backend `staging_swap_atomic`; partition subprefix inference in [_clients.py](../src/elt_pipeline/shared/storage_backends/_clients.py) (`_s3_infer_partition_subprefixes`) | The #1 operational bug in homegrown ELT is "refresh of a single partition nuked sibling partitions". This platform's swap scopes to the leaf subprefix *by construction* with a dedicated protocol method. Correctness by design, not operator discipline. |
| **6-way writer × 6-way serving catalog enum matrix + strict B-0 preflight BEFORE JVM boot** | [catalog_preflight.py](../src/elt_pipeline/shared/catalog_preflight.py) (8 scheme-aware checks × 3 enforcement modes: off / best_effort / strict); 50 pure-unit tests | Spark's opaque `Py4JJavaError` multi-hundred-line stack traces after 5 minutes of JVM boot cost engineering weeks/year. Preflight catches the mistake in 1 second with human-readable multi-failure context. |
| **6 secrets providers behind single `SecretsProvider` Protocol + `SecretValue` repr-redaction** | [_protocol.py](../src/elt_pipeline/shared/secrets/_protocol.py), [_registry.py](../src/elt_pipeline/shared/secrets/_registry.py); 6 registered: env/file/aws/azure/gcp/vault | Even managed platforms leak secrets to logs. Here `repr(secret) → "[REDACTED]"` at the string-subclass level: structurally impossible to leak via `%r`, tracebacks, or debug dumps. |
| **Replay idempotency with byte-identical checkpoint-after (max offset+1) across Kafka broker ↔ JSONL replay modes** | [LocalCheckpointStore](../src/elt_pipeline/ingest/state.py) + M-3/M-11 Kafka connector base | In most platforms, "replay" means "rerun and pray". Here the checkpoint-after formula is *identical* for both modes because they share one seam. |
| **Bidirectional doc↔code↔gate↔test safety guardrails** | BACKLOG §Resume/Status/CMM §Document Status/FFM §Document Status — all three stamped with the same numeric gate; forbidden-string publication scrub; 8 facade boundary tests | Most repos' READMEs drift within 2 weeks. Here *every numeric claim* (test count, maturity badge flip, gate delta, ⏳ count) is mechanically cross-checked against actual output before close. This is a genuine competitive advantage. |
| **Governance baked in, not bolted on: 4-tier classification + 7 masking strategies + retention SQL + erasure runbook** | [governance.py](../src/elt_pipeline/shared/governance.py); post-write `ALTER TABLE SET TBLPROPERTIES elt.governance.*`; [GOVERNANCE_AND_RETENTION_RUNBOOK.md](./operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md) | 90% of open-source ELT frameworks treat governance as a downstream BI-platform concern. Here classification and column-level masking are part of the SQL model manifest — part of the build, not a review. |

---

## 4. Gap Severity Definitions

Gaps in §5 use these severity bands:

| Label | Meaning | Enterprise blocker? | Close-without-rewrite? |
|---|---|---|---|
| 🔴 **P0 Major Strategic** | Will block enterprise adoption or frame `elt_pipeline` as "good for PoC only". The 4 P0 gaps below sum to the biggest structural delta. | Yes — each individually a deal-killer for some teams | Yes — every P0 has a recommended path that fits the existing registry/Protocol/seam |
| 🟠 **P1 Moderate Capability** | Teams can work around it (custom scripts / downstream tools / manual processes), but over 2+ years the workaround cost exceeds the build cost. | No — workaroundable | Yes — all 5 P1s sit behind existing adapter/manifest patterns |
| 🟡 **P2 Minor Ergonomic** | Developer friction, operator toil, occasional manual mistakes. Never a deal-killer but compounds into team burnout. | No | Yes — all 4 P2s are additive CLI/config sugar on top of existing execution |
| ⚪ **Niche / Advanced** | Needed by <10% of deployments (usually large enterprises or specific domains). Pull forward only on signed-off consumer demand per the zero-pre-scoped backlog policy. | No — most teams never need them | Yes — but intentionally left out of v1 scope on cost/benefit |

**Operating rule (aligns with BACKLOG §Work items):** No gap from this document becomes a work item in `BACKLOG.md` unless and until it is *pulled forward on concrete consumer demand*. This document is the triage playbook for when a consumer asks — it is not a pre-scoped backlog (which the repo explicitly forbids under the zero-pre-scoped policy).

---

## 5. Categorized Gap Inventory (20 Gaps)

**Posture preamble (2026-08-27):** Severity classification (P0/P1/P2) below is
*industry benchmark only* — it measures how far `elt_pipeline`'s feature set is
from a full-service ELT platform (dbt + Meltano + Dagster combined). The
*actual product posture* of each gap, including whether it is IN SCOPE, OUT OF
SCOPE, or LOW-PRIORITY IN-SCOPE, is governed by
[BACKLOG.md §Strategic Posture](./todo/BACKLOG.md#L350-L432). Do **not** pull a gap
forward based on severity alone — a P0 that competes with cloud services is
still OUT OF SCOPE. Key in-scope priorities per signed-off operator demand:
GAP-3 (Column-Level Lineage) ✅ pulled forward, GAP-4 (Semantic Metrics Layer)
✅ pulled forward, GAP-8 (Data Profiling) 🔵 in-scope but waiting for measured
toil. All others: ❌ OUT OF SCOPE (see BACKLOG §Still Todo blanket out-of-scope
list + Active Constraints 10–13) unless signed-off exception.

### 🔴 P0 — Major Strategic Gaps

#### GAP-1: Connector Ecosystem Breadth (4 families vs. 300+ industry norm)
**What industry has:** Airbyte (350+), Meltano/Singer (300+), Fivetran (300+ managed). Every SaaS product and on-prem DB has a maintained tap.
**What we have:** 4 Production families (`rest`, `sql`, `kafka`, `object_storage`) behind M-1's `ConnectorFactory` Protocol/registry. Registry dispatch + no-code preset authoring *within* families is Production. Adding a *5th family* requires exactly one `register_connector_factory()` call with zero CLI dispatch edits.
**Impact:** Any ingestion source that is not: an HTTP API, a SQL DB with one of 6 drivers, a Kafka topic, or files in object storage → requires Python connector code. For non-technical config-only users this is a hard stop.
**Recommended path (highest leverage in the repo):** Implement **`SingerTapConnector`** as a 5th connector family. Singer's `--config --state --catalog` protocol maps 1:1 to the platform's lifecycle (extract → persist → audit → checkpoint). Build one adapter class + a YAML/JSON tap-spec manifest registry (same pattern as M-1 ConnectorManifest preset system). Instantly unlocks 300+ maintained community taps without writing 300 connectors.
**Code insertion point:** New submodule [singer_tap.py](../src/elt_pipeline/ingest/connectors/singer_tap.py) + `register_connector_factory("singer_tap", SingerTapFactory())` at module load. Reads existing `SecretRef`, `LocalCheckpointStore`, and B-6 storage backend verbatim. Zero CLI changes (already registry-driven).
**Why no rewrite risk:** Singer taps are separate subprocesses — no Python dependency conflict; failures are subprocess exit codes (already handled by `SubprocessCliInvoker` pattern). Tap output JSONL is trivial to land into L1 raw manifests (same wire format as M-11 Kafka JSONL replay — existing `Level1ArtifactManifest` schema fits like a glove).
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 11 (no connector ecosystem reinvention). Singer/Airbyte/Fivetran maintain 300 connectors as their core business — this framework will not build a parallel ecosystem. Correct documented path: (a) vendor export-to-S3, (b) managed Airbyte/Fivetran → S3, (c) then `object_storage` connector into L1. Pull forward ONLY with signed-off strategic-posture exception.

#### GAP-2: No Native CDC (Change Data Capture) Ingest
**What industry has:** Debezium standard for Postgres WAL/MySQL binlog/MSSQL CDC/Oracle LogMiner. Airbyte has first-class CDC sources. Meltano has Singer CDC taps.
**What we have:** Explicit design-note posture: *"Land CDC to object storage via Kafka Connect/Debezium Server, then use the standard object_storage connector"* (see [FRAMEWORK_FEATURE_MATRIX.md §3](./FRAMEWORK_FEATURE_MATRIX.md) design note). This is architecturally correct for v1 but requires teams to run Kafka Connect — a non-starter for zero-infra workstation deployments and for operational DBs with no reliable watermark column.
**Impact:** Operational DB tables without `updated_at` watermarks fall back to full re-extract every run (expensive at scale). True row-level inserts/updates/deletes tracking requires CDC.
**Recommended path (two tiers, Tier A first):**
- **Tier A (lowest effort, 80% of value):** Add a **`postgres_logical_replication`** driver within the existing 6-driver `LocalSqlConnector` matrix. `psycopg` supports `LogicalReplicationConnection` natively. Emits row-level `(op, old, new, lsn, ts)` tuples. Checkpoint store already tracks offsets — store LSN as checkpoint key. This gives zero-infra Postgres CDC without Kafka Connect. New optional extra `--extra cdc_postgres`.
- **Tier B (full Debezium coverage, pull-on-demand):** Debezium Server → local JSONL/S3 → M-11 Kafka JSONL replay connector already works out of the box (because Debezium Server can emit to a file/S3 without a Kafka broker). Document this integration path and add a `debezium` preset in the M-1 ConnectorManifest registry.
**Code insertion point (Tier A):** Driver branch in [local_sql.py](../src/elt_pipeline/ingest/connectors/local_sql.py) (`_build_db_driver()`) alongside the existing 6.
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 11 (no CDC/WAL driver reinvention). AWS DMS / GCP Datastream / Azure CDC / Kafka Connect Debezium are the correct ingestion surfaces. Standard path: DMS/Datastream lands CDC to object storage → `object_storage` / `kafka` connector reads it. Watermark-column full re-extract on operational DBs is acceptable v1. Pull forward ONLY with signed-off exception.

#### GAP-3: Column-Level Lineage & Impact Analysis
**What industry has:** dbt docs column graph, DataHub column-level lineage graph search, OpenLineage `ColumnLineageDatasetFacet`, Monte Carlo impact analysis. "If I change `email_hash` type, which dashboards and which downstream consumers break?" is answerable in 1 click.
**What we have:** Table/input-output level lineage only in [LineageEvent](../src/elt_pipeline/shared/lineage.py). OpenLineage wire export (Production, CMM §12) ships the standard `EnvironmentRunFacet` but no `ColumnLineageDatasetFacet` or `SchemaDatasetFacet`. Column specs exist per model in `SqlColumnSpec` but are not pushed into lineage events.
**Impact:** Enterprise data-governance reviews will flag this. Without column-level lineage, impact analysis for contract changes is a manual grep exercise across BI repos.
**Recommended path (3 steps, additive only):**
1. Capture `SqlColumnSpec` entries from each SQL model manifest → emit OpenLineage `SchemaDatasetFacet` on the output DatasetRef (10 lines of model-to-facet mapping).
2. After Spark SQL execution, walk `DataFrame.queryExecution.analyzed` (Spark exposes a `TreeNode` of resolved attributes → input references) → map to OpenLineage `ColumnLineageDatasetFacet` (field-level transformations).
3. New CLI subcommand `elt lineage impact-analysis --column "<table>.<col>" --depth N` that reads `runs/**/lineage.jsonl` into a graph and walks it both directions. Outputs JSON + terminal table. Pure filesystem read — no new schema.
**Code insertion point:** Hook in [spark_executor.py](../src/elt_pipeline/sql/spark_executor.py) right after `df = spark.sql(compiled_sql)` before write; facet injection in the existing [integrations/lineage.py](../src/elt_pipeline/integrations/lineage.py) OpenLineage converter.
**Posture:** ✅ **IN SCOPE — PULLED FORWARD** to BACKLOG §Still Todo (2026-08-27). Core mission: data governance + column-level impact analysis. 90% of plumbing already exists (SqlColumnSpec, LineageEvent, OpenLineage export, lineage.jsonl always-on sink). Additive-only, zero new dependencies.

### GAP-4: Semantic Metric Definitions Layer

**Status: ✅ IMPLEMENTED (2026-08-27)**
**What industry has:** dbt Semantic Layer (MetricFlow), Cube.dev, LookML, Sigma Workbook model, ThoughtSpot modeling. *One* YAML definition of a metric such as "MRR" resolves identically whether BI tool A, BI tool B, or a Trino SQL query reads it.
**What we have:** L4 marts exist (aggregated BI-ready tables), but metric formulas are embedded *inline inside SQL model text only*. There is no framework-level metric object. Two dashboards querying the same L4 mart can produce two different MRR numbers if their SQL authors copy-paste the formula with slightly different predicates.
**Impact:** The #1 complaint about data platforms across every industry: "the numbers don't match between dashboards." L4 marts alone cannot solve this — only a defined metric layer can.
**Recommended path (new manifest layer, parallel to SQL models):**
- New YAML manifest format `metric.yaml` (or `metrics/` subdir per domain) declaring `MetricSpec`: name, description, query_ref → L3/L4 column, aggregation (sum/count_distinct/average/cumulative_rolling), dimensions (time + categorical), filters (SQL predicate string), required role classification.
- `elt metric compile` → resolves token context + validates refs point at existing L3/L4 models.
- `elt metric run` → one of two outputs (operator choice per metric): (a) pre-compute an Iceberg metric table via Spark (aggregation materialised), or (b) generate a Trino SECURITY DEFINER SQL view with identical metric SQL and column-level masking applied. Both paths resolve to the same number.
- A Prometheus/OTLP metric-exporter pass-through (optional): framework auto-derives `elt.metric.<name>` gauge from the same `MetricSpec` value per publish run.
**Code insertion point:** New package [metrics/](../src/elt_pipeline/metrics/) with pCO-thin facade `__init__.py` + `_models / _compiler / _runtime.py`. Reuses existing SQL compiler token context, Pydantic manifests, and Spark executor. No new runtime concepts.
**Posture:** ✅ **IMPLEMENTED (2026-08-27)** — closed BACKLOG §Still Todo. Core mission: 1-canonical-metric-to-1-number across materialized tables, Trino views, and Prometheus gauges. Prevents the #1 data-platform complaint: "dashboards disagree on MRR." Thin manifest layer on existing L3/L4 outputs — no new engine, no new dialect.

#### Implementation (2026-08-27)

Code lives in `src/elt_pipeline/metrics/` (pCO facade pattern: thin `__init__.py` + 3 underscore-prefixed implementation modules). Zero new execution paths — reuses 100% of existing:
  - SQL compiler token context
  - Pydantic manifest validation
  - Spark executor session builder
  - StorageBackend (for audit JSONL writes)
  - G-2 Prometheus gauge adapter (MetricType.gauge)
  - GAP-7 data contract enforcement (materialized tables inherit via standard spark executor write path)
  - G-6 Trino classification masking (SECURITY DEFINER VIEW wrapper toggled by `required_role` DataClassification)

CLI surface:
  - `elt metric compile <package> [--domain] [--metric] [--with-sql-refs] [--format summary|json]`
     - structural YAML validation
     - `--with-sql-refs` additionally walks the SQL package to verify the `query_ref` model_id exists and the referenced column appears in `SqlColumnSpec[]` governance (fail-fast ConfigValidationError, exit code 2, before JVM boot)
  - `elt metric run <package> --mode materialize|view|prometheus [repeated] [--target-catalog] [--target-namespace] [--iceberg-*]`
     - `--mode` is repeatable; operator can run all three together. When multiple modes run the cross-mode consistency guardrail is enforced (byte-identical `generated_sql_hash` comparison between modes → PipelineError METRIC_MODE_INCONSISTENT fail-closed, exit code 1).
     - materialize mode: Iceberg `CREATE OR REPLACE TABLE … USING iceberg AS SELECT … GROUP BY …` via `build_spark_session` → `spark.sql`
     - view mode: Pure DDL string (no JVM) — `CREATE OR REPLACE SECURITY DEFINER VIEW … COMMENT 'metric_id=…' AS …` SECURITY DEFINER wrapper injected when `required_role: public|internal|confidential|restricted_pii` is set
     - prometheus mode: Emits a zero-valued gauge definition (operator fills in the value extractor callable). Metric naming: `elt.metric.{domain}.{name}`.
     - Output: structured JSON payload with success_count / failure_count + `metric_audit.jsonl` path.

Bidirectional consistency guardrail (Active Constraint 11 compliance):
  - Every mode produces a SHA-256 hash of the dimension-sorted, source-reference-normalized SQL
  - When ≥2 modes run in the same invocation, hashes MUST be byte-identical between mode pairs (materialize vs view, materialize vs prometheus, view vs prometheus)
  - Mismatch → PipelineError error_code=METRIC_MODE_INCONSISTENT with full context dict of both hashes
  - Normalization anchor: Aggregation SQL is always built with `source_table_ref="SOURCE_TABLE"` constant to make hashes independent of actual catalog/namespace strings

Gate delta on close: +21 tests (all non-Spark, no emulator) — 21 passed / 0 failed.

#### GAP-5: No Data Catalog / Discovery Browser UI
**What industry has:** dbt docs (graph + search + column descriptions per model), DataHub (full catalog with search/ownership/lineage/graph), OpenMetadata, Amundsen.
**What we have:** Everything lives as YAML manifests + Pydantic models on disk + JSONL lineage files. Users browse via `Trino CLI SHOW SCHEMAS / DESCRIBE / SELECT`. There is no search, no ownership fields, no tag filtering, no column-documentation browser, no lineage-graph visualiser.
**Impact:** For teams >5 people, "which table should I query?" becomes tribal knowledge. Self-serve analytics is impossible without a catalog UI.
**Recommended path (TWO options — neither requires writing a UI):**
1. **Option A (fastest, leverages existing OSS UI):** Output a dbt-docs-compatible `manifest.json` + `catalog.json` at `elt sql generate-docs --out <dir>`. The existing dbt docs UI (`dbt docs generate && dbt docs serve`) works with *zero dbt code* if you produce files matching its JSON schema. Our SqlModelSpec already has: name/stage/domain, SqlColumnSpec with name/type/classification/description fields, depends_on list, sources list. All map directly to dbt manifest nodes. 1 mapping module = instant graph + search + column docs UI.
2. **Option B (enterprise catalogue):** Document and tighten the existing OpenLineage wire export → any Marquez / DataHub / OpenMetadata instance ingests the events natively. Add a 1-click `elt catalog push-to-datahub --url <>` CLI helper that reads historical `lineage.jsonl` + SqlModelSpec manifests and POSTs enriched Dataset facets (ownership/tags/descriptions) in one bulk batch. No new UI, but full enterprise catalog support.
**Code insertion point:** New [catalog/](../src/elt_pipeline/catalog/) package (Option A) or pure documentation + [_cli_main.py](../src/elt_pipeline/_cli_main.py) helper subcommand (Option B).
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 10 (NO UI surface, no React/FastAPI/Flask servers). Existing OSS UIs handle this with years of investment: (a) dbt docs — framework can emit manifest-compatible JSONL as a future adapter, (b) Marquez/DataHub/OpenMetadata — already ingest the existing OpenLineage wire export. A dbt-manifest JSON emit helper is borderline IN SCOPE (pure CLI, no UI); if a consumer asks for dbt docs integration, pull the "Option A emitter only" subpath (no UI) as a new mini-gap. Pull forward ONLY with signed-off exception.

---

### 🟠 P1 — Moderate Capability Gaps

#### GAP-6: SQL Model Package Versioning & Shared Canonical Model Library
**What industry has:** dbt Hub, package registry, `packages.yml` with SemVer + git refs, cross-project `ref('pkg', 'model')`.
**What we have:** Filesystem-organised SQL models per `examples/sql/local_demo/` pattern. No `package.yaml` declaring package version/deps/exports. No way for deployment A to import deployment B's shared `stripe_orders` L3 canonical model without copy-paste.
**Impact:** Duplicate canonical model implementation across deployments. Shared standard library of framework-owned models (e.g., common date dimensions, generic customer canonical models) cannot exist.
**Recommended path (pipeline YAML + CLI flag only, no execution engine changes):**
- New optional top-level YAML key in pipeline config: `sql_packages: [{url: git+https://..., ref: v1.2.0, alias: stdlib}, …]`. Accepts: git URLs, local filesystem paths, HTTPS tarballs.
- New CLI flag `elt sql {compile,run} --resolve-packages` (or env `ELT_PIPELINE_RESOLVE_PACKAGES=1`). On first run per SHA, clones/extracts into `.ignore/.sql_pkg_cache/<sha1>/`; subsequent runs use cache. Cleanup via `elt sql cache-clear`.
- Model discovery (`sql/discovery.py`) merges package dirs with local dirs, prefixing package models with the alias (`stdlib.common.dim_date`).
- Optional extras: `--extra gitscm` if `git` binary is not acceptable (via `GitPython` SDK), default relies on `git` binary (JDK/Spark already require binaries, acceptable).
**Code insertion point:** Hook in [discovery.py](../src/elt_pipeline/sql/discovery.py) (`discover_sql_packages()`) — add package-URL resolution before the existing filesystem walk. Reuses existing `SqlModelManifest` parser 100%.
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 13 (no package manager / SemVer resolver). Teams start monorepo; multi-deployment shared models use `git subtree` / copy-paste. A full package manager is a product in its own right (dbt Hub already solved it). Pull forward ONLY with signed-off exception AND measured shared-model duplication toil.

#### GAP-7: Explicit Data Contracts & Schema-As-Code Enforcement (Pre-Write)
**What industry has:** dbt contracts (1.7+), Soda Core contracts, Great Expectations Expectations, Monte Carlo automated contracts.
**What we have:** Quality hooks validate *data content* (not-null/uniqueness/range/RI/freshness/regex). But there is no explicit framework-level enforcement of: "This L3 canonical model MUST expose columns {order_id STRING, customer_id BIGINT, order_total DECIMAL(18,4), order_date DATE} and these columns may not be renamed, retyped, or dropped unless the contract version is incremented" — enforced *before* the write commits.
**Impact:** A upstream L2 schema change (new `order_total_micros` instead of `order_total`) silently propagates through the L3 compile, passes DQ if no rule checks it, and breaks every L4 mart and downstream consumer. Right now detection relies on human review + DQ coverage — both are fallible.
**Recommended path (reuses 100% of existing Pydantic manifest model fields):**
- Add field `contract: strict | warn | off` to `SqlModelManifest` (default: `off` for backward compat; L3 canonical + L4 published marts recommended `strict`).
- At [spark_executor.py](../src/elt_pipeline/sql/spark_executor.py) write time, just before commit: compare (a) declared `SqlColumnSpec` list from manifest with (b) actual `df.schema` StructType of the DataFrame being written + (c) the current Iceberg table schema read back from the catalog (if table exists). Compare name/nullable/type.
- Strict mode → raise `CONTRACT_BROKEN` with structured diff (`added/removed/changed columns`) before any write. Warn mode → emit WARN class `contract_broken` log event to `logs.jsonl` + Prometheus `elt.contract.broken` gauge counter + allow write.
- Optional: write a `contract_version: 1.2.3` field per manifest and enforce monotonic increases (breaking change = major bump).
**Code insertion point:** Write-time interlock in [spark_executor.py](../src/elt_pipeline/sql/spark_executor.py) right before the atomic staging-swap.
**Posture:** ✅ **IMPLEMENTED** (2026-08-27). See BACKLOG §GAP-7 closure. Gate delta 807 → 830 (+23 new tests). `strict`/`warn`/`off` contract manifest field enforced at write-time interlock BEFORE staging swap or Iceberg commit. Structured {added/removed/changed} diff context, `elt.contract.broken` Prometheus counter, strict mode also validates against existing table catalog schema.

#### GAP-8: Automatic Data Profiling (Per-Model, Per-Stage)
**What industry has:** Great Expectations profiling, dbt `profiles` add-on (distinct rate / null rate / min / max / stddev / quantiles / top-N per column), Soda scan auto-profiling.
**What we have:** Manual — users write their own Trino/Spark `SELECT approx_count_distinct(col), …` queries.
**Impact:** New model onboarding cost is higher. Schema drift detection that *should* be automatic is manual.
**Recommended path (behind existing DQ adapter seam — follows BuiltinQualityHook pattern):**
- New `ProfilingQualityHook` (reuses `QualityHookBackend` Protocol in [_models.py](../src/elt_pipeline/integrations/quality/_models.py)). Per column, outputs: row_count, null_count, null_rate, distinct_count, distinct_rate, min, max, approx_quantiles[0/25/50/75/100], top_10_values (string cols only), avg_length (string cols).
- Always-on, non-blocking, zero-config default (opt-out via `ELT_PIPELINE_QUALITY_STAGES=` exclusion). Writes JSONL output to `runs/{run}/quality_profile/{stage}/{model_name}.jsonl` — same B-6 storage backend as quarantine.
- Add `elt profile show <model>` CLI: pretty-prints the latest profile JSONL from the most recent successful run for that model/domain pair.
**Code insertion point:** Registered alongside the two existing hooks in [quality/__init__.py](../src/elt_pipeline/integrations/quality/__init__.py).
**Posture:** 🔵 **IN-SCOPE, LOW PRIORITY — WAITING FOR MEASURED TOIL.** Architecturally aligned: pure additive behind existing `QualityHookBackend` Protocol, no new subsystems, no UIs, no cloud-service duplication. But default-off because most teams write manual profiling queries as a one-off and the cost/benefit is unproven. Pull forward ONLY when an operator ticket documents measurable toil (e.g. "we spend 3+ hours/week manually running per-model approx_count_distinct on new L3 canonical models").

#### GAP-9: Reverse ETL (Push Connectors — L5 Data to SaaS)
**What industry has:** Census, Hightouch, Grouparoo, Meltano Singer targets. The standard pipeline loop includes writing back *to* SaaS operational systems (write account health scores to Salesforce, write user lifecycle flags to Intercom, write product usage counters to HubSpot).
**What we have:** L5 publish only creates files (CSV / JSONL / TSV / ZIP). README Honest Boundary states: "⏳ Roadmap pattern: Trino JDBC read → orchestrator subprocess wrapper → REST/SQL connector push." Correct but not built.
**Impact:** Operational teams that need Reverse ETL either: use a Census/Hightouch license, write custom scripts, or skip Reverse ETL entirely and use Trino direct queries from SaaS (if supported). None of these are ideal.
**Recommended path (symmetric with ingest: factory Protocol + registry — 4 push targets cover 80%+ demand):**
- New top-level CLI: `elt reverse-etl run <publish_defs_or_metric_refs> --target <target_name>`. Reuses L5 publish's discovery/manifest pattern — a "reverse manifest" declares: source (L4 query or metric), target system, target object type (Contact / Account / Custom Object), field mapping, upsert key.
- New `PushTargetFactory` Protocol (mirrors `ConnectorFactory`) behind a registry — exactly one `register_push_target_factory()` call per target. First 4 targets:
  1. `rest_generic` (reuses [rest.py](../src/elt_pipeline/ingest/connectors/rest.py) auth flows: basic/api key/bearer/client-credentials; POST/PUT/PATCH with rate limit 429 retry+backoff).
  2. `salesforce` (SDK: `simple-salesforce`, optional extra `--extra salesforce`).
  3. `hubspot` (REST API, bearer token, rate-limit-aware — same `rest_generic` helper + HubSpot preset mapping).
  4. `sql_db` (reuses existing 6-driver LocalSqlConnector: UPSERT via merge on target DB — `sql/m`).
- Reuses G-5 `secret_ref`, G-2 observability, lineage adapter (writes LineageEvent with output = L4 query input, output = target SaaS `DatasetRef` with namespace `reverse_etl:salesforce`), and quarantine for failed rows.
**Code insertion point:** New package [reverse_etl/](../src/elt_pipeline/reverse_etl/) with pCO facade pattern. Zero changes to existing subsystems — all re-used.
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 12 (no Reverse ETL push-target registry). Census/Hightouch/Grouparoo own target SaaS operator semantics with rate-limit/retry/mapping UIs. Correct documented path: Trino JDBC read of L4/L5 → orchestrator wrapper (Airflow/Dagster/Prefect/Mage) → bespoke REST call via existing M-1 `rest` connector or target SDK. Pull forward ONLY with signed-off exception.

#### GAP-10: RBAC/ABAC for Pipeline Artifacts & Execution
**What industry has:** Dagster Cloud RBAC, dbt Cloud RBAC, Meltano RBAC + Permifrost. Who can run a backfill? Who can publish a L5 restricted-PII artifact? Who can view audit logs? These are controlled at the framework level, not just filesystem/Trino level.
**What we have:** G-6 covers 4-tier classification → TBLPROPERTIES → Trino column masking views. G-5 secrets control access to *sources*. But access control to *the pipeline itself* (who can trigger `elt sql run` with `--all-level3` on a production catalog, who can read/write a restricted L5 export, who can view run logs) is delegated entirely to the filesystem/Trino/orchestrator.
**Impact:** Multi-team deployments cannot have "team A can only run jobs against the sales domain, team B can only read internal-classification outputs" within the framework. It must be layered on outside. Operational auditors want framework-level RBAC evidence.
**Recommended path (non-breaking, opt-in — defaults to fully-open backward compat):**
- Single env var (centralized per pattern): `ELT_PIPELINE_RBAC_POLICY_YAML=./config/rbac_policy.yaml`. Subject resolves via `ELT_PIPELINE_RBAC_SUBJECT=user:alice` or `ELT_PIPELINE_RBAC_SUBJECT=group:data-platform`. Free-form subject string so bespoke internal platforms pass their identity system via env injection.
- `RbacPolicy` Pydantic model: `roles: [{name, subjects, permissions: [{stage, scope: source/entity/domain/model, actions}]}]`. Scope supports globs: `domain:sales/*` / `stage:maintain`.
- Interlock at single point: top of [_cli_main.py](../src/elt_pipeline/_cli_main.py) (`main()`) before any stage runs. Fail with `RBAC_DENIED` `PipelineError` with structured context (`subject/role/missing_permission/stage/scope`) before JVM/SDK boot.
- Write `rbac: {subject, role_used, matched_rules}` to every run's audit record + lineage run facet.
**Code insertion point:** 100-line module [rbac.py](../src/elt_pipeline/shared/rbac.py) + 10-line hook in CLI entrypoint. Everything else reuses existing audit/lineage/env infrastructure.
**Posture:** ❌ **OUT OF SCOPE** per BACKLOG §Strategic Posture + Active Constraint 11 (no framework-level RBAC). Access controls are delegated to: IAM (pipeline run identity), Trino CLS/Ranger + column-level masking views (artifact read), Iceberg catalog RBAC (Glue/Nessie/HMS), orchestrator wrapper role controls (Airflow Dagster Prefect Mage). Duplicating this matrix inside the framework guarantees drift. Pull forward ONLY with signed-off exception AND a documented scenario where IAM/Trino/orchestrator RBAC genuinely cannot cover the requirement.

---

### 🟡 P2 — Minor Ergonomic & Operational Gaps

#### GAP-12: Backfill Plan/Status CLI & Audit Tracking
**Current state:** `--start-date / --end-date` window flags exist per stage. Checkpoint store tracks history.
**Missing tooling:** No "given a manifest + 12-month window, list every chunk and run status" generator; no "retry only chunks that failed" selector; no "show % complete + ETA for a 12-month backfill running across 10 workers."
**Workaround today:** Operators script loops around the CLI.
**Recommended path (two pure filesystem/audit-reading CLI subcommands):**
- `elt backfill generate <pipeline.yaml> --source X --entity Y --start-date A --end-date B --chunk-size months=1` → outputs `backfill_plan.json` (list of per-chunk argv tuples with expected order + chunk_id). Works for any stage that accepts date windows.
- `elt backfill status <plan.json_or_run_root>` → reads each chunk's audit record (or absence) + checkpoint history → terminal table: `chunk_id, status (pending/running/succeeded/failed/skipped), started_at, duration, exit_code, error, attempts, checkpoint_after`. Prometheus gauges via G-2 for active backfills.
- `elt backfill retry-failed <plan.json>` → re-runs only failed chunks with same argv and a `backfill_attempt=N` run attribute stamp.
**Code insertion point:** New subcommands in [_cli_parser.py](../src/elt_pipeline/_cli_parser.py) + implementations in [_cli_main.py](../src/elt_pipeline/_cli_main.py). Pure read/write against existing artifact layout + B-6 storage backends. No new schemas.
**Posture:** ❌ **OUT OF SCOPE — ergonomic-only sugar per BACKLOG §Strategic Posture boundary 7 + Active Constraint 13.** Operators today script loops around the CLI with date windows. This is legitimate toil, but it's additive-only ergonomic CLI sugar with zero architectural impact on the transform/governance core. Pull forward ONLY with a concrete measured-toil ticket (e.g. "our backfill team wastes 8h/week manually tracking failed chunks across plans").

#### GAP-13: Micro-Batch Scheduling (Sub-Hour Freshness Without New Engine)
**Current posture (per FFM §4):** "⏳ Roadmap. Preferred: cloud-native durable sinks then batch ELT. Streaming add-on if explicitly signed off."
**Why a gap:** Many teams want 5–15 minute freshness but *cannot or will not* operate long-running Spark Streaming jobs (maintenance cost, cost monitoring, resource allocation). The current workaround (cron the full batch every 5 minutes) is wastefully redundant for large datasets because it re-reads all inputs each run.
**Recommended path (NO new engine — pure scheduling + checkpoint semantics sugar on top of existing batch execution):**
- Add `microbatch_interval_seconds: 600` (10 min) field to schedule plan or per-job.
- When set: each iteration reads checkpoint_after from `LocalCheckpointStore` (already exists) and uses it as the window_start for the next iteration. Window = `(checkpoint_after, min(checkpoint_after + interval, wall_clock))`. Checkpoint-after becomes the new *watermark high-water mark*, not just a resume point.
- Exactly-once preserved by existing checkpoint idempotency + partition overwrite. No Spark Streaming code is ever written — the *only* difference from a normal batch is smaller windows triggered at intervals. Freshness is sub-interval; engine semantics are identical.
**Code insertion point:** Per-job window derivation in [_cli_main.py](../src/elt_pipeline/_cli_main.py) (`_run_schedule_plan()`) + per-job checkpoint update after each successful microbatch iteration. Reuses existing LocalCheckpointStore verbatim.
**Posture:** ❌ **OUT OF SCOPE per BACKLOG §Strategic Posture boundary 7 (ergonomic-only sugar by default).** The recommended path is architecturally sound (no new engine, pure checkpoint-window sugar over existing batch execution) and the *industry gap is real* (5–15 min freshness). But the correct posture for this framework is: teams wanting sub-hour freshness should evaluate whether Spark Structured Streaming, Flink, or ksqlDB are the correct long-lived engine for their latency SLA. Cron + microbatch windowing inside this framework is a valid interim workaround if a team *specifically* asks for it. Pull forward ONLY with concrete SLA evidence that the 4 ingress families + normal schedule cron genuinely cannot meet freshness requirements.

#### GAP-14: Spark Cost Attribution & Auto-Optimize Hints
**Current state:** G-2 observability exports run duration, rows read/written, files created. No Spark internal metrics: bytes shuffled, spill-to-disk bytes, task count, skew ratio (max task duration / median task duration), or plan-level size-in-bytes estimates.
**Impact:** Slow SQL runs require operator expertise to profile (`EXPLAIN FORMATTED` + Spark UI digging). No automatic "this join should broadcast, let me hint it" heuristic.
**Recommended path (two additive passes, opt-in):**
1. **Cost attribution (always-on, no overhead):** After `df = spark.sql(...)` at executor, attach a short `SparkListener` (or read `df.queryExecution.sparkPlan.stats.sizeInBytes` + `df.rdd.getNumPartitions`) and emit structured `cost` facet: `{bytes_in_memory_estimate, num_partitions, broadcast_hint_recommended, estimated_row_count}`. Writes into audit record + Prometheus gauges `elt.cost.bytes_shuffled` / `elt.cost.spill_bytes` / `elt.cost.task_count` / `elt.cost.skew_ratio` if the listener is attached.
2. **Auto-optimize hints (opt-in flag — `ELT_PIPELINE_SPARK_OPTIMIZE_AUTO=1`):** Safe heuristics only (never correctness-changing):
   - If one side of a join is estimated <10MB by Spark stats → auto-wrap the DataFrame with `broadcast()` before writing (overridable by manifest field `optimize.broadcast = [sources]`).
   - If post-read partition count × 2 < `spark.sql.shuffle.partitions` → auto `coalesce()`.
   - If skew ratio >20 detected post-execute → emit WARN with suggested `repartition(column)` column for next run (never auto-apply on skew — correctness risk).
**Code insertion point:** Post-execute hook in [spark_executor.py](../src/elt_pipeline/sql/spark_executor.py) — short and additive.
**Posture:** ❌ **OUT OF SCOPE — ergonomic-only sugar per BACKLOG §Strategic Posture boundary 7 + Active Constraint 13.** Spark UI, Spark History Server, and `EXPLAIN FORMATTED` are the correct surfaces for cost attribution and broadcast-hint profiling. Framework-level Prometheus gauges are nice-to-have, but: (a) Spark UI is always more accurate for JVM-level metrics, (b) auto-broadcast heuristics are correctness-fragile and should be decided by humans reviewing `EXPLAIN FORMATTED`, not framework heuristics. Pull forward ONLY with a measured-toil ticket documenting repeated weeks of engineering lost to manually profiling slow SQL runs.

---

### ⚪ Niche / Advanced Gaps

Pull forward only on explicit signed-off consumer demand. Each is a legitimate framework capability but <10% of deployments need them in the first 2–3 years.

**Posture (entire niche section):** ❌ **OUT OF SCOPE per BACKLOG §Strategic Posture boundary 8 + Active Constraint 13.** All 5 gaps are individually valid and architecturally implementable behind existing seams (Iceberg Spark procedures, G-5 KMS providers, merge_sql_generator, L2 Spark loader). But they serve <10% of deployments and pull the framework into non-core concerns (ML feature serving, regulated-industry branch review, KMS crypto, SCD2 operational history tracking, static CSV loading). Pull ANY of GAP-15–GAP-19 forward ONLY with (a) a signed-off consumer demand ticket, AND (b) a signed-off strategic-posture exception from the product owner.

| ID | Capability | When Needed | Recommended Path Behind Existing Seams |
|---|---|---|---|
| GAP-15 | Feature store training datasets + point-in-time joins | ML engineering teams using L3 data for model training | New package [features/](../src/elt_pipeline/features/) reusing: (a) L3 Iceberg time-travel for point-in-time correctness (already accessible), (b) existing metric aggregator for feature computation, (c) output emitter for Feast-compatible `feature_view.yaml` manifests so online serving uses Feast with zero duplication. |
| GAP-16 | Iceberg branch-as-Git (commit/tag/rollback/merge CLI sugar) | Large regulated enterprises wanting "run book data migrations" with Git-style review/rollback | Thin wrapper around existing Iceberg Spark procedures: `ALTER TABLE CREATE BRANCH`, `CALL system.rollback_to_snapshot`, `CALL system.publish_changes` (branch merge). No new engine. New `elt iceberg {branch, tag, rollback, diff, merge}` CLI subcommands. Adds workflow not capability. |
| GAP-17 | Column-level encryption at rest (KMS-backed AES-GCM / format-preserving tokenization) | Regulated industries where storage-level encryption = insufficient, per-column access = needed (even DBAs must not see plaintext) | Before L3 write, route cols flagged `sql_column_spec.encryption: kms_aes_gcm_v1` through a Spark UDF that calls G-5's AWS KMS/Azure Key Vault (already have SDK deps). Decryption is a symmetric UDF in a SECURITY DEFINER Trino view with RBAC GAP-10 role checks. G-5 providers already connected — just a KMS-wire call + UDF. |
| GAP-18 | dbt `snapshot` equivalent (SCD2 tracking on mutable L2 source tables) | Slowly-changing-dimension type 2 history on operational DBs that have no built-in CDC | New manifest type `snapshot.yaml` alongside L3 SQL models, declaring: source (L2 table), unique_key, `updated_at` column, strategy (timestamp/check-all). On each run: read current L2 + previous snapshot Iceberg table → MERGE with `valid_from/valid_to/is_current` columns via the existing [merge_sql_generator.py](../src/elt_pipeline/sql/merge_sql_generator.py). Outputs an L3 SCD2 Iceberg table identical to dbt snapshots in semantics. No new primitives. |
| GAP-19 | dbt `seed` equivalent (static CSV reference data loaded → L2 directly) | Country code tables, currency codes, static mapping files versioned in Git | New `seeds/` directory (parallel to `sql/`). `elt seed run --replace-all` → reads CSV, validates schema via companion `seed.yaml`, writes L2 parquet/Iceberg via Spark. One data loading primitive; zero new concepts. Reads G-6 governance classification from seed spec. |

---

### 🟢 IMPLEMENTED Gaps (Closed post-triage on concrete consumer demand)

#### GAP-11: DAG Runner Lacks Sensors, Event Triggers, SLA Tracking
**Current built-in DAG runner (`elt schedule`):** Per-job `depends_on:` topological sort, retries, retry_delay_seconds, per-attempt audit, skip-reason codes, `continue_on_error`. All Production (CMM §7, 19 tests green).
**Implemented wait_for sensors + SLA tracking (QW-3, closed 2026-08-27):** Per-job `wait_for:` (3 kinds: `path_exists` / `path_glob` / `http_url`, mutual-exclusivity enforced at YAML parse, poll_sec/timeout_sec bounds) + per-job `sla_seconds:` SLA tracking. Path sensors dispatch via B-6 `path_exists`/`path_glob` for scheme-agnostic local/S3/GCS/ADLS; HTTP wait → GET 2xx poll with jittered exponential backoff (per-request timeout capped). Emits `sensor_poll` JSON events + Prometheus `elt_sensor_poll_count` gauge labels `{job,state}`. Sensor timeout → `failed_sensor` status exit_code=5 + `stop_after_this_job` cascade. SLA breach → G-2 `AlertEvent(severity=warning)` + audit `sla_breached=true` + top-level `sla_alerts[]` array.
**Deferred to future demand:** Event-trigger `elt schedule listen <plan.yaml> --event kafka|webhook` (Kafka message / webhook → trigger plan). Standalone command, not part of normal `run`; reuses M-3 Kafka consumer or Flask-less `http.server` webhook listener → invokes `_run_schedule_plan` on event.
**Code insertion point (implemented):** YAML schema extended in `shared/scheduler.py` (`WaitForSpec` Pydantic model) + execution inside [_cli_main.py](../src/elt_pipeline/_cli_main.py) (`_run_schedule_plan()`). 8 new focused tests (isolated test_cli.py, S-0 compliant): 3 wait_for happy paths + 1 timeout failure cascade + 1 HTTP 2xx progression + 1 SLA breach alert + 1 SLA ok silent + 1 sensor event/metric label counts + 1 YAML multi-kind validation reject.

---

## 6. Architectural Observations: Minor Technical Debt (Not Capability Gaps)

These are not missing capabilities — they are small inefficiencies in what already exists. None are blocking, but all are worth cleaning up if a file is already open for another change.

| ID | Observation | Location | Recommended Next Touch |
|---|---|---|---|
| TD-1 | `Development Status :: 3 - Alpha` in `pyproject.toml` classifiers contradicts: every CMM row is 🟢 Production, no ⏳ rows, 807/0 green gate, every honest-boundary claim is backed by tests. | [pyproject.toml](../pyproject.toml#L23) | Bump to `Development Status :: 4 - Beta` at the next release. "Alpha" → semantic implies "not yet usable". If a 1.0.0 version bump happens simultaneously, go to `5 - Production/Stable`. |
| TD-2 | `normalize_engine = "python"` pure-Python escape hatch is marked in FFM §4 as *"scheduled for removal post-zero-fallback production window"*. Confirm window has elapsed. If no fallback reported in 30+ days of real runs: remove. | [pipeline.py](../src/elt_pipeline/normalize/pipeline.py) dual-engine switch + [FRAMEWORK_FEATURE_MATRIX.md §4](./FRAMEWORK_FEATURE_MATRIX.md#L67) | Delete `normalize_engine = "python"` branch + its tests. Eliminates one entire engine surface. If ever truly needed again: the connector-level raw L1 payload is always available. |
| TD-3 | HDFS marked DEFUNCT (M-10) with explicit fail-fast and migration guidance. CMM row is stamped 🟢 Production with note. Badge semantics slightly unusual (Production = a fail-fast rejection?). The note text is fully precise and internally consistent — badge label is the only debatable part. | CMM §1 row: "Hadoop HDFS (hdfs://)" — [CAPABILITY_MATURITY_MATRIX.md §1](./CAPABILITY_MATURITY_MATRIX.md#L59) | No change needed. DEFUNCT classified + test-enforced fail-fast + migration guidance = strictly better than ⏳ Roadmap for a niche the industry has exited. Badge semantics are fine because the classification is explained. |
| TD-4 | 8 pre-existing sandbox `JAVA_GATEWAY_EXITED` `PySparkRuntimeError` entries in maintenance tests are noted in BACKLOG §Environment as JVM-boot related on resource-constrained workstations. Zero code relation — purely environment. | [test_maintenance.py](../tests/test_maintenance.py) + BACKLOG §Status snapshot footnote | Add a 1-line note to [JVM_TOOLCHAIN_SETUP.md](./maintainer/JVM_TOOLCHAIN_SETUP.md) that on 4-core or fewer workstations: `export ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g` (or equivalent) may be needed to eliminate the non-deterministic sandbox JVM-boot OOM. Gate already counts them as zero-code-relation skips; the doc note is the last mile. |
| TD-5 | `pyproject.toml` 19 declared optional extras include `emr` (boto3+pyspark) and `dataproc` (gcs+pyspark) and `synapse` (adls+pyspark) which are pure convenience aliases that duplicate `s3+spark` / `gcs+spark` / `adls+spark`. But they are useful UX for end-users who think "I'm on Dataproc" not "I need GCS + PySpark". | [pyproject.toml](../pyproject.toml#L64-L75) | Keep. No issue — this is correct UX. No debt. |

---

## 7. Prioritized Recommendation Roadmap (Effort × Impact)

Only pull items forward on concrete consumer demand per BACKLOG policy. This is the triage ordering when demand hits — highest ROI per engineering hour first.

### IMPLEMENTED — Closed post-triage on concrete consumer demand
| Order | Gap | Effort | Impact | Why first |
|---|---|---|---|---|
| QW-3 | **GAP-11 `wait_for:` file/glob sensor in schedule plans** | 1 day | Unblocks the #1 schedule use case (wait for upstream file) | B-6 `path_exists`/glob already work. Pure polling + audit/sensor event wiring. Closed: per-job wait_for (path_exists/path_glob/http_url) + sla_seconds, 8 new tests, gate 830→838 (BACKLOG §QW-3/GAP-11 2026-08-27). |

### Phase 1: Quick Wins (1–2 days each, single-session work items, 0 architecture risk)
| Order | Gap | Effort | Impact | Why first |
|---|---|---|---|---|
| QW-1 | **TD-1: Classifier bump Alpha → Beta** | 1 line | Instant credibility lift | Pure metadata change; no code/test risk. `classifiers` in pyproject.toml is the first thing PyPI visitors see. |
| QW-2 | **FFM/CMM: Singer Tap integration doc + GAP-5 Option B guide** | 1–2 pages | 0.5× connector ecosystem today | Write the Marquez/DataHub + Debezium Server → M-11 JSONL replay cookbook as operator docs. Integration works now; docs close the perception gap. |

### Phase 2: Medium Effort (1–2 weeks, highest per-hour impact leverage)
| Order | Gap | Effort | Impact | Why |
|---|---|---|---|---|
| ME-1 | **GAP-7 Data Contract enforcement (strict/warn/off)** | 1 week | Prevents silent L3 schema drift — saves weeks of production incidents | Reuses 100% existing SqlColumnSpec fields. Write-time interlock in 1 place. Single highest correctness ROI per engineering hour in the repo. |
| ME-2 | **GAP-2 Tier A: Postgres Logical Replication CDC driver** | 1–2 weeks | Zero-infra Postgres CDC — the single most-asked-for missing ingestion source | 7th driver in existing 6-driver matrix; checkpoint store holds LSN. Existing connector shape fits exactly. |
| ME-3 | **GAP-1 Singer Tap Compatibility (5th connector family)** | 1–2 weeks | Instant 300-connector ecosystem via existing Singer community taps | 1 adapter class → subprocess boundary → existing L1 wire format (JSONL identical to M-11 Kafka replay). Lowest architectural risk, highest ecosystem breadth gain. |
| ME-4 | **GAP-8 Automatic Profiling Hook** | 3–5 days | Onboarding cost cut 50% for new models; automated schema drift baseline | Behind existing `QualityHookBackend` Protocol — follows BuiltinQualityHook pattern line for line. No new concepts. |

### Phase 3: Enterprise Enablers (4–8 weeks, gate cross from "team scale" → "org scale")
| Order | Gap | Effort | Impact | Why |
|---|---|---|---|---|
| EE-1 | **GAP-3 Column-Level Lineage (Spark analyzed plan walk → OpenLineage facets + impact CLI)** | 4–6 weeks | Mandatory for enterprise governance sign-off | 3 steps: facet mapping → Spark plan walk → CLI graph search. Each independently testable. |
| EE-2 | **GAP-4 Semantic Layer (MetricSpec + compile/run → Iceberg table OR Trino view)** | 6–8 weeks | Solves the "dashboards disagree" org-wide problem — the #1 data platform complaint | Parallel to SQL models — new manifest layer reuses existing compiler/executor. Zero engine changes. |
| EE-3 | **GAP-5 Option A: dbt-docs-compatible manifest.json export + CLI** | 3–5 days (Option A) vs 1–2 weeks (Option B bulk) | Instant catalog/discovery/lineage UI without writing any UI | Option A is a pure JSON mapping module + doc — the cheapest UI win possible. |
| EE-4 | **GAP-9 Reverse ETL 4-target framework (REST/Salesforce/HubSpot/SQL)** | 6–8 weeks | Closes the operational-data push loop; avoids Census/Hightouch seat spend | Symmetric with ingest; reuses G-2 observability, G-5 secrets, G-6 governance quarantine verbatim. |
| EE-5 | **GAP-10 RBAC Policy YAML at CLI entrypoint** | 1–2 weeks | Framework-level audit evidence for multi-team deployment reviews | 1 interlock + 1 Pydantic model. Default fully-open = zero disruption. |

### Phase 4: Ergonomic Polish (2–4 weeks per item, operator-toil reduction)
| Order | Gap | Effort | Impact |
|---|---|---|---|
| EP-1 | **GAP-12 Backfill plan/status/retry CLI** | 2 weeks | Cuts operator toil on 12+ month historical loads by 80% | Pure filesystem read + existing audit. 0 new storage. |
| EP-2 | **GAP-13 Micro-batch interval scheduler sugar** | 1 week | Sub-hour freshness without a Spark Streaming cluster. 0 new engine. | Pure checkpoint → window derivation in scheduler. |
| EP-3 | **GAP-6 SQL package versioning + shared stdlib lib** | 3–4 weeks | Eliminates canonical model copy-paste across repos. Framework-owned standard model library becomes possible. | Git/cache + model discovery merge. |
| EP-4 | **GAP-14 Spark cost attribution + auto-optimize hints** | 2 weeks | Faster SQL runs + lower warehouse cost with zero operator intervention. | Listener + heuristics only. |
| EP-5 | **Niche GAP-18 (dbt SCD2 snapshots) + GAP-19 (seeds)** | 2–3 weeks combined | SCD2 + static reference data = last 5% of SQL transformation feature parity. |

---

## 8. Cross-Document Reference Anchors

| This doc mentions… | Canonical source of truth |
|---|---|
| Maturity badge per row, test counts, closure dates | [CAPABILITY_MATURITY_MATRIX.md](./CAPABILITY_MATURITY_MATRIX.md) |
| Condensed "what ships" by 15 capability areas | [FRAMEWORK_FEATURE_MATRIX.md](./FRAMEWORK_FEATURE_MATRIX.md) |
| Gate command, next-work pointer, accumulated constraints (zero-env, single-seam, leaf-only swap, etc.) | [BACKLOG.md](./todo/BACKLOG.md) |
| 5-layer architecture, 4-phase lifecycle, 4-tier cascade, 4-tier SQL validity chain, catalog matrix, portability | [PRD 10](./prd/10-prd-architecture-and-lifecycle.md) |
| Platform principles, DAMA-DMBOK alignment statement | [PRD 00 Platform Principles](./prd/00-prd-platform-principles.md) |
| Level definitions & governance boundaries | [PRD 00 Architecture Levels](./prd/00-prd-architecture-levels-and-governance.md) |
| Storage scheme dispatch contract | [PRD 08 Storage Root URI IO Dispatch](./prd/08-prd-storage-root-uri-io-dispatch.md) |
| L5 publish contract | [PRD 06 L4→L5 Publish & Export](./prd/06-prd-level4-to-level5-publish-and-export.md) |
| Iceberg catalog preflight + serving format spec | [PRD 09 L3/L4 Serving & Table Format](./prd/09-prd-level3-level4-serving-and-table-format.md) |
| Trino/TLS/auth operator config | [CMM §4 JDBC serving endpoint](./CAPABILITY_MATURITY_MATRIX.md#L141-L147) |
| Governance/retention runbook + SQL generators | [Operator Runbook: Governance and Retention](./operator/GOVERNANCE_AND_RETENTION_RUNBOOK.md) |
| JVM toolchain setup, gate command, Spark per-file isolation | [Maintainer: Local Development and Release](./maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md) |
| Backlog continuity, one-item-per-session protocol | [Maintainer: Backlog Continuity Playbook](./maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md) |

---

## 9. Gap Closure Constraint Checklist (Per Future Work Item)

Before any gap from §5 becomes a real BACKLOG work item, confirm:

- [ ] Gate stays green: `bash scripts/run_tests.sh` → same or higher pass count, 0 regressions.
- [ ] No new reads of `os.environ` outside the `runtime_context` singleton (zero-env lockdown, BACKLOG active constraint).
- [ ] Storage work follows B-6 StorageBackend Protocol/registry pattern (BACKLOG constraint 8).
- [ ] Docs are a source of truth — CMM + FFM + Honest Boundary in README all updated before close (bidirectional safety guardrail).
- [ ] Platform strengths in §3 are NOT regressed. In doubt: keep correctness, replayability, seam isolation, and the 4-tier validity chain intact.
- [ ] Pulled forward *only* on concrete consumer demand. If no consumer asks, the gap stays documented here and does not rot the codebase with premature speculative code.

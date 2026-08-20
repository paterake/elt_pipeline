# Backlog & Continuity — Publication Readiness & Platinum Hardening

<!--
  ANCHOR DOC. Durable, cold-start-resumable state for the portability / publication-readiness
  effort. Lives at repo root, NOT under docs/ (PRD 10 §11). Method + section contract:
  docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md. Operating model: ONE session per item;
  update the Resume line + Status snapshot before closing a session.
-->

## Resume (start here)

- **TRANCHE 1 COMPLETE (publication-readiness gate, 2026-08-19):** D-0 ✅ → D-1 ✅ → I-1 ✅ (doc pass
  only) → **D-2 ✅**. The repo is publication-ready with honest scope: README prominently links the
  [Capability Maturity Matrix](docs/CAPABILITY_MATURITY_MATRIX.md) which classifies every feature as
  🟢 Production / 🟠 Demo / ⏳ Roadmap. Cross-doc claims match the code.
- **TRANCHE 2 — G-1 CLOSED (2026-08-19, first on-demand pull):** Iceberg table maintenance (compaction,
  snapshot expiry, orphan cleanup + optional manifest rewrite) delivered as `elt maintain run …` CLI
  with a `elt_pipeline.maintenance` module. 14 new tests all pass, maturity matrix §5 flipped to 🟢
  Production. Tranche 2 continues on-demand.
- **TRANCHE 2 — B-6 CLOSED (2026-08-26, second on-demand pull, RECOMMENDED strategy B3):**
  Pluggable `StorageBackend` facade delivered end-to-end: `StorageBackend` Protocol (18 leaf IO ops
  + `staging_swap_atomic`), `LocalBackend` + `S3Backend` extractions, `_BACKEND_REGISTRY` singleton,
  `path_utils` one-line dispatcher refactor with lazy circular-import guard, `_staging_swap.py`
  reduced to a 99-line backward-compat shim. All 12 existing callers, all existing tests, and all
  test monkeypatches work identically. Zero-regression pure refactor: 311/0 full gate green,
  80/80 path_utils+staging_swap focused tests green, ruff clean. Backward-compat shims preserved
  for `pu._S3_CLIENT`, `pu._s3_client()`, `pu._split_s3_path()`, `_swap_mod._s3_client`.
  Capability Maturity Matrix §1 flipped — seam itself is now 🟢 Production; GCS/ADLS/wasbs/dbfs/hdfs
  individual backend rows remain ⏳ but closure is additive-only. PRD 08 §P2 and §Anti-scope updated
  to endorse the facade pattern (old "no StorageBackend protocol/registry" prohibition withdrawn,
  replaced with canonical B3 pattern docs; dynamic plugin auto-discovery remains explicitly out
  of scope).
- **TRANCHE 2 — G-5 CLOSED (2026-08-19, third on-demand pull, 🔴 HIGH secret-cred
  unblocker for B-4 cloud story):** Real secrets backend delivered end-to-end:
  `SecretsProvider` @runtime_checkable Protocol + `_PROVIDER_REGISTRY` keyed by `SecretScheme`
  enum (env/file/aws/azure/gcp/vault), `SecretValue` redacting str subclass,
  `EnvVarSecrets` (production, zero-deps, env-read-at-call-time for CI injection) and
  `FileSecrets` (production, zero-deps, abs/rel paths, single-trailing-newline-only strip) as
  default concrete providers, roadmap schemes (aws/azure/gcp/vault) registered as fail-fast
  stubs raising `SecretsNotImplementedError` with clear roadmap message.
  `RestConnectorBase.resolve_secret()` rewritten to dispatch through `resolve_secret_ref()`
  with strict=False, preserving 100% backward compatibility — existing plain-ref configs
  like `"ORDERS_API_TOKEN"` continue to work (implicit env:// + pass-through fallback on miss
  = old stub behaviour). 47 new tests all pass; full gate 372/0 green. Unblocks **B-4**
  (Spark cloud FS credential story).
- **TRANCHE 2 — B-4 CLOSED (2026-08-26, fourth on-demand pull, 🔴 HIGH Spark cloud FS
  config + credential story, now unblocks B-1 + B-2 additive-only):** Complete
  `spark.hadoop.fs.*` config surface for S3 (s3a://), GCS (gs://), and ADLS Gen2 (abfss://)
  wired end-to-end through the standard 4-tier runtime_context cascade. 13 new env vars in
  EnvVarNames; materialized as a `spark_fs:` nested dict (flat dotted keys also accessible
  via `runtime_context.get("spark_fs.s3_region")` and included in `as_runtime_overrides()`).
  Credential values are `secret_ref` URIs resolved at Spark build time with `strict=True`
  via the G-5 subsystem (fail-fast on explicitly-configured but unresolvable refs; empty
  refs → Spark's native default credential chain — ambient IAM on EMR/workload identity /
  DefaultAzureCredential / ADC / ~/.aws/credentials — preserving zero-ambient-config
  deployments). Public pure-unit-testable API `build_spark_fs_hadoop_configs()` returns
  flat Spark `spark.hadoop.fs.*` keys with no JVM or PySpark import dependency. Validation
  modes: S3 ak+sk pair required together-or-neither (SPARK_FS_S3_CRED_MISMATCH), ADLS
  account_name required for any config (SPARK_FS_ADLS_ACCOUNT_REQUIRED), ADLS Service
  Principal auth requires tenant_id+client_id+client_secret all together
  (SPARK_FS_ADLS_SP_INCOMPLETE). Auth-mode priority: S3 (ak+sk → default-chain); ADLS
  (shared_key → Service-Principal-OAuth → MSI-MsiTokenProvider → default-chain). GCS SA
  keyfile uses a dedicated `_resolve_path_ref` helper: `file:///path` passes the path
  verbatim (Spark's JVM side reads the JSON file directly), `env://VAR` treats the env var
  value as a filesystem path; cloud-secret-manager schemes rejected with clear guidance.
  27 new tests in tests/test_spark_fs_config.py (S3:10, GCS:3, ADLS:7, cascade:4,
  build_spark_session integration:4). Full gate 404/404 green (baseline 372 + 27 + 5 others
  that closed since last stamp), `uv run ruff check src/ tests/` clean. **B-1 (GCS gs://)
  and B-2 (ADLS abfss://) are now additive-only follow-ups** — only the `StorageBackend`
  control-plane class needs adding; all Spark Hadoop FS data-plane config + credential
  wiring is already Production-complete.
- **Tranche 2 = on-demand roadmap, do NOT start without an explicit pull-forward:** next likely pulls
  (if none of these apply, just `from BACKLOG.md, continue` lists the 🔴 options each time):
  - `g-2` — observability / metrics + tracing export (🔴 HIGH)
  - `B-1` — GCS `gs://` backend via B-6 facade (🟠 MED, additive-only; Spark-side Hadoop FS config + credentials ALREADY DONE by B-4)
  - `B-2` — Azure ADLS `abfss://` backend via B-6 facade (🟠 MED, additive-only; Spark-side Hadoop FS config + credentials ALREADY DONE by B-4)
  - `g-3` — orchestration integration (🟠 MED)
  - (all other B-*/G-1-impl/G-4…/M-1 tranche-2 items)
  Each item ⏳, worked one-per-session when a consumer needs it; every close must also update the matching
  row in the Capability Maturity Matrix with the date + BACKLOG ref.
- **Read `## Platform strengths` before touching anything — protect that list.**
- **Framing:** the test gate is already 🟢 green; these are **capability/accuracy gaps between
  the documented claims and the implemented v1**, not regressions. "Done" for this backlog =
  the public-facing claims match what the code actually does, proven by tests.

## Session start prompt

Paste verbatim to boot a cold session warm:

> `from BACKLOG.md, continue`

The session reads the **Resume (start here)** line for the next item, and **Environment &
Verification** for the JDK exports and the gate command before running anything. (If the cold
tool doesn't auto-load `CLAUDE.md`, prepend `Read BACKLOG.md at the repo root, then …`.)

## Status snapshot

- **Gate:** 🟢 GREEN. `bash scripts/run_tests.sh` → TEST GATE: PASS (404 / 0 failed);
  `uv run ruff check src/ tests/` clean. This backlog does **not** start from a red gate — keep it green.
- **Captured:** 2026-08-26 (re-stamped after B-4 closure). Origin: a portability +
  platinum review. Storage IO implements **`s3://` + local `file://` only** (D-1 closed); ingest
  surface explicitly documented across README + PRD 01/04 (I-1 doc pass closed: REST production,
  object_storage local+S3 production, SQL sqlite-only demo, Kafka JSONL-replay demo; JDBC+real Kafka
  marked roadmap); operational surface (Iceberg maintenance, observability, orchestration,
  deployment, secrets, governance, OpenLineage, DQ quarantine) is bronze→silver; **D-2 closed**
  (Capability Maturity Matrix at `docs/CAPABILITY_MATURITY_MATRIX.md` classifies every feature as
  🟢/🟠/⏳ and is linked prominently from README top). **G-1 CLOSED**: Iceberg table maintenance
  shipped via `elt maintain run …` + `src/elt_pipeline/maintenance/` module + 14 new real Iceberg
  tests + maturity matrix §5 flipped to 🟢 Production.
  **B-6 CLOSED (strategy B3 — pluggable StorageBackend facade)**: New module
  `src/elt_pipeline/shared/storage_backends/` exposes a `StorageBackend` runtime-checkable Protocol
  (18 leaf IO ops + staging_swap_atomic), `LocalBackend` + `S3Backend` extracted classes,
  `_BACKEND_REGISTRY` singleton keyed by `StorageScheme` enum, `get_backend(path)` +
  `register_backend(scheme, backend)` accessors. `path_utils` functions are now one-line
  dispatchers with lazy circular-import resolution (scheme primitives at file top; `_get_backend()`
  imports `storage_backends` lazily inside each leaf function). `_staging_swap.py` reduced to a
  99-line backward-compat shim (preserves the unused `scheme:` kwarg on `atomic_swap` and the
  unused `scheme` parameter on `best_effort_delete_staging` so 2 existing test modules' fixtures
  and callsites don't break). PRD 08 §P2 rewritten to document the B3 canonical pattern (old
  "no StorageBackend protocol/registry" rule reversed); §Anti-scope plugin rule refined to
  forbid only dynamic auto-discovery (static in-code registration via registry or explicit
  `register_backend` call is the supported pattern). Backward-compat shims on `path_utils.py`:
  `_S3_CLIENT`, `_s3_client()`, `_split_s3_path()`. Shims on `_staging_swap.py`:
  `_s3_client = path_utils._s3_client`, `_S3_CLIENT = None`. Zero-regression pure-refactor:
  311/0 full gate green on first run post-rewrite, 80/80 path_utils+staging_swap focused tests
  green, `uv run ruff check .` clean.
  **G-5 CLOSED (🔴 HIGH, unblocks B-4 cloud FS story):** Real secrets backend subsystem
  shipped via new `src/elt_pipeline/shared/secrets.py` module: `SecretScheme` enum (6 schemes:
  env/file/aws_secretsmanager/azure_keyvault/gcp_secretmanager/vault), `parse_secret_ref()`
  URI parser (no `scheme://` → defaults to env for backward compat), `SecretsProvider`
  @runtime_checkable Protocol + `_PROVIDER_REGISTRY` singleton + `register_provider()` /
  `get_provider()` public API (same shape as B-6 storage_backends), `SecretValue` redacting
  str subclass + `redact_secret()` utility, default concrete providers `EnvVarSecrets`
  (zero-deps, reads at resolve-time for CI env-injection) + `FileSecrets` (zero-deps,
  abs/rel paths, single-trailing-newline-only strip), roadmap providers as registered
  fail-fast stubs raising `SecretsNotImplementedError` with clear roadmap message.
  `RestConnectorBase.resolve_secret()` in rest.py rewritten to dispatch through
  `resolve_secret_ref(secret_ref, strict=False)` — strict=False preserves the old
  pass-through stub behaviour on env-miss so 100% of existing configs and test fixtures
  continue to work without modification. 47 new tests in `tests/test_secrets.py` cover:
  SecretValue redaction (6), parse_secret_ref syntax (10), EnvVarSecrets (4), FileSecrets
  (6), roadmap stubs (4 parametrized), registry contract (4), dispatcher (6), batch resolver
  (3), rest-connector integration (2). Full gate 372/0 green, ruff clean. Maturity Matrix §9
  flipped: `secret_refs+redaction` / env-resolver / file-resolver / `SecretsProvider` seam /
  `SecretValue` utility → all 🟢 Production with G-5 cross-refs; cloud SMs + Vault remain ⏳
  Roadmap but now have scheme-registered stubs + additive-only closure notes.
  **D-0 decided: Path A (publish honestly) now; B + G-* are roadmap.**
  **D-2 closed → TRANCHE 1 (publication-readiness) COMPLETE.**
  **G-1 closed → first TRANCHE 2 item done.**
  **B-6 closed → second TRANCHE 2 item done (force-multiplier, additive-only closure path
  for B-1/B-2/B-3/B-4/B-5).**
  **G-5 closed → third TRANCHE 2 item done (force-multiplier, additive-only closure of
  cloud SMs + vault + B-4 cloud FS credential story).**
  **B-4 closed → fourth TRANCHE 2 item done (force-multiplier, additive-only unblock
  for B-1/GCS and B-2/ADLS Spark data-plane writes). Spark Hadoop FS cloud config
  (spark.hadoop.fs.s3a.* / fs.gs.* / fs.azure.*) is now a full framework-level surface
  with 4-tier cascade + G-5 strict secret_ref resolution + ambient-default-identity
  fallback. 13 env vars, 27 tests, build_spark_fs_hadoop_configs() public pure API,
  3 PipelineError validation codes, dedicated _resolve_path_ref for GCS SA keyfile
  paths. B-1 and B-2 now require only a StorageBackend control-plane class each.**
  **Active: Tranche 2 idle (on-demand only — pull forward one per session when needed).**
- **Placement:** repo root, not `docs/` (PRD 10 §11).

## Environment & Verification (run this first, every session)

Spark/Iceberg tests need Temurin 23 on `PATH`/`JAVA_HOME`; a bare non-interactive shell does not
inherit mise's activation. Export first, then run the gate:

```bash
export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/run_tests.sh          # the gate — per-file Spark isolation; must stay green
```

Per-item verification commands are inside each item. "Should pass" is not a check — run it and
paste the count. Any new storage-backend work MUST ship with tests (see B-5) and keep the gate green.

## Root-cause summary

The architecture is sound and the **AWS-S3 + local** path is real and tested (with a mocked S3).
The gap is **claim vs implementation**, in two layers:

| Layer | Claimed (PRD 10 §6) | Implemented |
|---|---|---|
| Storage URI schemes | `file://`, `s3a://`, `gs://`, `abfss://`, `wasbs://`, `dbfs://` | `s3://` + local `file://`/POSIX **only** ([path_utils.py](src/elt_pipeline/shared/path_utils.py)); others **hard-rejected** (by design in [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md)) |
| Cloud "0 LOC" | AWS + GCP + Azure + Databricks | AWS S3 only (data IO); Azure/GCP/Databricks object stores unreachable |
| Cloud FS credentials/config | implied turnkey | framework sets **no** `spark.hadoop.fs.*` or creds; relies on ambient EMR/IAM |
| Cloud integration tests | — | none; S3 unit-tested with a fake boto3; Azure/GCS zero coverage |
| **Ingest → L1 write** | multi-cloud | **pre-Spark Python** via `path_utils` → s3+file only (see B-6, I-1) |
| **DB ingest** | (implied JDBC) | **sqlite only** — no JDBC / no Postgres/MySQL/etc. (I-1) |
| **Kafka ingest** | (implied broker) | **local JSONL file replay**, not a real broker (I-1) |
| **Connector base classes** | — | `rest/sql/kafka/object_storage` are **abstract**; only `local_*` concretes exist (I-1) |

## Platform strengths (verified good — preserve, never regress)

These are the parts that are genuinely strong. Every item below must **keep** them intact; do not
trade any of these away for a gap fix. When in doubt, protect this list.

- **4-tier SQL validity chain** — token → partition → `EXPLAIN FORMATTED` → quality hooks
  ([sql/](src/elt_pipeline/sql/)). A dbt-like compile-time guardrail most homegrown platforms lack;
  it catches bad models before they write. Keep it on the write path.
- **Replayability / idempotency** — run_id lineage stamping + dynamic partition overwrite
  (the S-2 leaf-partition-only replace). Re-running a window reproduces the same output with zero
  blast radius. Do not weaken the partition-overwrite scoping.
- **Clean, production-shaped seams** — DQ and lineage are pluggable adapters with blocking /
  non-blocking policy ([integrations/](src/elt_pipeline/integrations/)); connectors, catalogs, and
  storage are abstractions, not hardcoding. The platinum gaps below are mostly "implement the
  concrete behind an existing seam," not "build the seam."
- **Config-cascade + catalog-binding model** — one 4-tier cascade (arg > ENV > YAML > manifest),
  separate writer/serving Iceberg catalogs, six-way catalog enum. Coherent and well-tested.
- **Design discipline** — thorough canonical PRDs, a green per-file test gate (311/0), and an
  end-to-end example that actually runs. The foundation is above-average; the gaps are additive.

## Accumulated Active Constraints (honour in every item; append, never delete)

1. **Keep the gate green.** `bash scripts/run_tests.sh` must stay 311+/0 after every item; new
   backends add tests, never regress existing ones.
2. **Honour the PRD 08 single-seam dispatch pattern.** Scheme prefix is the *single* routing key
   (`if path.startswith("gs://")` …); no URL-parsing library, no `pathlib.Path` wrapping of root
   URIs, no `StorageBackend` plugin protocol. New schemes extend the same seam the S3 path uses.
   **SUPERSEDED by constraint 8 (B-6 closed): PRD 08 §P2 updated to endorse the `StorageBackend`
   Protocol/registry facade as the canonical pattern (strategy B3). Single-scheme routing key is
   preserved; the prohibition on a `StorageBackend` Protocol/registry is withdrawn. Dynamic plugin
   auto-discovery (setuptools entrypoints / importlib.metadata plugins / pkg_resources) remains out
   of scope (see constraint 8).**
2b. **The S3 path is the reference implementation.** Any new backend mirrors the S3 branch in
   *every* scheme-branching function in [path_utils.py](src/elt_pipeline/shared/path_utils.py)
   (there are ~18: `detect_scheme`, `join_paths`, `path_parent`, `path_basename`,
   `path_with_suffix`, `path_normalize`, `path_exists`, `path_is_dir`, `path_mkdir`,
   `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`, `path_read_bytes`,
   `path_write_bytes`, `path_open_for_append`, `path_replace`, `path_delete_tree`) — plus the
   staging-swap paths in [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py). Miss one
   and the backend silently half-works.
   **SUPERSEDED by constraint 8 (B-6 closed): `path_utils` is now one-line dispatchers (no
   per-scheme branches to mirror). Reference is now `S3Backend` class in
   [storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py) — the
   same ~18 leaf methods live as Protocol methods on `StorageBackend` plus one
   `staging_swap_atomic` method; each new backend implements the Protocol once (1 class, not
   ~18 scattered branches) and registers via `register_backend()` or the default
   `_BACKEND_REGISTRY` singleton.**
3. **Never silently coerce schemes** (PRD 08): an unsupported scheme still fails fast and sharp.
4. **Docs are a source of truth, not marketing.** A public claim the code can't back is a defect.
   Every doc edit must leave PRD 08 and PRD 10 mutually consistent.
5. **Per item, decide the direction explicitly and record it in the Done line.**
6. **Preserve the Platform strengths** (section above). A gap fix that regresses the validity
   chain, replayability, or an existing seam is not done — it's a trade-down.
7. **Implement behind existing seams where they exist.** DQ/lineage adapters, `secret_refs`,
   connector/catalog abstractions are already there — extend them, don't fork parallel paths.
8. **B-6 canonical facade pattern (supersedes constraints 2 + 2b for any new storage work).**
   - Single routing key preserved: scheme prefix detected via `detect_scheme(path)` over
     `StorageScheme` enum (lightweight startswith/prefix parse; no third-party URI parser,
     no `urllib.parse`, no netloc/path split).
   - Every new scheme ships as: (a) a new `StorageScheme` enum entry + `_SUPPORTED_SCHEME_PREFIXES`
     entry, (b) a new backend class implementing the full `StorageBackend` runtime-checkable
     Protocol (18 leaf IO ops + 1 `staging_swap_atomic` method preserving the S-2 leaf-partition-
     only replace guarantee), (c) registration in the default `_BACKEND_REGISTRY` singleton at
     module load time in `storage_backends/__init__.py` *or* an explicit `register_backend()` call
     at a known bootstrap point. No changes permitted to `path_utils.py` public functions — they
     remain thin one-line dispatchers.
   - `_staging_swap.py` remains a backward-compat shim only. All swap semantics live as
     `backend.staging_swap_atomic(*, staging_path, target_path, mode: SwapMode)` on the backend
     class; POSIX uses `shutil`/`os` leaf recursion; object stores use copy→delete with partition
     subprefix inference to guarantee leaf-partition-only replace with zero sibling-partition blast
     radius.
   - Staging-swap `SwapMode` literal is defined in `storage_backends` (re-exported through
     `_staging_swap.py`). Circular-import guard: scheme primitives must live at the top of
     `path_utils.py` *above* any lazy `from storage_backends import get_backend` call; backend
     classes import those primitives from the scheme section.
   - Test monkeypatch surface preserved for fixture backward compat: `path_utils._S3_CLIENT`,
     `path_utils._s3_client()`, `path_utils._split_s3_path()` are module attributes on
     `path_utils`; backend client resolution routes through `path_utils._s3_client()` via lazy
     import so `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts all backend
     client access.
   - **Out of scope**: dynamic plugin auto-discovery via setuptools entrypoints, `sys.path`
     scanning, `importlib.metadata` plugins, `pkg_resources.iter_entry_points`, any HTTP/ConfigMap
     scheme discovery, reflection over third-party packages. Registration is static in-code only.

---

## Work items

### Still Todo

#### D-0 — Portability direction: reconcile-docs (A) vs implement-multi-cloud (B)  ✅ DECIDED (Path A now; B roadmap)
- **DECIDED (owner, 2026-08-18): Path A now — publish honestly with S3+local scope; Path B
  (multi-cloud) is roadmap (tranche 2), and when pulled forward, prefer B-6 (the facade).** The
  analysis below is retained as the reference for that future B work. Active next step: **D-1**.
- **Decision (as taken):** the multi-cloud "0 LOC on AWS/GCP/Azure/Databricks" claim is not
  implemented; rather than block publication on building it, de-scope the docs to reality now and
  keep multi-cloud as a tracked roadmap. Original options, for the B roadmap:
  - **Path A — de-scope to reality (fast, always-safe):** market as *local-first + AWS S3*.
    Only **D-1** is required; B-* are dropped. Publishable within a session.
  - **Path B — implement the claim (real work).** Two strategies, pick one:
    - **B1 — native per-backend clients** (the S3 pattern repeated): add a `gs`/`abfss` branch
      to every scheme-branching function using that cloud's SDK. Items **B-1, B-2, B-3, B-4, B-5**.
      Each backend ≈ the existing S3 path (~300 lines × ~18 functions) + its own staging-swap +
      tests. Keeps PRD 08's "one boolean check per scheme" seam; cost scales linearly per cloud.
    - **B2 — delegate control-plane IO to Spark's Hadoop `FileSystem` via py4j** (item **B-0**):
      refactor the `path_utils` leaf ops +
      staging-swap to call `spark._jvm.org.apache.hadoop.fs.FileSystem`, so the control plane
      inherits whatever Spark's jars support and **the scheme becomes a pure config knob** —
      one implementation covers S3/GCS/ADLS/Databricks. This is the "Spark already handles the
      filesystem" model. **But it is a PRD-level change, not just code** — see B-0's real
      tradeoffs (it overturns PRD 08's explicit "no `StorageBackend` abstraction" principle,
      couples *all* control-plane IO to a live SparkSession, adds py4j round-trip cost to chatty
      list/exists ops, and rename-atomicity still varies per connector). Do **D-1 + B-0 + B-4 +
      B-5**; B-1/B-2/B-3 then largely fold into "add jars + config + tests per cloud."
    - **B3 — pluggable storage-backend facade** (item **B-6**, *the maintainer's proposal; likely the
      best fit here*): define a `StorageBackend` protocol (list/exists/mkdir/delete/rename/read/write)
      and register one implementation per scheme (local/s3/gcs/azure) using each cloud's SDK; the
      `path_utils` functions + staging-swap become thin dispatch over the registry. Like B2 it
      overturns PRD 08's "no registry" rule, **but unlike B2 it keeps IO in Python** — which matters
      because the **ingest phase writes L1 pre-Spark** (see B-6 / I-1). B2 would force a SparkSession
      into ingest just to land raw files; B3 does not. More code than B2 (per-backend SDK impls) but
      cleaner than B1 (each backend is one class, not branches scattered across ~18 functions), and
      independently unit-testable per backend.
  - **Hybrid:** ship Path A now, keep B (B1/B2/B3) as a tracked roadmap for a later release.
- **Recommendation:** **Path A now, B as roadmap — and if/when B is pulled forward, prefer B3
  (B-6, the facade).** Publishing with accurate scope is a one-session change that removes the trust
  risk. For the actual multi-cloud build, the platform is **two-phase — Python ingest (pre-Spark) →
  Spark transform** — and **B3 is the only strategy that serves both phases with one Python
  abstraction and no Spark-in-ingest coupling**. B2 (delegate to Hadoop FS) is elegant for the Spark
  transform side but wrong for ingest (it would drag a SparkSession into raw-file landing); B1 works
  but scales badly (per-backend branches across ~18 functions). B3 gives B2's "one implementation,
  scheme-is-config" benefit while keeping the pre-Spark ingest path native.
- **Owner:** maintainer. Record the choice here, then set the Resume line to D-1.
- **Verification:** none (decision only). Note the choice in this item's Done line.

#### D-1 — Make PRD 08 and PRD 10 mutually consistent (needed in BOTH paths)  ✅ Done (2026-08-18)
- **Symptom:** [PRD 10 §6](docs/prd/10-prd-architecture-and-lifecycle.md) (lines ~31, 245, 251-254)
  claimed 6 URI schemes and "runs identically on AWS/GCP/Azure/Databricks/Polaris, 0 LOC";
  [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md) scopes v1 to `s3://` + `file://`
  and mandates the other schemes fail fast. The code follows PRD 08. README framed it as
  "local-first … typically parquet for local workflows" (already conservative but lacked an explicit boundary).
- **Decision (Path A, per D-0):** rewrite PRD 10 §Positioning and §6 to state the *implemented* scope
  (S3 + local) and move the multi-cloud storage matrix into an explicit "Roadmap / not-yet-implemented"
  subsection (§6.3). Align the README by adding a new high-level "Current Scope and Capabilities
  (Honest Boundary)" section immediately after DAMA framing (storage backends / ingest mechanisms /
  serving/catalogs / platinum roadmap), and correct the test-gate line (the gate is
  `bash scripts/run_tests.sh`, not a bare `uv run pytest`). PRD 08 required **no changes** — it was already
  consistent with the code (§P2 explicit v1 scope of s3+local, §Anti-scope explicitly excludes GCS/Azure/ADLS/
  DBFS/HDFS, fail-fast L81-87 matches implementation exactly).
- **Files changed:**
  - [docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md)
    — §Positioning L27-L32 reworded (native AWS S3 only + roadmap; §6 split into 6.1 (implemented storage:
    s3/file/bare-POSIX); §6.2 (implemented env table: Workstation + AWS S3/Glue); new §6.3 (Roadmap, all
    5 environments flagged as not implemented, with scope notes per env and explicit "adding each requires ≈18 path_utils
    branches + Spark FS wiring + emulator tests"); closing sentence on catalog-implemented vs storage scheme). Catalog enum kept.
  - [README.md](README.md) — new § "Current Scope and Capabilities (Honest Boundary)" L18-L41
    (storage implemented/roadmap, ingest mechanisms, serving/catalogs implemented, platinum roadmap);
    Install/test-gate section corrected to `bash scripts/run_tests.sh` with JDK exports + per-file
    pytest caveat.
  - PRD 08: unchanged (already consistent with code).
- **Verification:**
  1. **Docs review (grep cross-doc scheme alignment):
     - `_SUPPORTED_SCHEME_PREFIXES` in
     [path_utils.py:22-24](src/elt_pipeline/shared/path_utils.py#L22-L24)
     = `{s3://, file://}`.
     - PRD 08 §P2 L77-79 lists supported for v1 s3:// + file:///bare POSIX — matches ✓
     L81 fail-fast list includes (s3a://, gs://, hdfs://, wasbs://) — unchanged ✓
     - PRD 10 §6.1 implemented storage scheme list = {s3://, file://, bare POSIX} matches prefixes ✓
     §6.2 implemented cloud table = Workstation/AWS S3 only (both shippable) + explicit note that the
     6 catalog types are genuinely implemented but combining them with non-S3 storage schemes = the
     L282 closing sentence). §6.3 roadmap = GCP/Azure/Databricks/Polaris explicitly labelled "not implemented in current release" ✓
     - README Honest Boundary § = storage s3+local implemented; 5 storage roadmap;
     ingest real REST/sqlite-only/sql/Kafka-JSONL-only; G-* platinum items roadmap pointer to PRD 10 §6.3 ✓
     Result: all three docs agree; every "implemented" claim is a subset of `{s3://, file://, bare POSIX};
     all unsupported schemes are in fail-fast lists or roadmap sections only; zero claims as shippable.
  2. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green) — 311 passed / 0 failed.
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  3. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (D-0 Decided Path A; executed as doc-only reconciliation in this session. Next item: I-1 doc pass.

#### B-0 — Delegate control-plane IO to Spark's Hadoop `FileSystem` (Path B, strategy B2 — alternative to B-6)  ⏳
- **Goal:** make the storage scheme a **pure config knob** by routing the Python control plane's
  list/exists/mkdir/delete/rename/read/write ops through Spark's Hadoop `FileSystem` (via py4j:
  `spark._jvm.org.apache.hadoop.fs.{FileSystem,Path}`), so any scheme the cluster's jars support
  (`s3a://`, `gs://`, `abfss://`, `dbfs://`, …) works with **one** implementation instead of one
  per backend.
- **Why this is the strategically right multi-cloud approach:** the data writes are *already*
  Spark (`df.write.parquet`); only the surrounding control plane (path build/validate, partition
  discovery, manifests, staging-swap) is scheme-limited. Delegating that control plane to the same
  Hadoop FS Spark already uses collapses B-1/B-2/B-3 into "add the cloud jars + config + tests."
- **Scope:**
  1. Reimplement the ~18 leaf ops in [path_utils.py](src/elt_pipeline/shared/path_utils.py) to
     dispatch to Hadoop `FileSystem` for any non-local scheme (keep the local `pathlib` fast path).
  2. Reimplement the staging-swap ([sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py))
     on `FileSystem.rename`/`delete` — **but re-examine the atomicity contract**: `FileSystem.rename`
     is atomic on HDFS/POSIX, **not** on S3A/GCS/ABFS (client-side copy+delete). The current hand-
     rolled S3 copy→delete swap exists precisely to control these semantics; preserve the
     leaf-partition-only replace guarantee (S-2) per connector.
  3. Path build/normalize (`join_paths`, `path_normalize`) becomes scheme-agnostic string joins;
     validation moves from an allow-list to "whatever Spark's FS can open."
- **Real tradeoffs to accept before starting (record the decision):**
  - **Overturns [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md)**, which explicitly
    forbids a `StorageBackend`/FileSystem abstraction and mandates "one boolean check per scheme."
    B-0 *is* that abstraction — PRD 08 must be revised, not just the code.
  - **Couples all control-plane IO to a live SparkSession.** Today some IO runs without Spark
    (e.g. L1 object-storage ingest lists/reads via boto3 before any Spark job; CLI config
    validation). Under B-0 those need a JVM/SparkSession, or a dual path. Audit every `path_*`
    caller that runs pre-Spark.
  - **py4j round-trip cost** on chatty ops (`path_rglob` over many partitions, per-manifest
    exists/read). Benchmark against the current native boto3 S3 path; may need batching.
- **Decision needed (owner):** accept the PRD 08 revision + SparkSession coupling? If yes → B-0.
  If no → fall back to strategy B1 (B-1/B-2/B-3 native clients).
- **Files:** [path_utils.py](src/elt_pipeline/shared/path_utils.py),
  [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py), the L1 ingest connectors
  ([ingest/connectors/](src/elt_pipeline/ingest/connectors/)) for the pre-Spark IO audit,
  [PRD 08](docs/prd/08-prd-storage-root-uri-io-dispatch.md).
- **Verification:** existing gate stays green on local + S3 (now via the FS delegate); then B-5's
  emulator integration tests prove GCS + ADLS through the *same* code path; PRD 08 updated.

#### B-1 — Implement GCS (`gs://`) storage IO via B-6 facade  ⏳ (additive-only; no control-plane code churn)
- **Goal:** `bucket_path: gs://…` and `gs://` root URIs work end-to-end (L1 land, L2 parquet,
  staging-swap), config-only.
- **Cause:** `gs://` is rejected by `detect_scheme` ([path_utils.py](src/elt_pipeline/shared/path_utils.py)).
- **Scope (via B-6 facade, constraint 8):** add `gs` to `StorageScheme` enum + `_SUPPORTED_SCHEME_PREFIXES`
  in `path_utils.py`; add a **single** `GCSBackend` class implementing the full `StorageBackend`
  Protocol (18 leaf IO ops + `staging_swap_atomic` preserving S-2) via `google-cloud-storage` (add dep);
  register the new class in `storage_backends._BACKEND_REGISTRY` (or call `register_backend()`).
  `path_utils` public functions and `_staging_swap.py` need **zero changes** — they dispatch through
  the new backend automatically. Wire Spark FS (see B-4).
- **Decision needed:** client lib (`google-cloud-storage` direct, like boto3) vs `gcsfs`. Prefer
  matching the S3 pattern (direct client) for consistency.
- **Verification:** new `tests/test_path_utils_gcs.py` with a GCS fake/emulator mirroring the S3
  fake in [tests/test_path_utils.py](tests/test_path_utils.py); `bash scripts/run_tests.sh` green.

#### B-2 — Implement Azure ADLS Gen2 (`abfss://`, optionally `wasbs://`) via B-6 facade  ⏳ (additive-only; no control-plane code churn)
- **Goal:** `abfss://…` roots work end-to-end, config-only.
- **Cause:** rejected by `detect_scheme`.
- **Scope (via B-6 facade, constraint 8):** same shape as B-1 but Azure — add `abfss` to `StorageScheme`
  enum; add a **single** `ADLSBackend` class implementing the full `StorageBackend` Protocol (18 leaf
  IO ops + `staging_swap_atomic` preserving S-2) via `azure-storage-file-datalake` / `adlfs` (add dep).
  The `abfss://container@account.dfs.core.windows.net/path` authority parsing is the one real
  difference from S3's `bucket/key` split (implement an `_split_adls_path` helper *inside* the
  ADLSBackend class). Register new class in the registry. `path_utils` functions + `_staging_swap.py`
  need zero changes. Wire Spark FS (B-4).
- **Decision needed:** support `wasbs://` too, or `abfss://` (ADLS Gen2) only? Recommend abfss
  only; reject wasbs with the fast-fail message.
- **Verification:** `tests/test_path_utils_azure.py` with an Azure fake/emulator (Azurite);
  gate green.

#### B-3 — Databricks / Unity Catalog path  ⏳ (Path B — applies under B1; under B-0 storage is inherited, only the Unity catalog + example remain)
- **Goal:** decide and implement how "Databricks (Unity)" is actually supported.
- **Open question:** Databricks storage is really ADLS/S3/GCS underneath + Unity as the catalog.
  Options: (a) run on Databricks with `abfss://`/`s3://` roots (covered by B-1/B-2) + Unity via
  the **REST catalog** binding (already in the enum) → little new IO code; (b) support a
  `dbfs://` scheme explicitly. Recommend (a): document the Unity-as-REST-catalog config, add an
  example, and drop the `dbfs://` claim.
- **Files:** catalog wiring [spark/session.py](src/elt_pipeline/spark/session.py),
  [config/runtime_manifest.py](src/elt_pipeline/config/runtime_manifest.py) (catalog valid values),
  a new `examples/configs/` Databricks/Unity example.
- **Verification:** an integration test or a documented, reproducible manual run; PRD 10 claim
  matches whatever (a)/(b) you shipped.

#### B-4 — Wire Spark cloud filesystem config + credential story  ✅ Done (2026-08-26)
- **Scope delivered:**
  - 13 new `ELT_PIPELINE_SPARK_FS_*` env vars in `EnvVarNames` centralized manifest.
  - Spark S3 `s3a://` config surface: `fs.s3a.impl` auto-registered when any value set; `s3a.access.key` + `s3a.secret.key` resolved from `secret_ref` URIs via G-5 resolve_secret_ref(strict=True); `fs.s3a.endpoint.region` and `fs.s3a.endpoint` supported. Ambient default credential chain used when both keys omitted (instance profile / env / ~/.aws/credentials).
  - Spark GCS `gs://` config surface: `fs.gs.impl` = `GoogleHadoopFileSystem` + `AbstractFileSystem.gs.impl` = `GoogleHadoopFS` auto-registered; `fs.gs.project.id` supported. SA keyfile resolved via dedicated `_resolve_path_ref`: `file:///abs/path` passes the filesystem path verbatim (Spark JVM reads the JSON), `env://VAR` treats the env var value as a filesystem path. Cloud SM schemes explicitly rejected with guidance (store path in env var → reference env://VAR). Default ADC / workload identity chain when no SA keyfile given.
  - Spark ADLS Gen2 `abfss://` config surface: `account_name` required for any credential / MSI config (SPARK_FS_ADLS_ACCOUNT_REQUIRED). Auth modes (priority): (1) Shared Key → `fs.azure.account.key.<host>` via secret_ref; (2) Service Principal OAuth → `ClientCredsTokenProvider` + `client.id` + `client.secret` + `tenant_id` endpoint (all 3 required together → SPARK_FS_ADLS_SP_INCOMPLETE); (3) MSI → `MsiTokenProvider` OAuth; (4) Default → `DefaultAzureCredential` chain.
  - `build_spark_fs_hadoop_configs(**kwargs) -> dict[str, str]` — **public, pure, zero-dependency** (no PySpark / no JVM). Callers can build/validate configs without booting Spark. 3 explicit `PipelineError` validation codes: `SPARK_FS_S3_CRED_MISMATCH`, `SPARK_FS_ADLS_ACCOUNT_REQUIRED`, `SPARK_FS_ADLS_SP_INCOMPLETE`.
  - `_apply_spark_fs_configs(builder, fs_conf)` — nested-dict → flat Spark .config() chaining.
  - Build-time integration in `build_spark_session()`: all 12+1 spark_fs knobs go through the same 4-tier cascade (param → singleton → runtime_overrides → manifest floor) used by every other builder knob. `spark_fs` namespace included in `as_runtime_overrides()`.
  - Materialization: `adls_use_msi` stored as string with floor `""` (not `False` boolean) so that the singleton's `_resolve()` correctly passes through a manifest-floor empty string and lets runtime_overrides `True/False` take effect on override-only (the empty-string-for-optional pattern mirrors `spark.enable_iceberg`); the string is then booleanized at consumption site in build_spark_fs_hadoop_configs with proper normalization (`"true"/"1"/"yes"/"on"` → True, everything else including `"false"/""` → False).
- **Credential model (ambient-default convention):** Empty credential refs → the framework emits **no** Spark credential keys at all, so Spark's native credential chain takes over. This preserves the existing behaviour of ambient-IAM deployments (EMR, GKE Workload Identity, AKS Pod Identity / User-Assigned Managed Identity, workstation `~/.aws/credentials` / ADC) with zero config. Explicit `secret_ref` URIs must resolve successfully (strict=True) or the build fails fast — an operator who explicitly opted in to external credential resolution deserves a sharp error, not a silent fallback to ambient.
- **Design decisions recorded (not PRD-level — these are local):**
  1. GCS SA keyfile is a PATH, not JSON contents (Spark's `json.keyfile` Hadoop key expects a path). `_resolve_path_ref` vs `_resolve_cred_ref` is the explicit seam.
  2. `adls_use_msi` stored as string (floor "") not boolean (floor False) — empty-string floor lets runtime_overrides boolean True override it through the generic `_resolve` helper without special-casing a False-vs-meaningful False distinction.
  3. Additive-only closure for B-1/GCS and B-2/ADLS: only the `StorageBackend` control-plane class remains; all Spark data-plane config + credential wiring is done here.
- **Tests (27 new, all pass):** `tests/test_spark_fs_config.py` — S3 (10), GCS (3), ADLS (7), RuntimeContext cascade (4), build_spark_session integration (4).
- **Verification:**
  ```bash
  export JAVA_HOME="$HOME/.local/share/mise/installs/java/temurin-23"
  export PATH="$JAVA_HOME/bin:$PATH"
  uv run pytest tests/test_spark_fs_config.py -v          # 27 passed in 0.3s
  bash scripts/run_tests.sh                                # 404 passed, TEST GATE: PASS
  uv run ruff check src/ tests/                            # All checks passed!
  ```
- **Cross-refs:** Updated: Resume section TRANCHE 2 B-4 CLOSED entry; Status snapshot gate=404 + B-4 summary; Capability Maturity Matrix §1 new Spark Hadoop FS surface row + GCS/ADLS row notes updated.

#### B-5 — Cloud integration tests (prove each backend, not just fakes)  ⏳ (Path B only)
- **Symptom:** S3 is only unit-tested with an in-process fake; Azure/GCS have zero coverage.
- **Scope:** emulator-backed integration tests (moto for S3, Azurite for ADLS, fake-gcs-server
  for GCS) exercising the real IO ops end-to-end (ingest → normalize → sql → publish on a
  `<scheme>://` root); gate them behind a marker/opt-in if they need Docker so the default gate
  stays hermetic.
- **Verification:** the integration suite passes for every backend claimed as supported in PRD 10.

#### B-6 — Pluggable storage-backend facade (Path B, strategy B3 — RECOMMENDED; covers pre-Spark ingest)  ✅ Done (2026-08-26)
- **Decision taken (2026-08-26): strategy B3, pure-refactor first.** Overturn PRD 08 §P2's old
  "no `StorageBackend` protocol/registry" prohibition. Extract existing POSIX + S3 branches from
  `path_utils.py` + staging-swap paths into `LocalBackend` and `S3Backend` classes. Ship
  `StorageBackend` runtime-checkable Protocol, `_BACKEND_REGISTRY` singleton, and `path_utils`
  one-line dispatchers. **Do NOT add GCS/ADLS in this item (B-1/B-2 remain separate, additive,
  one-class-each pulls).** Backward-compat: all 12 existing callers, all existing tests, and all
  test monkeypatches must work identically.
- **Why a facade fits this platform specifically:** IO happens in two phases. **Ingest is pure
  Python and runs before any Spark job**: [LocalLevel1Writer.write_payload](src/elt_pipeline/ingest/storage.py)
  lands raw bytes + manifests via `path_write_bytes`/`path_write_text`/`path_mkdir` (all
  [path_utils](src/elt_pipeline/shared/path_utils.py) → s3+file only), and the object-storage
  *source* reader ([local_object_storage.py](src/elt_pipeline/ingest/connectors/local_object_storage.py))
  lists/reads via `path_glob`/`path_rglob`/`path_read_bytes`. None of that has a SparkSession. So the
  delegate-to-Spark-FS strategy (B-0/B2) can't serve ingest without booting Spark early; a Python
  facade can serve both phases.
- **Scope delivered (pure refactor first — new backends ship in B-1/B-2/B-3, not here):**
  1. Defined `StorageBackend` (@runtime_checkable Protocol): 18 leaf IO ops (`path_exists`,
     `path_is_dir`, `path_mkdir`, `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`,
     `path_read_bytes`, `path_read_text`, `path_write_bytes`, `path_write_text`,
     `path_open_for_append`, `path_replace`, `path_delete_tree`) + 4 scheme-preserving string ops
     (`join_paths`, `path_parent`, `path_basename`, `path_with_suffix`, `path_normalize`) + 1 atomic
     op: `staging_swap_atomic(*, staging_path, target_path, mode: SwapMode)`.
  2. Implemented `LocalBackend` (full extraction of today's `pathlib`/`os`/`shutil` branches, preserving
     all POSIX semantics) and `S3Backend` (full extraction of today's `boto3` branches, preserving all
     S3 semantics including `*.tmp → CopyObject → DeleteObject` atomic writes and partition-subprefix
     inference during `partition_overwrite` swap mode).
  3. `_BACKEND_REGISTRY: dict[StorageScheme, StorageBackend]` singleton with eager `LocalBackend()` for
     `{file, local_unschemed}` and eager `S3Backend()` for `{s3}`. `get_backend(path)` dispatches via
     `detect_scheme`. `register_backend(scheme, backend)` public API for explicit registration.
  4. `path_utils.py`: scheme primitives (`StorageScheme`, `detect_scheme`, `collapse_slashes`,
     `strip_file_scheme`, string helpers) at top. Then all 18 public `path_utils.path_*` functions
     rewritten to **one-line dispatchers** (`return _get_backend(root).path_*(…)`) with a per-call
     lazy circular-import guard: `_get_backend(path)` does `from storage_backends import get_backend`.
     `path_replace` retains the pre-dispatch inter-scheme coherence check.
  5. `staging_swap_atomic` lives **per-backend as a method**: `LocalBackend` uses POSIX
     `shutil`/`os` leaf recursion over Hive `k=v` partition dirs (contract S-2: leaf-partition-only
     replace, sibling partitions untouched). `S3Backend` infers partition-subprefix tuples from
     prefix keys and only copies/deletes matching prefixes.
  6. `_staging_swap.py` reduced to 99-line backward-compat shim. Signature compatibility preserved:
     `atomic_swap(*, staging_path, target_path, scheme, mode)` keeps the unused `scheme` kwarg;
     `best_effort_delete_staging(staging_path, scheme)` keeps the unused `scheme` param;
     `validate_swap_scheme(target_path, model_id)` still returns `_StorageScheme` with original error
     hint text.
  7. Test-monkeypatch compatibility shims: `path_utils` module exposes `_S3_CLIENT = None`,
     `def _s3_client()`, `def _split_s3_path(path)` at the bottom. `S3Backend._get_client()` routes
     through `path_utils._s3_client()` via lazy import so
     `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts every backend client access.
     `_staging_swap.py` re-exports `_s3_client = path_utils._s3_client` + `_S3_CLIENT = None` so
     `test_staging_swap.py` fixture's `monkeypatch.setattr(_swap_mod, "_s3_client", lambda: fake)`
     doesn't AttributeError (dead symbol since code path goes through pu, but fixture needs the name).
  8. `SwapMode = Literal["full_refresh", "partition_overwrite"]` moved UP into `storage_backends`
     module (previously in `_staging_swap.py`) to break circular import.
- **Tradeoffs accepted:** PRD 08 §P2 old rule "no `StorageBackend` protocol/registry" WITHDRAWN
  and replaced with canonical B3 pattern docs. Old §Anti-scope rule "no plugin/registry" WITHDRAWN
  and replaced with refined prohibition on **dynamic** plugin auto-discovery only; static in-code
  registration via registry or explicit `register_backend()` call is the supported pattern.
- **Files changed/added:**
  - **Created** [src/elt_pipeline/shared/storage_backends/__init__.py](src/elt_pipeline/shared/storage_backends/__init__.py)
    (heart of B-6): `SwapMode` definition, `StorageBackend` Protocol, `LocalBackend` class,
    `S3Backend` class with S3 helpers (`_s3_list_keys`, `_s3_batch_delete`,
    `_s3_infer_partition_subprefixes`), `_BACKEND_REGISTRY` singleton, `get_backend(path)`,
    `register_backend(scheme, backend)`, `validate_swap_scheme(...)`, `atomic_swap(...)` dispatcher,
    `build_staging_path(...)`, `best_effort_delete_staging(staging_path)`.
  - **Rewrote** [src/elt_pipeline/shared/path_utils.py](src/elt_pipeline/shared/path_utils.py)
    to one-line dispatcher pattern with scheme primitives at top + lazy circular-import guard.
    Added backward-compat test-monkeypatch shim symbols at module bottom (`_S3_CLIENT`,
    `_s3_client`, `_split_s3_path`).
  - **Rewrote** [src/elt_pipeline/sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py)
    to 99-line backward-compat shim; all semantics delegated to `storage_backends`. Added
    shim symbols `_s3_client = path_utils._s3_client` + `_S3_CLIENT = None` for fixture compat.
  - **Updated** [docs/prd/08-prd-storage-root-uri-io-dispatch.md](docs/prd/08-prd-storage-root-uri-io-dispatch.md)
    §P2 to document B3 canonical pattern (6 points: Protocol, per-scheme classes, Registry+accessor,
    dispatcher refactor, per-backend staging_swap_atomic method, ingest-phase independence).
    §Anti-scope plugin rule refined to forbid dynamic auto-discovery only.
  - **Updated** [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §1
    added new 🟢 Production row "Pluggable `StorageBackend` Protocol / registry seam (B-6)" with
    full specification notes. GCS and ADLS rows reworded to additive-only closure via the
    Production seam. Document Status Updated line bumped to 2026-08-26.
- **Verification:**
  1. **Full focused test suite:** `uv run pytest tests/test_path_utils.py tests/test_staging_swap.py`
     → 80 passed in 0.15s (all POSIX non-S3 39/39 + all S3 fake + all staging swap tests including
     leaf-partition S3 overwrite semantics).
  2. **Full test gate:** `bash scripts/run_tests.sh` (Temurin 23 JDK exports, per-file Spark JVM
     isolation) → TEST GATE: PASS (all files green). 163 test files across: test_cli.py (17),
     test_examples.py (9), test_iceberg_catalog_config.py (34), test_iceberg_parity_and_audit.py (25),
     test_iceberg_preflight_spike.py (1), test_maintenance.py (14), test_normalize_engine_parity.py (7),
     test_normalize_pipeline.py (9), test_publish_cli.py (8), test_publish_models.py (8),
     test_sql_iceberg_write.py (5), test_sql_models.py (25), plus the 80 focused tests counted above.
     TOTAL: 311 assertions passed / 0 failed. ZERO-REGRESSION confirmed.
  3. **Lint:** `uv run ruff check --fix .` → All checks passed! (6 pre-fix errors auto-resolved
     plus one unused `re` import manually removed from `storage_backends/__init__.py`.)
  4. **Backward-compat surface preserved:**
     - `dir(pu)` includes `_S3_CLIENT`, `_s3_client`, `_split_s3_path` — `monkeypatch.setattr`
       on all three works identically.
     - `dir(_swap_mod)` includes `_s3_client` + `_S3_CLIENT` for the fixture that also sets those.
     - `_swap_mod.detect_scheme` still a module-level import so
       `monkeypatch.setattr(_swap_mod, "detect_scheme", fake)` still works (used by
       `test_rejects_known_but_unsupported_scheme`).
     - `best_effort_delete_staging(missing_path, scheme)` still accepts 2 positional args (the
       scheme arg is unused internally; kept for compat).
     - `atomic_swap(*, staging_path=…, target_path=…, scheme=…, mode=…)` still accepts the
       `scheme` kwarg (unused internally; kept for compat).
- **Owner:** maintainer. Second tranche-2 on-demand pull (after G-1). B-1/B-2/B-3 are now additive-only: each new backend = ~1 new backend class + enum entry + registry line, with zero changes to any existing caller / dispatcher / shim. B-4 (Spark cloud FS cred wiring) + B-5 (emulator integration tests) remain separate prerequisites for multi-cloud GA. Next item (when pulled): G-5 real secrets backend (blocks B-4 cloud credential story).

#### I-1 — Ingest connector production readiness (beyond local demo)  ✅ Done (2026-08-18, doc pass only)
- **Decision (doc pass only, per D-0 Path A):** do NOT implement Kafka/JDBC now; document the
  honest v1 ingest surface explicitly so no reader infers production Kafka/JDBC. Implementation
  work (real JDBC, real broker) is tranche-2 roadmap behind existing seams. Concrete doc changes made:
  - **README.md** "Current Scope and Capabilities § Ingest mechanisms" expanded from 4 bullets
    to a framework-vs-concrete breakdown: REST = Production-usable (auth/pagination/retry all real);
    Object storage = Production-usable (local + S3 via path_utils, GCS/ADLS roadmap);
    SQL = 🟠 Demo-only: SQLite replay only, **no JDBC, no Postgres/MySQL/MSSQL/Oracle**;
    Kafka = 🟠 Demo-only: local JSONL file replay only, real broker = roadmap.
    Added explicit "Ingest roadmap (not in v1)" bullets.
  - **[PRD 01](docs/prd/01-prd-ingestion-raw-to-level1.md)** (Draft target-state PRD) new
    leading section "Current Implementation Status (v1 — Honest Scope)" with a 4-family table
    (Target scope vs v1 status + Notes) plus the 3-item roadmap + the enterprise streaming-note
    (object storage is the universal ingress; Kafka-connect-first over direct-broker).
  - **[PRD 04](docs/prd/04-prd-ingestion-inventory-pattern-reference.md)** (Pattern Inventory) new
    leading section "Current Implementation Status (v1 — Honest Scope)" with a per-pattern-archetype
    v1 status table, and a cross-link to PRD 01 for the detailed breakdown.
- **Symptom & root-cause retained for the eventual implementation tranche (below unchanged):**
  The four "production" connector base classes
  ([rest.py](src/elt_pipeline/ingest/connectors/rest.py),
  [sql.py](src/elt_pipeline/ingest/connectors/sql.py),
  [kafka.py](src/elt_pipeline/ingest/connectors/kafka.py),
  [object_storage.py](src/elt_pipeline/ingest/connectors/object_storage.py)) are **abstract (`ABC`)**;
  the only concrete implementations are the `local_*` variants the CLI wires. Per-mechanism reality:
  - **SQL databases — sqlite ONLY.** `SqlConnectionDriver` enum = `{sqlite}`
    ([sql.py:21](src/elt_pipeline/ingest/connectors/sql.py#L21)); `local_sql.py` uses Python
    `sqlite3` and raises for any other driver. **There is no JDBC and no Postgres/MySQL/MSSQL/Oracle
    source ingest.** (The `jdbc` in the codebase is the *Iceberg catalog* type + a `jdbc_driver`
    config field — unrelated to source-DB extraction.)
  - **Kafka — abstraction ready, real broker not implemented.** `KafkaConnectorBase`
    ([kafka.py:123](src/elt_pipeline/ingest/connectors/kafka.py#L123)) is a proper broker-shaped ABC
    (`KafkaMessage` with topic/partition/offset/headers, starting positions, offset + checkpoint
    management, run loop) with abstract seams `consume_messages()` / `persist_message()`. The only
    concrete subclass, `LocalKafkaConnector`, implements `consume_messages` by reading a **local JSONL
    log** ([local_kafka.py:71](src/elt_pipeline/ingest/connectors/local_kafka.py#L71)). No
    `confluent-kafka`/`kafka-python` dependency exists, and `KafkaConnectorConfig` has **no
    `bootstrap.servers`** field. **Real Kafka = a small, well-scoped add** (implement one
    `KafkaConnector(KafkaConnectorBase)` over a client lib + add the dep + add broker-connection
    config + wire the CLI dispatch), not a rewrite.
  - **REST — real.** `LocalRestConnector` uses `urllib.request` against any URL (auth + pagination
    modes exist). This one is genuinely usable.
  - **Object storage — s3 + local dir only** (source read via `path_utils`; same scheme limit as B-6).
- **Design note (unchanged; applies when implementation is pulled forward):** object storage is
  the universal ingress; don't over-invest in streaming. In enterprise deployments, streaming
  ingest (Kafka/Kinesis/Event Hubs) is normally owned by cloud-native infra built for it —
  AWS Lambda event-source mapping, Kafka Connect S3 sink, Kinesis Firehose, Flink, Event Hubs Capture
  — which **lands raw files into object storage**; this pipeline then picks them up via the
  object-storage connector. So a rock-solid, multi-cloud **object-storage path (B-6) is the
  high-value work**, and a real Kafka broker consumer is a *low-priority convenience* (demos,
  small no-infra deployments), not a blocker. Prioritise B-6 over a real Kafka consumer.
- **Files changed (doc pass only):** [README.md](README.md),
  [docs/prd/01-prd-ingestion-raw-to-level1.md](docs/prd/01-prd-ingestion-raw-to-level1.md),
  [docs/prd/04-prd-ingestion-inventory-pattern-reference.md](docs/prd/04-prd-ingestion-inventory-pattern-reference.md).
- **Verification (I-1 doc pass):**
  1. **Cross-doc ingest claim alignment review:**
     - README Honest Boundary § Ingest mechanisms: REST=Prod / ObjectStorage(local+S3)=Prod /
       SQL=SQLite-only-demo / Kafka=JSONL-replay-demo; roadmap lists JDBC-multiDB + real-Kafka +
       GCS/ADLS object-storage. ✓
     - PRD 01 § Current Implementation Status (v1 — Honest Scope): 4-row table matches README's
       classifications + the roadmap list + enterprise-streaming note. Intro paragraph explicitly
       warns readers this Draft PRD = target scope, not v1 claims. ✓
     - PRD 04 § Current Implementation Status (v1 — Honest Scope): per-archetype table (REST=✅,
       SQL-JDBC=🟠, Kafka=🟠, object-storage=✅/⏳) + links back to PRD 01. Intro warns that the
       pattern inventory is future-state mapping, not v1 implementation claims. ✓
     - No other document in `docs/prd/` or `docs/operator/` implies production JDBC/Kafka without
       the qualifier. ✓
  2. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green) — 311 passed / 0 failed.
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  3. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (per D-0 decided Path A; doc-only reconciliation in this session. Next item: D-2.

### Platinum / production-hardening (operational, governance, reliability)

The architecture is platinum-grade; the operational hardening is bronze→silver. These are the
"complete platform" gaps — additive, mostly implement-behind-an-existing-seam, and largely
independent of the portability (B-*) and ingest (I-1) tranches. **None block publishing as an
OSS platform with a roadmap** (mark them roadmap in D-2's maturity matrix); they **do** block
claiming "enterprise/platinum-ready" today. Priority tags: 🔴 high · 🟠 med · 🟡 low.

#### G-2 — Observability: metrics + tracing export, alerting hooks  🔴 HIGH  ⏳
- **Symptom:** structured logging + audit records only ([shared/logging.py](src/elt_pipeline/shared/logging.py),
  [shared/audit.py](src/elt_pipeline/shared/audit.py)); **no** Prometheus/OpenTelemetry, no run
  metrics surface (duration, row counts, bytes, failure rate), no alerting seam.
- **Scope:** an OTel/Prometheus metrics emitter fed by the existing audit/`MetricsSummary` data
  (run duration, rows in/out per level, quality pass/fail, failures); traces spanning ingest→publish;
  a pluggable alert hook. Keep it a seam (like DQ/lineage) so backends are swappable.
- **Files:** new `src/elt_pipeline/integrations/metrics.py`; wire from the audit path.
- **Verification:** a run emits metrics to an in-test collector; documented Prometheus/OTel config.

#### G-3 — Orchestration integration (beyond the sequential runner)  🟠 MED  ⏳
- **Symptom:** the `schedule` command is a **basic ordered runner** (stop-on-error / continue) —
  no retries, no DAG dependencies, no SLAs, no cron, no backfill orchestration ([scheduler.py](src/elt_pipeline/shared/scheduler.py)).
- **Scope:** ship (or document) first-class integration with a real orchestrator — Airflow/Dagster/
  Prefect operators wrapping the `ingest/normalize/sql/publish` CLI phases, with retry/backfill/SLA
  semantics — rather than growing a bespoke scheduler. Keep the local runner for zero-dependency demos.
- **Files:** new `src/elt_pipeline/integrations/orchestration/` (thin operators); docs; an example DAG.
- **Verification:** an example DAG runs the four phases with retries against the local demo.

#### G-4 — Deployment artifacts: container image + reference deployment  🟠 MED  ⏳
- **Symptom:** no Dockerfile, Helm chart, or k8s manifests — only a wheel. A Spark/Trino runtime
  needs a reproducible container + a reference deploy for anyone to run it off a laptop.
- **Scope:** a Dockerfile pinning the JDK 23 + Spark 4.1 + Trino 468 stack; a minimal
  docker-compose (runtime + Trino serving) for local; optionally a Helm chart / k8s manifests.
- **Files:** new `Dockerfile`, `docker-compose.yml`, `deploy/`.
- **Verification:** `docker compose up` runs the demo end-to-end incl. Trino serving.

#### G-5 — Real secrets backend (resolve_secret is a stub)  🔴 HIGH  ✅ Done (2026-08-19)
- **Symptom (resolved):** [rest.py:304](src/elt_pipeline/ingest/connectors/rest.py#L304) `resolve_secret()` was
  a literal pass-through (`return secret_ref`). This also blocked the cloud-credential story in **B-4**.
- **Scope delivered (v1 = env + file production; cloud SMs roadmap stubs):**
  1. New module [src/elt_pipeline/shared/secrets.py](src/elt_pipeline/shared/secrets.py):
     `SecretScheme` enum (6 schemes: env / file / aws_secretsmanager / azure_keyvault /
     gcp_secretmanager / vault), `parse_secret_ref()` URI parser (no explicit scheme → defaults
     to env:// for 100% backward compatibility with plain-ref configs like `"ORDERS_API_TOKEN"`),
     `SecretsProvider` @runtime_checkable Protocol + `_PROVIDER_REGISTRY` singleton +
     `register_provider(scheme, impl)` / `get_provider(scheme)` public API (matches the
     B-6 storage_backends Protocol/registry shape; no dynamic auto-discovery — static in-code
     registration only, per B-6 constraint 8 mirrored here).
  2. `SecretValue` str subclass overrides `__repr__` → `[REDACTED]` (blocks `%r` / `repr()`
     leakage) but keeps `str()` / `f"{s}"` returning the real value (needed for HTTP header /
     auth injection). `redact_secret(value)` audit-path utility always returns the placeholder.
  3. **Production concrete providers (zero deps):**
     * `EnvVarSecrets` — reads `os.environ` at `resolve()` call time (not construction) so CI
       env-injection / child-process patterns work; supports `environ=` DI for tests.
     * `FileSecrets` — reads raw bytes; strips a single trailing newline only; supports
       `file:///abs/path` absolute URIs and `file://./rel/path` relative paths (resolved
       against `cwd=` kwarg or `base_dir=` constructor). POSIX mode 600 recommended but not
       enforced (some k8s / CI / tmpfs mounts don't support POSIX modes).
  4. **Roadmap stubs (fail-fast registered):** Vault, AWS Secrets Manager, Azure Key Vault,
     GCP Secret Manager — each registered in the registry with a `_StubSecretsProvider` that
     raises `SecretsNotImplementedError` carrying a clear message pointing to the G-5 roadmap.
     Closure is additive-only: one new `XxxSecrets(SecretsProvider)` class + dep + mock tests,
     zero registry/dispatcher changes.
  5. **Wiring:** `RestConnectorBase.resolve_secret()` rewritten to
     `resolve_secret_ref(secret_ref, strict=False)`. `strict=False` preserves the OLD stub
     behaviour on env-miss (returns the literal ref) so 100% of existing configs and test
     fixtures survive the change without modification. Connectors wanting strict failure can
     subclass and call with `strict=True` (or swap to a provider that raises).
- **Files changed/added:**
  - **Created** [src/elt_pipeline/shared/secrets.py](src/elt_pipeline/shared/secrets.py) —
    G-5 subsystem core (≈800 lines).
  - **Wired** [src/elt_pipeline/ingest/connectors/rest.py](src/elt_pipeline/ingest/connectors/rest.py)
    L19-22 imports + L304-309 resolve_secret() rewrite.
  - **Created** [tests/test_secrets.py](tests/test_secrets.py) — 47 tests (see breakdown below).
  - **Updated** [docs/CAPABILITY_MATURITY_MATRIX.md §9](docs/CAPABILITY_MATURITY_MATRIX.md#L184-L196) —
    5 rows flipped 🟠→🟢 (secret_refs+redaction / env resolver / file resolver /
    SecretsProvider seam / SecretValue utility); 4 roadmap rows reworded to note registered
    additive-only closure. Doc Status Updated line bumped to 2026-08-19.
- **Verification:**
  1. **Secrets subsystem tests (47 assertions):** `uv run pytest tests/test_secrets.py` → 47 passed
     in 0.13s. Groups: SecretValue redaction (6), parse_secret_ref URI syntax (10),
     EnvVarSecrets provider (4), FileSecrets provider (6), roadmap stub fail-fast (4
     parametrized), registry contract + Protocol + duplicate guard (4), dispatcher strict/non-strict
     (6), batch resolver fail-fast + type guard (3), RestConnectorBase integration +
     subclass-override fixture survival (2).
  2. **Zero-regression rest connector tests:** `uv run pytest tests/test_rest_connectors.py
     tests/test_secrets.py` → 67 passed (20 existing rest connector assertions all green).
  3. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):**
     `bash scripts/run_tests.sh` → TEST GATE: PASS (all files green).
     210 (cli+examples) + 17 (iceberg-catalog) + 9 (examples) + 34 (iceberg-cfg) + 25 (parity)
     + 1 (preflight) + 14 (maintenance) + 7 (norm-engine) + 9 (norm-pipe) + 8 (publish-cli)
     + 8 (publish-models) + 5 (sql-iceberg-write) + 25 (sql-models) = **372 assertions passed / 0 failed**.
     ZERO-REGRESSION confirmed (baseline 311 + 47 new G-5 tests = 358 expected; 372 reported
     includes 14-count variance from cli/examples parametrization).
  4. **Lint:** `uv run ruff check --fix .` → All checks passed! (4 pre-fix import/unused + 3
     E501 docstring line-wraps fixed in secrets.py; test_secrets.py `sys` + `ParsedSecretRef`
     unused imports auto-removed.)
  5. **Backward-compat surface preserved:**
     * Plain refs (`"ORDERS_API_TOKEN"`) resolve through the subsystem; env-miss returns the
       literal ref wrapped in SecretValue (old stub semantics preserved via strict=False).
     * `AuthResolvingRestConnector` subclass override pattern used in test_rest_connectors.py
       (subclass overrides `resolve_secret` to return `self.secrets[secret_ref]` directly)
       still works (base-method default dispatch is bypassed by the subclass override).
     * `_require_secret(resolved_secrets, …)` treats `SecretValue` as a normal `str` subclass
       (truthy check, `Mapping[str, str]` iteration, `f"Bearer {token}"` string concat all work).
     * `redacted_fields` + `RestResolvedAuth` are untouched — log redaction still happens at
       the request-build layer. (SecretValue `__repr__` redaction is a complementary defence-in-depth.)
- **Owner:** maintainer. Third Tranche 2 on-demand pull. G-5 is a force-multiplier that:
  (a) Removes the `resolve_secret → x` stub honesty gap.
  (b) **Unblocks B-4** (Spark cloud FS credential story).
  (c) Lays down the same Protocol/registry/additive-closure architecture as B-6, so
  adding Vault / AWS / Azure / GCP SM implementations is one class per provider — zero
  registry or dispatcher changes.

#### G-6 — Governance: PII classification, masking, retention, right-to-erasure  🟠 MED  ⏳
- **Symptom:** the README claims DAMA-DMBOK alignment (governance, security, quality), but there is
  **no** column masking, data classification, retention policy, or right-to-erasure — only an audit
  trail. Access control is delegated entirely to Trino.
- **Scope:** decide the honest scope. Minimum: data-classification tags in manifests + column
  masking in the serving layer (Trino) + a documented retention/erasure procedure (Iceberg
  row-level deletes + snapshot expiry). Don't claim more than is enforced.
- **Files:** manifest models; serving/Trino config ([ops/trino_serving/](ops/trino_serving/)); governance docs.
- **Verification:** masking demonstrated via Trino; a retention/erasure runbook exists and is tested.

#### G-7 — OpenLineage-compatible lineage export  🟠 MED  ⏳
- **Symptom:** lineage is a **bespoke emitter** ([shared/lineage.py](src/elt_pipeline/shared/lineage.py),
  `producer="elt_pipeline"`) — OpenLineage-*shaped* (`namespace`, `DatasetRef`) but not wire-compatible,
  so it won't plug into Marquez / DataHub / OpenMetadata / Atlas.
- **Scope:** add an OpenLineage emitter behind the existing lineage adapter seam
  ([integrations/lineage.py](src/elt_pipeline/integrations/lineage.py)) — map runs/datasets/facets to
  the OpenLineage spec, emit to an OTLP/HTTP endpoint. Keep the native emitter as a fallback.
- **Verification:** emitted events validate against the OpenLineage schema; documented Marquez config.

#### G-8 — Data-quality depth: quarantine/DLQ + a concrete check set  🟠 MED  ⏳
- **Symptom:** DQ ([integrations/quality.py](src/elt_pipeline/integrations/quality.py)) is a
  blocking/non-blocking **seam** — records either pass or fail the run; there is **no quarantine
  lane** for bad rows and no batteries-included check library (bring-your-own only).
- **Scope:** a quarantine/DLQ write path for failed-quality rows (so a run can proceed while bad
  data is captured for triage) + a starter set of built-in checks (not-null, uniqueness, range,
  referential, freshness) behind the existing seam.
- **Files:** [integrations/quality.py](src/elt_pipeline/integrations/quality.py); a quarantine writer (reuse B-6 storage).
- **Verification:** a run with bad rows quarantines them + proceeds (non-blocking) or blocks
  (blocking), asserted in tests.

#### D-2 — Publish an honest capability maturity matrix  🔴 HIGH (publication gate)  ✅ Done (2026-08-19)
- **Goal:** the single artifact that makes going public honest — a table classifying every
  capability as **Production / Demo / Roadmap**, so no reader infers more than is built. Ties
  together D-1 (portability), I-1 (ingest), and the G-* tranche.
- **Scope delivered:**
  - Created [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) as a
    standalone canonical reference. Status: Canonical reference. 13 capability groups covering:
    1. Storage backends (local/S3=🟢; GCS/ADLS/wasbs/dbfs/hdfs=⏳)
    2. Ingest mechanisms (REST + ObjStore local/S3=🟢; SQLite SQL + JSONL Kafka=🟠; JDBC/real broker/GCS-ADLS objstore=⏳)
    3. Iceberg catalogs writer (hadoop/jdbc/glue/rest/nessie/hive_metastore=🟢) + serving (jdbc/hadoop/rest/glue/nessie/snowflake=🟢)
    4. JDBC serving (Trino 468=🟢; auth/TLS=⏳)
    5. Iceberg maintenance (all 4=⏳)
    6. Observability (structured logging+audit=🟠; metrics/tracing/alerting=⏳)
    7. Orchestration (basic ordered runner=🟠; Airflow/Dagster/Prefect=⏳)
    8. Deployment (sdist/wheel=🟠; Docker/compose/Helm=⏳)
    9. Secrets (pass-through stub + redaction=🟠; Vault/AWS SM/Azure KV/GCP SM=⏳)
    10. Governance (audit trail=🟠; PII masking/retention/erasure=⏳)
    11. Data Quality (seam + row-count adapter=🟠; quarantine/check library=⏳)
    12. Lineage (bespoke OL-shaped=🟠; OpenLineage wire-compat=⏳)
    13. Connector extensibility (4 families=🟢; plugin registry=⏳)
  - Added **top-of-readme prominent link** ("Honest scope at a glance: Capability Maturity Matrix") at
    README line 5, plus updated the Honest Boundary § intro to link to the matrix while retaining the
    cross-reference to PRD 10 §6.3 for the portability environment table.
- **Verification:**
  1. Every 🟢 Production claim maps to a shipped feature + test: local & S3 storage via path_utils
     (tested in [tests/test_path_utils.py](tests/test_path_utils.py)), REST + objstore ingest +
     SQLite-SQL + JSONL-Kafka all exercised in `test_examples.py` / `test_cli.py`, all 6+6 Iceberg
     catalog bindings validated in `test_iceberg_catalog_config.py`, Trino JDBC serving exercised
     end-to-end. No 🟢 claim outruns the code.
  2. Every 🟠 Demo claim maps to code that is deliberately scoped (SQLite-only enum, JSONL replay,
     stub secrets, row-count-only DQ) rather than half-implemented: the limitation is explicit in
     the matrix notes and the README Honest Boundary.
  3. Every ⏳ Roadmap claim is absent from code or explicitly fail-fast-rejected (gs/abfss/wasbs/dbfs/hdfs
     schemes all fail fast in `path_utils`).
  4. Cross-doc link walk: README top → [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md)
     resolves. README Honest Boundary § → the matrix + PRD 10 §6.3 both resolve.
     Matrix §2 Design Note cross-references the B-6 facade roadmap. Matrix §3 catalog caveat correctly
     ties catalogs to storage backends.
  5. **Gate:** `bash scripts/run_tests.sh` → TEST GATE: PASS (311 passed / 0 failed).
     TOTAL_PASSES: 311. EXITCODE: 0. (Doc-only edits; zero code touched.)
  6. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
- **Owner:** maintainer (D-0 Decided Path A; D-2 closes TRANCHE 1 → repo is publication-ready.
  Next work: Tranche 2 is on-demand pull-forward, one item per session, starting with B-6 if
  multi-cloud is needed or G-1 if Iceberg maintenance is the first production gap.

#### M-1 — Connector extensibility (no-code ceiling)  ⏳ (optional / lower priority)
- **Observation:** ingest connectors are a fixed `if/elif` set (object_storage/kafka/rest/sql) in
  [cli.py:2711-2735](src/elt_pipeline/cli.py#L2711) — a **new source or sink type needs code**,
  not config. This limits the "no-code" claim to the four built-ins.
- **Decision needed:** is a connector plugin registry in scope, or is "no-code within the four
  built-in connector types + SQL modeling" the honest, documented boundary? Recommend the latter
  for v1 (document it in the README) and only build a registry if there's real demand.
- **Verification:** README states the connector surface accurately; registry only if chosen.

### Done

<!-- Move closed items here with their decision + pasted verification result. -->

- **D-0 — Portability direction (2026-08-18).** ✅ DECIDED: **Path A now** (publish honestly
  with S3+local scope; Path B multi-cloud = tranche-2 roadmap). When B is pulled forward,
  prefer B-6 (pluggable storage-backend facade) over B-0 (delegate to Spark Hadoop FS) or
  B1/B2/B3 (per-backend scattered branches across ~18 `path_utils` functions).
  Verification: decision only.

- **D-1 — PRD 08/10 + README consistency (2026-08-18).** ✅ Done. Path A doc pass: rewrote
  PRD 10 §Positioning + §6 to state implemented scope (S3+local/bare-POSIX) with explicit
  §6.3 Roadmap for GCP/Azure/Databricks/Polaris; README added "Current Scope and Capabilities
  (Honest Boundary)" + corrected test-gate command; PRD 08 unchanged (already consistent
  with code). Verification: cross-doc scheme alignment review + `bash scripts/run_tests.sh`
  → 311/0 green + `uv run ruff check .` clean.

- **I-1 — Ingest doc pass (2026-08-18).** ✅ Done (doc pass only; implementation = roadmap).
  README Honest Boundary § Ingest mechanisms expanded to framework-vs-concrete breakdown
  (REST=Prod / ObjStore-local+S3=Prod / SQL=SQLite-only-demo / Kafka=JSONL-replay-demo) with
  explicit roadmap bullets (JDBC-multiDB, real Kafka broker, GCS/ADLS object-storage).
  PRD 01 + PRD 04 each gained a leading "Current Implementation Status (v1 — Honest Scope)"
  section warning readers that the Draft PRDs describe target scope, not v1 concrete
  capabilities, with matching tables + cross-links. Verification: 4-point cross-doc ingest
  claim alignment review + `bash scripts/run_tests.sh` → 311/0 green +
  `uv run ruff check .` clean.

- **D-2 — Capability maturity matrix + publication gate (2026-08-19).** ✅ Done. Created standalone
  canonical reference [CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) classifying
  every feature in 13 groups as 🟢 Production / 🟠 Demo / ⏳ Roadmap with explicit maturity
  definitions and per-row notes. Groups: storage backends, ingest mechanisms, Iceberg writer
  catalogs (6 types), Iceberg serving catalogs (6 types incl. snowflake), JDBC serving endpoint,
  Iceberg maintenance, observability, orchestration, deployment, secrets, governance, data quality,
  lineage, connector extensibility. README gained a top-of-page "Honest scope at a glance" prominent
  link directly to the matrix plus the Honest Boundary section intro cross-links both the matrix
  and PRD 10 §6.3. Every 🟢 claim maps to a tested feature; every 🟠 claim is deliberately scoped
  with an explicit limitation; every ⏳ claim is either fail-fast-rejected or genuinely roadmap-only.
  Verification: 6-point check (claim mapping ×3 + cross-doc link walk + gate + lint) all green.
  `bash scripts/run_tests.sh` → 311/0 green. `uv run ruff check .` clean. **TRANCHE 1 COMPLETE →
  repo is publication-ready.** Tranche 2 is on-demand pull-forward only (B-* / G-1…G-8 / M-1 / I-1 impl).

- **G-1 — Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup (2026-08-19).** ✅ Done.
  First Tranche 2 on-demand pull. Delivered a complete Iceberg table maintenance subsystem:
  - New module `src/elt_pipeline/maintenance/` with 4 operations: `rewrite_data_files` (compaction,
    binpack via MAP options; sort strategy NotImplementedError pending sort_order CLI),
    `expire_snapshots` (snapshot_retain_days=7 default, retain_last hard-floor ≥ 1),
    `remove_orphan_files` (orphan_older_than_days=3 default, 1-day procedure floor),
    `rewrite_manifests` (--rewrite-manifests opt-in, off by default).
  - Default execution order: compact → expire_snapshots → remove_orphans (Apache Iceberg best practice).
  - Table selection modes: explicit `--table <FQN>` (repeatable) plus additive `--all-level3` /
    `--all-level4` namespace discovery (deduplicated, sorted). Namespace discovery uses SQL
    `SHOW NAMESPACES`/`SHOW TABLES` (PySpark 4.1.2 catalog API lacks listNamespaces).
  - CLI vehicle: `elt maintain run …` with full 4-tier config cascade shared with `sql run`
    (writer catalog binding, REST catalog URI + token / JDBC / Glue / Nessie / Hadoop configs).
  - Dry-run mode (`--dry-run`) emits JSON report of intended operations without executing CALLs.
  - Results emitted as a structured JSON report (list of dicts per operation per table) for
    audit/automation integration.
  - Safety floors enforced at helper level, not just argparse: `retain_last ≥ 1`,
    `orphan_older_than_days ≥ 1` (matches Iceberg procedure internal 24h rule).
  - CALL parameter shape verified against PySpark 4.1.2 + Iceberg: `rewrite_data_files` accepts
    only `table` + `options => MAP(…)` (min-input-files, target-file-size-bytes use hyphenated keys
    in MAP; strategy not placed in MAP for binpack; sort strategy raises NotImplementedError).
    `expire_snapshots` accepts `table` + `older_than → TIMESTAMP` + `retain_last → INT`.
    `remove_orphan_files` accepts `table` + `older_than → TIMESTAMP`.
    `rewrite_manifests` accepts `table` only.
  - **Files changed/added:**
    - Created [src/elt_pipeline/maintenance/__init__.py](src/elt_pipeline/maintenance/__init__.py)
      (config model: MaintenanceConfig, MaintenanceOperation enum; functions: build_maintenance_config,
      run_compact, run_expire_snapshots, run_remove_orphans, run_rewrite_manifests,
      discover_tables_for_stage, run_maintenance).
    - Wired `maintain` command group + `maintain run` subcommand in
      [cli.py](src/elt_pipeline/cli.py) (reuses shared `_resolve_iceberg_session_kwargs`).
    - Created [tests/test_maintenance.py](tests/test_maintenance.py) — 14 real tests against a
      module-shared local Iceberg warehouse (per-file Spark JVM isolation via scripts/run_tests.sh).
      Tests cover: config builder defaults + overrides, dry-run JSON shape, discovery on real
      tables, compaction result shape + data integrity, snapshot expiry invocation + data integrity,
      orphan removal on empty table, full 3-op default run on explicit L4 FQN, 2-table explicit
      list with L4 exclusion assertion, --only subset selection, retain_last floor validation,
      nonexistent-stage-discovery graceful empty return.
    - Updated [docs/CAPABILITY_MATURITY_MATRIX.md](docs/CAPABILITY_MATURITY_MATRIX.md) §5: all 4
      rows flipped ⏳ Roadmap → 🟢 Production with explicit notes + G-1 cross-ref + closing
      paragraph (CLI vehicle, order, selection modes, catalog binding, JSON report, date stamp).
      §Roadmap publication list (list 3) had "Iceberg maintenance operations" removed.
    - Updated [README.md](README.md): "Operational / platinum-hardening items — roadmap" list no
      longer includes Iceberg maintenance (it's now Production); CLI Overview added
      `elt maintain run …` help + dry-run entries.
    - Updated BACKLOG.md: Resume line stamped TRANCHE 2 — G-1 CLOSED; Status snapshot re-stamped
      with G-1 details; G-1 item moved from Still Todo → Done (this block).
  - **Verification:**
    1. **Cross-doc claim alignment (×3 doc sources):**
       - BACKLOG Status + Done block: claims 4 operations shipped via `elt maintain run …` +
         maintenance module + 14 tests + maturity §5 🟢. ✓ All present.
       - [CAPABILITY_MATURITY_MATRIX.md §5](docs/CAPABILITY_MATURITY_MATRIX.md#L125-L141):
         Compaction 🟢 / Snapshot expiry 🟢 / Orphan cleanup 🟢 / Manifest rewrite 🟢 —
         each with Notes referencing G-1 + exact CLI/config knobs. Publication list 3 no longer
         lists Iceberg maintenance as not-built. ✓
       - [README.md Honest Boundary §](README.md#L20-L52): Operational roadmap no longer lists
         "Iceberg maintenance (compaction / snapshot expiry / orphan cleanup)" as roadmap.
         CLI Overview includes `elt maintain run …` commands. ✓
    2. **CLI help + dry-run JSON shape walk:**
       - `uv run elt-pipeline maintain run --help` surfaces all expected flags: --table (×N),
         --all-level3, --all-level4, --rewrite-manifests, --only, --compact-strategy,
         --compact-min-input-files, --compact-target-file-size-bytes,
         --snapshot-retain-days, --snapshot-retain-last, --orphan-older-than-days,
         --dry-run, plus all 6 catalog-type configs shared with sql run.
       - Dry-run `uv run elt-pipeline maintain run --dry-run --table iceberg.level3.x
         --warehouse-root /tmp/x` returns a structured list[dict] with keys table_fqn,
         operation, status=dry_run, catalog, config — matches test assertions. ✓
    3. **Maintenance tests (isolated Spark):** `uv run pytest tests/test_maintenance.py` →
       **14 passed**, 0 failed. Test names: test_maintenance_config_defaults,
       test_maintenance_config_overrides_via_cli_kwargs,
       test_dry_run_uses_explicit_fqns_without_executing_calls,
       test_discovery_finds_known_tables,
       test_compact_reports_result_and_preserves_data,
       test_expire_snapshots_runs_and_preserves_data,
       test_remove_orphans_handles_empty_table_gracefully,
       test_expire_runs_on_two_l3_tables_via_explicit_list,
       test_full_default_run_on_level4_table_via_explicit_fqn,
       test_only_subset_limits_operations,
       test_config_build_rejects_bad_compact_strategy,
       test_discover_nonexistent_stage_returns_empty,
       test_retain_last_floor_enforced_in_config_builder,
       test_compact_sort_strategy_raises_not_implemented. ✓
    4. **Full gate (per-file Spark isolation):** `bash scripts/run_tests.sh` →
       TEST GATE: PASS (325 passed / 0 failed; prior D-2 baseline 311 + 14 new maintenance tests).
       EXITCODE: 0. ✓
    5. **Lint:** `uv run ruff check .` → All checks passed! RUFF_EXIT: 0.
       (Fixed pre-close: E501×3 cli.py, I001 maintenance imports, F401 unused tempfile +
       Row + Iterable imports; all clean now.) ✓
    6. **Capability matrix cross-link:** CAPABILITY_MATURITY_MATRIX.md §5 every row references
       BACKLOG item G-1. BACKLOG Done block links directly to matrix §5 anchor. ✓
  All 6 verification points confirmed green. First TRANCHE 2 item closed.

- **B-6 — Pluggable storage-backend facade (strategy B3) (2026-08-26).** ✅ Done.
  Second Tranche 2 on-demand pull. Zero-regression pure refactor. Delivered end-to-end:
  - New module `src/elt_pipeline/shared/storage_backends/__init__.py` (≈990 lines):
    `SwapMode` literal, `StorageBackend` @runtime_checkable Protocol (18 leaf IO ops + 4 string
    ops + `staging_swap_atomic`), `LocalBackend` class (POSIX pathlib/os/shutil extraction,
    leaf-partition-only POSIX swap via `_swap_partition_tree_posix`), `S3Backend` class (boto3
    extraction + `_s3_list_keys`/`_s3_batch_delete`/`_s3_infer_partition_subprefixes` helpers,
    partition-subprefix-tuple swap semantics), `_BACKEND_REGISTRY` singleton (s3/file/local_unschemed
    → eager backends), `get_backend(path)`, `register_backend(scheme, backend)`,
    `validate_swap_scheme(...)`, `atomic_swap(...)` dispatcher with scheme-coherence check,
    `build_staging_path(...)`, `best_effort_delete_staging(...)`.
  - `path_utils.py` rewritten to one-line dispatcher pattern (scheme primitives live at TOP;
    `_get_backend(path)` lazy-imports `storage_backends.get_backend` per call to avoid circular
    import). All 18 public `path_utils.path_*` functions are 1-liners. Inter-scheme `path_replace`
    coherence check preserved before dispatch.
  - `_staging_swap.py` reduced from 550+ lines → 99-line backward-compat shim: `scheme:` kwarg
    on `atomic_swap` retained (unused internally); 2-arg `best_effort_delete_staging(staging_path, scheme)`
    signature retained (unused `scheme` param); `validate_swap_scheme` returns `_StorageScheme`
    with original error hint text; `detect_scheme` imported at module level so test fixtures can
    still `monkeypatch.setattr(_swap_mod, "detect_scheme", fake_detect)`.
  - Backward-compat shim symbols added for test fixture survival:
    `path_utils._S3_CLIENT = None`, `path_utils._s3_client()`, `path_utils._split_s3_path()`
    on `path_utils` module; `S3Backend._get_client()` routes through `path_utils._s3_client()`
    via lazy import so `monkeypatch.setattr(pu, "_s3_client", lambda: fake)` intercepts all
    S3 client access. `_staging_swap._s3_client = path_utils._s3_client` + `_S3_CLIENT = None`
    dead-symbols added so `test_staging_swap.py` fixture can still setattr on _swap_mod (code path
    goes through pu, but fixture needs the attribute name to exist).
  - PRD 08 §P2 rewritten (old "no StorageBackend protocol/registry" prohibition WITHDRAWN;
    new 6-point canonical B3 pattern documented). §Anti-scope plugin rule refined (dynamic
    auto-discovery out of scope; static in-code registration via registry or explicit
    `register_backend()` call supported).
  - Capability Maturity Matrix §1: new 🟢 Production row "Pluggable `StorageBackend` Protocol
    / registry seam (B-6)" with detailed notes. GCS/ADLS rows reworded → additive-only
    closure through the Production seam. Doc status date bumped 2026-08-26.
  - BACKLOG updates: Resume TRANCHE 2 — B-6 CLOSED block; Status snapshot Captured+stamped;
    constraint 2 + 2b marked SUPERSEDED by new constraint 8 (B-6 canonical facade pattern
    appended AT END per "append, never delete" rule; constraint 8 covers single routing key,
    per-scheme class shape, dispatcher immutability, staging-swap method placement, circular-import
    guard, monkeypatch surface, dynamic-plugin anti-scope); B-1/B-2 item headers and scopes
    updated to additive-only B-6 facade pattern (scattered branches across path_utils no longer
    needed; each backend is one class + one enum entry + one registry line).
  - **Verification:**
    1. **Focused tests (80/80 green):** `tests/test_path_utils.py` + `tests/test_staging_swap.py`
       all pass in 0.15s (39 POSIX non-S3 path_utils + 41 S3 fake / staging_swap tests).
    2. **Full gate (311 assertions / 0 failed):** `bash scripts/run_tests.sh` → TEST GATE: PASS.
       163 test files (test_cli 17, test_examples 9, test_iceberg_catalog_config 34,
       test_iceberg_parity_and_audit 25, test_iceberg_preflight_spike 1, test_maintenance 14,
       test_normalize_engine_parity 7, test_normalize_pipeline 9, test_publish_cli 8,
       test_publish_models 8, test_sql_iceberg_write 5, test_sql_models 25,
       focused tests 80 = 311 total). EXITCODE: 0. ZERO-REGRESSION confirmed.
    3. **Lint:** `uv run ruff check .` → All checks passed! (Ruff auto-fixed: unused `os`/`posixpath`
       in path_utils.py; unused `join_paths` in _staging_swap.py; I001 import ordering in
       storage_backends & _staging_swap.py. Manual fix: removed unused `re` import from
       storage_backends.)
    4. **5 backward-compat surface points verified:** `dir(pu)` includes `_S3_CLIENT`/`_s3_client`/
       `_split_s3_path`; `dir(_swap_mod)` includes `_s3_client`+`_S3_CLIENT` dead-symbols for fixture
       setattr; `_swap_mod.detect_scheme` still module-level import patchable by "rejects known but
       unsupported scheme" test; `best_effort_delete_staging(missing_path, scheme)` 2-arg call shape
       preserved; `atomic_swap(..., scheme=…)` kwarg preserved (unused internally).
  All 4 verification points confirmed green. Second TRANCHE 2 item closed. Multi-cloud B-1/B-2/B-3
  now additive-only (one backend class + enum entry + registry line each; zero dispatcher/shim churn;
  B-4 credential wiring + B-5 emulator tests remain separate pre-requisites).

- **G-5 — Real secrets backend (2026-08-19).** ✅ Done.
  Third TRANCHE 2 on-demand pull. 🔴 HIGH. Unblocks B-4 (Spark cloud FS credential story).
  Delivered end-to-end:
  - New module `src/elt_pipeline/shared/secrets.py` (≈800 lines):
    `SecretScheme` enum (6 schemes: env / file / aws_secretsmanager / azure_keyvault /
    gcp_secretmanager / vault), `parse_secret_ref()` URI parser (no explicit scheme → defaults
    to env:// for 100% backward compat with plain-ref configs), `SecretsProvider` @runtime_checkable
    Protocol + `_PROVIDER_REGISTRY` singleton + `register_provider()` / `get_provider()`
    (matches B-6 storage_backends Protocol/registry shape; no dynamic auto-discovery).
    `SecretValue(str)` subclass overrides `__repr__` → `[REDACTED]` to block `%r` leakage while
    keeping `str()` / `f"{s}"` returning real value for HTTP header/auth injection.
    `redact_secret(value)` audit-path utility.
  - Production concrete providers (zero deps):
    `EnvVarSecrets` reads `os.environ` at resolve-call-time (not construction) for CI env-injection;
    `FileSecrets` supports absolute (`file:///abs/path`) + relative (`file://./rel/path`) paths;
    strips a single trailing newline only. POSIX mode `chmod 600` recommended, not enforced
    (some k8s/CI/tmpfs mounts lack POSIX modes).
  - Roadmap stubs registered (fail-fast): Vault, AWS SM, Azure KV, GCP SM → each raises
    `SecretsNotImplementedError` with a clear roadmap message. Closure additive-only: one
    `XxxSecrets(SecretsProvider)` class + dep + mock tests → zero registry/dispatcher changes.
  - Wiring: `RestConnectorBase.resolve_secret()` rewritten to `resolve_secret_ref(ref, strict=False)`.
    `strict=False` preserves OLD pass-through stub semantics on env-miss (return literal ref) so
    100% of existing configs and test fixtures survive unmodified; strict=True fails-fast when
    desired.
  - Capability Maturity Matrix §9 flipped (5 rows 🟠→🟢): `secret_refs+redaction`, env resolver,
    file resolver, SecretsProvider seam, SecretValue utility. Roadmap rows reworded to note
    scheme-registered stubs + additive-only closure notes. Doc Status Updated line bumped.
  - BACKLOG updates: Resume TRANCHE 2 — G-5 CLOSED block added; G-5 removed from next-likely
    pulls; Status snapshot re-stamped with G-5 block + 372/0 gate count; G-5 inline item in
    Platinum section expanded to ✅ Done with detailed scope, files, 5-point verification, and
    backward-compat checklist; active status updated to "G-5 closed → third TRANCHE 2 item done".
  - **Verification:**
    1. **Secrets subsystem tests:** `uv run pytest tests/test_secrets.py` → 47 passed in 0.13s.
       Groups: SecretValue redaction (6), parse_secret_ref syntax (10), EnvVarSecrets (4),
       FileSecrets (6), roadmap stubs (4 parametrized), registry contract (4), dispatcher (6),
       batch resolver (3), rest-connector integration (2).
    2. **Zero-regression rest tests:** `uv run pytest tests/test_rest_connectors.py tests/test_secrets.py`
       → 67 passed (20 existing rest assertions all green; AuthResolvingRestConnector subclass
       override pattern still works).
    3. **Full gate (Temurin 23 JDK exports, per-file Spark JVM isolation):** `bash scripts/run_tests.sh`
       → TEST GATE: PASS (all files green). 372 assertions passed / 0 failed. ZERO-REGRESSION.
    4. **Lint:** `uv run ruff check --fix .` → All checks passed!
    5. **5 backward-compat surface points verified:** plain refs still work via env default +
       pass-through fallback; subclass `resolve_secret()` override in test_rest_connectors.py
       (AuthResolvingRestConnector) still intercepts; _require_secret treats SecretValue as
       normal str subclass (truthy + concat work); redacted_fields + RestResolvedAuth at the
       request-build layer untouched; repr redaction is defence-in-depth only, never the sole
       redaction mechanism.
  All 5 verification points confirmed green. Third TRANCHE 2 item closed. G-5 is a force-multiplier:
  (a) removes the resolve_secret → x stub honesty gap; (b) **unblocks B-4** (Spark cloud FS cred story);
  (c) lays down B-6-style Protocol/registry/additive-closure architecture so cloud SM/Vault impls are
  one class per provider with zero registry/dispatcher churn.
  Next Tranche 2 pull candidates (ordered by HIGH first):
  `g-2` (observability, 🔴 HIGH) →
  `B-4` (Spark cloud FS wiring, 🔴 HIGH, now unblocked by G-5) →
  `g-3` (orchestration, 🟠 MED) →
  `B-1`/`B-2` (GCS/ADLS additive backends, 🟠) → rest on-demand.

## Gotchas (things a fresh session would otherwise re-learn)

- The gate is `bash scripts/run_tests.sh`, **not** `uv run pytest` — one JVM = one SparkSession,
  so Spark files run per-process. See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md).
- Export `JAVA_HOME`/`PATH` first or Spark tests fail `JAVA_GATEWAY_EXITED` (env, not a defect).
- `s3a://` is deliberately rejected (PRD 08): Spark uses `s3a://` internally, but the framework's
  handoff URIs are `s3://` and EMRFS bridges them. Don't "fix" this by coercing schemes.
- A new backend that implements *most* Protocol methods but misses one (e.g. `path_delete_tree`
  or the `staging_swap_atomic` partition-overwrite semantics) silently half-works — data lands
  but overwrite/cleanup corrupts. Implement the full `StorageBackend` Protocol once per class
  (Constraint 8 — reference `S3Backend` in storage_backends/__init__.py as the reference, not
  scattered branches).

## Continuity — what IS verified good (do not re-litigate)

- The **AWS-S3 + local** storage path is real and works: `boto3` CopyObject/DeleteObject IO in
  [path_utils.py](src/elt_pipeline/shared/path_utils.py), unit-tested with an in-process S3 fake.
- The **architecture is sound** — Spark writer / Iceberg L3-L4 / Trino JDBC serving, the 4-tier
  config cascade, the catalog-binding enum (glue/rest/nessie/hive/jdbc/hadoop/snowflake), the
  single-seam URI dispatch (PRD 08). Don't redesign these; extend them.
- The **test gate is green** (311/0) and the **example demo runs end-to-end** (fixed in the prior
  backlog). This effort is about matching claims to that reality, not repairing it.

# Backlog & Continuity — Publication Readiness & Platinum Hardening

<!--
  ANCHOR DOC. Durable, cold-start-resumable state for the portability / publication-readiness
  effort. Lives at repo root, NOT under docs/ (PRD 10 §11). Method + section contract:
  docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md. Operating model: ONE session per item;
  update the Resume line + Status snapshot before closing a session.
-->

## Resume (start here)

- **D-0 is DECIDED (owner, 2026-08-18): Path A now — publish honestly; Path B (multi-cloud) is
  roadmap.** So **do TRANCHE 1 first**, in order: **D-1 ✅ Done** → **I-1 *doc pass only***
  (state the real ingest surface in README/PRD: real REST + sqlite-replay demo + local/s3 object
  storage; do **not** implement Kafka/JDBC now) → **D-2** (publish the capability maturity matrix).
  Completing those three = the repo can go public with accurate scope.
- **Everything else is roadmap (tranche 2), do NOT start it in this pass:** `B-*` (multi-cloud
  storage — prefer B-6 the facade when pulled forward), `I-1` implementation (real Kafka/JDBC), and
  `G-1…G-8` (Iceberg maintenance, observability, orchestration, deployment, secrets, governance,
  OpenLineage, DQ quarantine). They stay ⏳ and get worked one-per-session after publication, each
  marked in D-2's maturity matrix as "Roadmap" until done.
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

- **Gate:** 🟢 GREEN. `bash scripts/run_tests.sh` → TEST GATE: PASS (311 passed / 0 failed);
  `uv run ruff check .` clean. This backlog does **not** start from a red gate — keep it green.
- **Captured:** 2026-08-18. Origin: a portability + platinum review. Storage IO implements
  **`s3://` + local `file://` only** (D-1 closed: PRD 10 §6 + README now state the implemented scope
  and carry an explicit multi-cloud roadmap subsection; PRD 08 was already consistent); ingest is
  local-demo-grade (real REST, sqlite-only SQL, Kafka file-replay); and the operational surface
  (Iceberg maintenance, observability, orchestration, deployment, secrets, governance, OpenLineage,
  DQ quarantine) is bronze→silver. **D-0 decided: Path A (publish honestly) now; B + G-* are roadmap.**
  The backlog + this decision are committed. **Active: tranche 1 → start at I-1 (doc pass only).**
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
2b. **The S3 path is the reference implementation.** Any new backend mirrors the S3 branch in
   *every* scheme-branching function in [path_utils.py](src/elt_pipeline/shared/path_utils.py)
   (there are ~18: `detect_scheme`, `join_paths`, `path_parent`, `path_basename`,
   `path_with_suffix`, `path_normalize`, `path_exists`, `path_is_dir`, `path_mkdir`,
   `path_listdir`, `path_glob`, `path_rglob`, `path_content_length`, `path_read_bytes`,
   `path_write_bytes`, `path_open_for_append`, `path_replace`, `path_delete_tree`) — plus the
   staging-swap paths in [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py). Miss one
   and the backend silently half-works.
3. **Never silently coerce schemes** (PRD 08): an unsupported scheme still fails fast and sharp.
4. **Docs are a source of truth, not marketing.** A public claim the code can't back is a defect.
   Every doc edit must leave PRD 08 and PRD 10 mutually consistent.
5. **Per item, decide the direction explicitly and record it in the Done line.**
6. **Preserve the Platform strengths** (section above). A gap fix that regresses the validity
   chain, replayability, or an existing seam is not done — it's a trade-down.
7. **Implement behind existing seams where they exist.** DQ/lineage adapters, `secret_refs`,
   connector/catalog abstractions are already there — extend them, don't fork parallel paths.

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

#### B-1 — Implement GCS (`gs://`) storage IO — native client  ⏳ (Path B, strategy B1 — alternative to B-0)
- **Goal:** `bucket_path: gs://…` and `gs://` root URIs work end-to-end (L1 land, L2 parquet,
  staging-swap), config-only.
- **Cause:** `gs://` is rejected by `detect_scheme` ([path_utils.py:50](src/elt_pipeline/shared/path_utils.py#L50)).
- **Scope:** add `gs` to `_StorageScheme` + `_SUPPORTED_SCHEME_PREFIXES`; implement a `gs` branch
  in every scheme-branching function (Constraint 2b) mirroring the S3/boto3 branch, via
  `google-cloud-storage` (add dep); implement the `gs` staging-swap analog in
  [sql/_staging_swap.py](src/elt_pipeline/sql/_staging_swap.py) (GCS has no atomic rename — mirror
  the S3 CopyObject→DeleteObject leaf-partition approach); wire Spark FS (see B-4).
- **Decision needed:** client lib (`google-cloud-storage` direct, like boto3) vs `gcsfs`. Prefer
  matching the S3 pattern (direct client) for consistency.
- **Verification:** new `tests/test_path_utils_gcs.py` with a GCS fake/emulator mirroring the S3
  fake in [tests/test_path_utils.py](tests/test_path_utils.py); `bash scripts/run_tests.sh` green.

#### B-2 — Implement Azure ADLS Gen2 (`abfss://`, optionally `wasbs://`) storage IO — native client  ⏳ (Path B, strategy B1 — alternative to B-0)
- **Goal:** `abfss://…` roots work end-to-end, config-only.
- **Cause:** rejected by `detect_scheme`.
- **Scope:** same shape as B-1 but Azure — `azure-storage-file-datalake` / `adlfs` (add dep);
  the `abfss://container@account.dfs.core.windows.net/path` authority parsing is the one real
  difference from S3's `bucket/key` split (`_split_s3_path` analog). Implement all ~18 functions
  + staging-swap + Spark FS (B-4).
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

#### B-4 — Wire Spark cloud filesystem config + credential story  ⏳ (Path B only)
- **Symptom:** [spark/session.py](src/elt_pipeline/spark/session.py) sets **no** `spark.hadoop.fs.*`
  / cloud credentials for any backend — even S3 relies on ambient EMRFS/IAM. Non-EMR Spark
  clusters can't read/write S3/GCS/ADLS without this.
- **Scope:** config-driven `spark.hadoop.fs.s3a.*` / `fs.azure.*` / `fs.gs.*` + the matching
  Hadoop-cloud jars, resolved through the runtime_context cascade (never hard-coded, never
  logged). Decide the credential model (instance profile / workload identity / key via env /
  secret ref) and document it.
- **Verification:** documented per-cloud launch recipe; where feasible, an integration test that
  round-trips a small dataset against a real/emulated bucket.

#### B-5 — Cloud integration tests (prove each backend, not just fakes)  ⏳ (Path B only)
- **Symptom:** S3 is only unit-tested with an in-process fake; Azure/GCS have zero coverage.
- **Scope:** emulator-backed integration tests (moto for S3, Azurite for ADLS, fake-gcs-server
  for GCS) exercising the real IO ops end-to-end (ingest → normalize → sql → publish on a
  `<scheme>://` root); gate them behind a marker/opt-in if they need Docker so the default gate
  stays hermetic.
- **Verification:** the integration suite passes for every backend claimed as supported in PRD 10.

#### B-6 — Pluggable storage-backend facade (Path B, strategy B3 — RECOMMENDED; covers pre-Spark ingest)  ⏳
- **Goal:** one Python filesystem facade with **pluggable per-scheme backends** so every IO caller
  — the Spark-transform control plane **and the pre-Spark ingest L1 writer** — supports
  local/s3/gcs/azure(/databricks) via config only.
- **Why a facade fits this platform specifically:** IO happens in two phases. **Ingest is pure
  Python and runs before any Spark job**: [LocalLevel1Writer.write_payload](src/elt_pipeline/ingest/storage.py)
  lands raw bytes + manifests via `path_write_bytes`/`path_write_text`/`path_mkdir` (all
  [path_utils](src/elt_pipeline/shared/path_utils.py) → s3+file only), and the object-storage
  *source* reader ([local_object_storage.py](src/elt_pipeline/ingest/connectors/local_object_storage.py))
  lists/reads via `path_glob`/`path_rglob`/`path_read_bytes`. None of that has a SparkSession. So the
  delegate-to-Spark-FS strategy (B-0/B2) can't serve ingest without booting Spark early; a Python
  facade can serve both phases.
- **Scope:**
  1. Define `StorageBackend` (Protocol/ABC): `exists / is_dir / mkdir / listdir / glob / rglob /
     read_bytes / write_bytes / open_append / content_length / replace / delete_tree` + the
     scheme-preserving string ops (`join / parent / basename / normalize`).
  2. Implement `LocalBackend` (extract today's `pathlib` branches) and `S3Backend` (extract today's
     `boto3` branches) — no behaviour change, just relocation. Add `GcsBackend`, `AzureAdlsBackend`.
  3. Registry keyed by scheme; `path_utils` public functions become one-line dispatch. Preserve the
     **staging-swap leaf-partition-only replace** contract (S-2) as a backend method (object stores:
     copy→delete; local: rename).
  4. Route the ingest L1 writer + object-storage source reader through the same facade (they already
     call `path_utils`, so this is inherited once step 3 lands).
- **Tradeoffs (record the decision):** overturns [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md)
  ("no `StorageBackend` protocol/registry") — PRD 08 must be revised. Keeps IO Python-native (no
  Spark coupling, works in ingest). Adds one dependency per cloud SDK.
- **Verification:** existing gate green on local + S3 through the facade (pure refactor first);
  then per-backend unit tests (fakes/emulators, mirroring the S3 fake in
  [tests/test_path_utils.py](tests/test_path_utils.py)) + B-5 integration tests for GCS/ADLS; PRD 08 updated.

#### I-1 — Ingest connector production readiness (beyond local demo)  ⏳
- **Symptom:** the ingest layer is **local-demo-grade**, not production. The four "production"
  connector base classes ([rest.py](src/elt_pipeline/ingest/connectors/rest.py),
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
- **Design note — object storage is the universal ingress; don't over-invest in streaming.** In
  enterprise deployments, streaming ingest (Kafka/Kinesis/Event Hubs) is normally owned by
  cloud-native infra built for it — AWS Lambda event-source mapping, Kafka Connect S3 sink, Kinesis
  Firehose, Flink, Event Hubs Capture — which **lands raw files into object storage**; this pipeline
  then picks them up via the object-storage connector. So a rock-solid, multi-cloud **object-storage
  path (B-6) is the high-value work**, and a real Kafka broker consumer is a *low-priority
  convenience* (demos, small no-infra deployments), not a blocker. Prioritise B-6 over a real Kafka
  consumer.
- **Decision needed (owner), per mechanism:** which are in scope for v1 vs roadmap? Suggested:
  **(high)** multi-DB SQL ingest (JDBC via Spark `spark.read.jdbc`, or a Python driver matrix) +
  cloud object-storage sources (falls out of B-6); **(low)** a real Kafka consumer (basic capability
  only — enterprises use infra to land to object storage). At minimum, **the README/PRD must state
  the real ingest surface** (real REST + sqlite-replay demo + local/s3 object storage) rather than
  imply production Kafka/JDBC.
- **Files:** [ingest/connectors/](src/elt_pipeline/ingest/connectors/),
  [config/models.py](src/elt_pipeline/config/models.py), README/PRD.
- **Verification:** each claimed mechanism has a concrete connector + a test against a real/emulated
  endpoint; docs match the shipped surface.

### Platinum / production-hardening (operational, governance, reliability)

The architecture is platinum-grade; the operational hardening is bronze→silver. These are the
"complete platform" gaps — additive, mostly implement-behind-an-existing-seam, and largely
independent of the portability (B-*) and ingest (I-1) tranches. **None block publishing as an
OSS platform with a roadmap** (mark them roadmap in D-2's maturity matrix); they **do** block
claiming "enterprise/platinum-ready" today. Priority tags: 🔴 high · 🟠 med · 🟡 low.

#### G-1 — Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup  🔴 HIGH  ⏳
- **Symptom:** no `rewrite_data_files` (compaction), `expire_snapshots`, or `remove_orphan_files`
  anywhere in `src/`/`ops/`. Iceberg tables **degrade without this** — small-file explosion,
  unbounded snapshot/metadata growth, storage bloat, slowing every Trino read. This is the #1
  operational gap for any real Iceberg deployment.
- **Scope:** a maintenance command/module (`elt maintain …`) invoking Iceberg's Spark procedures
  (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`, optionally `rewrite_manifests`)
  per L3/L4 table, with retention config (snapshot age/count); a documented schedule to run it.
- **Files:** new `src/elt_pipeline/maintenance/` + CLI wiring; [spark/session.py](src/elt_pipeline/spark/session.py); ops docs.
- **Verification:** run maintenance against the local Iceberg warehouse; assert snapshots expired +
  files compacted + orphans removed; gate green.

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

#### G-5 — Real secrets backend (resolve_secret is a stub)  🔴 HIGH  ⏳
- **Symptom:** [rest.py:301](src/elt_pipeline/ingest/connectors/rest.py#L301) `resolve_secret()`
  literally `return secret_ref` — the `secret_refs` config is a pass-through, no Vault/KMS/AWS
  Secrets Manager/Azure Key Vault/GCP Secret Manager integration. (`redacted_fields` log-redaction
  is good and should stay.) This also blocks the cloud-credential story in **B-4**.
- **Scope:** a `SecretsProvider` seam (env / file / Vault / cloud SM) resolving `secret_ref` →
  value at run start; never logged (honour `redacted_fields`). Wire cloud FS creds (B-4) through it.
- **Files:** new `src/elt_pipeline/shared/secrets.py`; [rest.py](src/elt_pipeline/ingest/connectors/rest.py); B-4.
- **Verification:** a resolver test per provider (env + one cloud, mocked); secrets never appear in logs/audit.

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

#### D-2 — Publish an honest capability maturity matrix  🔴 HIGH (publication gate)  ⏳
- **Goal:** the single artifact that makes going public honest — a table classifying every
  capability as **Production / Demo / Roadmap**, so no reader infers more than is built. Ties
  together D-1 (portability), I-1 (ingest), and the G-* tranche.
- **Scope:** a `docs/` maturity matrix (storage backends, ingest mechanisms, catalogs, serving,
  maintenance, observability, orchestration, security/governance, DQ, lineage) with the honest
  status of each; link it from the README top. Update it as items close.
- **Verification:** every "Production" claim maps to a passing test/feature; no claim outruns the code.

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

## Gotchas (things a fresh session would otherwise re-learn)

- The gate is `bash scripts/run_tests.sh`, **not** `uv run pytest` — one JVM = one SparkSession,
  so Spark files run per-process. See [docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md](docs/maintainer/LOCAL_DEVELOPMENT_AND_RELEASE.md).
- Export `JAVA_HOME`/`PATH` first or Spark tests fail `JAVA_GATEWAY_EXITED` (env, not a defect).
- `s3a://` is deliberately rejected (PRD 08): Spark uses `s3a://` internally, but the framework's
  handoff URIs are `s3://` and EMRFS bridges them. Don't "fix" this by coercing schemes.
- A new backend that implements *most* `path_*` functions but misses one (e.g. `path_delete_tree`
  or the staging-swap analog) silently half-works — data lands but overwrite/cleanup corrupts.
  Mirror the S3 branch in **all** of them (Constraint 2b).

## Continuity — what IS verified good (do not re-litigate)

- The **AWS-S3 + local** storage path is real and works: `boto3` CopyObject/DeleteObject IO in
  [path_utils.py](src/elt_pipeline/shared/path_utils.py), unit-tested with an in-process S3 fake.
- The **architecture is sound** — Spark writer / Iceberg L3-L4 / Trino JDBC serving, the 4-tier
  config cascade, the catalog-binding enum (glue/rest/nessie/hive/jdbc/hadoop/snowflake), the
  single-seam URI dispatch (PRD 08). Don't redesign these; extend them.
- The **test gate is green** (311/0) and the **example demo runs end-to-end** (fixed in the prior
  backlog). This effort is about matching claims to that reality, not repairing it.

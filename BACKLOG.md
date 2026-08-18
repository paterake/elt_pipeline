# Backlog & Continuity — Portability & Publication Readiness

<!--
  ANCHOR DOC. Durable, cold-start-resumable state for the portability / publication-readiness
  effort. Lives at repo root, NOT under docs/ (PRD 10 §11). Method + section contract:
  docs/maintainer/BACKLOG_CONTINUITY_PLAYBOOK.md. Operating model: ONE session per item;
  update the Resume line + Status snapshot before closing a session.
-->

## Resume (start here)

- From `BACKLOG.md`: Start with **D-0** — the **owner decision** that gates everything else:
  **Path A (reconcile docs to the built S3+local scope)** vs **Path B (implement multi-cloud IO)**.
  Path B has two strategies: **B1** = native per-backend clients (B-1/B-2/B-3), **B2** = delegate
  the control plane to Spark's Hadoop FileSystem (**B-0**, recommended — makes the scheme a pure
  config knob, but revisits PRD 08 + couples IO to a live SparkSession). Do not write B-* code
  until D-0 is decided. **D-1 (doc consistency) is needed either way** and is the safe first
  coding task. Then, if Path B: **B-6** (strategy B3 — recommended facade) *or* **B-0** (B2, delegate
  to Spark FS) *or* **B-1→B-3** (B1, native clients), plus **B-4, B-5**. Separately, **I-1** tracks the
  ingest-connector production gaps (SQL = sqlite-only, Kafka = file-replay, connectors abstract) —
  scope those with the owner too; they are independent of the storage-scheme work.
- **Also note:** the storage-scheme work must cover the **pre-Spark ingest L1 write** (Python, not
  Spark) — this is why strategy B3 (Python facade) is preferred over B2 (Spark-coupled). See B-6.
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
- **Captured:** 2026-08-18. Origin: a portability review found that the storage IO layer
  implements **`s3://` + local `file://` only**, while [PRD 10 §6](docs/prd/10-prd-architecture-and-lifecycle.md)
  advertises 6 URI schemes and "runs identically on AWS/GCP/Azure/Databricks, 0 LOC." The
  code matches [PRD 08](docs/prd/08-prd-storage-root-uri-io-dispatch.md) (v1 = s3+file, reject
  the rest); PRD 10 overclaims. No commits yet for this backlog.
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

---

## Work items

### Still Todo

#### D-0 — Portability direction: reconcile-docs (A) vs implement-multi-cloud (B)  🚦 DECISION (owner)
- **Decision needed:** the multi-cloud "0 LOC on AWS/GCP/Azure/Databricks" claim is not
  implemented. Pick the resolution before any B-* code:
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

#### D-1 — Make PRD 08 and PRD 10 mutually consistent (needed in BOTH paths)  ⏳
- **Symptom:** [PRD 10 §6](docs/prd/10-prd-architecture-and-lifecycle.md) (lines ~31, 245, 251-254)
  claims 6 URI schemes and "runs identically on AWS/GCP/Azure/Databricks/Polaris, 0 LOC";
  [PRD 08 §P2](docs/prd/08-prd-storage-root-uri-io-dispatch.md) scopes v1 to `s3://` + `file://`
  and mandates the other schemes fail fast. The code follows PRD 08. README frames it as
  "local-first … typically parquet for local workflows."
- **Decision:**
  - **If Path A:** rewrite PRD 10 §6 + the cloud-portability table to state the *implemented*
    scope (S3 + local; catalogs glue/rest/nessie/hive/jdbc/hadoop). Move the multi-cloud storage
    matrix into an explicit "Roadmap / not-yet-implemented" subsection. Align the README.
  - **If Path B:** update PRD 08's "v1 scope" to include the schemes you implement, and keep
    PRD 10 in sync as each backend lands (don't claim a backend before its B-item is Done).
- **Files:** [docs/prd/10-prd-architecture-and-lifecycle.md](docs/prd/10-prd-architecture-and-lifecycle.md),
  [docs/prd/08-prd-storage-root-uri-io-dispatch.md](docs/prd/08-prd-storage-root-uri-io-dispatch.md),
  [README.md](README.md).
- **Verification:** docs review — grep both PRDs for scheme lists and confirm they agree with
  `_SUPPORTED_SCHEME_PREFIXES` in [path_utils.py](src/elt_pipeline/shared/path_utils.py); no
  cloud is claimed whose B-item is not Done.

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
  - **Kafka — file replay, not a broker.** `LocalKafkaConnector` reads a local JSONL log via
    `path_read_text` ([local_kafka.py:71](src/elt_pipeline/ingest/connectors/local_kafka.py#L71));
    there is no `confluent_kafka`/`KafkaConsumer`, no `bootstrap.servers` connection.
  - **REST — real.** `LocalRestConnector` uses `urllib.request` against any URL (auth + pagination
    modes exist). This one is genuinely usable.
  - **Object storage — s3 + local dir only** (source read via `path_utils`; same scheme limit as B-6).
- **Decision needed (owner), per mechanism:** which are in scope for v1 vs roadmap? Likely:
  real multi-DB SQL ingest (JDBC via Spark `spark.read.jdbc`, or a Python driver matrix), a real
  Kafka consumer, cloud object-storage sources (falls out of B-6). At minimum, **the README/PRD must
  state the real ingest surface** (REST + sqlite-replay demo + local/s3 object storage) rather than
  imply production Kafka/JDBC.
- **Files:** [ingest/connectors/](src/elt_pipeline/ingest/connectors/),
  [config/models.py](src/elt_pipeline/config/models.py), README/PRD.
- **Verification:** each claimed mechanism has a concrete connector + a test against a real/emulated
  endpoint; docs match the shipped surface.

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

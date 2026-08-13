# PRD 08: URI-Aware Storage Root Paths and Explicit-Config I/O Dispatch

## Document Status

- **Status:** Approved v1
- **Product area:** `elt_pipeline` / cross-stage (ingest → level1 → level2 → sql → publish)
- **Scope:** Platform-wide storage contract and I/O dispatch for stage root paths, object paths, and bucket paths
- **Design precedence:** This PRD codifies the conventions already used in `mercell` and `camelot` repositories. It is a **correction** to the earlier local-POSIX-only implementation in `elt_pipeline`, which incorrectly assumed `pathlib.Path` for all path operations.

## Purpose

This PRD defines the contract for **storage root paths** (`--root-path`, `--warehouse-root`, `bucket_path`, and level-to-level handoff paths) in `elt_pipeline`.

The non-negotiable rules set by this PRD are:

1. **Root paths are always defined explicitly in configuration or CLI flags, as string URIs.**
   - The codebase must **never infer**, **never guess**, and **never assume** a local filesystem.
   - The scheme/prefix is the single decision point for routing I/O; everything else flows directly from that string.
2. **Root paths are passed through the function call graph as plain strings.**
   - No `pathlib.Path` wrapping is ever applied to root paths or joined child paths, because `pathlib.Path` collapses URI schemes and breaks `s3://bucket/prefix` semantics.
3. **Path concatenation is a scheme-preserving string join, not a `Path` operator.**
   - `join_paths(root, *segments)` is the single authority for building a child path string.
4. **Leaf I/O (read/write/list/exists) dispatches on the path string's scheme prefix or lack thereof.**
   - `s3://bucket/...` → use Spark's native S3 handling for Spark parquet calls; use `boto3` at the Python I/O layer.
   - `file:///abs/path` or any bare POSIX path (starts with `/`, `./`, or a relative non-URI string) → use `pathlib.Path` *at the leaf I/O call site only*, after stripping the optional `file://` scheme.
5. **Level-to-level handoff paths always contain the full explicit URI, including the scheme and root prefix.**
   - Example: `s3://bucket/data/elt/prod/level1/source=orders/entity=line_items/ingest_date=20260813/run_id=.../data.json` is passed verbatim and handled as-is.
   - No re-writing, no stripping, no prefixing. Sharp and direct, exactly matching the convention used in `mercell` / `camelot` for level-1 → level-2 file handoff.
6. **We do not implement cloud I/O primitives ourselves where Spark or Hadoop SDKs already ship them.**
   - Spark `read.parquet("s3://…")` / `write.parquet("s3://…")` is the authority for parquet R/W on object storage. On EMR, this uses EMRFS natively with IAM role credentials.
   - Python-level metadata, audit, manifest, and small-object writes use `boto3` (standard on EMR) directly, not a re-implementation of S3 multipart logic.

This PRD restores the correct design that was incorrectly descoped earlier (as "object storage URIs for level2+") and brings `elt_pipeline` into alignment with its sibling repos.

This document is **normative.** Any stage PRD, TODO backlog item, or implementation that conflicts with this document must be updated to conform.

## Background

In sibling repositories `mercell` and `camelot`, the storage contract is sharp and explicit:
- Every pipeline config defines one or more root URIs (typically `s3://<bucket>/<prefix>` for cloud runs, `file:///<absolute-path>` for local development).
- The execution engine concatenates segments onto that root string for every path it builds.
- Leaf operations dispatch based on the prefix string.
- Level-1 object handoff always carries full absolute URIs with scheme and root included, so level-2 consumers never need to "figure out" where an object lived.

The initial `elt_pipeline` implementation landed with `pathlib.Path` used universally for root paths and all path joins. This was correct for POSIX local development but broke the sibling-repo contract:
- `Path("s3://bucket/prefix") / "level2" / "source=orders"` → collapses to `s3a:/bucket/prefix/level2/source=orders` **single-slash broken**, interpreted as a local directory named `s3a:`.
- `Path.exists()` on a `s3://…` string → returns `False` locally and throws on EMR if the path is misinterpreted.
- The `local_object_storage` connector hard-requires `.is_dir()` on `bucket_path`, rejecting S3 outright.
- 23 files across the codebase import `pathlib.Path` and use the `/` operator for root joins, making the code structurally unable to handle URI schemes without a coordinated change.

This PRD is the correction scope: make the root path contract match the sibling repo conventions. The change is architecture-level (string path semantics, not code features) but the implementation is mechanical.

## Non-Negotiable Design Principles (inherited from `mercell` / `camelot`)

### P1: No Inference, Ever
Given a pipeline that runs correctly on a laptop:
```yaml
ingest:
  sources:
    - connector_type: object_storage
      object_storage:
        bucket_path: /Users/you/data/raw/orders
```
the same pipeline running in AWS EMR should be:
```yaml
ingest:
  sources:
    - connector_type: object_storage
      object_storage:
        bucket_path: s3://my-data-lake/raw/orders
```
with **zero other changes** in the YAML, SQL, or manifest content. The `bucket_path` string's scheme is the only thing that changes; the code handles the rest. The user must never be forced to add a new `execution_mode` field, a new YAML branch, or a conditional "if cloud use S3 connector, otherwise use local connector" section in their config.

### P2: Scheme Is the Single Routing Key
The routing decision must be one boolean check: `if path_str.startswith("s3://")`. No URI parsing library, no scheme registry, no `StorageBackend` protocol/registry, no URL parser extracting netloc vs path, no plugin system.

Supported schemes explicitly in scope for v1 of this PRD:
- `s3://` → AWS S3 (uses `boto3`; EMR credentials handled by IAM role, no access key management required)
- `file://` and un-prefixed local POSIX paths → local filesystem (handled via `pathlib.Path` at the leaf call site, **not** propagated back into the call graph)

Unsupported schemes (e.g. `s3a://`, `gs://`, `hdfs://`, `wasbs://`) MUST fail **fast and sharp** with a clear error:
```
Unsupported storage scheme in path: 's3a://bucket/prefix'.
Supported schemes: s3:// (AWS S3), file:// (explicit local POSIX), or a bare local POSIX path (no scheme).
Note: Spark internally uses s3a:// for some configurations; on EMR use s3:// and let EMRFS handle it.
```
Never silently reinterpret. Never coerce `s3a://` to `s3://` automatically. If a user types the wrong scheme, tell them immediately.

### P3: Pass Path Strings Through the Call Graph, Not Path Objects
- All function signatures that accept a root path must type-annotate as `str`, not `Path`.
- Child paths are built exclusively via `join_paths(root_str, *segments)` and returned as `str`.
- A leaf function that performs actual POSIX I/O may convert a **local file path** to `Path` immediately before the I/O call. This conversion must not "escape" up the call graph. Spark I/O calls receive the raw string path directly and call `.parquet(str_path)` with zero `Path` intervention.

### P4: Use Vendor/Native Capabilities, Don't Re-Invent
- Spark already handles parquet R/W on S3/HDFS/etc. via `str` paths: **use it directly, do not wrap, do not pre-validate, do not convert to Path.**
- On EMR, S3 credential handling is the responsibility of the EMR instance profile / step role: **do not require or read `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars by default.** The default `boto3` client uses the standard credential chain (which on EMR picks up the role automatically). If a user is running outside EMR on their laptop and wants to test S3, they configure credentials via `aws configure` or env vars as normal.

### P5: Level-to-Level Handoffs Always Carry Full Absolute URI
Where a prior stage writes an output file reference to a manifest (L1 manifest → L2 reader; L2 table catalog → L3 SQL source; L4 dataset path → publish read), the reference must be the **full absolute URI including scheme and root prefix**. The consumer must not be forced to "re-prefix" it relative to a root. Matching the Mercell/Camelot pattern.

This does not mean the L1 manifest `data_path` is always a global URI. The existing `relative_to` behavior can remain for file POSIX cases **if and only if** the manifest also records the `storage_root` it was produced under, such that re-reading from another context reconstructs the full URI. Implementation can choose between (a) absolute URIs always, or (b) (relative_path, root_uri) tuple in the manifest. Either is acceptable as long as level-to-level reading does not require external context or user-supplied configuration to locate the bytes.

## Anti-Scope (Deliberately NOT in this PRD)

- **Support for schemes beyond `s3://` and POSIX local.** GCS, Azure Blob, HDFS native URI, ADLS Gen2, and any others are out of scope. The hard stop on unsupported schemes preserves correctness without artificially expanding scope. (They can be added later with a tiny 2-line change to the scheme check, following exactly the same pattern.)
- **A plugin/registry system for new schemes.** Not needed. Add a branch in the dispatcher when/if required. Simplicity over extensibility.
- **Catalog, metastore, or table-format changes.** Delta Lake, Iceberg, Hive Metastore, AWS Glue catalog integration are out of scope and tracked under separate future PRDs. This PRD changes only the path-representation + I/O dispatch layer.
- **Changes to SQL model semantics, partition conventions, lineage column rules, load modes, or quality/lineage backend behavior.** All of those remain as approved and are unaffected by this PRD. They operate on dataframes once Spark has read them; this PRD is only about locating the bytes that feed into Spark and writing out the non-parquet metadata/audit artifacts.
- **Mount products.** Mountpoint-S3, s3fs-fuse, HDFS NFS gateway, EMRFS-at-local-path, or any other FUSE-style POSIX-over-object-storage product are explicitly **not required** for this PRD to pass. The user's declared operating model is "no mounts, pure URI flags."
- **`elt_pipeline_cfg` centralized config service.** Still out of scope / separate future PRD. This PRD works the same way as today with per-pipeline YAML files passed as CLI args or as `s3://…` URIs to `load_pipeline_config`.
- **Atomic-write on S3 semantics beyond standard PUT overwrite.** S3's native PUT overwrite for objects is sufficient (atomicity is per single object). The existing `*.tmp → replace` pattern for local files should be implemented for S3 as: write the candidate object to a `{key}.tmp` path via standard PutObject, then COPY to the destination key and DELETE the `.tmp` copy — a safe and standard pattern. We do not implement multipart commit protocols here.

## Scope of Changes (Implementation Topology)

### Deliverable 0: New Shared Module `elt_pipeline/shared/path_utils.py`
Small, sharp, testable. ~80 lines. Contains:
```
join_paths(root: str, *segments: str) -> str:
    # Scheme-preserving string join. Normalizes single '/' between segments.
    # Does NOT touch pathlib, does NOT parse URI components beyond "scheme:".

strip_file_scheme(path: str) -> str:
    # "file:///tmp/x" → "/tmp/x" ; "s3://b/x" → unchanged (pass-through)
    # Used at leaf POSIX I/O sites only.

class _Scheme(str, Enum):  # internal only
    s3 = "s3"
    file = "file"
    local_unschemed = "local_unschemed"

def detect_scheme(path: str) -> _Scheme:
    # Sharp dispatch. Fail fast on unsupported.

def path_exists(path: str) -> bool:  # dispatch by scheme
def path_is_dir(path: str) -> bool:  # dispatch by scheme
def path_mkdir(path: str, *, parents: bool = True, exist_ok: bool = True) -> None:
    # S3: no-op (prefixes are not real directories). Local: Path.mkdir(...)
def path_listdir(path: str) -> list[str]:  # returns full URI strings
def path_glob(base: str, pattern: str) -> list[str]:
def path_rglob(base: str, pattern: str) -> list[str]:  # S3 = list-objects-v2 paginated
def path_read_bytes(path: str) -> bytes:
def path_read_text(path: str, encoding: str = "utf-8") -> str:
def path_write_bytes(path: str, data: bytes, *, atomic: bool = True) -> None:
def path_write_text(path: str, data: str, encoding: str = "utf-8", *, atomic: bool = True) -> None:
def path_open_for_append(path: str, encoding: str = "utf-8") -> ContextManager[IO[str]]:
    # For JSONL append writes. S3: read existing, concat, atomic rewrite. That is acceptable
    # because JSONL append files in this codebase are small (audit logs, events) — not data payloads.

def path_replace(src: str, dst: str) -> None:  # S3: copy-then-delete-src; local: os.replace
def path_with_suffix(path: str, suffix: str) -> str:  # string op, no pathlib
def path_parent(path: str) -> str:
def path_basename(path: str) -> str:
def path_relative_to(path: str, base: str) -> str:  # string strip, asserts path starts with base + "/"
def path_normalize(path: str) -> str:  # "resolve" equivalent. POSIX realpath on local; s3 identity + collapse // → /
```
Internal use of `boto3` only inside the S3 branches. Never imported at the top-level (lazy import inside scheme branches so users with no boto3 installed can still run local POSIX pipelines fine).

### Deliverable 1: CLI Root Types + Resolution (`src/elt_pipeline/cli.py`)
- Change all argument parsers: `--root-path`, `--warehouse-root`, `--kafka-log-path`, `--plan-path`, `bucket_path` (via config) → `type=str` instead of `type=Path`.
- Replace all `Path(...).resolve()` calls with `path_normalize(str_path)`.
- CLI passes `root_path: str`, `warehouse_root: str` into every stage runner, never `Path`.

### Deliverable 2: Root-Join Sites (23 files with `from pathlib import Path`)
For each of the 23 files:
- Audit each root + segment join pattern. Replace with `join_paths()` from the path_utils module.
- Keep `Path` imports only when the module performs **leaf local I/O** and uses `strip_file_scheme()` + `Path(stripped_local_path)` at the immediate call site. For modules that only join but don't I/O, the `Path` import can be removed entirely.
- For strings that are handed to Spark R/W: hand them directly as strings, unchanged, to `spark.read.parquet(path_str)` / `.write.parquet(path_str)`. Spark already handles the scheme routing.

### Deliverable 3: Ingest Object-Storage Connector (`ingest/connectors/`)
- Remove the explicit `.is_dir()` validation guard at [local_object_storage.py:42-59](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/src/elt_pipeline/ingest/connectors/local_object_storage.py#L42-L59).
- Replace bucket listing with `path_rglob()` / `path_listdir()` dispatcher.
- Replace per-object read with `path_read_bytes()` dispatcher.
- Rename note: the class `LocalObjectStorageConnector` has "Local" in its name, which is misleading after this PRD. Either (a) rename to `ObjectStorageConnector` and drop the base/split pattern, or (b) keep the class name for backward compat but route internally through the scheme dispatcher. Implementation call during deliverable. Keep in mind: per P1 ("No Inference"), the user does NOT change their config to select a different connector class; the routing is inside the existing connector code path and driven only by `bucket_path`'s scheme.

### Deliverable 4: Storage and Metadata Writes (ingest_storage, ingest_state, normalize_storage, level2_storage, shared artifact store used by quality/lineage)
- All `.mkdir() / .write_text() / .write_bytes() / .open("a") / .replace() / .with_suffix() / .exists()` → dispatch to path_utils equivalents.
- Existing `_write_json_file()` / `_append_jsonl_file()` helpers in these modules are thin wrappers; rewrite them to use `path_write_text()` / `path_open_for_append()` and drop their own `Path()` usage internally.

### Deliverable 5: SQL Stage (level2_source discovery, spark_executor path join, discovery, runtime)
- `level2_source.py` parent-directory discovery: replace `.glob()` + `.is_dir()` with dispatched glob + exists.
- `spark_executor.py` `_table_path()`: replace `warehouse_root / stage.value / table_name` with `join_paths(warehouse_root, stage.value, table_name)` returning a string.
- All `str(target_path)` / `str(dependency_path)` calls that feed into Spark: keep them, they already work with string paths; no change.

### Deliverable 6: Publish Stage (runtime path resolution, output writes, discovery)
- `_resolve_run_scoped_output_path` and `_resolve_stable_delivery_path` use `join_paths()`.
- CSV/TSV/JSONL output writes use path_utils writes.
- Publish discovery uses path_utils `path_rglob` for manifest scanning.

### Deliverable 7: Safety + Hardening
- In `pyproject.toml`, add optional extras:
  ```toml
  [project.optional-dependencies]
  s3 = ["boto3>=1.34"]
  emr = ["elt_pipeline[s3,spark]"]
  ```
  (No change is required for default local POSIX pipelines; boto3 is not required unless users use `s3://` paths, at which point they should install with either `[s3]` or `[emr]` — or just run on EMR, where boto3 is pre-installed.)
- Add a smoke-test / guard function `validate_config_root_schemes(root_path, warehouse_root, bucket_paths)` that runs early in CLI execution: it calls `detect_scheme` on every provided root; if any scheme is the unsupported fail-fast list, raise immediately with the friendly error message above. Fail before touching any I/O or starting any Spark context.

## Success Criteria

1. **Static behavior preservation:** Local POSIX pipelines run identically to today. No functional regression for `uv run pytest tests/ -k "not spark_integration"` — **all 37 non-Spark tests green with zero test changes.**
2. **Unit tests for the new layer:** Dedicated `tests/test_path_utils.py` or equivalent with:
   - `join_paths` edge cases: `s3://bucket` + empty segments; trailing slash in root; leading slash in segment; nested segments for `s3://`, `file://`, and bare POSIX paths; double-`//` collapse.
   - Scheme detection: correct `s3/file/local_unschemed` detection; unsupported schemes like `s3a://` raise the exact supported-schemes error.
   - Local POSIX round-trip: `path_write_text` / `path_read_text` matches current `Path.write_text/.read_text` behavior (tmp atomic replace, etc.).
   - Local POSIX glob/listdir/rglob matches pathlib's behavior for pre-canned fixture directories.
   - At least one mocked S3 test (moto or fake boto3) verifying the scheme branch calls `list_objects_v2` pagination and `get_object`/`put_object` without hitting the POSIX code path.
3. **Spark I/O unchanged:** existing spark tests (where runnable with JVM) continue to pass with string root paths; no change in Spark execution semantics.
4. **EMR E2E green (on a JVM-equipped workstation + AWS account):** Run the full `examples/configs/local_object_storage_orders.yaml` pipeline with `--root-path s3://<test-bucket>/elt_pipeline_test --warehouse-root s3://<test-bucket>/elt_warehouse_test` and `bucket_path: s3://<test-bucket>/raw_demo_inputs/orders/` end-to-end (ingest → normalize → sql run → publish run), **no mounts, no FUSE, no HDFS**, and verify:
   - Level1 data, L1 manifests, level2 parquet, mapping catalogs, audit logs, run artifacts all land in the `s3://…` root prefix.
   - L3 canonical model and L4 summary model parquet files land in `warehouse_root` prefix with correct `partitionBy` layout.
   - Level5 publish exports (CSV) land correctly and are readable via `aws s3 cp` + standard `cat`/`head`.
5. **Lint + IDE diagnostics clean:** `ruff check` passes on all touched files; `GetDiagnostics` returns zero errors.
6. **Cross-ref update:** Top-level tracker at `docs/todo/TODO.md` lists this TODO as active implementation backlog. Archived prior TODOs' descoped-object-URI notes cross-reference this PRD as the approved-scope restoration (not descoped anymore).
7. **Operator documentation:** [LOCAL_OPERATOR_RUNBOOK.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/operator/LOCAL_OPERATOR_RUNBOOK.md) gets a new § *Cloud Native (No-Mounts) EMR Execution Pattern* describing the root-path convention, scheme routing, IAM-role credential note, and the 4-stage CLI invocation pattern with `s3://` URIs.

## Implementation Order (Gates)

The implementation must follow this gated order. Each gate is a complete, independently reviewable + testable increment:

1. **Gate 0 (path_utils module + tests):** Deliverable 0 plus the unit tests. All dispatch functions defined; local POSIX branch passes 100% on filesystem fixtures; S3 branch mocked and exercised. No other files touched yet.
2. **Gate 1 (CLI typing only):** Deliverable 1. Change only CLI arg types, root resolution, and signatures of the top-level runner entry points. All root flow-ins must be `str` by the end of this gate. No behavior change yet because downstream call sites are still updated next.
3. **Gate 2 (connector + storage modules):** Deliverables 3+4. Object-storage connector plus all shared storage write paths.
4. **Gate 3 (SQL + Publish stages):** Deliverables 5+6.
5. **Gate 4 (Full sweep + hardening):** Audit every remaining `from pathlib import Path` in the 23 files. Any stray `root_path / "X"` joins are fixed. Add scheme-validation guard, optional extras, docs/runbook.
6. **Gate 5 (Verification):** Run success criteria 1–5 locally + on EMR.

This order ensures at no point is the repo in a "half-broken" state: each gate moves one concern over to the new string contract without touching others.

## Operator Consequences and Rollback

Rollback strategy is trivial since the change is internal I/O dispatch without changing YAML/SQL contracts:
- If a bug is discovered in S3 dispatch after release but before broad usage: local POSIX pipelines are **entirely unaffected** because they do not exercise that code branch. The POSIX branch is the same `pathlib.Path` semantics, only wrapped in a dispatcher function.
- Rollback unit of work: revert the commit range.

## References and Precedents

- `mercell` repository: config-driven root URI string + scheme-dispatch pattern (source of truth for this PRD's conventions).
- `camelot` repository: identical conventions, specifically full-URI level-object handoff between stage 1 and stage 2.
- Existing PRDs in `docs/prd/`: [00-prd-platform-principles.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/00-prd-platform-principles.md) (Client Neutrality, Layered as Contract), [01-prd-ingestion-raw-to-level1.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/01-prd-ingestion-raw-to-level1.md), [02-prd-level1-to-level2.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/02-prd-level1-to-level2.md), [03-prd-sql-level2-to-level3-and-level3-to-level4.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md), [06-prd-level4-to-level5-publish-and-export.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/prd/06-prd-level4-to-level5-publish-and-export.md).
- Archived backlog [TODO_PATHING_COMPLETED.md](file:///Users/Rakesh.Patel/Documents/__code/git/emailrak/elt_pipeline/docs/todo/archive/TODO_PATHING_COMPLETED.md) previously marked object-store URIs as "descoped deferred." With this PRD's approval they are restored to approved scope.

## Document Control

- Approved v1, 2026-08-13: Initial version. Restores correct storage-root convention from sibling repos; replaces prior descoped-item note with active approved scope.

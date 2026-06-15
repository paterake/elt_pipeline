# Implementation Source Provenance

## Purpose

This file captures the implementation-provenance baseline for `elt_pipeline`.

It is a continuity document intended to support detailed, evidence-based review of how the current implementation was derived and validated.

This file records:

- which legacy codebases informed the design and implementation
- how those sources were used (high-level derivation approach)
- how legacy configuration concepts map to the current `elt_pipeline_cfg` contract
- guardrails that ensure the repository remains client-neutral

## Scope

This provenance capture is about **inputs to implementation thinking**, not a statement that any specific legacy repository’s behavior is preserved byte-for-byte.

The current authoritative requirements remain the PRDs in `docs/prd/` and the current code under `src/elt_pipeline/`.

## Client-Neutral Guardrails

`elt_pipeline` remains client-neutral.

The provenance captured here must not be interpreted as permission to:

- copy proprietary code verbatim
- copy configuration verbatim
- embed client identifiers into runtime contracts
- depend on archived repositories at runtime

Legacy sources are treated as design baselines only.

## Legacy Sources Used (Redacted Identifiers)

The current implementation was derived from a set of archived repositories, grouped as:

- **Legacy ingestion runtime(s)**: prior ELT ingestion framework(s) and connector implementations
- **Legacy config repository(s)**: prior YAML configuration contracts that informed the current config schema and layering approach

The repository identifiers and local machine paths for those archives are intentionally kept out of this committed file to preserve the client-neutral contract.

### Local Machine Source Map (Not Committed)

For a reviewer working on a specific workstation, create a local-only file:

- `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.local.md`

This file is expected to contain workstation-specific absolute paths to the archive repositories that were used as baseline references during the 36-session implementation loop.

That local file is ignored by `.gitignore` and should not be committed.

## Review Breakdown (Legacy Baselines)

This section defines the review breakdown for the legacy baselines that were referenced during implementation.

Use it to structure a detailed review of how the current `elt_pipeline` runtime aligns to (or intentionally diverges from) prior approaches.

The entries below use **module / directory identifiers** only. Reviewers should use `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.local.md` to locate the workstation-specific absolute paths for each legacy repository.

## Review Positioning (How To Weight The Baselines)

Use this as guidance for review emphasis:

- Treat `edp-elt-ingestion-main` as the newer “baseline standard” reference for **level2+ handling** and the raw→parquet relationalisation approach.
- Treat `bi-bdp-elt` as a strong reference for **ingestion breadth** (multiple source families, SQL-based databases, and envelope/message handling patterns).
- Treat both as cautionary references for **config and path derivation overengineering** (complex logic to infer/construct write locations and to process config).

### Ingestion Approaches

Focus: ingestion connector patterns, checkpointing, retries, decoding/parsing, and “raw landing” output mechanics.

Legacy baseline modules to review:

- `bi-bdp-elt/ingest-*`
- `edp-elt-ingestion-main/edp-elt-ingest-*`

### Level 1 -> Level 2 Approaches

Focus: normalization/relationalisation from raw landing into a structured parquet/table layer; schema evolution; table naming; partition strategy.

Legacy baseline modules to review:

- `bi-bdp-elt/transform-ingest`
- `edp-elt-ingestion-main/edp-elt-transform-ingest`

Review note:

- When comparing “raw → initial parquet relationalised outputs”, weight `edp-elt-transform-ingest` more heavily as the primary baseline reference.

### Level 2 -> Level 3/4/5 Approaches

Focus: SQL-driven transforms, warehouse/datamart materialization, view creation, downstream publish/export patterns (if present), and “data exists / ready” checks.

Legacy baseline modules to review:

- `edp-elt-ingestion-main/edp-elt-transform-sql`
- `bi-bdp-elt/transform-glue` (contains a `level2 -> level3` relationalize/write pattern)
- `bi-bdp-elt/transform-cfg` (SQL transform configuration handling)
- `bi-bdp-elt/transform-check` (data-existence checks for SQL transform outputs)
- `bi-bdp-elt/transform-report` (level4 view/report materialization patterns)
- `bi-bdp-elt/utility-athena` (Athena execution utilities used by transform/report tooling)

Notes on ambiguity:

- The `bi-bdp-elt` baseline does not present as a single `transform-sql` module; instead, SQL-transform behavior appears split across config, execution utilities, checks, and reporting/view materialization.
- During review, treat the `bi-bdp-elt` “level2 -> level3/4” story as the combination of:
  - `transform-glue` for L2->L3 parquet writing + relationalisation patterns, and
  - `transform-cfg` + `utility-athena` + `transform-check` + `transform-report` for the SQL/materialization and consumer-facing level4 patterns.

### Level 4 -> Level 5 (Consumer Delivery) Approaches

Focus: consumer-facing delivery workflows that package, push, or otherwise emit “pickup-ready” outputs derived from analytical tables/views.

Legacy baseline modules to review:

- `bi-bdp-elt/consume-*`

Notes:

- In the `bi-bdp-elt` baseline, `consume-rest` executes configured SQL (via Athena), then emits consumer-facing payloads by POSTing to an external API and persisting the resulting response payloads as JSON artifacts.
- Treat these `consume-*` modules as the closest baseline analog to “delivery/publish” behavior, rather than raw ingestion.

## Config Mapping: Legacy CFG -> `elt_pipeline_cfg`

The current platform config contract is implemented as a separate sibling configuration repository:

- `../elt_pipeline_cfg`

The mapping intent is:

- legacy “cfg” repositories informed the current schema and layering approach
- current authoritative config behavior is implemented via:
  - YAML parsing and schema validation in `src/elt_pipeline/config/`
  - runtime config layering (global defaults → environment overlay → source → entity)
  - runnable example configs under `examples/configs/`

## Known Pain Point (Legacy Baselines): Config and Path Derivation

Both legacy baselines implement complex config processing and path derivation logic (often computing write locations from multiple config fragments and inferred context).

During review, treat “config overengineering” and “path derivation complexity” as explicit anti-patterns to avoid re-introducing:

- prefer explicit, validated config inputs over inferred paths
- keep path layout rules small and deterministic
- ensure the config repository (`elt_pipeline_cfg`) is the single source of truth for config, rather than scattering config interpretation across many runtime modules

## How The Legacy Sources Were Used

The legacy sources were used as:

- a baseline inventory of connector patterns (REST, SQL, object storage, Kafka)
- a baseline inventory of checkpoint/state patterns
- a baseline inventory of runtime concerns (retryability, classification, operator evidence)

They were not used as runtime dependencies.

## Review Checklist

Use this checklist when conducting the detailed review:

1. Confirm the authoritative requirement set is the PRDs under `docs/prd/`.
2. Confirm no runtime code depends on archived repositories or archived config at runtime.
3. Confirm `elt_pipeline_cfg` remains the only supported config repository for this runtime.
4. For each connector family, validate the implemented contract against the PRDs and example configs.
5. For each stage (ingest, normalize, sql, publish), validate:
   - audit artifacts
   - logs
   - lineage
   - error taxonomy consistency
6. Confirm optional integrations (lineage/orchestration/quality) remain optional and local-first.

## Notes

- The implementation backlog in `docs/todo/IMPLEMENTATION_BACKLOG.md` is complete for the currently approved scope.
- The completed backlog snapshot is archived in `docs/todo/archive/IMPLEMENTATION_BACKLOG_COMPLETED.md`.
- This provenance file exists to support a post-completion review of how the implementation was derived, not to expand scope.

## Current Implementation Pointers (Review Anchors)

Use these as review anchors when comparing the current implementation to the legacy baselines and PRDs.

- Config loading + schema contract: `src/elt_pipeline/config/`
- Ingestion runtime + connector families: `src/elt_pipeline/ingest/`
  - Connectors: `src/elt_pipeline/ingest/connectors/`
  - Checkpoint/state: `src/elt_pipeline/ingest/state.py`
  - Raw landing storage layout: `src/elt_pipeline/ingest/storage.py`
- Level1->Level2 normalization: `src/elt_pipeline/normalize/`
- SQL discovery/compile/execute (Level2->Level3/4): `src/elt_pipeline/sql/`
- Publish/export runtime (Level4->Level5): `src/elt_pipeline/publish/`
- Shared contracts (audit, errors, logging, lineage, runtime): `src/elt_pipeline/shared/`

## Evidence Capture During Provenance Review

Collect and validate evidence using the runtime’s own artifacts rather than relying on memory or baseline assumptions.

Recommended evidence sources:

- Example configs: `examples/configs/`
- Example SQL manifests/models: `examples/sql/`
- Publish manifests: `examples/publish/`
- Local run artifacts (audit, logs, lineage): `runs/`

For each stage under review (ingest / normalize / sql / publish), verify:

- audit artifacts include the expected identifiers and timestamps for the run
- logs are sufficient to reconstruct “what happened” without client-specific context
- lineage is emitted consistently and remains optional/local-first
- error taxonomy matches the PRDs and remains stable across connectors

## Local-Only Source Map Guidance (Not Committed)

In `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.local.md` (ignored by `.gitignore`), record reviewer-local details needed to reproduce the provenance review:

- absolute filesystem path to each legacy archive repository
- the archive’s “frozen” identifier (commit hash, tag, or archive label), if available
- any workstation notes required to run searches locally (e.g., monorepo layout quirks)

Do not include client identifiers or proprietary configuration payloads in committed files.

## Provenance Review Workflow

Execute the provenance review in a consistent order so conclusions are tied to current requirements and current runtime evidence.

1. Populate `docs/todo/IMPLEMENTATION_SOURCE_PROVENANCE.local.md` with reviewer-local archive paths and frozen identifiers.
2. Select one review area at a time:
   - ingest
   - normalize
   - sql
   - publish
3. Re-read the corresponding PRD section under `docs/prd/` before inspecting either the current code or the legacy baseline.
4. Identify the current implementation anchor in `src/elt_pipeline/` and the relevant legacy baseline module(s).
5. Compare behavior at the level of:
   - runtime contract
   - config contract
   - evidence emitted by the run
   - known intentional simplifications
6. Validate the comparison against current example assets and local run artifacts rather than relying on memory.
7. Record findings as one of:
   - aligned baseline reuse
   - intentional divergence
   - open question
   - follow-up candidate for PRD/backlog

## Recommended Review Record Format

For each review area, capture notes in a structured format so the result is auditable and easy to hand off.

Suggested record fields:

- `Review area`: ingest / normalize / sql / publish
- `Current anchor(s)`: current runtime modules reviewed under `src/elt_pipeline/`
- `PRD anchor(s)`: requirement documents used as the authoritative reference
- `Legacy baseline(s)`: redacted module identifiers from the archive repositories
- `Evidence inspected`: configs, manifests, logs, audit artifacts, lineage artifacts, or run outputs used during the review
- `Observed alignment`: where the current implementation clearly follows a legacy pattern that still matches the PRD
- `Intentional divergence`: where the current implementation deliberately differs for simplicity, client-neutrality, or correctness
- `Risk or ambiguity`: anything that needs clarification, deeper validation, or PRD expansion
- `Outcome`: accepted / needs follow-up / convert to backlog candidate

## Divergence Classification Guidance

Not every difference from a legacy baseline is a problem.

Classify differences using the following rules:

- `Accept as intentional divergence` when the current implementation is simpler, more explicit, or more client-neutral while still satisfying the PRD.
- `Accept as modernization` when the legacy baseline used older operational patterns and the current implementation replaces them with clearer contracts or better evidence capture.
- `Flag for review` when a legacy capability appears materially required by a PRD but is not represented in the current implementation or examples.
- `Convert to PRD/backlog candidate` when the review reveals genuinely valuable behavior that is outside the currently approved scope.

## Review Boundaries

During provenance review, do not treat the existence of a legacy pattern as sufficient reason to expand scope.

Specifically:

- do not reintroduce client-specific config shapes into `elt_pipeline_cfg`
- do not rebuild inferred path derivation logic just because it existed historically
- do not treat legacy module count or feature sprawl as a target architecture
- do not mark a review item as a defect unless it conflicts with the PRDs or the current documented contract

## Completion Criteria

The provenance review can be considered complete when all of the following are true:

- each stage (`ingest`, `normalize`, `sql`, `publish`) has a recorded review pass
- each review pass references the authoritative PRD sections that were used
- each review pass identifies the current implementation anchors that were inspected
- each review pass lists the relevant legacy baseline modules consulted
- intentional divergences are explicitly recorded rather than left implicit
- no finding depends on non-committed client identifiers or proprietary payloads
- any scope-expanding observation is redirected into PRD/backlog workflow instead of being treated as an immediate implementation task

## Expected Deliverable

The output of this provenance exercise should be a concise review artifact, not a new implementation backlog.

That deliverable should summarize:

- where the current implementation clearly inherits useful baseline patterns
- where the current implementation intentionally simplifies or departs from the baselines
- where evidence from local runs confirms the current behavior
- which items, if any, require PRD clarification before further work is considered

## Review Pass 1: Ingest

- `Review area`: ingest
- `Current anchor(s)`:
  - `src/elt_pipeline/ingest/connectors/rest.py`
  - `src/elt_pipeline/ingest/connectors/local_rest.py`
  - `src/elt_pipeline/ingest/connectors/sql.py`
  - `src/elt_pipeline/ingest/connectors/local_sql.py`
  - `src/elt_pipeline/ingest/connectors/object_storage.py`
  - `src/elt_pipeline/ingest/connectors/local_object_storage.py`
  - `src/elt_pipeline/ingest/connectors/kafka.py`
  - `src/elt_pipeline/ingest/connectors/local_kafka.py`
  - `src/elt_pipeline/ingest/storage.py`
  - `src/elt_pipeline/ingest/state.py`
  - `src/elt_pipeline/cli.py`
- `PRD anchor(s)`:
  - `docs/prd/01-prd-ingestion-raw-to-level1.md`
  - `docs/prd/00-prd-shared-observability-audit-and-error-handling.md`
  - `docs/prd/00-prd-platform-principles.md`
- `Legacy baseline(s)`:
  - `bi-bdp-elt/ingest-*`
  - `edp-elt-ingestion-main/edp-elt-ingest-*`
- `Evidence inspected`:
  - example configs under `examples/configs/`
  - connector and storage tests under `tests/test_rest_connectors.py`, `tests/test_sql_connectors.py`, `tests/test_object_storage_connectors.py`, `tests/test_kafka_connectors.py`, `tests/test_ingest_storage.py`, and `tests/test_cli.py`
  - one local object-storage ingest run captured under `tmp_provenance_ingest_review/`
  - resulting local artifacts inspected:
    - `tmp_provenance_ingest_review/level1/.../orders.json.json.manifest.json`
    - `tmp_provenance_ingest_review/state/environment=default/source=local_files/entity=orders.json`
- `Observed alignment`:
  - the runtime implements the four required first-class connector families: `rest`, `sql`, `object_storage`, and `kafka`
  - all four families follow a shared lifecycle shape: validate config, resolve prior checkpoint state, extract or consume, persist raw `level1` artifacts, and update checkpoints only after persistence
  - `LocalLevel1Writer` provides a standardized `level1` path contract using environment, source, entity, ingest date, optional window label, and `run_id`
  - `Level1ArtifactManifest` captures the minimum reproducibility fields expected by the ingestion PRD: `run_id`, source/entity, trigger, extraction mode, timestamps, checkpoint context, payload format, content hash, and artifact paths
  - `LocalCheckpointStore` preserves current state plus checkpoint history with `manifest_paths`, enabling replay and backfill seed resolution rather than opaque point-in-time state only
  - CLI backfill flow reuses saved checkpoint history to seed replay windows, which aligns with the PRD emphasis on replayability and controlled reprocessing
  - REST support includes config-driven request templating, auth strategies, retry policy, pagination, and envelope item extraction while preserving the original response artifact in `level1`
- `Intentional divergence`:
  - the current implementation is explicitly local-first and uses pragmatic local adapters (`sqlite`, local directories, local JSONL Kafka log) instead of reproducing cloud-runtime operational complexity from the baselines
  - config handling is materially simpler and more client-neutral than the legacy stacks; connector config is validated into typed models instead of distributing path and behavior inference across many modules
  - object storage is currently modeled as local-path pickup for v1 review evidence, which is a simplification of broader historical object-store patterns
- `Risk or ambiguity`:
  - `Flag for review`: the current `ingest run` path does not appear to emit the shared run artifact set under `runs/stage=ingest/...` even though the ingestion PRD and shared observability PRD call for authoritative audit, structured logs, structured errors, and lineage-compatible events for ingestion runs
  - `Flag for review`: envelope extraction and payload decoding are implemented inside the REST connector path, but there is not yet evidence of a connector-agnostic shared envelope capability reused across multiple ingestion families as described in the ingestion PRD
  - `Flag for review`: SQL watermark resolution currently supports checkpoint/static sources, but there is not yet evidence of the broader PRD aspiration to resolve delta state from prior `level1`/`level2`/`level3` platform-managed history sources
  - `Open scope note`: the current examples prove local object pickup, but they do not yet demonstrate the PRD's cross-account object-storage coverage target
- `Outcome`: needs follow-up

Review conclusion:

- Accept the current ingest implementation as strongly aligned on connector-family coverage, replayable raw persistence, checkpoint-after-durable-write semantics, and simpler client-neutral config handling.
- Keep the observability/artifact gap and the still-REST-localized envelope capability as explicit provenance findings until they are either implemented or intentionally narrowed in the PRD/review record.

## Review Pass 2: Normalize

- `Review area`: normalize
- `Current anchor(s)`:
  - `src/elt_pipeline/normalize/pipeline.py`
  - `src/elt_pipeline/normalize/runner.py`
  - `src/elt_pipeline/normalize/partitioning.py`
  - `src/elt_pipeline/normalize/level2_storage.py`
  - `src/elt_pipeline/normalize/storage.py`
  - `src/elt_pipeline/cli.py` (normalize dispatch + bypass)
  - `src/elt_pipeline/integrations/quality.py`
  - `src/elt_pipeline/shared/audit.py`, `src/elt_pipeline/shared/errors.py`, `src/elt_pipeline/shared/lineage.py`, `src/elt_pipeline/shared/logging.py`
- `PRD anchor(s)`:
  - `docs/prd/02-prd-level1-to-level2.md`
  - `docs/prd/00-prd-shared-observability-audit-and-error-handling.md`
  - `docs/prd/00-prd-architecture-levels-and-governance.md`
  - `docs/prd/07-prd-optional-platform-integrations.md`
- `Legacy baseline(s)`:
  - `bi-bdp-elt/transform-ingest`
  - `edp-elt-ingestion-main/edp-elt-transform-ingest`
- `Evidence inspected`:
  - example configs under `examples/configs/`, including `local_object_storage_orders.yaml` and `local_object_storage_orders_csv_bypass.yaml`
  - normalization tests under `tests/test_normalize_pipeline.py` and `tests/test_normalize_runner.py`
  - local SQL stage demo package under `examples/sql/local_demo/` (used as a downstream consumer of level2 materializations)
- `Observed alignment`:
  - the runtime makes the stage optional via config (`level2_mode`), and explicitly records bypass runs with audit + lineage + log artifacts (rather than silently skipping work)
  - the normalize stage emits structured run artifacts (audit record, log events, structured errors, lineage events) using the shared observability contract patterns
  - `NormalizationRunner` implements deterministic flattening/explosion into multiple tables with join key propagation and identifier sanitization
  - mapping catalogs are deterministic and stable: `mapping_version` is derived from a canonical structural payload hash, and the mapping catalog is stored under a `source/entity/mapping_version` location so reruns reuse the same artifact path when structure is unchanged
  - level2 outputs are materialized with a consistent path contract that includes environment/source/entity/mapping_version/partition/table/run_id, with per-table manifests capturing run and input-provenance context
  - the normalize stage supports a first-slice quality hook integration and records the evaluation outcome in run metadata
- `Intentional divergence`:
  - the first implementation materializes `level2` as local `jsonl` files (not parquet) as a pragmatic local-first contract that still preserves table/partition semantics and replayability
  - normalization is generic and payload-driven (JSON/CSV) rather than being driven by a rich, per-source mapping configuration layer (schema definitions, flatten rules, evolution policies)
  - type enforcement and schema evolution policies are not implemented in v1; normalization focuses on structural flattening and reproducible mapping catalogs
- `Risk or ambiguity`:
  - `Flag for review`: PRD 02 describes a source mapping configuration contract (schemas, flatten rules, evolution policy, write mode) and multiple format families (XML/Avro/etc.); current normalization supports JSON/CSV only and does not yet expose a mapping-config surface beyond `level2_mode` and partitioning strategy
  - `Flag for review`: PRD 02 emphasizes deterministic type casting with error capture; current normalize runner does not yet apply type casting rules or produce typed schemas as artifacts
- `Outcome`: needs follow-up

Review conclusion:

- Accept the current normalize stage as aligned on explicit optionality (bypass vs normalize), deterministic mapping catalogs, stable table naming with collision/length safeguards, and shared audit/log/error/lineage artifacts.
- Keep the missing mapping-config contract (schemas, evolution, and broader payload formats) as an explicit provenance finding until PRD scope is narrowed or the contract is implemented.

## Review Pass 3: SQL

- `Review area`: sql
- `Current anchor(s)`:
  - `src/elt_pipeline/sql/discovery.py`
  - `src/elt_pipeline/sql/models.py`
  - `src/elt_pipeline/sql/compiler.py`
  - `src/elt_pipeline/sql/graph.py`
  - `src/elt_pipeline/sql/executor.py`
  - `src/elt_pipeline/sql/runtime.py`
  - `src/elt_pipeline/cli.py` (sql dispatch)
  - `src/elt_pipeline/integrations/quality.py`
  - `src/elt_pipeline/shared/audit.py`, `src/elt_pipeline/shared/errors.py`, `src/elt_pipeline/shared/lineage.py`, `src/elt_pipeline/shared/logging.py`
- `PRD anchor(s)`:
  - `docs/prd/03-prd-sql-level2-to-level3-and-level3-to-level4.md`
  - `docs/prd/00-prd-shared-observability-audit-and-error-handling.md`
  - `docs/prd/00-prd-architecture-levels-and-governance.md`
  - `docs/prd/07-prd-optional-platform-integrations.md`
- `Legacy baseline(s)`:
  - `edp-elt-ingestion-main/edp-elt-transform-sql`
  - `bi-bdp-elt/transform-cfg`
  - `bi-bdp-elt/utility-athena`
  - `bi-bdp-elt/transform-check`
  - `bi-bdp-elt/transform-report`
- `Evidence inspected`:
  - example SQL package under `examples/sql/local_demo/` (model manifests + SQL text)
  - SQL tests under `tests/test_sql_models.py`
  - runtime smoke coverage under `tests/test_runtime.py` and `tests/test_examples.py`
- `Observed alignment`:
  - SQL models are treated as first-class artifacts via `manifest.yaml` + `model.sql`, with typed validation and directory-structure enforcement at discovery time
  - manifests encode the core PRD-required metadata: stage (`level3`/`level4`), domain, model name, target table, explicit load mode, dependencies, and model-level quality expectations
  - dependency ordering is deterministic via topological sorting and supports selective runs with optional inclusion of upstream dependencies
  - runtime parameterization is implemented as simple, deterministic token replacement with fail-fast validation for missing tokens, while recording resolved token values for auditability
  - load modes support the initial PRD slice (`full_refresh`, `append`, `partition_overwrite`) with explicit runtime validation when partition overwrite is selected
  - model-level validations (row count minimum, uniqueness, and not-null checks) are executed and persisted as part of the run record, with failures treated as blocking errors
  - SQL runs emit structured audit/log/error artifacts and lineage events, including per-model execution events for traceability
- `Intentional divergence`:
  - the first implementation is local-first and executes against `sqlite`, avoiding warehouse-specific coupling from the baselines
  - the execution surface is intentionally minimal (table materialization only; no merge/upsert semantics; no snapshot/SCD helpers)
  - tokenization intentionally avoids a macro engine (no Jinja/dbt-like templating), favoring a small validated placeholder system
- `Risk or ambiguity`:
  - `Flag for review`: PRD 03 calls out merge/upsert and broader materialization strategies; current runtime does not implement merge/upsert or snapshot/SCD patterns
  - `Flag for review`: legacy “data exists / ready” checks are not present as a first-class step; the current contract relies on model execution + validations rather than explicit preflight existence checks
- `Outcome`: needs follow-up

Review conclusion:

- Accept the SQL stage as aligned on model packaging, dependency ordering, deterministic tokenization, explicit load modes (within the v1 slice), blocking validations, and shared observability artifacts.
- Keep merge/upsert and broader materialization behavior as explicit gaps pending PRD narrowing or runtime expansion.

## Review Pass 4: Publish

- `Review area`: publish
- `Current anchor(s)`:
  - `src/elt_pipeline/publish/discovery.py`
  - `src/elt_pipeline/publish/models.py`
  - `src/elt_pipeline/publish/runtime.py`
  - `src/elt_pipeline/cli.py` (publish dispatch)
  - `src/elt_pipeline/shared/audit.py`, `src/elt_pipeline/shared/errors.py`, `src/elt_pipeline/shared/lineage.py`, `src/elt_pipeline/shared/logging.py`
- `PRD anchor(s)`:
  - `docs/prd/06-prd-level4-to-level5-publish-and-export.md`
  - `docs/prd/00-prd-shared-observability-audit-and-error-handling.md`
  - `docs/prd/00-prd-architecture-levels-and-governance.md`
- `Legacy baseline(s)`:
  - `bi-bdp-elt/consume-*`
- `Evidence inspected`:
  - example publish package under `examples/publish/local_demo/` (manifests and optional query.sql)
  - publish tests under `tests/test_publish_models.py` and `tests/test_publish_cli.py`
- `Observed alignment`:
  - publish definitions are treated as first-class artifacts (`manifest.yaml`, optional `query.sql`) with typed validation and directory-structure enforcement at discovery time
  - the v1 contract is enforced in the manifest: `source.stage` is constrained to `level4` and local filesystem delivery is the only supported target type
  - required client-neutral delivery metadata is present and validated (owning domain, owner team, consumer label, delivery purpose)
  - publish supports both direct dataset exports and query-driven selection, matching the approved v1 PRD decision
  - output formats include the required first-slice formats (`csv`, `jsonl`) plus `tsv`, and support optional archive packaging via `delivery.packaging.archive_format`
  - runs always write run-scoped artifacts and include a run-scoped manifest describing what was produced, with optional stable delivery paths depending on replacement mode
  - publish runs emit structured audit/log/error artifacts and lineage events mapping sqlite inputs to file outputs
- `Intentional divergence`:
  - the runtime includes additional convenience beyond the mandatory slice (TSV output, optional archive packaging) while keeping the default contract local-first and manifest-driven
  - replacement semantics are implemented as explicit modes, without introducing external transport or consumer acknowledgment workflows
- `Risk or ambiguity`:
  - `Flag for review`: PRD 06 reserves several follow-on capabilities (parquet extracts, canned reports, broader packaging semantics); the current implementation includes optional archive packaging, so ensure the feature stays optional and does not expand the required contract without PRD update
  - `Open scope note`: only local filesystem delivery is supported, as required by v1; external delivery adapters remain future work
- `Outcome`: accepted

Review conclusion:

- Accept the publish stage as aligned to PRD 06 v1 on local-only delivery, run-scoped manifests, required metadata validation, and direct vs query selection modes.
- Treat optional archive packaging as acceptable "ahead of need" implementation only if it remains non-mandatory and PRD 06 reserved-scope guidance is preserved.

## Cross-Stage Provenance Summary

- The current implementation clearly inherits useful baseline patterns around stage-oriented runtime separation, manifest-driven contracts, deterministic artifact paths, replayability, and evidence capture.
- The strongest areas of alignment are:
  - ingest connector-family coverage with replayable raw persistence and checkpoint-after-durable-write behavior
  - normalize determinism around mapping catalogs, table naming, and explicit bypass handling
  - SQL packaging, dependency ordering, validated parameterization, and blocking validations
  - publish manifest discovery, local-first delivery, and run-scoped output evidence
- The strongest intentional divergences are:
  - local-first execution surfaces instead of reproducing cloud-runtime coupling from the baselines
  - materially simpler, typed, and client-neutral config handling
  - explicit avoidance of legacy config/path derivation sprawl
  - smaller v1 execution surfaces that preserve the approved contract without inheriting historical feature breadth

## Evidence-Based Conclusion

- Local examples, targeted tests, and existing run artifacts are sufficient to show that the current runtime is not a thin copy of either legacy baseline; it selectively preserves the durable patterns that still match the PRDs.
- The current implementation should be treated as a modernization of the legacy approaches rather than a direct recreation:
  - simpler configuration contracts
  - clearer run evidence
  - local-first optional integrations
  - reduced operational complexity
- No review pass required client-specific identifiers or proprietary payloads to explain the current behavior.

## Follow-Up Candidates

The provenance review identifies a small set of items that should remain explicit for PRD clarification or later backlog triage rather than being silently absorbed into scope:

1. Ingest observability parity:
   - confirm whether ingestion must emit the same authoritative `runs/stage=ingest/...` artifact set already present for normalize, SQL, and publish
2. Shared envelope capability:
   - decide whether envelope extraction should become a cross-connector ingestion contract or remain a REST-specific implementation detail
3. Broader delta-state resolution:
   - confirm whether SQL/ingest delta handling must resolve from platform-managed level history beyond checkpoint/static sources
4. Normalize mapping contract:
   - decide whether PRD 02 should be narrowed to the implemented JSON/CSV structural normalization slice or expanded into typed schema/mapping policy implementation
5. SQL materialization breadth:
   - decide whether merge/upsert, snapshot, and SCD behavior are required for the approved SQL stage scope

## Final Review Outcome

- `Ingest`: needs follow-up
- `Normalize`: needs follow-up
- `SQL`: needs follow-up
- `Publish`: accepted

Overall disposition:

- Accept the repository as broadly aligned with the intended baseline patterns and current PRDs for the approved v1 implementation.
- Keep the remaining gaps framed as provenance review findings, not automatic defects, unless and until the PRDs explicitly require the broader behavior.
- Redirect any scope-expanding discoveries into PRD or backlog workflow rather than reopening implementation by implication from the archived baselines.

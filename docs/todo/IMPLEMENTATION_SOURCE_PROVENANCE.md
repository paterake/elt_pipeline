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

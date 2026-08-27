### PR description

**What does this PR change?** One-paragraph summary.

**If capability PR (new green CMM row):** What GAP-ID + RFC issue is it closing? e.g., "Closes GAP-4; closes #123". Paste the 6-part Design Decision Log (D-1…D-6) inline below OR link to the archive entry in `docs/todo/archive/WORK_ITEMS_CLOSED.md`.

**If bug fix:** Paste the bug-report issue link. Paste the regression test name — confirm it fails WITHOUT this patch: e.g., `test_query_ref_missing_model_column_raises_contextual` in tests/test_semantic_metrics.py.

**If docs/scaffold only (no code):** Confirm no API/behavior changes, no tests needed, ruff 0 errors.

---

## Checklist

All items `[x]` required for merge. Maintainers: if an item is N/A, check it AND write a one-line note next to it explaining why.

### Environment & gates

- [ ] **Ruff 0 errors**
  ```
  uv run ruff check src/ tests/ examples/
  # Expected: All checks passed!
  ```
- [ ] **Non-Spark test subset green** (for non-Spark PRs — confirm all `test_*.py` files that DON'T import Spark pass):
  ```
  uv run pytest tests/test_cli.py tests/test_semantic_metrics.py tests/test_path_utils.py -v
  # paste 2-line exit summary here: e.g., 21 passed in 0.57s
  ```
- [ ] **Official gate green locally** (for ANY PR that adds or modifies Spark/Iceberg/Trino/normalization/ingest code). Include EXIT=$? line + tail:
  ```
  bash scripts/run_tests.sh 2>&1 | tail -15 ; echo "EXIT=$?"
  # paste output here. Minimum expected: non-Spark passed ≥607, 0 failed, 28 skipped unchanged
  ```

### Tests

- [ ] **1 regression test per bug** — link the test name + confirm failure without patch.
- [ ] **≥1 test per behavior change** (new CLI flag, new Enum arm, new YAML key, new error message). If this is a capability PR: **≥+16 individual new tests, categorized in PR body** (not 1 parametrized loop asserting 16 things — 16 test functions).
- [ ] **Zero previously-passing tests now fail.** If any do, explain in the notes section below why this is acceptable.

### Code layout (pCO)

- [ ] New source code follows **thin-facade pCO pattern** ([CONTRIBUTING.md §3](CONTRIBUTING.md#3-adding-code--the-pco-pattern-do-this-not-a-gold-file)):
  - Package root `__init__.py` = alphabetical imports from underscore submodules + public `__all__` list. Zero implementation in `__init__.py`.
  - Implementation files underscore-prefixed: `_models.py` / `_compiler.py` / `_runtime.py` / `_discovery.py`.
  - **One concern per file.** No files combining "models + compile + runtime" into a 1,000-line gold file.
- [ ] **Breaking changes are labeled.** If any public `__all__` export, CLI flag, Protocol signature, or Enum value changes:
  - PR title prefixed with `[BREAKING]`.
  - Deprecation plan in PR body (shim + `DeprecationWarning` ≥2 minor releases before removal, OR written justification why this is impossible).

### Docs

- [ ] **README / examples / operator docs** updated for any new user-facing CLI/YAML surface.
- [ ] **CAPABILITY_MATURITY_MATRIX.md** green-row flipped (if capability PR) + **INDUSTRY_GAP_ANALYSIS.md §5** GAP-N status ⏳/🔴 → ✅ IMPLEMENTED with Implementation subsection (template copy-paste from GAP-4).
- [ ] **BACKLOG.md §Resume pointer** updated + **Still Todo** section updated with the corresponding delta. Capability PRs: close the GAP-N block and add a Closed Work Items row. Bug/docs-only PRs: BACKLOG unchanged. See [BACKLOG.md](../docs/todo/BACKLOG.md).
- [ ] **WORK_ITEMS_CLOSED.md archival narrative** (capability PRs only). Paste the 12-section closure narrative here OR commit to `docs/todo/archive/WORK_ITEMS_CLOSED.md` in this PR.

### Safety

- [ ] **No secrets, no PII, no internal hostnames** in any new or modified file. Diff review covers:
  - `sk-*`, `ghp_*`, `xoxb-*`, non-example `AKIA` keys (example `AKIAIOSFODNN7EXAMPLE` is fine).
  - `.pem`, `.env`, `.pypirc`, `id_rsa*` paths (these are all gitignored; double-check example configs).
  - RFC-1918 corporate addresses (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16). `127.0.0.1` / `localhost` / `.local` placeholders are fine.
  - Personal emails (use author-attribution emails in pyproject.toml/LICENSE/README only; no personal addresses in examples).

### Notes

<!-- Any context you want maintainers to see during review. -->

**Diff size summary:** `+X -Y lines across N files` — paste output of `git diff --stat HEAD...HEAD~1`.

**Files changed category (tick the dominant one):**
- [ ] Capability PR (new green row, ≥+16 tests)
- [ ] Bug fix (1 regression test, core behavior correction)
- [ ] Internal refactor (byte-identical API, no user changes)
- [ ] Docs / examples / tutorials only
- [ ] Publishing hygiene / repo scaffolding (CONTRIBUTING.md, templates, CI config — no code)

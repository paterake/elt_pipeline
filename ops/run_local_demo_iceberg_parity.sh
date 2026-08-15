#!/usr/bin/env bash
#
# Gate I5 end-to-end parity proof: materialize the local_demo SQL project
# once with plain-parquet staging-swap (legacy default) and once with Iceberg
# table-format (--iceberg-enabled) then compare row-counts + md5 checksums
# per model.
#
# All output (Spark warehouse, runtime output, artifacts, parity reports)
# lives UNDER the shared repo-run directory:
#   $ELT_PIPELINE_REPO_RUN_DIR/results/elt_pipeline/
# falling back to the canonical path:
#   $HOME/Documents/__data/repo_run/results/elt_pipeline/
#
# This script MUST be run from an OUTSIDE-SANDBOX shell, with Java on PATH
# and a populated venv at the repo root.
#
# Usage (from repo root):
#   ./ops/run_local_demo_iceberg_parity.sh [parquet|iceberg|compare|all]
#
# Optional env:
#   ELT_PIPELINE_REPO_RUN_DIR
#   ELT_PIPELINE_ICEBERG_CATALOG_TYPE   (hadoop or jdbc)
#   ELT_PIPELINE_ICEBERG_CATALOG_URI    (when jdbc)
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
VENV_PY="${VENV_PY:-${REPO_ROOT}/.venv/bin/python}"

if [[ -n "${ELT_PIPELINE_REPO_RUN_DIR:-}" ]]; then
  ELT_RUN_PARENT="${ELT_PIPELINE_REPO_RUN_DIR%/}/results/elt_pipeline"
else
  ELT_RUN_PARENT="$HOME/Documents/__data/repo_run/results/elt_pipeline"
fi

PACKAGE_PATH="${REPO_ROOT}/examples/sql/local_demo"
PARITY_DIR="${ELT_RUN_PARENT}/.artifacts/parity"
PARQUET_ROOT="${ELT_RUN_PARENT}/parity_parquet_runtime"
PARQUET_WH="${ELT_RUN_PARENT}/parity_parquet_warehouse"
ICEBERG_ROOT="${ELT_RUN_PARENT}/parity_iceberg_runtime"
ICEBERG_WH="${ELT_RUN_PARENT}/parity_iceberg_warehouse"
ICEBERG_WAREHOUSE_DIR="${ICEBERG_WH}/iceberg"
PARQUET_REPORT_JSON="${PARITY_DIR}/local_demo_parquet_report.json"
ICEBERG_REPORT_JSON="${PARITY_DIR}/local_demo_iceberg_report.json"
COMPARE_JSON="${PARITY_DIR}/local_demo_compare_report.json"

ELT_CLI=("${VENV_PY}" -m elt_pipeline.cli)

mkdir -p \
  "${PARITY_DIR}" \
  "${PARQUET_ROOT}" \
  "${PARQUET_WH}" \
  "${ICEBERG_ROOT}" \
  "${ICEBERG_WH}"

log() { printf '[gate_i5_parity] %s\n' "$*"; }

require_clean_repo_for_demo() {
  if [[ ! -f "${PACKAGE_PATH}/level3/sales/base_orders/model.sql" ]]; then
    echo "ERROR: expected demo package at ${PACKAGE_PATH}" >&2
    exit 20
  fi
  if ! "${VENV_PY}" -c 'import pyspark, elt_pipeline' >/dev/null 2>&1; then
    echo "ERROR: venv needs pyspark + project installed. See LOCAL_DEVELOPMENT_AND_RELEASE.md." >&2
    exit 21
  fi
}

run_parquet() {
  log "Legacy parquet run → root=${PARQUET_ROOT} wh=${PARQUET_WH}"
  ELT_PIPELINE_ICEBERG_ENABLED=false \
  "${ELT_CLI[@]}" sql run \
    --package-path "${PACKAGE_PATH}" \
    --environment parity_parquet \
    --include-deps \
    --root-path "${PARQUET_ROOT}" \
    --warehouse-root "${PARQUET_WH}" \
    --job-name "parity-parquet-run" \
    2>&1 | tee "${PARITY_DIR}/parquet_run.log" | tail -50
  log "Writing parquet parity report -> ${PARQUET_REPORT_JSON}"
  "${VENV_PY}" - <<PY
from pathlib import Path
import sys
sys.path.insert(0, "${REPO_ROOT}/src")
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql.discovery import discover_sql_models, topologically_sort_sql_models
from elt_pipeline.sql.parity_check import measure_model_parity, write_parity_report
spark = build_spark_session(app_name="eltp_parity_parquet")
try:
    discovered = discover_sql_models("${PACKAGE_PATH}")
    ordered = topologically_sort_sql_models(discovered)
    from elt_pipeline.sql.compiler import compile_sql_model
    from elt_pipeline.shared.token_context import build_token_context
    compiled = []
    for m in ordered:
        compiled.append(compile_sql_model(
            m,
            token_context=build_token_context(
                environment="parity_parquet",
                run_id="parity-parquet-report",
                stage=m.manifest.stage.value,
                domain=m.manifest.domain,
                model_name=m.manifest.name,
                target_table_name=m.manifest.target.table_name,
            ),
        ))
    report = measure_model_parity(
        spark=spark,
        models=compiled,
        warehouse_root="${PARQUET_WH}",
    )
    write_parity_report("${PARQUET_REPORT_JSON}", report)
    for entry in report:
        print(f"{entry.model_id}: rows={entry.row_count} md5={entry.md5_of_sorted_row_hashes}")
finally:
    spark.stop()
PY
}

run_iceberg() {
  local -a extra_iceberg=()
  extra_iceberg+=(--iceberg-enabled)
  if [[ -n "${ELT_PIPELINE_ICEBERG_CATALOG_TYPE:-}" ]]; then
    extra_iceberg+=(--iceberg-catalog-type "${ELT_PIPELINE_ICEBERG_CATALOG_TYPE}")
  fi
  if [[ -n "${ELT_PIPELINE_ICEBERG_CATALOG_URI:-}" ]]; then
    extra_iceberg+=(--iceberg-catalog-uri "${ELT_PIPELINE_ICEBERG_CATALOG_URI}")
  fi
  log "Iceberg run → root=${ICEBERG_ROOT} wh=${ICEBERG_WH} iceberg_warehouse=${ICEBERG_WAREHOUSE_DIR}"
  "${ELT_CLI[@]}" sql run \
    --package-path "${PACKAGE_PATH}" \
    --environment parity_iceberg \
    --include-deps \
    --root-path "${ICEBERG_ROOT}" \
    --warehouse-root "${ICEBERG_WH}" \
    --iceberg-warehouse-dir "${ICEBERG_WAREHOUSE_DIR}" \
    --job-name "parity-iceberg-run" \
    "${extra_iceberg[@]}" \
    2>&1 | tee "${PARITY_DIR}/iceberg_run.log" | tail -50
  log "Writing iceberg parity report -> ${ICEBERG_REPORT_JSON}"
  "${VENV_PY}" - <<PY
from pathlib import Path
import sys, os
sys.path.insert(0, "${REPO_ROOT}/src")
os.environ["ELT_PIPELINE_ICEBERG_ENABLED"] = "true"
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql.discovery import discover_sql_models, topologically_sort_sql_models
from elt_pipeline.sql.parity_check import measure_model_parity, write_parity_report
spark_kwargs = dict(app_name="eltp_parity_iceberg", iceberg_enabled=True, iceberg_warehouse_dir="${ICEBERG_WAREHOUSE_DIR}")
ct = os.environ.get("ELT_PIPELINE_ICEBERG_CATALOG_TYPE", "")
cu = os.environ.get("ELT_PIPELINE_ICEBERG_CATALOG_URI", "")
if ct: spark_kwargs["iceberg_catalog_type"] = ct
if cu: spark_kwargs["iceberg_catalog_uri"] = cu
spark = build_spark_session(**spark_kwargs)
try:
    discovered = discover_sql_models("${PACKAGE_PATH}")
    ordered = topologically_sort_sql_models(discovered)
    from elt_pipeline.sql.compiler import compile_sql_model
    from elt_pipeline.shared.token_context import build_token_context
    compiled = []
    for m in ordered:
        compiled.append(compile_sql_model(
            m,
            token_context=build_token_context(
                environment="parity_iceberg",
                run_id="parity-iceberg-report",
                stage=m.manifest.stage.value,
                domain=m.manifest.domain,
                model_name=m.manifest.name,
                target_table_name=m.manifest.target.table_name,
            ),
        ))
    report = measure_model_parity(
        spark=spark,
        models=compiled,
        warehouse_root="${ICEBERG_WH}",
    )
    write_parity_report("${ICEBERG_REPORT_JSON}", report)
    for entry in report:
        print(f"{entry.model_id}: rows={entry.row_count} md5={entry.md5_of_sorted_row_hashes}")
finally:
    spark.stop()
PY
}

run_compare() {
  log "Comparing reports: parquet=${PARQUET_REPORT_JSON} vs iceberg=${ICEBERG_REPORT_JSON}"
  "${VENV_PY}" - <<PY
import json, sys
sys.path.insert(0, "${REPO_ROOT}/src")
from elt_pipeline.sql.parity_check import (
    load_parity_report,
    compare_parity_reports,
)
left = load_parity_report("${PARQUET_REPORT_JSON}")
right = load_parity_report("${ICEBERG_REPORT_JSON}")
out = compare_parity_reports(left, right)
with open("${COMPARE_JSON}", "w") as f:
    json.dump(out, f, indent=2, sort_keys=True)
if out["parity"]:
    print(f"PARITY OK: matched {out['match_count']}/{out['total_models']} models")
else:
    print(f"PARITY FAIL: mismatches={out['mismatch_count']} missing_l={out['missing_left']} missing_r={out['missing_right']}")
    sys.exit(33)
PY
}

require_clean_repo_for_demo
action="${1:-all}"
case "${action}" in
  parquet) run_parquet ;;
  iceberg) run_iceberg ;;
  compare) run_compare ;;
  all)
    run_parquet
    run_iceberg
    run_compare
    ;;
  *)
    echo "Usage: $0 (parquet|iceberg|compare|all)" >&2
    exit 10
    ;;
esac

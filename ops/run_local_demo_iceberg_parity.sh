#!/usr/bin/env bash
#
# Iceberg end-to-end parity: run shared ingest+normalize to seed L2,
# then materialize the local_demo SQL project once with plain-parquet
# staging-swap (legacy default) and once with Iceberg table-format
# (--iceberg-enabled) then compare row-counts + md5 checksums per model.
#
# Only the sales-domain is run (4 models: base_orders, canonical_orders,
# orders_ingest_snapshot, order_summary). inventory.canonical_shipments is
# excluded because the orders-only object_storage config has no shipments
# entity, so that model would fail level2 resolution.
#
# All output (Spark warehouse, runtime output, artifacts, parity reports)
# lives UNDER the shared repo-run directory:
#   $ELT_PIPELINE_REPO_RUN_DIR/results/elt_pipeline/
# falling back to the canonical path:
#   $HOME/Documents/__data/repo_run/results/elt_pipeline/
#
# Usage (from repo root, with Java on PATH and venv populated):
#   ./ops/run_local_demo_iceberg_parity.sh [setup|parquet|iceberg|compare|all]
#
# Optional env:
#   ELT_PIPELINE_REPO_RUN_DIR
#   ELT_PIPELINE_ICEBERG_CATALOG_TYPE   (hadoop or jdbc)
#   ELT_PIPELINE_ICEBERG_CATALOG_URI    (when jdbc)
#   ELT_PIPELINE_PARITY_START_DATE      default 2026-01-01
#   ELT_PIPELINE_PARITY_END_DATE        default 2026-01-31
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

CONFIG_PATH="${REPO_ROOT}/examples/configs/local_object_storage_orders.yaml"
PACKAGE_PATH="${REPO_ROOT}/examples/sql/local_demo"
SHARED_ROOT="${ELT_RUN_PARENT}/parity_shared_runtime"
PARITY_DIR="${ELT_RUN_PARENT}/.artifacts/parity"
PARQUET_WH="${ELT_RUN_PARENT}/parity_parquet_warehouse"
ICEBERG_WH="${ELT_RUN_PARENT}/parity_iceberg_warehouse"
ICEBERG_WAREHOUSE_DIR="${ICEBERG_WH}/iceberg"
PARQUET_REPORT_JSON="${PARITY_DIR}/local_demo_parquet_report.json"
ICEBERG_REPORT_JSON="${PARITY_DIR}/local_demo_iceberg_report.json"
COMPARE_JSON="${PARITY_DIR}/local_demo_compare_report.json"

START_DATE="${ELT_PIPELINE_PARITY_START_DATE:-2026-01-01}"
END_DATE="${ELT_PIPELINE_PARITY_END_DATE:-2026-01-31}"
SELECTION_DOMAIN="sales"

ELT_CLI=("${VENV_PY}" -m elt_pipeline)

mkdir -p \
  "${PARITY_DIR}" \
  "${SHARED_ROOT}" \
  "${PARQUET_WH}" \
  "${ICEBERG_WH}"

log() { printf '[gate_i5_parity] %s\n' "$*"; }

require_clean_repo_for_demo() {
  if [[ ! -f "${PACKAGE_PATH}/level3/sales/base_orders/model.sql" ]]; then
    echo "ERROR: expected demo package at ${PACKAGE_PATH}" >&2
    exit 20
  fi
  if [[ ! -f "${CONFIG_PATH}" ]]; then
    echo "ERROR: expected local orders config at ${CONFIG_PATH}" >&2
    exit 22
  fi
  if ! "${VENV_PY}" -c 'import pyspark, elt_pipeline' >/dev/null 2>&1; then
    echo "ERROR: venv needs pyspark + project installed. See LOCAL_DEVELOPMENT_AND_RELEASE.md." >&2
    exit 21
  fi
}

run_setup() {
  log "Shared setup → clearing SHARED_ROOT then ingest+normalize → ${SHARED_ROOT}"
  rm -rf "${SHARED_ROOT:?}"
  mkdir -p "${SHARED_ROOT}"

  log "validate-config: ${CONFIG_PATH}"
  "${ELT_CLI[@]}" validate-config "${CONFIG_PATH}" >/dev/null

  log "ingest run (object_storage local_files:orders)"
  "${ELT_CLI[@]}" ingest run "${CONFIG_PATH}" \
    --root-path "${SHARED_ROOT}" \
    --job-name "parity-setup-ingest" \
    2>&1 | tee "${PARITY_DIR}/ingest.log" | tail -10

  log "normalize run (L1 raw files → L2 relational parquet + MappingCatalog)"
  "${ELT_CLI[@]}" normalize run "${CONFIG_PATH}" \
    --root-path "${SHARED_ROOT}" \
    --job-name "parity-setup-normalize" \
    2>&1 | tee "${PARITY_DIR}/normalize.log" | tail -20
  log "Shared setup complete."
}

run_parquet() {
  log "Legacy parquet run → shared_root=${SHARED_ROOT} wh=${PARQUET_WH} window=${START_DATE}..${END_DATE} domain=${SELECTION_DOMAIN}"
  rm -rf "${PARQUET_WH:?}"
  mkdir -p "${PARQUET_WH}"
  ELT_PIPELINE_ICEBERG_ENABLED=false \
  "${ELT_CLI[@]}" sql run \
    --environment parity_parquet \
    --include-deps \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --domain "${SELECTION_DOMAIN}" \
    --root-path "${SHARED_ROOT}" \
    --warehouse-root "${PARQUET_WH}" \
    --job-name "parity-parquet-run" \
    "${PACKAGE_PATH}" \
    2>&1 | tee "${PARITY_DIR}/parquet_run.log" | tail -20
  log "Writing parquet parity report -> ${PARQUET_REPORT_JSON}"
  "${VENV_PY}" - <<PY
from pathlib import Path
import sys
sys.path.insert(0, "${REPO_ROOT}/src")
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql.discovery import discover_sql_models
from elt_pipeline.sql import topologically_sort_sql_models, filter_sql_models, resolve_selected_model_ids
from elt_pipeline.sql.parity_check import measure_model_parity, write_parity_report
from elt_pipeline.sql.models import CompiledSqlModel
spark = build_spark_session(app_name="eltp_parity_parquet", iceberg_enabled=False)
try:
    discovered = discover_sql_models("${PACKAGE_PATH}")
    ordered = topologically_sort_sql_models(discovered)
    selected = filter_sql_models(discovered, stage=None, domain="${SELECTION_DOMAIN}", model_name=None)
    selected_ids = resolve_selected_model_ids(all_models=discovered, selected_models=selected, include_dependencies=True)
    models_in_scope = [m for m in ordered if m.model_id in selected_ids]
    compiled = []
    for m in models_in_scope:
        compiled.append(CompiledSqlModel(
            model_id=m.model_id,
            stage=m.manifest.stage,
            domain=m.manifest.domain,
            name=m.manifest.name,
            target_table_name=m.manifest.target.table_name,
            partition_columns=list(m.manifest.target.partition_columns),
            load_mode=m.manifest.load_mode,
            materialization=m.manifest.materialization,
            manifest_path=Path(m.manifest_path),
            sql_path=Path(m.sql_path),
            compiled_sql=m.sql_text,
            depends_on=list(m.manifest.depends_on),
            sources=list(m.manifest.sources),
            quality=m.manifest.quality,
            staging_root=m.manifest.staging_root,
        ))
    report = measure_model_parity(
        spark=spark,
        models=compiled,
        warehouse_root="${PARQUET_WH}",
    )
    write_parity_report("${PARQUET_REPORT_JSON}", report)
    for entry in report:
        print(f"  {entry.model_id}: rows={entry.row_count} md5={entry.md5_of_sorted_row_hashes}")
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
  log "Iceberg run → shared_root=${SHARED_ROOT} wh=${ICEBERG_WH} iceberg_warehouse=${ICEBERG_WAREHOUSE_DIR} window=${START_DATE}..${END_DATE} domain=${SELECTION_DOMAIN}"
  rm -rf "${ICEBERG_WH:?}"
  mkdir -p "${ICEBERG_WH}"
  "${ELT_CLI[@]}" sql run \
    --environment parity_iceberg \
    --include-deps \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --domain "${SELECTION_DOMAIN}" \
    --root-path "${SHARED_ROOT}" \
    --warehouse-root "${ICEBERG_WH}" \
    --iceberg-warehouse-dir "${ICEBERG_WAREHOUSE_DIR}" \
    --job-name "parity-iceberg-run" \
    "${extra_iceberg[@]}" \
    "${PACKAGE_PATH}" \
    2>&1 | tee "${PARITY_DIR}/iceberg_run.log" | tail -20
  log "Writing iceberg parity report -> ${ICEBERG_REPORT_JSON}"
  "${VENV_PY}" - <<PY
from pathlib import Path
import sys, os
sys.path.insert(0, "${REPO_ROOT}/src")
os.environ["ELT_PIPELINE_ICEBERG_ENABLED"] = "true"
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql.discovery import discover_sql_models
from elt_pipeline.sql import topologically_sort_sql_models, filter_sql_models, resolve_selected_model_ids
from elt_pipeline.sql.parity_check import measure_model_parity, write_parity_report
from elt_pipeline.sql.models import CompiledSqlModel
spark_kwargs = dict(app_name="eltp_parity_iceberg", iceberg_enabled=True, iceberg_warehouse_dir="${ICEBERG_WAREHOUSE_DIR}")
ct = os.environ.get("ELT_PIPELINE_ICEBERG_CATALOG_TYPE", "")
cu = os.environ.get("ELT_PIPELINE_ICEBERG_CATALOG_URI", "")
if ct: spark_kwargs["iceberg_catalog_type"] = ct
if cu: spark_kwargs["iceberg_catalog_uri"] = cu
spark = build_spark_session(**spark_kwargs)
try:
    discovered = discover_sql_models("${PACKAGE_PATH}")
    ordered = topologically_sort_sql_models(discovered)
    selected = filter_sql_models(discovered, stage=None, domain="${SELECTION_DOMAIN}", model_name=None)
    selected_ids = resolve_selected_model_ids(all_models=discovered, selected_models=selected, include_dependencies=True)
    models_in_scope = [m for m in ordered if m.model_id in selected_ids]
    compiled = []
    for m in models_in_scope:
        compiled.append(CompiledSqlModel(
            model_id=m.model_id,
            stage=m.manifest.stage,
            domain=m.manifest.domain,
            name=m.manifest.name,
            target_table_name=m.manifest.target.table_name,
            partition_columns=list(m.manifest.target.partition_columns),
            load_mode=m.manifest.load_mode,
            materialization=m.manifest.materialization,
            manifest_path=Path(m.manifest_path),
            sql_path=Path(m.sql_path),
            compiled_sql=m.sql_text,
            depends_on=list(m.manifest.depends_on),
            sources=list(m.manifest.sources),
            quality=m.manifest.quality,
            staging_root=m.manifest.staging_root,
        ))
    report = measure_model_parity(
        spark=spark,
        models=compiled,
        warehouse_root="${ICEBERG_WH}",
    )
    write_parity_report("${ICEBERG_REPORT_JSON}", report)
    for entry in report:
        print(f"  {entry.model_id}: rows={entry.row_count} md5={entry.md5_of_sorted_row_hashes}")
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
    for m in out.get("matches", []):
        print(f"  MATCH {m}")
else:
    print(f"PARITY FAIL: mismatches={out['mismatch_count']} missing_l={out['missing_left']} missing_r={out['missing_right']}")
    for m in out.get("mismatches", []):
        print(f"  MISMATCH {m}")
    for m in out.get("missing_left", []):
        print(f"  MISSING(parquet) {m}")
    for m in out.get("missing_right", []):
        print(f"  MISSING(iceberg) {m}")
    sys.exit(33)
PY
}

require_clean_repo_for_demo
action="${1:-all}"
case "${action}" in
  setup) run_setup ;;
  parquet) run_parquet ;;
  iceberg) run_iceberg ;;
  compare) run_compare ;;
  all)
    run_setup
    run_parquet
    run_iceberg
    run_compare
    ;;
  *)
    echo "Usage: $0 (setup|parquet|iceberg|compare|all)" >&2
    exit 10
    ;;
esac

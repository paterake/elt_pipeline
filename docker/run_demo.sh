#!/usr/bin/env bash
#
# End-to-end demo runner inside the container.
#
# Runs the 4-phase pipeline for the local_demo example against the shared
# /var/lib/elt_pipeline volume (so Trino can serve the resulting Iceberg
# tables immediately after).
#
# Phases:
#   1. validate-config
#   2. ingest run   (L1 raw → object_storage local_files: orders)
#   3. normalize run  (L1 → L2 relational parquet + MappingCatalog)
#   4. sql run  (L2 → L3 canonical Iceberg + L4 marts, --iceberg-enabled)
#   5. (optional) publish run  (L4 → L5 static exports)
#
set -euo pipefail

REPO_ROOT="/usr/share/elt_pipeline"
ELT_HOME="/var/lib/elt_pipeline"
ELT_CLI=(elt-pipeline)

CONFIG="${ELT_PIPELINE_CONFIG_PATH:-${REPO_ROOT}/../pipeline.yaml}"
EXAMPLE_CONFIG="/examples/configs/local_object_storage_orders.yaml"
PACKAGE_PATH="/examples/sql/local_demo"
PUBLISH_EXAMPLES="/examples/publish/local_demo"

START_DATE="${ELT_PIPELINE_DEMO_START_DATE:-2026-01-01}"
END_DATE="${ELT_PIPELINE_DEMO_END_DATE:-2026-01-31}"
DOMAIN="${ELT_PIPELINE_DEMO_DOMAIN:-sales}"

# Use the example config when it's mounted at /examples (docker-compose default)
# otherwise fall back to the shipped copy.
if [[ -f "${EXAMPLE_CONFIG}" ]]; then
  RUN_CONFIG="${EXAMPLE_CONFIG}"
else
  RUN_CONFIG="/usr/share/elt_pipeline/examples/configs/local_object_storage_orders.yaml"
fi
if [[ -d "/usr/share/elt_pipeline/examples/sql/local_demo" && ! -d "${PACKAGE_PATH}" ]]; then
  PACKAGE_PATH="/usr/share/elt_pipeline/examples/sql/local_demo"
fi

log() { printf '[elt_demo] %s\n' "$*"; }

log "Configuration"
log "  config        = ${RUN_CONFIG}"
log "  package_path  = ${PACKAGE_PATH}"
log "  root_path     = ${ELT_HOME}"
log "  window        = ${START_DATE}..${END_DATE}"
log "  domain        = ${DOMAIN}"

log "[1/5] validate-config"
"${ELT_CLI[@]}" validate-config "${RUN_CONFIG}"

log "[2/5] ingest run → L1 raw landing"
"${ELT_CLI[@]}" ingest run "${RUN_CONFIG}" \
  --root-path "${ELT_HOME}" \
  --job-name "demo-ingest"

log "[3/5] normalize run → L2 parquet + MappingCatalog"
"${ELT_CLI[@]}" normalize run "${RUN_CONFIG}" \
  --root-path "${ELT_HOME}" \
  --job-name "demo-normalize"

log "[4/5] sql run → L3 Iceberg canonical + L4 marts (${DOMAIN} domain)"
"${ELT_CLI[@]}" sql run \
  --environment docker_demo \
  --include-deps \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --domain "${DOMAIN}" \
  --root-path "${ELT_HOME}" \
  --iceberg-enabled \
  --job-name "demo-sql" \
  "${PACKAGE_PATH}"

log "[5/5] maintain run → compact + expire snapshots (Iceberg L3/L4 hygiene)"
"${ELT_CLI[@]}" maintain run \
  --root-path "${ELT_HOME}" \
  --warehouse-root "${ELT_HOME}/results/elt_pipeline/warehouse/iceberg" \
  --compact \
  --expire-snapshots \
  --job-name "demo-maintain"

log ""
log "Demo complete."
log "  Iceberg warehouse : ${ELT_HOME}/results/elt_pipeline/warehouse/iceberg"
log "  Audit artifacts   : ${ELT_HOME}/results/elt_pipeline/.artifacts"
log ""
log "Next steps (with docker-compose trino serving):"
log "  docker compose up -d trino"
log "  sleep 30"
log "  docker compose exec trino trino --catalog iceberg --execute 'SHOW SCHEMAS'"
log "  docker compose exec trino trino --catalog iceberg --execute 'SELECT COUNT(*) FROM sales.order_summary'"

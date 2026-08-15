#!/usr/bin/env bash
#
# Trino serving-engine cookbook for the elt_pipeline Iceberg warehouse.
#
# Runtime data (catalog configs, logs, PID) lives *outside* the repo under
#   $ELT_PIPELINE_REPO_RUN_DIR/results/elt_pipeline/trino/
# falling back to the canonical repo_run path:
#   $HOME/Documents/__data/repo_run/results/elt_pipeline/trino/
#
# The Trino binary tarball is extracted one time into the shared
#   $ELT_PIPELINE_REPO_RUN_DIR/results/elt_pipeline/.cache/trino/
# directory (the Gate-0 spike cache is reused by default; pass TRINO_SERVER_TGZ
# to re-bootstrap).
#
# Usage:
#   ops/trino_serving/run_trino.sh (start|stop|status|restart|cli|write-configs|env)
#
# Optional env (honor in this order):
#   ELT_PIPELINE_REPO_RUN_DIR          Override shared output root.
#   ELT_PIPELINE_TRINO_PORT            HTTP port, default 8080.
#   ELT_PIPELINE_TRINO_HOST            Bind host, default 127.0.0.1.
#   ELT_PIPELINE_TRINO_VERSION         Default 468 (matches Gate-0 spike).
#   ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR Iceberg warehouse dir (default:
#                                      $repo_run_parent/warehouse/iceberg).
#   ELT_PIPELINE_ICEBERG_CATALOG_TYPE  hadoop | jdbc | rest | glue, default hadoop.
#   ELT_PIPELINE_ICEBERG_CATALOG_URI   Required when type=jdbc (H2 file / PG etc)
#                                      or type=rest (Polaris/Nessie/Lakekeeper/Tabular).
#   ELT_PIPELINE_ICEBERG_REST_TOKEN    Bearer/API token for type=rest.
#   ELT_PIPELINE_ICEBERG_REST_WAREHOUSE Warehouse name/ID for multi-tenant REST.
#   ELT_PIPELINE_ICEBERG_GLUE_REGION   AWS region for type=glue.
#   ELT_PIPELINE_ICEBERG_CATALOG_NAME  Exposed catalog name, default iceberg.
#   TRINO_SERVER_TGZ                   Path to a cached trino-server-*.tar.gz.
#   TRINO_DOWNLOAD_URL                 Override for bootstrap download.
#
# Examples:
#   ops/trino_serving/run_trino.sh start
#   ops/trino_serving/run_trino.sh cli -- --catalog iceberg --execute 'SHOW NAMESPACES'
#   ELT_PIPELINE_ICEBERG_CATALOG_TYPE=glue \
#     ELT_PIPELINE_ICEBERG_GLUE_REGION=us-east-1 \
#     ops/trino_serving/run_trino.sh start
#   ELT_PIPELINE_ICEBERG_CATALOG_TYPE=rest \
#     ELT_PIPELINE_ICEBERG_CATALOG_URI=http://localhost:8181/api/v1 \
#     ELT_PIPELINE_ICEBERG_REST_TOKEN=my-token \
#     ops/trino_serving/run_trino.sh start
#   ops/trino_serving/run_trino.sh stop
#
set -euo pipefail

if [[ -n "${ELT_PIPELINE_REPO_RUN_DIR:-}" ]]; then
  REPO_RUN_ELT="${ELT_PIPELINE_REPO_RUN_DIR%/}/results/elt_pipeline"
else
  REPO_RUN_ELT="$HOME/Documents/__data/repo_run/results/elt_pipeline"
fi
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." >/dev/null 2>&1 && pwd)"

TRINO_PORT="${ELT_PIPELINE_TRINO_PORT:-8080}"
TRINO_HOST="${ELT_PIPELINE_TRINO_HOST:-127.0.0.1}"
TRINO_VERSION="${ELT_PIPELINE_TRINO_VERSION:-468}"
TRINO_CACHE_DIR="${REPO_RUN_ELT}/.cache/trino"
TRINO_HOME_DIR="${TRINO_CACHE_DIR}"
TRINO_SERVER_NAME="trino-server-${TRINO_VERSION}"
TRINO_RUNTIME="${REPO_RUN_ELT}/trino"
TRINO_ETC_DIR="${TRINO_RUNTIME}/etc"
TRINO_CATALOG_DIR="${TRINO_ETC_DIR}/catalog"
TRINO_DATA_DIR="${TRINO_RUNTIME}/data"
TRINO_LOG_DIR="${REPO_RUN_ELT}/.artifacts/trino"
TRINO_PID_FILE="${TRINO_RUNTIME}/var/run/launcher.pid"

ICEBERG_CATALOG_NAME="${ELT_PIPELINE_ICEBERG_CATALOG_NAME:-iceberg}"
ICEBERG_CATALOG_TYPE="${ELT_PIPELINE_ICEBERG_CATALOG_TYPE:-hadoop}"
ICEBERG_WAREHOUSE_DIR="${ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR:-${REPO_RUN_ELT}/warehouse/iceberg}"
ICEBERG_CATALOG_URI="${ELT_PIPELINE_ICEBERG_CATALOG_URI:-}"
ICEBERG_REST_TOKEN="${ELT_PIPELINE_ICEBERG_REST_TOKEN:-}"
ICEBERG_REST_WAREHOUSE="${ELT_PIPELINE_ICEBERG_REST_WAREHOUSE:-}"
ICEBERG_GLUE_REGION="${ELT_PIPELINE_ICEBERG_GLUE_REGION:-}"

mkdir -p \
  "${TRINO_CACHE_DIR}" \
  "${TRINO_ETC_DIR}" \
  "${TRINO_CATALOG_DIR}" \
  "${TRINO_DATA_DIR}" \
  "${TRINO_LOG_DIR}"

log() { printf '[trino_serving] %s\n' "$*"; }

ensure_bootstrap() {
  if [[ -x "${TRINO_HOME_DIR}/bin/launcher" ]]; then
    return 0
  fi
  local tgz="${TRINO_SERVER_TGZ:-${REPO_RUN_ELT}/.cache/${TRINO_SERVER_NAME}.tar.gz}"
  local -a tgz_candidates=(
    "${tgz}"
    "${REPO_RUN_ELT}/.cache/trino-server-${TRINO_VERSION}.tar.gz"
  )
  local found_tgz=""
  for c in "${tgz_candidates[@]}"; do
    if [[ -f "${c}" ]]; then found_tgz="${c}"; break; fi
  done
  if [[ -z "${found_tgz}" ]]; then
    local url="${TRINO_DOWNLOAD_URL:-https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/${TRINO_SERVER_NAME}.tar.gz}"
    log "Bootstrap: fetching ${TRINO_SERVER_NAME} -> ${tgz}"
    mkdir -p "$(dirname -- "${tgz}")"
    if command -v curl >/dev/null 2>&1; then
      curl -fSL --retry 3 -o "${tgz}" "${url}"
    elif command -v wget >/dev/null 2>&1; then
      wget --tries=3 -O "${tgz}" "${url}"
    else
      echo "ERROR: need curl or wget to bootstrap Trino." >&2
      exit 2
    fi
    found_tgz="${tgz}"
  fi
  log "Bootstrap: extracting ${found_tgz} into staging under ${TRINO_CACHE_DIR}"
  local stage="${TRINO_CACHE_DIR}/.stage_$$"
  rm -rf -- "${stage}"
  mkdir -p -- "${stage}"
  tar -xzf "${found_tgz}" -C "${stage}" --strip-components=1
  # Copy candidate contents without clobbering if launcher exists (TOCTOU just in case).
  if [[ -x "${TRINO_HOME_DIR}/bin/launcher" ]]; then
    rm -rf -- "${stage}"
    return 0
  fi
  mkdir -p -- "${TRINO_HOME_DIR}"
  # Copy to avoid rename across filesystems bounds; no -n fallback on macOS cp? Use rsync if avail else cp with dest overwrite guard:
  if command -v rsync >/dev/null 2>&1; then
    rsync -a -- "${stage}/" "${TRINO_HOME_DIR}/"
  else
    cp -R -- "${stage}/"* "${TRINO_HOME_DIR}/"
  fi
  rm -rf -- "${stage}"
  chmod +x "${TRINO_HOME_DIR}/bin/launcher"
}

write_configs() {
  local plugin_dir="${TRINO_HOME_DIR}/plugin"
  if [[ ! -d "${plugin_dir}" ]]; then
    log "WARNING: ${plugin_dir} not present after bootstrap. Trino may fail to start." >&2
  fi
  log "Writing configs into ${TRINO_ETC_DIR}"
  cat > "${TRINO_ETC_DIR}/node.properties" <<EOF
node.environment=elt_pipeline_iceberg
node.id=eltp-coordinator-1
node.data-dir=${TRINO_DATA_DIR}
EOF
  cat > "${TRINO_ETC_DIR}/jvm.config" <<EOF
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
EOF
  cat > "${TRINO_ETC_DIR}/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=${TRINO_PORT}
http-server.https.enabled=false
http-server.http.host=${TRINO_HOST}
discovery.uri=http://${TRINO_HOST}:${TRINO_PORT}
plugin.dir=${plugin_dir}
web-ui.enabled=false
query.max-memory=2GB
query.max-memory-per-node=2GB
node.internal-address=${TRINO_HOST}
node.environment=elt_pipeline_iceberg
discovery-server.enabled=true
EOF
  mkdir -p -- "${TRINO_ETC_DIR}/catalog"
  cat > "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
connector.name=iceberg
EOF
  case "${ICEBERG_CATALOG_TYPE}" in
    hadoop)
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=hadoop
iceberg.warehouse=${ICEBERG_WAREHOUSE_DIR}
fs.hadoop.enabled=true
EOF
      ;;
    jdbc)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ELT_PIPELINE_ICEBERG_CATALOG_URI is required when type=jdbc." >&2
        exit 3
      fi
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.connection-url=${ICEBERG_CATALOG_URI}
iceberg.warehouse=${ICEBERG_WAREHOUSE_DIR}
fs.hadoop.enabled=true
EOF
      jdbc_driver="${ELT_PIPELINE_ICEBERG_JDBC_DRIVER:-}"
      if [[ -n "${jdbc_driver}" ]]; then
        echo "iceberg.jdbc-catalog.driver-class=${jdbc_driver}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    rest)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ELT_PIPELINE_ICEBERG_CATALOG_URI is required when type=rest." >&2
        exit 3
      fi
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${ICEBERG_CATALOG_URI}
EOF
      if [[ -n "${ICEBERG_REST_TOKEN}" ]]; then
        echo "iceberg.rest-catalog.token=${ICEBERG_REST_TOKEN}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      rest_wh="${ICEBERG_REST_WAREHOUSE:-${ICEBERG_WAREHOUSE_DIR}}"
      if [[ -n "${rest_wh}" ]]; then
        echo "iceberg.warehouse=${rest_wh}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    glue)
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=glue
EOF
      if [[ -n "${ICEBERG_GLUE_REGION}" ]]; then
        echo "iceberg.glue.region=${ICEBERG_GLUE_REGION}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      if [[ -n "${ICEBERG_WAREHOUSE_DIR}" ]]; then
        echo "iceberg.warehouse=${ICEBERG_WAREHOUSE_DIR}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    *)
      echo "ERROR: unsupported ELT_PIPELINE_ICEBERG_CATALOG_TYPE=${ICEBERG_CATALOG_TYPE}" >&2
      exit 4
      ;;
  esac
}

launcher_args() {
  local -a args=(
    --verbose
    --etc-dir="${TRINO_ETC_DIR}"
    --data-dir="${TRINO_DATA_DIR}"
    --pid-file="${TRINO_PID_FILE}"
    --launcher-log-file="${TRINO_LOG_DIR}/launcher.log"
    --server-log-file="${TRINO_LOG_DIR}/server.log"
  )
  printf '%s\n' "${args[@]}"
}

action="${1:-status}"
shift || true

case "${action}" in
  write-configs)
    ensure_bootstrap
    write_configs
    log "Configs written. serving_endpoint.jdbc = jdbc:trino://${TRINO_HOST}:${TRINO_PORT}/${ICEBERG_CATALOG_NAME}"
    ;;
  start)
    ensure_bootstrap
    write_configs
    log "Starting Trino on ${TRINO_HOST}:${TRINO_PORT} (catalog=${ICEBERG_CATALOG_NAME})"
    "${TRINO_HOME_DIR}/bin/launcher" $(launcher_args) start
    log "Started. PID file: ${TRINO_PID_FILE}  JDBC: jdbc:trino://${TRINO_HOST}:${TRINO_PORT}/${ICEBERG_CATALOG_NAME}"
    ;;
  stop)
    if [[ -x "${TRINO_HOME_DIR}/bin/launcher" ]]; then
      "${TRINO_HOME_DIR}/bin/launcher" $(launcher_args) stop || true
      log "Stopped."
    else
      log "Trino not bootstrapped; nothing to stop."
    fi
    ;;
  restart)
    ensure_bootstrap
    write_configs
    "${TRINO_HOME_DIR}/bin/launcher" $(launcher_args) restart
    log "Restarted."
    ;;
  status)
    if [[ ! -x "${TRINO_HOME_DIR}/bin/launcher" ]]; then
      echo "Trino not bootstrapped. Run: $0 start (or $0 write-configs first)."
      exit 1
    fi
    "${TRINO_HOME_DIR}/bin/launcher" $(launcher_args) status
    rc=$?
    echo "jdbc_endpoint: jdbc:trino://${TRINO_HOST}:${TRINO_PORT}/${ICEBERG_CATALOG_NAME}"
    echo "iceberg_warehouse: ${ICEBERG_WAREHOUSE_DIR}"
    echo "catalog_name: ${ICEBERG_CATALOG_NAME}"
    echo "catalog_type: ${ICEBERG_CATALOG_TYPE}"
    case "${ICEBERG_CATALOG_TYPE}" in
      jdbc)  echo "jdbc_catalog_uri: ${ICEBERG_CATALOG_URI}" ;;
      rest)
        echo "rest_catalog_uri: ${ICEBERG_CATALOG_URI}"
        echo "rest_warehouse: ${ICEBERG_REST_WAREHOUSE:-<default>}"
        echo "rest_token_provided: $([[ -n "${ICEBERG_REST_TOKEN}" ]] && echo yes || echo no)"
        ;;
      glue)
        echo "glue_region: ${ICEBERG_GLUE_REGION:-<AWS SDK default>}"
        ;;
    esac
    exit ${rc}
    ;;
  cli)
    CLI_BIN="${TRINO_HOME_DIR}/bin/trino"
    if [[ ! -x "${CLI_BIN}" ]]; then
      # Trino 468 tarball ships `trino` in bin/ — if not, fail with hint.
      echo "ERROR: ${CLI_BIN} not found in Trino bootstrap. Provide a client CLI separately." >&2
      exit 5
    fi
    exec "${CLI_BIN}" \
      --server "${TRINO_HOST}:${TRINO_PORT}" \
      --catalog "${ICEBERG_CATALOG_NAME}" \
      --user elt_pipeline \
      "$@"
    ;;
  env)
    cat <<EOF
REPO_RUN_ELT=${REPO_RUN_ELT}
TRINO_RUNTIME=${TRINO_RUNTIME}
TRINO_HOME_DIR=${TRINO_HOME_DIR}
TRINO_PORT=${TRINO_PORT}
TRINO_HOST=${TRINO_HOST}
ICEBERG_CATALOG_NAME=${ICEBERG_CATALOG_NAME}
ICEBERG_CATALOG_TYPE=${ICEBERG_CATALOG_TYPE}
ICEBERG_WAREHOUSE_DIR=${ICEBERG_WAREHOUSE_DIR}
ICEBERG_CATALOG_URI=${ICEBERG_CATALOG_URI}
ICEBERG_REST_TOKEN_PROVIDED=$([[ -n "${ICEBERG_REST_TOKEN}" ]] && echo yes || echo no)
ICEBERG_REST_WAREHOUSE=${ICEBERG_REST_WAREHOUSE:-}
ICEBERG_GLUE_REGION=${ICEBERG_GLUE_REGION:-}
jdbc_endpoint=jdbc:trino://${TRINO_HOST}:${TRINO_PORT}/${ICEBERG_CATALOG_NAME}
athena_aws_binding_doc=docs/operator/LOCAL_OPERATOR_RUNBOOK.md
EOF
    ;;
  *)
    echo "Usage: $0 (start|stop|status|restart|cli|write-configs|env)" >&2
    exit 10
    ;;
esac

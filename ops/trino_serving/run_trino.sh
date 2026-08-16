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
#   ELT_PIPELINE_TRINO_VERSION         Default 468 (matches manifest).
#   ELT_PIPELINE_ICEBERG_WAREHOUSE_DIR Iceberg warehouse dir (default:
#                                      $repo_run_parent/warehouse/iceberg).
#   ELT_PIPELINE_ICEBERG_SERVING_CATALOG_TYPE
#                         Trino SERVING catalog: jdbc | rest | glue | nessie | snowflake
#                         Default = jdbc (zero-service workstation; AUTO sqlite URI below
#                         when ELT_PIPELINE_ICEBERG_CATALOG_URI is unset).
#                         Backward compat: ELT_PIPELINE_ICEBERG_CATALOG_TYPE (LEGACY) also
#                         honored, with one exception — legacy "hadoop" on the Trino
#                         serving side is remapped to "jdbc" (Trino 468 removed the
#                         hadoop catalog type enum entirely; jdbc+sqlite is the bridge).
#   ELT_PIPELINE_ICEBERG_CATALOG_URI   Required when SERVING_CATALOG_TYPE in
#                                      {rest, nessie, snowflake} or when SERVING=jdbc
#                                      and you want Postgres/MySQL/H2 instead of the
#                                      AUTO sqlite workstation default.
#                                      When SERVING_CATALOG_TYPE=jdbc and this is
#                                      UNSET we auto-generate a file-based SQLite URI
#                                      pointing at .artifacts/trino/iceberg_jdbc_metastore.db
#                                      (this is the zero-service workstation default; the
#                                      SQLite file is a disposable cache populated by
#                                      CALL system.register_table() from canonical
#                                      Iceberg metadata JSON files in the warehouse).
#   ELT_PIPELINE_ICEBERG_REST_TOKEN    Bearer/API token for type=rest|nessie|snowflake.
#   ELT_PIPELINE_ICEBERG_REST_WAREHOUSE Warehouse name/ID for multi-tenant REST.
#   ELT_PIPELINE_ICEBERG_GLUE_REGION   AWS region for type=glue.
#   ELT_PIPELINE_ICEBERG_CATALOG_NAME  Exposed catalog name, default iceberg.
#   ELT_PIPELINE_ICEBERG_JDBC_DRIVER   Optional driver-class override when SERVING=jdbc.
#   TRINO_SERVER_TGZ                   Path to a cached trino-server-*.tar.gz.
#   TRINO_DOWNLOAD_URL                 Override for bootstrap download.
#
# Examples:
#   ops/trino_serving/run_trino.sh start
#   ops/trino_serving/run_trino.sh cli -- --catalog iceberg --execute 'SHOW SCHEMAS'
#   ELT_PIPELINE_ICEBERG_SERVING_CATALOG_TYPE=glue \
#     ELT_PIPELINE_ICEBERG_GLUE_REGION=us-east-1 \
#     ops/trino_serving/run_trino.sh start
#   ELT_PIPELINE_ICEBERG_SERVING_CATALOG_TYPE=rest \
#     ELT_PIPELINE_ICEBERG_CATALOG_URI=http://localhost:8181/api/v1 \
#     ELT_PIPELINE_ICEBERG_REST_TOKEN=my-token \
#     ops/trino_serving/run_trino.sh start
#   ops/trino_serving/run_trino.sh stop
#
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." >/dev/null 2>&1 && pwd)"

# ==============================================================================
# BOOTSTRAP: load every default, env-var name, path, version, and boolean flag
#   ONE TIME from the single manifest:
#     src/elt_pipeline/config/runtime_manifest.py
#
# Python source files and this ONE bash script share ZERO duplicated defaults.
# Python reads runtime_manifest.py directly; bash uses the manifest Python to
# emit a KEY=VALUE file we then source. Fail-fast on any error so that a missing
# venv / bad manifest attribute / broken PYTHONPATH aborts immediately with a
# clear error (no silent fallback to empty strings that turn into "////etc").
# ==============================================================================
if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  _MANIFEST_PYTHON="${REPO_ROOT}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  _MANIFEST_PYTHON="python3"
else
  cat >&2 <<'EOF'
ERROR [run_trino.sh bootstrap]: python3 not found in PATH.
  → Either activate the elt_pipeline .venv, or install uv/python on PATH.
  → Expected: ${REPO_ROOT}/.venv/bin/python (preferred) or `python3` on PATH.
EOF
  exit 7
fi

_MANIFEST_BOOTSTRAP_FILE="$(mktemp)"
_MANIFEST_PY_ERR="$(mktemp)"
_MANIFEST_DEBUG_OUT="${ELT_PIPELINE_DEBUG_BOOTSTRAP_FILE:-}"
if [[ -n "${_MANIFEST_DEBUG_OUT}" ]]; then
  _MANIFEST_BOOTSTRAP_FILE="${_MANIFEST_DEBUG_OUT}"
fi
trap '[[ -f "${_MANIFEST_PY_ERR}" ]] && rm -f "${_MANIFEST_PY_ERR}"
[[ -n "${_MANIFEST_DEBUG_OUT}" ]] || { [[ -f "${_MANIFEST_BOOTSTRAP_FILE}" ]] && rm -f "${_MANIFEST_BOOTSTRAP_FILE}"; }' EXIT

if ! REPO_ROOT="${REPO_ROOT}" PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
     "${_MANIFEST_PYTHON}" - "${_MANIFEST_BOOTSTRAP_FILE}" \
     <<'PY' 2>"${_MANIFEST_PY_ERR}"
import pathlib, sys, os
sys.path.insert(0, next(p for p in os.environ.get("PYTHONPATH","").split(os.pathsep) if p))
from elt_pipeline.config.runtime_manifest import runtime_manifest as M

# ---------------------------------------------------------------------------
# PORTABILITY CONTRACT: YAML runtime. section is the user-visible no-code
# interface. Load it via the same helper used by the CLI so defaults cascade
# consistently. Layering applied here:
#   YAML (pipeline.runtime → environments[default].runtime →
#         environments[$ELT_PIPELINE_ENVIRONMENT].runtime)
#   OVER frozen manifest defaults.
# User ENV vars then take highest precedence via bash _lookup_env later.
# ENV is NEVER required — it is purely an override (CI / secrets convenience).
# Auto-discovery chain (first hit wins):
#   1. ELT_PIPELINE_CONFIG_PATH env var  (user override for multi-config setups)
#   2. <repo_root>/pipeline.yaml         (clone-n-run root config, always present)
# ---------------------------------------------------------------------------
_yaml_overrides: dict = {}
_yaml_cp = os.environ.get("ELT_PIPELINE_CONFIG_PATH", "").strip() or None
_yaml_env = os.environ.get("ELT_PIPELINE_ENVIRONMENT", "").strip() or None
if _yaml_cp is None:
    _repo_root_from_env = os.environ.get("REPO_ROOT", "").strip()
    if _repo_root_from_env:
        _candidate = pathlib.Path(_repo_root_from_env) / "pipeline.yaml"
        if _candidate.is_file():
            _yaml_cp = str(_candidate)
if _yaml_cp is not None:
    from elt_pipeline.config.loader import load_runtime_overrides
    _yaml_overrides = load_runtime_overrides(_yaml_cp, environment=_yaml_env)

_ts = _yaml_overrides.get("trino_serving", {}) if isinstance(_yaml_overrides, dict) else {}
_iw = _yaml_overrides.get("iceberg_writer", {}) if isinstance(_yaml_overrides, dict) else {}
_is = _yaml_overrides.get("iceberg_serving", {}) if isinstance(_yaml_overrides, dict) else {}
_y_repo_run_dir_raw = (
    _yaml_overrides.get("repo_run_dir") if isinstance(_yaml_overrides, dict) else None
)
_y_repo_run_dir = "" if _y_repo_run_dir_raw in (None, "") else str(_y_repo_run_dir_raw)
def _y(field, mapping, default):
    v = mapping.get(field) if isinstance(mapping, dict) else None
    return default if v in (None, "") else v

lines = []
def emit(key, val):
    lines.append(f'{key}="{val}"')
def emit_bool(key, python_bool):
    lines.append(f'{key}="{'1' if python_bool else '0'}"')

_y_trino_port = _y("port", _ts, M.serving.default_trino_port)
_y_trino_host = _y("host", _ts, M.serving.default_trino_host)
_y_trino_version = _y("version", _ts, M.versions.trino_server)
_y_catalog_name = _y("catalog_name", _iw,
                      _y("catalog_name", _is, M.catalogs.default_catalog_name))
_y_writer_catalog = _y("catalog_type", _iw, M.catalogs.workstation_default_writer_catalog)
_y_serving_catalog = _y("catalog_type", _is, M.catalogs.workstation_default_serving_catalog)
_y_jdbc_sqlite_class = _y("jdbc_driver", _is, M.jdbc.sqlite_class)
_y_node_environment = _y("node_environment", _ts, M.serving.default_node_environment)
_y_http_auth_type = _y("http_authentication_type", _ts,
                        M.serving.default_http_server_authentication_type)
_y_coord = _y("coordinator", _ts, M.serving.default_coordinator)
_y_inc_coord = _y("include_coordinator", _ts, M.serving.default_include_coordinator)
_y_fs_hadoop = _y("fs_hadoop_enabled", _ts, M.serving.always_emit_fs_hadoop_enabled)
_y_reg_tbl = _y("register_table_procedure_enabled", _ts,
                 M.serving.always_enable_register_table_procedure)

emit("VAR_DEFAULT_PORT",                       _y_trino_port)
emit("VAR_DEFAULT_HOST",                       _y_trino_host)
emit("VAR_DEFAULT_TRINO_VERSION",              _y_trino_version)
emit("VAR_DEFAULT_CATALOG_NAME",               _y_catalog_name)
emit("VAR_DEFAULT_WRITER_CATALOG",             _y_writer_catalog)
emit("VAR_DEFAULT_SERVING_CATALOG",            _y_serving_catalog)
emit("VAR_DEFAULT_JDBC_SQLITE_CLASS",          _y_jdbc_sqlite_class)
emit("VAR_DEFAULT_NODE_ENVIRONMENT",           _y_node_environment)
emit("VAR_DEFAULT_HTTP_AUTH_TYPE",             _y_http_auth_type)
emit_bool("VAR_COORDINATOR",                   bool(_y_coord))
emit_bool("VAR_INCLUDE_COORDINATOR",           bool(_y_inc_coord))
emit_bool("VAR_FS_HADOOP_ENABLED",             bool(_y_fs_hadoop))
emit_bool("VAR_REGISTER_TABLE_PROCEDURE_ENABLED", bool(_y_reg_tbl))

emit("VAR_ENV_REPO_RUN_DIR",                   M.env.repo_run_dir)
emit("VAR_ENV_TRINO_PORT",                     M.env.trino_port)
emit("VAR_ENV_TRINO_HOST",                     M.env.trino_host)
emit("VAR_ENV_TRINO_VERSION",                  M.env.trino_version)
emit("VAR_ENV_CATALOG_NAME",                   M.env.iceberg_catalog_name)
emit("VAR_ENV_WRITER_CATALOG_TYPE",            M.env.iceberg_writer_catalog_type)
emit("VAR_ENV_SERVING_CATALOG_TYPE",           M.env.iceberg_serving_catalog_type)
emit("VAR_ENV_CATALOG_TYPE_LEGACY",            M.env.iceberg_catalog_type_legacy)
emit("VAR_ENV_WAREHOUSE_DIR",                  M.env.iceberg_warehouse_dir)
emit("VAR_ENV_CATALOG_URI",                    M.env.iceberg_catalog_uri)
emit("VAR_ENV_REST_TOKEN",                     M.env.iceberg_rest_token)
emit("VAR_ENV_REST_WAREHOUSE",                 M.env.iceberg_rest_warehouse)
emit("VAR_ENV_GLUE_REGION",                    M.env.iceberg_glue_region)
emit("VAR_ENV_JDBC_DRIVER",                    M.env.iceberg_jdbc_driver)

emit("VAR_PATH_RESULTS_ELT_RELPATH",              M.paths.repo_run_results_elt_relpath)
emit("VAR_PATH_USER_REPO_RUN_HOME",               M.paths.default_user_repo_run_home)
emit("VAR_PATH_TRINO_CACHE_RELPATH",              M.paths.trino_cache_relpath)
emit("VAR_PATH_TRINO_ARTIFACTS_RELPATH",          M.paths.trino_artifacts_relpath)
emit("VAR_PATH_TRINO_INSTALL_RELPATH",            M.paths.trino_install_relpath)
emit("VAR_PATH_ICEBERG_WAREHOUSE_RELPATH",        M.paths.iceberg_warehouse_relpath)
emit("VAR_PATH_SERVING_JDBC_METASTORE_RELPATH",   M.paths.serving_jdbc_metastore_relpath)
emit("VAR_PATH_TRINO_TARBALL_TEMPLATE",           M.paths.trino_server_tarball_relpath_template)
emit("VAR_YAML_REPO_RUN_DIR",                     _y_repo_run_dir)
lines.append(f'VAR_SERVING_CATALOG_VALIDS="{" ".join(M.catalogs.serving_catalog_type_valid_values)}"')
lines.append(f'VAR_WRITER_CATALOG_VALIDS="{" ".join(M.catalogs.writer_catalog_type_valid_values)}"')

pathlib.Path(sys.argv[1]).write_text("\n".join(lines) + "\n")
PY
then
  _manifest_exit=$?
  cat >&2 <<EOF
ERROR [run_trino.sh bootstrap]: manifest python emit failed (rc=${_manifest_exit}).
  REPO_ROOT   = ${REPO_ROOT}
  PYTHON      = ${_MANIFEST_PYTHON}
  PYTHONPATH  = ${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}
  Output file = ${_MANIFEST_BOOTSTRAP_FILE}
  --- Python stderr --------------------------------------------------------------
$(cat "${_MANIFEST_PY_ERR}")
--------------------------------------------------------------------------------
  → Ensure the .venv has the elt_pipeline package installed in develop mode:
      cd ${REPO_ROOT} && uv sync --dev
EOF
  exit 8
fi

# Source the KEY=VALUE file generated by the manifest python. Validate every
# scalar we depend on is non-empty.
set -a
# shellcheck disable=SC1090
source "${_MANIFEST_BOOTSTRAP_FILE}"
set +a

_REQUIRED_SCALARS=(
  VAR_DEFAULT_PORT VAR_DEFAULT_HOST VAR_DEFAULT_TRINO_VERSION
  VAR_DEFAULT_CATALOG_NAME VAR_DEFAULT_SERVING_CATALOG VAR_DEFAULT_JDBC_SQLITE_CLASS
  VAR_DEFAULT_NODE_ENVIRONMENT VAR_DEFAULT_HTTP_AUTH_TYPE
  VAR_COORDINATOR VAR_INCLUDE_COORDINATOR VAR_FS_HADOOP_ENABLED VAR_REGISTER_TABLE_PROCEDURE_ENABLED
  VAR_ENV_REPO_RUN_DIR VAR_ENV_TRINO_PORT VAR_ENV_TRINO_HOST VAR_ENV_TRINO_VERSION
  VAR_ENV_CATALOG_NAME VAR_ENV_SERVING_CATALOG_TYPE VAR_ENV_WAREHOUSE_DIR VAR_ENV_CATALOG_URI
  VAR_PATH_RESULTS_ELT_RELPATH VAR_PATH_USER_REPO_RUN_HOME VAR_PATH_TRINO_ARTIFACTS_RELPATH
  VAR_PATH_TRINO_INSTALL_RELPATH VAR_PATH_ICEBERG_WAREHOUSE_RELPATH
  VAR_PATH_SERVING_JDBC_METASTORE_RELPATH VAR_PATH_TRINO_TARBALL_TEMPLATE
  VAR_SERVING_CATALOG_VALIDS
)
_missing=()
for _s in "${_REQUIRED_SCALARS[@]}"; do
  if [[ -z "${!_s:-}" ]]; then
    _missing+=("${_s}")
  fi
done
if ((${#_missing[@]} > 0)); then
  cat >&2 <<EOF
ERROR [run_trino.sh bootstrap]: one or more manifest scalars resolved empty after source.
  Missing vars: ${_missing[*]}
  Manifest source: ${_MANIFEST_BOOTSTRAP_FILE}
  Emitted file contents:
$(cat "${_MANIFEST_BOOTSTRAP_FILE}")
EOF
  exit 9
fi
unset _s _missing _REQUIRED_SCALARS _manifest_exit

# Repo run root resolution.
# Priority (portability contract — ENV is optional override only):
#   1. User ENV via VAR_ENV_REPO_RUN_DIR → highest (CI/secrets override)
#   2. YAML runtime.repo_run_dir → no-code portable user sets once after clone
#   3. Frozen manifest default ($HOME/...) → absolute floor, zero-config workstation
_rer_override=""
# Portable "is variable set" check using ${VARNAME+marker — works on bash 3.2
# (macOS default) AND newer bash; avoids `[[ -v NAME ]]` which chokes on indirect
# names on older bash (emits: "conditional binary operator expected").
_rer_set_marker="${!VAR_ENV_REPO_RUN_DIR+SET}"
if [[ -n "${_rer_set_marker}" ]]; then
  _rer_override="${!VAR_ENV_REPO_RUN_DIR}"
fi
if [[ -n "${_rer_override}" ]]; then
  REPO_RUN_ELT="${_rer_override%/}/${VAR_PATH_RESULTS_ELT_RELPATH}"
elif [[ -n "${VAR_YAML_REPO_RUN_DIR}" ]]; then
  REPO_RUN_ELT="${VAR_YAML_REPO_RUN_DIR%/}/${VAR_PATH_RESULTS_ELT_RELPATH}"
else
  REPO_RUN_ELT="${HOME}/${VAR_PATH_USER_REPO_RUN_HOME}/${VAR_PATH_RESULTS_ELT_RELPATH}"
fi
unset _rer_override _rer_set_marker

# _lookup_env VAR_ENV_REF [DEFAULT]
# Look up a runtime env via its manifest-driven VAR_ENV_* name.  VAR_ENV_REF names
# the manifest constant that holds the USER-FACING env var name (e.g.
# VAR_ENV_TRINO_PORT → value is "ELT_PIPELINE_TRINO_PORT").  If that user-facing
# env var exists and is non-empty, return its value; otherwise return DEFAULT.
_lookup_env() {
  local _name_ref="${1}"
  local _default="${2:-}"
  local _user_var="${!_name_ref}"
  if [[ -z "${_user_var}" ]]; then
    printf '%s' "${_default}"
    return 0
  fi
  local _set_marker="${!_user_var+SET}"
  if [[ -z "${_set_marker}" ]]; then
    printf '%s' "${_default}"
    return 0
  fi
  local _user_val="${!_user_var}"
  if [[ -n "${_user_val}" ]]; then
    printf '%s' "${_user_val}"
  else
    printf '%s' "${_default}"
  fi
}

TRINO_PORT="$(_lookup_env VAR_ENV_TRINO_PORT "${VAR_DEFAULT_PORT}")"
TRINO_HOST="$(_lookup_env VAR_ENV_TRINO_HOST "${VAR_DEFAULT_HOST}")"
TRINO_VERSION="$(_lookup_env VAR_ENV_TRINO_VERSION "${VAR_DEFAULT_TRINO_VERSION}")"
TRINO_CACHE_DIR="${REPO_RUN_ELT}/${VAR_PATH_TRINO_CACHE_RELPATH}"
TRINO_HOME_DIR="${TRINO_CACHE_DIR}"
TRINO_SERVER_NAME="trino-server-${TRINO_VERSION}"
TRINO_RUNTIME="${REPO_RUN_ELT}/${VAR_PATH_TRINO_INSTALL_RELPATH}"
TRINO_ETC_DIR="${TRINO_RUNTIME}/etc"
TRINO_CATALOG_DIR="${TRINO_ETC_DIR}/catalog"
TRINO_DATA_DIR="${TRINO_RUNTIME}/data"
TRINO_LOG_DIR="${REPO_RUN_ELT}/${VAR_PATH_TRINO_ARTIFACTS_RELPATH}"
TRINO_PID_FILE="${TRINO_RUNTIME}/var/run/launcher.pid"

ICEBERG_CATALOG_NAME="$(_lookup_env VAR_ENV_CATALOG_NAME "${VAR_DEFAULT_CATALOG_NAME}")"

# SERVING_CATALOG_TYPE resolution (Trino serving side only):
#   1. Canonical user env named by VAR_ENV_SERVING_CATALOG_TYPE
#   2. Legacy env named by VAR_ENV_CATALOG_TYPE_LEGACY (remap "hadoop" → "jdbc"
#      since Trino 468 dropped HADOOP from the CatalogType enum).
_user_serving="$(_lookup_env VAR_ENV_SERVING_CATALOG_TYPE "")"
if [[ -z "${_user_serving}" ]]; then
  _legacy="$(_lookup_env VAR_ENV_CATALOG_TYPE_LEGACY "")"
  if [[ -z "${_legacy}" ]]; then
    _user_serving="${VAR_DEFAULT_SERVING_CATALOG}"
  elif [[ "${_legacy}" == "hadoop" ]]; then
    _user_serving="${VAR_DEFAULT_SERVING_CATALOG}"
  else
    _user_serving="${_legacy}"
  fi
fi
SERVING_CATALOG_TYPE="${_user_serving}"
unset _user_serving _legacy

_warehouse_default="${REPO_RUN_ELT}/${VAR_PATH_ICEBERG_WAREHOUSE_RELPATH}"
ICEBERG_WAREHOUSE_DIR="$(_lookup_env VAR_ENV_WAREHOUSE_DIR "${_warehouse_default}")"
unset _warehouse_default
ICEBERG_CATALOG_URI="$(_lookup_env VAR_ENV_CATALOG_URI "")"
ICEBERG_REST_TOKEN="$(_lookup_env VAR_ENV_REST_TOKEN "")"
ICEBERG_REST_WAREHOUSE="$(_lookup_env VAR_ENV_REST_WAREHOUSE "")"
ICEBERG_GLUE_REGION="$(_lookup_env VAR_ENV_GLUE_REGION "")"
JDBC_DRIVER="$(_lookup_env VAR_ENV_JDBC_DRIVER "")"

# ---------------------------------------------------------------------------
# Sanity check: every derived path must be absolute AND have zero occurrences
# of empty segments ("/$HOME//path" → regex "//" matches → fail).  This
# catches misconfigured manifest lookups INSTANTLY instead of letting the
# script continue to weird "mkdir: ////etc" errors 60 lines later.
# ---------------------------------------------------------------------------
_path_check_ok=1
_path_check() {
  local _name="${1}" _val="${2}"
  if [[ -z "${_val}" ]]; then
    echo "  ERROR: ${_name} resolved to empty string." >&2
    _path_check_ok=0
    return 0
  fi
  case "${_val}" in
    /*) : ;;
    *) echo "  ERROR: ${_name}=${_val} is NOT an absolute path (must start with /)." >&2; _path_check_ok=0; return 0 ;;
  esac
  # Disallow empty path segments: if we strip trailing /, the remainder must
  # not contain "//".
  local _s="${_val%/}"
  if [[ "${_s}" == *"//"* ]]; then
    echo "  ERROR: ${_name}=${_val} contains empty segment (//); manifest path config is broken." >&2
    _path_check_ok=0
  fi
}
_path_check HOME_DIR              "${HOME}"
_path_check REPO_RUN_ELT          "${REPO_RUN_ELT}"
_path_check TRINO_CACHE_DIR       "${TRINO_CACHE_DIR}"
_path_check TRINO_RUNTIME         "${TRINO_RUNTIME}"
_path_check TRINO_ETC_DIR         "${TRINO_ETC_DIR}"
_path_check TRINO_CATALOG_DIR     "${TRINO_CATALOG_DIR}"
_path_check TRINO_DATA_DIR        "${TRINO_DATA_DIR}"
_path_check TRINO_LOG_DIR         "${TRINO_LOG_DIR}"
_path_check ICEBERG_WAREHOUSE_DIR "${ICEBERG_WAREHOUSE_DIR}"
if [[ "${_path_check_ok}" -ne 1 ]]; then
  cat >&2 <<EOF
ERROR [run_trino.sh path check]: one or more derived paths resolved invalid (see above).
  This almost always means the runtime_manifest.py Python emit produced empty
  VAR_PATH_* scalars, or the user-facing env var referenced by VAR_ENV_REPO_RUN_DIR
  points at a bad location.  The manifest file is single source of truth:
    ${REPO_ROOT}/src/elt_pipeline/config/runtime_manifest.py
  Current resolved scalars (for debugging):
    VAR_ENV_REPO_RUN_DIR                    = ${VAR_ENV_REPO_RUN_DIR}
    \${${VAR_ENV_REPO_RUN_DIR}} (user env)  = ${!VAR_ENV_REPO_RUN_DIR:-<unset>}
    VAR_PATH_USER_REPO_RUN_HOME             = ${VAR_PATH_USER_REPO_RUN_HOME}
    VAR_PATH_RESULTS_ELT_RELPATH            = ${VAR_PATH_RESULTS_ELT_RELPATH}
    VAR_PATH_TRINO_INSTALL_RELPATH          = ${VAR_PATH_TRINO_INSTALL_RELPATH}
    VAR_PATH_TRINO_ARTIFACTS_RELPATH        = ${VAR_PATH_TRINO_ARTIFACTS_RELPATH}
    VAR_PATH_ICEBERG_WAREHOUSE_RELPATH      = ${VAR_PATH_ICEBERG_WAREHOUSE_RELPATH}
EOF
  exit 10
fi
unset _path_check_ok _s _path_check

# Also verify SERVING_CATALOG_TYPE is in the manifest-enumerated valid set
# BEFORE we do anything heavy (download, start, etc).
_in_valid=0
for _v in ${VAR_SERVING_CATALOG_VALIDS}; do
  if [[ "${_v}" == "${SERVING_CATALOG_TYPE}" ]]; then
    _in_valid=1
    break
  fi
done
if [[ "${_in_valid}" -ne 1 ]]; then
  cat >&2 <<EOF
ERROR [run_trino.sh catalog check]: SERVING_CATALOG_TYPE=${SERVING_CATALOG_TYPE} is not in the manifest-enumerated valid set.
  Valid values (from runtime_manifest.catalogs.serving_catalog_type_valid_values):
    ${VAR_SERVING_CATALOG_VALIDS}
  User-facing env vars (choose one):
    canonical : ${VAR_ENV_SERVING_CATALOG_TYPE}
    legacy    : ${VAR_ENV_CATALOG_TYPE_LEGACY} (legacy "hadoop" is auto-remapped to "jdbc")
EOF
  exit 11
fi
unset _v _in_valid

# WORKSTATION ZERO-SERVICE DEFAULT: when SERVING_CATALOG_TYPE=jdbc and the user
# has NOT provided an explicit ICEBERG_CATALOG_URI, synthesize a SQLite URI
# pointing at a disposable, reproducible cache file under .artifacts/trino/.
# This SQLite db is populated from the canonical Iceberg metadata JSON files
# in the warehouse via CALL system.register_table() and can be deleted at any
# time with zero data loss (source of truth = parquet + JSON files on disk).
SERVING_JDBC_AUTO_SQLITE="false"
if [[ "${SERVING_CATALOG_TYPE}" == "jdbc" && -z "${ICEBERG_CATALOG_URI}" ]]; then
  SERVING_JDBC_AUTO_SQLITE="true"
  SERVING_JDBC_METASTORE_PATH="${REPO_RUN_ELT}/${VAR_PATH_SERVING_JDBC_METASTORE_RELPATH}"
  SERVING_JDBC_METASTORE_DIR="$(dirname -- "${SERVING_JDBC_METASTORE_PATH}")"
  mkdir -p -- "${SERVING_JDBC_METASTORE_DIR}"
  ICEBERG_CATALOG_URI="jdbc:sqlite:${SERVING_JDBC_METASTORE_PATH}"
  JDBC_DRIVER="${JDBC_DRIVER:-${VAR_DEFAULT_JDBC_SQLITE_CLASS}}"
fi

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
  local _tarball_rel="${VAR_PATH_TRINO_TARBALL_TEMPLATE//\{version\}/${TRINO_VERSION}}"
  local tgz="${TRINO_SERVER_TGZ:-${REPO_RUN_ELT}/${_tarball_rel}}"
  local -a tgz_candidates=(
    "${tgz}"
    "${REPO_RUN_ELT}/${_tarball_rel}"
  )
  unset _tarball_rel
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
node.environment=${VAR_DEFAULT_NODE_ENVIRONMENT}
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
  _coordinator_bool="false"; [[ "${VAR_COORDINATOR}" == "1" ]] && _coordinator_bool="true"
  _include_coord_bool="false"; [[ "${VAR_INCLUDE_COORDINATOR}" == "1" ]] && _include_coord_bool="true"
  _shared_secret="$(dd if=/dev/urandom bs=1 count=16 2>/dev/null | base64 2>/dev/null || echo 'eltp-dev-static-shared-secret')"
  cat > "${TRINO_ETC_DIR}/config.properties" <<EOF
coordinator=${_coordinator_bool}
node-scheduler.include-coordinator=${_include_coord_bool}
http-server.http.port=${TRINO_PORT}
http-server.https.enabled=false
discovery.uri=http://${TRINO_HOST}:${TRINO_PORT}
plugin.dir=${plugin_dir}
web-ui.enabled=false
query.max-memory=2GB
query.max-memory-per-node=2GB
node.internal-address=${TRINO_HOST}
node.environment=${VAR_DEFAULT_NODE_ENVIRONMENT}
http-server.authentication.type=${VAR_DEFAULT_HTTP_AUTH_TYPE}
internal-communication.shared-secret=eltp-${_shared_secret}
EOF
  unset _coordinator_bool _include_coord_bool _shared_secret
  mkdir -p -- "${TRINO_ETC_DIR}/catalog"
  cat > "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
connector.name=iceberg
EOF
  _emit_fs_hadoop_enabled=""
  [[ "${VAR_FS_HADOOP_ENABLED}" == "1" ]] && _emit_fs_hadoop_enabled="true"
  _emit_register_table_procedure_enabled=""
  [[ "${VAR_REGISTER_TABLE_PROCEDURE_ENABLED}" == "1" ]] && _emit_register_table_procedure_enabled="true"
  case "${SERVING_CATALOG_TYPE}" in
    jdbc)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ${VAR_ENV_CATALOG_URI} is required when SERVING_CATALOG_TYPE=jdbc (or omit to enable AUTO sqlite workstation default)." >&2
        exit 3
      fi
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.connection-url=${ICEBERG_CATALOG_URI}
iceberg.jdbc-catalog.catalog-name=${ICEBERG_CATALOG_NAME}
iceberg.jdbc-catalog.default-warehouse-dir=${ICEBERG_WAREHOUSE_DIR}
EOF
      if [[ -n "${_emit_register_table_procedure_enabled}" ]]; then
        echo "iceberg.register-table-procedure.enabled=${_emit_register_table_procedure_enabled}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      if [[ -n "${_emit_fs_hadoop_enabled}" ]]; then
        echo "fs.hadoop.enabled=${_emit_fs_hadoop_enabled}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      if [[ -n "${JDBC_DRIVER}" ]]; then
        echo "iceberg.jdbc-catalog.driver-class=${JDBC_DRIVER}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    rest)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ${VAR_ENV_CATALOG_URI} is required when SERVING_CATALOG_TYPE=rest." >&2
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
      if [[ -n "${_emit_fs_hadoop_enabled}" ]]; then
        echo "fs.hadoop.enabled=${_emit_fs_hadoop_enabled}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    nessie)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ${VAR_ENV_CATALOG_URI} is required when SERVING_CATALOG_TYPE=nessie." >&2
        exit 3
      fi
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=${ICEBERG_CATALOG_URI}
EOF
      if [[ -n "${ICEBERG_REST_TOKEN}" ]]; then
        echo "iceberg.nessie-catalog.authentication.type=BEARER" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
        echo "iceberg.nessie-catalog.authentication.token=${ICEBERG_REST_TOKEN}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      rest_wh="${ICEBERG_REST_WAREHOUSE:-${ICEBERG_WAREHOUSE_DIR}}"
      if [[ -n "${rest_wh}" ]]; then
        echo "iceberg.warehouse=${rest_wh}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      if [[ -n "${_emit_fs_hadoop_enabled}" ]]; then
        echo "fs.hadoop.enabled=${_emit_fs_hadoop_enabled}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    snowflake)
      if [[ -z "${ICEBERG_CATALOG_URI}" ]]; then
        echo "ERROR: ${VAR_ENV_CATALOG_URI} is required when SERVING_CATALOG_TYPE=snowflake." >&2
        exit 3
      fi
      cat >> "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties" <<EOF
iceberg.catalog.type=snowflake
iceberg.snowflake-catalog.uri=${ICEBERG_CATALOG_URI}
EOF
      if [[ -n "${ICEBERG_REST_TOKEN}" ]]; then
        echo "iceberg.snowflake-catalog.authentication.token=${ICEBERG_REST_TOKEN}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      rest_wh="${ICEBERG_REST_WAREHOUSE:-${ICEBERG_WAREHOUSE_DIR}}"
      if [[ -n "${rest_wh}" ]]; then
        echo "iceberg.warehouse=${rest_wh}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      if [[ -n "${_emit_fs_hadoop_enabled}" ]]; then
        echo "fs.hadoop.enabled=${_emit_fs_hadoop_enabled}" >> \
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
      if [[ -n "${_emit_fs_hadoop_enabled}" ]]; then
        echo "fs.hadoop.enabled=${_emit_fs_hadoop_enabled}" >> \
          "${TRINO_ETC_DIR}/catalog/${ICEBERG_CATALOG_NAME}.properties"
      fi
      ;;
    *)
      echo "ERROR: unsupported ${VAR_ENV_SERVING_CATALOG_TYPE}=${SERVING_CATALOG_TYPE} (valid: ${VAR_SERVING_CATALOG_VALIDS})" >&2
      exit 4
      ;;
  esac
  unset _emit_fs_hadoop_enabled _emit_register_table_procedure_enabled
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

#!/usr/bin/env bash
#
# Foreground wrapper for Trino inside the container / k8s pod.
#
# The canonical ops/trino_serving/run_trino.sh supports daemon mode
# (start|stop|status) via bin/launcher. For container orchestration we
# need a foreground process, so:
#   1. Run `run_trino.sh write-configs` once to materialize etc/ catalogs.
#   2. Launch `bin/launcher --verbose run` (foreground subcommand).
#
# Stdout/stderr from the launcher → container logs; docker stop / SIGTERM
# → launcher graceful shutdown.
#
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/usr/share/elt_pipeline}"
export REPO_ROOT
export VENV_PY="${VENV_PY:-/opt/elt_pipeline_venv/bin/python}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT_ABS="$(cd -- "${SCRIPT_DIR}/../../ops/trino_serving" >/dev/null 2>&1 && pwd)"

# Step 1 — write configs (uses the singleton runtime_context cascade).
bash "${REPO_ROOT_ABS}/run_trino.sh" write-configs

# The write-configs step above emits its final paths. Re-run the singleton
# materializer one more time in pure bash-output mode to get TRINO_ETC_DIR
# and TRINO_DATA_DIR and TRINO_RUNTIME as absolute final values. This
# avoids parsing the script's output.
_manifest_file="$(mktemp)"
trap 'rm -f "${_manifest_file}"' EXIT

REPO_ROOT="${REPO_ROOT}" PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
  "${VENV_PY}" - "${_manifest_file}" <<'PY'
import pathlib, sys, os
sys.path.insert(0, next(p for p in os.environ.get("PYTHONPATH","").split(os.pathsep) if p))
from elt_pipeline.config import runtime_context
from elt_pipeline.config.runtime_manifest import runtime_manifest as M

_env_cp = os.environ.get("ELT_PIPELINE_CONFIG_PATH", "").strip()
_config_path_arg = _env_cp or None
if not _config_path_arg:
    _candidate = pathlib.Path("/etc/elt_pipeline/pipeline.yaml")
    if _candidate.is_file():
        _config_path_arg = str(_candidate)
runtime_context.initialize(
    config_path_arg=_config_path_arg,
    environment_arg=os.environ.get("ELT_PIPELINE_ENVIRONMENT", "").strip() or None,
)
def _final(k, d):
    v = runtime_context.get(k)
    return d if v in (None, "") else v

repo_run_dir = _final("repo_run_dir", "")
results_rel = M.paths.repo_run_results_elt_relpath
if repo_run_dir:
    REPO_RUN_ELT = repo_run_dir.rstrip("/") + "/" + results_rel
else:
    home = pathlib.Path.home()
    REPO_RUN_ELT = str(home / M.paths.default_user_repo_run_home / results_rel)

TRINO_RUNTIME = REPO_RUN_ELT + "/" + M.paths.trino_install_relpath
TRINO_ETC_DIR = TRINO_RUNTIME + "/etc"
TRINO_DATA_DIR = TRINO_RUNTIME + "/data"
TRINO_PID_DIR = TRINO_RUNTIME + "/var/run"

lines = [
    f'TRINO_HOME_DIR="{_final("trino_serving.install_dir", "/opt/trino") if False else "/opt/trino"}"',
    f'TRINO_RUNTIME="{TRINO_RUNTIME}"',
    f'TRINO_ETC_DIR="{TRINO_ETC_DIR}"',
    f'TRINO_DATA_DIR="{TRINO_DATA_DIR}"',
    f'TRINO_PID_DIR="{TRINO_PID_DIR}"',
    f'TRINO_PORT="{_final("trino_serving.port", M.serving.default_trino_port)}"',
]
pathlib.Path(sys.argv[1]).write_text("\n".join(lines) + "\n")
PY
set -a
# shellcheck disable=SC1090
source "${_manifest_file}"
set +a

mkdir -p "${TRINO_ETC_DIR}" "${TRINO_DATA_DIR}" "${TRINO_PID_DIR}"

# Step 2 — foreground launcher (Trino launcher's native `run` command).
exec /opt/trino/bin/launcher \
  --verbose \
  --etc-dir="${TRINO_ETC_DIR}" \
  --data-dir="${TRINO_DATA_DIR}" \
  --pid-file="${TRINO_PID_DIR}/launcher.pid" \
  run

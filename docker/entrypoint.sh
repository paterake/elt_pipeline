#!/usr/bin/env bash
#
# Container entrypoint for elt_pipeline.
#
# Responsibilities:
#   1. Ensure required directories exist with correct permissions.
#   2. Seed a user-friendly pipeline.yaml if /etc/elt_pipeline/pipeline.yaml
#      is missing (shouldn't happen for a built image, but safe for derived
#      images that bind-mount /etc/elt_pipeline empty).
#   3. Translate the special subcommand "demo" → run_demo.sh (keeps the
#      docker-compose UI clean: `docker compose run --rm elt_pipeline demo`).
#   4. Otherwise exec whatever the user passed (elt-pipeline, bash, python, …).
#
set -euo pipefail

ELT_HOME="/var/lib/elt_pipeline"
ELT_CONFIG_DIR="/etc/elt_pipeline"
ELT_SHARE="/usr/share/elt_pipeline"

mkdir -p \
  "${ELT_HOME}/results/elt_pipeline" \
  "${ELT_HOME}/results/elt_pipeline/.cache" \
  "${ELT_HOME}/results/elt_pipeline/.artifacts" \
  "/var/cache/elt_pipeline/ivy2" \
  "/var/log/elt_pipeline"

# Seed the reference pipeline.yaml if the config directory is empty (rare).
if [[ ! -f "${ELT_CONFIG_DIR}/pipeline.yaml" ]]; then
  mkdir -p "${ELT_CONFIG_DIR}"
  cp "${ELT_SHARE}/../pipeline.yaml" "${ELT_CONFIG_DIR}/pipeline.yaml" 2>/dev/null || true
fi

first_arg="${1:-}"

# "demo" is a container-only sugar command (not part of the real CLI).
if [[ "${first_arg}" == "demo" ]]; then
  shift || true
  exec bash "${ELT_SHARE}/docker/run_demo.sh" "$@"
fi

# "trino-start" sugar: foreground Trino launcher (reuses the ops script).
if [[ "${first_arg}" == "trino-start" ]]; then
  shift || true
  export REPO_ROOT="${ELT_SHARE}"
  export VENV_PY="/opt/elt_pipeline_venv/bin/python"
  exec bash "${ELT_SHARE}/ops/trino_serving/run_trino.sh" start-foreground "$@"
fi

# Default: run whatever was passed (elt-pipeline …, bash, python -c "…", …).
exec "$@"

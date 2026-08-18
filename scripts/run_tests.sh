#!/usr/bin/env bash
#
# Test gate: per-file process isolation for Spark/Iceberg suites.
#
# Why this exists (the S-0 constraint):
#   The whole test process shares ONE JVM, and one JVM can hold only ONE
#   SparkSession. `build_spark_session().getOrCreate()` returns the first-built
#   session for the life of the process, so:
#     * Iceberg on/off is frozen at session-build time — a file that needs
#       Iceberg off and a file that needs it on cannot coexist in one process.
#     * Some Spark suites call `spark.stop()` in teardown, which tears down the
#       shared JVM SparkContext for every test that runs afterwards
#       (`AttributeError: 'NoneType' object has no attribute 'sc'`).
#   A single `uv run pytest` therefore cross-contaminates Spark files. The fix is
#   process isolation: every Spark-backed test file runs in its OWN process; all
#   the non-Spark files run together in one fast process.
#
# The shared `spark_session` fixture defaults to Iceberg OFF (correct for the
# L2/parity and CLI-parity suites). Files with their own Iceberg-on fixtures
# (e.g. test_sql_iceberg_write) build their own session and ignore this default.
#
# Usage: scripts/run_tests.sh [extra pytest args...]
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# Non-interactive shells do not inherit mise's activation; export the JDK Spark
# needs unless the caller already provided one.
: "${JAVA_HOME:=$HOME/.local/share/mise/installs/java/temurin-23}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

# Shared Spark fixture mode: Iceberg OFF by default (harness knob, not product
# config). Callers may override, but every file is green with the default.
export ELT_PIPELINE_TEST_SPARK_ICEBERG="${ELT_PIPELINE_TEST_SPARK_ICEBERG:-0}"

pytest_args=("$@")

# Detect Spark-backed test files (they reference a Spark session in some form).
spark_files="$(grep -rlE 'SparkSession|build_spark_session|spark_session' tests --include='test_*.py' | sort)"

# Everything else runs together; build --ignore flags for the bulk run.
ignore_flags=()
while IFS= read -r f; do
  [ -n "$f" ] && ignore_flags+=("--ignore=$f")
done <<< "$spark_files"

failures=()

# Guarded expansions: under `set -u`, an empty array errors on older bash (3.2 on
# macOS). `${arr[@]+"${arr[@]}"}` expands to nothing when empty, to the elements
# otherwise.
echo "==> Non-Spark tests (single process)"
if ! uv run pytest -q -p no:cacheprovider \
    ${ignore_flags[@]+"${ignore_flags[@]}"} ${pytest_args[@]+"${pytest_args[@]}"}; then
  failures+=("non-spark")
fi

while IFS= read -r f; do
  [ -z "$f" ] && continue
  echo "==> $f (isolated process)"
  if ! uv run pytest -q -p no:cacheprovider "$f" ${pytest_args[@]+"${pytest_args[@]}"}; then
    failures+=("$f")
  fi
done <<< "$spark_files"

echo
if [ "${#failures[@]}" -eq 0 ]; then
  echo "TEST GATE: PASS (all files green)"
  exit 0
fi
echo "TEST GATE: FAIL (${#failures[@]} file group(s)):"
printf '  - %s\n' "${failures[@]}"
exit 1

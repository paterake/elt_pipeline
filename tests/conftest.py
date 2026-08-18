import os

import pytest

from elt_pipeline.config import runtime_context
from elt_pipeline.spark.session import build_spark_session

# Test-harness-only knob (NOT product config; never read via runtime_context).
# Selects the shared Spark session's Iceberg mode so the L2/parity unit tests can be
# run in their required iceberg-off mode without editing this file. Default "1" keeps
# the production-faithful iceberg-on checkpoint. Until S-0 (per-file process isolation)
# lands, run parity/L2 files with this set to 0, e.g.:
#     ELT_PIPELINE_TEST_SPARK_ICEBERG=0 uv run pytest tests/test_sql_models.py -q
_TEST_SPARK_ICEBERG = os.environ.get("ELT_PIPELINE_TEST_SPARK_ICEBERG", "1").strip().lower()
_SHARED_ICEBERG_ENABLED = _TEST_SPARK_ICEBERG not in {"0", "false", "no", "off"}


@pytest.fixture(autouse=True)
def _reset_runtime_singleton():
    """Isolate the module-level runtime_context singleton across tests.

    The singleton (``runtime_context._SINGLETON``) is a process-global that lazily
    materializes from repo-root ``pipeline.yaml`` on first access. Without a reset,
    state materialized by one test (or a value derived from ``pipeline.yaml``) leaks
    into the next, defeating tests that assert cleared-environment behaviour (e.g. the
    catalog-URI validation tests). Reset before and after every test so each starts
    from an un-materialized singleton.
    """
    runtime_context._reset_for_tests()
    yield
    runtime_context._reset_for_tests()


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    # Iceberg is opt-out (on by default) in production; L3/L4 use the Iceberg table
    # format (L2 stays plain parquet regardless). All tests in a JVM share ONE Spark
    # session, so this shared session matches the production default (Iceberg enabled)
    # and supplies a session-scoped warehouse dir so the HadoopCatalog can initialize.
    warehouse_dir = tmp_path_factory.mktemp("iceberg_warehouse")
    session = build_spark_session(
        app_name="elt-pipeline-tests",
        master="local[1]",
        iceberg_enabled=_SHARED_ICEBERG_ENABLED,
        iceberg_warehouse_dir=str(warehouse_dir),
    )
    yield session
    session.stop()

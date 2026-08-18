import pytest

from elt_pipeline.config import runtime_context
from elt_pipeline.spark.session import build_spark_session


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
        iceberg_enabled=True,
        iceberg_warehouse_dir=str(warehouse_dir),
    )
    yield session
    session.stop()

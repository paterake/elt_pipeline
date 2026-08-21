import os
from typing import Iterator

import pytest

from elt_pipeline.config import runtime_context
from elt_pipeline.spark.session import build_spark_session

# Test-harness-only knob (NOT product config; never read via runtime_context).
# Selects the shared Spark session's Iceberg mode. Default is OFF ("0"): the
# many L2/parity + CLI-parity suites that use this shared fixture need Iceberg
# off, and the Iceberg-on suites build their own iceberg-on session (e.g.
# test_sql_iceberg_write) and ignore this knob. Per-file process isolation is
# provided by scripts/run_tests.sh (the S-0 test gate); within one process the
# fixture can only be one mode, so OFF is the correct shared default. Override to
# "1" only for a deliberate iceberg-on experiment on this shared fixture.
_TEST_SPARK_ICEBERG = os.environ.get("ELT_PIPELINE_TEST_SPARK_ICEBERG", "0").strip().lower()
_SHARED_ICEBERG_ENABLED = _TEST_SPARK_ICEBERG not in {"0", "false", "no", "off"}

_EMULATOR_ENV = os.environ.get("ELT_PIPELINE_TEST_EMULATORS", "0").strip().lower()
_EMULATORS_ENABLED_BY_ENV = _EMULATOR_ENV in {"1", "true", "yes", "on"}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-emulator",
        action="store_true",
        default=False,
        help="Run tests marked @pytest.mark.emulator (cloud emulator tests; may need Docker)",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "emulator: tests that require cloud emulators (Docker/network). Opt-in only.",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    run_emulator = config.getoption("--run-emulator", False) or _EMULATORS_ENABLED_BY_ENV
    if run_emulator:
        return
    skip_emulator = pytest.mark.skip(
        reason="emulator test: pass --run-emulator or set ELT_PIPELINE_TEST_EMULATORS=1 to run"
    )
    for item in items:
        if "emulator" in item.keywords:
            item.add_marker(skip_emulator)


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


@pytest.fixture()
def moto_s3() -> Iterator[None]:
    """Activate moto's S3 mock for the duration of a test.

    Usage:

        @pytest.mark.emulator
        def test_s3_write(moto_s3):
            import boto3
            client = boto3.client("s3", ...)
            ...
    """
    pytest.importorskip("moto")
    from moto import mock_aws

    with mock_aws():
        yield

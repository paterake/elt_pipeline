import pytest

from elt_pipeline.spark.session import build_spark_session


@pytest.fixture(scope="session")
def spark_session():
    session = build_spark_session(app_name="elt-pipeline-tests", master="local[1]")
    yield session
    session.stop()

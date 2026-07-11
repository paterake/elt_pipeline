from __future__ import annotations

import os

from pyspark.sql import SparkSession

_MASTER_ENV_VAR = "ELT_PIPELINE_SPARK_MASTER"
_DEFAULT_MASTER = "local[*]"


def build_spark_session(*, app_name: str, master: str | None = None) -> SparkSession:
    resolved_master = master or os.environ.get(_MASTER_ENV_VAR) or _DEFAULT_MASTER

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession

from elt_pipeline.shared.path_utils import (
    join_paths,
    path_is_dir,
    path_rglob,
)
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.models import SqlModelSourceRef

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_path_fragment(value: str) -> str:
    cleaned = _SAFE_PATH_FRAGMENT.sub("_", value.strip())
    return cleaned or "unknown"


class Level2DatasetLocator:
    def __init__(self, *, root_path: str, spark: SparkSession) -> None:
        self.root_path = root_path
        self.spark = spark

    def read(self, *, source_ref: SqlModelSourceRef, environment: str) -> DataFrame:
        _ = environment
        table_name = source_ref.table_name or source_ref.logical_name
        sanitized_table = _sanitize_path_fragment(table_name)
        entity_root = join_paths(
            self.root_path,
            "level2",
            f"source={_sanitize_path_fragment(source_ref.source_name)}",
            f"entity={_sanitize_path_fragment(source_ref.entity_name)}",
        )
        assert "environment=" not in entity_root, (
            "Generated L2 read path contains 'environment=' segment; per P5 decision, "
            "environment is handled exclusively by which root path is pointed at."
        )
        matches = sorted(
            path
            for path in path_rglob(entity_root, f"**/table={sanitized_table}/run_id=*")
            if path_is_dir(path)
        )
        if not matches:
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.level2_source_not_found,
                message=(
                    f"No level2 data found for source '{source_ref.source_name}', "
                    f"entity '{source_ref.entity_name}', table '{table_name}'"
                ),
                context={
                    "logical_name": source_ref.logical_name,
                    "source_name": source_ref.source_name,
                    "entity_name": source_ref.entity_name,
                    "table_name": table_name,
                    "sanitized_table_name": sanitized_table,
                    "environment": environment,
                    "search_root": entity_root,
                },
            )
        dataframe = (
            self.spark.read.option("mergeSchema", "true")
            .option("basePath", entity_root)
            .parquet(*matches)
        )
        dataframe = dataframe.filter(dataframe["table"] == sanitized_table)
        return dataframe

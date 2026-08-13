from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, StringType, StructField, StructType

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.models import Level2TableManifest, NormalizedTable
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.path_utils import (
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_glob,
    path_parent,
    path_read_bytes,
    path_relative_to,
    strip_file_scheme,
)
from elt_pipeline.shared.runtime import RunContext
from elt_pipeline.spark.errors import SparkRuntimeErrorCode, build_spark_runtime_error

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")

_NO_ENV_IN_PATH_MESSAGE = (
    "Generated path contains 'environment=' segment; per P5 decision, environment is "
    "handled exclusively by which root path is pointed at, never as an in-path segment."
)

_EMPTY_TABLE_SCHEMA = StructType(
    [
        StructField("_row_id", StringType(), nullable=False),
        StructField("_parent_row_id", StringType(), nullable=True),
        StructField("_array_index", LongType(), nullable=True),
    ]
)


def _sanitize_path_fragment(value: str) -> str:
    cleaned = _SAFE_PATH_FRAGMENT.sub("_", value.strip())
    return cleaned or "unknown"


def _write_json_file(path: str, payload: Any) -> None:
    import posixpath

    from elt_pipeline.shared.path_utils import (
        path_mkdir,
        path_parent,
        path_replace,
        path_with_suffix,
        path_write_text,
    )

    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    temp_path = path_with_suffix(path, f"{posixpath.splitext(path)[1]}.tmp")
    path_write_text(
        temp_path,
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
        atomic=True,
    )
    path_replace(temp_path, path)


def _rows_to_dataframe(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    if not rows:
        return spark.createDataFrame([], schema=_EMPTY_TABLE_SCHEMA)
    json_lines = [json.dumps(row, sort_keys=True, default=str) for row in rows]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)


def _summarize_parquet_dir(data_dir: str) -> tuple[int, int]:
    try:
        part_files = sorted(path_glob(join_paths(data_dir, "*.parquet")))
    except PipelineError:
        return (0, 0)
    file_count = len(part_files)
    total_bytes = 0
    for part_file in part_files:
        scheme = detect_scheme(part_file)
        if scheme in (_StorageScheme.file, _StorageScheme.local_unschemed):
            local_path = strip_file_scheme(part_file)
            try:
                total_bytes += os.stat(local_path).st_size
            except OSError:
                continue
        else:
            try:
                total_bytes += len(path_read_bytes(part_file))
            except PipelineError:
                continue
    return (file_count, total_bytes)


def _basename_named_manifest(data_dir: str) -> str:
    import posixpath

    name = posixpath.basename(data_dir.rstrip("/"))
    return join_paths(path_parent(data_dir), f"{name}.manifest.json")


class LocalLevel2Layout:
    def __init__(self, root_path: str) -> None:
        self.root_path = root_path

    def table_run_dir(
        self,
        *,
        environment: str,
        source_name: str,
        entity_name: str,
        mapping_version: str,
        partition: dict[str, str],
        table_name: str,
        run_id: str,
    ) -> str:
        _ = environment
        segments = [
            self.root_path,
            "level2",
            f"source={_sanitize_path_fragment(source_name)}",
            f"entity={_sanitize_path_fragment(entity_name)}",
            f"mapping_version={_sanitize_path_fragment(mapping_version)}",
        ]
        for key in sorted(partition):
            segments.append(
                f"{_sanitize_path_fragment(key)}={_sanitize_path_fragment(partition[key])}"
            )
        segments.append(f"table={_sanitize_path_fragment(table_name)}")
        segments.append(f"run_id={_sanitize_path_fragment(run_id)}")
        result = join_paths(*segments)
        assert "environment=" not in result, _NO_ENV_IN_PATH_MESSAGE
        return result


class SparkLevel2Writer:
    def __init__(self, root_path: str, spark: SparkSession) -> None:
        self.layout = LocalLevel2Layout(root_path)
        self.spark = spark

    def write_table(
        self,
        *,
        run_context: RunContext,
        manifest: Level1ArtifactManifest,
        mapping_version: str,
        table: NormalizedTable,
        partition: dict[str, str],
        normalize_completed_at: datetime | None = None,
    ) -> Level2TableManifest:
        completed_at = normalize_completed_at or datetime.now(tz=UTC)
        data_dir = self.layout.table_run_dir(
            environment=manifest.environment,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            mapping_version=mapping_version,
            partition=partition,
            table_name=table.physical_name,
            run_id=run_context.run_id,
        )
        dataframe = _rows_to_dataframe(self.spark, table.rows)
        if "source_name" not in dataframe.columns:
            dataframe = dataframe.withColumn("source_name", lit(manifest.source_name))
        if "ingest_date" not in dataframe.columns:
            dataframe = dataframe.withColumn(
                "ingest_date", lit(manifest.ingest_started_at.date().isoformat())
            )
        if "_run_id" not in dataframe.columns:
            dataframe = dataframe.withColumn("_run_id", lit(run_context.run_id))
        try:
            dataframe.write.mode("error").parquet(data_dir)
        except Exception as exc:
            raise build_spark_runtime_error(
                code=SparkRuntimeErrorCode.write_failed,
                message=f"Failed to write level2 parquet dataset: {data_dir}",
                context={"path": data_dir},
            ) from exc

        file_count, total_file_size_bytes = _summarize_parquet_dir(data_dir)

        relative_data_path = path_relative_to(data_dir, self.layout.root_path)
        manifest_path = _basename_named_manifest(data_dir)
        relative_manifest_path = path_relative_to(manifest_path, self.layout.root_path)
        artifact_id = hashlib.sha256(
            f"{run_context.run_id}:{relative_data_path}".encode("utf-8")
        ).hexdigest()[:24]
        manifest_payload = Level2TableManifest(
            artifact_id=artifact_id,
            run_id=run_context.run_id,
            job_name=run_context.job_name,
            trigger_type=run_context.trigger_type,
            environment=manifest.environment,
            source_name=manifest.source_name,
            entity_name=manifest.entity_name,
            mapping_version=mapping_version,
            input_artifact_id=manifest.artifact_id,
            input_data_path=manifest.data_path,
            input_manifest_path=manifest.manifest_path,
            table_name=table.physical_name,
            partition=partition,
            normalize_started_at=run_context.started_at,
            normalize_completed_at=completed_at,
            record_count=len(table.rows),
            file_count=file_count,
            total_file_size_bytes=total_file_size_bytes,
            data_path=relative_data_path,
            manifest_path=relative_manifest_path,
        )
        _write_json_file(manifest_path, manifest_payload.model_dump(mode="json"))
        return manifest_payload

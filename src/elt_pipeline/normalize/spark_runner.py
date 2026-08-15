from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, expr, lit, posexplode_outer

from elt_pipeline.normalize.planner import NormalizationPlan, PlannedTable


class SparkRelationalizer:
    def execute(
        self,
        *,
        raw_df: DataFrame,
        plan: NormalizationPlan,
    ) -> dict[str, DataFrame]:
        result: dict[str, DataFrame] = {}
        tables_by_physical: dict[str, PlannedTable] = {
            t.physical_table_name: t for t in plan.tables
        }
        for planned in plan.tables:
            if planned.parent_table_name is None:
                df = self._build_root_df(raw_df, planned)
            else:
                parent_planned = tables_by_physical[planned.parent_table_name]
                parent_df = result[parent_planned.physical_table_name]
                explosion = next(
                    e
                    for e in parent_planned.child_arrays
                    if e.child_table_logical_path == planned.logical_path
                )
                df = self._build_child_df(
                    parent_df=parent_df,
                    child_planned=planned,
                    array_accessor=explosion.array_accessor,
                )
            result[planned.physical_table_name] = df
        return result

    def _build_root_df(self, raw_df: DataFrame, planned: PlannedTable) -> DataFrame:
        projections: list[Column] = []
        for physical_name, field_accessor in planned.scalar_accessors:
            projections.append(col(field_accessor).alias(physical_name))
        for explosion in planned.child_arrays:
            projections.append(col(explosion.array_accessor))
        if not projections:
            projections.append(lit(None).cast("string").alias("_empty_marker"))
        df = raw_df.select(*projections)
        if "_empty_marker" in df.columns:
            df = df.drop("_empty_marker")
        df = df.withColumn("_row_id", expr("uuid()"))
        return df

    def _build_child_df(
        self,
        *,
        parent_df: DataFrame,
        child_planned: PlannedTable,
        array_accessor: str,
    ) -> DataFrame:
        exploded = parent_df.select(
            col("_row_id").alias("_parent_row_id"),
            posexplode_outer(col(array_accessor)).alias("_array_index", "item"),
        )
        projections: list[Column] = [
            col("_parent_row_id"),
            col("_array_index"),
        ]
        for physical_name, field_accessor in child_planned.scalar_accessors:
            if field_accessor == "value":
                projections.append(col("item").alias(physical_name))
            else:
                projections.append(col(f"item.{field_accessor}").alias(physical_name))
        for explosion in child_planned.child_arrays:
            projections.append(col(f"item.{explosion.array_accessor}").alias(explosion.array_accessor))
        df = exploded.select(*projections)
        df = df.where(col("_array_index").isNotNull())
        df = df.withColumn("_row_id", expr("uuid()"))
        return df

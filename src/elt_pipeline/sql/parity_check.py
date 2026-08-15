from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    collect_list,
    lit,
    sort_array,
    struct,
)
from pyspark.sql.functions import (
    md5 as spark_md5,
)

from elt_pipeline.sql.models import CompiledSqlModel, SqlModelStage
from elt_pipeline.sql.spark_executor import (
    _iceberg_table_fq,
    _is_iceberg_enabled,
)


@dataclass(frozen=True)
class ModelParity:
    model_id: str
    stage: str
    domain: str
    name: str
    row_count: int
    md5_of_sorted_row_hashes: str
    columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _sorted_row_md5(df: DataFrame) -> tuple[int, str, list[str]]:
    columns = sorted(df.columns)
    if not columns:
        return 0, hashlib.md5(b"").hexdigest(), []
    row_struct = struct(*[col(c).cast("string").alias(c) for c in columns])
    per_row_md5 = df.select(spark_md5(row_struct).alias("rh"))
    agg = per_row_md5.select(
        sort_array(collect_list(col("rh"))).alias("sorted_rh"),
        collect_list(lit(1)).alias("__n"),
    )
    single_row = agg.first()
    if single_row is None or single_row["sorted_rh"] is None:
        return 0, hashlib.md5(b"").hexdigest(), columns
    sorted_rh: list[str] = list(single_row["sorted_rh"])
    n_rows = len(sorted_rh)
    if not sorted_rh:
        return 0, hashlib.md5(b"").hexdigest(), columns
    joined = "|".join(sorted_rh)
    combined = hashlib.md5(joined.encode("utf-8")).hexdigest()
    return n_rows, combined, columns


def _warehouse_path_for_stage(
    *, warehouse_root: str, stage: SqlModelStage, table_name: str
) -> Path:
    return Path(warehouse_root) / stage.value / table_name


def measure_model_parity(
    *,
    spark: SparkSession,
    models: list[CompiledSqlModel],
    warehouse_root: str,
) -> list[ModelParity]:
    use_iceberg = _is_iceberg_enabled(spark)
    results: list[ModelParity] = []
    for m in models:
        if use_iceberg:
            fq = _iceberg_table_fq(
                stage=m.stage, domain=m.domain, name=m.target_table_name
            )
            reader: DataFrame = spark.table(fq)
        else:
            path = _warehouse_path_for_stage(
                warehouse_root=warehouse_root,
                stage=m.stage,
                table_name=m.target_table_name,
            )
            reader = spark.read.parquet(str(path))
        n_rows, md5_digest, cols = _sorted_row_md5(reader)
        results.append(
            ModelParity(
                model_id=m.model_id,
                stage=m.stage.value,
                domain=m.domain,
                name=m.target_table_name,
                row_count=n_rows,
                md5_of_sorted_row_hashes=md5_digest,
                columns=cols,
            )
        )
    return results


def compare_parity_reports(
    left: list[ModelParity], right: list[ModelParity]
) -> dict[str, Any]:
    lmap = {p.model_id: p for p in left}
    rmap = {p.model_id: p for p in right}
    all_ids = sorted(set(lmap) | set(rmap))
    diffs: list[dict[str, Any]] = []
    matches: list[dict[str, Any]] = []
    missing_left: list[str] = []
    missing_right: list[str] = []
    for mid in all_ids:
        lp = lmap.get(mid)
        rp = rmap.get(mid)
        if lp is None:
            missing_left.append(mid)
            continue
        if rp is None:
            missing_right.append(mid)
            continue
        ok = (
            lp.row_count == rp.row_count
            and lp.md5_of_sorted_row_hashes == rp.md5_of_sorted_row_hashes
            and sorted(lp.columns) == sorted(rp.columns)
        )
        entry = {
            "model_id": mid,
            "left": lp.to_dict(),
            "right": rp.to_dict(),
            "row_count_match": lp.row_count == rp.row_count,
            "md5_match": lp.md5_of_sorted_row_hashes == rp.md5_of_sorted_row_hashes,
            "columns_match": sorted(lp.columns) == sorted(rp.columns),
        }
        if ok:
            matches.append(entry)
        else:
            diffs.append(entry)
    return {
        "total_models": len(all_ids),
        "match_count": len(matches),
        "mismatch_count": len(diffs),
        "missing_left": missing_left,
        "missing_right": missing_right,
        "matches": matches,
        "mismatches": diffs,
        "parity": len(diffs) == 0 and not missing_left and not missing_right,
    }


def write_parity_report(path: str | Path, entries: list[ModelParity]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"models": [e.to_dict() for e in entries]}
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_parity_report(path: str | Path) -> list[ModelParity]:
    raw = json.loads(Path(path).read_text())
    entries: list[ModelParity] = []
    for item in raw.get("models", []):
        entries.append(
            ModelParity(
                model_id=str(item["model_id"]),
                stage=str(item["stage"]),
                domain=str(item["domain"]),
                name=str(item["name"]),
                row_count=int(item["row_count"]),
                md5_of_sorted_row_hashes=str(item["md5_of_sorted_row_hashes"]),
                columns=list(item["columns"]),
            )
        )
    return entries

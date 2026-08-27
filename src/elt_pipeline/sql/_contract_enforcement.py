from __future__ import annotations

from typing import TYPE_CHECKING, Any

from elt_pipeline.shared.governance import SqlColumnSpec
from elt_pipeline.sql.errors import SqlRuntimeErrorCode, build_sql_runtime_error
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    DataContractBrokenChange,
    DataContractDiff,
    DataContractWarningRecord,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DataType, StructField, StructType


def _normalise_type_string(raw: str) -> str:
    return raw.strip().upper().replace(" ", "")


def _normalise_spark_decimal(dt: Any) -> str:
    precision = getattr(dt, "precision", 10)
    scale = getattr(dt, "scale", 0)
    return f"DECIMAL({precision},{scale})"


def normalise_spark_data_type(dt: "DataType") -> str:
    type_name = type(dt).__name__.lower()
    if type_name == "stringtype":
        return "STRING"
    if type_name == "integertype":
        return "INT"
    if type_name == "longtype":
        return "BIGINT"
    if type_name == "shorttype":
        return "SMALLINT"
    if type_name == "bytetype":
        return "TINYINT"
    if type_name == "doubletype":
        return "DOUBLE"
    if type_name == "floattype":
        return "FLOAT"
    if type_name == "booleantype":
        return "BOOLEAN"
    if type_name == "datetype":
        return "DATE"
    if type_name == "timestamptype":
        return "TIMESTAMP"
    if type_name == "timestampntztype":
        return "TIMESTAMP_NTZ"
    if type_name == "binarytype":
        return "BINARY"
    if type_name == "decimaltype":
        return _normalise_spark_decimal(dt)
    if type_name.startswith("arraytype"):
        element_type = normalise_spark_data_type(dt.elementType)
        return f"ARRAY<{element_type}>"
    if type_name.startswith("maptype"):
        key_type = normalise_spark_data_type(dt.keyType)
        val_type = normalise_spark_data_type(dt.valueType)
        return f"MAP<{key_type},{val_type}>"
    if type_name.startswith("structtype"):
        fields = []
        for f in dt.fields:
            nt = normalise_spark_data_type(f.dataType)
            fields.append(f"{f.name}:{nt}")
        return "STRUCT<" + ",".join(fields) + ">"
    return _normalise_type_string(type_name.replace("type", ""))


def extract_schema_fields(
    schema: "StructType",
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    field: "StructField"
    for field in schema.fields:
        out[field.name] = {
            "type": normalise_spark_data_type(field.dataType),
            "nullable": bool(field.nullable),
        }
    return out


def compute_contract_diff(
    declared_columns: list[SqlColumnSpec],
    actual_columns: dict[str, dict[str, Any]],
) -> DataContractDiff:
    declared_by_name: dict[str, SqlColumnSpec] = {}
    for spec in declared_columns:
        declared_by_name[spec.name] = spec

    declared_names = set(declared_by_name.keys())
    actual_names = set(actual_columns.keys())

    added = sorted(actual_names - declared_names)
    removed = sorted(declared_names - actual_names)
    changed: list[DataContractBrokenChange] = []

    for col_name in sorted(declared_names & actual_names):
        spec = declared_by_name[col_name]
        actual = actual_columns[col_name]
        delta = DataContractBrokenChange(column=col_name)
        modified = False
        if spec.type is not None:
            expected_t = _normalise_type_string(spec.type)
            actual_t = actual["type"]
            if expected_t != actual_t:
                delta.expected_type = expected_t
                delta.actual_type = actual_t
                modified = True
        if spec.nullable is not None and bool(spec.nullable) != bool(actual["nullable"]):
            delta.expected_nullable = bool(spec.nullable)
            delta.actual_nullable = bool(actual["nullable"])
            modified = True
        if modified:
            changed.append(delta)

    return DataContractDiff(
        added_columns=added,
        removed_columns=removed,
        changed_columns=changed,
    )


def try_read_existing_iceberg_table_schema(
    spark: "SparkSession",
    fq_table: str,
) -> dict[str, dict[str, Any]] | None:
    try:
        table_df = spark.table(fq_table)
    except Exception:
        return None
    return extract_schema_fields(table_df.schema)


def check_contract_against_dataframe_schema(
    *,
    model: CompiledSqlModel,
    dataframe_schema: "StructType",
) -> DataContractDiff:
    actual = extract_schema_fields(dataframe_schema)
    return compute_contract_diff(
        declared_columns=model.governance.columns,
        actual_columns=actual,
    )


def check_contract_against_catalog_schema(
    *,
    model: CompiledSqlModel,
    catalog_columns: dict[str, dict[str, Any]],
) -> DataContractDiff:
    return compute_contract_diff(
        declared_columns=model.governance.columns,
        actual_columns=catalog_columns,
    )


def enforce_data_contract_at_write(
    *,
    model: CompiledSqlModel,
    dataframe_schema: "StructType | None" = None,
    dataframe_diff: DataContractDiff | None = None,
    catalog_columns: dict[str, dict[str, Any]] | None = None,
    execution_result_contract_warnings: list[DataContractWarningRecord],
) -> None:
    if model.contract == "off":
        return

    if dataframe_diff is None:
        if dataframe_schema is None:
            raise build_sql_runtime_error(
                code=SqlRuntimeErrorCode.contract_broken,
                message=(
                    f"Data contract enforcement for model '{model.model_id}' requires "
                    "either dataframe_schema or precomputed dataframe_diff"
                ),
                context={"model_id": model.model_id},
            )
        dataframe_diff = check_contract_against_dataframe_schema(
            model=model,
            dataframe_schema=dataframe_schema,
        )

    if not dataframe_diff.is_empty():
        _apply_contract_decision(
            model=model,
            target="dataframe_schema",
            diff=dataframe_diff,
            execution_result_contract_warnings=execution_result_contract_warnings,
        )

    if catalog_columns is not None:
        catalog_diff = check_contract_against_catalog_schema(
            model=model,
            catalog_columns=catalog_columns,
        )
        if not catalog_diff.is_empty():
            _apply_contract_decision(
                model=model,
                target="catalog_schema",
                diff=catalog_diff,
                execution_result_contract_warnings=execution_result_contract_warnings,
            )


def _apply_contract_decision(
    *,
    model: CompiledSqlModel,
    target: str,
    diff: DataContractDiff,
    execution_result_contract_warnings: list[DataContractWarningRecord],
) -> None:
    diff_dict = {
        "added_columns": diff.added_columns,
        "removed_columns": diff.removed_columns,
        "changed_columns": [c.model_dump() for c in diff.changed_columns],
    }
    context: dict[str, Any] = {
        "model_id": model.model_id,
        "contract_mode": model.contract,
        "comparison_target": target,
        "diff": diff_dict,
    }
    if model.contract_version is not None:
        context["contract_version"] = model.contract_version

    if model.contract == "strict":
        summary_parts: list[str] = []
        if diff.added_columns:
            summary_parts.append(
                f"added columns [{', '.join(diff.added_columns)}]"
            )
        if diff.removed_columns:
            summary_parts.append(
                f"removed columns [{', '.join(diff.removed_columns)}]"
            )
        if diff.changed_columns:
            change_descs = []
            for c in diff.changed_columns:
                bits = [f"'{c.column}'"]
                if c.expected_type is not None:
                    bits.append(
                        f"type {c.expected_type!r} != {c.actual_type!r}"
                    )
                if c.expected_nullable is not None:
                    bits.append(
                        f"nullable {c.expected_nullable} != {c.actual_nullable}"
                    )
                change_descs.append(" ".join(bits))
            summary_parts.append("changed columns: " + "; ".join(change_descs))
        message = (
            f"Data contract broken for model '{model.model_id}' (mode=strict) "
            f"against {target}: " + ", ".join(summary_parts)
            + ". Write blocked before commit."
        )
        raise build_sql_runtime_error(
            code=SqlRuntimeErrorCode.contract_broken,
            message=message,
            retryable=False,
            context=context,
        )

    if model.contract == "warn":
        comparison_target_value: Any = target
        execution_result_contract_warnings.append(
            DataContractWarningRecord(
                model_id=model.model_id,
                mode="warn",
                comparison_target=comparison_target_value,  # type: ignore[arg-type]
                diff=diff,
            )
        )

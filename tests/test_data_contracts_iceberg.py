from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.governance import (
    SqlColumnSpec,
    SqlModelGovernance,
)
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql import SparkSqlModelExecutor
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    DataContractMode,
    SqlLoadMode,
    SqlModelStage,
    SqlQualityExpectations,
)
from elt_pipeline.sql.spark_executor import _iceberg_table_fq


@pytest.fixture(scope="module")
def iceberg_spark_contract(tmp_path_factory):  # type: ignore[no-untyped-def]
    module_root = tmp_path_factory.mktemp("contract-ivy")
    ivy_home = str(module_root / "ivy2")
    os.environ["ELT_PIPELINE_IVY_HOME"] = ivy_home
    test_warehouse_root = tmp_path_factory.mktemp("contract-iceberg-wh")
    warehouse_dir = str(test_warehouse_root / "iceberg_warehouse")
    spark = build_spark_session(
        app_name="elt_pipeline_contract_iceberg_test",
        iceberg_enabled=True,
        iceberg_catalog_type="hadoop",
        iceberg_warehouse_dir=warehouse_dir,
    )
    yield spark
    spark.stop()


def _contract_model(
    *,
    name: str = "contract_test",
    compiled_sql: str,
    contract: DataContractMode,
    governance: SqlModelGovernance,
    contract_version: str | None = None,
    partition_columns: list[str] | None = None,
) -> CompiledSqlModel:
    return CompiledSqlModel(
        model_id=f"level3.contract.{name}",
        stage=SqlModelStage.level3,
        domain="contract",
        name=name,
        target_table_name=name,
        load_mode=SqlLoadMode.full_refresh,
        materialization="table",  # type: ignore[arg-type]
        manifest_path=Path("/tmp/_fake_manifest.yaml"),
        sql_path=Path("/tmp/_fake.sql"),
        compiled_sql=compiled_sql,
        partition_columns=partition_columns or [],
        quality=SqlQualityExpectations(row_count_min=1),
        governance=governance,
        contract=contract,
        contract_version=contract_version,
    )


def test_strict_mode_iceberg_catalog_mismatch_raises(
    iceberg_spark_contract: SparkSession,
    tmp_path: Path,
) -> None:
    """Checklist 6 (Iceberg): existing Iceberg table schema differs from declared spec."""
    warehouse_root = str(tmp_path / "wh_ic_catalog")

    # ── Run 1: off mode → write Iceberg table with schema {id INT, legacy_col STRING} ──
    ex1 = SparkSqlModelExecutor(
        spark=iceberg_spark_contract,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-ic1",
    )
    gov1 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql1 = "SELECT CAST(1 AS INT) AS id, CAST('old' AS STRING) AS legacy_col"
    model1 = _contract_model(
        name="icecat_test",
        compiled_sql=sql1,
        contract="off",
        governance=gov1,
    )
    ex1.execute([model1])

    # Confirm Iceberg catalog table now exists with legacy_col
    fq = _iceberg_table_fq(
        stage=SqlModelStage.level3, domain="contract", name="icecat_test",
        spark=iceberg_spark_contract,
    )
    existing_cols = {f.name for f in iceberg_spark_contract.table(fq).schema.fields}
    assert "legacy_col" in existing_cols

    # ── Run 2: strict mode; DF matches declared (id INT only) but Iceberg catalog
    # still has legacy_col → should raise CONTRACT_BROKEN with comparison_target=catalog_schema
    ex2 = SparkSqlModelExecutor(
        spark=iceberg_spark_contract,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-ic2",
    )
    gov2 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql2 = "SELECT CAST(9 AS INT) AS id"
    model2 = _contract_model(
        name="icecat_test",
        compiled_sql=sql2,
        contract="strict",
        governance=gov2,
        contract_version="cv-ic-2",
    )
    with pytest.raises(PipelineError) as excinfo:
        ex2.execute([model2])
    exc = excinfo.value
    assert exc.error_code == "SQL_CONTRACT_BROKEN"
    assert exc.context["comparison_target"] == "catalog_schema"
    assert exc.context["contract_version"] == "cv-ic-2"
    diff = exc.context["diff"]
    assert "legacy_col" in diff["added_columns"]

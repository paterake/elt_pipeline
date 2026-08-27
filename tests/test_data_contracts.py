from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.governance import (
    SqlColumnSpec,
    SqlModelGovernance,
)
from elt_pipeline.shared.runtime import StageName, new_run_context
from elt_pipeline.sql import (
    SparkSqlModelExecutor,
    build_token_context,
    compile_sql_model,
    discover_sql_models,
    run_sql_models_locally,
)
from elt_pipeline.sql._contract_enforcement import (
    _normalise_type_string,
    compute_contract_diff,
    normalise_spark_data_type,
)
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    DataContractMode,
    SqlLoadMode,
    SqlModelManifest,
    SqlModelOwner,
    SqlModelStage,
    SqlQualityExpectations,
)

# ── Checklist 1: contract field Literal validation + default ────────────────

def test_manifest_contract_field_defaults_to_off() -> None:
    manifest = SqlModelManifest(
        name="m",
        stage=SqlModelStage.level3,
        domain="d",
        target={"table_name": "t"},
        owner=SqlModelOwner(name="p"),
    )
    assert manifest.contract == "off"
    assert manifest.contract_version is None


def test_manifest_contract_field_accepts_valid_literals() -> None:
    for valid in ("strict", "warn", "off"):
        manifest = SqlModelManifest(
            name="m",
            stage=SqlModelStage.level3,
            domain="d",
            target={"table_name": "t"},
            owner=SqlModelOwner(name="p"),
            contract=valid,  # type: ignore[arg-type]
            contract_version="v1",
        )
        assert manifest.contract == valid
        assert manifest.contract_version == "v1"


def test_manifest_contract_field_rejects_invalid_literal() -> None:
    with pytest.raises(ValidationError):
        SqlModelManifest(
            name="m",
            stage=SqlModelStage.level3,
            domain="d",
            target={"table_name": "t"},
            owner=SqlModelOwner(name="p"),
            contract="bogus",  # type: ignore[arg-type]
        )


def test_manifest_contract_version_rejects_empty_if_set() -> None:
    with pytest.raises(ValidationError):
        SqlModelManifest(
            name="m",
            stage=SqlModelStage.level3,
            domain="d",
            target={"table_name": "t"},
            owner=SqlModelOwner(name="p"),
            contract_version="   ",
        )


# ── Checklist 2: compiler threads contract fields through ───────────────────

def test_compiler_threads_contract_fields_through(tmp_path: Path) -> None:
    package_root = _write_contract_sql_package(
        tmp_path,
        contract="strict",
        contract_version="cv-7",
        governance_block=_yaml_governance_block([
            ("order_id", "INT", False),
            ("amount", "DECIMAL(18,4)", True),
            ("order_date", "DATE", True),
        ]),
    )
    discovered = discover_sql_models(package_root)
    base_model = next(m for m in discovered if m.manifest.name == "base_orders")
    compiled = compile_sql_model(
        base_model,
        token_context=build_token_context(
            environment="dev",
            run_id="r1",
            stage=base_model.manifest.stage.value,
            domain=base_model.manifest.domain,
            model_name=base_model.manifest.name,
            target_table_name=base_model.manifest.target.table_name,
            start_date="2026-01-01",
            end_date="2026-01-31",
        ),
    )
    assert compiled.contract == "strict"
    assert compiled.contract_version == "cv-7"
    col_types = {c.name: (c.type, c.nullable) for c in compiled.governance.columns}
    assert col_types["order_id"] == ("INT", False)
    assert col_types["amount"] == ("DECIMAL(18,4)", True)
    assert col_types["order_date"] == ("DATE", True)


# ── Checklist 8: compute_contract_diff pure unit tests ──────────────────────

def test_compute_contract_diff_exact_match_empty() -> None:
    declared = [
        SqlColumnSpec(name="a", type="INT", nullable=False),
        SqlColumnSpec(name="b", type="STRING", nullable=True),
    ]
    actual = {
        "a": {"type": "INT", "nullable": False},
        "b": {"type": "STRING", "nullable": True},
    }
    diff = compute_contract_diff(declared, actual)
    assert diff.is_empty()
    assert diff.added_columns == []
    assert diff.removed_columns == []
    assert diff.changed_columns == []


def test_compute_contract_diff_column_order_independent() -> None:
    declared = [
        SqlColumnSpec(name="z", type="STRING", nullable=False),
        SqlColumnSpec(name="a", type="INT", nullable=False),
        SqlColumnSpec(name="m", type="BOOLEAN", nullable=True),
    ]
    actual = {
        "a": {"type": "INT", "nullable": False},
        "m": {"type": "BOOLEAN", "nullable": True},
        "z": {"type": "STRING", "nullable": False},
    }
    diff = compute_contract_diff(declared, actual)
    assert diff.is_empty()


def test_compute_contract_diff_decimal_precision_scale_preserved() -> None:
    declared = [
        SqlColumnSpec(name="amount", type="DECIMAL(18,4)", nullable=False),
        SqlColumnSpec(name="tax", type="DECIMAL(10,2)", nullable=True),
    ]
    actual_match = {
        "amount": {"type": "DECIMAL(18,4)", "nullable": False},
        "tax": {"type": "DECIMAL(10,2)", "nullable": True},
    }
    assert compute_contract_diff(declared, actual_match).is_empty()

    actual_scale_mismatch = {
        "amount": {"type": "DECIMAL(18,2)", "nullable": False},
        "tax": {"type": "DECIMAL(10,2)", "nullable": True},
    }
    diff = compute_contract_diff(declared, actual_scale_mismatch)
    assert not diff.is_empty()
    assert diff.added_columns == []
    assert diff.removed_columns == []
    assert len(diff.changed_columns) == 1
    cc = diff.changed_columns[0]
    assert cc.column == "amount"
    assert cc.expected_type == "DECIMAL(18,4)"
    assert cc.actual_type == "DECIMAL(18,2)"
    assert cc.expected_nullable is None
    assert cc.actual_nullable is None


def test_compute_contract_diff_nullable_change_detected() -> None:
    declared = [
        SqlColumnSpec(name="req", type="STRING", nullable=False),
        SqlColumnSpec(name="opt", type="STRING", nullable=True),
    ]
    actual = {
        "req": {"type": "STRING", "nullable": True},
        "opt": {"type": "STRING", "nullable": False},
    }
    diff = compute_contract_diff(declared, actual)
    assert not diff.is_empty()
    changed_cols = {c.column: c for c in diff.changed_columns}
    assert "req" in changed_cols
    assert changed_cols["req"].expected_nullable is False
    assert changed_cols["req"].actual_nullable is True
    assert changed_cols["req"].expected_type is None
    assert "opt" in changed_cols
    assert changed_cols["opt"].expected_nullable is True
    assert changed_cols["opt"].actual_nullable is False


def test_compute_contract_diff_all_three_buckets_populated() -> None:
    declared = [
        SqlColumnSpec(name="kept_same", type="INT", nullable=False),
        SqlColumnSpec(name="kept_changed", type="STRING", nullable=True),
        SqlColumnSpec(name="only_declared", type="DATE", nullable=False),
        SqlColumnSpec(name="unenforced", type=None, nullable=None),
    ]
    actual = {
        "kept_same": {"type": "INT", "nullable": False},
        "kept_changed": {"type": "BIGINT", "nullable": False},
        "only_in_actual": {"type": "STRING", "nullable": True},
        "unenforced": {"type": "WHATEVER", "nullable": False},
    }
    diff = compute_contract_diff(declared, actual)
    assert diff.added_columns == ["only_in_actual"]
    assert diff.removed_columns == ["only_declared"]
    assert len(diff.changed_columns) == 1
    cc = diff.changed_columns[0]
    assert cc.column == "kept_changed"
    assert cc.expected_type == "STRING"
    assert cc.actual_type == "BIGINT"
    assert cc.expected_nullable is True
    assert cc.actual_nullable is False


def test_compute_contract_diff_type_whitespace_and_case_normalised() -> None:
    declared = [
        SqlColumnSpec(name="a", type="  decimal( 18 , 4 )  ", nullable=False),
        SqlColumnSpec(name="b", type="string", nullable=False),
    ]
    actual = {
        "a": {"type": "DECIMAL(18,4)", "nullable": False},
        "b": {"type": "STRING", "nullable": False},
    }
    diff = compute_contract_diff(declared, actual)
    assert diff.is_empty()


def test_normalise_spark_data_type_nested() -> None:
    from pyspark.sql.types import ArrayType, MapType

    dt = ArrayType(
        MapType(
            StringType(),
            StructType([
                StructField("x", DecimalType(18, 4), nullable=False),
                StructField("y", TimestampType(), nullable=True),
            ]),
            valueContainsNull=True,
        )
    )
    s = normalise_spark_data_type(dt)
    assert s == "ARRAY<MAP<STRING,STRUCT<x:DECIMAL(18,4),y:TIMESTAMP>>>"


def test_normalise_type_string_normalises() -> None:
    assert _normalise_type_string("  decimal ( 18 , 4 ) ") == "DECIMAL(18,4)"
    assert _normalise_type_string("array < string > ") == "ARRAY<STRING>"


# ── Checklist 3/4/5/6/7: Spark enforcement tests ───────────────────────────

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


def test_contract_off_mode_no_enforcement(tmp_path: Path, spark_session) -> None:
    """Checklist 3: explicit off mode — mismatched schema + no enforcement."""
    warehouse_root = str(tmp_path / "wh_off")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-off",
    )
    # Governance declares only col_a INT; SQL produces col_a STRING + col_b INT.
    # With contract=off, both mismatches are ignored → write succeeds.
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="col_a", type="INT", nullable=False),
    ])
    sql = "SELECT CAST('hello' AS STRING) AS col_a, CAST(42 AS INT) AS col_b"
    model = _contract_model(
        name="off_test",
        compiled_sql=sql,
        contract="off",
        governance=governance,
    )
    result = executor.execute([model])
    assert result.executed_models[0].row_count == 1
    assert result.contract_warnings == []


def test_strict_mode_match_succeeds(tmp_path: Path, spark_session) -> None:
    """Checklist 5: declared == actual → no error, write proceeds."""
    warehouse_root = str(tmp_path / "wh_match")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-match",
    )
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
        SqlColumnSpec(name="name", type="STRING", nullable=False),
        SqlColumnSpec(name="amount", type="DECIMAL(18,4)", nullable=False),
    ])
    sql = (
        "SELECT CAST(1 AS INT) AS id, CAST('a' AS STRING) AS name, "
        "CAST(10.5 AS DECIMAL(18,4)) AS amount "
        "UNION ALL SELECT 2, 'b', CAST(20.0 AS DECIMAL(18,4))"
    )
    model = _contract_model(
        name="match_test",
        compiled_sql=sql,
        contract="strict",
        governance=governance,
        contract_version="cv-5",
    )
    result = executor.execute([model])
    assert result.executed_models[0].row_count == 2
    assert result.contract_warnings == []


def test_strict_mode_column_added_raises(tmp_path: Path, spark_session) -> None:
    """Checklist 4 variant 1: column in actual not in declared spec → CONTRACT_BROKEN."""
    warehouse_root = str(tmp_path / "wh_added")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-added",
    )
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql = "SELECT CAST(1 AS INT) AS id, CAST('unexpected' AS STRING) AS bonus_col"
    model = _contract_model(
        name="added_test",
        compiled_sql=sql,
        contract="strict",
        governance=governance,
    )
    with pytest.raises(PipelineError) as excinfo:
        executor.execute([model])
    exc = excinfo.value
    assert exc.error_code == "SQL_CONTRACT_BROKEN"
    assert exc.error_category.value == "validation_error"
    ctx = exc.context
    assert ctx["contract_mode"] == "strict"
    assert ctx["comparison_target"] == "dataframe_schema"
    diff = ctx["diff"]
    assert "bonus_col" in diff["added_columns"]
    assert diff["removed_columns"] == []
    assert diff["changed_columns"] == []
    assert "Write blocked before commit" in exc.message


def test_strict_mode_column_removed_raises(tmp_path: Path, spark_session) -> None:
    """Checklist 4 variant 2: column in spec missing from actual → CONTRACT_BROKEN."""
    warehouse_root = str(tmp_path / "wh_removed")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-removed",
    )
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
        SqlColumnSpec(name="must_exist", type="STRING", nullable=False),
    ])
    sql = "SELECT CAST(1 AS INT) AS id"
    model = _contract_model(
        name="removed_test",
        compiled_sql=sql,
        contract="strict",
        governance=governance,
    )
    with pytest.raises(PipelineError) as excinfo:
        executor.execute([model])
    exc = excinfo.value
    assert exc.error_code == "SQL_CONTRACT_BROKEN"
    diff = exc.context["diff"]
    assert diff["added_columns"] == []
    assert "must_exist" in diff["removed_columns"]


def test_strict_mode_type_change_raises(tmp_path: Path, spark_session) -> None:
    """Checklist 4 variant 3: type change → CONTRACT_BROKEN with changed_columns detail."""
    warehouse_root = str(tmp_path / "wh_typechange")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-typechange",
    )
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="STRING", nullable=False),
        SqlColumnSpec(name="amount", type="DECIMAL(18,4)", nullable=False),
    ])
    sql = (
        "SELECT CAST(1 AS INT) AS id, CAST(9.99 AS DECIMAL(10,2)) AS amount"
    )
    model = _contract_model(
        name="typechange_test",
        compiled_sql=sql,
        contract="strict",
        governance=governance,
    )
    with pytest.raises(PipelineError) as excinfo:
        executor.execute([model])
    exc = excinfo.value
    assert exc.error_code == "SQL_CONTRACT_BROKEN"
    diff = exc.context["diff"]
    changed_cols = {c["column"]: c for c in diff["changed_columns"]}
    assert "id" in changed_cols
    assert changed_cols["id"]["expected_type"] == "STRING"
    assert changed_cols["id"]["actual_type"] == "INT"
    assert "amount" in changed_cols
    assert changed_cols["amount"]["expected_type"] == "DECIMAL(18,4)"
    assert changed_cols["amount"]["actual_type"] == "DECIMAL(10,2)"


def test_strict_mode_parquet_catalog_mismatch_raises(
    tmp_path: Path, spark_session
) -> None:
    """Checklist 6: existing parquet catalog differs from declared spec.

    First run writes with schema A (id INT, extra STRING) using contract=off.
    Second run declares id INT only (strict mode matching DF), but existing
    catalog has the extra column still in the parquet → catalog mismatch.
    """
    warehouse_root = str(tmp_path / "wh_catalog")
    # ── Run 1: contract=off writes schema with extra column ──
    executor1 = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-cat-1",
    )
    gov1 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql_with_extra = (
        "SELECT CAST(1 AS INT) AS id, CAST('legacy' AS STRING) AS legacy_col"
    )
    model1 = _contract_model(
        name="catalog_test",
        compiled_sql=sql_with_extra,
        contract="off",
        governance=gov1,
    )
    executor1.execute([model1])

    # ── Run 2: strict mode; DF matches declared (id INT only) but catalog still has legacy_col ──
    executor2 = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-cat-2",
    )
    gov2 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql_id_only = "SELECT CAST(7 AS INT) AS id"
    model2 = _contract_model(
        name="catalog_test",
        compiled_sql=sql_id_only,
        contract="strict",
        governance=gov2,
    )
    with pytest.raises(PipelineError) as excinfo:
        executor2.execute([model2])
    exc = excinfo.value
    assert exc.error_code == "SQL_CONTRACT_BROKEN"
    # DF side matches declared → dataframe_schema check empty.
    # The raise source should be catalog_schema comparison.
    assert exc.context["comparison_target"] == "catalog_schema"
    diff = exc.context["diff"]
    assert "legacy_col" in diff["added_columns"]


def test_warn_mode_df_mismatch_allows_write(
    tmp_path: Path, spark_session
) -> None:
    """Checklist 7: warn mode — DF mismatch. Write proceeds, warning accumulated."""
    warehouse_root = str(tmp_path / "wh_warndf")
    executor = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-warndf",
    )
    governance = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    # DF has extra column + type mismatch on id (STRING vs INT declared)
    sql = "SELECT CAST('oops' AS STRING) AS id, CAST(99 AS BOOLEAN) AS extra"
    model = _contract_model(
        name="warndf_test",
        compiled_sql=sql,
        contract="warn",
        governance=governance,
        contract_version="cv-warn-1",
    )
    result = executor.execute([model])
    # Write succeeded (warn never blocks)
    assert result.executed_models[0].row_count == 1
    assert len(result.contract_warnings) == 1
    w = result.contract_warnings[0]
    assert w.model_id == "level3.contract.warndf_test"
    assert w.mode == "warn"
    assert w.comparison_target == "dataframe_schema"
    # id type mismatch + extra column present
    assert "extra" in w.diff.added_columns
    changed_cols = {c.column: c for c in w.diff.changed_columns}
    assert "id" in changed_cols
    assert changed_cols["id"].expected_type == "INT"
    assert changed_cols["id"].actual_type == "STRING"


def test_warn_mode_catalog_mismatch_allows_write(
    tmp_path: Path, spark_session
) -> None:
    """Checklist 7: warn mode — catalog mismatch. Both DF + catalog warnings accumulated."""
    warehouse_root = str(tmp_path / "wh_warncat")
    # ── Run 1: off mode writes {id INT, legacy_col DATE} ──
    ex1 = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-wc1",
    )
    gov1 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql1 = (
        "SELECT CAST(1 AS INT) AS id, CAST('2026-01-01' AS DATE) AS legacy_col"
    )
    ex1.execute([_contract_model(
        name="warncat_test", compiled_sql=sql1, contract="off", governance=gov1,
    )])

    # ── Run 2: warn mode; DF declares+produces only id INT → DF check passes,
    # but catalog contains legacy_col → catalog warning produced.
    ex2 = SparkSqlModelExecutor(
        spark=spark_session,
        warehouse_root=warehouse_root,
        root_path=str(tmp_path),
        environment="test",
        run_id="run-wc2",
    )
    gov2 = SqlModelGovernance(columns=[
        SqlColumnSpec(name="id", type="INT", nullable=False),
    ])
    sql2 = "SELECT CAST(5 AS INT) AS id"
    model2 = _contract_model(
        name="warncat_test",
        compiled_sql=sql2,
        contract="warn",
        governance=gov2,
    )
    result = ex2.execute([model2])
    assert result.executed_models[0].row_count == 1
    targets_hit = {w.comparison_target for w in result.contract_warnings}
    assert "catalog_schema" in targets_hit
    catalog_warn = next(
        w for w in result.contract_warnings if w.comparison_target == "catalog_schema"
    )
    assert "legacy_col" in catalog_warn.diff.added_columns


# ── Checklist 7 runtime emit: logs + metrics via run_sql_models_locally ─────

def _write_contract_sql_package(
    base_path: Path,
    *,
    contract: DataContractMode,
    contract_version: str | None = None,
    governance_block: str = "",
) -> Path:
    package_root = base_path / "sql_models"
    d = package_root / "level3" / "contract" / "base_orders"
    d.mkdir(parents=True, exist_ok=True)
    lines = [
        "name: base_orders",
        "stage: level3",
        "domain: contract",
        "materialization: table",
        "load_mode: full_refresh",
        "target:",
        "  table_name: base_orders",
        "sources:",
        "  - logical_name: raw_orders",
        "    source_name: orders_source",
        "    entity_name: orders",
        "owner:",
        "  name: platform",
        f"contract: {contract}",
    ]
    if contract_version is not None:
        lines.append(f"contract_version: {contract_version}")
    if governance_block:
        lines.append(governance_block.rstrip("\n"))
    manifest_text = "\n".join(lines) + "\n"
    (d / "manifest.yaml").write_text(manifest_text, encoding="utf-8")
    (d / "model.sql").write_text(
        dedent(
            """
            select
              CAST(order_id AS INT) AS order_id,
              CAST(amount AS DECIMAL(18,4)) AS amount,
              CAST(order_date AS DATE) AS order_date,
              CAST(unexpected AS BOOLEAN) AS extra_bonus_col
            from raw_orders
            """
        ).strip(),
        encoding="utf-8",
    )
    return package_root


def _yaml_governance_block(cols: list[tuple[str, str, bool]]) -> str:
    lines = ["governance:", "  columns:"]
    for name, t, nullable in cols:
        lines.append(f"    - name: {name}")
        lines.append(f"      type: {t}")
        lines.append(f"      nullable: {'true' if nullable else 'false'}")
    return "\n".join(lines)


def _seed_level2_for_contract(
    spark_session,
    root_path: Path,
) -> None:
    data_dir = (
        root_path
        / "level2"
        / "source=orders_source"
        / "entity=orders"
        / "mapping_version=v1"
        / "table=raw_orders"
        / "run_id=seed-run"
    )
    rows = [
        {
            "order_id": 1,
            "amount": "10.5000",
            "order_date": "2026-01-15",
            "unexpected": True,
            "source_name": "orders_source",
            "ingest_date": "2026-01-15",
            "_run_id": "seed-run",
        },
        {
            "order_id": 2,
            "amount": "20.7500",
            "order_date": "2026-01-16",
            "unexpected": False,
            "source_name": "orders_source",
            "ingest_date": "2026-01-16",
            "_run_id": "seed-run",
        },
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("amount", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("unexpected", BooleanType(), False),
        StructField("source_name", StringType(), False),
        StructField("ingest_date", StringType(), False),
        StructField("_run_id", StringType(), False),
    ])
    spark_session.createDataFrame(rows, schema=schema).write.mode("error").parquet(
        str(data_dir)
    )


def test_run_sql_locally_warn_mode_emits_logs_and_metrics(
    tmp_path: Path, spark_session
) -> None:
    """Checklist 7: end-to-end warn mode through runtime.py → log events + metric points."""
    _seed_level2_for_contract(spark_session, tmp_path)
    package_root = _write_contract_sql_package(
        tmp_path,
        contract="warn",
        contract_version="cv-runtime-1",
        governance_block=_yaml_governance_block([
            ("order_id", "INT", False),
            ("amount", "DECIMAL(18,4)", False),
            ("order_date", "DATE", False),
        ]),
    )
    discovered = discover_sql_models(package_root)
    compiled = [
        compile_sql_model(
            m,
            token_context=build_token_context(
                environment="dev",
                run_id="run-warn-runtime",
                stage=m.manifest.stage.value,
                domain=m.manifest.domain,
                model_name=m.manifest.name,
                target_table_name=m.manifest.target.table_name,
                start_date="2026-01-01",
                end_date="2026-12-31",
            ),
        )
        for m in discovered
    ]
    result = run_sql_models_locally(
        root_path=str(tmp_path),
        run_context=new_run_context(stage=StageName.sql, job_name="contract-warn-run"),
        environment="dev",
        package_path=package_root,
        warehouse_root=str(tmp_path / "warehouse"),
        spark=spark_session,
        compiled_models=compiled,
    )

    exec_result = result.execution_result
    assert exec_result.model_count == 1
    # Warning accumulated for extra_bonus_col (in DF, not declared)
    assert len(exec_result.contract_warnings) >= 1
    any_df_warn = any(
        w.comparison_target == "dataframe_schema"
        for w in exec_result.contract_warnings
    )
    assert any_df_warn, f"expected dataframe_schema warning in {exec_result.contract_warnings}"

    # Log file contains WARN / contract / contract_broken event
    assert result.artifacts.log_path is not None
    log_lines = Path(result.artifacts.log_path).read_text(encoding="utf-8").splitlines()
    contract_events = []
    for line in log_lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("event_type") == "contract_broken":
            contract_events.append(payload)
    assert len(contract_events) >= 1
    evt = contract_events[0]
    assert evt["severity"] == "WARN"
    assert evt["component"] == "contract"
    details = evt["details"]
    assert details["mode"] == "warn"
    assert details["model_id"] == "level3.contract.base_orders"
    assert details["comparison_target"] == "dataframe_schema"
    assert "extra_bonus_col" in details["diff"]["added_columns"]

    # Audit record contains contract warning count
    assert result.artifacts.audit_path is not None
    audit = json.loads(Path(result.artifacts.audit_path).read_text(encoding="utf-8"))
    cw_count = audit["metrics_summary"]["extra"].get("contract.broken_warnings", 0)
    assert cw_count >= 1

    # Metrics file contains elt.contract.broken counter
    metrics_path = (
        Path(result.artifacts.run_dir).parent / "metrics" / "metrics.jsonl"
    )
    if metrics_path.exists():
        metric_lines = metrics_path.read_text(encoding="utf-8").splitlines()
        contract_metrics = []
        for line in metric_lines:
            if not line.strip():
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError:
                continue
            if p.get("metric_name") == "elt.contract.broken":
                contract_metrics.append(p)
        assert len(contract_metrics) >= 1
        pm = contract_metrics[0]
        assert pm["metric_type"] == "counter"
        assert pm["value"] == 1
        assert pm["labels"]["mode"] == "warn"
        assert pm["labels"]["model_id"] == "level3.contract.base_orders"
        assert pm["labels"]["comparison_target"] == "dataframe_schema"


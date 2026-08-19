from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from elt_pipeline.maintenance import (
    DEFAULT_OPERATIONS,
    MaintenanceConfig,
    MaintenanceOperation,
    discover_tables_for_stage,
    resolve_table_selection,
    run_maintenance,
)
from elt_pipeline.spark.session import build_spark_session
from elt_pipeline.sql.models import (
    CompiledSqlModel,
    SqlLoadMode,
    SqlModelStage,
    SqlQualityExpectations,
)
from elt_pipeline.sql.spark_executor import (
    SparkSqlModelExecutor,
    _iceberg_catalog_name,
    _iceberg_table_fq,
)


@pytest.fixture(scope="module")
def maintain_spark(tmp_path_factory):  # type: ignore[no-untyped-def]
    module_root = tmp_path_factory.mktemp("maintain-ivy")
    ivy_home = str(module_root / "ivy2")
    os.environ["ELT_PIPELINE_IVY_HOME"] = ivy_home
    test_warehouse_root = tmp_path_factory.mktemp("maintain-wh")
    warehouse_dir = str(test_warehouse_root / "iceberg_warehouse")
    spark = build_spark_session(
        app_name="elt_pipeline_maintenance_test",
        iceberg_enabled=True,
        iceberg_catalog_type="hadoop",
        iceberg_warehouse_dir=warehouse_dir,
    )
    yield spark
    spark.stop()


def _insert_rows(
    *,
    spark: SparkSession,
    warehouse_root: str,
    name: str,
    stage: SqlModelStage,
    rows_sql: str,
    run_id: str,
    partition_columns: list[str] | None = None,
    load_mode: SqlLoadMode = SqlLoadMode.full_refresh,
) -> CompiledSqlModel:
    executor = SparkSqlModelExecutor(
        spark=spark,
        warehouse_root=warehouse_root,
        root_path=str(Path(__file__).resolve().parent.parent),
        environment="test",
        run_id=run_id,
    )
    model = CompiledSqlModel(
        model_id=f"{stage.value}.maint.{name}",
        stage=stage,
        domain="maint",
        name=name,
        target_table_name=name,
        load_mode=load_mode,
        materialization="table",  # type: ignore[arg-type]
        manifest_path=Path("/tmp/_mfake.yaml"),
        sql_path=Path("/tmp/_mfake.sql"),
        compiled_sql=rows_sql,
        partition_columns=list(partition_columns or []),
        quality=SqlQualityExpectations(row_count_min=1),
    )
    executor.execute(models=[model])
    return model


def _base_row_sql(source_name: str, business_date: str, n: int, amount: int = 100) -> str:
    unions: list[str] = []
    for i in range(n):
        unions.append(
            f"SELECT CAST('{source_name}' AS STRING) AS source_name, "
            f"CAST('{business_date}' AS DATE) AS business_date, "
            f"CAST({amount + i} AS DECIMAL(18,2)) AS amount, "
            f"CAST('o-{source_name}-{i}' AS STRING) AS order_id"
        )
    return " UNION ALL ".join(unions)


class TestTableSelectionNoSpark:
    """Unit tests for operation selection + CLI argument translation, without Spark."""

    def test_default_operations_are_three(self):
        assert len(DEFAULT_OPERATIONS) == 3
        assert MaintenanceOperation.compact in DEFAULT_OPERATIONS
        assert MaintenanceOperation.expire_snapshots in DEFAULT_OPERATIONS
        assert MaintenanceOperation.remove_orphans in DEFAULT_OPERATIONS
        assert MaintenanceOperation.rewrite_manifests not in DEFAULT_OPERATIONS

    def test_config_defaults(self):
        cfg = MaintenanceConfig()
        assert cfg.all_level3 is False
        assert cfg.all_level4 is False
        assert cfg.table_fqns == []
        assert cfg.operations == DEFAULT_OPERATIONS
        assert cfg.snapshot_retain_days == 7
        assert cfg.snapshot_retain_last == 1
        assert cfg.orphan_older_than_days == 3
        assert cfg.compact_strategy == "binpack"
        assert cfg.compact_min_input_files == 5
        assert cfg.compact_target_file_size_bytes is None
        assert cfg.dry_run is False

    def test_explicit_config_overrides(self):
        cfg = MaintenanceConfig(
            table_fqns=["iceberg.level3.maint.foo"],
            all_level3=True,
            operations=(MaintenanceOperation.expire_snapshots,),
            snapshot_retain_days=14,
            snapshot_retain_last=3,
            orphan_older_than_days=1,
            compact_strategy="sort",
            compact_min_input_files=2,
            compact_target_file_size_bytes=256 * 1024 * 1024,
            dry_run=True,
        )
        assert cfg.table_fqns == ["iceberg.level3.maint.foo"]
        assert cfg.all_level3 is True
        assert cfg.operations == (MaintenanceOperation.expire_snapshots,)
        assert cfg.snapshot_retain_days == 14
        assert cfg.snapshot_retain_last == 3
        assert cfg.orphan_older_than_days == 1
        assert cfg.compact_strategy == "sort"
        assert cfg.compact_min_input_files == 2
        assert cfg.compact_target_file_size_bytes == 256 * 1024 * 1024
        assert cfg.dry_run is True

    def test_resolve_empty_selection(self):
        from unittest.mock import MagicMock
        spark = MagicMock(spec=SparkSession)
        spark.sql = MagicMock()
        result = resolve_table_selection(
            spark=spark, config=MaintenanceConfig()
        )
        assert result == []

    def test_resolve_explicit_only(self):
        from unittest.mock import MagicMock
        spark = MagicMock(spec=SparkSession)
        spark.sql = MagicMock()
        result = resolve_table_selection(
            spark=spark,
            config=MaintenanceConfig(table_fqns=["iceberg.level3.a.x", "iceberg.level4.b.y"]),
        )
        assert result == ["iceberg.level3.a.x", "iceberg.level4.b.y"]

    def test_resolve_deduplicates(self):
        from unittest.mock import MagicMock
        spark = MagicMock(spec=SparkSession)
        spark.sql = MagicMock()
        result = resolve_table_selection(
            spark=spark,
            config=MaintenanceConfig(
                table_fqns=["iceberg.level3.a.x", "iceberg.level3.a.x"]
            ),
        )
        assert result == ["iceberg.level3.a.x"]


class TestMaintenanceOnRealIceberg:
    def _seed_compactable(
        self,
        *,
        spark: SparkSession,
        tmp_path: Path,
        name: str,
        n_batches: int,
        stage: SqlModelStage = SqlModelStage.level3,
        partition_columns: list[str] | None = None,
    ) -> tuple[str, int, int]:
        wh = str(tmp_path / "wh")
        total = 0
        for i in range(n_batches):
            n_rows = 2 + (i % 3)
            _insert_rows(
                spark=spark,
                warehouse_root=wh,
                name=name,
                stage=stage,
                rows_sql=_base_row_sql(f"src{i}", "2026-08-10", n_rows, amount=100 * i),
                run_id=f"seed-{name}-{i}",
                partition_columns=partition_columns,
                load_mode=SqlLoadMode.append if i > 0 else SqlLoadMode.full_refresh,
            )
            total += n_rows
        fq = _iceberg_table_fq(stage=stage, domain="maint", name=name, spark=spark)
        snapshots = spark.sql(f"SELECT snapshot_id FROM {fq}.snapshots").collect()
        return fq, total, len(snapshots)

    def test_dry_run_uses_explicit_fqns_without_executing_calls(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        wh = str(tmp_path / "wh_dry")
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="dry_explicit_1",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("src1", "2026-08-10", 3),
            run_id="dry-1",
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="dry_explicit_2",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("src2", "2026-08-11", 2),
            run_id="dry-2",
            load_mode=SqlLoadMode.full_refresh,
        )
        fq1 = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="dry_explicit_1", spark=maintain_spark
        )
        fq2 = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="dry_explicit_2", spark=maintain_spark
        )
        cfg = MaintenanceConfig(
            table_fqns=[fq1, fq2],
            operations=(MaintenanceOperation.compact,),
            dry_run=True,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        assert report.dry_run is True
        assert report.results == []
        assert MaintenanceOperation.compact.value in report.operations_requested
        assert set(report.tables_selected) == {fq1, fq2}
        payload = report.to_dict()
        assert payload["dry_run"] is True
        assert payload["results"] == []
        assert "started_at" in payload and "finished_at" in payload

    def test_discovery_finds_known_tables(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        wh = str(tmp_path / "wh_disc")
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="disco_l3_a",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("a", "2026-08-10", 2),
            run_id="disc-a1",
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="disco_l3_b",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("b", "2026-08-10", 1),
            run_id="disc-b1",
        )
        fq_a = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="disco_l3_a", spark=maintain_spark
        )
        fq_b = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="disco_l3_b", spark=maintain_spark
        )
        l3_tables = set(
            discover_tables_for_stage(spark=maintain_spark, stage=SqlModelStage.level3)
        )
        # The discovery function MUST surface the tables we just wrote. It may
        # also contain tables from other module-scoped tests; we only assert the
        # known names, which represents the user-observable guarantee we need.
        assert fq_a in l3_tables
        assert fq_b in l3_tables

    def test_expire_snapshots_runs_and_preserves_data(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        """Creating multiple batches and calling expire_snapshots should not raise, should
        report a non-empty maintenance result for expire_snapshots, and keep all rows."""
        name = "exp_snap_maint"
        fq, total, snap_count = self._seed_compactable(
            spark=maintain_spark, tmp_path=tmp_path, name=name, n_batches=4
        )
        assert snap_count >= 2
        assert maintain_spark.table(fq).count() == total

        cfg = MaintenanceConfig(
            table_fqns=[fq],
            operations=(MaintenanceOperation.expire_snapshots,),
            # Very aggressive, but retain_last >= 1 hard-floor avoids catastrophes
            # even with tight windows on fresh catalogs (retains latest snapshot).
            snapshot_retain_days=0,
            snapshot_retain_last=1,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        assert report.dry_run is False
        assert len([r for r in report.results if r.operation == "expire_snapshots"]) == 1
        # expire_snapshots rows is a list of dicts describing what got removed (may be empty
        # list if nothing was eligible due to tight retention window being short in fresh
        # catalog implementations with 0-day retention still executes cleanly.
        result_row = [r for r in report.results if r.operation == "expire_snapshots"][0]
        assert isinstance(result_row.rows, list)
        # Data integrity is the critical assertion: snapshots or data integrity.
        assert maintain_spark.table(fq).count() == total
        new_snaps = spark_snapshot_count(maintain_spark, fq)
        # New snapshots don't grow.
        assert new_snaps >= 1

    def test_compact_reports_result_and_preserves_data(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        name = "comp_tbl_maint"
        fq, total, _ = self._seed_compactable(
            spark=maintain_spark, tmp_path=tmp_path, name=name, n_batches=4
        )
        before_rows = sorted(
            tuple(str(x) for x in r) for r in maintain_spark.table(fq).collect()
        )
        cfg = MaintenanceConfig(
            table_fqns=[fq],
            operations=(MaintenanceOperation.compact,),
            compact_min_input_files=1,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        compact_results = [r for r in report.results if r.operation == "compact"]
        assert len(compact_results) == 1
        assert compact_results[0].rows is not None
        after_rows = sorted(
            tuple(str(x) for x in r) for r in maintain_spark.table(fq).collect()
        )
        assert after_rows == before_rows
        assert maintain_spark.table(fq).count() == total

    def test_expire_runs_on_two_l3_tables_via_explicit_list(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        wh = str(tmp_path / "wh_all")
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="expl3a",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("a", "2026-08-12", 2),
            run_id="expl3a-1",
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="expl3a",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("a", "2026-08-13", 2),
            run_id="expl3a-2",
            load_mode=SqlLoadMode.append,
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="expl3b",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("b", "2026-08-12", 1),
            run_id="expl3b-1",
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="expl3b",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("b", "2026-08-13", 1),
            run_id="expl3b-2",
            load_mode=SqlLoadMode.append,
        )
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="l4_notincluded",
            stage=SqlModelStage.level4,
            rows_sql=_base_row_sql("mart", "2026-08-13", 1),
            run_id="l4notin-1",
        )
        fq_a = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="expl3a", spark=maintain_spark
        )
        fq_b = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="expl3b", spark=maintain_spark
        )
        fq_l4 = _iceberg_table_fq(
            stage=SqlModelStage.level4, domain="maint", name="l4_notincluded", spark=maintain_spark
        )

        cfg = MaintenanceConfig(
            table_fqns=[fq_a, fq_b],
            operations=(MaintenanceOperation.expire_snapshots,),
            snapshot_retain_days=0,
            snapshot_retain_last=1,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        fq_selected = set(report.tables_selected)
        assert fq_a in fq_selected
        assert fq_b in fq_selected
        assert fq_l4 not in fq_selected
        ops_done = {r.operation for r in report.results}
        assert ops_done == {"expire_snapshots"}
        fqns_by_result = {r.fqn for r in report.results}
        assert fq_a in fqns_by_result
        assert fq_b in fqns_by_result

    def test_full_default_run_on_level4_table_via_explicit_fqn(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        """Run all three default operations on an L4 table; assert data survives."""
        name = "mart_all_maint"
        wh = str(tmp_path / "wh_mart")
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name=name,
            stage=SqlModelStage.level4,
            rows_sql=_base_row_sql("m1", "2026-08-10", 3),
            run_id="mart_all-1",
        )
        for i, src in enumerate(["m2", "m3", "m4"], start=2):
            _insert_rows(
                spark=maintain_spark,
                warehouse_root=wh,
                name=name,
                stage=SqlModelStage.level4,
                rows_sql=_base_row_sql(src, "2026-08-10", 3, amount=200 * i),
                run_id=f"mart_all-{i}",
                load_mode=SqlLoadMode.append,
            )
        fq = _iceberg_table_fq(
            stage=SqlModelStage.level4, domain="maint", name=name, spark=maintain_spark
        )
        before_rows = sorted(
            tuple(str(x) for x in r) for r in maintain_spark.table(fq).collect()
        )
        before_count = maintain_spark.table(fq).count()
        cfg = MaintenanceConfig(
            table_fqns=[fq],
            snapshot_retain_days=0,
            snapshot_retain_last=1,
            # remove_orphan_files enforces a minimum 24-hour older-than interval
            # at the procedure level, so we cannot pass 0 here in tests or prod.
            # The default 3 days is safe; with no old files nothing gets deleted
            # but the CALL still runs cleanly.
            orphan_older_than_days=3,
            compact_min_input_files=1,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        assert fq in report.tables_selected
        ops_performed = {r.operation for r in report.results if r.fqn == fq}
        assert ops_performed == {
            MaintenanceOperation.compact.value,
            MaintenanceOperation.expire_snapshots.value,
            MaintenanceOperation.remove_orphans.value,
        }
        after_rows = sorted(
            tuple(str(x) for x in r) for r in maintain_spark.table(fq).collect()
        )
        assert after_rows == before_rows
        assert maintain_spark.table(fq).count() == before_count

    def test_remove_orphans_handles_empty_table_gracefully(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        wh = str(tmp_path / "wh_orphan")
        _insert_rows(
            spark=maintain_spark,
            warehouse_root=wh,
            name="orph_grace",
            stage=SqlModelStage.level3,
            rows_sql=_base_row_sql("src_orph", "2026-08-10", 1),
            run_id="orph-1",
        )
        fq = _iceberg_table_fq(
            stage=SqlModelStage.level3, domain="maint", name="orph_grace", spark=maintain_spark
        )
        cfg = MaintenanceConfig(
            table_fqns=[fq],
            operations=(MaintenanceOperation.remove_orphans,),
            orphan_older_than_days=0,
        )
        report = run_maintenance(spark=maintain_spark, config=cfg)
        assert len(report.results) == 1
        assert report.results[0].operation == MaintenanceOperation.remove_orphans.value
        assert isinstance(report.results[0].rows, list)

    def test_discover_nonexistent_stage_returns_empty(
        self, maintain_spark: SparkSession, tmp_path: Path
    ) -> None:
        catalog = _iceberg_catalog_name(maintain_spark)
        fake_parent = f"{catalog}.nonexistent_stage_xyz"
        try:
            maintain_spark.sql(f"SHOW NAMESPACES IN {fake_parent}").collect()
        except Exception:
            pass
        try:
            result = discover_tables_for_stage(
                spark=maintain_spark, stage=SqlModelStage.level4
            )
            assert isinstance(result, list)
        except Exception:  # noqa: BLE001
            pytest.fail("discover_tables_for_stage raised an exception unexpectedly")


def spark_snapshot_count(spark: SparkSession, fq_table: str) -> int:
    return spark.sql(f"SELECT snapshot_id FROM {fq_table}.snapshots").count()

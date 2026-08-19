"""Iceberg table maintenance operations: compaction, snapshot expiry, orphan cleanup.

Iceberg tables degrade over time without maintenance:
  - Small-file explosion from incremental writes → slow queries.
  - Unbounded snapshot + metadata growth → storage bloat.
  - Orphan data/metadata files from failed writes → slow query planning and storage cost.

This module wraps Iceberg's Spark SQL procedures (`CALL catalog.system.*`) into a
config-driven maintenance runner. The four operations are:

  1. ``rewrite_data_files``  — compact small files (strategy: binpack or sort).
  2. ``expire_snapshots``    — remove snapshots older than a retention window
                                (always ``retain_last`` at minimum).
  3. ``remove_orphan_files`` — delete files under the table location that are not
                                referenced by any current or retained snapshot.
  4. ``rewrite_manifests``   — optional. Rewrite manifests for plan pruning on
                                tables with very many manifest files.

The module is deliberately simple: one frozen config dataclass, one runner, four
single-procedure helpers. All Spark SQL goes through ``spark.sql(...)`` against the
Iceberg catalog's ``system`` namespace so it works on every valid catalog binding
(hadoop / jdbc / rest / nessie / glue / hive_metastore / snowflake serving).
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from elt_pipeline.sql.models import SqlModelStage
from elt_pipeline.sql.spark_executor import _iceberg_catalog_name


class MaintenanceOperation(str, enum.Enum):
    """Which Iceberg maintenance operations are allowed in a maintenance run."""

    compact = "compact"
    expire_snapshots = "expire_snapshots"
    remove_orphans = "remove_orphans"
    rewrite_manifests = "rewrite_manifests"


DEFAULT_OPERATIONS: tuple[MaintenanceOperation, ...] = (
    MaintenanceOperation.compact,
    MaintenanceOperation.expire_snapshots,
    MaintenanceOperation.remove_orphans,
)


@dataclass(frozen=True)
class MaintenanceConfig:
    """All knobs for one ``run_maintenance(...)`` invocation.

    Table selection is additive:
      - explicit ``table_fqns`` (fully qualified, e.g. ``iceberg.level3.sales.orders``),
      - plus ``all_level3`` (every table in the writer catalog under the ``level3`` namespace),
      - plus ``all_level4`` (same for ``level4``).
    The runner deduplicates the union.
    """

    table_fqns: list[str] = field(default_factory=list)
    all_level3: bool = False
    all_level4: bool = False

    operations: tuple[MaintenanceOperation, ...] = DEFAULT_OPERATIONS

    snapshot_retain_days: int = 7
    snapshot_retain_last: int = 1
    orphan_older_than_days: int = 3

    compact_strategy: str = "binpack"
    compact_min_input_files: int = 5
    compact_target_file_size_bytes: int | None = None

    dry_run: bool = False


@dataclass(frozen=True)
class MaintenanceTableResult:
    fqn: str
    operation: str
    rows: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "fqn": self.fqn,
            "operation": self.operation,
            "rows": [dict(r) for r in self.rows],
        }


@dataclass(frozen=True)
class MaintenanceReport:
    tables_selected: list[str]
    operations_requested: list[str]
    results: list[MaintenanceTableResult]
    dry_run: bool
    started_at: datetime
    finished_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "tables_selected": list(self.tables_selected),
            "operations_requested": list(self.operations_requested),
            "dry_run": self.dry_run,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "results": [r.to_dict() for r in self.results],
        }


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------


def _sql_row_first(row: Any) -> Any:
    """Return the first value of a Spark Row, regardless of attribute access."""
    try:
        return row[0]
    except Exception:  # noqa: BLE001
        return None


def discover_tables_for_stage(
    *, spark: SparkSession, stage: SqlModelStage
) -> list[str]:
    """Return fully-qualified Iceberg table names under ``<catalog>.<stage.value>``.

    Uses ``SHOW NAMESPACES`` / ``SHOW TABLES`` SQL so behavior is consistent across
    every Iceberg catalog type (hadoop / jdbc / rest / nessie / glue / hive_metastore
    / snowflake serving bindings).

    SqlModel tables live at ``<catalog>.<stage>.<domain>.<name>`` so we walk domain
    sub-namespaces one level below ``<stage>`` to find actual tables.
    """
    catalog = _iceberg_catalog_name(spark)
    parent_ns = f"{catalog}.{stage.value}"
    tables: list[str] = []
    try:
        ns_rows = spark.sql(f"SHOW NAMESPACES IN {parent_ns}").collect()
    except Exception:  # noqa: BLE001
        ns_rows = []
    for row in ns_rows:
        name = _sql_row_first(row)
        if not name:
            continue
        # SHOW NAMESPACES returns the full catalog-relative namespace path, e.g.
        # querying ``IN iceberg.level3`` returns ``level3.maint``, not just ``maint``.
        # Re-bind it to the catalog to get a fully-qualified namespace.
        if isinstance(name, str) and "." in name:
            child_ns = f"{catalog}.{name}"
        else:
            child_ns = f"{parent_ns}.{name}"
        try:
            tbl_rows = spark.sql(f"SHOW TABLES IN {child_ns}").collect()
        except Exception:  # noqa: BLE001
            tbl_rows = []
        for trow in tbl_rows:
            tname = None
            try:
                tname = trow["tableName"]
            except Exception:  # noqa: BLE001
                # SHOW TABLES also returns the namespace as the first column for
                # some catalogs; tableName is typically position 1.
                cols = list(trow.asDict().keys()) if hasattr(trow, "asDict") else []
                if "tableName" in cols:
                    tname = _sql_row_first(trow)
                elif len(cols) >= 2:
                    try:
                        tname = trow[1]
                    except Exception:  # noqa: BLE001
                        tname = _sql_row_first(trow)
                else:
                    tname = _sql_row_first(trow)
            if not tname:
                continue
            tables.append(f"{child_ns}.{tname}")
    # Fallback — some catalog types use a flatter namespace shape.
    if not tables:
        try:
            tbl_rows = spark.sql(f"SHOW TABLES IN {parent_ns}").collect()
        except Exception:  # noqa: BLE001
            tbl_rows = []
        for trow in tbl_rows:
            tname = None
            try:
                tname = trow["tableName"]
            except Exception:  # noqa: BLE001
                tname = _sql_row_first(trow)
            if not tname:
                continue
            tables.append(f"{parent_ns}.{tname}")
    return sorted(set(tables))


def resolve_table_selection(
    *, spark: SparkSession, config: MaintenanceConfig
) -> list[str]:
    """Combine explicit + ``all_level3`` / ``all_level4`` selections, dedupe, sort."""
    selected: set[str] = set(config.table_fqns)
    if config.all_level3:
        selected.update(discover_tables_for_stage(spark=spark, stage=SqlModelStage.level3))
    if config.all_level4:
        selected.update(discover_tables_for_stage(spark=spark, stage=SqlModelStage.level4))
    return sorted(selected)


# ---------------------------------------------------------------------------
# Single-procedure helpers. Each returns the Spark Row list the CALL produces.
# ---------------------------------------------------------------------------


def _literal_table_arg(value: str) -> str:
    """Single-quote escape for a CALL positional string argument."""
    return "'" + value.replace("'", "''") + "'"


def run_compact(
    *, spark: SparkSession, table_fqn: str, config: MaintenanceConfig
) -> list[dict[str, Any]]:
    strategy = str(config.compact_strategy).strip().lower()
    if strategy not in {"binpack", "sort"}:
        raise ValueError(
            f"compact_strategy must be 'binpack' or 'sort', got {config.compact_strategy!r}"
        )
    args_parts: list[str] = [f"table => {_literal_table_arg(table_fqn)}"]
    options_entries: dict[str, str] = {}
    if strategy == "sort":
        raise NotImplementedError(
            "compact_strategy='sort' requires an explicit sort_order expression, "
            "which is not yet exposed via MaintenanceConfig. Use 'binpack' (default)."
        )
    if config.compact_min_input_files is not None:
        options_entries["min-input-files"] = str(int(config.compact_min_input_files))
    if config.compact_target_file_size_bytes is not None:
        options_entries["target-file-size-bytes"] = str(
            int(config.compact_target_file_size_bytes)
        )
    if options_entries:
        map_parts = []
        for k, v in options_entries.items():
            map_parts.append(f"{_literal_table_arg(k)}, {_literal_table_arg(v)}")
        args_parts.append(f"options => MAP({', '.join(map_parts)})")
    call_sql = (
        f"CALL {_iceberg_catalog_name(spark)}.system.rewrite_data_files("
        + ", ".join(args_parts)
        + ")"
    )
    return _call_and_collect(spark, call_sql)


def run_expire_snapshots(
    *, spark: SparkSession, table_fqn: str, config: MaintenanceConfig
) -> list[dict[str, Any]]:
    older_than_dt = datetime.now(UTC) - timedelta(days=max(0, int(config.snapshot_retain_days)))
    retain_last = max(1, int(config.snapshot_retain_last))
    args_parts = [
        f"table => {_literal_table_arg(table_fqn)}",
        (
            "older_than => TIMESTAMP "
            f"{_literal_table_arg(older_than_dt.strftime('%Y-%m-%d %H:%M:%S'))}"
        ),
        f"retain_last => {retain_last}",
    ]
    call_sql = (
        f"CALL {_iceberg_catalog_name(spark)}.system.expire_snapshots("
        + ", ".join(args_parts)
        + ")"
    )
    return _call_and_collect(spark, call_sql)


def run_remove_orphans(
    *, spark: SparkSession, table_fqn: str, config: MaintenanceConfig
) -> list[dict[str, Any]]:
    # Iceberg remove_orphan_files enforces a minimum 24-hour older-than interval to guard
    # against races with concurrent writers, so we floor any sub-day values to 1 day.
    days = max(1, int(config.orphan_older_than_days))
    older_than_dt = datetime.now(UTC) - timedelta(days=days)
    args_parts = [
        f"table => {_literal_table_arg(table_fqn)}",
        (
            "older_than => TIMESTAMP "
            f"{_literal_table_arg(older_than_dt.strftime('%Y-%m-%d %H:%M:%S'))}"
        ),
    ]
    call_sql = (
        f"CALL {_iceberg_catalog_name(spark)}.system.remove_orphan_files({', '.join(args_parts)})"
    )
    return _call_and_collect(spark, call_sql)


def run_rewrite_manifests(
    *, spark: SparkSession, table_fqn: str
) -> list[dict[str, Any]]:
    args_parts = [f"table => {_literal_table_arg(table_fqn)}"]
    call_sql = (
        f"CALL {_iceberg_catalog_name(spark)}.system.rewrite_manifests({', '.join(args_parts)})"
    )
    return _call_and_collect(spark, call_sql)


def _call_and_collect(spark: SparkSession, call_sql: str) -> list[dict[str, Any]]:
    df: DataFrame = spark.sql(call_sql)
    return [r.asDict(recursive=True) for r in df.collect()]


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------


def run_maintenance(
    *, spark: SparkSession, config: MaintenanceConfig
) -> MaintenanceReport:
    """Run one set of Iceberg maintenance operations on the configured table set.

    Returns a structured :class:`MaintenanceReport` suitable for JSON serialisation
    in the CLI output. With ``config.dry_run=True``, no CALL is issued: the report
    lists the selected tables and requested operations only.
    """
    started_at = datetime.now(UTC)
    selected = resolve_table_selection(spark=spark, config=config)
    op_list = [op.value for op in config.operations]
    if config.dry_run:
        finished_at = datetime.now(UTC)
        return MaintenanceReport(
            tables_selected=selected,
            operations_requested=op_list,
            results=[],
            dry_run=True,
            started_at=started_at,
            finished_at=finished_at,
        )

    results: list[MaintenanceTableResult] = []
    ops_order: tuple[MaintenanceOperation, ...] = config.operations
    for fqn in selected:
        for op in ops_order:
            if op is MaintenanceOperation.compact:
                rows = run_compact(spark=spark, table_fqn=fqn, config=config)
            elif op is MaintenanceOperation.expire_snapshots:
                rows = run_expire_snapshots(spark=spark, table_fqn=fqn, config=config)
            elif op is MaintenanceOperation.remove_orphans:
                rows = run_remove_orphans(spark=spark, table_fqn=fqn, config=config)
            elif op is MaintenanceOperation.rewrite_manifests:
                rows = run_rewrite_manifests(spark=spark, table_fqn=fqn)
            else:  # pragma: no cover - enum guard
                raise ValueError(f"Unknown MaintenanceOperation: {op!r}")
            results.append(MaintenanceTableResult(fqn=fqn, operation=op.value, rows=rows))

    finished_at = datetime.now(UTC)
    return MaintenanceReport(
        tables_selected=selected,
        operations_requested=op_list,
        results=results,
        dry_run=False,
        started_at=started_at,
        finished_at=finished_at,
    )


__all__ = [
    "DEFAULT_OPERATIONS",
    "MaintenanceConfig",
    "MaintenanceOperation",
    "MaintenanceReport",
    "MaintenanceTableResult",
    "discover_tables_for_stage",
    "resolve_table_selection",
    "run_compact",
    "run_expire_snapshots",
    "run_maintenance",
    "run_remove_orphans",
    "run_rewrite_manifests",
]

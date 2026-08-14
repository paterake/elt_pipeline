from __future__ import annotations

from datetime import UTC, datetime

import pytest

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.planner import NormalizationPlanner
from elt_pipeline.normalize.runner import NormalizationRunner

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from elt_pipeline.normalize.spark_runner import SparkRelationalizer

    _HAS_PYSPARK_JVM = True
except Exception:  # pragma: no cover - Spark/JVM unavailable
    _HAS_PYSPARK_JVM = False


def build_manifest(
    *,
    entity_name: str = "orders",
    payload_format: str = "json",
) -> Level1ArtifactManifest:
    return Level1ArtifactManifest(
        artifact_id="artifact-001",
        run_id="run-001",
        job_name="normalize-orders",
        trigger_type="manual",
        environment="dev",
        source_name="rest_source",
        entity_name=entity_name,
        extraction_mode="scheduled_batch",
        ingest_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ingest_completed_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_format=payload_format,
        content_hash="abc123",
        file_size_bytes=128,
        data_path=(
            f"level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/orders."
            f"{payload_format}"
        ),
        manifest_path=(
            "level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/"
            f"orders.{payload_format}.manifest.json"
        ),
    )


def _nested_orders_payload() -> dict:
    return {
        "order_id": "ORD-001",
        "customer": {
            "id": "CUST-42",
            "name": "Ada Lovelace",
            "address": {"city": "London", "postcode": "SW1A 1AA"},
        },
        "items": [
            {
                "sku": "BOOK-ALG-001",
                "quantity": 2,
                "unit_price": 39.99,
                "tags": ["algorithm", "hardcover"],
                "tax_breakdowns": [
                    {
                        "jurisdiction": "uk_vat",
                        "rate": 0.20,
                        "amount": 15.996,
                        "jurisdictions": [
                            {"level": "national", "code": "GB", "allocated": 12.796},
                            {"level": "city", "code": "LDN", "allocated": 3.20},
                        ],
                    },
                    {
                        "jurisdiction": "uk_book_levis",
                        "rate": 0.00,
                        "amount": 0.00,
                        "jurisdictions": [],
                    },
                ],
            },
            {
                "sku": "JRNL-NB-009",
                "quantity": 1,
                "unit_price": 12.50,
                "tags": [],
                "tax_breakdowns": [
                    {
                        "jurisdiction": "uk_vat",
                        "rate": 0.20,
                        "amount": 2.50,
                        "jurisdictions": [
                            {"level": "national", "code": "GB", "allocated": 2.50},
                        ],
                    },
                ],
            },
        ],
        "priority_codes": ["EXPEDITE", "GIFT"],
    }


def _expected_logical_paths() -> set[str]:
    return {
        "$",
        "$.items",
        "$.items.tags",
        "$.items.tax_breakdowns",
        "$.items.tax_breakdowns.jurisdictions",
        "$.priority_codes",
    }


def _expected_physical_tables() -> set[str]:
    return {
        "orders",
        "orders__items",
        "orders__tags",
        "orders__tax_breakdowns",
        "orders__jurisdictions",
        "orders__priority_codes",
    }


def _csv_orders_text() -> str:
    return (
        "order_id,customer_name,amount,status\n"
        "ORD-A,Ada Lovelace,100.50,open\n"
        "ORD-B,Grace Hopper,250.00,fulfilled\n"
        "ORD-C,Katherine Johnson,17.25,refunded\n"
    )


@pytest.fixture(scope="session")
def spark_session_fixture():
    if not _HAS_PYSPARK_JVM:
        pytest.skip("PySpark JVM not available (no Java on PATH)")
    from elt_pipeline.spark.session import build_spark_session

    session = build_spark_session(app_name="elt-parity-tests", master="local[1]")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# 1. Mapping-version parity (Python runner vs Spark planner, metadata only —
#    does NOT require JVM)
# ---------------------------------------------------------------------------

def test_python_runner_and_spark_planner_produce_identical_mapping_version_nested_orders() -> None:
    """Contract C2 + Gate 2 parity requirement.

    Given the same 3-deep nested JSON payload shape, the legacy Python
    NormalizationRunner and the new Spark NormalizationPlanner (driven from an
    equivalent StructType schema) must emit a byte-identical 16-hex
    mapping_version hash.

    This test runs without a JVM — it exercises only the metadata / policy code
    path.
    """
    payload = _nested_orders_payload()

    legacy_result = NormalizationRunner().normalize_level1_json(
        manifest=build_manifest(),
        payload=payload,
    )

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("city", StringType(), True),
                StructField("postcode", StringType(), True),
            ]), True),
        ]), True),
        StructField("items", ArrayType(StructType([
            StructField("sku", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("tags", ArrayType(StringType(), True), True),
            StructField("tax_breakdowns", ArrayType(StructType([
                StructField("jurisdiction", StringType(), True),
                StructField("rate", DoubleType(), True),
                StructField("amount", DoubleType(), True),
                StructField("jurisdictions", ArrayType(StructType([
                    StructField("level", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("allocated", DoubleType(), True),
                ]), True), True),
            ]), True), True),
        ]), True), True),
        StructField("priority_codes", ArrayType(StringType(), True), True),
    ])

    plan = NormalizationPlanner().plan_from_schema(
        source_name="rest_source",
        entity_name="orders",
        schema=schema,
    )
    _, spark_mapping_version = plan.build_mapping_catalog()

    assert (
        spark_mapping_version == legacy_result.mapping_version
    ), (
        "Contract C2 violated: Spark planner mapping_version differs from legacy "
        "Python runner on the same logical schema."
    )


# ---------------------------------------------------------------------------
# 2. Table-name + column-name parity (metadata only, no JVM required)
# ---------------------------------------------------------------------------

def test_python_runner_and_spark_planner_produce_identical_table_and_column_names() -> None:
    """Contract C3 + logical-path set parity.

    Verifies that the set of logical paths, the physical_table_name for each
    logical_path, and the list of (logical_path, physical_name) column
    mappings are byte-identical between legacy runner and Spark planner.
    """
    payload = _nested_orders_payload()

    legacy_result = NormalizationRunner().normalize_level1_json(
        manifest=build_manifest(),
        payload=payload,
    )
    legacy_entries_by_path = {
        entry.logical_path: entry for entry in legacy_result.mapping_catalog.entries
    }
    assert legacy_entries_by_path.keys() == _expected_logical_paths()

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("city", StringType(), True),
                StructField("postcode", StringType(), True),
            ]), True),
        ]), True),
        StructField("items", ArrayType(StructType([
            StructField("sku", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("tags", ArrayType(StringType(), True), True),
            StructField("tax_breakdowns", ArrayType(StructType([
                StructField("jurisdiction", StringType(), True),
                StructField("rate", DoubleType(), True),
                StructField("amount", DoubleType(), True),
                StructField("jurisdictions", ArrayType(StructType([
                    StructField("level", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("allocated", DoubleType(), True),
                ]), True), True),
            ]), True), True),
        ]), True), True),
        StructField("priority_codes", ArrayType(StringType(), True), True),
    ])

    plan = NormalizationPlanner().plan_from_schema(
        source_name="rest_source",
        entity_name="orders",
        schema=schema,
    )
    spark_entries_by_path = {
        entry.logical_path: entry for entry in plan.mapping_entries()
    }
    assert spark_entries_by_path.keys() == _expected_logical_paths()

    physical_names = {e.physical_table_name for e in spark_entries_by_path.values()}
    assert physical_names == _expected_physical_tables()

    for logical_path in _expected_logical_paths():
        legacy_entry = legacy_entries_by_path[logical_path]
        spark_entry = spark_entries_by_path[logical_path]
        assert spark_entry.physical_table_name == legacy_entry.physical_table_name, (
            f"physical_table_name mismatch at {logical_path}"
        )
        assert spark_entry.parent_table_name == legacy_entry.parent_table_name, (
            f"parent_table_name mismatch at {logical_path}"
        )
        assert spark_entry.join_key_columns == legacy_entry.join_key_columns, (
            f"join_key_columns mismatch at {logical_path}"
        )

        legacy_cols = {(m.logical_path, m.physical_name) for m in legacy_entry.column_mappings}
        spark_cols = {(m.logical_path, m.physical_name) for m in spark_entry.column_mappings}
        assert spark_cols == legacy_cols, (
            f"column mappings mismatch at {logical_path}: "
            f"spark-only={spark_cols - legacy_cols}, legacy-only={legacy_cols - spark_cols}"
        )


# ---------------------------------------------------------------------------
# 3. Table-name collision policy parity (63-char + hash suffix fallback +
#    collision guard)
# ---------------------------------------------------------------------------

def test_table_name_hash_suffix_applied_identically_for_overflow_and_collision() -> None:
    """Contract C3: long names → 63-char truncation + SHA-8 suffix.

    Two logical arrays that sanitize to the same base name (e.g. line-items
    and line_items) must produce the same physical names and collision
    behaviour in legacy and Spark planner.
    """
    payload = {
        "line-items": [{"sku": "SKU-1"}],
        "line_items": [{"sku": "SKU-2"}],
    }
    manifest = build_manifest()
    legacy_result = NormalizationRunner().normalize_level1_json(
        manifest=manifest, payload=payload
    )
    legacy_entries = {e.logical_path: e for e in legacy_result.mapping_catalog.entries}

    schema = StructType([
        StructField("line-items", ArrayType(StructType([
            StructField("sku", StringType(), True),
        ]), True), True),
        StructField("line_items", ArrayType(StructType([
            StructField("sku", StringType(), True),
        ]), True), True),
    ])
    plan = NormalizationPlanner().plan_from_schema(
        source_name=manifest.source_name,
        entity_name=manifest.entity_name,
        schema=schema,
    )
    spark_entries = {e.logical_path: e for e in plan.mapping_entries()}

    li_legacy = legacy_entries["$.line-items"].physical_table_name
    li_spark = spark_entries["$.line-items"].physical_table_name
    assert li_legacy == li_spark
    li2_legacy = legacy_entries["$.line_items"].physical_table_name
    li2_spark = spark_entries["$.line_items"].physical_table_name
    assert li2_legacy == li2_spark

    assert legacy_entries["$.line-items"].physical_table_name == "orders__line_items"
    hashed_name = legacy_entries["$.line_items"].physical_table_name
    assert hashed_name.startswith("orders__line_items__")
    suffix_sep = "__"
    base_part, _, suffix = hashed_name.rpartition(suffix_sep)
    assert len(suffix) == 8, "SHA-8 suffix expected (8 hex chars)"
    assert all(c in "0123456789abcdef" for c in suffix)


# ---------------------------------------------------------------------------
# 4. CSV header parity (metadata only, no JVM required)
# ---------------------------------------------------------------------------

def test_csv_header_produces_same_mapping_version_in_runner_and_planner() -> None:
    csv_text = _csv_orders_text()
    manifest = build_manifest(payload_format="csv")

    legacy_result = NormalizationRunner().normalize_level1(
        manifest=manifest,
        payload=csv_text,
    )

    fieldnames = ["order_id", "customer_name", "amount", "status"]
    plan = NormalizationPlanner().plan_from_csv_header(
        source_name=manifest.source_name,
        entity_name=manifest.entity_name,
        fieldnames=fieldnames,
    )
    _, spark_mapping_version = plan.build_mapping_catalog()

    assert spark_mapping_version == legacy_result.mapping_version
    assert len(plan.tables) == 1
    assert plan.tables[0].logical_path == "$"
    assert plan.tables[0].physical_table_name == "orders"
    col_pairs = {(m.logical_path, m.physical_name) for m in plan.tables[0].column_mappings}
    expected_cols = {
        ("$.amount", "amount"),
        ("$.customer_name", "customer_name"),
        ("$.order_id", "order_id"),
        ("$.status", "status"),
    }
    assert col_pairs == expected_cols


# ---------------------------------------------------------------------------
# 5. Row-level output parity (Spark-required — needs JVM). DataFrames from
#    SparkRelationalizer are compared to legacy runner rows. Lineage constants
#    (source_name / ingest_date / _run_id) are added by the storage writer,
#    so we compare only the values + _array_index + logical parent FK link.
# ---------------------------------------------------------------------------

def _legacy_rows_by_path(result) -> dict[str, list[dict]]:
    return {t.logical_path: t.rows for t in result.tables}


@pytest.mark.skipif(not _HAS_PYSPARK_JVM, reason="PySpark JVM not available")
def test_spark_relationalizer_row_level_parity_for_3_deep_nested_arrays(
    spark_session_fixture: SparkSession,
) -> None:
    """Contract C1/C3 row-level value equivalence across the entire 4-table
    3-deep-nested-array fixture.

    Verifies for every logical path:
      - identical row counts
      - identical scalar values (stripping lineage constants source/ingest_date/run_id
        which the SparkRelationalizer does NOT add — they're added in
        level2_storage.write_dataframe, so test stops before that layer)
      - identical _array_index values (0..N-1) for child rows
      - FK chain consistency (child _parent_row_id maps to same logical parent
        row that the legacy runner linked to, using the stable sentinel scheme)
    """
    spark: SparkSession = spark_session_fixture
    payload = _nested_orders_payload()
    manifest = build_manifest()

    legacy_result = NormalizationRunner().normalize_level1_json(
        manifest=manifest, payload=payload
    )
    legacy_by_path = _legacy_rows_by_path(legacy_result)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("city", StringType(), True),
                StructField("postcode", StringType(), True),
            ]), True),
        ]), True),
        StructField("items", ArrayType(StructType([
            StructField("sku", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("tags", ArrayType(StringType(), True), True),
            StructField("tax_breakdowns", ArrayType(StructType([
                StructField("jurisdiction", StringType(), True),
                StructField("rate", DoubleType(), True),
                StructField("amount", DoubleType(), True),
                StructField("jurisdictions", ArrayType(StructType([
                    StructField("level", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("allocated", DoubleType(), True),
                ]), True), True),
            ]), True), True),
        ]), True), True),
        StructField("priority_codes", ArrayType(StringType(), True), True),
    ])

    raw_df = spark.createDataFrame(
        [
            {
                "order_id": payload["order_id"],
                "customer": payload["customer"],
                "items": payload["items"],
                "priority_codes": payload["priority_codes"],
            }
        ],
        schema=schema,
    )

    plan = NormalizationPlanner().plan_from_schema(
        source_name=manifest.source_name,
        entity_name=manifest.entity_name,
        schema=schema,
    )
    dfs_by_physical = SparkRelationalizer().execute(raw_df=raw_df, plan=plan)

    plan_by_logical = {t.logical_path: t for t in plan.tables}
    assert plan_by_logical.keys() == _expected_logical_paths()

    for logical_path in _expected_logical_paths():
        planned = plan_by_logical[logical_path]
        legacy_rows = legacy_by_path[logical_path]
        spark_df = dfs_by_physical[planned.physical_table_name]

        cnt = spark_df.count()
        assert cnt == len(legacy_rows), (
            f"Row count mismatch at {logical_path}: spark={cnt} legacy={len(legacy_rows)}"
        )

        _assert_row_value_parity(
            logical_path=logical_path,
            legacy_rows=legacy_rows,
            spark_df=spark_df,
        )


def _assert_row_value_parity(
    *,
    logical_path: str,
    legacy_rows: list[dict],
    spark_df,
) -> None:
    """Per-table assertion helper.

    - Sorts legacy rows deterministically (by _array_index or scalars).
    - Sorts Spark rows identically using the same scalar keys.
    - For each column present in legacy (excluding lineage constants + UUIDs),
      asserts equality of the value at the same rank position.
    - Checks _array_index values are 0..N-1 in both (if present).
    - Checks FK chain consistency by linking stable values.
    """
    def _stable_sort_key(row: dict) -> tuple:
        keys = sorted(k for k in row.keys() if k not in ("_row_id", "_parent_row_id"))
        vals = []
        for k in keys:
            v = row[k]
            if isinstance(v, float):
                vals.append(f"{v:.10f}")
            elif isinstance(v, bool):
                vals.append(str(int(v)))
            else:
                vals.append("" if v is None else str(v))
        return tuple(vals)

    legacy_sorted = sorted(legacy_rows, key=_stable_sort_key)
    non_rowid_cols = [c for c in spark_df.columns if c not in ("_row_id",)]
    spark_rows = [
        r.asDict(recursive=True) for r in spark_df.orderBy(*non_rowid_cols).collect()
    ]
    spark_sorted = sorted(spark_rows, key=_stable_sort_key)

    assert len(legacy_sorted) == len(spark_sorted), (
        f"Post-sort length mismatch {logical_path}: {len(legacy_sorted)} vs {len(spark_sorted)}"
    )

    lineage_constants = {"source_name", "ingest_date", "_run_id"}
    for i, (legacy_row, spark_row) in enumerate(zip(legacy_sorted, spark_sorted, strict=False)):
        for col_name, legacy_val in legacy_row.items():
            if col_name in lineage_constants:
                continue
            if col_name in ("_row_id",):
                continue
            if col_name == "_parent_row_id":
                continue

            spark_val = spark_row.get(col_name)
            if isinstance(legacy_val, float) and isinstance(spark_val, float):
                msg = (
                    f"{logical_path} row[{i}] col={col_name}: "
                    f"float differs {legacy_val} vs {spark_val}"
                )
                assert abs(legacy_val - spark_val) < 1e-9, msg
            else:
                assert spark_val == legacy_val, (
                    f"{logical_path} row[{i}] col={col_name}: "
                    f"legacy={legacy_val!r} spark={spark_val!r}"
                )

        if "_array_index" in legacy_row:
            assert spark_row.get("_array_index") == legacy_row["_array_index"], (
                f"{logical_path} row[{i}] _array_index differs"
            )


# ---------------------------------------------------------------------------
# 6. CSV row-level parity (Spark-required).
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _HAS_PYSPARK_JVM, reason="PySpark JVM not available")
def test_spark_csv_relationalizer_row_level_parity(
    spark_session_fixture: SparkSession,
) -> None:
    spark: SparkSession = spark_session_fixture
    csv_text = _csv_orders_text()
    manifest = build_manifest(payload_format="csv")

    legacy_result = NormalizationRunner().normalize_level1(
        manifest=manifest, payload=csv_text
    )
    legacy_root_rows = legacy_result.tables[0].rows
    assert len(legacy_root_rows) == 3

    fieldnames = ["order_id", "customer_name", "amount", "status"]
    plan = NormalizationPlanner().plan_from_csv_header(
        source_name=manifest.source_name,
        entity_name=manifest.entity_name,
        fieldnames=fieldnames,
    )

    csv_lines = [line for line in csv_text.splitlines() if line.strip()]
    header = csv_lines[0].split(",")
    data_rows = [dict(zip(header, line.split(","), strict=False)) for line in csv_lines[1:]]
    raw_df = spark.createDataFrame(data_rows, schema=StructType([
        StructField(f, StringType(), True) for f in fieldnames
    ]))

    dfs = SparkRelationalizer().execute(raw_df=raw_df, plan=plan)
    root_df = dfs[plan.tables[0].physical_table_name]

    assert root_df.count() == 3

    spark_rows_sorted = sorted(
        (r.asDict() for r in root_df.collect()),
        key=lambda r: r["order_id"],
    )
    legacy_rows_sorted = sorted(
        legacy_root_rows,
        key=lambda r: r["order_id"],
    )

    for legacy_row, spark_row in zip(legacy_rows_sorted, spark_rows_sorted, strict=False):
        for col in ("order_id", "customer_name", "amount", "status"):
            lv = legacy_row.get(col)
            sv = spark_row.get(col)
            assert str(sv) == str(lv), (
                f"CSV column {col} differs: legacy={lv!r} spark={sv!r}"
            )


# ---------------------------------------------------------------------------
# 7. Simple payload (no arrays) parity — sanity quick case.
# ---------------------------------------------------------------------------

def test_flat_payload_metadata_parity() -> None:
    payload = {"a": 1, "b": {"c": "x", "d": True}, "e": None}
    manifest = build_manifest()
    legacy_result = NormalizationRunner().normalize_level1_json(
        manifest=manifest, payload=payload
    )

    schema = StructType([
        StructField("a", LongType(), True),
        StructField("b", StructType([
            StructField("c", StringType(), True),
            StructField("d", StringType(), True),
        ]), True),
        StructField("e", StringType(), True),
    ])
    plan = NormalizationPlanner().plan_from_schema(
        source_name=manifest.source_name, entity_name=manifest.entity_name, schema=schema
    )
    _, spark_version = plan.build_mapping_catalog()
    assert spark_version == legacy_result.mapping_version

    spark_entry = plan.tables[0].to_mapping_entry()
    legacy_entry = legacy_result.mapping_catalog.entries[0]
    assert {(m.logical_path, m.physical_name) for m in spark_entry.column_mappings} == {
        (m.logical_path, m.physical_name) for m in legacy_entry.column_mappings
    }

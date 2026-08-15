from __future__ import annotations

from datetime import UTC, datetime

import pytest
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

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.planner import NormalizationPlanner
from elt_pipeline.normalize.spark_runner import SparkRelationalizer


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
    from elt_pipeline.spark.session import build_spark_session

    session = build_spark_session(app_name="elt-parity-tests", master="local[1]")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# 1. Mapping-version structural validity (metadata only — does NOT require JVM)
# ---------------------------------------------------------------------------

def test_spark_planner_produces_valid_mapping_version_nested_orders() -> None:
    """Structural contract for mapping_version hash (16 hex SHA-256 prefix)."""
    payload = _nested_orders_payload()

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

    assert len(spark_mapping_version) == 16
    assert all(c in "0123456789abcdef" for c in spark_mapping_version)


# ---------------------------------------------------------------------------
# 2. Table-name + column-name structural validity (metadata only, no JVM required)
# ---------------------------------------------------------------------------

def test_spark_planner_produces_expected_table_and_column_names() -> None:
    """Structural contract for logical paths, physical table names, column mappings."""
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
        spark_entry = spark_entries_by_path[logical_path]
        assert spark_entry.physical_table_name is not None
        assert isinstance(spark_entry.join_key_columns, list)
        for col_mapping in spark_entry.column_mappings:
            assert col_mapping.logical_path.startswith(logical_path) or logical_path == "$"


# ---------------------------------------------------------------------------
# 3. Table-name collision policy validity (63-char + hash suffix fallback +
#    collision guard)
# ---------------------------------------------------------------------------

def test_table_name_hash_suffix_applied_for_overflow_and_collision() -> None:
    """Contract: long names → 63-char truncation + SHA-8 suffix.

    Two logical arrays that sanitize to the same base name (e.g. line-items
    and line_items) must produce the same physical names and collision
    behaviour.
    """
    manifest = build_manifest()

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

    li_spark = spark_entries["$.line-items"].physical_table_name
    li2_spark = spark_entries["$.line_items"].physical_table_name

    assert li_spark == "orders__line_items"
    assert li2_spark.startswith("orders__line_items__")
    suffix_sep = "__"
    base_part, _, suffix = li2_spark.rpartition(suffix_sep)
    assert len(suffix) == 8, "SHA-8 suffix expected (8 hex chars)"
    assert all(c in "0123456789abcdef" for c in suffix)


# ---------------------------------------------------------------------------
# 4. CSV header structural validity (metadata only, no JVM required)
# ---------------------------------------------------------------------------

def test_csv_header_produces_valid_plan() -> None:
    csv_text = _csv_orders_text()
    manifest = build_manifest(payload_format="csv")

    fieldnames = ["order_id", "customer_name", "amount", "status"]
    plan = NormalizationPlanner().plan_from_csv_header(
        source_name=manifest.source_name,
        entity_name=manifest.entity_name,
        fieldnames=fieldnames,
    )
    _, spark_mapping_version = plan.build_mapping_catalog()

    assert len(spark_mapping_version) == 16
    assert all(c in "0123456789abcdef" for c in spark_mapping_version)
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
# 5. Row-level output validity (Spark-required — needs JVM).
#    For C3 cutover this asserts known expected row counts and values signed
#    off in Gate C1 parity tests against the legacy runner.
# ---------------------------------------------------------------------------

def _expected_nested_orders_row_counts() -> dict[str, int]:
    return {
        "$": 1,
        "$.items": 2,
        "$.items.tags": 2,
        "$.items.tax_breakdowns": 3,
        "$.items.tax_breakdowns.jurisdictions": 3,
        "$.priority_codes": 2,
    }


def test_spark_relationalizer_row_level_outputs_for_3_deep_nested_arrays(
    spark_session_fixture: SparkSession,
) -> None:
    """Row-level structural assertion against Gate C1 signed-off known values.

    Verifies for every logical path:
      - row counts match the signed-off parity counts
      - scalar values match the known fixture data
      - _array_index values are 0..N-1 for child rows
    """
    spark: SparkSession = spark_session_fixture
    payload = _nested_orders_payload()
    manifest = build_manifest()

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
    expected_counts = _expected_nested_orders_row_counts()

    for logical_path in _expected_logical_paths():
        planned = plan_by_logical[logical_path]
        spark_df = dfs_by_physical[planned.physical_table_name]
        cnt = spark_df.count()
        assert cnt == expected_counts[logical_path], (
            f"Row count mismatch at {logical_path}: spark={cnt} "
            f"expected={expected_counts[logical_path]}"
        )

    _assert_root_table_row_values(dfs_by_physical["orders"])
    _assert_items_row_values(dfs_by_physical["orders__items"])
    _assert_tags_row_values(dfs_by_physical["orders__tags"])
    _assert_tax_row_values(dfs_by_physical["orders__tax_breakdowns"])
    _assert_jurisdictions_row_values(dfs_by_physical["orders__jurisdictions"])
    _assert_priority_codes_row_values(dfs_by_physical["orders__priority_codes"])


def _assert_root_table_row_values(root_df) -> None:
    rows = [r.asDict(recursive=True) for r in root_df.collect()]
    assert len(rows) == 1
    row = rows[0]
    assert row["order_id"] == "ORD-001"
    assert row["customer__id"] == "CUST-42"
    assert row["customer__name"] == "Ada Lovelace"
    assert row["customer__address__city"] == "London"
    assert row["customer__address__postcode"] == "SW1A 1AA"
    assert "_row_id" in row and row["_row_id"] is not None


def _assert_items_row_values(items_df) -> None:
    rows = sorted(
        [r.asDict(recursive=True) for r in items_df.collect()],
        key=lambda r: (r.get("_array_index", -1), r["sku"]),
    )
    assert len(rows) == 2
    r0, r1 = rows
    assert r0["sku"] == "BOOK-ALG-001"
    assert r0["quantity"] == 2
    assert abs(r0["unit_price"] - 39.99) < 1e-9
    assert r0["_array_index"] == 0
    assert r1["sku"] == "JRNL-NB-009"
    assert r1["quantity"] == 1
    assert abs(r1["unit_price"] - 12.50) < 1e-9
    assert r1["_array_index"] == 1


def _assert_tags_row_values(tags_df) -> None:
    rows = sorted(
        [r.asDict(recursive=True) for r in tags_df.collect()],
        key=lambda r: r["value"],
    )
    values = sorted(r["value"] for r in rows)
    assert values == ["algorithm", "hardcover"]
    for r in rows:
        assert r["_array_index"] is not None
        assert r["_row_id"] is not None
        assert r["_parent_row_id"] is not None


def _assert_tax_row_values(tax_df) -> None:
    rows = sorted(
        [r.asDict(recursive=True) for r in tax_df.collect()],
        key=lambda r: (r.get("_array_index", -1), r["jurisdiction"]),
    )
    assert len(rows) == 3
    juris = sorted(r["jurisdiction"] for r in rows)
    assert juris == ["uk_book_levis", "uk_vat", "uk_vat"]
    amounts = sorted(r["amount"] for r in rows)
    assert amounts == [0.0, 2.5, 15.996]


def _assert_jurisdictions_row_values(jur_df) -> None:
    rows = [r.asDict(recursive=True) for r in jur_df.collect()]
    assert len(rows) == 3
    codes = sorted(r["code"] for r in rows)
    assert codes.count("GB") == 2
    assert codes.count("LDN") == 1
    levels = sorted(r["level"] for r in rows)
    assert levels.count("national") == 2
    assert levels.count("city") == 1


def _assert_priority_codes_row_values(pc_df) -> None:
    rows = sorted(
        [r.asDict(recursive=True) for r in pc_df.collect()],
        key=lambda r: r["value"],
    )
    assert [r["value"] for r in rows] == ["EXPEDITE", "GIFT"]


# ---------------------------------------------------------------------------
# 6. CSV row-level output validity (Spark-required).
# ---------------------------------------------------------------------------

def test_spark_csv_relationalizer_row_level_outputs(
    spark_session_fixture: SparkSession,
) -> None:
    spark: SparkSession = spark_session_fixture
    csv_text = _csv_orders_text()
    manifest = build_manifest(payload_format="csv")

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

    expected = [
        {"order_id": "ORD-A", "customer_name": "Ada Lovelace", "amount": "100.50", "status": "open"},
        {"order_id": "ORD-B", "customer_name": "Grace Hopper", "amount": "250.00", "status": "fulfilled"},
        {"order_id": "ORD-C", "customer_name": "Katherine Johnson", "amount": "17.25", "status": "refunded"},
    ]
    for exp, spark_row in zip(expected, spark_rows_sorted, strict=False):
        for col in ("order_id", "customer_name", "amount", "status"):
            assert str(spark_row.get(col)) == str(exp[col]), (
                f"CSV column {col} differs: expected={exp[col]!r} spark={spark_row.get(col)!r}"
            )


# ---------------------------------------------------------------------------
# 7. Flat payload (no arrays) structural validity — sanity quick case.
# ---------------------------------------------------------------------------

def test_flat_payload_metadata_validity() -> None:
    payload = {"a": 1, "b": {"c": "x", "d": True}, "e": None}
    manifest = build_manifest()

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
    assert len(spark_version) == 16
    assert all(c in "0123456789abcdef" for c in spark_version)

    spark_entry = plan.tables[0].to_mapping_entry()
    col_pairs = {(m.logical_path, m.physical_name) for m in spark_entry.column_mappings}
    expected_cols = {
        ("$.a", "a"),
        ("$.b.c", "b__c"),
        ("$.b.d", "b__d"),
        ("$.e", "e"),
    }
    assert col_pairs == expected_cols

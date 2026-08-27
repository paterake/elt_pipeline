from __future__ import annotations

import pytest

from elt_pipeline.shared.governance import (
    DataClassification,
    MaskingStrategy,
    SqlColumnSpec,
)
from elt_pipeline.shared.lineage import (
    OPENLINEAGE_COLUMN_LINEAGE_FACET_SCHEMA_URL,
    OPENLINEAGE_SCHEMA_FACET_SCHEMA_URL,
    build_openlineage_column_lineage_facet,
    build_openlineage_schema_dataset_facet,
)


@pytest.fixture(autouse=True)
def _reset_runtime_context_before_each() -> None:
    from elt_pipeline.config import runtime_context

    runtime_context._reset_for_tests()
    yield
    runtime_context._reset_for_tests()


def test_schema_facet_preserves_known_fields_for_declared_columns() -> None:
    columns = [
        SqlColumnSpec(
            name="customer_id",
            type="bigint",
            nullable=False,
            description="Opaque synthetic customer surrogate key.",
        ),
        SqlColumnSpec(
            name="email_hash",
            type="VARCHAR(64)",
            nullable=True,
            description="SHA-256 digest of normalized lower-case e-mail.",
            classification=DataClassification.restricted_pii,
            masking=MaskingStrategy.nullify,
            custom_tags={"source_system": "crm_sf", "contract.strict": "true"},
        ),
    ]
    facet = build_openlineage_schema_dataset_facet(columns=columns)

    assert facet["_schemaURL"] == OPENLINEAGE_SCHEMA_FACET_SCHEMA_URL
    fields = facet["fields"]
    assert isinstance(fields, list)
    assert len(fields) == 2
    cid = next(f for f in fields if f["name"] == "customer_id")
    assert cid["type"] == "BIGINT"
    assert cid["nullable"] is False
    assert cid["description"] == "Opaque synthetic customer surrogate key."
    assert "tags" not in cid

    email = next(f for f in fields if f["name"] == "email_hash")
    assert email["type"] == "VARCHAR(64)"
    assert email["nullable"] is True
    tags = sorted(email["tags"])
    assert "classification.restricted_pii" in tags
    assert "contract.strict=true" in tags
    assert "source_system=crm_sf" in tags


def test_schema_facet_handles_missing_types_with_fallback() -> None:
    columns = [
        SqlColumnSpec(name="a", type=None),
        SqlColumnSpec(name="c", type=" decimal(38, 18) "),
    ]
    facet = build_openlineage_schema_dataset_facet(columns=columns)
    fields = {f["name"]: f for f in facet["fields"]}
    assert fields["a"]["type"] == "UNKNOWN"
    assert fields["c"]["type"] == "DECIMAL(38,18)"


def test_schema_facet_handles_empty_column_list() -> None:
    facet = build_openlineage_schema_dataset_facet(columns=[])
    assert facet["_schemaURL"] == OPENLINEAGE_SCHEMA_FACET_SCHEMA_URL
    assert facet["fields"] == []


def test_column_lineage_facet_round_trips_input_references() -> None:
    column_lineage_map = {
        "order_total": [
            ("spark_parquet:orders_staging", "subtotal"),
            ("spark_parquet:orders_staging", "tax_amount"),
            ("spark_parquet:orders_staging", "shipping_fee"),
        ],
        "customer_email_hash": [
            ("iceberg:raw.crm.customer", "email_hash"),
        ],
        "ingested_at": [],
    }
    facet = build_openlineage_column_lineage_facet(column_lineage_map=column_lineage_map)

    assert facet["_schemaURL"] == OPENLINEAGE_COLUMN_LINEAGE_FACET_SCHEMA_URL
    fields = facet["fields"]
    assert set(fields.keys()) == {
        "customer_email_hash",
        "ingested_at",
        "order_total",
    }
    order_total = fields["order_total"]
    assert order_total["transformationType"] == "DIRECT"
    refs = sorted(
        (inp["namespace"], inp["name"], inp["field"])
        for inp in order_total["inputFields"]
    )
    assert refs == [
        ("elt_pipeline", "spark_parquet:orders_staging", "shipping_fee"),
        ("elt_pipeline", "spark_parquet:orders_staging", "subtotal"),
        ("elt_pipeline", "spark_parquet:orders_staging", "tax_amount"),
    ]
    literal = fields["ingested_at"]
    assert literal["inputFields"] == []
    assert literal["transformationType"] == "LITERAL"


def test_column_lineage_extraction_simple_identity(spark_session) -> None:
    from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe

    rows = [(1, "alpha"), (2, "beta")]
    input_df = spark_session.createDataFrame(rows, schema=["id", "name"])
    input_df.createOrReplaceTempView("in_identity")
    select_df = spark_session.sql("SELECT id, name FROM in_identity")

    mapping = extract_column_lineage_from_dataframe(
        select_df, input_datasets_by_alias={"in_identity": "staging:users"}
    )
    assert set(mapping.keys()) == {"id", "name"}
    assert mapping["id"] == [("staging:users", "id")]
    assert mapping["name"] == [("staging:users", "name")]


def test_column_lineage_extraction_concat_computed(spark_session) -> None:
    from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe

    rows = [("Ada", "Lovelace"), ("Grace", "Hopper")]
    df = spark_session.createDataFrame(rows, schema=["first", "last"])
    df.createOrReplaceTempView("people")
    result = spark_session.sql(
        "SELECT first, last, CONCAT(first, ' ', last) AS full_name FROM people"
    )
    mapping = extract_column_lineage_from_dataframe(
        result,
        input_datasets_by_alias={"people": "raw.staff.people"},
    )
    assert "first" in mapping
    assert "last" in mapping
    assert "full_name" in mapping
    sources = set(mapping["full_name"])
    assert ("raw.staff.people", "first") in sources
    assert ("raw.staff.people", "last") in sources


def test_column_lineage_extraction_group_by_aggregation(spark_session) -> None:
    from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe

    rows = [
        ("uk", "alpha", 10.0),
        ("uk", "beta", 20.0),
        ("us", "gamma", 30.0),
    ]
    df = spark_session.createDataFrame(
        rows, schema=["country", "sku", "amount"]
    )
    df.createOrReplaceTempView("sales")
    result = spark_session.sql(
        "SELECT country, COUNT(*) AS order_count, SUM(amount) AS revenue "
        "FROM sales GROUP BY country"
    )
    mapping = extract_column_lineage_from_dataframe(
        result,
        input_datasets_by_alias={"sales": "staging.sales_raw"},
    )
    assert mapping["country"] == [("staging.sales_raw", "country")]
    revenue_refs = set(mapping["revenue"])
    assert ("staging.sales_raw", "amount") in revenue_refs
    # COUNT(*) has no column reference in most Spark plans; accept any
    # deterministic result for order_count
    assert isinstance(mapping["order_count"], list)


def test_column_lineage_extraction_join_cross_table(spark_session) -> None:
    from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe

    customers = spark_session.createDataFrame(
        [(1, "Ada"), (2, "Grace")], schema=["customer_id", "customer_name"]
    )
    orders = spark_session.createDataFrame(
        [(1, 1, 99.99), (2, 2, 129.50)],
        schema=["order_id", "customer_id", "order_total"],
    )
    customers.createOrReplaceTempView("cust")
    orders.createOrReplaceTempView("ord")
    result = spark_session.sql(
        "SELECT c.customer_id, c.customer_name, o.order_id, o.order_total "
        "FROM cust AS c INNER JOIN ord AS o ON c.customer_id = o.customer_id"
    )
    mapping = extract_column_lineage_from_dataframe(
        result,
        input_datasets_by_alias={
            "c": "L2.customers",
            "cust": "L2.customers",
            "o": "L2.orders",
            "ord": "L2.orders",
        },
    )
    assert set(mapping.keys()) == {
        "customer_id",
        "customer_name",
        "order_id",
        "order_total",
    }
    customer_name_sources = set(mapping["customer_name"])
    order_total_sources = set(mapping["order_total"])
    assert ("L2.customers", "customer_name") in customer_name_sources
    assert ("L2.orders", "order_total") in order_total_sources
    # customer_id can resolve to either side of the equi-join — either is correct
    customer_id_sources = set(mapping["customer_id"])
    assert (
        ("L2.customers", "customer_id") in customer_id_sources
        or ("L2.orders", "customer_id") in customer_id_sources
    )


def test_column_lineage_extraction_unknown_alias_is_silent_no_crash(
    spark_session,
) -> None:
    from elt_pipeline.sql._column_lineage import extract_column_lineage_from_dataframe

    rows = [(7, "x")]
    df = spark_session.createDataFrame(rows, schema=["a", "b"])
    df.createOrReplaceTempView("only_in_sql")
    result = spark_session.sql("SELECT a, b, LOWER(b) AS b_lc FROM only_in_sql")
    # No alias registered at all — extraction MUST NOT crash, it just drops refs
    mapping = extract_column_lineage_from_dataframe(
        result, input_datasets_by_alias={}
    )
    assert set(mapping.keys()) == {"a", "b", "b_lc"}
    assert mapping["a"] == []
    assert mapping["b"] == []
    assert mapping["b_lc"] == []

import json
from datetime import UTC, datetime
from pathlib import Path

from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.normalize.runner import NormalizationRunner
from elt_pipeline.normalize.storage import LocalMappingCatalogStore


def build_manifest(*, entity_name: str = "orders") -> Level1ArtifactManifest:
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
        payload_format="json",
        content_hash="abc123",
        file_size_bytes=128,
        data_path="level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/orders.json",
        manifest_path="level1/environment=dev/source=rest_source/entity=orders/run_id=run-001/orders.json.manifest.json",
    )


def test_normalization_runner_flattens_nested_payloads_into_parent_child_tables() -> None:
    runner = NormalizationRunner()
    payload = {
        "order_id": "A-100",
        "customer": {"name": "Ada", "address": {"city": "London"}},
        "items": [
            {"sku": "SKU-1", "quantity": 2, "discounts": ["PROMO10", "VIP"]},
            {"sku": "SKU-2", "quantity": 1},
        ],
        "tags": ["priority", "gift"],
    }

    result = runner.normalize_level1_json(manifest=build_manifest(), payload=payload)
    tables = {table.logical_path: table for table in result.tables}

    root_row = tables["$"].rows[0]
    assert root_row["order_id"] == "A-100"
    assert root_row["customer__name"] == "Ada"
    assert root_row["customer__address__city"] == "London"
    assert "_parent_row_id" not in root_row

    item_rows = tables["$.items"].rows
    assert [row["_array_index"] for row in item_rows] == [0, 1]
    assert all(row["_parent_row_id"] == root_row["_row_id"] for row in item_rows)
    assert item_rows[0]["sku"] == "SKU-1"
    assert item_rows[0]["quantity"] == 2
    assert item_rows[1]["sku"] == "SKU-2"

    discount_rows = tables["$.items.discounts"].rows
    assert [row["value"] for row in discount_rows] == ["PROMO10", "VIP"]
    assert all(row["_parent_row_id"] == item_rows[0]["_row_id"] for row in discount_rows)

    tag_rows = tables["$.tags"].rows
    assert [row["value"] for row in tag_rows] == ["priority", "gift"]
    assert all(row["_parent_row_id"] == root_row["_row_id"] for row in tag_rows)

    catalog_entries = {entry.logical_path: entry for entry in result.mapping_catalog.entries}
    assert catalog_entries["$"].physical_table_name == "orders"
    assert catalog_entries["$.items"].physical_table_name == "orders__items"
    assert catalog_entries["$.items"].join_key_columns == ["_parent_row_id"]
    assert catalog_entries["$.tags"].column_mappings[0].physical_name == "value"


def test_mapping_version_is_structure_based_and_catalog_can_be_persisted(tmp_path: Path) -> None:
    runner = NormalizationRunner()
    first_payload = {
        "order_id": "A-100",
        "customer": {"name": "Ada"},
        "items": [{"sku": "SKU-1", "quantity": 2}],
    }
    second_payload = {
        "order_id": "A-200",
        "customer": {"name": "Grace"},
        "items": [{"sku": "SKU-9", "quantity": 4}],
    }

    first_result = runner.normalize_level1_json(manifest=build_manifest(), payload=first_payload)
    second_result = runner.normalize_level1_json(manifest=build_manifest(), payload=second_payload)

    assert first_result.mapping_version == second_result.mapping_version

    store = LocalMappingCatalogStore(tmp_path)
    catalog_path = store.write_catalog(first_result.mapping_catalog)
    stored_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))

    assert catalog_path.exists()
    assert f"mapping_version={first_result.mapping_version}" in str(catalog_path)
    assert stored_catalog["mapping_version"] == first_result.mapping_version
    assert stored_catalog["root_table_name"] == "orders"


def test_table_name_hash_fallback_only_applies_when_sanitized_names_collide() -> None:
    runner = NormalizationRunner()
    payload = {
        "line-items": [{"sku": "SKU-1"}],
        "line_items": [{"sku": "SKU-2"}],
    }

    result = runner.normalize_level1_json(manifest=build_manifest(), payload=payload)
    catalog_entries = {entry.logical_path: entry for entry in result.mapping_catalog.entries}

    assert catalog_entries["$.line-items"].physical_table_name == "orders__line_items"
    assert catalog_entries["$.line_items"].physical_table_name.startswith("orders__line_items__")
    assert catalog_entries["$.line_items"].physical_table_name != "orders__line_items"

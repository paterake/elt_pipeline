from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from elt_pipeline.config.loader import load_pipeline_config, resolve_entity_config


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIGS = {
    "examples/configs/local_object_storage_orders.yaml": ("local_files", "orders"),
    "examples/configs/local_object_storage_orders_csv_bypass.yaml": (
        "local_files",
        "orders",
    ),
    "examples/configs/local_sqlite_orders_delta.yaml": ("orders_db", "orders"),
    "examples/configs/local_kafka_orders_replay.yaml": ("events", "orders"),
    "examples/configs/local_rest_orders.yaml": ("local_api", "orders"),
}


def test_example_configs_load_and_resolve() -> None:
    for relative_path, (source_name, entity_name) in EXAMPLE_CONFIGS.items():
        config = load_pipeline_config(REPO_ROOT / relative_path)
        resolved = resolve_entity_config(
            config,
            environment="default",
            source_name=source_name,
            entity_name=entity_name,
        )
        assert resolved.environment == "default"
        assert resolved.source_name == source_name
        assert resolved.entity_name == entity_name


def test_object_storage_example_runs_ingest_and_normalize(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_object_storage_orders.yaml"

    ingest_result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    ingest_payload = json.loads(ingest_result.stdout)
    assert ingest_payload["result_count"] == 1
    assert ingest_payload["results"][0]["connector_type"] == "object_storage"

    normalize_result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    normalize_payload = json.loads(normalize_result.stdout)
    assert normalize_payload["processed_count"] == 1
    assert normalize_payload["results"][0]["bypassed"] is False
    assert normalize_payload["results"][0]["table_manifests"]


def test_object_storage_csv_bypass_example_runs(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_object_storage_orders_csv_bypass.yaml"

    _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    normalize_result = _run_cli(
        [
            "normalize",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    payload = json.loads(normalize_result.stdout)
    assert payload["results"][0]["bypassed"] is True
    assert payload["results"][0]["level2_mode"] == "bypass_level2"


def test_kafka_example_runs_ingest(tmp_path: Path) -> None:
    config_path = REPO_ROOT / "examples/configs/local_kafka_orders_replay.yaml"

    result = _run_cli(
        [
            "ingest",
            "run",
            str(config_path),
            "--root-path",
            str(tmp_path),
        ]
    )
    payload = json.loads(result.stdout)
    assert payload["result_count"] == 1
    assert payload["results"][0]["connector_type"] == "kafka"
    assert payload["results"][0]["result"]["message_count"] == 2


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "elt_pipeline", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

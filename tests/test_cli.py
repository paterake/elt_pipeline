import json
import subprocess
import sys
from pathlib import Path
from textwrap import dedent


def write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        dedent(
            """
        schema_version: v1
        environments:
          default:
            defaults: {}
        sources:
          - name: rest_source
            connector_type: rest
            entities:
              - name: orders
            """
        ).strip(),
        encoding="utf-8",
    )
    return config_path


def test_validate_config_command(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "validate-config",
            str(config_path),
            "--source",
            "rest_source",
            "--entity",
            "orders",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["connector_type"] == "rest"
    assert payload["entity_name"] == "orders"


def test_show_run_context_command() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "show-run-context",
            "--stage",
            "ingest",
            "--job-name",
            "demo-job",
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage"] == "ingest"
    assert payload["job_name"] == "demo-job"

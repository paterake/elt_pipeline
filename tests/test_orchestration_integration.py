from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from elt_pipeline.integrations import (
    CliInvocationRequest,
    OrchestrationMetadata,
    SubprocessCliInvoker,
    load_orchestration_metadata_from_env,
)
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_load_orchestration_metadata_from_env_parses_supported_fields() -> None:
    metadata = load_orchestration_metadata_from_env(
        {
            "ELT_PIPELINE_ORCHESTRATION_PLATFORM": "airflow",
            "ELT_PIPELINE_ORCHESTRATION_FLOW_NAME": "daily_orders",
            "ELT_PIPELINE_ORCHESTRATION_FLOW_RUN_ID": "manual__2026-06-14T00:00:00+00:00",
            "ELT_PIPELINE_ORCHESTRATION_TASK_NAME": "publish_level5",
            "ELT_PIPELINE_ORCHESTRATION_TASK_ATTEMPT": "2",
            "ELT_PIPELINE_ORCHESTRATION_TAGS_JSON": json.dumps(
                {"deployment": "local", "team": "platform"}
            ),
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="airflow",
        flow_name="daily_orders",
        flow_run_id="manual__2026-06-14T00:00:00+00:00",
        task_name="publish_level5",
        task_attempt=2,
        tags={"deployment": "local", "team": "platform"},
    )
    assert metadata.to_run_attributes()["orchestration_platform"] == "airflow"


def test_load_orchestration_metadata_from_env_requires_platform() -> None:
    with pytest.raises(ConfigValidationError) as exc_info:
        load_orchestration_metadata_from_env(
            {"ELT_PIPELINE_ORCHESTRATION_TASK_NAME": "normalize_orders"}
        )

    assert "ELT_PIPELINE_ORCHESTRATION_PLATFORM" in str(exc_info.value)


def test_subprocess_cli_invoker_uses_python_module_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def _fake_run(
        args,
        *,
        cwd,
        env,
        capture_output,
        text,
        check,
        timeout,
    ):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["env"] = env
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["check"] = check
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout='{"status":"ok"}',
            stderr="",
        )

    monkeypatch.setattr("elt_pipeline.integrations.orchestration.subprocess.run", _fake_run)

    request = CliInvocationRequest(
        subcommand=("publish", "explain"),
        arguments=("--environment", "default", "--job-name", "publish-orchestrated"),
        cwd=tmp_path,
        environment_overrides={"EXTRA_FLAG": "1"},
        orchestration_metadata=OrchestrationMetadata(
            platform="dagster",
            flow_name="publish_assets",
            task_name="publish_customers",
            task_attempt=3,
        ),
    )
    result = SubprocessCliInvoker().invoke(request, timeout_seconds=5.0)

    assert result.succeeded is True
    assert captured["args"] == [
        sys.executable,
        "-m",
        "elt_pipeline",
        "publish",
        "explain",
        "--environment",
        "default",
        "--job-name",
        "publish-orchestrated",
    ]
    assert captured["cwd"] == tmp_path
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["check"] is False
    assert captured["timeout"] == 5.0
    assert captured["env"]["ELT_PIPELINE_ORCHESTRATION_PLATFORM"] == "dagster"
    assert captured["env"]["ELT_PIPELINE_ORCHESTRATION_TASK_ATTEMPT"] == "3"
    assert captured["env"]["EXTRA_FLAG"] == "1"


def test_cli_invocation_result_raise_for_exit_code_raises_pipeline_error() -> None:
    result = SubprocessCliInvoker()
    request = CliInvocationRequest(subcommand=("show-run-context",), arguments=("--help",))

    invocation = result.invoke(request, timeout_seconds=5.0)

    assert invocation.succeeded is True

    failing = invocation.__class__(
        argv=invocation.argv,
        cwd=invocation.cwd,
        exit_code=2,
        stdout="",
        stderr="boom",
    )

    with pytest.raises(PipelineError) as exc_info:
        failing.raise_for_exit_code()

    assert exc_info.value.error_code == "ORCHESTRATION_WRAPPER_INVOCATION_FAILED"


def test_show_run_context_includes_orchestration_metadata_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELT_PIPELINE_ORCHESTRATION_PLATFORM", "prefect")
    monkeypatch.setenv("ELT_PIPELINE_ORCHESTRATION_FLOW_NAME", "nightly-normalize")
    monkeypatch.setenv("ELT_PIPELINE_ORCHESTRATION_TASK_NAME", "normalize-orders")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "elt_pipeline",
            "show-run-context",
            "--stage",
            "normalize",
            "--job-name",
            "normalize-orders",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["attributes"]["orchestration_platform"] == "prefect"
    assert payload["attributes"]["orchestration_flow_name"] == "nightly-normalize"
    assert payload["attributes"]["orchestration_task_name"] == "normalize-orders"

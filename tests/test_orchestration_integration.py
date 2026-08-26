from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from elt_pipeline.integrations import (
    AirflowCliWrapper,
    CliInvocationRequest,
    CliInvocationResult,
    DagsterCliWrapper,
    MageCliWrapper,
    OrchestrationMetadata,
    PrefectCliWrapper,
    SubprocessCliInvoker,
    build_airflow_orchestration_metadata,
    build_dagster_orchestration_metadata,
    build_mage_orchestration_metadata,
    build_prefect_orchestration_metadata,
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


def test_orchestration_metadata_normalizes_direct_constructor_values() -> None:
    metadata = OrchestrationMetadata(
        platform=" airflow ",
        flow_name=" nightly ",
        flow_run_id=" run-001 ",
        task_name=" normalize_orders ",
        task_attempt=2,
        tags={" team ": " platform "},
    )

    assert metadata == OrchestrationMetadata(
        platform="airflow",
        flow_name="nightly",
        flow_run_id="run-001",
        task_name="normalize_orders",
        task_attempt=2,
        tags={"team": "platform"},
    )


def test_orchestration_metadata_rejects_invalid_direct_constructor_values() -> None:
    with pytest.raises(ConfigValidationError, match="must not be empty"):
        OrchestrationMetadata(platform="   ")

    with pytest.raises(ConfigValidationError, match="greater than or equal to 1"):
        OrchestrationMetadata(platform="airflow", task_attempt=0)

    with pytest.raises(
        ConfigValidationError,
        match="must contain non-empty string keys and values",
    ):
        OrchestrationMetadata(platform="airflow", tags={"": "platform"})


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


def test_build_airflow_orchestration_metadata_maps_airflow_context() -> None:
    metadata = build_airflow_orchestration_metadata(
        {
            "dag": SimpleNamespace(dag_id="elt_pipeline_daily", tags=["finance", "nightly"]),
            "dag_run": SimpleNamespace(run_id="manual__2026-06-14T00:00:00+00:00"),
            "task_instance": SimpleNamespace(task_id="publish_level5", try_number=4),
            "logical_date": datetime(2026, 6, 14, tzinfo=UTC),
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="airflow",
        flow_name="elt_pipeline_daily",
        flow_run_id="manual__2026-06-14T00:00:00+00:00",
        task_name="publish_level5",
        task_attempt=4,
        tags={
            "dag_tags": "finance,nightly",
            "logical_date": "2026-06-14T00:00:00+00:00",
        },
    )


def test_airflow_cli_wrapper_build_request_uses_repo_root_and_airflow_metadata() -> None:
    wrapper = AirflowCliWrapper(repo_root=REPO_ROOT)

    request = wrapper.build_request(
        subcommand=("publish", "run"),
        arguments=(
            "/tmp/publish-package",
            "--database",
            "/tmp/demo.db",
        ),
        airflow_context={
            "dag_id": "daily_platform",
            "run_id": "scheduled__2026-06-14",
            "task": SimpleNamespace(task_id="publish_customers"),
            "ti": SimpleNamespace(try_number=2),
        },
        environment_overrides={"EXTRA_FLAG": "1"},
    )

    assert request.cwd == REPO_ROOT
    assert request.orchestration_metadata == OrchestrationMetadata(
        platform="airflow",
        flow_name="daily_platform",
        flow_run_id="scheduled__2026-06-14",
        task_name="publish_customers",
        task_attempt=2,
        tags={},
    )
    assert request.environment_overrides["EXTRA_FLAG"] == "1"
    assert request.argv()[3:] == (
        "publish",
        "run",
        "/tmp/publish-package",
        "--database",
        "/tmp/demo.db",
    )


def test_airflow_cli_wrapper_invokes_show_run_context_end_to_end() -> None:
    result = AirflowCliWrapper(repo_root=REPO_ROOT).invoke(
        subcommand=("show-run-context",),
        arguments=(
            "--stage",
            "publish",
            "--job-name",
            "publish-orders",
        ),
        airflow_context={
            "dag_id": "elt_pipeline_daily",
            "run_id": "scheduled__2026-06-14",
            "task_instance": SimpleNamespace(task_id="publish_orders", try_number=3),
        },
        timeout_seconds=10.0,
    )

    payload = json.loads(result.stdout)
    assert payload["attributes"]["orchestration_platform"] == "airflow"
    assert payload["attributes"]["orchestration_flow_name"] == "elt_pipeline_daily"
    assert payload["attributes"]["orchestration_flow_run_id"] == "scheduled__2026-06-14"
    assert payload["attributes"]["orchestration_task_name"] == "publish_orders"
    assert payload["attributes"]["orchestration_task_attempt"] == 3


def test_build_dagster_orchestration_metadata_maps_dagster_context() -> None:
    metadata = build_dagster_orchestration_metadata(
        {
            "job": SimpleNamespace(name="elt_pipeline_assets"),
            "run_id": "4f7c4a9e-1b2d-4c8a-9e6f-3a7b2c1d0e5f",
            "op": SimpleNamespace(name="normalize_orders"),
            "retry_number": 2,
            "tags": ["finance", "backfill"],
            "partition_key": "2026-06-14",
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="dagster",
        flow_name="elt_pipeline_assets",
        flow_run_id="4f7c4a9e-1b2d-4c8a-9e6f-3a7b2c1d0e5f",
        task_name="normalize_orders",
        task_attempt=2,
        tags={
            "run_tags": "finance,backfill",
            "partition_key": "2026-06-14",
        },
    )


def test_build_dagster_orchestration_metadata_explicit_overrides() -> None:
    metadata = build_dagster_orchestration_metadata(
        {
            "job_name": "explicit_job",
            "run_id": "run-001",
            "op_name": "explicit_op",
            "retry_number": 1,
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="dagster",
        flow_name="explicit_job",
        flow_run_id="run-001",
        task_name="explicit_op",
        task_attempt=1,
        tags={},
    )


def test_dagster_cli_wrapper_build_request() -> None:
    wrapper = DagsterCliWrapper(repo_root=REPO_ROOT)

    request = wrapper.build_request(
        subcommand=("normalize", "run"),
        arguments=(
            "--environment",
            "staging",
            "--source",
            "orders",
        ),
        dagster_context={
            "job_name": "dagster_elt",
            "run_id": "dagster-run-42",
            "op": SimpleNamespace(name="normalize_step"),
            "retry_number": 3,
            "tags": ["data-team"],
            "partition_key": "2026-06-15",
        },
        environment_overrides={"EXTRA_FLAG": "1"},
    )

    assert request.cwd == REPO_ROOT
    assert request.orchestration_metadata == OrchestrationMetadata(
        platform="dagster",
        flow_name="dagster_elt",
        flow_run_id="dagster-run-42",
        task_name="normalize_step",
        task_attempt=3,
        tags={
            "run_tags": "data-team",
            "partition_key": "2026-06-15",
        },
    )
    assert request.environment_overrides["EXTRA_FLAG"] == "1"
    assert request.argv()[3:] == (
        "normalize",
        "run",
        "--environment",
        "staging",
        "--source",
        "orders",
    )


def test_dagster_cli_wrapper_invokes_show_run_context_end_to_end() -> None:
    result = DagsterCliWrapper(repo_root=REPO_ROOT).invoke(
        subcommand=("show-run-context",),
        arguments=(
            "--stage",
            "ingest",
            "--job-name",
            "ingest-customers",
        ),
        dagster_context={
            "job_name": "dagster_daily",
            "run_id": "scheduled__2026-06-15",
            "op_name": "ingest_op",
            "retry_number": 2,
        },
        timeout_seconds=10.0,
    )

    payload = json.loads(result.stdout)
    assert payload["attributes"]["orchestration_platform"] == "dagster"
    assert payload["attributes"]["orchestration_flow_name"] == "dagster_daily"
    assert payload["attributes"]["orchestration_flow_run_id"] == "scheduled__2026-06-15"
    assert payload["attributes"]["orchestration_task_name"] == "ingest_op"
    assert payload["attributes"]["orchestration_task_attempt"] == 2


def test_build_prefect_orchestration_metadata_maps_prefect_context() -> None:
    metadata = build_prefect_orchestration_metadata(
        {
            "flow": SimpleNamespace(name="elt_pipeline_flow", tags=["marketing", "hourly"]),
            "flow_run": SimpleNamespace(id="prefect-flow-run-a1b2c3"),
            "task_run": SimpleNamespace(task_key="run_sql_step", id="prefect-task-xyz789"),
            "task_run_count": 2,
            "scheduled_start_time": "2026-06-15T03:00:00+00:00",
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="prefect",
        flow_name="elt_pipeline_flow",
        flow_run_id="prefect-flow-run-a1b2c3",
        task_name="run_sql_step",
        task_attempt=2,
        tags={
            "flow_tags": "marketing,hourly",
            "task_run_id": "prefect-task-xyz789",
            "scheduled_start_time": "2026-06-15T03:00:00+00:00",
        },
    )


def test_build_prefect_orchestration_metadata_explicit_overrides() -> None:
    metadata = build_prefect_orchestration_metadata(
        {
            "flow_name": "explicit_flow",
            "flow_run_id": "frun-001",
            "task_name": "explicit_task",
            "run_count": 1,
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="prefect",
        flow_name="explicit_flow",
        flow_run_id="frun-001",
        task_name="explicit_task",
        task_attempt=1,
        tags={},
    )


def test_build_prefect_orchestration_metadata_run_count_fallback() -> None:
    metadata = build_prefect_orchestration_metadata(
        {
            "flow_name": "fallback_flow",
            "run_count": 5,
        }
    )

    assert metadata.task_attempt == 5


def test_prefect_cli_wrapper_build_request() -> None:
    wrapper = PrefectCliWrapper(repo_root=REPO_ROOT)

    request = wrapper.build_request(
        subcommand=("sql", "run"),
        arguments=("--models", "orders_daily,customers_daily"),
        prefect_context={
            "flow_name": "prefect_elt",
            "flow_run_id": "prefect-fr-100",
            "task_run": SimpleNamespace(task_key="sql_exec_step", id="prefect-tr-500"),
            "task_run_count": 4,
            "tags": ["core"],
        },
        environment_overrides={"EXTRA_FLAG": "1"},
    )

    assert request.cwd == REPO_ROOT
    assert request.orchestration_metadata == OrchestrationMetadata(
        platform="prefect",
        flow_name="prefect_elt",
        flow_run_id="prefect-fr-100",
        task_name="sql_exec_step",
        task_attempt=4,
        tags={
            "flow_tags": "core",
            "task_run_id": "prefect-tr-500",
        },
    )
    assert request.environment_overrides["EXTRA_FLAG"] == "1"
    assert request.argv()[3:] == (
        "sql",
        "run",
        "--models",
        "orders_daily,customers_daily",
    )


def test_prefect_cli_wrapper_invokes_show_run_context_end_to_end() -> None:
    result = PrefectCliWrapper(repo_root=REPO_ROOT).invoke(
        subcommand=("show-run-context",),
        arguments=(
            "--stage",
            "sql",
            "--job-name",
            "sql-models",
        ),
        prefect_context={
            "flow_name": "prefect_daily",
            "flow_run_id": "prefect-scheduled-2026-06-15",
            "task_run": SimpleNamespace(task_key="models_step"),
            "task_run_count": 1,
        },
        timeout_seconds=10.0,
    )

    payload = json.loads(result.stdout)
    assert payload["attributes"]["orchestration_platform"] == "prefect"
    assert payload["attributes"]["orchestration_flow_name"] == "prefect_daily"
    assert payload["attributes"]["orchestration_flow_run_id"] == "prefect-scheduled-2026-06-15"
    assert payload["attributes"]["orchestration_task_name"] == "models_step"
    assert payload["attributes"]["orchestration_task_attempt"] == 1


def test_build_mage_orchestration_metadata_maps_mage_context_all_6_fields() -> None:
    metadata = build_mage_orchestration_metadata(
        {
            "pipeline_name": "elt_pipeline_full",
            "run_id": "mage-run-uuid-a1b2c3d4",
            "block_uuid": "ingest_orders_block",
            "block_attempt": 2,
            "tags": ["finance", "daily", "orders"],
            "execution_date": "2026-06-16T00:00:00+00:00",
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="mage",
        flow_name="elt_pipeline_full",
        flow_run_id="mage-run-uuid-a1b2c3d4",
        task_name="ingest_orders_block",
        task_attempt=2,
        tags={
            "mage_pipeline_tags": "finance,daily,orders",
            "execution_date": "2026-06-16T00:00:00+00:00",
        },
    )


def test_build_mage_orchestration_metadata_explicit_overrides() -> None:
    metadata = build_mage_orchestration_metadata(
        {
            "pipeline_name": "explicit_mage_pipeline",
            "run_id": "explicit-run-001",
            "block_uuid": "explicit_block",
            "block_attempt": 1,
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="mage",
        flow_name="explicit_mage_pipeline",
        flow_run_id="explicit-run-001",
        task_name="explicit_block",
        task_attempt=1,
        tags={},
    )


def test_mage_cli_wrapper_build_request() -> None:
    wrapper = MageCliWrapper(repo_root=REPO_ROOT)

    request = wrapper.build_request(
        subcommand=("ingest", "run"),
        arguments=(
            "--config-path",
            "/tmp/config.yaml",
            "--environment",
            "prod",
        ),
        mage_context={
            "pipeline_name": "mage_daily_elt",
            "run_id": "mage-scheduled-2026-06-16",
            "block_uuid": "ingest_customers",
            "block_attempt": 3,
            "tags": ["core-data"],
            "execution_date": "2026-06-16",
        },
        environment_overrides={"EXTRA_FLAG": "1"},
    )

    assert request.cwd == REPO_ROOT
    assert request.orchestration_metadata == OrchestrationMetadata(
        platform="mage",
        flow_name="mage_daily_elt",
        flow_run_id="mage-scheduled-2026-06-16",
        task_name="ingest_customers",
        task_attempt=3,
        tags={
            "mage_pipeline_tags": "core-data",
            "execution_date": "2026-06-16",
        },
    )
    assert request.environment_overrides["EXTRA_FLAG"] == "1"
    assert request.argv()[3:] == (
        "ingest",
        "run",
        "--config-path",
        "/tmp/config.yaml",
        "--environment",
        "prod",
    )


def test_mage_cli_wrapper_invokes_via_invoker(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeInvoker:
        def invoke(self, request, *, timeout_seconds=None):
            captured["request"] = request
            captured["timeout_seconds"] = timeout_seconds
            return CliInvocationResult(
                argv=request.argv(),
                cwd=request.cwd,
                exit_code=0,
                stdout='{"ok":true}',
                stderr="",
            )

    wrapper = MageCliWrapper(repo_root=REPO_ROOT, invoker=FakeInvoker())
    result = wrapper.invoke(
        subcommand=("show-run-context",),
        arguments=("--stage", "ingest"),
        mage_context={
            "pipeline_name": "fake_pipeline",
            "run_id": "fake-run-42",
            "block_uuid": "show_ctx_block",
            "block_attempt": 1,
        },
        timeout_seconds=15.0,
        check=False,
    )

    assert result.succeeded is True
    assert captured["timeout_seconds"] == 15.0
    req = captured["request"]
    assert isinstance(req, CliInvocationRequest)
    assert req.orchestration_metadata.platform == "mage"
    assert req.orchestration_metadata.flow_name == "fake_pipeline"


def test_mage_orchestration_metadata_to_env_roundtrip_all_6_fields() -> None:
    metadata = OrchestrationMetadata(
        platform="mage",
        flow_name="daily_sales_pipeline",
        flow_run_id="mage-run-abc123xyz",
        task_name="transform_sales_block",
        task_attempt=4,
        tags={
            "mage_pipeline_tags": "sales,revenue",
            "execution_date": "2026-06-16T08:00:00+00:00",
        },
    )

    env = metadata.to_env()
    assert env["ELT_PIPELINE_ORCHESTRATION_PLATFORM"] == "mage"
    assert env["ELT_PIPELINE_ORCHESTRATION_FLOW_NAME"] == "daily_sales_pipeline"
    assert env["ELT_PIPELINE_ORCHESTRATION_FLOW_RUN_ID"] == "mage-run-abc123xyz"
    assert env["ELT_PIPELINE_ORCHESTRATION_TASK_NAME"] == "transform_sales_block"
    assert env["ELT_PIPELINE_ORCHESTRATION_TASK_ATTEMPT"] == "4"

    tags_from_env = json.loads(env["ELT_PIPELINE_ORCHESTRATION_TAGS_JSON"])
    assert tags_from_env["mage_pipeline_tags"] == "sales,revenue"
    assert tags_from_env["execution_date"] == "2026-06-16T08:00:00+00:00"

    roundtripped = load_orchestration_metadata_from_env(env)
    assert roundtripped == metadata


def test_load_orchestration_metadata_from_env_mage_platform() -> None:
    metadata = load_orchestration_metadata_from_env(
        {
            "ELT_PIPELINE_ORCHESTRATION_PLATFORM": "mage",
            "ELT_PIPELINE_ORCHESTRATION_FLOW_NAME": "nightly_elt",
            "ELT_PIPELINE_ORCHESTRATION_FLOW_RUN_ID": "mage-run-nightly-789",
            "ELT_PIPELINE_ORCHESTRATION_TASK_NAME": "normalize_orders",
            "ELT_PIPELINE_ORCHESTRATION_TASK_ATTEMPT": "2",
            "ELT_PIPELINE_ORCHESTRATION_TAGS_JSON": json.dumps(
                {"mage_pipeline_tags": "ops,nightly", "execution_date": "2026-06-16"}
            ),
        }
    )

    assert metadata == OrchestrationMetadata(
        platform="mage",
        flow_name="nightly_elt",
        flow_run_id="mage-run-nightly-789",
        task_name="normalize_orders",
        task_attempt=2,
        tags={
            "mage_pipeline_tags": "ops,nightly",
            "execution_date": "2026-06-16",
        },
    )
    assert metadata.to_run_attributes()["orchestration_platform"] == "mage"
    assert metadata.to_run_attributes()["orchestration_tags"]["mage_pipeline_tags"] == "ops,nightly"

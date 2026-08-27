from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from elt_pipeline.integrations.orchestration._metadata import (
    build_airflow_orchestration_metadata,
    build_dagster_orchestration_metadata,
    build_mage_orchestration_metadata,
    build_prefect_orchestration_metadata,
)
from elt_pipeline.integrations.orchestration._models import (
    CliInvocationRequest,
    CliInvocationResult,
    OrchestrationCliInvoker,
    _coerce_strings,
)


def _orchestration_subprocess():
    """Resolve ``subprocess`` through the facade module dict.

    This ensures ``monkeypatch.setattr("elt_pipeline.integrations.orchestration.subprocess.run", …)
    intercepts the call, matching the pre-split behaviour where SubprocessCliInvoker
    was defined in the facade module and resolved ``subprocess`` locally there.
    """
    import subprocess as _subprocess

    facade = sys.modules.get("elt_pipeline.integrations.orchestration")
    if facade is None:
        return _subprocess
    return getattr(facade, "subprocess", _subprocess)


class SubprocessCliInvoker:
    def invoke(
        self,
        request: CliInvocationRequest,
        *,
        timeout_seconds: float | None = None,
    ) -> CliInvocationResult:
        subprocess_mod = _orchestration_subprocess()
        completed = subprocess_mod.run(
            list(request.argv()),
            cwd=request.cwd,
            env=request.build_env(),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        return CliInvocationResult(
            argv=tuple(_coerce_strings(completed.args)),
            cwd=request.cwd,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass
class AirflowCliWrapper:
    repo_root: Path
    invoker: OrchestrationCliInvoker = field(default_factory=SubprocessCliInvoker)
    environment_overrides: dict[str, str] = field(default_factory=dict)

    def build_request(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        airflow_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> CliInvocationRequest:
        combined_environment = dict(self.environment_overrides)
        if environment_overrides is not None:
            combined_environment.update(
                {key: str(value) for key, value in environment_overrides.items()}
            )
        return CliInvocationRequest(
            subcommand=tuple(str(value) for value in subcommand),
            arguments=tuple(str(value) for value in arguments),
            cwd=self.repo_root.resolve(),
            environment_overrides=combined_environment,
            orchestration_metadata=build_airflow_orchestration_metadata(airflow_context),
        )

    def invoke(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        airflow_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        check: bool = True,
    ) -> CliInvocationResult:
        request = self.build_request(
            subcommand=subcommand,
            arguments=arguments,
            airflow_context=airflow_context,
            environment_overrides=environment_overrides,
        )
        result = self.invoker.invoke(request, timeout_seconds=timeout_seconds)
        if check:
            result.raise_for_exit_code()
        return result


@dataclass
class DagsterCliWrapper:
    repo_root: Path
    invoker: OrchestrationCliInvoker = field(default_factory=SubprocessCliInvoker)
    environment_overrides: dict[str, str] = field(default_factory=dict)

    def build_request(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        dagster_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> CliInvocationRequest:
        combined_environment = dict(self.environment_overrides)
        if environment_overrides is not None:
            combined_environment.update(
                {key: str(value) for key, value in environment_overrides.items()}
            )
        return CliInvocationRequest(
            subcommand=tuple(str(value) for value in subcommand),
            arguments=tuple(str(value) for value in arguments),
            cwd=self.repo_root.resolve(),
            environment_overrides=combined_environment,
            orchestration_metadata=build_dagster_orchestration_metadata(dagster_context),
        )

    def invoke(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        dagster_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        check: bool = True,
    ) -> CliInvocationResult:
        request = self.build_request(
            subcommand=subcommand,
            arguments=arguments,
            dagster_context=dagster_context,
            environment_overrides=environment_overrides,
        )
        result = self.invoker.invoke(request, timeout_seconds=timeout_seconds)
        if check:
            result.raise_for_exit_code()
        return result


@dataclass
class PrefectCliWrapper:
    repo_root: Path
    invoker: OrchestrationCliInvoker = field(default_factory=SubprocessCliInvoker)
    environment_overrides: dict[str, str] = field(default_factory=dict)

    def build_request(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        prefect_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> CliInvocationRequest:
        combined_environment = dict(self.environment_overrides)
        if environment_overrides is not None:
            combined_environment.update(
                {key: str(value) for key, value in environment_overrides.items()}
            )
        return CliInvocationRequest(
            subcommand=tuple(str(value) for value in subcommand),
            arguments=tuple(str(value) for value in arguments),
            cwd=self.repo_root.resolve(),
            environment_overrides=combined_environment,
            orchestration_metadata=build_prefect_orchestration_metadata(prefect_context),
        )

    def invoke(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        prefect_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        check: bool = True,
    ) -> CliInvocationResult:
        request = self.build_request(
            subcommand=subcommand,
            arguments=arguments,
            prefect_context=prefect_context,
            environment_overrides=environment_overrides,
        )
        result = self.invoker.invoke(request, timeout_seconds=timeout_seconds)
        if check:
            result.raise_for_exit_code()
        return result


@dataclass
class MageCliWrapper:
    repo_root: Path
    invoker: OrchestrationCliInvoker = field(default_factory=SubprocessCliInvoker)
    environment_overrides: dict[str, str] = field(default_factory=dict)

    def build_request(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        mage_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> CliInvocationRequest:
        combined_environment = dict(self.environment_overrides)
        if environment_overrides is not None:
            combined_environment.update(
                {key: str(value) for key, value in environment_overrides.items()}
            )
        return CliInvocationRequest(
            subcommand=tuple(str(value) for value in subcommand),
            arguments=tuple(str(value) for value in arguments),
            cwd=self.repo_root.resolve(),
            environment_overrides=combined_environment,
            orchestration_metadata=build_mage_orchestration_metadata(mage_context),
        )

    def invoke(
        self,
        *,
        subcommand: Sequence[str],
        arguments: Sequence[str] = (),
        mage_context: Mapping[str, Any] | None = None,
        environment_overrides: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        check: bool = True,
    ) -> CliInvocationResult:
        request = self.build_request(
            subcommand=subcommand,
            arguments=arguments,
            mage_context=mage_context,
            environment_overrides=environment_overrides,
        )
        result = self.invoker.invoke(request, timeout_seconds=timeout_seconds)
        if check:
            result.raise_for_exit_code()
        return result

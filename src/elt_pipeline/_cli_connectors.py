from __future__ import annotations

from typing import Any

from elt_pipeline._cli_models import _CheckpointOverride
from elt_pipeline.ingest import (
    BrokerKafkaConnector,
    KafkaConnectorConfig,
    Level1ArtifactManifest,
    LocalKafkaConnector,
    LocalObjectStorageConnector,
    LocalRestConnector,
    LocalSqlConnector,
    ObjectStorageConnectorConfig,
    RestConnectorConfig,
    RestRequestWindow,
    SqlConnectorConfig,
)
from elt_pipeline.shared.runtime import ExecutionWindow


class _CliCheckpointOverrideMixin:
    def __init__(
        self,
        *,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        self._checkpoint_override = checkpoint_override
        self._cli_window = window

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        if self._checkpoint_override.active:
            return self._checkpoint_override.value
        return super().resolve_checkpoint_before()


class _CliLocalRestConnector(_CliCheckpointOverrideMixin, LocalRestConnector):
    def __init__(
        self,
        *,
        config: RestConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalRestConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def resolve_window(self):
        if self._cli_window.start is None and self._cli_window.end is None:
            return super().resolve_window()
        return RestRequestWindow(
            start=self._cli_window.start,
            end=self._cli_window.end,
            label=self._cli_window.label,
        )


class _CliLocalSqlConnector(_CliCheckpointOverrideMixin, LocalSqlConnector):
    def __init__(
        self,
        *,
        config: SqlConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalSqlConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "sql"},
        )
        return None


class _CliLocalObjectStorageConnector(
    _CliCheckpointOverrideMixin,
    LocalObjectStorageConnector,
):
    def __init__(
        self,
        *,
        config: ObjectStorageConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalObjectStorageConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "object_storage"},
        )
        return None


class _CliLocalKafkaConnector(_CliCheckpointOverrideMixin, LocalKafkaConnector):
    def __init__(
        self,
        *,
        config: KafkaConnectorConfig,
        run_context,
        root_path: str,
        log_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        LocalKafkaConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
            log_path=log_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "kafka"},
        )
        return None


class _CliBrokerKafkaConnector(_CliCheckpointOverrideMixin, BrokerKafkaConnector):
    def __init__(
        self,
        *,
        config: KafkaConnectorConfig,
        run_context,
        root_path: str,
        checkpoint_override: _CheckpointOverride,
        window: ExecutionWindow,
    ) -> None:
        BrokerKafkaConnector.__init__(
            self,
            config=config,
            run_context=run_context,
            root_path=root_path,
        )
        _CliCheckpointOverrideMixin.__init__(
            self,
            checkpoint_override=checkpoint_override,
            window=window,
        )

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        if checkpoint_after is None or checkpoint_after == checkpoint_before:
            return None
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            window_start=self._cli_window.start,
            window_end=self._cli_window.end,
            window_label=self._cli_window.label,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "kafka_broker"},
        )
        return None

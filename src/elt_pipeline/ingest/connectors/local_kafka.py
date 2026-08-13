from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from elt_pipeline.ingest.connectors.kafka import (
    KafkaConnectorBase,
    KafkaConnectorConfig,
    KafkaMessage,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError
from elt_pipeline.shared.path_utils import path_exists, path_read_text
from elt_pipeline.shared.runtime import RunContext

_SAFE_ARTIFACT_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_artifact_fragment(value: Any) -> str:
    cleaned = _SAFE_ARTIFACT_FRAGMENT.sub("_", str(value).strip())
    return cleaned or "unknown"


class LocalKafkaConnector(KafkaConnectorBase):
    def __init__(
        self,
        *,
        config: KafkaConnectorConfig,
        run_context: RunContext,
        root_path: str,
        log_path: str,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)
        self.log_path = log_path

    def validate_config(self) -> KafkaConnectorConfig:
        config = super().validate_config()
        if not path_exists(self.log_path):
            raise ConfigValidationError(
                message="Local Kafka connector requires an existing log_path",
                context={
                    "source_name": config.source_name,
                    "entity_name": config.entity_name,
                    "log_path": self.log_path,
                },
            )
        return config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        checkpoint_document = self.checkpoint_store.load(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
        )
        return checkpoint_document.current_checkpoint

    def consume_messages(
        self,
        *,
        start_offset: int,
        max_messages: int,
    ) -> list[KafkaMessage]:
        messages: list[KafkaMessage] = []
        try:
            payload = path_read_text(self.log_path, encoding="utf-8")
        except PipelineError as exc:
            raise PipelineError(
                message=f"Kafka log_path read failed: {exc}",
                error_code="KAFKA_LOG_READ_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "log_path": self.log_path,
                    "error_type": type(exc).__name__,
                },
            ) from exc
        except OSError as exc:
            raise PipelineError(
                message=f"Kafka log_path read failed: {exc}",
                error_code="KAFKA_LOG_READ_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "log_path": self.log_path,
                    "error_type": type(exc).__name__,
                },
            ) from exc
        try:
            for line in payload.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                document = json.loads(stripped)
                topic = str(document.get("topic") or "")
                partition = int(document.get("partition", 0))
                offset = int(document.get("offset", -1))
                if topic != self.config.topic or partition != self.config.partition:
                    continue
                if offset < start_offset:
                    continue

                timestamp = _parse_timestamp(document.get("timestamp"))
                headers = document.get("headers") or {}
                if not isinstance(headers, dict):
                    headers = {}
                key = _coerce_optional_bytes(document.get("key"))
                value = _coerce_optional_bytes(document.get("value"))
                messages.append(
                    KafkaMessage(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        timestamp=timestamp,
                        key=key,
                        value=value,
                        headers={str(k): str(v) for k, v in headers.items()},
                        metadata={},
                    ),
                )
        except json.JSONDecodeError as exc:
            raise PipelineError(
                message=f"Kafka log_path contained invalid JSON: {exc}",
                error_code="KAFKA_LOG_INVALID_JSON",
                error_category=ErrorCategory.input_contract_error,
                retryable=False,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "log_path": self.log_path,
                },
            ) from exc

        messages.sort(key=lambda msg: msg.offset)
        return messages[:max_messages]

    def persist_message(
        self,
        *,
        message: KafkaMessage,
        checkpoint_before: dict[str, Any] | None,
    ) -> Level1ArtifactManifest:
        payload = self.encode_message_payload(message)
        artifact_name = self._artifact_name_for_message(message)
        return self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=payload,
            payload_format=self.config.payload_format,
            extraction_mode=self.config.execution_mode,
            artifact_name=artifact_name,
            checkpoint_before=checkpoint_before,
            record_count_estimate=1,
            metadata={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp.isoformat() if message.timestamp else None,
                "header_count": len(message.headers),
                "key_size_bytes": len(message.key) if message.key else 0,
                "value_size_bytes": len(message.value) if message.value else 0,
            },
            ingest_completed_at=message.timestamp or datetime.now(tz=UTC),
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
            manifest_paths=[manifest.manifest_path for manifest in manifests],
            metadata={"connector_type": "kafka"},
        )
        return None

    def _artifact_name_for_message(self, message: KafkaMessage) -> str:
        return "-".join(
            [
                _safe_artifact_fragment(self.config.entity_name),
                _safe_artifact_fragment(self.config.topic),
                f"p{message.partition}",
                f"o{message.offset}",
            ],
        )


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    raw = str(value)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _coerce_optional_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


__all__ = ["LocalKafkaConnector"]

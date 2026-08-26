from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import RunContext


class KafkaStartingPosition(str, Enum):
    checkpoint = "checkpoint"
    earliest = "earliest"


class KafkaMessage(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: datetime | None = None
    key: bytes | None = None
    value: bytes | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


_KAFKA_DRIVER_INSTALL_HINTS: dict[str, str] = {
    "broker": "uv sync --extra kafka",
}


class KafkaConnectorConfig(BaseModel):
    schema_version: str
    environment: str
    source_name: str
    entity_name: str
    execution_mode: str
    topic: str
    partition: int = 0
    starting_position: KafkaStartingPosition = KafkaStartingPosition.checkpoint
    start_offset: int | None = Field(default=None, ge=0)
    max_messages: int = Field(default=1000, ge=1)
    payload_format: str = "json"
    bootstrap_servers: str | list[str] | None = Field(default=None)
    consumer_group_id: str | None = Field(default=None)
    persistence: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("bootstrap_servers")
    @classmethod
    def _validate_bootstrap_servers(cls, value: Any) -> str | list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("bootstrap_servers string must not be empty")
            return stripped
        if isinstance(value, list):
            if not value:
                raise ValueError("bootstrap_servers list must not be empty")
            normalized: list[str] = []
            for entry in value:
                s = str(entry).strip()
                if not s:
                    raise ValueError("bootstrap_servers list entries must not be empty")
                normalized.append(s)
            return normalized
        raise ValueError("bootstrap_servers must be a string or list of strings")

    @field_validator("topic")
    @classmethod
    def _validate_topic(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("topic must not be empty")
        return normalized

    @classmethod
    def from_resolved_entity_config(
        cls,
        resolved_config: ResolvedEntityConfig,
    ) -> KafkaConnectorConfig:
        if resolved_config.connector_type != "kafka":
            raise ConfigValidationError(
                message="Resolved entity config is not a Kafka connector",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "connector_type": resolved_config.connector_type,
                },
            )

        extraction = resolved_config.extraction
        topic = extraction.get("topic") or extraction.get("topics")
        if isinstance(topic, list):
            topic = topic[0] if topic else None
        if not topic:
            raise ConfigValidationError(
                message="kafka extraction config must define topic",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                },
            )

        payload = {
            "schema_version": resolved_config.schema_version,
            "environment": resolved_config.environment,
            "source_name": resolved_config.source_name,
            "entity_name": resolved_config.entity_name,
            "execution_mode": resolved_config.trigger_mode or "manual",
            "topic": topic,
            "partition": extraction.get("partition", 0),
            "starting_position": extraction.get("starting_position", "checkpoint"),
            "start_offset": extraction.get("start_offset"),
            "max_messages": extraction.get("max_messages", 1000),
            "payload_format": extraction.get("payload_format", "json"),
            "bootstrap_servers": extraction.get("bootstrap_servers"),
            "consumer_group_id": extraction.get("consumer_group_id"),
            "persistence": resolved_config.persistence,
            "state": resolved_config.state,
            "settings": resolved_config.settings,
            "raw": resolved_config.raw,
        }
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            raise ConfigValidationError(
                message="Kafka connector configuration validation failed",
                context={
                    "source_name": resolved_config.source_name,
                    "entity_name": resolved_config.entity_name,
                    "errors": exc.errors(include_url=False),
                },
            ) from exc


class KafkaRunResult(BaseModel):
    manifests: list[Level1ArtifactManifest] = Field(default_factory=list)
    checkpoint_before: dict[str, Any] | None = None
    checkpoint_after: dict[str, Any] | None = None
    message_count: int = 0


class KafkaConnectorBase(ABC):
    def __init__(self, *, config: KafkaConnectorConfig, run_context: RunContext) -> None:
        self.config = config
        self.run_context = run_context

    def validate_config(self) -> KafkaConnectorConfig:
        return self.config

    def resolve_checkpoint_before(self) -> dict[str, Any] | None:
        return None

    def resolve_start_offset(self, *, checkpoint_before: dict[str, Any] | None) -> int:
        if self.config.starting_position == KafkaStartingPosition.earliest:
            return 0

        if checkpoint_before and "offset" in checkpoint_before:
            raw_offset = checkpoint_before.get("offset")
            try:
                return int(raw_offset)
            except (TypeError, ValueError) as exc:
                raise ConfigValidationError(
                    message="Kafka checkpoint offset must be an integer",
                    context={
                        "source_name": self.config.source_name,
                        "entity_name": self.config.entity_name,
                        "checkpoint_offset": raw_offset,
                    },
                ) from exc

        if self.config.start_offset is not None:
            return int(self.config.start_offset)

        return 0

    @abstractmethod
    def consume_messages(
        self,
        *,
        start_offset: int,
        max_messages: int,
    ) -> list[KafkaMessage]:
        raise NotImplementedError

    @abstractmethod
    def persist_message(
        self,
        *,
        message: KafkaMessage,
        checkpoint_before: dict[str, Any] | None,
    ) -> Level1ArtifactManifest:
        raise NotImplementedError

    def build_checkpoint_after(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        messages: list[KafkaMessage],
        manifests: list[Level1ArtifactManifest],
    ) -> dict[str, Any] | None:
        del manifests

        if not messages:
            return checkpoint_before

        last_offset = max(message.offset for message in messages)
        return {
            "topic": self.config.topic,
            "partition": self.config.partition,
            "offset": last_offset + 1,
            "recorded_at": datetime.now(tz=UTC).isoformat(),
        }

    def update_checkpoint(
        self,
        *,
        checkpoint_before: dict[str, Any] | None,
        checkpoint_after: dict[str, Any] | None,
        manifests: list[Level1ArtifactManifest],
    ) -> None:
        del checkpoint_before
        del checkpoint_after
        del manifests
        return None

    def run(self) -> KafkaRunResult:
        self.validate_config()
        checkpoint_before = self.resolve_checkpoint_before()
        start_offset = self.resolve_start_offset(checkpoint_before=checkpoint_before)
        messages = self.consume_messages(
            start_offset=start_offset,
            max_messages=self.config.max_messages,
        )

        manifests: list[Level1ArtifactManifest] = []
        for message in messages:
            manifests.append(
                self.persist_message(
                    message=message,
                    checkpoint_before=checkpoint_before,
                )
            )

        checkpoint_after = self.build_checkpoint_after(
            checkpoint_before=checkpoint_before,
            messages=messages,
            manifests=manifests,
        )
        self.update_checkpoint(
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            manifests=manifests,
        )
        return KafkaRunResult(
            manifests=manifests,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            message_count=len(messages),
        )

    @staticmethod
    def encode_message_payload(message: KafkaMessage) -> str:
        payload: dict[str, Any] = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": message.timestamp.isoformat() if message.timestamp else None,
            "headers": dict(message.headers),
            "key": message.key.decode("utf-8", errors="replace") if message.key else None,
            "value": message.value.decode("utf-8", errors="replace") if message.value else None,
            "metadata": dict(message.metadata),
        }
        return json.dumps(payload, sort_keys=True)


__all__ = [
    "KafkaConnectorBase",
    "KafkaConnectorConfig",
    "KafkaMessage",
    "KafkaRunResult",
    "KafkaStartingPosition",
]

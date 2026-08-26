from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from elt_pipeline.ingest.connectors.kafka import (
    _KAFKA_DRIVER_INSTALL_HINTS,
    KafkaConnectorBase,
    KafkaConnectorConfig,
    KafkaMessage,
)
from elt_pipeline.ingest.connectors.local_kafka import (
    _safe_artifact_fragment,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError
from elt_pipeline.shared.runtime import RunContext

_KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS = 1.0


def _get_kafka_python_module() -> Any:
    try:
        import kafka  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ConfigValidationError(
            message=(
                "kafka-python SDK is not installed. "
                f"Install it with: {_KAFKA_DRIVER_INSTALL_HINTS['broker']}"
            ),
            context={
                "error_code": "KAFKA_SDK_MISSING",
                "install_hint": _KAFKA_DRIVER_INSTALL_HINTS["broker"],
                "missing_package": "kafka-python",
            },
        ) from exc
    return kafka


def _normalize_bootstrap_servers(config: KafkaConnectorConfig) -> list[str]:
    servers = config.bootstrap_servers
    if servers is None:
        raise ConfigValidationError(
            message="Broker Kafka connector requires bootstrap_servers in config",
            context={
                "source_name": config.source_name,
                "entity_name": config.entity_name,
            },
        )
    if isinstance(servers, str):
        return [servers]
    return list(servers)


def _parse_timestamp_ms(ts_ms: Any) -> datetime | None:
    if ts_ms is None or ts_ms == -1:
        return None
    try:
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


class BrokerKafkaConnector(KafkaConnectorBase):
    def __init__(
        self,
        *,
        config: KafkaConnectorConfig,
        run_context: RunContext,
        root_path: str,
    ) -> None:
        super().__init__(config=config, run_context=run_context)
        self.writer = LocalLevel1Writer(root_path)
        self.checkpoint_store = LocalCheckpointStore(root_path)
        self._kafka_module = None

    def _get_kafka_module(self) -> Any:
        if self._kafka_module is None:
            self._kafka_module = _get_kafka_python_module()
        return self._kafka_module

    def validate_config(self) -> KafkaConnectorConfig:
        config = super().validate_config()
        _normalize_bootstrap_servers(config)
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
        kafka_mod = self._get_kafka_module()
        bootstrap_servers = _normalize_bootstrap_servers(self.config)
        topic = self.config.topic
        partition_idx = self.config.partition

        consumer_kwargs: dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "enable_auto_commit": False,
            "consumer_timeout_ms": int(_KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS * 1000),
        }
        if self.config.consumer_group_id:
            consumer_kwargs["group_id"] = self.config.consumer_group_id

        try:
            consumer = kafka_mod.KafkaConsumer(**consumer_kwargs)
        except Exception as exc:
            raise PipelineError(
                message=f"Kafka broker consumer creation failed: {exc}",
                error_code="KAFKA_BROKER_CONSUMER_CREATE_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={
                    "source_name": self.config.source_name,
                    "entity_name": self.config.entity_name,
                    "bootstrap_servers": bootstrap_servers,
                    "error_type": type(exc).__name__,
                },
            ) from exc

        messages: list[KafkaMessage] = []
        try:
            tp = kafka_mod.TopicPartition(topic, partition_idx)
            consumer.assign([tp])
            consumer.seek(tp, start_offset)

            collected = 0
            empty_polls = 0
            max_empty_polls = 3

            while collected < max_messages and empty_polls < max_empty_polls:
                remaining = max_messages - collected
                poll_batch = consumer.poll(
                    timeout_ms=_KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS * 1000,
                    max_records=remaining,
                )
                if not poll_batch:
                    empty_polls += 1
                    continue
                empty_polls = 0
                for record_tp, records in poll_batch.items():
                    if record_tp.topic != topic or record_tp.partition != partition_idx:
                        continue
                    for record in records:
                        if collected >= max_messages:
                            break
                        headers: dict[str, str] = {}
                        if record.headers:
                            for hdr in record.headers:
                                try:
                                    key = hdr[0] if isinstance(hdr, tuple) else str(hdr)
                                    val = None
                                    if isinstance(hdr, tuple) and len(hdr) > 1:
                                        raw_val = hdr[1]
                                        if isinstance(raw_val, bytes):
                                            val = raw_val.decode("utf-8", errors="replace")
                                        elif raw_val is not None:
                                            val = str(raw_val)
                                    headers[str(key)] = val or ""
                                except Exception:
                                    pass

                        key_bytes: bytes | None = None
                        if record.key is not None:
                            if isinstance(record.key, bytes):
                                key_bytes = record.key
                            else:
                                try:
                                    key_bytes = str(record.key).encode("utf-8")
                                except Exception:
                                    key_bytes = None

                        value_bytes: bytes | None = None
                        if record.value is not None:
                            if isinstance(record.value, bytes):
                                value_bytes = record.value
                            else:
                                try:
                                    value_bytes = json.dumps(record.value).encode("utf-8")
                                except Exception:
                                    try:
                                        value_bytes = str(record.value).encode("utf-8")
                                    except Exception:
                                        value_bytes = None

                        messages.append(
                            KafkaMessage(
                                topic=record.topic,
                                partition=record.partition,
                                offset=record.offset,
                                timestamp=_parse_timestamp_ms(record.timestamp),
                                key=key_bytes,
                                value=value_bytes,
                                headers=headers,
                                metadata={
                                    "timestamp_type": (
                                        int(record.timestamp_type)
                                        if record.timestamp_type is not None
                                        else None
                                    ),
                                },
                            ),
                        )
                        collected += 1
                        if collected >= max_messages:
                            break
                    if collected >= max_messages:
                        break
        finally:
            try:
                consumer.close()
            except Exception:
                pass

        messages.sort(key=lambda msg: msg.offset)
        return messages

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
                "source": "kafka_broker",
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
            metadata={"connector_type": "kafka_broker"},
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


__all__ = ["BrokerKafkaConnector"]

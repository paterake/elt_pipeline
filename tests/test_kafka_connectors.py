from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from elt_pipeline.config.models import ResolvedEntityConfig
from elt_pipeline.ingest.connectors import (
    KafkaConnectorBase,
    KafkaConnectorConfig,
    KafkaMessage,
    LocalKafkaConnector,
)
from elt_pipeline.ingest.models import Level1ArtifactManifest
from elt_pipeline.ingest.state import LocalCheckpointStore
from elt_pipeline.ingest.storage import LocalLevel1Writer
from elt_pipeline.shared.errors import ConfigValidationError
from elt_pipeline.shared.runtime import StageName, new_run_context


def test_kafka_connector_config_builds_from_resolved_entity_config() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        connector_type="kafka",
        trigger_mode="micro_batch",
        extraction={
            "topic": "orders-events",
            "partition": 2,
            "starting_position": "earliest",
            "max_messages": 250,
        },
    )

    connector_config = KafkaConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.topic == "orders-events"
    assert connector_config.partition == 2
    assert connector_config.starting_position.value == "earliest"
    assert connector_config.max_messages == 250
    assert connector_config.execution_mode == "micro_batch"


def test_kafka_connector_config_rejects_non_kafka_connector() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="orders_api",
        entity_name="orders",
        connector_type="rest",
    )

    with pytest.raises(ConfigValidationError, match="not a Kafka connector"):
        KafkaConnectorConfig.from_resolved_entity_config(resolved_config)


def test_kafka_connector_base_persists_before_checkpoint_update(tmp_path: Path) -> None:
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="kafka-ingest",
        trigger_type="micro_batch",
    )
    connector = FakeKafkaConnector(tmp_path=tmp_path, run_context=run_context)

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="events",
        entity_name="orders",
    )

    assert connector.call_order == [
        "resolve_checkpoint_before",
        "consume_messages",
        "persist_message",
        "update_checkpoint",
    ]
    assert result.message_count == 1
    assert result.checkpoint_before == {"offset": 10}
    assert result.checkpoint_after == {
        "topic": "orders-events",
        "partition": 0,
        "offset": 12,
        "recorded_at": result.checkpoint_after["recorded_at"],
    }
    assert checkpoint_document.current_checkpoint["offset"] == 12
    assert [Path(p) for p in checkpoint_document.history[0].manifest_paths] == [
        Path(result.manifests[0].manifest_path)
    ]


def test_local_kafka_connector_consumes_from_log_and_updates_checkpoint(tmp_path: Path) -> None:
    log_path = tmp_path / "topic.jsonl"
    log_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "topic": "orders-events",
                        "partition": 0,
                        "offset": 5,
                        "timestamp": "2026-01-01T10:00:00+00:00",
                        "key": "k1",
                        "value": "v1",
                        "headers": {"x": "1"},
                    }
                ),
                json.dumps(
                    {
                        "topic": "orders-events",
                        "partition": 0,
                        "offset": 6,
                        "timestamp": "2026-01-01T10:01:00+00:00",
                        "key": "k2",
                        "value": "v2",
                        "headers": {"x": "2"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    connector = LocalKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            partition=0,
            starting_position="earliest",
            max_messages=100,
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
        log_path=str(log_path),
    )

    result = connector.run()
    checkpoint_document = connector.checkpoint_store.load(
        environment="dev",
        source_name="events",
        entity_name="orders",
    )

    assert result.message_count == 2
    assert result.checkpoint_before is None
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 7
    assert checkpoint_document.current_checkpoint["offset"] == 7
    assert len(result.manifests) == 2


def test_local_kafka_connector_uses_saved_checkpoint_next_run(tmp_path: Path) -> None:
    log_path = tmp_path / "topic.jsonl"
    log_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "topic": "orders-events",
                        "partition": 0,
                        "offset": 0,
                        "timestamp": "2026-01-01T10:00:00+00:00",
                        "key": "k0",
                        "value": "v0",
                    }
                ),
                json.dumps(
                    {
                        "topic": "orders-events",
                        "partition": 0,
                        "offset": 1,
                        "timestamp": "2026-01-01T10:01:00+00:00",
                        "key": "k1",
                        "value": "v1",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    checkpoint_store = LocalCheckpointStore(str(tmp_path))
    checkpoint_store.commit(
        environment="dev",
        source_name="events",
        entity_name="orders",
        run_id="prior-run",
        checkpoint_before=None,
        checkpoint_after={"topic": "orders-events", "partition": 0, "offset": 1},
        recorded_at=new_run_context(
            stage=StageName.ingest,
            job_name="prior-kafka-ingest",
            trigger_type="micro_batch",
        ).started_at,
    )

    connector = LocalKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            partition=0,
            starting_position="checkpoint",
            max_messages=100,
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
        log_path=str(log_path),
    )

    result = connector.run()

    assert result.checkpoint_before == {"topic": "orders-events", "partition": 0, "offset": 1}
    assert result.message_count == 1
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 2


class FakeKafkaConnector(KafkaConnectorBase):
    def __init__(self, *, tmp_path: Path, run_context) -> None:
        self.call_order: list[str] = []
        self.writer = LocalLevel1Writer(str(tmp_path))
        self.checkpoint_store = LocalCheckpointStore(str(tmp_path))
        super().__init__(
            config=KafkaConnectorConfig(
                schema_version="v1",
                environment="dev",
                source_name="events",
                entity_name="orders",
                execution_mode="micro_batch",
                topic="orders-events",
                partition=0,
                starting_position="checkpoint",
                max_messages=10,
            ),
            run_context=run_context,
        )

    def resolve_checkpoint_before(self) -> dict[str, int]:
        self.call_order.append("resolve_checkpoint_before")
        return {"offset": 10}

    def consume_messages(self, *, start_offset: int, max_messages: int) -> list[KafkaMessage]:
        self.call_order.append("consume_messages")
        assert start_offset == 10
        assert max_messages == 10
        return [
            KafkaMessage(
                topic="orders-events",
                partition=0,
                offset=11,
                timestamp=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
                key=b"k",
                value=b"v",
            )
        ]

    def persist_message(
        self,
        *,
        message: KafkaMessage,
        checkpoint_before: dict[str, int] | None,
    ) -> Level1ArtifactManifest:
        self.call_order.append("persist_message")
        manifest = self.writer.write_payload(
            run_context=self.run_context,
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            payload=self.encode_message_payload(message),
            payload_format="json",
            extraction_mode=self.config.execution_mode,
            artifact_name="orders-events-11",
            checkpoint_before=checkpoint_before,
            metadata={"offset": message.offset},
            ingest_completed_at=message.timestamp,
        )
        return manifest

    def update_checkpoint(
        self,
        *,
        checkpoint_before,
        checkpoint_after,
        manifests,
    ) -> None:
        self.call_order.append("update_checkpoint")
        self.checkpoint_store.commit(
            environment=self.config.environment,
            source_name=self.config.source_name,
            entity_name=self.config.entity_name,
            run_id=self.run_context.run_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            recorded_at=self.run_context.started_at,
            manifest_paths=[manifest.manifest_path for manifest in manifests],
        )

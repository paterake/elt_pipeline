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


def test_kafka_connector_config_builds_with_bootstrap_servers_string() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        connector_type="kafka",
        trigger_mode="micro_batch",
        extraction={
            "topic": "orders-events",
            "bootstrap_servers": "broker1:9092,broker2:9092",
            "consumer_group_id": "elt-pipeline-group",
        },
    )

    connector_config = KafkaConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.bootstrap_servers == "broker1:9092,broker2:9092"
    assert connector_config.consumer_group_id == "elt-pipeline-group"


def test_kafka_connector_config_builds_with_bootstrap_servers_list() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        connector_type="kafka",
        trigger_mode="micro_batch",
        extraction={
            "topic": "orders-events",
            "bootstrap_servers": ["broker1:9092", "broker2:9092"],
        },
    )

    connector_config = KafkaConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.bootstrap_servers == ["broker1:9092", "broker2:9092"]


def test_kafka_connector_config_default_bootstrap_servers_none() -> None:
    resolved_config = ResolvedEntityConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        connector_type="kafka",
        trigger_mode="micro_batch",
        extraction={"topic": "orders-events"},
    )

    connector_config = KafkaConnectorConfig.from_resolved_entity_config(resolved_config)

    assert connector_config.bootstrap_servers is None


def test_kafka_connector_config_rejects_empty_bootstrap_servers() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="bootstrap_servers string must not be empty"):
        KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            bootstrap_servers="   ",
        )

    with pytest.raises(ValidationError, match="bootstrap_servers list must not be empty"):
        KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            bootstrap_servers=[],
        )


def test_broker_connector_factory_routes_when_bootstrap_servers_set(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors.broker_kafka import BrokerKafkaConnector
    from elt_pipeline.ingest.connectors.registry import _KafkaConnectorFactory

    factory = _KafkaConnectorFactory()
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="kafka-ingest",
        trigger_type="micro_batch",
    )
    config = KafkaConnectorConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        execution_mode="micro_batch",
        topic="orders-events",
        bootstrap_servers="broker:9092",
    )

    connector = factory.build_connector(
        config=config,
        run_context=run_context,
        root_path=str(tmp_path),
    )

    assert isinstance(connector, BrokerKafkaConnector)


def test_local_connector_factory_routes_when_bootstrap_servers_missing(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors.registry import _KafkaConnectorFactory

    factory = _KafkaConnectorFactory()
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="kafka-ingest",
        trigger_type="micro_batch",
    )
    config = KafkaConnectorConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        execution_mode="micro_batch",
        topic="orders-events",
    )

    connector = factory.build_connector(
        config=config,
        run_context=run_context,
        root_path=str(tmp_path),
        log_path=str(tmp_path / "log.jsonl"),
    )

    assert isinstance(connector, LocalKafkaConnector)


def test_local_connector_factory_raises_without_log_path_when_no_bootstrap(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors.registry import _KafkaConnectorFactory

    factory = _KafkaConnectorFactory()
    run_context = new_run_context(
        stage=StageName.ingest,
        job_name="kafka-ingest",
        trigger_type="micro_batch",
    )
    config = KafkaConnectorConfig(
        schema_version="v1",
        environment="dev",
        source_name="events",
        entity_name="orders",
        execution_mode="micro_batch",
        topic="orders-events",
    )

    with pytest.raises(ConfigValidationError, match="log_path="):
        factory.build_connector(
            config=config,
            run_context=run_context,
            root_path=str(tmp_path),
        )


def test_broker_connector_sdk_missing_raises_install_hint(tmp_path: Path, monkeypatch) -> None:
    from elt_pipeline.ingest.connectors import BrokerKafkaConnector

    real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def blocked_import(name, *args, **kwargs):
        if name == "kafka":
            raise ImportError("No module named 'kafka'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", blocked_import)

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            bootstrap_servers="broker:9092",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )

    with pytest.raises(ConfigValidationError, match="uv sync --extra kafka") as exc_info:
        connector.consume_messages(start_offset=0, max_messages=10)

    assert exc_info.value.context["error_code"] == "KAFKA_SDK_MISSING"
    assert "kafka-python" in exc_info.value.context["missing_package"]


def test_broker_connector_validate_config_passes_with_servers(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors import BrokerKafkaConnector

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            bootstrap_servers=["broker1:9092", "broker2:9092"],
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )

    validated = connector.validate_config()
    assert validated.bootstrap_servers == ["broker1:9092", "broker2:9092"]


def test_broker_connector_consume_uses_injected_fake_kafka_module(
    tmp_path: Path, monkeypatch,
) -> None:
    from datetime import UTC

    from elt_pipeline.ingest.connectors import BrokerKafkaConnector

    class FakeTopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

    class FakeConsumerRecord:
        def __init__(self, topic, partition, offset, value, timestamp_ms, key=None, headers=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset
            self.key = key
            self.value = value
            self.timestamp = timestamp_ms
            self.timestamp_type = 0
            self.headers = headers or []

    class FakeConsumer:
        assigned = []
        seek_calls = []
        closed = False
        _call_count = 0

        def __init__(self, **kwargs):
            self._kwargs = kwargs

        def assign(self, tps):
            FakeConsumer.assigned = list(tps)

        def seek(self, tp, offset):
            FakeConsumer.seek_calls.append((tp.topic, tp.partition, offset))

        def poll(self, timeout_ms=None, max_records=None):
            FakeConsumer._call_count += 1
            if FakeConsumer._call_count > 1:
                return {}
            tp = FakeTopicPartition("orders-events", 0)
            return {
                tp: [
                    FakeConsumerRecord(
                        topic="orders-events",
                        partition=0,
                        offset=5,
                        value=b'{"id": 1, "sku": "A"}',
                        timestamp_ms=1704067200000,
                        key=b"key1",
                        headers=[("h1", b"v1")],
                    ),
                    FakeConsumerRecord(
                        topic="orders-events",
                        partition=0,
                        offset=6,
                        value=b'{"id": 2, "sku": "B"}',
                        timestamp_ms=1704070800000,
                    ),
                ]
            }

        def close(self):
            FakeConsumer.closed = True

    class FakeKafkaModule:
        TopicPartition = FakeTopicPartition

        def KafkaConsumer(self, **kwargs):
            return FakeConsumer(**kwargs)

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            partition=0,
            bootstrap_servers="broker:9092",
            consumer_group_id="test-group",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )
    connector._kafka_module = FakeKafkaModule()

    messages = connector.consume_messages(start_offset=5, max_messages=10)

    assert len(messages) == 2
    assert messages[0].offset == 5
    assert messages[0].key == b"key1"
    assert messages[0].value == b'{"id": 1, "sku": "A"}'
    assert messages[0].headers == {"h1": "v1"}
    assert messages[0].timestamp == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert messages[1].offset == 6
    assert FakeConsumer.seek_calls[0] == ("orders-events", 0, 5)
    assert FakeConsumer.closed is True


def test_broker_connector_build_checkpoint_from_messages(tmp_path: Path) -> None:

    from elt_pipeline.ingest.connectors import BrokerKafkaConnector
    from elt_pipeline.ingest.connectors.kafka import KafkaMessage

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            partition=2,
            bootstrap_servers="broker:9092",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )

    messages = [
        KafkaMessage(topic="orders-events", partition=2, offset=100),
        KafkaMessage(topic="orders-events", partition=2, offset=101),
        KafkaMessage(topic="orders-events", partition=2, offset=102),
    ]
    result = connector.build_checkpoint_after(
        checkpoint_before=None,
        messages=messages,
        manifests=[],
    )

    assert result is not None
    assert result["topic"] == "orders-events"
    assert result["partition"] == 2
    assert result["offset"] == 103


def test_broker_connector_resolve_start_offset_earliest(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors import BrokerKafkaConnector
    from elt_pipeline.ingest.connectors.kafka import KafkaStartingPosition

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            starting_position=KafkaStartingPosition.earliest,
            bootstrap_servers="broker:9092",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )

    assert connector.resolve_start_offset(checkpoint_before=None) == 0


def test_broker_connector_resolve_start_offset_from_checkpoint(tmp_path: Path) -> None:
    from elt_pipeline.ingest.connectors import BrokerKafkaConnector

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            bootstrap_servers="broker:9092",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )

    assert (
        connector.resolve_start_offset(
            checkpoint_before={"topic": "orders-events", "partition": 0, "offset": 42}
        )
        == 42
    )


def test_broker_connector_end_to_end_run_with_fake_consumer(tmp_path: Path, monkeypatch) -> None:
    from elt_pipeline.ingest.connectors import BrokerKafkaConnector

    class FakeTopicPartition:
        def __init__(self, topic, partition):
            self.topic = topic
            self.partition = partition

    class FakeConsumerRecord:
        def __init__(self, topic, partition, offset, value):
            self.topic = topic
            self.partition = partition
            self.offset = offset
            self.key = None
            self.value = value
            self.timestamp = 1704067200000
            self.timestamp_type = 0
            self.headers = []

    class FakeConsumer:
        _poll_count = 0

        def __init__(self, **kwargs):
            pass

        def assign(self, tps):
            pass

        def seek(self, tp, offset):
            pass

        def poll(self, timeout_ms=None, max_records=None):
            FakeConsumer._poll_count += 1
            if FakeConsumer._poll_count > 1:
                return {}
            tp = FakeTopicPartition("orders-events", 0)
            return {
                tp: [
                    FakeConsumerRecord("orders-events", 0, 0, b'{"id": 1}'),
                    FakeConsumerRecord("orders-events", 0, 1, b'{"id": 2}'),
                ]
            }

        def close(self):
            pass

    class FakeKafkaModule:
        TopicPartition = FakeTopicPartition

        def KafkaConsumer(self, **kwargs):
            return FakeConsumer(**kwargs)

    connector = BrokerKafkaConnector(
        config=KafkaConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="events",
            entity_name="orders",
            execution_mode="micro_batch",
            topic="orders-events",
            starting_position="earliest",
            max_messages=100,
            bootstrap_servers="broker:9092",
        ),
        run_context=new_run_context(
            stage=StageName.ingest,
            job_name="kafka-ingest",
            trigger_type="micro_batch",
        ),
        root_path=str(tmp_path),
    )
    connector._kafka_module = FakeKafkaModule()

    result = connector.run()

    assert result.message_count == 2
    assert len(result.manifests) == 2
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 2
    checkpoint = connector.checkpoint_store.load(
        environment="dev", source_name="events", entity_name="orders"
    )
    assert checkpoint.current_checkpoint["offset"] == 2


def test_local_kafka_connector_empty_log_returns_zero_messages(tmp_path: Path) -> None:
    log_path = tmp_path / "empty.jsonl"
    log_path.write_text("", encoding="utf-8")

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
    assert result.message_count == 0
    assert result.checkpoint_before is None
    assert result.checkpoint_after is None
    assert result.manifests == []


def test_local_kafka_connector_offset_gaps_sorted_deterministically(tmp_path: Path) -> None:
    log_path = tmp_path / "gappy.jsonl"
    records = [
        {"topic": "orders-events", "partition": 0, "offset": 100, "value": "v100"},
        {"topic": "orders-events", "partition": 0, "offset": 50, "value": "v50"},
        {"topic": "orders-events", "partition": 0, "offset": 75, "value": "v75"},
    ]
    log_path.write_text(
        "\n".join(json.dumps(rec) for rec in records),
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
    assert result.message_count == 3
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 101
    offsets_in_order = [m.metadata["offset"] for m in result.manifests]
    assert offsets_in_order == [50, 75, 100]


def test_local_kafka_connector_skips_other_partitions_and_topics(tmp_path: Path) -> None:
    log_path = tmp_path / "multipart.jsonl"
    records = [
        {"topic": "orders-events", "partition": 0, "offset": 10, "value": "me"},
        {"topic": "other-topic", "partition": 0, "offset": 20, "value": "skip-topic"},
        {"topic": "orders-events", "partition": 1, "offset": 30, "value": "skip-partition"},
        {"topic": "orders-events", "partition": 0, "offset": 11, "value": "me2"},
    ]
    log_path.write_text(
        "\n".join(json.dumps(r) for r in records),
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
    assert result.message_count == 2
    offsets = [m.metadata["offset"] for m in result.manifests]
    assert offsets == [10, 11]
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 12


def test_local_kafka_connector_replay_from_middle_checkpoint(tmp_path: Path) -> None:
    log_path = tmp_path / "window.jsonl"
    records = [
        {"topic": "orders-events", "partition": 0, "offset": 0, "value": "a"},
        {"topic": "orders-events", "partition": 0, "offset": 1, "value": "b"},
        {"topic": "orders-events", "partition": 0, "offset": 2, "value": "c"},
        {"topic": "orders-events", "partition": 0, "offset": 3, "value": "d"},
        {"topic": "orders-events", "partition": 0, "offset": 4, "value": "e"},
    ]
    log_path.write_text(
        "\n".join(json.dumps(r) for r in records),
        encoding="utf-8",
    )

    checkpoint_store = LocalCheckpointStore(str(tmp_path))
    checkpoint_store.commit(
        environment="dev",
        source_name="events",
        entity_name="orders",
        run_id="prior-run",
        checkpoint_before=None,
        checkpoint_after={"topic": "orders-events", "partition": 0, "offset": 2},
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
    assert result.checkpoint_before == {"topic": "orders-events", "partition": 0, "offset": 2}
    assert result.message_count == 3
    offsets = [m.metadata["offset"] for m in result.manifests]
    assert offsets == [2, 3, 4]
    assert result.checkpoint_after is not None
    assert result.checkpoint_after["offset"] == 5


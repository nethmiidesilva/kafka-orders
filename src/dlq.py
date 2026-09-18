"""Dead Letter Queue writer.

A DLQ message is deliberately *not* a plain Order. If the original bytes could
not even be deserialised, there is no Order to write. So the DLQ carries an
envelope: the raw bytes plus enough metadata to diagnose and replay the failure.

The envelope is itself Avro-encoded (schemas/failed_order.avsc), so every topic
in the system stays schema-governed, which is what the assignment asks for.
Error details are duplicated into Kafka headers so tools like kafka-console-consumer
or Kafka UI can show the reason without decoding the payload.
"""
import time
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from src import config


class DLQPublisher:
    def __init__(self, producer: Optional[Producer] = None):
        self._producer = producer or Producer(
            {
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
        self._serializer = AvroSerializer(
            schema_registry_client=sr_client,
            schema_str=config.load_schema("failed_order.avsc"),
        )
        self._key_serializer = StringSerializer("utf_8")
        self._ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)

    def send(
        self,
        *,
        raw_payload: bytes,
        error_type: str,
        error_message: str,
        attempts: int,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        order_id: Optional[str] = None,
        key: Optional[bytes] = None,
    ) -> None:
        envelope = {
            "orderId": order_id,
            "rawPayload": raw_payload or b"",
            "errorType": error_type,
            "errorMessage": error_message[:1000],
            "attempts": attempts,
            "sourceTopic": source_topic,
            "sourcePartition": source_partition,
            "sourceOffset": source_offset,
            "failedAtMillis": int(time.time() * 1000),
        }
        self._producer.produce(
            topic=config.DLQ_TOPIC,
            key=key if key is not None else self._key_serializer(order_id or "unknown"),
            value=self._serializer(envelope, self._ctx),
            headers=[
                ("error_type", error_type.encode()),
                ("error_message", error_message[:200].encode()),
                ("attempts", str(attempts).encode()),
                ("source_topic", source_topic.encode()),
                ("source_offset", str(source_offset).encode()),
            ],
        )
        # Block until the DLQ write is acknowledged. This matters: we must not
        # commit the source offset before the DLQ copy is safely persisted, or a
        # crash in between would lose the message entirely.
        self._producer.flush(10)

    def close(self) -> None:
        self._producer.flush(10)

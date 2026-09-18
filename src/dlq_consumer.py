#!/usr/bin/env python3
"""Inspect the Dead Letter Queue, and optionally replay its messages.

    python -m src.dlq_consumer                 # print every DLQ record
    python -m src.dlq_consumer --replay        # re-publish the originals to `orders`

Replay is the reason the DLQ envelope stores the raw bytes: once the underlying
bug or outage is fixed, the parked messages can be pushed back through the
normal pipeline without the producer having to resend anything.
"""
import argparse
import datetime as dt

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src import config


def main() -> int:
    parser = argparse.ArgumentParser(description="Read the orders DLQ")
    parser.add_argument("--group", default="dlq-inspector")
    parser.add_argument("--timeout", type=float, default=8.0,
                        help="seconds of silence before exiting")
    parser.add_argument("--replay", action="store_true",
                        help="re-publish each raw payload back onto the orders topic")
    args = parser.parse_args()

    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=config.load_schema("failed_order.avsc"),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.DLQ_TOPIC])
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS}) if args.replay else None

    print(f"Reading '{config.DLQ_TOPIC}' (exits after {args.timeout:.0f}s of no new messages)\n")
    seen = 0
    idle_since = None
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                import time as _t
                idle_since = idle_since or _t.monotonic()
                if _t.monotonic() - idle_since > args.timeout:
                    break
                continue
            idle_since = None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            record = deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
            seen += 1
            ts = dt.datetime.fromtimestamp(record["failedAtMillis"] / 1000)
            print("-" * 60)
            print(f" DLQ record #{seen}  (offset {msg.offset()})")
            print(f"   orderId    : {record['orderId']}")
            print(f"   errorType  : {record['errorType']}")
            print(f"   message    : {record['errorMessage']}")
            print(f"   attempts   : {record['attempts']}")
            print(f"   origin     : {record['sourceTopic']}"
                  f"[{record['sourcePartition']}] @ {record['sourceOffset']}")
            print(f"   failedAt   : {ts:%Y-%m-%d %H:%M:%S}")
            print(f"   payload    : {len(record['rawPayload'])} bytes")

            if producer is not None:
                producer.produce(
                    topic=config.ORDERS_TOPIC,
                    key=msg.key(),
                    value=record["rawPayload"],
                )
                producer.poll(0)
                print("   -> replayed to", config.ORDERS_TOPIC)
    finally:
        if producer is not None:
            producer.flush(10)
        consumer.close()

    print("-" * 60)
    print(f"Total DLQ records: {seen}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

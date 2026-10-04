#!/usr/bin/env python3
"""Order producer.

Serialises each order with Avro (schema fetched from / registered with the
Confluent Schema Registry) and publishes it to the `orders` topic.

Usage
-----
    python -m src.producer                     # 100 random valid orders, then stop
    python -m src.producer --count 250         # a different volume
    python -m src.producer --count 100 --interval 0.1
    python -m src.producer --scenario          # deterministic demo messages
"""
import argparse
import random
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from src import config
from src.models import Order

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def build_serializer() -> AvroSerializer:
    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    return AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=config.load_schema("order.avsc"),
        to_dict=lambda order, ctx: order.to_dict(),
    )


def delivery_report(err, msg) -> None:
    """Called once per message when the broker acknowledges it (or gives up)."""
    if err is not None:
        print(f"  !! delivery FAILED: {err}", file=sys.stderr)
    else:
        print(
            f"  -> delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
        )


def random_order(order_id: int) -> Order:
    return Order(
        orderId=str(order_id),
        product=random.choice(PRODUCTS),
        price=round(random.uniform(5.0, 500.0), 2),
    )


def scenario_orders(start_id: int) -> list:
    """A fixed set of orders that exercises every path in the consumer.

    The consumer recognises these product names and simulates the matching
    failure, which makes the live demonstration reproducible instead of
    depending on a random dice roll.
    """
    return [
        Order(str(start_id + 0), "Item1", 120.50),          # happy path
        Order(str(start_id + 1), "Item2", 80.00),           # happy path
        Order(str(start_id + 2), "FlakyItem", 200.00),      # fails twice, then succeeds -> RETRY
        Order(str(start_id + 3), "Item3", 45.25),           # happy path
        Order(str(start_id + 4), "AlwaysFailItem", 310.00), # always transient -> retries exhausted -> DLQ
        Order(str(start_id + 5), "Item4", -19.99),          # invalid price -> permanent -> DLQ
        Order(str(start_id + 6), "", 60.00),                # empty product -> permanent -> DLQ
        Order(str(start_id + 7), "Item5", 260.75),          # happy path
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce Avro-encoded order messages")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="number of random orders to send (default: 100; ignored with --scenario)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="seconds to wait between messages (default: 0.5)",
    )
    parser.add_argument("--start-id", type=int, default=1001, help="first orderId")
    parser.add_argument(
        "--scenario",
        action="store_true",
        help="send the fixed demo set (happy path + retry + DLQ cases)",
    )
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            # Durability settings: wait for all in-sync replicas, retry internally,
            # and keep ordering guarantees even while retrying.
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "linger.ms": 5,
        }
    )
    value_serializer = build_serializer()
    key_serializer = StringSerializer("utf_8")

    orders = scenario_orders(args.start_id) if args.scenario else [
        random_order(args.start_id + i) for i in range(args.count)
    ]

    print(f"Producing {len(orders)} order(s) to '{config.ORDERS_TOPIC}' "
          f"via {config.BOOTSTRAP_SERVERS}\n")

    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)
    for order in orders:
        print(f"[SEND] orderId={order.orderId:<6} product={order.product or '<empty>':<16} "
              f"price={order.price}")
        producer.produce(
            topic=config.ORDERS_TOPIC,
            # Keying by orderId puts all events for one order on the same
            # partition, which preserves their relative ordering.
            key=key_serializer(order.orderId),
            value=value_serializer(order, ctx),
            on_delivery=delivery_report,
        )
        producer.poll(0)          # serve delivery callbacks without blocking
        time.sleep(args.interval)

    remaining = producer.flush(15)
    if remaining:
        print(f"WARNING: {remaining} message(s) were not delivered", file=sys.stderr)
        return 1
    print("\nAll messages delivered.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

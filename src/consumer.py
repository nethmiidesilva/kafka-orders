#!/usr/bin/env python3
"""Order consumer.

Responsibilities
----------------
1. Deserialise Avro order messages using the Schema Registry.
2. Maintain a real-time running average of prices (overall and per product).
3. Retry transient processing failures with exponential backoff + jitter.
4. Route permanently failed messages to the Dead Letter Queue.
5. Commit offsets manually, only after a message has reached a terminal state
   (processed, or safely parked in the DLQ) - so nothing is silently lost.

Usage
-----
    python -m src.consumer
    python -m src.consumer --max-attempts 3 --failure-rate 0.2
"""
import argparse
import random
import signal
import sys
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src import config
from src.aggregator import PriceAggregator
from src.dlq import DLQPublisher
from src.models import Order, PermanentError, TransientError, validate

# Counts how many times we have attempted each orderId. Used only by the
# simulated "FlakyItem" fault so that it recovers after a couple of retries.
_attempt_history = defaultdict(int)

_running = True


def _handle_shutdown(signum, frame):
    global _running
    print("\nShutdown signal received, finishing current message...")
    _running = False


def process_order(order: Order, failure_rate: float) -> None:
    """Pretend to do the real work: enrich, persist to a database, call an API.

    Raises TransientError or PermanentError to drive the retry/DLQ machinery.
    """
    _attempt_history[order.orderId] += 1
    attempt = _attempt_history[order.orderId]

    # --- simulated faults, for the demonstration -----------------------------
    if order.product == "AlwaysFailItem":
        raise TransientError("downstream inventory service is unreachable")

    if order.product == "FlakyItem" and attempt <= 2:
        raise TransientError(f"database connection timed out (attempt {attempt})")

    if failure_rate and random.random() < failure_rate:
        raise TransientError("simulated random network glitch")
    # -------------------------------------------------------------------------

    # Real work would happen here. Kept trivial on purpose.
    time.sleep(0.01)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with full jitter, capped at BACKOFF_MAX_SECONDS."""
    window = min(config.BACKOFF_MAX_SECONDS, config.BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
    return random.uniform(0, window)


def main() -> int:
    parser = argparse.ArgumentParser(description="Consume and aggregate Avro order messages")
    parser.add_argument("--max-attempts", type=int, default=config.MAX_ATTEMPTS)
    parser.add_argument("--failure-rate", type=float, default=config.RANDOM_TRANSIENT_FAILURE_RATE,
                        help="probability that a valid order hits a simulated transient fault")
    parser.add_argument("--group", default=config.CONSUMER_GROUP)
    parser.add_argument("--from-beginning", action="store_true",
                        help="read the topic from offset 0 for a new consumer group")
    parser.add_argument("--reset-state", action="store_true",
                        help="discard the saved aggregation snapshot and start counting from zero")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    if args.reset_state and config.STATE_FILE.exists():
        config.STATE_FILE.unlink()
    aggregator = PriceAggregator.load(config.STATE_FILE)

    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=config.load_schema("order.avsc"),
        from_dict=lambda d, ctx: Order.from_dict(d),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            # Manual commits: we decide when a message is truly done.
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }
    )
    consumer.subscribe([config.ORDERS_TOPIC])
    dlq = DLQPublisher()

    print(f"Consuming '{config.ORDERS_TOPIC}' as group '{args.group}' "
          f"(max attempts={args.max_attempts}, DLQ='{config.DLQ_TOPIC}')")
    print("Press Ctrl+C to stop.\n")

    processed = dead_lettered = retried = 0

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw = msg.value()

            # ---------- 1. Deserialise ---------------------------------------
            try:
                order = deserializer(raw, SerializationContext(msg.topic(), MessageField.VALUE))
            except Exception as exc:  # noqa: BLE001 - any decode failure is permanent
                print(f"[DLQ ] undecodable message at offset {msg.offset()}: {exc}")
                dlq.send(
                    raw_payload=raw,
                    error_type="DeserializationError",
                    error_message=str(exc),
                    attempts=1,
                    source_topic=msg.topic(),
                    source_partition=msg.partition(),
                    source_offset=msg.offset(),
                    key=msg.key(),
                )
                dead_lettered += 1
                consumer.commit(msg, asynchronous=False)
                continue

            # ---------- 2. Validate (permanent failures skip retrying) -------
            try:
                validate(order)
            except PermanentError as exc:
                print(f"[DLQ ] orderId={order.orderId} rejected, no retry: {exc}")
                dlq.send(
                    raw_payload=raw,
                    error_type="ValidationError",
                    error_message=str(exc),
                    attempts=1,
                    source_topic=msg.topic(),
                    source_partition=msg.partition(),
                    source_offset=msg.offset(),
                    order_id=order.orderId,
                    key=msg.key(),
                )
                dead_lettered += 1
                consumer.commit(msg, asynchronous=False)
                continue

            # ---------- 3. Process with bounded retries -----------------------
            last_error = None
            succeeded = False
            for attempt in range(1, args.max_attempts + 1):
                try:
                    process_order(order, args.failure_rate)
                    succeeded = True
                    break
                except PermanentError as exc:
                    last_error = exc
                    break
                except TransientError as exc:
                    last_error = exc
                    if attempt < args.max_attempts:
                        delay = backoff_seconds(attempt)
                        retried += 1
                        print(f"[RETRY] orderId={order.orderId} attempt {attempt}/"
                              f"{args.max_attempts} failed ({exc}); retrying in {delay:.2f}s")
                        time.sleep(delay)

            # ---------- 4. Terminal outcome ----------------------------------
            if succeeded:
                aggregator.add(order.product, order.price)
                processed += 1
                print(f"[OK  ] orderId={order.orderId:<6} product={order.product:<14} "
                      f"price={order.price:>8.2f} | {aggregator.one_line(order.product)}")
                aggregator.save(config.STATE_FILE)
            else:
                error_type = ("ValidationError" if isinstance(last_error, PermanentError)
                              else "RetriesExhausted")
                print(f"[DLQ ] orderId={order.orderId} -> DLQ after {args.max_attempts} "
                      f"attempt(s): {last_error}")
                dlq.send(
                    raw_payload=raw,
                    error_type=error_type,
                    error_message=str(last_error),
                    attempts=args.max_attempts,
                    source_topic=msg.topic(),
                    source_partition=msg.partition(),
                    source_offset=msg.offset(),
                    order_id=order.orderId,
                    key=msg.key(),
                )
                dead_lettered += 1

            # Commit only now, once the message has reached a terminal state.
            consumer.commit(msg, asynchronous=False)

    except KafkaException as exc:
        print(f"Fatal Kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        print(aggregator.report())
        print(f" Succeeded: {processed} | Retries performed: {retried} | "
              f"Sent to DLQ: {dead_lettered}")
        aggregator.save(config.STATE_FILE)
        dlq.close()
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

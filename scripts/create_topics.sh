#!/usr/bin/env bash
# Create the topics used by this project. Auto-creation is disabled in
# docker-compose.yml on purpose: topics should be explicit, with a chosen
# partition count and retention.
set -euo pipefail

BROKER="${BROKER:-kafka:29092}"
ORDERS_TOPIC="${ORDERS_TOPIC:-orders}"
DLQ_TOPIC="${DLQ_TOPIC:-orders.DLQ}"

echo "Creating topic '$ORDERS_TOPIC' (3 partitions)..."
docker exec kafka kafka-topics --bootstrap-server "$BROKER" \
  --create --if-not-exists --topic "$ORDERS_TOPIC" \
  --partitions 3 --replication-factor 1

echo "Creating topic '$DLQ_TOPIC' (1 partition, 7-day retention)..."
docker exec kafka kafka-topics --bootstrap-server "$BROKER" \
  --create --if-not-exists --topic "$DLQ_TOPIC" \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

echo
docker exec kafka kafka-topics --bootstrap-server "$BROKER" --list

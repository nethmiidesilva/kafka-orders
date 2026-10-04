# Live Demonstration Runbook

A 7–9 minute walkthrough. Do the setup steps **before** the demo starts so you
are not watching Docker pull images in front of the examiner.

## Before the demo (do this 10 minutes early)

Start Docker Desktop first and wait for the whale icon to settle.

```bash
docker compose up -d
powershell -ExecutionPolicy Bypass -File .\scripts\create_topics.ps1
python -m pytest -q
python -m src.producer --count 3
```

`pytest` must print `12 passed`, and the warm-up run registers the schema so the
first demo message is not slowed by registration.

Then reset so the demo starts clean:

```bash
docker compose down -v && docker compose up -d && powershell -ExecutionPolicy Bypass -File .\scripts\create_topics.ps1
```

```bash
del .aggregator_state.json
```

Schema Registry is on **host port 8085** (container 8081). Port 8081 is left free
because Apache/XAMPP commonly occupies it.

Have four things open: **two terminals**, a browser on
<http://localhost:8080> (Kafka UI), and the code in an editor.

---

## Step 1 — Show the schema (45 s)

Open `schemas/order.avsc`.

> "The contract is three fields — orderId and product as strings, price as a
> float, exactly as the specification requires. This file is the single source
> of truth: the producer registers it with the Schema Registry, and the consumer
> retrieves it by ID rather than hard-coding a copy."

---

## Step 2 — Start the consumer (30 s)

**Terminal 1:**
```bash
python -m src.consumer --from-beginning --reset-state --group demo-1
```

Bump the group name (`demo-2`, `demo-3`…) for each rehearsal. `--from-beginning`
only applies to a group with no committed offsets, and `--reset-state` clears the
saved running average so the numbers start from zero on camera.

> "It is idle, waiting on `poll()`. Note that auto-commit is off — I commit
> offsets manually, and I'll explain why when we get to the DLQ."

---

## Step 3 — Happy path and the running average (90 s)

**Terminal 2:**
```bash
python -m src.producer --count 8 --interval 0.5
```

Point at the consumer output as it scrolls.

> "Every line shows the running average recalculated after each message. It is
> not recomputed from a stored list — it is an incremental update, constant time
> and constant memory per message, which is what makes it work on an unbounded
> stream. You can also see the per-product breakdown on the right."

---

## Step 4 — Prove Avro is really being used (60 s)

**Terminal 2:**
```bash
curl -s http://localhost:8085/subjects
curl -s http://localhost:8085/subjects/orders-value/versions/1 | python -m json.tool
```

Then in Kafka UI → Topics → `orders` → Messages, show a decoded message.

> "The schema was registered automatically on the first send. Each message on
> the wire is a 5-byte header — a magic byte plus the 4-byte schema ID — followed
> by the binary Avro payload. No field names on the wire, which is why Avro is
> so much more compact than JSON here."

Optional, if you want to make the point vividly:
```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic orders --from-beginning --max-messages 1
```
> "Raw bytes — unreadable without the registry. That is the trade-off Avro makes."

---

## Step 5 — Retry that succeeds (60 s)

**Terminal 2:**
```bash
python -m src.producer --scenario --interval 0.4
```

Watch for `orderId=1003`, product `FlakyItem`.

> "This one fails twice with a simulated database timeout, then succeeds on the
> third attempt. The delays are exponential with jitter — roughly 0.5, then 1,
> then 2 seconds, randomised. The jitter stops a fleet of consumers from
> retrying in perfect lockstep and re-crashing a service that is trying to recover."

---

## Step 6 — Retries exhausted → DLQ (45 s)

Same scenario run, `orderId=1005`, product `AlwaysFailItem`.

> "This one never recovers. After four attempts the consumer gives up and routes
> it to the dead letter queue rather than blocking the partition forever."

---

## Step 7 — Permanent failure skips retrying entirely (45 s)

Same run, `orderId=1006` with price `-19.99`, and `orderId=1007` with an empty
product name.

> "These go straight to the DLQ with zero retries. That is the key distinction in
> the design: a negative price is not going to become valid because I waited two
> seconds. Retrying permanent failures just wastes time and delays every message
> behind them."

---

## Step 8 — Inspect the DLQ (75 s)

**Terminal 2:**
```bash
python -m src.dlq_consumer --group demo-dlq-1
```

Use a fresh `--group` on each rehearsal — a group that has already read the DLQ
has committed its offsets and will show nothing.

> "Three records, each with the error type, the failure reason, how many attempts
> were made, and the exact source topic, partition and offset. The envelope also
> stores the original raw bytes — so once the bug is fixed, the messages can be
> replayed."

```bash
python -m src.dlq_consumer --replay
```

Point at Terminal 1 as the replayed messages are reprocessed.

---

## Step 9 — Shutdown report (30 s)

`Ctrl+C` in Terminal 1.

> "Final aggregation: total processed, average price, per-product breakdown, plus
> the count of retries performed and messages dead-lettered. The state is
> checkpointed to disk, so restarting the consumer continues the average rather
> than starting over."

---

## Optional extras, if there is time

**Consumer group rebalancing** — start a second consumer in a third terminal with
the same `--group`; the three partitions redistribute across the two instances.

**Higher failure rate** — `python -m src.consumer --failure-rate 0.4 --max-attempts 2`
produces a much noisier, more DLQ-heavy run.

---

## Questions you should be ready for

**Why Avro rather than JSON?**
Compact binary encoding — field names are not repeated on the wire — plus an
enforced, versioned contract. The registry rejects an incompatible schema at
publish time instead of letting a broken message reach production.

**What is the 5-byte prefix on each message?**
Confluent wire format: one magic byte (`0x00`) plus a 4-byte big-endian schema
ID. The consumer reads the ID, fetches that exact writer schema from the
registry, and decodes against it.

**How do you decide what is retryable?**
By exception type, not by guessing. `TransientError` for timeouts and unreachable
dependencies; `PermanentError` for anything wrong with the data itself.
Permanent failures are dead-lettered immediately.

**Why manual offset commits?**
So an offset advances only after a message has genuinely reached a terminal
state. With auto-commit, a crash mid-retry could move the offset past a message
that was never processed — a silent loss. This gives at-least-once delivery.

**Why flush the DLQ write before committing?**
If the process died between the commit and the DLQ write, the message would be
gone from both places. Doing it in this order means the worst case is a duplicate
DLQ record, which is recoverable.

**What happens if the consumer crashes mid-retry?**
The offset was never committed, so on restart the message is redelivered and the
retry budget starts fresh. That is at-least-once — deliberately chosen, since a
duplicate is far cheaper than a lost order.

**What is the weakness of your retry approach?**
Head-of-line blocking: sleeping in the poll loop stalls the whole partition. The
production fix is non-blocking retry topics with tiered delays. It also means
total backoff must stay under `max.poll.interval.ms`, or the broker declares the
consumer dead and rebalances.

**Why is the DLQ a different schema?**
An undecodable message has no Order to write. The envelope stores `bytes`, so it
can hold anything, while remaining Avro and registry-governed.

**How would you scale this?**
Add partitions and run one consumer instance per partition in the same group.
Because messages are keyed by orderId, per-order ordering survives the scale-out.
The aggregation would then need a shared store — or Kafka Streams with a state
store — rather than a per-instance JSON file.

**Is the running average exact?**
Yes. Welford's method is mathematically identical to the arithmetic mean and
numerically more stable than a large running sum. `tests/test_logic.py` asserts
it against `sum(prices)/len(prices)`.

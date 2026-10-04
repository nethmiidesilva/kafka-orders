# Kafka Order Processing — Avro, Real-Time Aggregation, Retries and DLQ

A Kafka pipeline that produces and consumes purchase-order messages using Avro
serialisation with the Confluent Schema Registry. The consumer maintains a
real-time running average of prices, retries transient failures with exponential
backoff, and parks permanently failed messages in a Dead Letter Queue.

**Language:** Python 3.9+ · **Client:** `confluent-kafka` · **Infra:** Docker Compose (Kafka in KRaft mode + Schema Registry + Kafka UI)

---

## 1. Architecture

```
                            ┌──────────────────────┐
                            │   Schema Registry    │
                            │  order.avsc (v1)     │
                            │  failed_order.avsc   │
                            └──────────┬───────────┘
             registers schema          │          fetches schema by ID
        ┌───────────────────────────────┴────────────────────────────┐
        │                                                            │
┌───────┴────────┐        ┌──────────────────┐            ┌──────────┴─────────┐
│    Producer    │ Avro   │  topic: orders   │   Avro     │      Consumer      │
│  src/producer  ├───────►│  (3 partitions)  ├───────────►│   src/consumer     │
└────────────────┘ bytes  └──────────────────┘            └─────┬──────────┬───┘
                                                                │          │
                                            success ────────────┘          │ terminal
                                                │                          │ failure
                                    ┌───────────▼───────────┐   ┌──────────▼─────────┐
                                    │  Running aggregation  │   │ topic: orders.DLQ  │
                                    │  count / avg / min /  │   │  Avro envelope +   │
                                    │  max, per product     │   │  error metadata    │
                                    └───────────────────────┘   └──────────┬─────────┘
                                                                           │
                                                              ┌────────────▼──────────┐
                                                              │  src/dlq_consumer     │
                                                              │  inspect / replay     │
                                                              └───────────────────────┘
```

### Message flow inside the consumer

```
poll() ──► deserialise ──► validate ──► process ──► aggregate ──► commit offset
              │               │            │
              │ fails         │ fails      │ TransientError
              │               │            ▼
              │               │      retry with exponential backoff + jitter
              │               │            │ attempts exhausted
              ▼               ▼            ▼
        ┌────────────────────────────────────────┐
        │  Dead Letter Queue  (orders.DLQ)       │
        │  raw bytes + errorType + attempts +    │
        │  source topic/partition/offset + time  │
        └────────────────────────────────────────┘
                          │
                    commit offset
```

---

## 2. Repository layout

| Path | Purpose |
|---|---|
| `schemas/order.avsc` | The order schema required by the assignment: `orderId` (string), `product` (string), `price` (float) |
| `schemas/failed_order.avsc` | Avro envelope for DLQ records — raw payload plus error metadata |
| `src/producer.py` | Publishes Avro-encoded orders; `--scenario` sends a deterministic demo set |
| `src/consumer.py` | Deserialises, validates, retries, aggregates, and routes failures to the DLQ |
| `src/aggregator.py` | Incremental running average (Welford), per-product breakdown, checkpointing |
| `src/dlq.py` | Writes the Avro DLQ envelope with error metadata and Kafka headers |
| `src/dlq_consumer.py` | Reads the DLQ; `--replay` pushes originals back onto `orders` |
| `src/models.py` | `Order` model and the `TransientError` / `PermanentError` taxonomy |
| `src/config.py` | Environment-driven configuration (brokers, topics, retry policy) |
| `tests/test_logic.py` | Unit tests for aggregation and validation — run without Kafka |
| `docker-compose.yml` | Kafka (KRaft), Schema Registry, Kafka UI |
| `scripts/create_topics.sh` | Creates `orders` (3 partitions) and `orders.DLQ` |
| `scripts/create_topics.ps1` | Same, for Windows PowerShell |

---

## 3. Setup

**Prerequisites:** Docker Desktop, Python 3.9+, and `make` (optional).

```bash
git clone <your-repo-url>
cd kafka-avro-orders

python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d                 # or: make up
./scripts/create_topics.sh           # or: make topics
```

On Windows PowerShell, `.sh` scripts do not run. Use the PowerShell equivalent
(Docker Desktop must be running first):

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d
powershell -ExecutionPolicy Bypass -File .\scripts\create_topics.ps1
```

Verify the stack is healthy:

```bash
curl http://localhost:8085/subjects           # Schema Registry -> []
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Kafka UI is at <http://localhost:8080> — useful for showing topics, messages and
registered schemas during the demonstration.

---

## 4. Running it

Open **two terminals**, both with the virtualenv activated.

**Terminal 1 — consumer:**
```bash
python -m src.consumer --from-beginning
```

**Terminal 2 — producer:**
```bash
python -m src.producer --scenario        # deterministic: happy path + retry + DLQ
python -m src.producer --count 20 --interval 0.3   # random orders
```

**Terminal 2 — inspect the DLQ afterwards:**
```bash
python -m src.dlq_consumer
```

Stop the consumer with `Ctrl+C`; it prints a final aggregation report.

### Sample consumer output

```
[OK  ] orderId=1001   product=Item1          price=  120.50 | count=1 avg=120.50 min=120.50 max=120.50 | Item1: n=1 avg=120.50
[OK  ] orderId=1002   product=Item2          price=   80.00 | count=2 avg=100.25 min=80.00 max=120.50 | Item2: n=1 avg=80.00
[RETRY] orderId=1003 attempt 1/4 failed (database connection timed out (attempt 1)); retrying in 0.31s
[RETRY] orderId=1003 attempt 2/4 failed (database connection timed out (attempt 2)); retrying in 0.87s
[OK  ] orderId=1003   product=FlakyItem      price=  200.00 | count=3 avg=133.50 ...
[RETRY] orderId=1005 attempt 1/4 failed (downstream inventory service is unreachable); retrying in 0.42s
[RETRY] orderId=1005 attempt 2/4 failed (downstream inventory service is unreachable); retrying in 1.55s
[RETRY] orderId=1005 attempt 3/4 failed (downstream inventory service is unreachable); retrying in 2.90s
[DLQ ] orderId=1005 -> DLQ after 4 attempt(s): downstream inventory service is unreachable
[DLQ ] orderId=1006 rejected, no retry: price must be positive, got -19.99
```

### Useful flags

| Command | Effect |
|---|---|
| `--failure-rate 0.3` (consumer) | 30% of valid orders hit a simulated transient fault |
| `--max-attempts 2` (consumer) | Tighter retry budget — more messages reach the DLQ |
| `--reset-state` (consumer) | Start the running average from zero |
| `--group my-group` (consumer) | New consumer group; run twice to demo partition rebalancing |
| `--replay` (dlq_consumer) | Re-publish DLQ payloads back to `orders` |

---

## 5. How each requirement is met

### Avro serialisation
Both topics are schema-governed. The producer registers `order.avsc` under the
subject `orders-value` on first send; the consumer fetches the writer schema by
the ID embedded in each message's 5-byte Confluent wire-format prefix (magic byte
`0x00` + 4-byte schema ID). Nothing hard-codes a schema on the read side, so a
compatible schema evolution does not require a consumer redeploy.

**Show this in the demo:**
```bash
curl -s http://localhost:8085/subjects | python -m json.tool
curl -s http://localhost:8085/subjects/orders-value/versions/1 | python -m json.tool
```

### Real-time aggregation
`aggregator.py` updates the mean incrementally using Welford's method:

```
mean_n = mean_(n-1) + (x_n - mean_(n-1)) / n
```

This is O(1) per message in time and memory — no list of prices grows without
bound — and it avoids the precision loss of accumulating one large running sum.
Statistics are kept globally and per product, and checkpointed to
`.aggregator_state.json` so the average survives a consumer restart.

### Retry logic
Failures are classified before anything is retried:

- **`TransientError`** — a timeout, an unreachable service, a 503. Retried up to
  `--max-attempts` times.
- **`PermanentError`** — bad or invalid data. Retried **zero** times and sent
  straight to the DLQ, because no number of retries will make a negative price valid.

Backoff is exponential with **full jitter**: `random.uniform(0, base * 2^(n-1))`,
capped at 8 s. The jitter matters when several consumer instances retry the same
downstream dependency — without it they synchronise and hammer the recovering
service in lockstep.

### Dead Letter Queue
Three distinct paths reach `orders.DLQ`:

1. `DeserializationError` — the bytes are not valid Avro for the registered schema.
2. `ValidationError` — decoded fine, but the data breaks a business rule.
3. `RetriesExhausted` — a transient fault that never cleared.

Each DLQ record is an Avro `FailedOrder` envelope carrying the **original raw
bytes** plus `errorType`, `errorMessage`, `attempts`, source topic/partition/offset
and a failure timestamp. The same details are duplicated into Kafka headers so
they are readable without decoding the payload. Because the raw bytes are kept,
`--replay` can push the messages back through the pipeline once the underlying
problem is fixed.

---

## 6. Design decisions worth defending

**Manual offset commits (`enable.auto.commit=False`).** Offsets are committed
only after a message reaches a terminal state — processed, or safely written to
the DLQ. With auto-commit, a crash mid-retry could advance the offset past a
message that was never handled, silently losing it. This gives at-least-once
delivery; a duplicate is recoverable, a lost order is not.

**DLQ write is flushed before the offset commit.** If the process died between
committing and writing, the message would vanish. Ordering the two operations
this way means the worst case is a duplicate DLQ entry, not a lost one.

**The DLQ has its own schema, not `order.avsc`.** An undecodable message has no
`Order` to write. The envelope holds `bytes`, so the DLQ can accept anything —
while still being Avro-encoded and registry-governed.

**Messages are keyed by `orderId`.** Kafka guarantees ordering only within a
partition, and the key determines the partition. Keying by `orderId` means all
events for one order stay in order even across three partitions.

**Idempotent producer (`enable.idempotence=True`, `acks=all`).** Prevents
duplicates caused by the producer's own internal retries after a network timeout.

**Known limitation — head-of-line blocking.** Retrying in-process with `sleep`
blocks the whole partition while one message backs off. Fine at this scale and
much easier to demonstrate; the production alternative is non-blocking retry
topics (`orders.retry.5s`, `orders.retry.30s`, …) where a failed message is
forwarded to a delay topic and the original offset is committed immediately.
Also note that total backoff must stay well below `max.poll.interval.ms`
(300 s here) or the broker will consider the consumer dead and rebalance.

---

## 7. Tests

```bash
python -m pytest -q      # 12 tests, no Kafka required
```

Covers: the running average against the arithmetic mean, incremental updates,
per-product isolation, state persistence across restarts, and validation rules.

---

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `Connection refused` on `localhost:9092` | `docker compose ps` — wait for the broker to be healthy |
| Schema Registry returns nothing | It starts after Kafka; retry `curl http://localhost:8085/subjects` |
| Consumer prints nothing | It defaults to `latest`; use `--from-beginning`, or produce while it runs |
| Consumer stops receiving after a restart | Offsets are committed — either use a new `--group` or `--from-beginning` on a fresh group |
| `UNKNOWN_TOPIC_OR_PART` | Run `./scripts/create_topics.sh` (`scripts/create_topics.ps1` on Windows); auto-creation is disabled on purpose |
| `Microsoft Visual C++ 14.0 or greater is required` on `pip install` | Your Python has no prebuilt wheel for the pinned version. `confluent-kafka>=2.6.0` and `fastavro>=1.10.0` are the first releases with Python 3.13 wheels |
| `No module named src.consumer` | Run from the repository root, the directory containing `src/` |
| `open //./pipe/dockerDesktopLinuxEngine` | Docker Desktop is not running — start it and wait for the whale icon to settle |
| Port 8080/8081/9092 already in use | Change the host-side port in `docker-compose.yml` |

"""Central configuration.

Everything is read from environment variables with sensible defaults, so the same
code runs against the local Docker stack or a remote cluster without edits.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schemas"

# --- Connection -------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")

# --- Topics -----------------------------------------------------------------
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processor")

# --- Retry policy -----------------------------------------------------------
# A transient failure is retried MAX_ATTEMPTS times in total (1 initial try +
# MAX_ATTEMPTS-1 retries). Backoff grows exponentially and is jittered so that a
# fleet of consumers does not retry in lockstep ("thundering herd").
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "0.5"))
BACKOFF_MAX_SECONDS = float(os.getenv("BACKOFF_MAX_SECONDS", "8"))

# --- Failure injection (so the demo is reproducible) ------------------------
# Probability that a perfectly valid order hits a simulated transient fault.
RANDOM_TRANSIENT_FAILURE_RATE = float(os.getenv("RANDOM_TRANSIENT_FAILURE_RATE", "0.0"))

# State file where the aggregator snapshot is written, so the running average
# survives a consumer restart.
STATE_FILE = Path(os.getenv("STATE_FILE", str(PROJECT_ROOT / ".aggregator_state.json")))


def load_schema(filename: str) -> str:
    """Read an .avsc file from schemas/ and return it as a string."""
    return (SCHEMA_DIR / filename).read_text(encoding="utf-8")

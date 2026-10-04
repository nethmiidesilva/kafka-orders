"""Unit tests for the pure logic. These run without Kafka, which is exactly why
the aggregation and validation code lives in its own modules.

    python -m pytest -q
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.aggregator import PriceAggregator  # noqa: E402
from src.models import Order, PermanentError, validate  # noqa: E402


def test_running_average_matches_arithmetic_mean():
    agg = PriceAggregator()
    prices = [10.0, 20.0, 30.0, 45.5, 100.25]
    for p in prices:
        agg.add("Item1", p)
    assert agg.overall.count == len(prices)
    assert agg.overall.mean == pytest.approx(sum(prices) / len(prices))
    assert agg.overall.minimum == 10.0
    assert agg.overall.maximum == 100.25


def test_average_updates_incrementally():
    agg = PriceAggregator()
    agg.add("Item1", 100.0)
    assert agg.overall.mean == pytest.approx(100.0)
    agg.add("Item1", 200.0)
    assert agg.overall.mean == pytest.approx(150.0)
    agg.add("Item1", 300.0)
    assert agg.overall.mean == pytest.approx(200.0)


def test_per_product_breakdown_is_independent():
    agg = PriceAggregator()
    agg.add("Item1", 10.0)
    agg.add("Item2", 90.0)
    agg.add("Item1", 30.0)
    assert agg.per_product["Item1"].mean == pytest.approx(20.0)
    assert agg.per_product["Item2"].mean == pytest.approx(90.0)
    assert agg.overall.mean == pytest.approx(130.0 / 3)


def test_state_survives_a_restart(tmp_path):
    path = tmp_path / "state.json"
    agg = PriceAggregator()
    agg.add("Item1", 50.0)
    agg.add("Item2", 150.0)
    agg.save(path)

    restored = PriceAggregator.load(path)
    assert restored.overall.count == 2
    assert restored.overall.mean == pytest.approx(100.0)
    assert restored.per_product["Item2"].mean == pytest.approx(150.0)
    assert json.loads(path.read_text())["overall"]["count"] == 2


def test_load_missing_file_returns_empty_aggregator(tmp_path):
    agg = PriceAggregator.load(tmp_path / "nope.json")
    assert agg.overall.count == 0


@pytest.mark.parametrize(
    "order",
    [
        Order("1001", "Item1", -1.0),
        Order("1002", "Item1", 0.0),
        Order("1003", "", 10.0),
        Order("", "Item1", 10.0),
        Order("1004", "Item1", 2_000_000.0),
    ],
)
def test_invalid_orders_are_permanent_failures(order):
    with pytest.raises(PermanentError):
        validate(order)


def test_valid_order_passes_validation():
    validate(Order("1001", "Item1", 99.99))


def test_order_round_trips_through_dict():
    original = Order("1001", "Item1", 12.5)
    assert Order.from_dict(original.to_dict()) == original

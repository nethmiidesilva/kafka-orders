"""Domain model and the error taxonomy that drives the retry / DLQ decision.

The single most important design idea in this project:

    TransientError  -> worth retrying (network blip, database timeout, 503)
    PermanentError  -> retrying can never help (bad data, failed validation)

Retrying a PermanentError just burns CPU and delays the rest of the partition,
so permanent failures go straight to the Dead Letter Queue.
"""
from dataclasses import dataclass


@dataclass
class Order:
    orderId: str
    product: str
    price: float

    def to_dict(self) -> dict:
        return {"orderId": self.orderId, "product": self.product, "price": self.price}

    @staticmethod
    def from_dict(d: dict) -> "Order":
        return Order(orderId=d["orderId"], product=d["product"], price=float(d["price"]))


class TransientError(Exception):
    """A failure that may succeed if we simply try again shortly."""


class PermanentError(Exception):
    """A failure that will never succeed, no matter how many times we retry."""


def validate(order: Order) -> None:
    """Business validation. Any breach here is permanent - the data itself is wrong."""
    if not order.orderId or not order.orderId.strip():
        raise PermanentError("orderId is empty")
    if not order.product or not order.product.strip():
        raise PermanentError("product name is empty")
    if order.price <= 0:
        raise PermanentError(f"price must be positive, got {order.price}")
    if order.price > 1_000_000:
        raise PermanentError(f"price {order.price} exceeds the sane upper bound")

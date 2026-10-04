"""Real-time aggregation of order prices.

The running average is computed incrementally with Welford's method:

    mean_n = mean_(n-1) + (x_n - mean_(n-1)) / n

rather than by keeping a growing list of prices. This is O(1) in both time and
memory per message, which is what makes it suitable for an unbounded stream, and
it is numerically more stable than accumulating a large running sum.
"""
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Optional


@dataclass
class Stats:
    count: int = 0
    mean: float = 0.0
    total: float = 0.0
    minimum: Optional[float] = None
    maximum: Optional[float] = None

    def update(self, value: float) -> None:
        self.count += 1
        self.total += value
        # Welford incremental mean
        self.mean += (value - self.mean) / self.count
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)


@dataclass
class PriceAggregator:
    """Global stats plus a per-product breakdown."""

    overall: Stats = field(default_factory=Stats)
    per_product: Dict[str, Stats] = field(default_factory=dict)

    def add(self, product: str, price: float) -> None:
        self.overall.update(price)
        self.per_product.setdefault(product, Stats()).update(price)

    # --- reporting ----------------------------------------------------------
    def one_line(self, product: str) -> str:
        p = self.per_product[product]
        return (
            f"count={self.overall.count} "
            f"avg={self.overall.mean:.2f} "
            f"min={self.overall.minimum:.2f} max={self.overall.maximum:.2f} "
            f"| {product}: n={p.count} avg={p.mean:.2f}"
        )

    def report(self) -> str:
        lines = [
            "",
            "=" * 62,
            " RUNNING AGGREGATION SNAPSHOT",
            "=" * 62,
            f" Orders processed : {self.overall.count}",
            f" Total value      : {self.overall.total:.2f}",
            f" Average price    : {self.overall.mean:.2f}",
        ]
        if self.overall.count:
            lines.append(f" Min / Max price  : {self.overall.minimum:.2f} / {self.overall.maximum:.2f}")
        lines.append("-" * 62)
        lines.append(f" {'PRODUCT':<24}{'COUNT':>8}{'AVG':>12}{'TOTAL':>14}")
        for name in sorted(self.per_product):
            s = self.per_product[name]
            lines.append(f" {name:<24}{s.count:>8}{s.mean:>12.2f}{s.total:>14.2f}")
        lines.append("=" * 62)
        return "\n".join(lines)

    # --- persistence --------------------------------------------------------
    def save(self, path: Path) -> None:
        payload = {
            "overall": asdict(self.overall),
            "per_product": {k: asdict(v) for k, v in self.per_product.items()},
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "PriceAggregator":
        if not path.exists():
            return cls()
        raw = json.loads(path.read_text(encoding="utf-8"))
        agg = cls(overall=Stats(**raw["overall"]))
        agg.per_product = {k: Stats(**v) for k, v in raw.get("per_product", {}).items()}
        return agg

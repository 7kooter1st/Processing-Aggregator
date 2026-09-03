from __future__ import annotations

from collections import defaultdict


class MetricsRegistry:
    def __init__(self) -> None:
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}

    def inc(self, name: str, value: float = 1) -> None:
        self._counters[name] += value

    def set_gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def render_prometheus(self) -> str:
        lines = ["# HELP comparator_info Comparator pipeline metrics", "# TYPE comparator_info gauge"]
        for name, value in sorted(self._counters.items()):
            lines.append(f"comparator_{name}_total {value}")
        for name, value in sorted(self._gauges.items()):
            lines.append(f"comparator_{name} {value}")
        return "\n".join(lines) + "\n"


metrics = MetricsRegistry()

from collections.abc import Iterator
from typing import Any


class DemoSource:
    """Example third-party source yielding one predictable record."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.record_id = int(config.get("record_id", 42))

    def records(self) -> Iterator[dict[str, object]]:
        yield {"id": self.record_id, "source": "separately-installed-plugin"}


def create_source(config: dict[str, Any]) -> DemoSource:
    return DemoSource(config)

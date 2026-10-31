from importlib.metadata import EntryPoint, entry_points
from typing import Any

PLUGIN_GROUPS = {
    "sources": "datapipe.sources",
    "transforms": "datapipe.transforms",
    "sinks": "datapipe.sinks",
}


def plugin_entry_points() -> dict[str, dict[str, EntryPoint]]:
    discovered = entry_points()
    return {
        short_name: {point.name: point for point in discovered.select(group=group)}
        for short_name, group in PLUGIN_GROUPS.items()
    }


def discover_plugins() -> dict[str, list[str]]:
    return {group: sorted(entries) for group, entries in plugin_entry_points().items()}


def load_plugin(kind: str, name: str) -> Any:
    try:
        point = plugin_entry_points()[kind][name]
    except KeyError as exc:
        raise ValueError(f"unknown {kind} plugin {name!r}") from exc
    return point.load()

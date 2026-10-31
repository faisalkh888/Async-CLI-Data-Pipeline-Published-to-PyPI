import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator
from datetime import date, datetime
from typing import Any

from datapipe.config.schema import AggregateDaily, Deduplicate, FieldMapping, Filter, HeavyTransform
from datapipe.core.protocols import Record


def apply_transform(records: Iterable[Record], transform: Any) -> Iterator[Record]:
    if isinstance(transform, FieldMapping):
        for record in records:
            yield {target: record[source] for source, target in transform.mapping.items() if source in record}
        return
    if isinstance(transform, Filter):
        for record in records:
            if record_matches(record, transform):
                yield record
        return
    if isinstance(transform, Deduplicate):
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        connection = sqlite3.connect(db_path)
        try:
            connection.execute("CREATE TABLE seen (value TEXT PRIMARY KEY)")
            for record in records:
                value = json.dumps(record.get(transform.key), sort_keys=True, separators=(",", ":"), default=str)
                cursor = connection.execute("INSERT OR IGNORE INTO seen(value) VALUES (?)", (value,))
                if cursor.rowcount:
                    yield record
        finally:
            connection.close()
            os.unlink(db_path)
        return
    if isinstance(transform, AggregateDaily):
        totals: dict[str, float] = {}
        for record in records:
            raw_date = record.get(transform.date_field)
            raw_value = record.get(transform.value_field)
            if raw_date is None or raw_value is None:
                continue
            parsed = datetime.fromisoformat(str(raw_date))
            day = date(parsed.year, parsed.month, parsed.day).isoformat()
            totals[day] = totals.get(day, 0.0) + float(str(raw_value))
        for day, total in sorted(totals.items()):
            yield {transform.output_date_field: day, transform.output_value_field: total}
        return
    if isinstance(transform, HeavyTransform):
        for record in records:
            updated = dict(record)
            seed = str(record.get(transform.field, "")).encode()
            digest: bytes = seed
            for _ in range(transform.iterations):
                digest = hashlib.sha256(digest).digest()
            updated[transform.field] = digest.hex()
            yield updated
        return
    raise TypeError(f"unsupported transform: {type(transform).__name__}")


def record_matches(record: Record, transform: Filter) -> bool:
    actual: Any = record.get(transform.field)
    expected: Any = transform.value
    if transform.operator == "eq":
        return bool(actual == expected)
    if transform.operator == "ne":
        return bool(actual != expected)
    if transform.operator == "gt":
        return bool(actual is not None and actual > expected)
    if transform.operator == "gte":
        return bool(actual is not None and actual >= expected)
    if transform.operator == "lt":
        return bool(actual is not None and actual < expected)
    if transform.operator == "lte":
        return bool(actual is not None and actual <= expected)
    if transform.operator == "contains":
        return bool(actual is not None and expected in actual)
    return transform.field in record

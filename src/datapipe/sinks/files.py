import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from datapipe.core.protocols import Record


def _atomic_target(path: Path) -> tuple[int, Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    return fd, Path(temporary)


def write_jsonl(records: Iterable[Record], path: Path) -> int:
    fd, temporary = _atomic_target(path)
    count = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return count


def write_parquet(records: Iterable[Record], path: Path, batch_size: int) -> int:
    fd, temporary = _atomic_target(path)
    os.close(fd)
    writer = None
    count = 0
    batch: list[Record] = []
    try:
        for record in records:
            batch.append(record)
            if len(batch) >= batch_size:
                writer = _write_batch(batch, temporary, writer)
                count += len(batch)
                batch.clear()
        if batch:
            writer = _write_batch(batch, temporary, writer)
            count += len(batch)
        if writer is None:
            writer = pq.ParquetWriter(temporary, pa.schema([]))
        writer.close()
        os.replace(temporary, path)
    except BaseException:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        raise
    return count


def _write_batch(batch: list[Record], path: Path, writer: Any) -> Any:
    table = pa.Table.from_pylist(batch)
    if writer is None:
        writer = pq.ParquetWriter(path, table.schema)
    elif table.schema != writer.schema:
        table = table.cast(writer.schema, safe=False)
    writer.write_table(table)
    return writer


class AtomicRecordWriter:
    """Incremental writer that atomically replaces its target on successful close."""

    def __init__(self, path: Path, kind: str, batch_size: int = 1000):
        self.path = path
        self.kind = kind
        self.batch_size = batch_size
        self.fd, self.temporary = _atomic_target(path)
        self.count = 0
        self.batch: list[Record] = []
        self.parquet_writer: Any | None = None
        self.stream: TextIO | None = os.fdopen(self.fd, "w", encoding="utf-8") if kind == "jsonl" else None
        if kind != "jsonl":
            os.close(self.fd)

    def write(self, record: Record) -> None:
        if self.kind == "jsonl":
            assert self.stream is not None
            self.stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        else:
            self.batch.append(record)
            if len(self.batch) >= self.batch_size:
                self._flush()
        self.count += 1

    def _flush(self) -> None:
        if self.batch:
            self.parquet_writer = _write_batch(self.batch, self.temporary, self.parquet_writer)
            self.batch.clear()

    def commit(self) -> int:
        try:
            if self.kind == "jsonl":
                assert self.stream is not None
                self.stream.close()
            else:
                self._flush()
                if self.parquet_writer is None:
                    self.parquet_writer = pq.ParquetWriter(self.temporary, pa.schema([]))
                assert self.parquet_writer is not None
                self.parquet_writer.close()
            os.replace(self.temporary, self.path)
            return self.count
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        if self.stream is not None and not self.stream.closed:
            self.stream.close()
        if self.parquet_writer is not None:
            self.parquet_writer.close()
        self.temporary.unlink(missing_ok=True)


class PartitionedParquetWriter:
    """Write partition files atomically; the path is a dataset directory."""

    def __init__(self, path: Path, fields: list[str], batch_size: int):
        self.path = path
        self.fields = fields
        self.batch_size = batch_size
        self.writers: dict[str, AtomicRecordWriter] = {}
        self.count = 0

    def write(self, record: Record) -> None:
        values = tuple(record.get(field) for field in self.fields)
        partition_key = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
        writer = self.writers.get(partition_key)
        if writer is None:
            parts = [f"{quote(field, safe='-_.')}={quote(str(value if value is not None else '__null__'), safe='-_.')}" for field, value in zip(self.fields, values)]
            filename = f"part-{hashlib.sha256(partition_key.encode()).hexdigest()[:16]}.parquet"
            writer = AtomicRecordWriter(self.path.joinpath(*parts, filename), "parquet", self.batch_size)
            self.writers[partition_key] = writer
        writer.write(record)
        self.count += 1

    def commit(self) -> int:
        try:
            for writer in self.writers.values():
                writer.commit()
            return self.count
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        for writer in self.writers.values():
            writer.abort()

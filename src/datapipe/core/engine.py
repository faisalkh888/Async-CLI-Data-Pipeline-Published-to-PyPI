import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
from collections import defaultdict
from collections.abc import AsyncIterator
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from datapipe.config.schema import (
    AggregateDaily,
    CsvSource,
    Deduplicate,
    FieldMapping,
    Filter,
    HeavyTransform,
    HttpSource,
    JsonlSource,
    PipelineConfig,
    PluginSource,
    PluginTransform,
)
from datapipe.core.protocols import Record
from datapipe.core.validation import validate_record
from datapipe.plugins.registry import load_plugin
from datapipe.sinks.files import AtomicRecordWriter, PartitionedParquetWriter
from datapipe.sources.files import csv_records, http_records, jsonl_records
from datapipe.transforms.operations import record_matches

logger = logging.getLogger("datapipe")
_END = object()


def _output_path(config: PipelineConfig, relative_path: Path) -> Path:
    base = config.pipeline.output_dir.resolve()
    target = (base / relative_path).resolve()
    if target != base and base not in target.parents:
        raise ValueError(f"sink path escapes output_dir: {relative_path}")
    return target


def _heavy(record: Record, transform: HeavyTransform) -> Record:
    result = dict(record)
    digest = str(record.get(transform.field, "")).encode()
    for _ in range(transform.iterations):
        digest = hashlib.sha256(digest).digest()
    result[transform.field] = digest.hex()
    return result


class _TransformState:
    def __init__(self, config: PipelineConfig):
        self.transforms = config.transforms
        self.plugin_transforms = {
            index: _make_plugin("transforms", transform.plugin, transform.config)
            for index, transform in enumerate(config.transforms)
            if isinstance(transform, PluginTransform)
        }
        self.totals: dict[str, float] = defaultdict(float)
        fd, temp_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.temp_path = Path(temp_path)
        self.db = sqlite3.connect(self.temp_path)
        self.db.execute("CREATE TABLE seen (transform_index INTEGER, value TEXT, PRIMARY KEY(transform_index,value))")

    async def apply(self, record: Record, pool: ProcessPoolExecutor | None) -> Record | None:
        current: Record | None = record
        loop = asyncio.get_running_loop()
        for index, transform in enumerate(self.transforms):
            if current is None:
                return None
            if isinstance(transform, AggregateDaily):
                raw_date, raw_value = current.get(transform.date_field), current.get(transform.value_field)
                if raw_date is None or raw_value is None:
                    return None
                parsed = datetime.fromisoformat(str(raw_date))
                day = date(parsed.year, parsed.month, parsed.day).isoformat()
                self.totals[day] += float(str(raw_value))
                return None
            if isinstance(transform, FieldMapping):
                current = {target: current[source] for source, target in transform.mapping.items() if source in current}
            elif isinstance(transform, Filter):
                if not record_matches(current, transform):
                    return None
            elif isinstance(transform, Deduplicate):
                value = json.dumps(current.get(transform.key), sort_keys=True, separators=(",", ":"), default=str)
                cursor = self.db.execute(
                    "INSERT OR IGNORE INTO seen(transform_index,value) VALUES (?,?)", (index, value)
                )
                if not cursor.rowcount:
                    return None
            elif isinstance(transform, HeavyTransform):
                current = (
                    await loop.run_in_executor(pool, _heavy, current, transform)
                    if pool is not None
                    else _heavy(current, transform)
                )
            elif isinstance(transform, PluginTransform):
                plugin = self.plugin_transforms[index]
                operation = getattr(plugin, "apply", plugin)
                current = operation(current)
        return current

    def finish(self) -> list[Record]:
        result: list[Record] = []
        for transform in self.transforms:
            if isinstance(transform, AggregateDaily):
                result = [
                    {transform.output_date_field: day, transform.output_value_field: total}
                    for day, total in sorted(self.totals.items())
                ]
        return result

    def close(self) -> None:
        self.db.close()
        self.temp_path.unlink(missing_ok=True)


async def _source_records(
    source: Any,
    config: PipelineConfig,
    client: httpx.AsyncClient,
    semaphores: dict[str, asyncio.Semaphore],
    cursors: dict[str, str | None],
) -> AsyncIterator[Record]:
    if isinstance(source, CsvSource):
        for record in csv_records(source.path):
            yield record
    elif isinstance(source, JsonlSource):
        for record in jsonl_records(source.path):
            yield record
    elif isinstance(source, HttpSource):
        host = source.url.host
        if host is None:
            raise ValueError("HTTP source URL must include a host")
        semaphore = semaphores.setdefault(host, asyncio.Semaphore(config.settings.max_concurrency_per_host))
        async for record in http_records(
            source, config.settings, client, semaphore,
            cursor_callback=lambda value: cursors.__setitem__(source.name, value),
        ):
            yield record
    elif isinstance(source, PluginSource):
        plugin = _make_plugin("sources", source.plugin, source.config)
        source_method = getattr(plugin, "records", plugin)
        stream = source_method()
        if hasattr(stream, "__aiter__"):
            async for record in stream:
                yield record
        else:
            for record in stream:
                yield record
    else:
        raise TypeError(f"unsupported source: {type(source).__name__}")


async def run_pipeline(config: PipelineConfig, *, dry_run: bool = False, resume: bool = False) -> dict[str, int]:
    run_id = str(uuid4())
    sinks = [sink for sink in config.sinks if not sink.dead_letters]
    dead_letter_sinks = [sink for sink in config.sinks if sink.dead_letters]
    output_queues: dict[str, asyncio.Queue[Any]] = {
        sink.name: asyncio.Queue(maxsize=config.settings.queue_size) for sink in sinks
    }
    queues = dict(output_queues)
    if dead_letter_sinks:
        queues[dead_letter_sinks[0].name] = asyncio.Queue(maxsize=config.settings.queue_size)
    errors: list[dict[str, str]] = []
    semaphores: dict[str, asyncio.Semaphore] = {}
    cursors: dict[str, str | None] = {}
    results: dict[str, int] = {}
    outputs = {
        sink.name: _output_path(config, sink.path)
        for sink in config.sinks
        if sink.type != "plugin"
    }

    if resume:
        recovered = _recover_checkpoint(config, outputs)
        if recovered is not None:
            return recovered
    async with httpx.AsyncClient(timeout=config.settings.request_timeout, follow_redirects=True) as client:
        pool = ProcessPoolExecutor(max_workers=config.settings.heavy_workers) if config.settings.heavy_workers else None
        try:
            async with asyncio.TaskGroup() as group:
                async def consume(sink: Any) -> None:
                    if dry_run:
                        writer = None
                    elif sink.type == "plugin":
                        writer = _make_plugin("sinks", sink.plugin, sink.config)
                    elif sink.partition_by:
                        writer = PartitionedParquetWriter(outputs[sink.name], sink.partition_by, config.settings.batch_size)
                    else:
                        writer = AtomicRecordWriter(outputs[sink.name], sink.type, config.settings.batch_size)
                    state = _TransformState(config) if not sink.dead_letters else None
                    count = 0
                    try:
                        while True:
                            item = await queues[sink.name].get()
                            if item is _END:
                                break
                            if sink.dead_letters:
                                assert writer is not None
                                writer.write(item)
                                count += 1
                                continue
                            assert state is not None
                            try:
                                record = await state.apply(item["record"], pool)
                            except Exception as exc:
                                errors.append({"source": item["source"], "error": str(exc)})
                                logger.error(
                                    "record_transform_failed",
                                    extra={"source": item["source"], "run_id": run_id},
                                )
                                if dead_letter_sinks:
                                    await queues[dead_letter_sinks[0].name].put(
                                        {"source": item["source"], "error": str(exc), "record": item["record"]}
                                    )
                                elif config.pipeline.fail_fast:
                                    raise
                                continue
                            if record is not None:
                                if writer:
                                    writer.write(record)
                                count += 1
                        if state:
                            for record in state.finish():
                                if writer:
                                    writer.write(record)
                                count += 1
                        if writer:
                            committed = writer.commit()
                            results[sink.name] = committed if isinstance(committed, int) else count
                        else:
                            results[sink.name] = count
                    except BaseException:
                        if writer:
                            writer.abort()
                        raise
                    finally:
                        if state:
                            state.close()

                for sink in config.sinks:
                    group.create_task(consume(sink))

                async def produce(source: Any) -> None:
                    try:
                        async for raw in _source_records(source, config, client, semaphores, cursors):
                            try:
                                if not isinstance(raw, dict):
                                    raise TypeError("source records must be JSON objects")
                                record = validate_record(raw, config.validation)
                            except Exception as exc:
                                if dead_letter_sinks:
                                    await queues[dead_letter_sinks[0].name].put({"source": source.name, "error": str(exc), "record": raw})
                                    continue
                                raise
                            for queue in output_queues.values():
                                await queue.put({"source": source.name, "record": record})
                        logger.info("source_complete", extra={"source": source.name, "run_id": run_id})
                    except Exception as exc:
                        errors.append({"source": source.name, "error": str(exc)})
                        logger.exception("source_failed", extra={"source": source.name, "run_id": run_id})
                        if config.pipeline.fail_fast:
                            raise

                producers = [group.create_task(produce(source)) for source in config.sources]
                async def stop_consumers() -> None:
                    for task in producers:
                        await task
                    for queue in queues.values():
                        await queue.put(_END)

                group.create_task(stop_consumers())
        finally:
            if pool:
                pool.shutdown(wait=True, cancel_futures=True)
    if errors:
        results["source_errors"] = len(errors)
    if not dry_run:
        _write_checkpoint(config, run_id, outputs, errors, cursors, results)
    return results


def _config_fingerprint(config: PipelineConfig) -> str:
    serialized = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def _recover_checkpoint(config: PipelineConfig, outputs: dict[str, Path]) -> dict[str, int] | None:
    try:
        checkpoint = json.loads(config.settings.checkpoint_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if checkpoint.get("config_fingerprint") != _config_fingerprint(config):
        return None
    if not outputs or any(not target.exists() for target in outputs.values()):
        return None
    recovered = checkpoint.get("results")
    return recovered if isinstance(recovered, dict) else None


def _write_checkpoint(
    config: PipelineConfig,
    run_id: str,
    outputs: dict[str, Path],
    errors: list[dict[str, str]],
    cursors: dict[str, str | None],
    results: dict[str, int],
) -> None:
    path = config.settings.checkpoint_file
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "pipeline": config.pipeline.name,
        "config_fingerprint": _config_fingerprint(config),
        "committed_cursors": cursors,
        "results": results,
        "committed_outputs": {name: str(target) for name, target in outputs.items() if target.exists()},
        "source_errors": errors,
    }
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _make_plugin(kind: str, name: str, config: dict[str, object]) -> Any:
    factory = load_plugin(kind, name)
    return factory(config)

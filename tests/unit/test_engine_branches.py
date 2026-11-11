import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from datapipe.config.schema import PipelineConfig
from datapipe.core import engine
from datapipe.core.engine import _heavy, _source_records, _TransformState


def _config(tmp_path: Path, *, transforms=None, sources=None, sinks=None, fail_fast=False, validation=None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source_file = tmp_path / "source.jsonl"
    source_file.write_text('{"id":1}\n', encoding="utf-8")
    return PipelineConfig.model_validate(
        {
            "pipeline": {"name": "coverage", "output_dir": str(tmp_path / "out"), "fail_fast": fail_fast},
            "settings": {"checkpoint_file": str(tmp_path / "checkpoint.json"), "queue_size": 1},
            "sources": sources or [{"name": "input", "type": "jsonl", "path": str(source_file)}],
            "transforms": transforms or [],
            "validation": validation or {},
            "sinks": sinks or [{"name": "output", "type": "jsonl", "path": "out.jsonl"}],
        }
    )


def test_transform_state_mapping_filter_dedup_and_serial_heavy(tmp_path):
    config = _config(
        tmp_path,
        transforms=[
            {"type": "field_mapping", "mapping": {"raw": "value", "absent": "not_added"}},
            {"type": "filter", "field": "value", "operator": "gt", "value": 0},
            {"type": "deduplicate", "key": "value"},
            {"type": "heavy_transform", "field": "value", "iterations": 2},
        ],
    )
    state = _TransformState(config)
    try:
        first = asyncio.run(state.apply({"raw": 4}, None))
        duplicate = asyncio.run(state.apply({"raw": 4}, None))
        filtered = asyncio.run(state.apply({"raw": -1}, None))
        assert first is not None and len(str(first["value"])) == 64
        assert duplicate is None
        assert filtered is None
    finally:
        temp_path = state.temp_path
        state.close()
    assert not temp_path.exists()
    assert _heavy({"value": "seed"}, config.transforms[-1])["value"] != "seed"


def test_aggregate_state_skips_missing_values_and_finishes_sorted(tmp_path):
    config = _config(
        tmp_path,
        transforms=[
            {
                "type": "aggregate_daily",
                "date_field": "created",
                "value_field": "amount",
                "output_date_field": "day",
                "output_value_field": "sum",
            }
        ],
    )
    state = _TransformState(config)
    try:
        assert asyncio.run(state.apply({"created": "2026-02-02T00:00:00", "amount": 2}, None)) is None
        assert asyncio.run(state.apply({"created": "2026-02-01T00:00:00", "amount": 3}, None)) is None
        assert asyncio.run(state.apply({"created": "2026-02-03T00:00:00"}, None)) is None
        assert state.finish() == [
            {"day": "2026-02-01", "sum": 3.0},
            {"day": "2026-02-02", "sum": 2.0},
        ]
    finally:
        state.close()


@pytest.mark.parametrize("plugin_shape", ["apply_method", "callable"])
def test_plugin_transform_accepts_method_or_callable(tmp_path, monkeypatch, plugin_shape):
    config = _config(tmp_path, transforms=[{"type": "plugin", "plugin": "double"}])
    if plugin_shape == "apply_method":
        plugin = SimpleNamespace(apply=lambda record: {**record, "value": record["value"] * 2})
    else:
        plugin = lambda record: {**record, "value": record["value"] * 3}
    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: plugin)
    state = _TransformState(config)
    try:
        result = asyncio.run(state.apply({"value": 2}, None))
        assert result == {"value": 4 if plugin_shape == "apply_method" else 6}
    finally:
        state.close()


def test_source_records_reads_csv_jsonl_http_and_plugins(tmp_path, monkeypatch):
    config = _config(tmp_path)
    csv_path = tmp_path / "records.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    jsonl_path = tmp_path / "records.jsonl"
    jsonl_path.write_text('{"id":2}\n', encoding="utf-8")
    monkeypatch.setattr(engine, "csv_records", lambda _path: iter([{"id": 1}]))
    monkeypatch.setattr(engine, "jsonl_records", lambda _path: iter([{"id": 2}]))

    async def collect(source):
        async with httpx.AsyncClient() as client:
            return [
                row async for row in _source_records(source, config, client, {}, {})
            ]

    assert asyncio.run(collect(engine.CsvSource(name="csv", type="csv", path=csv_path))) == [{"id": 1}]
    assert asyncio.run(collect(engine.JsonlSource(name="jsonl", type="jsonl", path=jsonl_path))) == [{"id": 2}]

    async def fake_http(_source, _settings, _client, _semaphore, cursor_callback):
        cursor_callback("cursor-2")
        yield {"id": 3}

    monkeypatch.setattr(engine, "http_records", fake_http)
    cursors = {}
    http_source = engine.HttpSource(name="http", type="http", url="https://example.test/data")

    async def collect_http():
        async with httpx.AsyncClient() as client:
            return [row async for row in _source_records(http_source, config, client, {}, cursors)]

    assert asyncio.run(collect_http()) == [{"id": 3}]
    assert cursors["http"] == "cursor-2"

    def sync_factory(_config):
        return SimpleNamespace(records=lambda: iter([{"id": 4}]))

    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: sync_factory({}))
    plugin_source = engine.PluginSource(name="plugin", type="plugin", plugin="sync")
    assert asyncio.run(collect(plugin_source)) == [{"id": 4}]

    class AsyncSource:
        async def records(self):
            yield {"id": 5}

    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: AsyncSource())
    assert asyncio.run(collect(engine.PluginSource(name="plugin", type="plugin", plugin="async"))) == [{"id": 5}]


def test_source_records_rejects_unsupported_source(tmp_path):
    config = _config(tmp_path)

    async def collect():
        async with httpx.AsyncClient() as client:
            return [row async for row in _source_records(object(), config, client, {}, {})]

    with pytest.raises(TypeError, match="unsupported source"):
        asyncio.run(collect())


def test_dry_run_writes_no_output_or_checkpoint(tmp_path):
    config = _config(tmp_path)
    result = asyncio.run(engine.run_pipeline(config, dry_run=True))
    assert result == {"output": 1}
    assert not (tmp_path / "out/out.jsonl").exists()
    assert not (tmp_path / "checkpoint.json").exists()


def test_source_failure_isolated_and_fail_fast_aborts(tmp_path, monkeypatch):
    class BrokenSource:
        def records(self):
            yield {"id": 1}
            raise RuntimeError("source broke")

    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: BrokenSource())
    source = {"name": "broken", "type": "plugin", "plugin": "broken"}
    isolated = _config(tmp_path / "isolated", sources=[source])
    result = asyncio.run(engine.run_pipeline(isolated))
    assert result["source_errors"] == 1
    assert (tmp_path / "isolated/out/out.jsonl").exists()

    fail_fast = _config(tmp_path / "fail-fast", sources=[source], fail_fast=True)
    with pytest.raises(ExceptionGroup):
        asyncio.run(engine.run_pipeline(fail_fast))
    assert not (tmp_path / "fail-fast/out/out.jsonl").exists()
    assert not (tmp_path / "fail-fast/checkpoint.json").exists()


def test_invalid_records_route_to_dead_letter_sink(tmp_path, monkeypatch):
    class InvalidSource:
        def records(self):
            yield ["not", "an", "object"]
            yield {"id": "not-an-integer"}

    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: InvalidSource())
    config = _config(
        tmp_path,
        sources=[{"name": "invalid", "type": "plugin", "plugin": "invalid"}],
        validation={"field_types": {"id": "integer"}, "required_fields": ["id"]},
        sinks=[
            {"name": "output", "type": "jsonl", "path": "out.jsonl"},
            {"name": "dead", "type": "jsonl", "path": "dead.jsonl", "dead_letters": True},
        ],
    )
    result = asyncio.run(engine.run_pipeline(config))
    assert result["output"] == 0
    assert result["dead"] == 2


@pytest.mark.parametrize(
    ("dead_letters", "fail_fast"),
    [(True, False), (False, False), (False, True)],
)
def test_transform_failures_are_dead_lettered_isolated_or_fail_fast(
    tmp_path, monkeypatch, dead_letters, fail_fast
):
    class BrokenTransform:
        def apply(self, _record):
            raise ValueError("transform broke")

    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: BrokenTransform())
    sinks = [{"name": "output", "type": "jsonl", "path": "out.jsonl"}]
    if dead_letters:
        sinks.append({"name": "dead", "type": "jsonl", "path": "dead.jsonl", "dead_letters": True})
    config = _config(
        tmp_path,
        transforms=[{"type": "plugin", "plugin": "broken"}],
        sinks=sinks,
        fail_fast=fail_fast,
    )

    if fail_fast:
        with pytest.raises(ExceptionGroup):
            asyncio.run(engine.run_pipeline(config))
        assert not (tmp_path / "out/out.jsonl").exists()
        assert not (tmp_path / "checkpoint.json").exists()
    else:
        result = asyncio.run(engine.run_pipeline(config))
        assert result["source_errors"] == 1
        if dead_letters:
            assert result["dead"] == 1
        else:
            assert result["output"] == 0


def test_plugin_transform_none_stops_remaining_transforms(tmp_path, monkeypatch):
    config = _config(
        tmp_path,
        transforms=[
            {"type": "plugin", "plugin": "drop"},
            {"type": "field_mapping", "mapping": {"value": "mapped"}},
        ],
    )
    monkeypatch.setattr(engine, "_make_plugin", lambda *_args: lambda _record: None)
    state = _TransformState(config)
    try:
        assert asyncio.run(state.apply({"value": "x"}, None)) is None
    finally:
        state.close()


def test_bad_checkpoint_is_ignored_and_checkpoint_replace_cleans_temp(tmp_path, monkeypatch):
    config = _config(tmp_path)
    outputs = {"output": tmp_path / "out" / "out.jsonl"}
    assert engine._recover_checkpoint(config, outputs) is None
    config.settings.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    config.settings.checkpoint_file.write_text("not json", encoding="utf-8")
    assert engine._recover_checkpoint(config, outputs) is None

    def fail_replace(_source, _destination):
        raise OSError("replace failed")

    monkeypatch.setattr(engine.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        engine._write_checkpoint(config, "run", outputs, [], {}, {})
    assert not list(config.settings.checkpoint_file.parent.glob(".checkpoint.json.*.tmp"))

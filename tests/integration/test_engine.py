import asyncio
import json

import pytest

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline


def _config(tmp_path, *, fail_fast=False):
    first = tmp_path / "one.jsonl"
    second = tmp_path / "two.jsonl"
    first.write_text('{"id":1,"amount":3}\n{"id":1,"amount":3}\n', encoding="utf-8")
    second.write_text('{"id":2,"amount":7}\n{"id":"bad","amount":8}\n', encoding="utf-8")
    return PipelineConfig.model_validate({
        "pipeline": {"name": "test", "output_dir": str(tmp_path / "out"), "fail_fast": fail_fast},
        "settings": {"queue_size": 1, "batch_size": 1, "checkpoint_file": str(tmp_path / "checkpoint.json")},
        "sources": [
            {"name": "one", "type": "jsonl", "path": str(first)},
            {"name": "two", "type": "jsonl", "path": str(second)},
        ],
        "validation": {"field_types": {"id": "integer"}, "required_fields": ["id"]},
        "transforms": [{"type": "deduplicate", "key": "id"}],
        "sinks": [
            {"name": "data", "type": "jsonl", "path": "data.jsonl"},
            {"name": "dead", "type": "jsonl", "path": "dead.jsonl", "dead_letters": True},
        ],
    })


def test_async_pipeline_routes_dead_letters_and_checkpoints(tmp_path):
    config = _config(tmp_path)
    result = asyncio.run(run_pipeline(config))
    rows = [json.loads(line) for line in (tmp_path / "out/data.jsonl").read_text().splitlines()]
    dead = [json.loads(line) for line in (tmp_path / "out/dead.jsonl").read_text().splitlines()]
    assert result["data"] == 2
    assert len(rows) == 2
    assert dead[0]["source"] == "two"
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text())
    assert checkpoint["committed_outputs"]["data"].endswith("data.jsonl")


def test_resume_reuses_matching_committed_run(tmp_path):
    config = _config(tmp_path)
    first = asyncio.run(run_pipeline(config))
    second = asyncio.run(run_pipeline(config, resume=True))
    assert first == second


def test_sink_path_traversal_is_rejected(tmp_path):
    config = _config(tmp_path)
    config.sinks[0].path = __import__("pathlib").Path("../../outside.jsonl")
    with pytest.raises(ValueError, match="escapes output_dir"):
        asyncio.run(run_pipeline(config))


def test_cancellation_aborts_atomic_outputs_and_propagates(tmp_path, monkeypatch):
    from datapipe.core import engine

    class SlowSource:
        async def records(self):
            await asyncio.sleep(60)
            yield {"id": 1}

    monkeypatch.setattr(engine, "load_plugin", lambda kind, name: lambda config: SlowSource())
    config = PipelineConfig.model_validate({
        "pipeline": {"name": "cancel", "output_dir": str(tmp_path / "out")},
        "settings": {"checkpoint_file": str(tmp_path / "checkpoint.json"), "queue_size": 1},
        "sources": [{"name": "slow", "type": "plugin", "plugin": "slow"}],
        "sinks": [{"name": "out", "type": "jsonl", "path": "out.jsonl"}],
    })

    async def run_and_cancel():
        task = asyncio.create_task(run_pipeline(config))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_and_cancel())
    assert not (tmp_path / "out/out.jsonl").exists()
    assert not (tmp_path / "checkpoint.json").exists()


def test_heavy_transform_runs_in_process_pool(tmp_path):
    source = tmp_path / "source.jsonl"
    source.write_text('{"value":"seed"}\n', encoding="utf-8")
    config = PipelineConfig.model_validate({
        "pipeline": {"name": "pool", "output_dir": str(tmp_path / "out")},
        "settings": {"heavy_workers": 1, "checkpoint_file": str(tmp_path / "checkpoint.json")},
        "sources": [{"name": "in", "type": "jsonl", "path": str(source)}],
        "transforms": [{"type": "heavy_transform", "field": "value", "iterations": 2}],
        "sinks": [{"name": "out", "type": "jsonl", "path": "out.jsonl"}],
    })
    asyncio.run(run_pipeline(config))
    result = json.loads((tmp_path / "out/out.jsonl").read_text().strip())
    assert len(result["value"]) == 64

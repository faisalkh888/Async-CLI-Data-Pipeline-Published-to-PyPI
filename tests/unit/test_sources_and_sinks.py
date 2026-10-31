import asyncio
import json

import httpx
import pyarrow.parquet as pq

from datapipe.config.schema import HttpSource, Settings
from datapipe.sinks.files import AtomicRecordWriter, PartitionedParquetWriter
from datapipe.sources.files import csv_records, http_records, jsonl_records


def test_csv_and_jsonl_stream_records(tmp_path):
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("id,name\n1,A\n", encoding="utf-8")
    jsonl_path = tmp_path / "in.jsonl"
    jsonl_path.write_text('{"id":2}\n\n{"id":3}\n', encoding="utf-8")
    assert list(csv_records(csv_path)) == [{"id": "1", "name": "A"}]
    assert list(jsonl_records(jsonl_path)) == [{"id": 2}, {"id": 3}]


def test_atomic_jsonl_and_parquet_output(tmp_path):
    records = [{"id": 1, "group": "a"}, {"id": 2, "group": "b"}]
    json_path = tmp_path / "out.jsonl"
    writer = AtomicRecordWriter(json_path, "jsonl")
    for record in records:
        writer.write(record)
    assert writer.commit() == 2
    assert [json.loads(line) for line in json_path.read_text().splitlines()] == records

    parquet_path = tmp_path / "out.parquet"
    writer = AtomicRecordWriter(parquet_path, "parquet", batch_size=1)
    for record in records:
        writer.write(record)
    assert writer.commit() == 2
    assert pq.read_table(parquet_path).to_pylist() == records


def test_partitioned_parquet_writes_atomic_partition_files(tmp_path):
    writer = PartitionedParquetWriter(tmp_path / "dataset", ["group"], 2)
    writer.write({"group": "west", "id": 1})
    writer.write({"group": "east/../../escape", "id": 2})
    assert writer.commit() == 2
    files = list((tmp_path / "dataset").rglob("*.parquet"))
    assert len(files) == 2
    assert all(tmp_path / "dataset" in path.parents for path in files)


def test_http_next_url_pagination():
    calls = []

    def handler(request: httpx.Request):
        calls.append(str(request.url))
        if request.url.path.endswith("/first"):
            return httpx.Response(200, json={"data": [{"id": 1}], "next": "/second"})
        return httpx.Response(200, json={"data": [{"id": 2}], "next": None})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        source = HttpSource(name="api", type="http", url="https://example.test/first", pagination="next_url")
        output = [record async for record in http_records(source, Settings(), client, asyncio.Semaphore(1))]
        await client.aclose()
        return output

    assert asyncio.run(run()) == [{"id": 1}, {"id": 2}]
    assert len(calls) == 2


def test_http_retries_server_error():
    attempts = 0

    def handler(request: httpx.Request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"data": [{"ok": True}]})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        source = HttpSource(name="api", type="http", url="https://example.test/data")
        output = [record async for record in http_records(source, Settings(max_retries=1), client, asyncio.Semaphore(1))]
        await client.aclose()
        return output

    assert asyncio.run(run()) == [{"ok": True}]
    assert attempts == 2

import argparse
import asyncio
import json
import threading
import time
from pathlib import Path

import psutil

from datapipe.config.schema import PipelineConfig
from datapipe.core.engine import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=2_000_000)
    parser.add_argument("--work-dir", type=Path, default=Path(".benchmark"))
    parser.add_argument("--heavy-iterations", type=int, default=0)
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--queue-size", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=4096)
    args = parser.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    source, output = args.work_dir / "records.jsonl", args.work_dir / "output"
    with source.open("w", encoding="utf-8") as stream:
        for index in range(args.records):
            stream.write(json.dumps({"id": index, "group": index % 100, "value": index / 10}) + "\n")
    config = PipelineConfig.model_validate({
        "pipeline": {"name": "memory-benchmark", "output_dir": str(output), "fail_fast": True},
        "settings": {
            "queue_size": args.queue_size,
            "batch_size": args.batch_size,
            "heavy_workers": args.workers,
            "checkpoint_file": str(args.work_dir / "checkpoint.json"),
        },
        "sources": [{"name": "records", "type": "jsonl", "path": str(source)}],
        "transforms": ([{
            "type": "heavy_transform",
            "field": "value",
            "iterations": args.heavy_iterations,
        }] if args.heavy_iterations else []),
        "sinks": [{"name": "parquet", "type": "parquet", "path": "records.parquet"}],
    })
    process = psutil.Process()
    peak_rss = [process.memory_info().rss]
    stop = threading.Event()

    def sample() -> None:
        while not stop.wait(0.05):
            peak_rss[0] = max(peak_rss[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    started = time.perf_counter()
    try:
        result = asyncio.run(run_pipeline(config))
    finally:
        stop.set()
        sampler.join()
    elapsed = time.perf_counter() - started
    print(json.dumps({
        "records": result.get("parquet", 0),
        "seconds": round(elapsed, 3),
        "records_per_second": round(args.records / elapsed, 1),
        "heavy_iterations": args.heavy_iterations,
        "workers": args.workers,
        "queue_size": args.queue_size,
        "batch_size": args.batch_size,
        "peak_rss_mib": round(peak_rss[0] / (1024 * 1024), 1),
        "output_bytes": (output / "records.parquet").stat().st_size,
    }, indent=2))


if __name__ == "__main__":
    main()

"""Profile the actual heavy-transform worker function in process workers."""

from __future__ import annotations

import argparse
import cProfile
import pstats
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from datapipe.config.schema import HeavyTransform
from datapipe.core.engine import _heavy_batch
from datapipe.core.protocols import Record


def _profile_batch(
    records: list[Record], transform: HeavyTransform, profile_path: str
) -> tuple[int, str]:
    profiler = cProfile.Profile()
    transformed = profiler.runcall(_heavy_batch, records, transform)
    profiler.dump_stats(profile_path)
    return len(transformed), profile_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=4096)
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--profile-dir", type=Path, default=Path(".benchmark/worker-profiles"))
    args = parser.parse_args()
    if min(args.records, args.iterations, args.workers, args.batch_size) < 1:
        parser.error("records, iterations, workers, and batch-size must all be positive")

    args.profile_dir.mkdir(parents=True, exist_ok=True)
    transform = HeavyTransform(type="heavy_transform", field="value", iterations=args.iterations)
    records: list[Record] = [
        {"id": index, "value": index / 10}
        for index in range(args.records)
    ]
    chunks = [records[index : index + args.batch_size] for index in range(0, len(records), args.batch_size)]
    profile_paths = [args.profile_dir / f"worker-task-{index:04d}.prof" for index in range(len(chunks))]

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = list(
            pool.map(
                _profile_batch,
                chunks,
                [transform] * len(chunks),
                [str(path) for path in profile_paths],
            )
        )

    processed = sum(count for count, _ in results)
    print(f"Profiled {processed} records in {len(profile_paths)} worker tasks ({args.workers} workers).")
    stats = pstats.Stats(str(profile_paths[0]))
    for path in profile_paths[1:]:
        stats.add(str(path))
    stats.strip_dirs().sort_stats("cumtime").print_stats(20)


if __name__ == "__main__":
    main()

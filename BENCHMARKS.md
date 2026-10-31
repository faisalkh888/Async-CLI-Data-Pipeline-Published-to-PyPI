# Benchmarks

The benchmark creates a JSONL source incrementally, runs it through the streaming engine,
and samples process resident memory. It does not materialize the two-million-record input
or output in a Python list. Run it on an otherwise idle machine for meaningful results:

```sh
uv run python benchmarks/benchmark_pipeline.py --records 2000000
```

The report includes elapsed time, records per second, and peak sampled RSS. The RSS
sampler runs every 50 ms, so a shorter transient peak can be missed. Results depend on
disk, CPU, Python, and PyArrow versions; no speedup is assumed. `heavy_workers=0` is the
sequential baseline. To compare process-pool hashing, pass `--heavy-iterations 1000
--workers 4`, then repeat with `--workers 0`. Keep the input, iteration count, and machine
conditions fixed when comparing results.

## Recorded run

On Windows with CPython 3.12.14, the default two-million-record run completed in 16.881
seconds (118,475.9 records/second) with 60.6 MiB peak sampled RSS and a 22,735,802-byte
Parquet output. This measures JSONL parsing and Parquet writing with no transforms on the
current development machine; it is a baseline, not a cross-machine performance claim.
The raw JSON report is in [`benchmarks/results.json`](benchmarks/results.json).

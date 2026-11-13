# Benchmarks

The benchmark creates a JSONL source incrementally, runs it through the streaming engine,
and samples process resident memory. It does not materialize the two-million-record input
or output in a Python list. Run it on an otherwise idle machine for meaningful results:

```sh
uv run python benchmarks/benchmark_pipeline.py --records 2000000
```

The report includes elapsed time, records per second, queue and batch sizes, and peak
sampled RSS. The RSS sampler runs every 50 ms, so a shorter transient peak can be missed.
Results depend on disk, CPU, Python, and PyArrow versions; no speedup is assumed.
`heavy_workers=0` is the sequential baseline. The benchmark accepts `--queue-size` and
`--batch-size` so the same settings can be used in both modes. Use a different `--work-dir`
for every run, and run sequential and parallel measurements one at a time.

## CPU-heavy transform comparison

The engine sends each heavy-transform batch to the configured process workers concurrently.
Previously it awaited one executor task at a time, which left the other workers idle. The
current worker path splits each batch across the configured workers and awaits all pieces
together.

On Windows with CPython 3.14.7 and an AMD Ryzen 7 7840HS, three full-pipeline runs used
10,000 records, 2,000 SHA-256 iterations per record, queue size 8,192, batch size 2,048,
and four workers. Each measurement includes JSONL generation, transformation, and Parquet
output. The sequential times were 25.663, 25.540, and 27.431 seconds (median 25.663 s).
The four-worker times were 8.221, 8.695, and 8.376 seconds (median 8.376 s). The median
speedup is `25.663 / 8.376 = 3.06x`; two of the three paired measurements exceeded 3x, and
one was 2.94x. The median therefore meets the 3x target on this machine, with enough run
to-run variation that results should be repeated on the target machine. All six runs wrote
736,867-byte Parquet outputs. Raw samples and profiling summary are in
[`benchmarks/heavy-transform-results.json`](benchmarks/heavy-transform-results.json).

To repeat the comparison, run each command separately and use a fresh work directory:

```powershell
uv run python benchmarks/benchmark_pipeline.py --records 10000 --heavy-iterations 2000 --workers 0 --queue-size 8192 --batch-size 2048 --work-dir .benchmark\seq
uv run python benchmarks/benchmark_pipeline.py --records 10000 --heavy-iterations 2000 --workers 4 --queue-size 8192 --batch-size 2048 --work-dir .benchmark\parallel
```

The engine profiles actual worker processes with this command:

```sh
uv run python benchmarks/profile_heavy_transform.py --records 4096 --iterations 2000 --workers 4 --batch-size 1024 --profile-dir .benchmark/worker-profiles
```

The profile aggregated 16.4 million calls across four worker tasks. The main costs were
the `_heavy` Python loop and the SHA-256 digest/OpenSSL calls. Profiling adds overhead, so
those profile timings are diagnostic and are not used to calculate speedup. The benchmark's
RSS sampler tracks the parent process only; it does not include worker memory.

## Recorded run

On Windows with CPython 3.12.14, the default two-million-record run completed in 16.881
seconds (118,475.9 records/second) with 60.6 MiB peak sampled RSS and a 22,735,802-byte
Parquet output. This measures JSONL parsing and Parquet writing with no transforms on the
current development machine; it is a baseline, not a cross-machine performance claim.
The raw JSON report is in [`benchmarks/results.json`](benchmarks/results.json).

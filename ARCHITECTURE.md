# Architecture

## Runtime flow

The CLI loads TOML into strict Pydantic v2 models and resolves local source and output
paths from the configuration file. `asyncio.TaskGroup` starts one producer per source
and one consumer per sink. Every queue is bounded by `settings.queue_size`; a slow sink
therefore applies backpressure to producers. HTTP sources share an `httpx.AsyncClient`
and host-scoped semaphores. File readers yield records incrementally.

Each output consumer validates and transforms records independently so multiple sinks
can receive the same stream. Invalid input records are sent to the configured dead-letter
queue. Source exceptions are reported and isolated unless `fail_fast = true`; cancellation
is allowed to propagate so TaskGroup can stop sibling work.

## Commit and recovery

JSONL output is written to a temporary file in the destination directory and atomically
replaced on success. Parquet uses bounded record batches and follows the same temporary
file protocol. Partitioned Parquet uses one temporary file per partition and atomically
replaces each partition file. Checkpoint JSON is itself fsynced and atomically replaced
only after all sinks finish. Its fingerprint covers the validated config, and its cursor
map records the continuation observed for each HTTP source. `--resume` reuses a fully
committed matching run. If a process stops before the checkpoint is written, restart
re-reads sources and safely replaces outputs; the current implementation does not claim
transactional exactly-once semantics across multiple sinks or support resuming a partially
written HTTP page.

Deduplication state is held in a temporary SQLite file. Aggregation retains one total per
date key. Heavy hashing can run in a bounded process pool controlled by `heavy_workers`.
Credentials are obtained from environment variables and are not copied to logs or
checkpoint data.

## Plugin API

Entry-point groups:

- `datapipe.sources`: factory `config -> object`, exposing `records()` as a sync or async
  iterable of record dictionaries.
- `datapipe.transforms`: factory `config -> object`, exposing `apply(record)` or returning
  a callable. Return a record to keep it or `None` to filter it.
- `datapipe.sinks`: factory `config -> object`, exposing `write(record)`, `commit()`, and
  `abort()`.

Plugin implementations run in-process and should treat configuration as untrusted input.
The host validates source records and controls output paths for built-in sinks.

## Security boundaries

No `eval`, pickle, or YAML loading is used. TOML rejects unknown fields. Built-in sink
paths are resolved and constrained beneath `output_dir`; partition values are URL-quoted
before becoming directory names. Secrets are looked up by environment variable name at
request time. Plugin code has the same process privileges as datapipe and must be
installed from a trusted package source.

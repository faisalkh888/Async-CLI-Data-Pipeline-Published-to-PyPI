# datapipe

`datapipe` is an async command-line data pipeline for streaming local files and HTTP APIs
through validation and transformations into Parquet or JSONL. It is packaged as a normal
Python CLI, with bounded queues, per-source failure isolation, atomic output commits,
checkpoints, entry-point plugins, and structured logs.

## Requirements and install

Python 3.12 or newer is required.

```sh
uv sync --locked --extra dev
uv run datapipe --version
uv run datapipe --help
```

The distribution name is `async-datapipe-cli`. The package has not been uploaded yet;
after release to TestPyPI, install it with:

```sh
pip install --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple async-datapipe-cli
```

## Example

```sh
uv run datapipe validate-config examples/pipeline.toml
uv run datapipe list-plugins
uv run datapipe run examples/pipeline.toml --dry-run
uv run datapipe run examples/pipeline.toml --json
```

`--dry-run` reads and transforms the input without writing output. `--resume` reuses
outputs only when the last fully committed run has the same validated configuration and
all its output paths still exist. A run that stops before commit can safely be rerun;
temporary files are discarded and the pipeline starts from its sources again.

## Configuration

See [`examples/pipeline.toml`](examples/pipeline.toml). Relative source paths and
`output_dir` are resolved from the configuration file. Relative sink paths are resolved
from `output_dir`, and path traversal outside that directory is rejected. The
[`examples/http-pipeline.toml`](examples/http-pipeline.toml) file shows HTTP pagination,
environment-based bearer authentication, validation, aggregation, and partitioned output;
its example API host must be replaced with a real endpoint before running.

Built-in sources are CSV, JSONL, and HTTP. HTTP supports no pagination, `next_url`, and
`page_number` pagination. Put bearer tokens in environment variables and reference them
with `auth_env`; tokens are never written to logs or checkpoints. HTTP retries use
exponential backoff with jitter. Configure `queue_size` to bound in-flight records and
`max_concurrency_per_host` to limit requests to each host.

Records can be validated with `[validation]` field types (`string`, `integer`, `number`,
`boolean`) and `required_fields`. Invalid records go to the optional JSONL sink marked
`dead_letters = true`. Transforms run in configured order: field mapping, filtering,
deduplication, daily aggregation, CPU-heavy hashing, or a registered transform plugin.
Deduplication uses a temporary SQLite database to avoid retaining all seen keys in RAM.

Parquet supports optional `partition_by` fields. Each partition file is staged and
atomically replaced. JSONL sinks also stage to a temporary sibling file before replace.
The checkpoint is updated only after all built-in sinks commit. Checkpoints capture the
committed run, source cursor positions, and output locations. A replay after a crash
before checkpoint commit starts over and atomically replaces outputs.

## Plugins

Packages register entry points under `datapipe.sources`, `datapipe.transforms`, and
`datapipe.sinks`. Each entry point is a factory that accepts a `dict` config. A source
factory returns an object with `records()` (sync or async iterable), a transform factory
returns an object with `apply(record)` (or a callable), and a sink factory returns an
object with `write(record)`, `commit()`, and `abort()` methods. Use `type = "plugin"`, a
`plugin` entry-point name, and optional `[...config]` values. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the contracts and recovery model.

## Development

```sh
uv run pytest
uv run ruff check .
uv run mypy src
uv build
```

CI runs the unit, integration, and property tests, lint, typing, and wheel build on
Python 3.12 and 3.13. See [BENCHMARKS.md](BENCHMARKS.md) for the two-million-record
memory benchmark. TestPyPI publishing is configured for GitHub Actions trusted
publishing on version tags; an authorized PyPI/TestPyPI account and GitHub environment
configuration are required to publish. See [RELEASE.md](RELEASE.md) for the setup.

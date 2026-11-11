import asyncio
import json
import logging
import os
import signal
from collections.abc import Callable
from pathlib import Path
from types import FrameType

import typer

from datapipe import __version__
from datapipe.config.loader import ConfigError, load_config
from datapipe.config.schema import HttpSource, PipelineConfig
from datapipe.core.engine import run_pipeline
from datapipe.observability.logging import SecretRedactionFilter
from datapipe.plugins.registry import discover_plugins

app = typer.Typer(
    no_args_is_help=True,
    invoke_without_command=True,
    help="Stream local files through a configured data pipeline.",
)


def _sigterm_canceller(
    loop: asyncio.AbstractEventLoop, task: asyncio.Task[dict[str, int]]
) -> Callable[[int, FrameType | None], None]:
    def request_cancel(_signum: int, _frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(task.cancel)

    return request_cancel


async def _run_with_sigterm(config: PipelineConfig, *, dry_run: bool, resume: bool) -> dict[str, int]:
    """Cancel the active pipeline on SIGTERM so sinks can discard staged output."""
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(run_pipeline(config, dry_run=dry_run, resume=resume))
    previous_handler = signal.getsignal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, _sigterm_canceller(loop, task))
    try:
        return await task
    finally:
        signal.signal(signal.SIGTERM, previous_handler)


def _logging(json_logs: bool, secrets: tuple[str, ...] = ()) -> None:
    if json_logs:
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                return json.dumps({"level": record.levelname, "message": record.getMessage(), **{
                    key: value for key, value in record.__dict__.items()
                    if key in {"source", "run_id"}
                }})
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    else:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", force=True)
    redaction_filter = SecretRedactionFilter(secrets)
    for root_handler in logging.getLogger().handlers:
        root_handler.addFilter(redaction_filter)


def _configured_secrets(config: PipelineConfig) -> tuple[str, ...]:
    return tuple(
        secret
        for source in config.sources
        if isinstance(source, HttpSource) and source.auth_env
        if (secret := os.environ.get(source.auth_env))
    )


@app.callback()
def main(version: bool = typer.Option(False, "--version", help="Show version and exit.")) -> None:
    if version:
        typer.echo(__version__)
        raise typer.Exit()


@app.command("validate-config")
def validate_config(path: Path) -> None:
    try:
        config = load_config(path)
    except ConfigError as exc:
        typer.echo(f"Invalid configuration: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Valid pipeline '{config.pipeline.name}' ({len(config.sources)} source(s), {len(config.sinks)} sink(s))")


@app.command("list-plugins")
def list_plugins() -> None:
    typer.echo("Built-in sources: csv, jsonl, http")
    typer.echo("Built-in transforms: field_mapping, filter, deduplicate, aggregate_daily, heavy_transform")
    typer.echo("Built-in sinks: parquet, jsonl")
    plugins = discover_plugins()
    for group, names in plugins.items():
        for name in names:
            typer.echo(f"{group}: {name}")


@app.command()
def run(
    path: Path,
    dry_run: bool = typer.Option(False, "--dry-run"),
    json_output: bool = typer.Option(False, "--json"),
    resume: bool = typer.Option(False, "--resume", help="Reuse outputs from the last fully committed matching run."),
) -> None:
    try:
        config = load_config(path)
        _logging(json_output, _configured_secrets(config))
        results = asyncio.run(_run_with_sigterm(config, dry_run=dry_run, resume=resume))
    except asyncio.CancelledError as exc:
        typer.echo("Interrupted: pipeline cancelled; uncommitted outputs discarded.", err=True)
        raise typer.Exit(code=130) from exc
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    payload = {"pipeline": config.pipeline.name, "dry_run": dry_run, "records": results}
    typer.echo(json.dumps(payload) if json_output else f"Completed {config.pipeline.name}: {results}")
    if results.get("source_errors", 0):
        raise typer.Exit(code=1)

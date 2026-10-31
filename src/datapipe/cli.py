import asyncio
import json
import logging
from pathlib import Path

import typer

from datapipe import __version__
from datapipe.config.loader import ConfigError, load_config
from datapipe.core.engine import run_pipeline
from datapipe.plugins.registry import discover_plugins

app = typer.Typer(
    no_args_is_help=True,
    invoke_without_command=True,
    help="Stream local files through a configured data pipeline.",
)


def _logging(json_logs: bool) -> None:
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
    _logging(json_output)
    try:
        config = load_config(path)
        results = asyncio.run(run_pipeline(config, dry_run=dry_run, resume=resume))
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    payload = {"pipeline": config.pipeline.name, "dry_run": dry_run, "records": results}
    typer.echo(json.dumps(payload) if json_output else f"Completed {config.pipeline.name}: {results}")
    if results.get("source_errors", 0):
        raise typer.Exit(code=1)

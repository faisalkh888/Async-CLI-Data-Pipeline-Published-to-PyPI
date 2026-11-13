from importlib.metadata import version

from typer.testing import CliRunner

from datapipe import __version__
from datapipe.cli import app

runner = CliRunner()


def test_version_option():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == version("async-datapipe-cli") == __version__


def test_validate_config_command():
    result = runner.invoke(app, ["validate-config", "examples/pipeline.toml"])
    assert result.exit_code == 0
    assert "Valid pipeline 'sales-ingestion'" in result.stdout


def test_list_plugins_command():
    result = runner.invoke(app, ["list-plugins"])
    assert result.exit_code == 0
    assert "Built-in sources: csv, jsonl, http" in result.stdout

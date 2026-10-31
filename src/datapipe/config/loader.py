import tomllib
from pathlib import Path

from pydantic import ValidationError

from datapipe.config.schema import CsvSource, JsonlSource, PipelineConfig


class ConfigError(ValueError):
    """Invalid or unreadable pipeline configuration."""


def load_config(path: Path) -> PipelineConfig:
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
        config = PipelineConfig.model_validate(raw)
        config.pipeline.output_dir = _relative_to(path, config.pipeline.output_dir)
        config.settings.checkpoint_file = _relative_to(path, config.settings.checkpoint_file)
        for source in config.sources:
            if isinstance(source, (CsvSource, JsonlSource)):
                source.path = _relative_to(path, source.path)
        return config
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as exc:
        raise ConfigError(str(exc)) from exc


def _relative_to(config_path: Path, value: Path) -> Path:
    if value.is_absolute():
        return value
    return (config_path.resolve().parent / value).resolve()

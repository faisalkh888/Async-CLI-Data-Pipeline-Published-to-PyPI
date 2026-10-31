from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Pipeline(StrictModel):
    name: str = Field(min_length=1)
    output_dir: Path = Path("./output")
    fail_fast: bool = False


class Settings(StrictModel):
    queue_size: int = Field(default=1000, ge=1, le=100_000)
    batch_size: int = Field(default=1000, ge=1, le=100_000)
    request_timeout: float = Field(default=30, gt=0, le=600)
    max_retries: int = Field(default=3, ge=0, le=20)
    max_concurrency_per_host: int = Field(default=5, ge=1, le=100)
    checkpoint_file: Path = Path("./.datapipe-checkpoint.json")
    heavy_workers: int = Field(default=0, ge=0, le=64)


class CsvSource(StrictModel):
    name: str = Field(min_length=1)
    type: Literal["csv"]
    path: Path


class JsonlSource(StrictModel):
    name: str = Field(min_length=1)
    type: Literal["jsonl"]
    path: Path


class HttpSource(StrictModel):
    name: str = Field(min_length=1)
    type: Literal["http"]
    url: HttpUrl
    auth_env: str | None = None
    pagination: Literal["none", "next_url", "page_number"] = "none"
    records_path: str = "data"
    next_url_path: str = "next"
    page_param: str = "page"
    start_page: int = Field(default=1, ge=0)
    max_pages: int = Field(default=100_000, ge=1)


class PluginSource(StrictModel):
    name: str = Field(min_length=1)
    type: Literal["plugin"]
    plugin: str = Field(min_length=1)
    config: dict[str, object] = Field(default_factory=dict)


Source = Annotated[CsvSource | JsonlSource | HttpSource | PluginSource, Field(discriminator="type")]


class FieldMapping(StrictModel):
    type: Literal["field_mapping"]
    mapping: dict[str, str]


class Filter(StrictModel):
    type: Literal["filter"]
    field: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "contains", "exists"]
    value: object = None


class Deduplicate(StrictModel):
    type: Literal["deduplicate"]
    key: str


class AggregateDaily(StrictModel):
    type: Literal["aggregate_daily"]
    date_field: str
    value_field: str
    output_date_field: str = "date"
    output_value_field: str = "total"


class HeavyTransform(StrictModel):
    type: Literal["heavy_transform"]
    field: str
    iterations: int = Field(default=100_000, ge=1, le=10_000_000)


class PluginTransform(StrictModel):
    type: Literal["plugin"]
    plugin: str = Field(min_length=1)
    config: dict[str, object] = Field(default_factory=dict)


Transform = Annotated[
    FieldMapping | Filter | Deduplicate | AggregateDaily | HeavyTransform | PluginTransform,
    Field(discriminator="type"),
]


class Validation(StrictModel):
    field_types: dict[str, Literal["string", "integer", "number", "boolean"]] = Field(default_factory=dict)
    required_fields: list[str] = Field(default_factory=list)


class BuiltinSink(StrictModel):
    type: Literal["parquet", "jsonl"]
    name: str = Field(min_length=1)
    path: Path
    partition_by: list[str] = Field(default_factory=list)
    dead_letters: bool = False


class PluginSink(StrictModel):
    type: Literal["plugin"]
    name: str = Field(min_length=1)
    plugin: str = Field(min_length=1)
    config: dict[str, object] = Field(default_factory=dict)
    dead_letters: bool = False


Sink = Annotated[BuiltinSink | PluginSink, Field(discriminator="type")]


class PipelineConfig(StrictModel):
    pipeline: Pipeline
    settings: Settings = Settings()
    sources: list[Source] = Field(min_length=1)
    transforms: list[Transform] = Field(default_factory=list)
    validation: Validation = Validation()
    sinks: list[Sink] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_sink_names(self) -> "PipelineConfig":
        if len({sink.name for sink in self.sinks}) != len(self.sinks):
            raise ValueError("sink names must be unique")
        if sum(sink.dead_letters for sink in self.sinks) > 1:
            raise ValueError("at most one sink may set dead_letters = true")
        if any(sink.dead_letters and sink.type not in {"jsonl", "plugin"} for sink in self.sinks):
            raise ValueError("dead-letter sink must be JSONL")
        if any(isinstance(sink, BuiltinSink) and sink.partition_by and sink.type != "parquet" for sink in self.sinks):
            raise ValueError("partition_by is supported only for Parquet sinks")
        if any(isinstance(transform, AggregateDaily) for transform in self.transforms[:-1]):
            raise ValueError("aggregate_daily must be the final transform")
        return self

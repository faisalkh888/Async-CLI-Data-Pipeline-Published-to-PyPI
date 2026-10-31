from functools import lru_cache
from typing import Any, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    create_model,
)

from datapipe.config.schema import Validation
from datapipe.core.protocols import Record

_TYPES: dict[str, type[Any]] = {
    "string": StrictStr,
    "integer": StrictInt,
    "number": StrictFloat,
    "boolean": StrictBool,
}


@lru_cache(maxsize=128)
def _record_model(field_items: tuple[tuple[str, str], ...], required: tuple[str, ...]) -> type[BaseModel]:
    definitions: dict[str, tuple[type[Any], Any]] = {
        name: (_TYPES[field_type], ... if name in required else None)
        for name, field_type in field_items
    }
    for name in required:
        definitions.setdefault(name, (Any, ...))
    dynamic_create_model = cast(Any, create_model)
    return cast(type[BaseModel], dynamic_create_model("PipelineRecord", __config__=ConfigDict(extra="allow"), **definitions))


def validate_record(record: Record, schema: Validation) -> Record:
    if not schema.field_types and not schema.required_fields:
        return record
    model = _record_model(tuple(sorted(schema.field_types.items())), tuple(sorted(schema.required_fields)))
    return cast(Record, model.model_validate(record).model_dump())

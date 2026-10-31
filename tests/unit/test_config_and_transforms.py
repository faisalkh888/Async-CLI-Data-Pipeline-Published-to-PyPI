import json

import pytest
from pydantic import ValidationError

from datapipe.config.loader import load_config
from datapipe.config.schema import AggregateDaily, Deduplicate, FieldMapping, Filter
from datapipe.core.validation import validate_record
from datapipe.transforms.operations import apply_transform


def test_config_resolves_paths_relative_to_config(tmp_path):
    (tmp_path / "records.csv").write_text("id\n1\n", encoding="utf-8")
    cfg = tmp_path / "pipeline.toml"
    cfg.write_text(
        '[pipeline]\nname="t"\noutput_dir="out"\n'
        '[[sources]]\nname="s"\ntype="csv"\npath="records.csv"\n'
        '[[sinks]]\nname="o"\ntype="jsonl"\npath="records.jsonl"\n',
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded.sources[0].path == tmp_path / "records.csv"
    assert loaded.pipeline.output_dir == tmp_path / "out"


def test_config_rejects_unknown_keys(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text('[pipeline]\nname="x"\nunknown=1\n', encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(path)


def test_mapping_filter_and_disk_backed_deduplicate():
    rows = [{"id": 1, "amount": 5}, {"id": 2, "amount": 1}, {"id": 1, "amount": 5}]
    mapped = apply_transform(rows, FieldMapping(type="field_mapping", mapping={"id": "key", "amount": "total"}))
    filtered = apply_transform(mapped, Filter(type="filter", field="total", operator="gte", value=5))
    deduped = list(apply_transform(filtered, Deduplicate(type="deduplicate", key="key")))
    assert deduped == [{"key": 1, "total": 5}]


def test_record_validation_uses_strict_types():
    from datapipe.config.schema import Validation

    schema = Validation(required_fields=["id"], field_types={"id": "integer"})
    assert validate_record({"id": 3, "extra": True}, schema)["id"] == 3
    with pytest.raises(ValidationError):
        validate_record({"id": "3"}, schema)


def test_mapping_does_not_mutate_input():
    row = {"a": 2}
    list(apply_transform([row], FieldMapping(type="field_mapping", mapping={"a": "b"})))
    assert json.loads(json.dumps(row)) == {"a": 2}


def test_daily_aggregation_groups_dates():
    transform = AggregateDaily(type="aggregate_daily", date_field="created", value_field="amount")
    output = list(apply_transform([
        {"created": "2026-10-01T11:00:00Z", "amount": 2},
        {"created": "2026-10-01T18:00:00Z", "amount": 3},
        {"created": "2026-10-02", "amount": 7},
    ], transform))
    assert output == [{"date": "2026-10-01", "total": 5.0}, {"date": "2026-10-02", "total": 7.0}]

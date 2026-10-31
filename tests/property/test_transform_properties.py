from hypothesis import given
from hypothesis import strategies as st

from datapipe.config.schema import Deduplicate, FieldMapping
from datapipe.transforms.operations import apply_transform


@given(st.lists(st.integers(), max_size=100))
def test_deduplicate_preserves_first_occurrence(values):
    rows = [{"id": value} for value in values]
    output = list(apply_transform(rows, Deduplicate(type="deduplicate", key="id")))
    assert [row["id"] for row in output] == list(dict.fromkeys(values))


@given(st.dictionaries(st.text(min_size=1), st.integers(), max_size=20))
def test_mapping_preserves_values_and_leaves_source_unchanged(row):
    mapping = {key: f"mapped_{key}" for key in row}
    output = next(iter(apply_transform([row], FieldMapping(type="field_mapping", mapping=mapping))))
    assert output == {f"mapped_{key}": value for key, value in row.items()}
    assert set(row) == set(mapping)

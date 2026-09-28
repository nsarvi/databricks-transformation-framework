"""Unit tests for DataTransformer.apply_combine routing and combine chaining; no Spark needed.

Reads and joins are mocked, so these tests only check which combine path runs and what gets read.
"""
from unittest import mock

import pytest

from wilsonelser.transformation.data_transformer import DataTransformer


def make_transformer(combines: dict, sources=("s1", "s2", "s3")) -> DataTransformer:
    transformer = DataTransformer.__new__(DataTransformer)   # skip __init__: no Spark session or config file
    transformer.logger = mock.MagicMock()
    transformer.source_lookup = {name: {"source_id": name} for name in sources}
    transformer.combine_lookup = combines
    transformer.read_source_table = mock.MagicMock(side_effect=lambda source_id: mock.MagicMock(name=source_id))
    return transformer


def union(*inputs):
    return {"unions": {"source_ids": list(inputs)}}


def join(*inputs):
    return {"joins": {
        "source_ids": [{"source_id": name, "alias": f"a{i}", "join_order": i} for i, name in enumerate(inputs)],
        "join_conditions": [{"left": f"a{i - 1}", "right": f"a{i}", "condition": "true"} for i in range(1, len(inputs))],
    }}


def test_unions_only_runs_apply_unions():
    transformer = make_transformer({"c": union("s1", "s2")})
    with mock.patch.object(transformer, "apply_unions") as apply_unions, \
            mock.patch.object(transformer, "apply_joins") as apply_joins:
        transformer.apply_combine("c")

    apply_unions.assert_called_once_with("c", ("c",))
    apply_joins.assert_not_called()


def test_joins_only_runs_apply_joins():
    transformer = make_transformer({"c": join("s1", "s2")})
    with mock.patch.object(transformer, "apply_unions") as apply_unions, \
            mock.patch.object(transformer, "apply_joins") as apply_joins:
        transformer.apply_combine("c")

    apply_joins.assert_called_once_with("c", ("c",))
    apply_unions.assert_not_called()


@pytest.mark.parametrize("combine, found", [
    ({**union("s1", "s2"), **join("s1", "s2")}, "both"),
    ({}, "neither"),
])
def test_combine_needs_exactly_one_of_unions_or_joins(combine, found):
    transformer = make_transformer({"c": combine or {"alias": "c"}})

    with pytest.raises(ValueError, match=f"exactly one of 'unions' or 'joins', found {found}"):
        transformer.apply_combine("c")


def test_join_can_use_a_union_combine_as_input():
    transformer = make_transformer({"all_sales": union("s1", "s2"), "joined": join("all_sales", "s3")})
    with mock.patch.object(transformer, "_apply_joins", return_value=mock.MagicMock()), \
            mock.patch.object(transformer, "_select_final_columns", return_value=[]):
        transformer.apply_combine("joined")

    read = [call.args[0] for call in transformer.read_source_table.call_args_list]
    assert sorted(read) == ["s1", "s2", "s3"]      # the union's sources are read; "all_sales" isn't read as a source


def test_union_can_use_a_join_combine_as_input():
    transformer = make_transformer({"joined": join("s1", "s2"), "all": union("joined", "s3")})
    with mock.patch.object(transformer, "_apply_joins", return_value=mock.MagicMock()), \
            mock.patch.object(transformer, "_select_final_columns", return_value=[]):
        transformer.apply_combine("all")

    assert sorted(call.args[0] for call in transformer.read_source_table.call_args_list) == ["s1", "s2", "s3"]


def test_combines_referring_to_each_other_raise():
    transformer = make_transformer({"a": join("b", "s1"), "b": union("a", "s2")})

    with pytest.raises(ValueError, match="loop: a -> b -> a"):
        transformer.apply_combine("a")


def test_combine_referring_to_itself_raises():
    transformer = make_transformer({"a": union("a", "s1")})

    with pytest.raises(ValueError, match="loop: a -> a"):
        transformer.apply_combine("a")


def test_id_that_is_both_source_and_combine_raises():
    transformer = make_transformer({"s1": union("s2", "s3"), "c": union("s1", "s2")})

    with pytest.raises(ValueError, match="'s1' is both a source_id and a combine_id"):
        transformer.apply_combine("c")

"""Unit tests for choosing a join result's columns; no Spark needed."""
import pytest

from wilsonelser.transformation.data_transformer import DataTransformer

ALIAS_COLUMNS = {                      # joined inputs, in join order
    "c": ["customer_id", "customer_name", "customer_email"],
    "s": ["customer_id", "transaction_id", "price"],
    "st": ["store_id", "store_name"],
}


def test_listed_aliases_keep_only_their_listed_columns():
    selected, skipped = DataTransformer._final_column_names(
        {"c": ["customer_id", "customer_name"], "s": ["transaction_id", "price"], "st": ["store_name"]},
        ALIAS_COLUMNS,
    )

    assert selected == [("c", "customer_id"), ("c", "customer_name"), ("s", "transaction_id"), ("s", "price"), ("st", "store_name")]
    assert skipped == []


def test_without_select_columns_every_alias_keeps_all_columns():
    selected, skipped = DataTransformer._final_column_names({}, ALIAS_COLUMNS)

    assert selected == [
        ("c", "customer_id"), ("c", "customer_name"), ("c", "customer_email"),
        ("s", "transaction_id"), ("s", "price"),
        ("st", "store_id"), ("st", "store_name"),
    ]
    assert skipped == ["s.customer_id"]          # the join key appears once, from the first alias


def test_unlisted_aliases_keep_all_columns_after_the_listed_ones():
    selected, _ = DataTransformer._final_column_names({"s": ["transaction_id"]}, ALIAS_COLUMNS)

    assert selected == [
        ("s", "transaction_id"),
        ("c", "customer_id"), ("c", "customer_name"), ("c", "customer_email"),
        ("st", "store_id"), ("st", "store_name"),
    ]


def test_duplicate_names_keep_the_first_listed():
    selected, skipped = DataTransformer._final_column_names(
        {"s": ["customer_id", "price"], "c": ["customer_id"], "st": []}, ALIAS_COLUMNS
    )

    assert selected == [("s", "customer_id"), ("s", "price")]
    assert skipped == ["c.customer_id"]


def test_select_columns_for_an_alias_that_isnt_joined_raises():
    with pytest.raises(ValueError, match="aren't joined: x"):
        DataTransformer._final_column_names({"c": ["customer_id"], "x": ["a"]}, ALIAS_COLUMNS)

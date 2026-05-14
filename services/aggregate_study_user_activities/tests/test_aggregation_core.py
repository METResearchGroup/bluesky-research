"""Unit tests for aggregation_core pure helpers."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from services.aggregate_study_user_activities.aggregation_core import (
    author_did_series_for_follows,
    copy_dtypes_without_partition,
    finalize_activity_df,
    json_object_per_row,
)


def test_copy_dtypes_without_partition_does_not_mutate_input() -> None:
    original = {"a": "string", "partition_date": "string"}
    snapshot = dict(original)
    trimmed = copy_dtypes_without_partition(original)

    assert trimmed == {"a": "string"}
    assert original == snapshot
    assert trimmed is not original


def test_copy_dtypes_without_partition_no_partition_key() -> None:
    d = {"x": "Int64"}
    assert copy_dtypes_without_partition(d) == {"x": "Int64"}


def test_json_object_per_row_nulls_and_order() -> None:
    df = pd.DataFrame(
        {
            "a": [1, np.nan],
            "b": ["x", None],
        }
    )
    out = json_object_per_row(df, ["a", "b"])
    rows = [json.loads(s) for s in out.tolist()]
    assert rows[0] == {"a": 1, "b": "x"}
    assert rows[1]["a"] is None
    assert rows[1]["b"] is None
    assert list(rows[0].keys()) == ["a", "b"]


def test_finalize_activity_df_column_order() -> None:
    base = pd.DataFrame({"extra": [1, 2]})
    cols = [
        "author_did",
        "author_handle",
        "data_type",
        "data",
        "activity_timestamp",
    ]
    got = finalize_activity_df(
        base,
        author_did=["d1", "d2"],
        author_handle=["h1", "h2"],
        data_type="post",
        data='{"k":1}',
        activity_timestamp=["t1", "t2"],
        cols=cols,
    )
    assert list(got.columns) == cols
    assert got["author_did"].tolist() == ["d1", "d2"]
    assert got["data_type"].tolist() == ["post", "post"]


def test_author_did_series_follower_vs_followee() -> None:
    df = pd.DataFrame(
        {
            "relationship_to_study_user": ["follower", "follow", "follower"],
            "follow_did": ["fd_a", "fd_b", "fd_c"],
            "follower_did": ["fr_a", "fr_b", "fr_c"],
        }
    )
    series = author_did_series_for_follows(df)
    assert series.tolist() == ["fd_a", "fr_b", "fd_c"]

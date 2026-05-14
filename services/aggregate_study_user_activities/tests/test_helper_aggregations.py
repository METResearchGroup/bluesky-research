"""Integration-style tests for aggregate_study_user_activities.helper."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from lib.db.service_constants import MAP_SERVICE_TO_METADATA

PARTITION_DATE = "2024-06-01"


@pytest.fixture()
def sut(monkeypatch):
    """Helper module with a deterministic author handle map."""
    import services.aggregate_study_user_activities.helper as mod

    monkeypatch.setattr(
        mod,
        "map_author_did_to_author_handle",
        {"did:plc:testauthor": "@test.handle"},
        raising=False,
    )
    return mod


def test_aggregate_latest_user_likes_reads_parquet_partition(
    tmp_path: Path, sut, monkeypatch
) -> None:
    root = tmp_path / "likes"
    part_dir = root / "active" / f"partition_date={PARTITION_DATE}"
    part_dir.mkdir(parents=True)

    dtypes = MAP_SERVICE_TO_METADATA["study_user_likes"]["dtypes_map"]
    df_raw = pd.DataFrame(
        {
            "author": pd.Series(["did:plc:testauthor"], dtype=dtypes["author"]),
            "cid": pd.Series(["c"], dtype=dtypes["cid"]),
            "record": pd.Series([r'{"x": 1}'], dtype=dtypes["record"]),
            "uri": pd.Series(["at://uri"], dtype=dtypes["uri"]),
            "synctimestamp": pd.Series(
                ["2024-06-01T12:00:00"], dtype=dtypes["synctimestamp"]
            ),
        }
    )
    df_raw.to_parquet(part_dir / "batch.parquet")

    monkeypatch.setitem(MAP_SERVICE_TO_METADATA["study_user_likes"], "local_prefix", str(root))

    out = sut.aggregate_latest_user_likes(PARTITION_DATE)
    assert list(out.columns) == sut.cols_to_keep
    assert len(out) == 1
    row = out.iloc[0]
    assert row["author_did"] == "did:plc:testauthor"
    assert row["author_handle"] == "@test.handle"
    assert row["data_type"] == "like"
    assert row["data"] == r'{"x": 1}'
    assert row["activity_timestamp"] == "2024-06-01T12:00:00"


def test_aggregate_latest_user_likes_missing_partition_returns_empty_columns(
    tmp_path: Path, sut, monkeypatch
) -> None:
    bare = tmp_path / "no_partitions"
    bare.mkdir()
    monkeypatch.setitem(MAP_SERVICE_TO_METADATA["study_user_likes"], "local_prefix", str(bare))

    out = sut.aggregate_latest_user_likes(PARTITION_DATE)
    assert out.empty
    assert list(out.columns) == sut.cols_to_keep


def test_aggregate_latest_user_session_logs_uses_athena_and_json_shape(
    sut, monkeypatch
) -> None:
    df_raw = pd.DataFrame(
        {
            "user_did": ["did:plc:testauthor"],
            "timestamp": ["t1"],
            "cursor": ["c1"],
            "limit": [20],
            "feed_length": [5],
            "feed": ["f"],
            "ignored_meta": ["x"],
        }
    )

    monkeypatch.setattr(sut.athena, "query_results_as_df", lambda _q: df_raw)

    out = sut.aggregate_latest_user_session_logs(PARTITION_DATE)
    assert list(out.columns) == sut.cols_to_keep
    assert len(out) == 1
    payload = json.loads(out.iloc[0]["data"])
    assert list(payload.keys()) == sut.SESSION_LOG_JSON_COLUMNS
    assert payload == {"cursor": "c1", "limit": 20, "feed_length": 5, "feed": "f"}

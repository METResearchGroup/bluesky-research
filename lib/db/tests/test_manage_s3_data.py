"""Tests for lib/db/manage_s3_data.py"""

from unittest.mock import Mock

import pandas as pd
import pytest

from lib.db.manage_s3_data import (
    S3ParquetBackend,
    S3ParquetDatasetRef,
    STUDY_ROOT_KEY_PREFIX,
)
from lib.db.models import StorageTier
from lib.db.sql.duckdb_wrapper import DuckDB


def test_list_parquet_uris_single_date_filters_and_converts_to_s3_uris():
    mock_s3 = Mock()

    dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")
    partition_date = "2024-11-13"
    tier = StorageTier.CACHE

    expected_prefix = (
        f"{STUDY_ROOT_KEY_PREFIX}/{dataset.dataset}/{tier.value}/partition_date={partition_date}/"
    )

    # Include a non-parquet key to ensure filtering.
    keys = [
        f"{expected_prefix}a.parquet",
        f"{expected_prefix}b.parquet",
        f"{expected_prefix}not_parquet.txt",
    ]
    mock_s3.list_keys_given_prefix.return_value = keys

    backend = S3ParquetBackend(s3=mock_s3, duckdb_engine=DuckDB())
    uris = backend.list_parquet_uris(
        dataset=dataset,
        storage_tiers=[tier],
        partition_date=partition_date,
    )

    mock_s3.list_keys_given_prefix.assert_called_once_with(prefix=expected_prefix)
    assert uris == [
        f"s3://bluesky-research/{expected_prefix}a.parquet",
        f"s3://bluesky-research/{expected_prefix}b.parquet",
    ]


def test_list_parquet_uris_date_range_lists_each_day(monkeypatch):
    mock_s3 = Mock()
    dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")
    tier = StorageTier.CACHE

    monkeypatch.setattr(
        "lib.db.manage_s3_data.get_partition_dates",
        lambda start_date, end_date: ["2024-11-13", "2024-11-14"],
    )

    def side_effect(prefix: str):
        return [f"{prefix}file.parquet"]

    mock_s3.list_keys_given_prefix.side_effect = side_effect

    backend = S3ParquetBackend(s3=mock_s3, duckdb_engine=DuckDB())
    uris = backend.list_parquet_uris(
        dataset=dataset,
        storage_tiers=[tier],
        start_partition_date="2024-11-13",
        end_partition_date="2024-11-14",
    )

    expected_prefixes = [
        f"{STUDY_ROOT_KEY_PREFIX}/{dataset.dataset}/{tier.value}/partition_date=2024-11-13/",
        f"{STUDY_ROOT_KEY_PREFIX}/{dataset.dataset}/{tier.value}/partition_date=2024-11-14/",
    ]
    assert mock_s3.list_keys_given_prefix.call_count == 2
    assert [c.kwargs["prefix"] for c in mock_s3.list_keys_given_prefix.call_args_list] == expected_prefixes
    assert uris == [
        f"s3://bluesky-research/{expected_prefixes[0]}file.parquet",
        f"s3://bluesky-research/{expected_prefixes[1]}file.parquet",
    ]


def test_list_parquet_uris_rejects_partition_date_and_range():
    backend = S3ParquetBackend(s3=Mock(), duckdb_engine=DuckDB())
    with pytest.raises(ValueError):
        backend.list_parquet_uris(
            dataset=S3ParquetDatasetRef(dataset="preprocessed_posts"),
            partition_date="2024-11-13",
            start_partition_date="2024-11-13",
            end_partition_date="2024-11-13",
        )


def test_query_parquet_as_df_empty_uris_returns_expected_columns():
    backend = S3ParquetBackend(s3=Mock(), duckdb_engine=DuckDB())
    df = backend.query_parquet_as_df(
        uris=[],
        query="SELECT 1",
        query_metadata={"tables": [{"name": "t", "columns": ["a", "b"]}]},
    )
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]
    assert df.empty


"""Tests for lib/db/manage_s3_data.py.

Testing standards:
- One test class per function/method
- Arrange / Act / Assert
- Heavy mocking of external dependencies (S3, DuckDB, date utils, logging)
"""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from lib.db.manage_s3_data import (
    S3Backend,
    S3ParquetDatasetRef,
    STUDY_BUCKET,
    STUDY_ROOT_KEY_PREFIX,
)
from lib.db.models import StorageTier


class TestS3ParquetDatasetRef:
    """Tests for S3ParquetDatasetRef class."""

    def test_stores_dataset_name(self):
        """Test that the dataclass stores the dataset name."""
        # Arrange
        expected = "preprocessed_posts"

        # Act
        result = S3ParquetDatasetRef(dataset=expected)

        # Assert
        assert result.dataset == expected

    def test_is_frozen(self):
        """Test that the dataclass is frozen (immutable)."""
        # Arrange
        ref = S3ParquetDatasetRef(dataset="preprocessed_posts")

        # Act / Assert
        with pytest.raises(dataclasses.FrozenInstanceError):
            ref.dataset = "other"


class TestS3BackendInit:
    """Tests for S3Backend.__init__ method."""

    def test_uses_injected_s3_and_duckdb(self, mocker):
        """Test that injected dependencies are used as-is."""
        # Arrange
        mock_s3 = mocker.Mock()
        mock_duckdb = mocker.Mock()

        # Act
        result = S3Backend(s3=mock_s3, duckdb_engine=mock_duckdb)

        # Assert
        assert result.s3 is mock_s3
        assert result.duckdb_engine is mock_duckdb
        assert result.storage_tier_default == StorageTier.CACHE

    def test_lazy_imports_s3_client_when_s3_is_none(self, mocker):
        """Test that S3 is lazily imported/instantiated when s3=None."""
        # Arrange
        mock_s3_cls = mocker.patch("lib.aws.s3.S3")
        mock_s3_instance = mock_s3_cls.return_value
        mock_duckdb_cls = mocker.patch("lib.db.manage_s3_data.DuckDB")
        mock_duckdb_instance = mock_duckdb_cls.return_value

        # Act
        result = S3Backend(s3=None, duckdb_engine=None)

        # Assert
        mock_s3_cls.assert_called_once()
        mock_duckdb_cls.assert_called_once()
        assert result.s3 is mock_s3_instance
        assert result.duckdb_engine is mock_duckdb_instance


class TestS3BackendKeyToUri:
    """Tests for S3Backend._key_to_uri method."""

    def test_uses_default_bucket(self):
        """Test that default bucket is used when none is provided."""
        # Arrange
        key = "some/key.parquet"
        expected = f"s3://{STUDY_BUCKET}/{key}"

        # Act
        result = S3Backend._key_to_uri(key)

        # Assert
        assert result == expected

    def test_uses_custom_bucket(self):
        """Test that a custom bucket can be provided."""
        # Arrange
        key = "some/key.parquet"
        bucket = "my-bucket"
        expected = f"s3://{bucket}/{key}"

        # Act
        result = S3Backend._key_to_uri(key, bucket=bucket)

        # Assert
        assert result == expected


class TestS3BackendBuildPartitionPrefixKey:
    """Tests for S3Backend._build_partition_prefix_key method."""

    def test_builds_expected_prefix(self):
        """Test that the partition prefix is constructed as expected."""
        # Arrange
        dataset = "preprocessed_posts"
        tier = StorageTier.CACHE
        partition_date = "2024-11-13"
        expected = (
            f"{STUDY_ROOT_KEY_PREFIX}/{dataset}/{tier.value}/partition_date={partition_date}/"
        )

        # Act
        result = S3Backend._build_partition_prefix_key(
            dataset=dataset, tier=tier, partition_date=partition_date
        )

        # Assert
        assert result == expected


class TestS3BackendReturnValidPartitionDates:
    """Tests for S3Backend._return_valid_partition_dates method."""

    def test_returns_single_date_when_partition_date_provided(self, mocker):
        """Test that a single provided partition_date is returned as a list."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())
        expected = ["2024-11-13"]

        # Act
        result = backend._return_valid_partition_dates(partition_date="2024-11-13")

        # Assert
        assert result == expected

    def test_returns_range_when_start_and_end_provided(self, mocker):
        """Test that a date range uses get_partition_dates to enumerate all days."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())
        mock_get_dates = mocker.patch(
            "lib.db.manage_s3_data.get_partition_dates",
            return_value=["2024-11-13", "2024-11-14"],
        )
        expected = ["2024-11-13", "2024-11-14"]

        # Act
        result = backend._return_valid_partition_dates(
            start_partition_date="2024-11-13", end_partition_date="2024-11-14"
        )

        # Assert
        mock_get_dates.assert_called_once_with(
            start_date="2024-11-13", end_date="2024-11-14"
        )
        assert result == expected

    def test_raises_value_error_when_both_single_and_range_provided(self, mocker):
        """Test that providing both partition_date and range raises ValueError."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())

        # Act / Assert
        with pytest.raises(
            ValueError,
            match="Provide either partition_date OR start_partition_date/end_partition_date",
        ):
            backend._return_valid_partition_dates(
                partition_date="2024-11-13",
                start_partition_date="2024-11-13",
                end_partition_date="2024-11-14",
            )

    def test_raises_value_error_when_no_date_filters_provided(self, mocker):
        """Test that missing all date filters raises ValueError."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())

        # Act / Assert
        with pytest.raises(
            ValueError,
            match="Must provide either partition_date or start_partition_date/end_partition_date",
        ):
            backend._return_valid_partition_dates()

    def test_raises_value_error_when_max_days_exceeded(self, mocker):
        """Test that max_days bounds range listing."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())

        # Act / Assert
        with pytest.raises(ValueError, match="Refusing to list n_days=2 > max_days=1"):
            backend._return_valid_partition_dates(
                start_partition_date="2024-11-13",
                end_partition_date="2024-11-14",
                max_days=1,
            )


class TestS3BackendGetParquetKeysForPartitionDate:
    """Tests for S3Backend._get_parquet_keys_for_partition_date method."""

    def test_filters_non_parquet_keys_and_converts_to_s3_uris(self, mocker):
        """Test that only .parquet keys are returned as DuckDB-readable S3 URIs."""
        # Arrange
        mock_logger = mocker.patch("lib.db.manage_s3_data.logger")
        mock_s3 = mocker.Mock()
        mock_duckdb = mocker.Mock()

        dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")
        partition_date = "2024-11-13"
        tier = StorageTier.CACHE

        expected_prefix = (
            f"{STUDY_ROOT_KEY_PREFIX}/{dataset.dataset}/{tier.value}/partition_date={partition_date}/"
        )
        keys = [
            f"{expected_prefix}a.parquet",
            f"{expected_prefix}b.parquet",
            f"{expected_prefix}not_parquet.txt",
        ]
        mock_s3.list_keys_given_prefix.return_value = keys

        backend = S3Backend(s3=mock_s3, duckdb_engine=mock_duckdb)
        expected = [
            f"s3://{STUDY_BUCKET}/{expected_prefix}a.parquet",
            f"s3://{STUDY_BUCKET}/{expected_prefix}b.parquet",
        ]

        # Act
        result = backend._get_parquet_keys_for_partition_date(
            dataset=dataset, tier=tier, partition_date=partition_date
        )

        # Assert
        mock_s3.list_keys_given_prefix.assert_called_once_with(prefix=expected_prefix)
        mock_logger.info.assert_called_once()
        assert result == expected

    def test_returns_empty_list_when_no_parquet_keys(self, mocker):
        """Test that empty list is returned when no parquet keys are present."""
        # Arrange
        mock_logger = mocker.patch("lib.db.manage_s3_data.logger")
        mock_s3 = mocker.Mock()
        mock_s3.list_keys_given_prefix.return_value = ["x.txt", "y.jsonl"]

        backend = S3Backend(s3=mock_s3, duckdb_engine=mocker.Mock())

        # Act
        result = backend._get_parquet_keys_for_partition_date(
            dataset=S3ParquetDatasetRef(dataset="preprocessed_posts"),
            tier=StorageTier.CACHE,
            partition_date="2024-11-13",
        )

        # Assert
        assert result == []
        mock_logger.info.assert_not_called()


class TestS3BackendListParquetUris:
    """Tests for S3Backend.list_parquet_uris method."""

    def test_lists_across_tiers_and_dates(self, mocker):
        """Test that list_parquet_uris aggregates all URIs across tiers and partition dates."""
        # Arrange
        mocker.patch("lib.db.manage_s3_data.logger")
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())
        dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")

        mock_dates = mocker.patch.object(
            backend, "_return_valid_partition_dates", return_value=["2024-11-13", "2024-11-14"]
        )

        def side_effect(*, dataset, tier, partition_date):
            return [f"s3://b/{dataset.dataset}/{tier.value}/{partition_date}.parquet"]

        mock_get = mocker.patch.object(
            backend, "_get_parquet_keys_for_partition_date", side_effect=side_effect
        )

        tiers = [StorageTier.CACHE, StorageTier.ACTIVE]
        expected = [
            "s3://b/preprocessed_posts/cache/2024-11-13.parquet",
            "s3://b/preprocessed_posts/cache/2024-11-14.parquet",
            "s3://b/preprocessed_posts/active/2024-11-13.parquet",
            "s3://b/preprocessed_posts/active/2024-11-14.parquet",
        ]

        # Act
        result = backend.list_parquet_uris(
        dataset=dataset,
            storage_tiers=tiers,
        start_partition_date="2024-11-13",
        end_partition_date="2024-11-14",
    )

        # Assert
        mock_dates.assert_called_once_with(
            partition_date=None,
            start_partition_date="2024-11-13",
            end_partition_date="2024-11-14",
            max_days=None,
        )
        assert mock_get.call_count == 4
        assert result == expected

    def test_truncates_when_max_files_reached(self, mocker):
        """Test that listing truncates and warns when max_files is reached."""
        # Arrange
        mock_logger = mocker.patch("lib.db.manage_s3_data.logger")
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())
        dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")
        mocker.patch.object(backend, "_return_valid_partition_dates", return_value=["2024-11-13"])

        mocker.patch.object(
            backend,
            "_get_parquet_keys_for_partition_date",
            return_value=["u1", "u2", "u3"],
        )
        expected = ["u1", "u2"]

        # Act
        result = backend.list_parquet_uris(
            dataset=dataset,
            storage_tiers=[StorageTier.CACHE],
            partition_date="2024-11-13",
            max_files=2,
        )

        # Assert
        mock_logger.warning.assert_called_once_with("Reached max_files=2; truncating listed URIs.")
        assert result == expected


class TestS3BackendQueryParquetAsDf:
    """Tests for S3Backend.query_parquet_as_df method."""

    def test_delegates_to_duckdb_engine(self, mocker):
        """Test that the DuckDB engine is called with the expected parameters."""
        # Arrange
        mock_duckdb = mocker.Mock()
        expected_df = pd.DataFrame({"a": [1, 2]})
        mock_duckdb.run_query_as_df.return_value = expected_df
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mock_duckdb)

        uris = ["s3://b/k1.parquet", "s3://b/k2.parquet"]
        query = "SELECT * FROM read_parquet(?)"
        query_metadata = {"tables": [{"name": "t", "columns": ["a"]}]}

        # Act
        result = backend.query_parquet_as_df(
            uris=uris, query=query, query_metadata=query_metadata
        )

        # Assert
        mock_duckdb.run_query_as_df.assert_called_once_with(
            query=query, mode="parquet", filepaths=uris, query_metadata=query_metadata
        )
        pd.testing.assert_frame_equal(result, expected_df)


class TestS3BackendQueryDatasetAsDf:
    """Tests for S3Backend.query_dataset_as_df method."""

    def test_lists_uris_then_queries(self, mocker):
        """Test that the dataset query lists parquet URIs then queries them."""
        # Arrange
        backend = S3Backend(s3=mocker.Mock(), duckdb_engine=mocker.Mock())
        dataset = S3ParquetDatasetRef(dataset="preprocessed_posts")
        query = "SELECT 1"
        query_metadata = {"tables": [{"name": "t", "columns": ["x"]}]}

        mock_list = mocker.patch.object(
            backend, "list_parquet_uris", return_value=["u1", "u2"]
        )
        expected_df = pd.DataFrame({"x": [1]})
        mock_query = mocker.patch.object(
            backend, "query_parquet_as_df", return_value=expected_df
        )

        # Act
        result = backend.query_dataset_as_df(
            dataset=dataset,
            query=query,
            query_metadata=query_metadata,
            storage_tiers=[StorageTier.CACHE],
            partition_date="2024-11-13",
            max_days=7,
            max_files=100,
        )

        # Assert
        mock_list.assert_called_once_with(
            dataset=dataset,
            storage_tiers=[StorageTier.CACHE],
            partition_date="2024-11-13",
            start_partition_date=None,
            end_partition_date=None,
            max_days=7,
            max_files=100,
        )
        mock_query.assert_called_once_with(
            uris=["u1", "u2"], query=query, query_metadata=query_metadata
        )
        pd.testing.assert_frame_equal(result, expected_df)

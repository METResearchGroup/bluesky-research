"""Tests for join_feed_preprocessed_posts module."""

import pandas as pd
from unittest.mock import patch

from lib.db.models import StorageTier
from services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts import (
    load_posts_used_in_feeds_from_storage,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
)


class TestLoadPostsUsedInFeedsFromStorage:
    """Tests for load_posts_used_in_feeds_from_storage function."""

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_data_from_local_storage"
    )
    def test_concatenates_active_and_cache_and_dedupes_uri(self, mock_load):
        """Active and cache rows merge; duplicate URIs collapse to one row."""
        # Arrange
        def side_effect(**kwargs):
            tiers = kwargs["storage_tiers"]
            if StorageTier.ACTIVE in tiers:
                return pd.DataFrame(
                    {"uri": ["a", "b"], "partition_date": ["2024-01-01", "2024-01-01"]}
                )
            return pd.DataFrame({"uri": ["b"], "partition_date": ["2024-01-01"]})

        mock_load.side_effect = side_effect

        # Act
        result = load_posts_used_in_feeds_from_storage("2024-01-01")

        # Assert
        expected_uris = {"a", "b"}
        assert mock_load.call_count == 2
        assert len(result) == 2
        assert set(result["uri"].tolist()) == expected_uris

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_data_from_local_storage"
    )
    def test_returns_empty_when_both_tiers_empty(self, mock_load):
        """When both storage tiers are empty, result is an empty frame."""
        # Arrange
        mock_load.return_value = pd.DataFrame()

        # Act
        result = load_posts_used_in_feeds_from_storage("2024-01-01")

        # Assert
        assert result.empty


class TestLoadPreprocessedPostsUsedInFeedsForPartitionDate:
    """Tests for load_preprocessed_posts_used_in_feeds_for_partition_date function."""

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_posts_used_in_feeds_from_storage"
    )
    def test_returns_empty_frame_when_no_feed_uris(self, mock_posts_used):
        """If there are no in-feed URIs, skip preprocessed load and return empty."""
        # Arrange
        mock_posts_used.return_value = pd.DataFrame()

        # Act
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-01-05",
            lookback_start_date="2024-01-01",
            lookback_end_date="2024-01-05",
            table_columns=["uri", "text"],
        )

        # Assert
        assert list(result.columns) == ["uri", "text"]
        assert len(result) == 0

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts._load_preprocessed_posts_lookback_raw"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_posts_used_in_feeds_from_storage"
    )
    def test_filters_preprocessed_to_in_feed_uris(self, mock_posts_used, mock_raw):
        """Only preprocessed rows whose uri appears in the feed URI set are kept."""
        # Arrange
        mock_posts_used.return_value = pd.DataFrame({"uri": ["u1", "u2"]})
        mock_raw.return_value = pd.DataFrame(
            {
                "uri": ["u1", "u3"],
                "text": ["a", "b"],
                "preprocessing_timestamp": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
            }
        )

        # Act
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-01-05",
            lookback_start_date="2024-01-01",
            lookback_end_date="2024-01-05",
        )

        # Assert
        expected = pd.DataFrame(
            {
                "uri": ["u1"],
                "text": ["a"],
                "preprocessing_timestamp": ["2024-01-01T00:00:00"],
            }
        )
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected.reset_index(drop=True)
        )

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts._load_preprocessed_posts_lookback_raw"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_posts_used_in_feeds_from_storage"
    )
    def test_dedupe_uri_keep_first_sorts_by_preprocessing_timestamp(
        self, mock_posts_used, mock_raw
    ):
        """With dedupe_uri_keep_first, earliest preprocessing_timestamp wins per uri."""
        # Arrange
        mock_posts_used.return_value = pd.DataFrame({"uri": ["u1"]})
        mock_raw.return_value = pd.DataFrame(
            {
                "uri": ["u1", "u1"],
                "text": ["later", "earlier"],
                "preprocessing_timestamp": ["2024-01-02T00:00:00", "2024-01-01T00:00:00"],
            }
        )

        # Act
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-01-05",
            lookback_start_date="2024-01-01",
            lookback_end_date="2024-01-05",
            dedupe_uri_keep_first=True,
        )

        # Assert
        assert len(result) == 1
        assert result.iloc[0]["text"] == "earlier"
        assert result.iloc[0]["preprocessing_timestamp"] == "2024-01-01T00:00:00"

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_data_from_local_storage"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.join_feed_preprocessed_posts.load_posts_used_in_feeds_from_storage"
    )
    def test_table_columns_passes_duckdb_query_to_storage_loader(
        self, mock_posts_used, mock_load
    ):
        """Column-projected loads use a DuckDB query with the requested SELECT list."""
        # Arrange
        mock_posts_used.return_value = pd.DataFrame({"uri": ["u1"]})

        def storage_side_effect(**kwargs):
            if kwargs.get("service") == "preprocessed_posts":
                return pd.DataFrame(
                    {
                        "uri": ["u1"],
                        "text": ["hi"],
                    }
                )
            return pd.DataFrame()

        mock_load.side_effect = storage_side_effect

        # Act
        load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-01-05",
            lookback_start_date="2024-01-01",
            lookback_end_date="2024-01-05",
            table_columns=["uri", "text"],
        )

        # Assert
        preprocessed_calls = [
            c for c in mock_load.call_args_list if c.kwargs.get("service") == "preprocessed_posts"
        ]
        assert len(preprocessed_calls) >= 1
        assert "duckdb_query" in preprocessed_calls[0].kwargs
        assert "uri" in preprocessed_calls[0].kwargs["duckdb_query"]
        assert "text" in preprocessed_calls[0].kwargs["duckdb_query"]

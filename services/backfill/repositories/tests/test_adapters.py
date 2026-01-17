"""Tests for adapters.py."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from services.backfill.exceptions import BackfillDataAdapterError
from services.backfill.models import PostToEnqueueModel, PostUsedInFeedModel


class TestLocalStorageAdapter_load_all_posts:
    """Tests for LocalStorageAdapter.load_all_posts method."""

    @pytest.fixture
    def sample_posts_data(self):
        """Sample posts data as DataFrame records."""
        return [
            {
                "uri": "test_uri_1",
                "text": "test_text_1",
                "preprocessing_timestamp": "2024-01-01T00:00:00",
            },
            {
                "uri": "test_uri_2",
                "text": "test_text_2",
                "preprocessing_timestamp": "2024-01-02T00:00:00",
            },
        ]

    def test_loads_posts_from_local_storage(self, local_storage_adapter, sample_posts_data):
        """Test that posts are loaded from local storage and converted to models."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_df = pd.DataFrame(sample_posts_data)

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.return_value = mock_df

            # Act
            result = local_storage_adapter.load_all_posts(start_date=start_date, end_date=end_date)

            # Assert
            mock_load_data.assert_called_once()
            call_kwargs = mock_load_data.call_args.kwargs
            assert call_kwargs["service"] == "preprocessed_posts"
            assert call_kwargs["directory"] == "cache"
            assert call_kwargs["export_format"] == "duckdb"
            assert call_kwargs["start_partition_date"] == start_date
            assert call_kwargs["end_partition_date"] == end_date
            assert len(result) == 2
            assert all(isinstance(post, PostToEnqueueModel) for post in result)
            assert result[0].uri == "test_uri_1"
            assert result[1].uri == "test_uri_2"

    def test_returns_empty_list_when_no_posts(self, local_storage_adapter):
        """Test that empty list is returned when no posts are found."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_df = pd.DataFrame([])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.return_value = mock_df

            # Act
            result = local_storage_adapter.load_all_posts(start_date=start_date, end_date=end_date)

            # Assert
            assert result == []

    def test_query_uses_correct_columns_and_filter(self, local_storage_adapter):
        """Test that the query uses correct columns and filter."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.return_value = pd.DataFrame([])

            # Act
            local_storage_adapter.load_all_posts(start_date=start_date, end_date=end_date)

            # Assert
            call_kwargs = mock_load_data.call_args.kwargs
            query = call_kwargs["duckdb_query"]
            assert "uri" in query
            assert "text" in query
            assert "preprocessing_timestamp" in query
            assert "text IS NOT NULL" in query
            assert "text != ''" in query
            assert "preprocessed_posts" in query


class TestLocalStorageAdapter_load_feed_posts:
    """Tests for LocalStorageAdapter.load_feed_posts method."""

    def test_loads_feed_posts_with_date_partitioning(self, local_storage_adapter):
        """Test that feed posts are loaded with date partitioning."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-03"
        partition_dates = ["2024-01-01", "2024-01-02", "2024-01-03"]

        with patch(
            "services.backfill.repositories.adapters.get_partition_dates"
        ) as mock_get_partition_dates, patch.object(
            local_storage_adapter, "_load_feed_posts_for_date"
        ) as mock_load_for_date:
            mock_get_partition_dates.return_value = partition_dates
            mock_load_for_date.side_effect = [
                [
                    PostToEnqueueModel(
                        uri="uri_1",
                        text="test_text",
                        preprocessing_timestamp="2024-01-01T00:00:00",
                    )
                ],
                [
                    PostToEnqueueModel(
                        uri="uri_2",
                        text="test_text",
                        preprocessing_timestamp="2024-01-02T00:00:00",
                    )
                ],
                [
                    PostToEnqueueModel(
                        uri="uri_3",
                        text="test_text",
                        preprocessing_timestamp="2024-01-03T00:00:00",
                    )
                ],
            ]

            # Act
            result = local_storage_adapter.load_feed_posts(start_date=start_date, end_date=end_date)

            # Assert
            mock_get_partition_dates.assert_called_once_with(
                start_date=start_date, end_date=end_date
            )
            assert mock_load_for_date.call_count == 3
            assert len(result) == 3
            assert result[0].uri == "uri_1"
            assert result[1].uri == "uri_2"
            assert result[2].uri == "uri_3"

    def test_returns_empty_list_when_no_partition_dates(self, local_storage_adapter):
        """Test that empty list is returned when no partition dates."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch(
            "services.backfill.repositories.adapters.get_partition_dates"
        ) as mock_get_partition_dates:
            mock_get_partition_dates.return_value = []

            # Act
            result = local_storage_adapter.load_feed_posts(start_date=start_date, end_date=end_date)

            # Assert
            assert result == []


class TestLocalStorageAdapter_load_feed_posts_for_date:
    """Tests for LocalStorageAdapter._load_feed_posts_for_date method."""

    def test_loads_feed_posts_for_single_date(self, local_storage_adapter):
        """Test loading feed posts for a single partition date."""
        # Arrange
        partition_date = "2024-01-01"
        posts_used_in_feeds = [
            PostUsedInFeedModel(uri="uri1"),
            PostUsedInFeedModel(uri="uri2"),
        ]
        candidate_pool_posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text2", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri3", text="text3", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
        ]
        lookback_start = "2023-12-25"
        lookback_end = "2024-01-01"

        with patch.object(
            local_storage_adapter, "_load_posts_used_in_feeds_for_date"
        ) as mock_load_feeds, patch(
            "services.backfill.repositories.adapters.calculate_start_end_date_for_lookback"
        ) as mock_calc_lookback, patch.object(
            local_storage_adapter, "_load_candidate_pool_posts_for_date"
        ) as mock_load_candidates, patch.object(
            local_storage_adapter, "_get_candidate_pool_posts_used_in_feeds_for_date"
        ) as mock_get_filtered:
            mock_load_feeds.return_value = posts_used_in_feeds
            mock_calc_lookback.return_value = (lookback_start, lookback_end)
            mock_load_candidates.return_value = candidate_pool_posts
            mock_get_filtered.return_value = candidate_pool_posts[:2]

            # Act
            result = local_storage_adapter._load_feed_posts_for_date(partition_date=partition_date)

            # Assert
            mock_load_feeds.assert_called_once_with(partition_date)
            mock_calc_lookback.assert_called_once()
            mock_load_candidates.assert_called_once_with(
                lookback_start_date=lookback_start, lookback_end_date=lookback_end
            )
            mock_get_filtered.assert_called_once_with(
                candidate_pool_posts=candidate_pool_posts,
                posts_used_in_feeds=posts_used_in_feeds,
            )
            assert len(result) == 2

    def test_returns_empty_list_when_no_posts_used_in_feeds(self, local_storage_adapter):
        """Test that empty list is returned when no posts used in feeds."""
        # Arrange
        partition_date = "2024-01-01"

        with patch.object(
            local_storage_adapter, "_load_posts_used_in_feeds_for_date"
        ) as mock_load_feeds, patch(
            "services.backfill.repositories.adapters.calculate_start_end_date_for_lookback"
        ) as mock_calc_lookback, patch.object(
            local_storage_adapter, "_load_candidate_pool_posts_for_date"
        ), patch.object(
            local_storage_adapter, "_get_candidate_pool_posts_used_in_feeds_for_date"
        ) as mock_get_filtered:
            mock_load_feeds.return_value = []
            mock_calc_lookback.return_value = ("2023-12-25", "2024-01-01")
            mock_get_filtered.return_value = []

            # Act
            result = local_storage_adapter._load_feed_posts_for_date(partition_date=partition_date)

            # Assert
            assert result == []


class TestLocalStorageAdapter_load_posts_used_in_feeds_for_date:
    """Tests for LocalStorageAdapter._load_posts_used_in_feeds_for_date method."""

    def test_loads_posts_used_in_feeds_for_date(self, local_storage_adapter):
        """Test loading posts used in feeds for a specific date."""
        # Arrange
        partition_date = "2024-01-01"
        mock_df = pd.DataFrame([{"uri": "uri1"}, {"uri": "uri2"}])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.return_value = mock_df

            # Act
            result = local_storage_adapter._load_posts_used_in_feeds_for_date(
                partition_date=partition_date
            )

            # Assert
            mock_load_data.assert_called_once()
            call_kwargs = mock_load_data.call_args.kwargs
            assert call_kwargs["service"] == "fetch_posts_used_in_feeds"
            assert call_kwargs["directory"] == "cache"
            assert call_kwargs["partition_date"] == partition_date
            assert len(result) == 2
            assert all(isinstance(post, PostUsedInFeedModel) for post in result)
            assert result[0].uri == "uri1"
            assert result[1].uri == "uri2"

    def test_returns_empty_list_when_no_data(self, local_storage_adapter):
        """Test that empty list is returned when no data found."""
        # Arrange
        partition_date = "2024-01-01"
        mock_df = pd.DataFrame([])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.return_value = mock_df

            # Act
            result = local_storage_adapter._load_posts_used_in_feeds_for_date(
                partition_date=partition_date
            )

            # Assert
            assert result == []


class TestLocalStorageAdapter_load_candidate_pool_posts_for_date:
    """Tests for LocalStorageAdapter._load_candidate_pool_posts_for_date method."""

    def test_loads_candidate_pool_posts_for_date(self, local_storage_adapter):
        """Test that candidate pool posts are loaded using load_all_posts."""
        # Arrange
        lookback_start = "2023-12-25"
        lookback_end = "2024-01-01"
        expected_posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
        ]

        with patch.object(local_storage_adapter, "load_all_posts") as mock_load_all:
            mock_load_all.return_value = expected_posts

            # Act
            result = local_storage_adapter._load_candidate_pool_posts_for_date(
                lookback_start_date=lookback_start, lookback_end_date=lookback_end
            )

            # Assert
            mock_load_all.assert_called_once_with(
                start_date=lookback_start, end_date=lookback_end
            )
            assert result == expected_posts


class TestLocalStorageAdapter_get_candidate_pool_posts_used_in_feeds_for_date:
    """Tests for LocalStorageAdapter._get_candidate_pool_posts_used_in_feeds_for_date method."""

    def test_filters_posts_by_uris_in_feeds(self, local_storage_adapter):
        """Test that posts are filtered to only those used in feeds."""
        # Arrange
        candidate_pool_posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text2", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri3", text="text3", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
        ]
        posts_used_in_feeds = [
            PostUsedInFeedModel(uri="uri1"),
            PostUsedInFeedModel(uri="uri3"),
        ]

        # Act
        result = local_storage_adapter._get_candidate_pool_posts_used_in_feeds_for_date(
            candidate_pool_posts=candidate_pool_posts,
            posts_used_in_feeds=posts_used_in_feeds,
        )

        # Assert
        assert len(result) == 2
        assert result[0].uri == "uri1"
        assert result[1].uri == "uri3"

    def test_returns_empty_list_when_no_matching_posts(self, local_storage_adapter):
        """Test that empty list is returned when no posts match."""
        # Arrange
        candidate_pool_posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
        ]
        posts_used_in_feeds = [PostUsedInFeedModel(uri="uri2")]

        # Act
        result = local_storage_adapter._get_candidate_pool_posts_used_in_feeds_for_date(
            candidate_pool_posts=candidate_pool_posts,
            posts_used_in_feeds=posts_used_in_feeds,
        )

        # Assert
        assert result == []

    def test_returns_empty_list_when_empty_candidate_pool(self, local_storage_adapter):
        """Test that empty list is returned when candidate pool is empty."""
        # Arrange
        candidate_pool_posts = []
        posts_used_in_feeds = [PostUsedInFeedModel(uri="uri1")]

        # Act
        result = local_storage_adapter._get_candidate_pool_posts_used_in_feeds_for_date(
            candidate_pool_posts=candidate_pool_posts,
            posts_used_in_feeds=posts_used_in_feeds,
        )

        # Assert
        assert result == []


class TestLocalStorageAdapter_get_previously_labeled_post_uris:
    """Tests for LocalStorageAdapter.get_previously_labeled_post_uris method."""

    def test_loads_post_uris_from_cache_and_active(self, local_storage_adapter):
        """Test that post URIs are loaded from both cache and active directories."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        cache_df = pd.DataFrame([{id_field: "uri1"}, {id_field: "uri2"}])
        active_df = pd.DataFrame([{id_field: "uri2"}, {id_field: "uri3"}])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data, patch("services.backfill.repositories.adapters.logger") as mock_logger:
            mock_load_data.side_effect = [cache_df, active_df]

            # Act
            result = local_storage_adapter.get_previously_labeled_post_uris(
                service=service,
                id_field=id_field,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            assert mock_load_data.call_count == 2
            # First call for cache
            assert mock_load_data.call_args_list[0].kwargs["directory"] == "cache"
            # Second call for active
            assert mock_load_data.call_args_list[1].kwargs["directory"] == "active"
            assert result == {"uri1", "uri2", "uri3"}
            assert len(result) == 3

    def test_deduplicates_uris_from_cache_and_active(self, local_storage_adapter):
        """Test that duplicate URIs are deduplicated when combining cache and active."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        cache_df = pd.DataFrame([{id_field: "uri1"}, {id_field: "uri2"}])
        active_df = pd.DataFrame([{id_field: "uri2"}, {id_field: "uri2"}])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.side_effect = [cache_df, active_df]

            # Act
            result = local_storage_adapter.get_previously_labeled_post_uris(
                service=service,
                id_field=id_field,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            assert result == {"uri1", "uri2"}
            assert len(result) == 2

    def test_raises_backfill_data_adapter_error_on_exception(self, local_storage_adapter):
        """Test that BackfillDataAdapterError is raised when exception occurs."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        test_error = Exception("Test error")

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data, patch("services.backfill.repositories.adapters.logger") as mock_logger:
            mock_load_data.side_effect = test_error

            # Act & Assert
            with pytest.raises(BackfillDataAdapterError) as exc_info:
                local_storage_adapter.get_previously_labeled_post_uris(
                    service=service,
                    id_field=id_field,
                    start_date=start_date,
                    end_date=end_date,
                )

            # Assert
            assert "Failed to load" in str(exc_info.value)
            assert service in str(exc_info.value)
            assert "Test error" in str(exc_info.value)
            mock_logger.warning.assert_called_once()

    def test_returns_empty_set_when_no_data(self, local_storage_adapter):
        """Test that empty set is returned when no data found."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        # Create empty DataFrames with the correct column structure
        empty_df = pd.DataFrame(columns=[id_field])

        with patch(
            "services.backfill.repositories.adapters.load_data_from_local_storage"
        ) as mock_load_data:
            mock_load_data.side_effect = [empty_df, empty_df]

            # Act
            result = local_storage_adapter.get_previously_labeled_post_uris(
                service=service,
                id_field=id_field,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            assert result == set()


class TestS3Adapter_load_all_posts:
    """Tests for S3Adapter.load_all_posts method."""

    def test_raises_not_implemented_error(self, s3_adapter):
        """Test that load_all_posts raises NotImplementedError."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch("services.backfill.repositories.adapters.logger") as mock_logger:
            # Act & Assert
            with pytest.raises(NotImplementedError) as exc_info:
                s3_adapter.load_all_posts(start_date=start_date, end_date=end_date)

            # Assert
            assert "S3 data loading is not yet implemented" in str(exc_info.value)
            mock_logger.warning.assert_called_once()


class TestS3Adapter_load_feed_posts:
    """Tests for S3Adapter.load_feed_posts method."""

    def test_raises_not_implemented_error(self, s3_adapter):
        """Test that load_feed_posts raises NotImplementedError."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch("services.backfill.repositories.adapters.logger") as mock_logger:
            # Act & Assert
            with pytest.raises(NotImplementedError) as exc_info:
                s3_adapter.load_feed_posts(start_date=start_date, end_date=end_date)

            # Assert
            assert "S3 data loading is not yet implemented" in str(exc_info.value)
            mock_logger.warning.assert_called_once()


class TestS3Adapter_get_previously_labeled_post_uris:
    """Tests for S3Adapter.get_previously_labeled_post_uris method."""

    def test_raises_not_implemented_error(self, s3_adapter):
        """Test that get_previously_labeled_post_uris raises NotImplementedError."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch("services.backfill.repositories.adapters.logger") as mock_logger:
            # Act & Assert
            with pytest.raises(NotImplementedError) as exc_info:
                s3_adapter.get_previously_labeled_post_uris(
                    service=service,
                    id_field=id_field,
                    start_date=start_date,
                    end_date=end_date,
                )

            # Assert
            assert "S3 data loading is not yet implemented" in str(exc_info.value)
            mock_logger.warning.assert_called_once()


class TestLocalStorageAdapter_deduplicate_feed_posts:
    """Tests for LocalStorageAdapter._deduplicate_feed_posts method."""

    def test_deduplicates_posts_with_same_uri(self, local_storage_adapter):
        """Test that duplicate posts with the same URI are deduplicated, keeping only the first occurrence."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri1", text="text2", preprocessing_timestamp="2024-01-02T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text3", preprocessing_timestamp="2024-01-03T00:00:00"
            ),
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 2
        assert result[0].uri == "uri1"
        assert result[0].text == "text1"
        assert result[0].preprocessing_timestamp == "2024-01-01T00:00:00"
        assert result[1].uri == "uri2"
        assert result[1].text == "text3"

    def test_returns_all_posts_when_no_duplicates(self, local_storage_adapter):
        """Test that all posts are returned when there are no duplicate URIs."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text2", preprocessing_timestamp="2024-01-02T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri3", text="text3", preprocessing_timestamp="2024-01-03T00:00:00"
            ),
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 3
        assert result[0].uri == "uri1"
        assert result[1].uri == "uri2"
        assert result[2].uri == "uri3"

    def test_returns_empty_list_when_no_posts(self, local_storage_adapter):
        """Test that empty list is returned when input is empty."""
        # Arrange
        posts = []

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert result == []

    def test_keeps_first_occurrence_when_multiple_duplicates(self, local_storage_adapter):
        """Test that the first occurrence is kept when there are multiple duplicates of the same URI."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="first", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri1", text="second", preprocessing_timestamp="2024-01-02T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri1", text="third", preprocessing_timestamp="2024-01-03T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="other", preprocessing_timestamp="2024-01-04T00:00:00"
            ),
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 2
        assert result[0].uri == "uri1"
        assert result[0].text == "first"
        assert result[1].uri == "uri2"

    def test_deduplicates_posts_across_multiple_unique_uris(self, local_storage_adapter):
        """Test that deduplication works correctly with multiple URIs that each have duplicates."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text2", preprocessing_timestamp="2024-01-02T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri1", text="text3", preprocessing_timestamp="2024-01-03T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri3", text="text4", preprocessing_timestamp="2024-01-04T00:00:00"
            ),
            PostToEnqueueModel(
                uri="uri2", text="text5", preprocessing_timestamp="2024-01-05T00:00:00"
            ),
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 3
        assert result[0].uri == "uri1"
        assert result[0].text == "text1"
        assert result[1].uri == "uri2"
        assert result[1].text == "text2"
        assert result[2].uri == "uri3"
        assert result[2].text == "text4"

    def test_returns_single_post_when_only_one_post(self, local_storage_adapter):
        """Test that single post is returned when input contains only one post."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="text1", preprocessing_timestamp="2024-01-01T00:00:00"
            )
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 1
        assert result[0].uri == "uri1"
        assert result[0].text == "text1"
        assert result[0].preprocessing_timestamp == "2024-01-01T00:00:00"

    def test_preserves_post_model_structure(self, local_storage_adapter):
        """Test that returned posts are valid PostToEnqueueModel instances with all fields preserved."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="uri1", text="test_text", preprocessing_timestamp="2024-01-01T12:00:00"
            ),
            PostToEnqueueModel(
                uri="uri1", text="duplicate_text", preprocessing_timestamp="2024-01-02T12:00:00"
            ),
        ]

        # Act
        result = local_storage_adapter._deduplicate_feed_posts(posts=posts)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], PostToEnqueueModel)
        assert result[0].uri == "uri1"
        assert result[0].text == "test_text"
        assert result[0].preprocessing_timestamp == "2024-01-01T12:00:00"


class TestLocalStorageAdapter_write_records_to_storage:
    """Tests for LocalStorageAdapter.write_records_to_storage method."""

    @pytest.mark.parametrize("records", [
        pytest.param([{"id": 1, "data": "record1"}, {"id": 2, "data": "record2"}], id="with_records"),
        pytest.param([], id="empty_records"),
    ])
    def test_writes_records_to_storage_successfully(self, local_storage_adapter, records):
        """Test that records are written to storage successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"

        with patch(
            "lib.db.manage_local_data.export_data_to_local_storage"
        ) as mock_export:
            # Act
            local_storage_adapter.write_records_to_storage(
                integration_name=integration_name, records=records
            )

            # Assert
            mock_export.assert_called_once()
            call_kwargs = mock_export.call_args.kwargs
            assert call_kwargs["service"] == integration_name
            assert call_kwargs["export_format"] == "parquet"
            assert isinstance(call_kwargs["df"], pd.DataFrame)
            assert len(call_kwargs["df"]) == len(records)

    def test_logs_correct_record_count(self, local_storage_adapter, sample_records):
        """Test that correct record count is logged."""
        # Arrange
        integration_name = "ml_inference_perspective_api"

        with patch(
            "lib.db.manage_local_data.export_data_to_local_storage"
        ) as mock_export, patch(
            "services.backfill.repositories.adapters.logger"
        ) as mock_logger:
            # Act
            local_storage_adapter.write_records_to_storage(
                integration_name=integration_name, records=sample_records
            )

            # Assert
            # Verify that info logs are called with record count
            assert mock_logger.info.call_count >= 2  # At least "Exporting" and "Finished exporting"
            log_call_args = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Exporting" in call_args for call_args in log_call_args)
            assert any(str(len(sample_records)) in call_args for call_args in log_call_args)
            assert any("Finished exporting" in call_args for call_args in log_call_args)

    def test_raises_error_when_export_fails(self, local_storage_adapter, sample_records):
        """Test that error is raised when export_data_to_local_storage fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        test_error = Exception("Export error")

        with patch(
            "lib.db.manage_local_data.export_data_to_local_storage"
        ) as mock_export:
            mock_export.side_effect = test_error

            # Act & Assert
            with pytest.raises(Exception) as exc_info:
                local_storage_adapter.write_records_to_storage(
                    integration_name=integration_name, records=sample_records
                )

            # Assert
            assert "Export error" in str(exc_info.value)
            mock_export.assert_called_once()
            call_kwargs = mock_export.call_args.kwargs
            assert call_kwargs["service"] == integration_name
            assert call_kwargs["export_format"] == "parquet"

    def test_creates_dataframe_from_records(self, local_storage_adapter, sample_records):
        """Test that DataFrame is created correctly from records."""
        # Arrange
        integration_name = "ml_inference_perspective_api"

        with patch(
            "lib.db.manage_local_data.export_data_to_local_storage"
        ) as mock_export:
            # Act
            local_storage_adapter.write_records_to_storage(
                integration_name=integration_name, records=sample_records
            )

            # Assert
            mock_export.assert_called_once()
            call_kwargs = mock_export.call_args.kwargs
            df = call_kwargs["df"]
            assert isinstance(df, pd.DataFrame)
            assert len(df) == len(sample_records)
            assert list(df.columns) == list(sample_records[0].keys())
            assert df.iloc[0]["id"] == sample_records[0]["id"]
            assert df.iloc[0]["data"] == sample_records[0]["data"]


class TestS3Adapter_write_records_to_storage:
    """Tests for S3Adapter.write_records_to_storage method."""

    def test_raises_not_implemented_error(self, s3_adapter, sample_records):
        """Test that write_records_to_storage raises NotImplementedError."""
        # Arrange
        integration_name = "ml_inference_perspective_api"

        # Act & Assert
        with pytest.raises(NotImplementedError) as exc_info:
            s3_adapter.write_records_to_storage(
                integration_name=integration_name, records=sample_records
            )

        # Assert
        assert "S3 data writing is not yet implemented" in str(exc_info.value)
        assert "Use LocalStorageAdapter for now" in str(exc_info.value)

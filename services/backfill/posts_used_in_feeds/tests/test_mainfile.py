"""Tests for main.py."""

from unittest.mock import call, patch, MagicMock
from datetime import datetime

import pandas as pd
import pytest

from services.backfill.posts.load_data import INTEGRATIONS_LIST
from services.backfill.posts_used_in_feeds.main import backfill_posts_used_in_feeds


class TestBackfillPostsUsedInFeeds:
    """Tests for backfill_posts_used_in_feeds function."""

    @pytest.fixture
    def mock_posts_to_backfill(self):
        """Fixture for mock posts to backfill."""
        return {
            "ml_inference_perspective_api": [
                {"uri": "post1", "text": "text1", "created_at": "2024-01-01"},
                {"uri": "post2", "text": "text2", "created_at": "2024-01-02"}
            ],
            "ml_inference_sociopolitical": [
                {"uri": "post3", "text": "text3", "created_at": "2024-01-03"},
                {"uri": "post4", "text": "text4", "created_at": "2024-01-04"}
            ]
        }

    @pytest.fixture
    def mock_load_data_response(self):
        """Fixture for mock load_data_from_local_storage response."""
        def create_mock_df(uris=None):
            if uris is None:
                uris = ["post1", "post2"]
            
            return pd.DataFrame({
                "uri": uris,
                "text": [f"text{i}" for i in range(len(uris))],
                "created_at": pd.to_datetime([f"2024-01-0{i+1}" for i in range(len(uris))]),
                "partition_date": pd.to_datetime([f"2024-01-0{i+1}" for i in range(len(uris))])
            })
        return create_mock_df

    @pytest.fixture(autouse=True)
    def mock_run_mode(self):
        """Mock RUN_MODE to be 'test' for all tests."""
        with patch("lib.helper.RUN_MODE", "test"):
            yield

    # @patch("api.integrations_router.main.route_and_run_integration_request")
    # @patch("services.backfill.posts_used_in_feeds.helper.Queue")
    # @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    # @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    # def test_add_to_queue_only(
    #     self, 
    #     mock_posts_load_data, 
    #     mock_feeds_load_data, 
    #     mock_queue, 
    #     mock_route
    # ):
    #     """Test backfilling posts with queue addition only."""
    #     # Create mock data with proper datetime types
    #     mock_df = pd.DataFrame({
    #         "uri": ["post1", "post2"],
    #         "text": ["text1", "text2"],
    #         "created_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    #         "partition_date": pd.to_datetime(["2024-01-01", "2024-01-02"])
    #     })
        
    #     # Mock all expected calls
    #     mock_feeds_load_data.return_value = mock_df
    #     mock_posts_load_data.side_effect = [
    #         mock_df,  # For load_posts_used_in_feeds
    #         mock_df,  # For load_base_pool_posts (cache)
    #         mock_df,  # For load_base_pool_posts (active)
    #         mock_df,  # For load_service_post_uris (cache)
    #         mock_df   # For load_service_post_uris (active)
    #     ]
        
    #     mock_queue_instance = MagicMock()
    #     mock_queue.return_value = mock_queue_instance

    #     payload = {
    #         "add_posts_to_queue": True,
    #         "run_integrations": False,
    #         "start_date": "2024-01-01",
    #         "end_date": "2024-01-31"
    #     }

    #     backfill_posts_used_in_feeds(payload)

    #     # Verify queue operations
    #     mock_queue.assert_called()
    #     mock_queue_instance.batch_add_items_to_queue.assert_called()

    # @patch("api.integrations_router.main.route_and_run_integration_request")
    # @patch("services.backfill.posts_used_in_feeds.helper.Queue")
    # @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    # @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    # def test_with_integration_kwargs(
    #     self, 
    #     mock_posts_load_data, 
    #     mock_feeds_load_data, 
    #     mock_queue, 
    #     mock_route
    # ):
    #     """Test backfilling posts with integration kwargs."""
    #     # Create mock data with proper datetime types
    #     mock_df = pd.DataFrame({
    #         "uri": ["post1", "post2"],
    #         "text": ["text1", "text2"],
    #         "created_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    #         "partition_date": pd.to_datetime(["2024-01-01", "2024-01-02"])
    #     })
        
    #     # Mock all expected calls
    #     mock_feeds_load_data.return_value = mock_df
    #     mock_posts_load_data.side_effect = [
    #         mock_df,  # For load_posts_used_in_feeds
    #         mock_df,  # For load_base_pool_posts (cache)
    #         mock_df,  # For load_base_pool_posts (active)
    #         mock_df,  # For load_service_post_uris (cache)
    #         mock_df   # For load_service_post_uris (active)
    #     ]
        
    #     mock_queue_instance = MagicMock()
    #     mock_queue.return_value = mock_queue_instance

    #     payload = {
    #         "add_posts_to_queue": True,
    #         "run_integrations": True,
    #         "start_date": "2024-01-01",
    #         "end_date": "2024-01-31",
    #         "integration": ["ml_inference_perspective_api"],
    #         "integration_kwargs": {
    #             "ml_inference_perspective_api": {
    #                 "backfill_period": "days",
    #                 "backfill_duration": 2
    #             }
    #         }
    #     }

    #     backfill_posts_used_in_feeds(payload)

    #     mock_route.assert_called_once_with({
    #         "service": "ml_inference_perspective_api",
    #         "payload": {
    #             "run_type": "backfill",
    #             "backfill_period": "days",
    #             "backfill_duration": 2
    #         },
    #         "metadata": {}
    #     })

    # @patch("api.integrations_router.main.route_and_run_integration_request")
    # @patch("services.backfill.posts_used_in_feeds.helper.Queue")
    # @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    # def test_run_integrations_only(self, mock_load_data, mock_queue, mock_route):
    #     """Test backfilling posts with integration runs only."""
    #     payload = {
    #         "add_posts_to_queue": False,
    #         "run_integrations": True,
    #         "integration": ["ml_inference_perspective_api"]
    #     }

    #     backfill_posts_used_in_feeds(payload)

    #     mock_load_data.assert_not_called()
    #     mock_queue.assert_not_called()
    #     mock_route.assert_called_once_with({
    #         "service": "ml_inference_perspective_api",
    #         "payload": {"run_type": "backfill"},
    #         "metadata": {}
    #     })

    @patch("services.backfill.posts_used_in_feeds.main.route_and_run_integration_request")
    @patch("services.backfill.posts_used_in_feeds.helper.Queue")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_no_posts_found(self, mock_load_data, mock_queue, mock_route):
        """Test backfilling when no posts are found."""
        mock_load_data.return_value = pd.DataFrame()

        payload = {
            "add_posts_to_queue": False,
            "run_integrations": True
        }

        backfill_posts_used_in_feeds(payload)

        mock_queue.assert_not_called()
        assert mock_route.call_count == 3  # Length of INTEGRATIONS_LIST
        mock_route.assert_has_calls([
            call({
                "service": "ml_inference_perspective_api",
                "payload": {"run_type": "backfill"},
                "metadata": {}
            }),
            call({
                "service": "ml_inference_sociopolitical", 
                "payload": {"run_type": "backfill"},
                "metadata": {}
            }),
            call({
                "service": "ml_inference_ime",
                "payload": {"run_type": "backfill"}, 
                "metadata": {}
            })
        ])

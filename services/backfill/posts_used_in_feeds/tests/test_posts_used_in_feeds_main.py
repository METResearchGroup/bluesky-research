"""Tests for main.py."""

from unittest.mock import call, patch

import pandas as pd
import pytest

from services.backfill.posts_used_in_feeds.main import backfill_posts_used_in_feeds


class TestBackfillPostsUsedInFeeds:
    """Tests for backfill_posts_used_in_feeds function."""

    @pytest.fixture
    def mock_posts_to_backfill(self):
        """Fixture for mock posts to backfill."""
        return {
            "ml_inference_perspective_api": [
                {"uri": "post1", "text": "text1", "preprocessing_timestamp": "2024-01-01"},
                {"uri": "post2", "text": "text2", "preprocessing_timestamp": "2024-01-02"}
            ],
            "ml_inference_sociopolitical": [
                {"uri": "post3", "text": "text3", "preprocessing_timestamp": "2024-01-03"},
                {"uri": "post4", "text": "text4", "preprocessing_timestamp": "2024-01-04"}
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
                "preprocessing_timestamp": pd.to_datetime([f"2024-01-0{i+1}" for i in range(len(uris))]),
                "partition_date": pd.to_datetime([f"2024-01-0{i+1}" for i in range(len(uris))])
            })
        return create_mock_df

    @pytest.fixture(autouse=True)
    def mock_run_mode(self):
        """Mock RUN_MODE to be 'test' for all tests."""
        with patch("lib.helper.RUN_MODE", "test"):
            yield

    @patch("services.backfill.posts_used_in_feeds.main.invoke_pipeline_handler")
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
            call(
                service="ml_inference_perspective_api",
                payload={"run_type": "backfill"},
                request_metadata={}
            ),
            call(
                service="ml_inference_sociopolitical", 
                payload={"run_type": "backfill"},
                request_metadata={}
            ),
            call(
                service="ml_inference_ime",
                payload={"run_type": "backfill"}, 
                request_metadata={}
            )
        ])

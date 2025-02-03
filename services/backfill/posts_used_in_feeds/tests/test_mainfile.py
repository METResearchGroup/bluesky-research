"""Tests for main.py."""

from unittest.mock import call, patch, MagicMock

import pytest

from services.backfill.posts_used_in_feeds.main import backfill_posts_used_in_feeds


@pytest.fixture
def mock_posts_to_backfill():
    """Fixture for mock posts to backfill."""
    return {
        "ml_inference_perspective_api": [
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"}
        ],
        "ml_inference_sociopolitical": [
            {"uri": "post3", "text": "text3"},
            {"uri": "post4", "text": "text4"}
        ]
    }


@pytest.fixture(autouse=True)
def mock_run_mode():
    """Mock RUN_MODE to be 'test' for all tests."""
    with patch("lib.helper.RUN_MODE", "test"):
        yield


@patch("services.backfill.posts_used_in_feeds.main.route_and_run_integration_request")
@patch("services.backfill.posts_used_in_feeds.main.Queue")
@patch("services.backfill.posts_used_in_feeds.main.backfill_posts_used_in_feed_for_partition_dates")
def test_backfill_posts_used_in_feeds_add_to_queue_only(mock_backfill, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts used in feeds with only adding to queue.
    
    Input:
        payload with add_posts_to_queue=True and run_integrations=False
        
    Expected behavior:
        - Should call backfill_posts_used_in_feed_for_partition_dates
        - Should create queues and add posts
        - Should not call route_and_run_integration_request
    """
    mock_backfill.return_value = mock_posts_to_backfill
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": False,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    }

    backfill_posts_used_in_feeds(payload)

    mock_backfill.assert_called_once_with(
        start_date="2024-01-01",
        end_date="2024-01-31",
        exclude_partition_dates=None
    )

    assert mock_queue.call_count == 2
    mock_queue_instance.batch_add_items_to_queue.assert_called()
    mock_route.assert_not_called()


@patch("services.backfill.posts_used_in_feeds.main.route_and_run_integration_request")
@patch("services.backfill.posts_used_in_feeds.main.Queue")
@patch("services.backfill.posts_used_in_feeds.main.backfill_posts_used_in_feed_for_partition_dates")
def test_backfill_posts_used_in_feeds_run_integrations_only(mock_backfill, mock_queue, mock_route):
    """Test backfilling posts used in feeds with only running integrations.
    
    Input:
        payload with add_posts_to_queue=False and run_integrations=True
        
    Expected behavior:
        - Should not call backfill_posts_used_in_feed_for_partition_dates
        - Should not create queues
        - Should call route_and_run_integration_request for each integration
    """
    payload = {
        "add_posts_to_queue": False,
        "run_integrations": True,
        "integration": ["ml_inference_perspective_api"]
    }

    backfill_posts_used_in_feeds(payload)

    mock_backfill.assert_not_called()
    mock_queue.assert_not_called()
    
    mock_route.assert_called_once_with({
        "service": "ml_inference_perspective_api",
        "payload": {"run_type": "backfill"},
        "metadata": {}
    })


@patch("services.backfill.posts_used_in_feeds.main.route_and_run_integration_request")
@patch("services.backfill.posts_used_in_feeds.main.Queue")
@patch("services.backfill.posts_used_in_feeds.main.backfill_posts_used_in_feed_for_partition_dates")
def test_backfill_posts_used_in_feeds_with_integration_kwargs(mock_backfill, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts used in feeds with integration-specific kwargs.
    
    Input:
        payload with integration_kwargs specified
        
    Expected behavior:
        - Should pass integration kwargs to route_and_run_integration_request
    """
    mock_backfill.return_value = mock_posts_to_backfill
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True,
        "integration": ["ml_inference_perspective_api"],
        "integration_kwargs": {
            "ml_inference_perspective_api": {
                "backfill_period": "days",
                "backfill_duration": 2
            }
        }
    }

    backfill_posts_used_in_feeds(payload)

    mock_route.assert_called_once_with({
        "service": "ml_inference_perspective_api",
        "payload": {
            "run_type": "backfill",
            "backfill_period": "days",
            "backfill_duration": 2
        },
        "metadata": {}
    })


@patch("services.backfill.posts_used_in_feeds.main.route_and_run_integration_request")
@patch("services.backfill.posts_used_in_feeds.main.Queue")
@patch("services.backfill.posts_used_in_feeds.main.backfill_posts_used_in_feed_for_partition_dates")
def test_backfill_posts_used_in_feeds_no_posts_found(mock_backfill, mock_queue, mock_route):
    """Test backfilling when no posts are found.
    
    Input:
        payload with add_posts_to_queue=True but no posts found
        
    Expected behavior:
        - Should not create queues since no posts found
        - Should still run integrations for all services in INTEGRATIONS_LIST
    """
    mock_backfill.return_value = {}

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True
    }

    backfill_posts_used_in_feeds(payload)

    mock_backfill.assert_called_once()
    mock_queue.assert_not_called()
    
    # Should call route_and_run_integration_request for each integration
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

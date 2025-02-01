"""Tests for main.py."""

from unittest.mock import call, patch, MagicMock

import pytest

from services.backfill.posts.main import backfill_posts
from services.backfill.posts.load_data import INTEGRATIONS_LIST


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


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_add_to_queue_only(mock_load_posts, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts with only adding to queue."""
    mock_load_posts.return_value = mock_posts_to_backfill
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": False
    }

    backfill_posts(payload)

    # Verify load_posts_to_backfill was called with default None dates
    mock_load_posts.assert_called_once_with(None, start_date=None, end_date=None)

    # Verify Queue creation and batch_add for each integration
    assert mock_queue.call_count == 2
    mock_queue_instance.batch_add_items_to_queue.assert_called()

    # Verify route_and_run_integration_request was not called
    mock_route.assert_not_called()


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_run_integrations_only(mock_load_posts, mock_queue, mock_route):
    """Test backfilling posts with only running integrations."""
    payload = {
        "add_posts_to_queue": False,
        "run_integrations": True
    }

    backfill_posts(payload)

    # Verify load_posts_to_backfill was not called
    mock_load_posts.assert_not_called()

    # Verify Queue was not created
    mock_queue.assert_not_called()

    # Verify route_and_run_integration_request was called for each integration
    assert mock_route.call_count == len(INTEGRATIONS_LIST)
    for integration in INTEGRATIONS_LIST:
        mock_route.assert_any_call({
            "service": integration,
            "payload": {"run_type": "backfill"},
            "metadata": {}
        })


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_with_integration_kwargs(mock_load_posts, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts with integration-specific kwargs."""
    mock_load_posts.return_value = {
        "ml_inference_perspective_api": [
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"}
        ]
    }
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True,
        "integration": ["ml_inference_perspective_api"],
        "integration_kwargs": {
            "ml_inference_perspective_api": {
                "backfill_period": "days",
                "backfill_duration": 2,
                "run_classification": True
            }
        }
    }

    backfill_posts(payload)

    # Verify route_and_run_integration_request was called with kwargs
    mock_route.assert_called_once_with({
        "service": "ml_inference_perspective_api",
        "payload": {
            "run_type": "backfill",
            "backfill_period": "days",
            "backfill_duration": 2,
            "run_classification": True
        },
        "metadata": {}
    })


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_specific_integration(mock_load_posts, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts for specific integration."""
    mock_load_posts.return_value = {
        "ml_inference_perspective_api": [
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"}
        ]
    }
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True,
        "integration": ["ml_inference_perspective_api"]
    }

    backfill_posts(payload)

    # Verify load_posts_to_backfill was called with specific integration
    mock_load_posts.assert_called_once_with(["ml_inference_perspective_api"])

    # Verify Queue was created only for specific integration
    mock_queue.assert_called_once_with(
        queue_name="input_ml_inference_perspective_api",
        create_new_queue=True
    )

    # Verify route_and_run_integration_request was called only for specific integration
    mock_route.assert_called_once_with({
        "service": "ml_inference_perspective_api",
        "payload": {"run_type": "backfill"},
        "metadata": {}
    })


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_no_posts_found(mock_load_posts, mock_queue, mock_route):
    """Test backfilling when no posts are found."""
    mock_load_posts.return_value = {}

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True
    }

    backfill_posts(payload)

    # Verify load_posts_to_backfill was called
    mock_load_posts.assert_called_once_with(None)

    # Verify no Queue was created
    mock_queue.assert_not_called()

    # Verify route_and_run_integration_request was not called
    assert mock_route.call_count == 0


@patch("services.backfill.posts.main.route_and_run_integration_request")
@patch("services.backfill.posts.main.Queue")
@patch("services.backfill.posts.main.load_posts_to_backfill")
def test_backfill_posts_with_date_range(mock_load_posts, mock_queue, mock_route, mock_posts_to_backfill):
    """Test backfilling posts with date range filters."""
    mock_load_posts.return_value = mock_posts_to_backfill
    mock_queue_instance = MagicMock()
    mock_queue.return_value = mock_queue_instance

    payload = {
        "add_posts_to_queue": True,
        "run_integrations": True,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    }

    backfill_posts(payload)

    # Verify load_posts_to_backfill was called with date range
    mock_load_posts.assert_called_once_with(
        None,
        start_date="2024-01-01",
        end_date="2024-01-31"
    )

"""Tests for load_data.py."""

from unittest.mock import patch

import pytest
import pandas as pd

from services.backfill.posts.load_data import load_posts_to_backfill, INTEGRATIONS_LIST


@pytest.fixture
def mock_preprocessed_posts():
    """Fixture for mock preprocessed posts."""
    return [
        {"uri": "post1", "text": "text1"},
        {"uri": "post2", "text": "text2"}, 
        {"uri": "post3", "text": "text3"},
        {"uri": "post4", "text": "text4"},
        {"uri": "post5", "text": "text5"}
    ]


@pytest.fixture
def mock_post_uris():
    """Fixture for mock post URIs."""
    return {
        "ml_inference_perspective_api": {
            "post1", "post2"  # Some posts already processed
        },
        "ml_inference_sociopolitical": {
            "post1", "post3", "post4"  # Different posts already processed
        },
        "ml_inference_ime": set()  # No posts processed yet
    }


@patch("services.backfill.posts.load_data.load_preprocessed_posts")
@patch("services.backfill.posts.load_data.load_service_post_uris") 
def test_load_posts_to_backfill_specific_integration(mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
    """Test loading posts to backfill for a specific integration."""
    mock_load_posts.return_value = mock_preprocessed_posts
    
    def mock_load_service_post_uris(service, start_date=None, end_date=None):
        return mock_post_uris[service]
    
    mock_load_uris.side_effect = mock_load_service_post_uris
    
    # Test with a single integration
    result = load_posts_to_backfill(["ml_inference_perspective_api"])

    assert isinstance(result, dict)
    assert len(result) == 1
    assert "ml_inference_perspective_api" in result
    assert isinstance(result["ml_inference_perspective_api"], list)
    assert all(isinstance(post, dict) for post in result["ml_inference_perspective_api"])
    # Should be all posts minus the ones already in ml_inference_perspective_api
    assert len(result["ml_inference_perspective_api"]) == 3
    assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])


@patch("services.backfill.posts.load_data.load_preprocessed_posts")
@patch("services.backfill.posts.load_data.load_service_post_uris")
def test_load_posts_to_backfill_no_integrations(mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
    """Test loading posts to backfill when no integrations specified."""
    mock_load_posts.return_value = mock_preprocessed_posts
    
    def mock_load_service_post_uris(service, start_date=None, end_date=None):
        return mock_post_uris[service]
    
    mock_load_uris.side_effect = mock_load_service_post_uris
    
    # Test with None for integrations (should use all integrations)
    result = load_posts_to_backfill(None)
    
    assert isinstance(result, dict)
    assert len(result) == len(INTEGRATIONS_LIST)
    assert all(isinstance(posts, list) for posts in result.values())
    assert all(isinstance(post, dict) for posts in result.values() for post in posts)
    
    # For each integration, should be all posts minus the ones already in that integration
    assert len(result["ml_inference_perspective_api"]) == 3
    assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])
    
    assert len(result["ml_inference_sociopolitical"]) == 2
    assert all(post["uri"] in {"post2", "post5"} for post in result["ml_inference_sociopolitical"])
    
    assert len(result["ml_inference_ime"]) == 5
    assert all(post["uri"] in {"post1", "post2", "post3", "post4", "post5"} for post in result["ml_inference_ime"])


@patch("services.backfill.posts.load_data.load_preprocessed_posts")
@patch("services.backfill.posts.load_data.load_service_post_uris")
def test_load_posts_to_backfill_multiple_integrations(mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
    """Test loading posts to backfill for multiple specific integrations."""
    mock_load_posts.return_value = mock_preprocessed_posts
    
    def mock_load_service_post_uris(service, start_date=None, end_date=None):
        return mock_post_uris[service]
    
    mock_load_uris.side_effect = mock_load_service_post_uris
    
    # Test with multiple specific integrations
    result = load_posts_to_backfill([
        "ml_inference_perspective_api",
        "ml_inference_ime"
    ])
    
    assert isinstance(result, dict)
    assert len(result) == 2
    assert "ml_inference_perspective_api" in result
    assert "ml_inference_ime" in result
    assert all(isinstance(posts, list) for posts in result.values())
    assert all(isinstance(post, dict) for posts in result.values() for post in posts)
    
    assert len(result["ml_inference_perspective_api"]) == 3
    assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])
    
    assert len(result["ml_inference_ime"]) == 5
    assert all(post["uri"] in {"post1", "post2", "post3", "post4", "post5"} for post in result["ml_inference_ime"])


@patch("services.backfill.posts.load_data.load_preprocessed_posts")
@patch("services.backfill.posts.load_data.load_service_post_uris")
def test_load_posts_to_backfill_with_date_range(mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
    """Test loading posts to backfill with date range filters."""
    mock_load_posts.return_value = mock_preprocessed_posts
    
    def mock_load_service_post_uris(service, start_date=None, end_date=None):
        return mock_post_uris[service]
    
    mock_load_uris.side_effect = mock_load_service_post_uris
    
    # Test with date range
    result = load_posts_to_backfill(
        ["ml_inference_perspective_api"],
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    # Verify the date range was passed through
    mock_load_posts.assert_called_once_with(
        start_date="2024-01-01",
        end_date="2024-01-31",
        sorted_by_partition_date=False,
        ascending=False,
        output_format="list"
    )
    mock_load_uris.assert_called_once_with(
        service="ml_inference_perspective_api",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    assert isinstance(result, dict)
    assert "ml_inference_perspective_api" in result


@patch("services.backfill.posts.load_data.load_preprocessed_posts")
@patch("services.backfill.posts.load_data.load_service_post_uris")
def test_load_posts_to_backfill_dataframe_output(mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
    """Test loading posts to backfill with dataframe output."""
    mock_df = pd.DataFrame(mock_preprocessed_posts)
    mock_load_posts.return_value = mock_df
    
    def mock_load_service_post_uris(service, start_date=None, end_date=None):
        return mock_post_uris[service]
    
    mock_load_uris.side_effect = mock_load_service_post_uris
    
    result = load_posts_to_backfill(
        ["ml_inference_perspective_api"],
        output_format="df"
    )
    
    mock_load_posts.assert_called_once_with(
        start_date=None,
        end_date=None,
        sorted_by_partition_date=False,
        ascending=False,
        output_format="df"
    )
    
    assert isinstance(result, dict)
    assert "ml_inference_perspective_api" in result
    assert isinstance(result["ml_inference_perspective_api"], pd.DataFrame)
    assert len(result["ml_inference_perspective_api"]) == 3
    assert all(uri in {"post3", "post4", "post5"} for uri in result["ml_inference_perspective_api"]["uri"])
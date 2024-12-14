"""Unit tests for the process_data functions in the link_activities_to_feeds service."""

import json

import pandas as pd

from services.link_activities_to_feeds.process_data import (
    add_supplementary_fields_to_user_session_logs,
    consolidate_paginated_user_session_logs,
    consolidate_user_session_log_paginated_requests,
    get_user_session_logs_with_feeds_shown,
    process_user_session_logs,
)


def test_get_user_session_logs_with_feeds_shown():
    """Tests extracting user session logs with feeds shown from study user activities."""
    # Create test data
    data = {
        "data_type": ["user_session_log", "other_type", "user_session_log"],
        "data": [
            json.dumps({"feed": json.dumps([{"post": "uri1"}, {"post": "uri2"}]), "cursor": "abc"}),
            json.dumps({"some": "data"}),
            json.dumps({"feed": json.dumps([{"post": "uri3"}]), "cursor": "def"})
        ]
    }
    df = pd.DataFrame(data)

    # Run function
    logs_df, uris = get_user_session_logs_with_feeds_shown(df)

    # Verify results
    assert len(logs_df) == 2  # Only user_session_log rows
    assert set(logs_df["cursor"]) == {"abc", "def"}
    assert uris == {"uri1", "uri2", "uri3"}


def test_consolidate_paginated_user_session_logs_single_log():
    """Tests consolidating a single user session log."""
    data = {
        "author_did": ["user1"],
        "author_handle": ["handle1"],
        "activity_timestamp": ["2024-01-01-12:00:00"],
        "insert_timestamp": ["2024-01-01-12:00:00"],
        "feed_shown": [[{"post": "uri1"}, {"post": "uri2"}]],
        "cursor": ["abc"]
    }
    df = pd.DataFrame(data)

    result = consolidate_paginated_user_session_logs(df)

    assert len(result) == 1
    assert result["cursor"].iloc[0] == "abc"
    assert len(result["feed_shown"].iloc[0]) == 2


def test_consolidate_paginated_user_session_logs_multiple_logs():
    """Tests consolidating multiple paginated user session logs."""
    data = {
        "author_did": ["user1", "user1"],
        "author_handle": ["handle1", "handle1"],
        "activity_timestamp": ["2024-01-01-12:00:00", "2024-01-01-12:01:00"],
        "insert_timestamp": ["2024-01-01-12:00:00", "2024-01-01-12:01:00"],
        "feed_shown": [
            [{"post": "uri1"}, {"post": "uri2"}],
            [{"post": "uri2"}, {"post": "uri3"}]
        ],
        "cursor": ["abc", "eof"]
    }
    df = pd.DataFrame(data)

    result = consolidate_paginated_user_session_logs(df)

    assert len(result) == 1
    assert result["cursor"].iloc[0] == "eof"
    assert len(result["feed_shown"].iloc[0]) == 3  # Deduped uri2


def test_consolidate_user_session_log_paginated_requests():
    """Tests consolidating paginated requests across multiple users."""
    data = {
        "author_did": ["user1", "user1", "user2"],
        "author_handle": ["handle1", "handle1", "handle2"],
        "activity_timestamp": [
            "2024-01-01-12:00:00",
            "2024-01-01-12:01:00",
            "2024-01-01-12:00:00"
        ],
        "insert_timestamp": [
            "2024-01-01-12:00:00",
            "2024-01-01-12:01:00", 
            "2024-01-01-12:00:00"
        ],
        "feed_shown": [
            [{"post": "uri1"}, {"post": "uri2"}],
            [{"post": "uri2"}, {"post": "uri3"}],
            [{"post": "uri4"}]
        ],
        "cursor": ["abc", "eof", "def"]
    }
    df = pd.DataFrame(data)

    result = consolidate_user_session_log_paginated_requests(df, window_period_minutes=5)

    assert len(result) == 2  # One consolidated log per user
    assert set(result["author_did"]) == {"user1", "user2"}
    # Check that the consolidated feed contains all unique posts in order
    expected_feed = [
        {"post": "uri1"},
        {"post": "uri2"}, 
        {"post": "uri3"}
    ]
    assert result.iloc[0]["feed_shown"] == expected_feed


def test_add_supplementary_fields_to_user_session_logs(mocker):
    """Tests adding supplementary fields to user session logs."""
    # Mock backfill_feed_generation_timestamp to return a fixed timestamp
    mock_timestamp = "2024-01-01-00:00:00"
    mocker.patch(
        "services.link_activities_to_feeds.process_data.backfill_feed_generation_timestamp",
        return_value=[mock_timestamp]
    )

    # Create test data
    data = {
        "author_did": ["user1"],
        "author_handle": ["handle1"], 
        "activity_timestamp": ["2024-01-01-12:00:00"],
        "insert_timestamp": ["2024-01-01-12:00:00"],
        "feed_shown": [
            [{"post": "uri1"}, {"post": "uri2"}]
        ]
    }
    df = pd.DataFrame(data)

    result = add_supplementary_fields_to_user_session_logs(df)

    # Verify feed generation timestamp was added
    assert "feed_generation_timestamp" in result.columns
    assert result["feed_generation_timestamp"].iloc[0] == mock_timestamp

    # Verify set of post URIs was added
    assert "set_of_post_uris_in_feed" in result.columns
    assert result["set_of_post_uris_in_feed"].iloc[0] == {"uri1", "uri2"}


def test_process_user_session_logs(mocker):
    """Tests end-to-end processing of user session logs."""
    # Mock backfill_feed_generation_timestamp to return a fixed timestamp
    mock_timestamp = "2024-01-01-00:00:00"
    mocker.patch(
        "services.link_activities_to_feeds.process_data.backfill_feed_generation_timestamp",
        return_value=[mock_timestamp]
    )

    data = {
        "author_did": ["user1", "user1"],
        "author_handle": ["handle1", "handle1"],
        "activity_timestamp": ["2024-01-01-12:00:00", "2024-01-01-12:01:00"],
        "insert_timestamp": ["2024-01-01-12:00:00", "2024-01-01-12:01:00"],
        "feed_shown": [
            [{"post": "uri1"}, {"post": "uri2"}],
            [{"post": "uri2"}, {"post": "uri3"}]
        ],
        "cursor": ["abc", "eof"]
    }
    df = pd.DataFrame(data)

    result = process_user_session_logs(df)

    assert len(result) == 1  # Consolidated logs
    assert "feed_generation_timestamp" in result.columns
    assert result["feed_generation_timestamp"].iloc[0] == mock_timestamp
    assert "set_of_post_uris_in_feed" in result.columns

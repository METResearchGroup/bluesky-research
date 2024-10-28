"""Unit tests for mapping activities to feeds."""

import pandas as pd

from services.link_activities_to_feeds.map_activities_to_feeds import (
    map_comments_to_feeds,
    map_follows_to_feeds,
    map_likes_to_feeds
)

class TestMapCommentsToFeeds:
    """Tests for the map_comments_to_feeds function."""
    def test_no_comments(self):
        """Tests mapping when there are no comments.
        
        Expected behavior: Should return empty DataFrame when no comments exist,
        even if user session logs exist.
        """
        posts = pd.DataFrame(columns=["data", "author_did", "author_handle", "activity_timestamp"])
        user_session_logs = pd.DataFrame({
            "author_did": ["user1"],
            "feed_shown": [[]],
            "activity_timestamp": ["2024-01-01T00:00:00Z"],
            "feed_generation_timestamp": ["2024-01-01T00:00:00Z"]
        })

        result = map_comments_to_feeds(posts, user_session_logs)
        assert len(result) == 0
        
    def test_no_user_session_logs(self):
        """Tests mapping when there are no user session logs.
        
        Expected behavior: Should return DataFrame with comments but no feed mapping
        fields populated (thread_post_uri, feed_activity_timestamp, etc should be None).
        """
        posts = pd.DataFrame({
            "data": ['{"uri": "comment1", "text": "test", "reply_parent": "parent1"}'],
            "author_did": ["user1"],
            "author_handle": ["user1"],
            "activity_timestamp": ["2024-01-01T00:00:00Z"]
        })
        user_session_logs = pd.DataFrame(columns=["author_did", "feed_shown", "activity_timestamp"])
        
        result = map_comments_to_feeds(posts, user_session_logs)
        assert len(result) == 1
        assert result.iloc[0]["thread_post_uri"] is None
        assert result.iloc[0]["feed_activity_timestamp"] is None
        
    def test_no_linkable_comments(self):
        """Tests when comments exist but none can be linked to user session logs.
        
        Expected behavior: Should return DataFrame with comments but no feed mapping
        fields populated since parent/root posts aren't in any feeds.
        """
        posts = pd.DataFrame({
            "data": ['{"uri": "comment1", "text": "test", "reply_parent": "parent1"}'],
            "author_did": ["user1"],
            "author_handle": ["user1"], 
            "activity_timestamp": ["2024-01-01T00:00:00Z"]
        })
        user_session_logs = pd.DataFrame({
            "author_did": ["user1"],
            "set_of_post_uris_in_feed": [{"different_post"}],
            "activity_timestamp": ["2024-01-01T00:00:00Z"],
            "feed_generation_timestamp": ["2024-01-01T00:00:00Z"]
        })
        
        result = map_comments_to_feeds(posts, user_session_logs)
        assert len(result) == 1
        assert result.iloc[0]["thread_post_uri"] is None
        assert result.iloc[0]["feed_activity_timestamp"] is None

    def test_single_linkable_comment(self):
        """Tests when a comment can be linked to exactly one user session log.
        
        Expected behavior: Should return DataFrame with comment mapped to the feed
        where its parent/root post appeared.
        """
        posts = pd.DataFrame({
            "data": ['{"uri": "comment1", "text": "test", "reply_parent": "parent1"}'],
            "author_did": ["user1"],
            "author_handle": ["user1"],
            "activity_timestamp": ["2024-01-01T01:00:00Z"]
        })
        user_session_logs = pd.DataFrame({
            "author_did": ["user1"],
            "set_of_post_uris_in_feed": [{"parent1"}],
            "activity_timestamp": ["2024-01-01T00:00:00Z"],
            "feed_generation_timestamp": ["2024-01-01T00:00:00Z"]
        })
        
        result = map_comments_to_feeds(posts, user_session_logs)
        assert len(result) == 1
        assert result.iloc[0]["thread_post_uri"] == "parent1"
        assert result.iloc[0]["feed_activity_timestamp"] == "2024-01-01T00:00:00Z"

    def test_multiple_linkable_sessions(self):
        """Tests when a comment can be linked to multiple user session logs.
        
        Expected behavior: Should link comment to the user session log with the closest
        timestamp before the comment was made.
        """
        posts = pd.DataFrame({
            "data": ['{"uri": "comment1", "text": "test", "reply_parent": "parent1"}'],
            "author_did": ["user1"],
            "author_handle": ["user1"],
            "activity_timestamp": ["2024-01-01T02:00:00Z"]
        })
        user_session_logs = pd.DataFrame({
            "author_did": ["user1", "user1"],
            "set_of_post_uris_in_feed": [{"parent1"}, {"parent1"}],
            "activity_timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "feed_generation_timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]
        })
        
        result = map_comments_to_feeds(posts, user_session_logs)
        assert len(result) == 1
        assert result.iloc[0]["thread_post_uri"] == "parent1"
        assert result.iloc[0]["feed_activity_timestamp"] == "2024-01-01T01:00:00Z"
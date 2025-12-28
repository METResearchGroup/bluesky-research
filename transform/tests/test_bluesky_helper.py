"""Unit tests for bluesky_helper.py

Tests with a real post and author (in this case, the NYTimes). Picked a post
that is unlikely to be deleted or changed, but the tests would fail if that
weren't the case.
"""
import sys
from unittest.mock import MagicMock, patch

# Mock boto3 before importing modules that depend on it
sys.modules["boto3"] = MagicMock()

import pytest
from atproto_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed  # noqa
from atproto_client.models.app.bsky.feed.defs import ThreadViewPost
from atproto_client.models.app.bsky.feed.post import GetRecordResponse
from atproto_client.models.app.bsky.feed.get_likes import Like

# Assuming the functions to test are in your_module.py
from transform.bluesky_helper import (
    get_author_handle_and_post_id_from_link,
    get_author_did_from_handle,
    get_post_record_from_post_link,
    get_repost_profiles,
    get_liked_by_profiles,
    get_post_thread_replies,
    calculate_post_engagement,
    calculate_post_engagement_from_link,
    get_author_profile_from_link,
)

link = "https://bsky.app/profile/nytimes.com/post/3kowbajil7r2y"
expected_author_handle = "nytimes.com"
expected_post_id = "3kowbajil7r2y"
expected_post_uri = "at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kowbajil7r2y"  # noqa
expected_author_did = "did:plc:eclio37ymobqex2ncko63h4r"


def test_get_author_handle_and_post_id_from_link():
    res: dict = get_author_handle_and_post_id_from_link(link)
    assert res["author"] == expected_author_handle
    assert res["post_id"] == expected_post_id


@pytest.mark.integration
def test_get_author_did_from_handle():
    """Test that get_author_did_from_handle returns correct DID.
    
    This is an integration test that makes real API calls.
    """
    # Arrange
    expected = expected_author_did
    
    # Act
    result = get_author_did_from_handle(expected_author_handle)
    
    # Assert
    assert result == expected


@pytest.mark.integration
def test_get_post_record_from_post_link():
    """Test that get_post_record_from_post_link returns GetRecordResponse.

    This is an integration test that makes real API calls.
    """
    # Act
    result = get_post_record_from_post_link(link)

    # Assert
    assert isinstance(result, GetRecordResponse)


@pytest.mark.integration
def test_get_repost_profiles():
    """Test that get_repost_profiles returns list of ProfileView.
    
    This is an integration test that makes real API calls.
    """
    # Act
    result = get_repost_profiles(post_uri=expected_post_uri)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], ProfileView)


@pytest.mark.integration
def test_get_liked_by_profiles():
    """Test that get_liked_by_profiles returns list of Like.
    
    This is an integration test that makes real API calls.
    """
    # Act
    result = get_liked_by_profiles(post_uri=expected_post_uri)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], Like)


@pytest.mark.integration
def test_get_post_thread_replies():
    """Test that get_post_thread_replies returns list of ThreadViewPost.
    
    This is an integration test that makes real API calls.
    """
    # Act
    result = get_post_thread_replies(post_uri=expected_post_uri)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], ThreadViewPost)


@pytest.mark.integration
def test_calculate_post_engagement():
    """Test that calculate_post_engagement returns engagement dict.
    
    This is an integration test that makes real API calls.
    """
    # Arrange
    post_response = get_post_record_from_post_link(link)
    
    # Act
    result = calculate_post_engagement(post_response)
    
    # Assert
    assert isinstance(result, dict)
    assert "uri" in result
    assert "num_likes" in result
    assert "num_reposts" in result
    assert "num_replies" in result


@pytest.mark.integration
def test_calculate_post_engagement_from_link():
    """Test that calculate_post_engagement_from_link returns engagement dict.
    
    This is an integration test that makes real API calls.
    """
    # Act
    result = calculate_post_engagement_from_link(link)
    
    # Assert
    assert isinstance(result, dict)
    assert "link" in result
    assert "created_at" in result
    assert "num_likes" in result
    assert "num_reposts" in result
    assert "num_replies" in result


@pytest.mark.integration
def test_get_author_profile_from_link():
    """Test that get_author_profile_from_link returns ProfileViewDetailed.
    
    This is an integration test that makes real API calls.
    """
    # Act
    result = get_author_profile_from_link(link)
    
    # Assert
    assert isinstance(result, ProfileViewDetailed)
    assert result.handle == expected_author_handle
    assert result.did == expected_author_did


# ============================================================================
# Tests for Lazy Client Initialization
# ============================================================================


class TestGetClient:
    """Tests for _get_client() lazy initialization pattern."""

    def test_import_does_not_create_client(self):
        """Verify importing the module doesn't trigger client creation.
        
        This is the key test - before the refactor, importing would trigger
        get_client() at module level, causing rate limit errors.
        """
        # Arrange
        if 'transform.bluesky_helper' in sys.modules:
            from transform.bluesky_helper import _get_client
            _get_client.cache_clear()
        
        # Act
        with patch('transform.bluesky_helper.get_client') as mock_get_client:
            import transform.bluesky_helper
            
            # Assert
            mock_get_client.assert_not_called()

    def test_client_created_on_first_call(self):
        """Verify client is created when _get_client() is first called."""
        from transform.bluesky_helper import _get_client
        
        # Arrange
        _get_client.cache_clear()
        mock_client = MagicMock()
        expected_client = mock_client
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client) as mock_get_client:
            result = _get_client()
            
            # Assert
            assert mock_get_client.call_count == 1
            assert result == expected_client

    def test_client_cached_across_multiple_calls(self):
        """Verify @lru_cache ensures client is only created once."""
        from transform.bluesky_helper import _get_client
        
        # Arrange
        _get_client.cache_clear()
        mock_client = MagicMock()
        expected_call_count = 1
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client) as mock_get_client:
            client1 = _get_client()
            client2 = _get_client()
            client3 = _get_client()
            
            # Assert
            assert mock_get_client.call_count == expected_call_count
            assert client1 == client2 == client3

    def test_can_be_mocked_directly(self):
        """Verify _get_client() can be easily mocked in tests."""
        from transform import bluesky_helper
        
        # Arrange
        bluesky_helper._get_client.cache_clear()
        mock_client = MagicMock()
        expected_client = mock_client
        
        # Act
        with patch.object(bluesky_helper, '_get_client', return_value=mock_client):
            result = bluesky_helper._get_client()
            
            # Assert
            assert result == expected_client


@pytest.fixture
def mock_client():
    """Fixture providing a fully configured mock client."""
    client = MagicMock()
    
    # Configure all client methods that might be called
    client.get_profile.return_value = MagicMock(
        did="did:test:123",
        handle="test.bsky.social"
    )
    client.get_post.return_value = MagicMock(
        uri="at://did:test:123/app.bsky.feed.post/123",
        value=MagicMock(created_at="2024-01-01T00:00:00Z"),
        cid="test_cid"
    )
    # Mock responses for pagination functions must have cursor=None to break the loop
    reposted_by_response = MagicMock()
    reposted_by_response.reposted_by = []
    reposted_by_response.cursor = None
    client.get_reposted_by.return_value = reposted_by_response
    
    likes_response = MagicMock()
    likes_response.likes = []
    likes_response.cursor = None
    client.get_likes.return_value = likes_response
    
    client.get_post_thread.return_value = MagicMock(
        thread=MagicMock(replies=[])
    )
    
    followers_response = MagicMock()
    followers_response.followers = []
    followers_response.cursor = None
    client.get_followers.return_value = followers_response
    
    client.app.bsky.feed.get_actor_feeds.return_value = MagicMock(feeds=[])
    
    feed_response = MagicMock()
    feed_response.feed = []
    feed_response.cursor = None
    client.app.bsky.feed.get_feed.return_value = feed_response
    
    client.app.bsky.graph.get_list.return_value = MagicMock(
        list=MagicMock(
            cid="list_cid",
            name="Test List",
            uri="at://did:test:123/app.bsky.graph.list/123",
            description="Test description",
            creator=MagicMock(did="did:test:123", handle="test.bsky.social")
        ),
        items=[]
    )
    
    return client


class TestGetAuthorRecord:
    """Tests for get_author_record function."""

    def test_uses_lazy_client_initialization(self, mock_client):
        """Test that get_author_record uses lazy client initialization."""
        from transform.bluesky_helper import get_author_record, _get_client
        
        # Arrange
        _get_client.cache_clear()
        handle = "test.bsky.social"
        expected_call_count = 1
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client) as mock_get_client:
            # Before calling, client shouldn't be created
            assert mock_get_client.call_count == 0
            
            result = get_author_record(handle=handle)
            
            # Assert
            assert mock_get_client.call_count == expected_call_count
            mock_client.get_profile.assert_called_once_with(handle)

    def test_returns_profile_with_did(self, mock_client):
        """Test that get_author_record returns profile when called with handle."""
        from transform.bluesky_helper import get_author_record, _get_client
        
        # Arrange
        _get_client.cache_clear()
        handle = "test.bsky.social"
        expected_did = "did:test:123"
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client):
            result = get_author_record(handle=handle)
            
            # Assert
            assert result.did == expected_did
            mock_client.get_profile.assert_called_once_with(handle)

    def test_returns_profile_with_did_when_did_provided(self, mock_client):
        """Test that get_author_record returns profile when called with DID."""
        from transform.bluesky_helper import get_author_record, _get_client
        
        # Arrange
        _get_client.cache_clear()
        did = "did:test:123"
        expected_did = "did:test:123"
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client):
            result = get_author_record(did=did)
            
            # Assert
            assert result.did == expected_did
            mock_client.get_profile.assert_called_once_with(did)

    def test_raises_value_error_when_no_arguments(self, mock_client):
        """Test that get_author_record raises ValueError when neither DID nor handle provided."""
        from transform.bluesky_helper import get_author_record, _get_client
        
        # Arrange
        _get_client.cache_clear()
        
        # Act & Assert
        with patch('transform.bluesky_helper.get_client', return_value=mock_client):
            with pytest.raises(ValueError, match="Either a DID or handle must be provided"):
                get_author_record()


class TestGetPostRecordGivenPostUri:
    """Tests for get_post_record_given_post_uri function."""

    def test_uses_lazy_client_initialization(self, mock_client):
        """Test that get_post_record_given_post_uri uses lazy client initialization."""
        from transform.bluesky_helper import get_post_record_given_post_uri, _get_client
        
        # Arrange
        _get_client.cache_clear()
        post_uri = "at://did:test:123/app.bsky.feed.post/123"
        expected_call_count = 1
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client) as mock_get_client:
            result = get_post_record_given_post_uri(post_uri)
            
            # Assert
            assert mock_get_client.call_count == expected_call_count
            mock_client.get_post.assert_called_once()

    def test_returns_post_record(self, mock_client):
        """Test that get_post_record_given_post_uri returns post record."""
        from transform.bluesky_helper import get_post_record_given_post_uri, _get_client
        
        # Arrange
        _get_client.cache_clear()
        post_uri = "at://did:test:123/app.bsky.feed.post/123"
        expected_uri = "at://did:test:123/app.bsky.feed.post/123"
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client):
            result = get_post_record_given_post_uri(post_uri)
            
            # Assert
            assert result is not None
            assert result.uri == expected_uri
            mock_client.get_post.assert_called_once()


class TestLazyClientSharing:
    """Tests that verify multiple functions share the same cached client instance."""

    def test_multiple_functions_use_same_client_instance(self, mock_client):
        """Verify all functions use the same cached client instance."""
        from transform.bluesky_helper import (
            get_author_record,
            get_repost_profiles,
            get_liked_by_profiles,
            _get_client
        )
        
        # Arrange
        _get_client.cache_clear()
        handle = "test.bsky.social"
        post_uri = "at://did:test:123/app.bsky.feed.post/123"
        expected_call_count = 1
        
        # Act
        with patch('transform.bluesky_helper.get_client', return_value=mock_client) as mock_get_client:
            get_author_record(handle=handle)
            get_repost_profiles(post_uri)
            get_liked_by_profiles(post_uri)
            
            # Assert
            assert mock_get_client.call_count == expected_call_count
            assert mock_client.get_profile.called
            assert mock_client.get_reposted_by.called
            assert mock_client.get_likes.called

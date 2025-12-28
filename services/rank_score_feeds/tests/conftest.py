"""Pytest configuration for rank_score_feeds tests.

This conftest mocks the Bluesky API client to prevent import-time API calls
that cause test failures due to rate limiting and authentication errors.

The key insight is that transform.bluesky_helper.py creates a client at module level:
    client = get_client()  # This runs at import time!

By patching lib.helper.get_client() at module level (before any imports),
we ensure that when transform.bluesky_helper is imported, it gets our mock client.
"""

import pytest
from unittest.mock import patch, MagicMock

# Create mock client with all necessary methods
_mock_client = MagicMock()
_mock_client.get_profile.return_value = {"did": "did:test:123", "handle": "test.bsky.social"}
_mock_client.get_post.return_value = MagicMock()
_mock_client.get_reposted_by.return_value = MagicMock()
_mock_client.get_likes.return_value = MagicMock()
_mock_client.get_post_thread.return_value = MagicMock()
_mock_client.get_followers.return_value = MagicMock()
_mock_client.app.bsky.feed.get_actor_feeds.return_value = MagicMock()
_mock_client.app.bsky.feed.get_feed.return_value = MagicMock()
_mock_client.app.bsky.graph.get_list.return_value = MagicMock()

# Patch get_client() at module level - this runs when conftest.py is imported,
# which happens before pytest imports test files. This ensures that when
# transform.bluesky_helper does `client = get_client()`, it gets our mock.
_patcher = patch("lib.helper.get_client", return_value=_mock_client)
_patcher.start()


@pytest.fixture(scope="session", autouse=True)
def ensure_bluesky_client_mocked():
    """Session-scoped fixture to ensure the mock is active throughout tests.
    
    This fixture doesn't need to do anything since we patched at module level,
    but it serves as documentation and ensures the patch stays active.
    """
    yield _mock_client


def pytest_unconfigure(config):
    """Clean up patches after all tests complete."""
    _patcher.stop()

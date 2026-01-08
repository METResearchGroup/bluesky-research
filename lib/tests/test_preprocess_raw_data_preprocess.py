"""Tests for services.preprocess_raw_data.preprocess.py.

This test suite verifies the behavior of `postprocess_posts`, which converts a
posts DataFrame into list-of-dicts for downstream writes.
"""

import pandas as pd

from services.preprocess_raw_data.preprocess import postprocess_posts


class TestPostprocessPosts:
    """Tests for postprocess_posts function."""

    def test_returns_list_of_dicts_from_dataframe(self):
        """Test that DataFrame rows are converted into list-of-dicts."""
        # Arrange
        df = pd.DataFrame(
            [
                {"uri": "at://did:plc:abc/app.bsky.feed.post/1", "text": "hello"},
                {"uri": "at://did:plc:abc/app.bsky.feed.post/2", "text": "world"},
            ]
        )
        expected = [
            {"uri": "at://did:plc:abc/app.bsky.feed.post/1", "text": "hello"},
            {"uri": "at://did:plc:abc/app.bsky.feed.post/2", "text": "world"},
        ]

        # Act
        result = postprocess_posts(df)

        # Assert
        assert result == expected


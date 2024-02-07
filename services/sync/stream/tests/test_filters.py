"""Tests for filters.py"""
from datetime import timedelta

from lib.constants import current_datetime, NUM_DAYS_POST_RECENCY
from services.sync.stream.filters import (
    has_inappropriate_content, filter_created_post
)


class MockRecord:
    def __init__(self, text, labels=None, langs=None, created_at=None):
        self.text = text
        self.labels = labels if labels else []
        self.langs = langs if langs else []
        self.created_at = created_at


class MockLabel:
    def __init__(self, val):
        self.val = val


def test_has_inappropriate_content():
    """Tests the `has_inappropriate_content` function."""
    mock_record = MockRecord(text="This is a clean post", labels=[])
    assert not has_inappropriate_content(mock_record), "Expected False for clean content" # noqa

    mock_record_inappropriate_text = MockRecord(text="This post contains porn", labels=[]) # noqa
    assert has_inappropriate_content(mock_record_inappropriate_text), "Expected True for inappropriate content (text)" # noqa

    mock_record_with_label = MockRecord(text="Normal content", labels=[MockLabel(val='porn')]) # noqa
    assert has_inappropriate_content(mock_record_with_label), "Expected True for content labeled as inappropriate" # noqa


def test_filter_created_post():
    """Tests the `filter_created_post` function."""
    current_date = current_datetime.isoformat()
    mock_post = {
        'record': MockRecord(text="English post", langs=["en"], created_at=current_date), # noqa
        'author': "author_id"
    }
    assert filter_created_post(mock_post) is not None, "Expected post to pass the filter" # noqa

    mock_post_non_english = {
        'record': MockRecord(text="Non-English post", langs=["es"], created_at=current_date), # noqa
        'author': "author_id"
    }
    assert filter_created_post(mock_post_non_english) is None, "Expected non-English post to be filtered out" # noqa

    old_date = (current_datetime - timedelta(days=NUM_DAYS_POST_RECENCY + 1)).isoformat() # noqa
    mock_post_old = {
        'record': MockRecord(text="Old English post", langs=["en"], created_at=old_date), # noqa
        'author': "author_id"
    }
    assert filter_created_post(mock_post_old) is None, "Expected None for posts that don't pass the recency filter" # noqa

import pytest
from services.filter_raw_data.non_ml_filters import (
    has_no_explicit_terms, author_is_not_blocked, filter_raw_data_non_ml
)

# Mock data for testing
INAPPROPRIATE_POST = {"text": "This is a porn post.", "labels": ["nudity"]}
CLEAN_POST = {"text": "This is a safe post.", "labels": []}
BLOCKED_AUTHOR_POST = {"author": "spam_account", "text": "This is a post by a blocked author.", "labels": []}
UNBLOCKED_AUTHOR_POST = {"author": "legit_account", "text": "This is a post by a legit author.", "labels": []}

# Update blocked authors for testing
@pytest.fixture(autouse=True)
def setup_blocked_authors(monkeypatch):
    monkeypatch.setattr("your_module.BLOCKED_AUTHORS", ["spam_account"])

# Tests for has_no_explicit_terms function
@pytest.mark.parametrize("post, expected", [
    (INAPPROPRIATE_POST, False),
    (CLEAN_POST, True)
])
def test_has_no_explicit_terms(post, expected):
    assert has_no_explicit_terms(post) == expected

# Tests for author_is_not_blocked function
@pytest.mark.parametrize("post, expected", [
    (BLOCKED_AUTHOR_POST, False),
    (UNBLOCKED_AUTHOR_POST, True)
])
def test_author_is_not_blocked(post, expected):
    assert author_is_not_blocked(post) == expected

# Tests for filter_raw_data_non_ml function
@pytest.mark.parametrize("post, expected", [
    (INAPPROPRIATE_POST, False),
    (BLOCKED_AUTHOR_POST, False),
    ({"author": "legit_account", "text": "This is a safe post.", "labels": []}, True)  # post that should pass
])
def test_filter_raw_data_non_ml(post, expected):
    assert filter_raw_data_non_ml(post) == expected

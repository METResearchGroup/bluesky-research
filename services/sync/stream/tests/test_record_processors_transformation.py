"""Tests for record_processors/transformation module."""

import pytest

from services.sync.stream.record_processors.transformation.follow import (
    build_delete_follow_filename,
    build_follow_filename,
    extract_follower_did,
    extract_followee_did,
    transform_follow,
)
from services.sync.stream.record_processors.transformation.helper import extract_uri_suffix
from services.sync.stream.record_processors.transformation.like import (
    build_delete_like_filename,
    build_like_filename,
    extract_liked_post_uri,
    transform_like,
)
from services.sync.stream.record_processors.transformation.post import (
    build_delete_post_filename,
    build_post_filename,
    transform_post,
)
from services.sync.stream.tests.mock_firehose_data import (
    mock_follow_records,
    mock_like_records,
    mock_post_records,
)
from services.sync.stream.types import Operation


class TestTransformPost:
    """Tests for transform_post function."""

    def test_transform_post_create_operation(self):
        """Test that transform_post transforms CREATE operation correctly."""
        # Arrange
        post_record = mock_post_records[0]
        operation = Operation.CREATE

        # Act
        result = transform_post(post_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "author_did" in result
        assert "uri" in result

    def test_transform_post_delete_operation(self):
        """Test that transform_post returns dict as-is for DELETE operation."""
        # Arrange
        post_record = mock_post_records[1]  # deleted post
        operation = Operation.DELETE

        # Act
        result = transform_post(post_record, operation)

        # Assert
        assert result == post_record

    def test_transform_post_raises_error_for_invalid_operation(self):
        """Test that transform_post raises ValueError for invalid operation."""
        # Arrange
        post_record = mock_post_records[0]

        # Act & Assert
        with pytest.raises(ValueError, match="Unknown operation"):
            transform_post(post_record, "invalid_operation")  # type: ignore


class TestExtractUriSuffix:
    """Tests for extract_uri_suffix function."""

    def test_extract_uri_suffix_extracts_suffix(self):
        """Test that extract_uri_suffix extracts the URI suffix correctly."""
        # Arrange
        post_uri = "at://did:plc:abc123/app.bsky.feed.post/3kwd3wuubke2i"
        expected = "3kwd3wuubke2i"

        # Act
        result = extract_uri_suffix(post_uri)

        # Assert
        assert result == expected


class TestBuildPostFilename:
    """Tests for build_post_filename function."""

    def test_build_post_filename_creates_correct_filename(self):
        """Test that build_post_filename creates correct filename format."""
        # Arrange
        author_did = "did:plc:test-user"
        post_uri_suffix = "3kwd3wuubke2i"
        expected = f"author_did={author_did}_post_uri_suffix={post_uri_suffix}.json"

        # Act
        result = build_post_filename(author_did, post_uri_suffix)

        # Assert
        assert result == expected


class TestTransformLike:
    """Tests for transform_like function."""

    def test_transform_like_create_operation(self):
        """Test that transform_like transforms CREATE operation correctly."""
        # Arrange
        like_record = mock_like_records[0]
        operation = Operation.CREATE

        # Act
        result = transform_like(like_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "author" in result
        assert "uri" in result
        assert "record" in result

    def test_transform_like_delete_operation(self):
        """Test that transform_like returns dict as-is for DELETE operation."""
        # Arrange
        like_record = mock_like_records[1]  # deleted like
        operation = Operation.DELETE

        # Act
        result = transform_like(like_record, operation)

        # Assert
        assert result == like_record


class TestTransformFollow:
    """Tests for transform_follow function."""

    def test_transform_follow_create_operation(self):
        """Test that transform_follow transforms CREATE operation correctly."""
        # Arrange
        follow_record = mock_follow_records[0]
        operation = Operation.CREATE

        # Act
        result = transform_follow(follow_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "follower_did" in result
        assert "followee_did" in result
        assert "uri" in result

    def test_transform_follow_delete_operation(self):
        """Test that transform_follow returns dict as-is for DELETE operation."""
        # Arrange
        follow_record = mock_follow_records[1]  # deleted follow
        operation = Operation.DELETE

        # Act
        result = transform_follow(follow_record, operation)

        # Assert
        assert result == follow_record


class TestExtractFollowerFolloweeDid:
    """Tests for extract_follower_did and extract_followee_did functions."""

    def test_extract_follower_did_extracts_follower(self):
        """Test that extract_follower_did extracts follower DID correctly."""
        # Arrange
        follow_dict = {
            "follower_did": "did:plc:follower",
            "followee_did": "did:plc:followee",
        }
        expected = "did:plc:follower"

        # Act
        result = extract_follower_did(follow_dict)

        # Assert
        assert result == expected

    def test_extract_followee_did_extracts_followee(self):
        """Test that extract_followee_did extracts followee DID correctly."""
        # Arrange
        follow_dict = {
            "follower_did": "did:plc:follower",
            "followee_did": "did:plc:followee",
        }
        expected = "did:plc:followee"

        # Act
        result = extract_followee_did(follow_dict)

        # Assert
        assert result == expected


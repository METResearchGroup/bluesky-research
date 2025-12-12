"""Tests for record_processors/transformation module."""

import pytest

from services.sync.stream.record_processors.transformation.follow import (
    build_follow_filename,
    transform_follow,
)
from services.sync.stream.record_processors.transformation.helper import (
    build_record_filename,
    extract_uri_suffix,
)
from services.sync.stream.record_processors.transformation.like import (
    extract_liked_post_uri,
    transform_like,
)
from services.sync.stream.record_processors.transformation.post import transform_post
from services.sync.stream.tests.mock_firehose_data import (
    mock_follow_records,
    mock_like_records,
    mock_post_records,
)
from services.sync.stream.types import Operation, RecordType


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


class TestBuildRecordFilename:
    """Tests for build_record_filename function."""

    def test_build_record_filename_create_post(self):
        """Test that build_record_filename creates correct filename for CREATE post."""
        # Arrange
        author_did = "did:plc:test-user"
        post_uri_suffix = "3kwd3wuubke2i"
        expected = f"author_did={author_did}_post_uri_suffix={post_uri_suffix}.json"

        # Act
        result = build_record_filename(
            RecordType.POST, Operation.CREATE, author_did, post_uri_suffix
        )

        # Assert
        assert result == expected

    def test_build_record_filename_create_like(self):
        """Test that build_record_filename creates correct filename for CREATE like."""
        # Arrange
        author_did = "did:plc:test-user"
        like_uri_suffix = "3kwckubmt342n"
        expected = f"author_did={author_did}_like_uri_suffix={like_uri_suffix}.json"

        # Act
        result = build_record_filename(
            RecordType.LIKE, Operation.CREATE, author_did, like_uri_suffix
        )

        # Assert
        assert result == expected

    def test_build_record_filename_delete_post(self):
        """Test that build_record_filename creates correct filename for DELETE post."""
        # Arrange
        post_uri_suffix = "3kwd3wuubke2i"
        expected = f"post_uri_suffix={post_uri_suffix}.json"

        # Act
        result = build_record_filename(
            RecordType.POST, Operation.DELETE, "", post_uri_suffix
        )

        # Assert
        assert result == expected

    def test_build_record_filename_delete_like(self):
        """Test that build_record_filename creates correct filename for DELETE like."""
        # Arrange
        like_uri_suffix = "3kwckubmt342n"
        expected = f"like_uri_suffix={like_uri_suffix}.json"

        # Act
        result = build_record_filename(
            RecordType.LIKE, Operation.DELETE, "", like_uri_suffix
        )

        # Assert
        assert result == expected

    def test_build_record_filename_delete_follow(self):
        """Test that build_record_filename creates correct filename for DELETE follow."""
        # Arrange
        follow_uri_suffix = "3kwcxduaskd2p"
        expected = f"follow_uri_suffix={follow_uri_suffix}.json"

        # Act
        result = build_record_filename(
            RecordType.FOLLOW, Operation.DELETE, "", follow_uri_suffix
        )

        # Assert
        assert result == expected

    def test_build_record_filename_raises_error_for_invalid_operation(self):
        """Test that build_record_filename raises ValueError for invalid operation."""
        # Arrange
        author_did = "did:plc:test-user"
        uri_suffix = "3kwd3wuubke2i"

        # Act & Assert
        with pytest.raises(ValueError, match="Unknown operation"):
            build_record_filename(
                RecordType.POST, "invalid_operation", author_did, uri_suffix  # type: ignore
            )


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




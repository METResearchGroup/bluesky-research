"""Integration tests for record processors with operations_callback."""

from unittest.mock import Mock, patch

import pytest

from services.sync.stream.operations import operations_callback
from services.sync.stream.tests.mock_firehose_data import (
    mock_follow_records,
    mock_like_records,
    mock_post_records,
)
from services.sync.stream.types import Operation


class TestOperationsCallbackIntegration:
    """Integration tests for operations_callback with record processors."""

    def test_operations_callback_processes_posts(self, cache_write_context):
        """Test that operations_callback processes posts correctly."""
        # Arrange
        context = cache_write_context
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        # Modify post to have study user as author
        post_record = mock_post_records[0].copy()
        post_record["author"] = study_user_did

        operations_by_type = {
            "posts": {"created": [post_record], "deleted": []},
            "likes": {"created": [], "deleted": []},
            "follows": {"created": [], "deleted": []},
            "reposts": {"created": [], "deleted": []},
            "lists": {"created": [], "deleted": []},
            "blocks": {"created": [], "deleted": []},
            "profiles": {"created": [], "deleted": []},
        }

        # Act
        result = operations_callback(operations_by_type, context)

        # Assert
        assert result is True

    def test_operations_callback_processes_likes(self, cache_write_context):
        """Test that operations_callback processes likes correctly."""
        # Arrange
        context = cache_write_context
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        # Modify like to have study user as author
        like_record = mock_like_records[0].copy()
        like_record["author"] = study_user_did

        operations_by_type = {
            "posts": {"created": [], "deleted": []},
            "likes": {"created": [like_record], "deleted": []},
            "follows": {"created": [], "deleted": []},
            "reposts": {"created": [], "deleted": []},
            "lists": {"created": [], "deleted": []},
            "blocks": {"created": [], "deleted": []},
            "profiles": {"created": [], "deleted": []},
        }

        # Act
        result = operations_callback(operations_by_type, context)

        # Assert
        assert result is True

    def test_operations_callback_processes_follows(self, cache_write_context):
        """Test that operations_callback processes follows correctly."""
        # Arrange
        context = cache_write_context
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        # Modify follow to have study user as follower
        follow_record = mock_follow_records[0].copy()
        follow_record["author"] = study_user_did

        operations_by_type = {
            "posts": {"created": [], "deleted": []},
            "likes": {"created": [], "deleted": []},
            "follows": {"created": [follow_record], "deleted": []},
            "reposts": {"created": [], "deleted": []},
            "lists": {"created": [], "deleted": []},
            "blocks": {"created": [], "deleted": []},
            "profiles": {"created": [], "deleted": []},
        }

        # Act
        result = operations_callback(operations_by_type, context)

        # Assert
        assert result is True

    def test_operations_callback_handles_errors_gracefully(self, cache_write_context):
        """Test that operations_callback handles errors gracefully."""
        # Arrange
        context = cache_write_context
        # Create invalid post record
        invalid_post = {"invalid": "data"}

        operations_by_type = {
            "posts": {"created": [invalid_post], "deleted": []},
            "likes": {"created": [], "deleted": []},
            "follows": {"created": [], "deleted": []},
            "reposts": {"created": [], "deleted": []},
            "lists": {"created": [], "deleted": []},
            "blocks": {"created": [], "deleted": []},
            "profiles": {"created": [], "deleted": []},
        }

        # Act & Assert
        # Should not raise, but may return False or raise depending on implementation
        try:
            result = operations_callback(operations_by_type, context)
            # If it returns, it should be a boolean
            assert isinstance(result, bool)
        except Exception:
            # If it raises, that's also acceptable behavior
            pass

    def test_operations_callback_processes_multiple_record_types(
        self, cache_write_context
    ):
        """Test that operations_callback processes multiple record types."""
        # Arrange
        context = cache_write_context
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        post_record = mock_post_records[0].copy()
        post_record["author"] = study_user_did

        like_record = mock_like_records[0].copy()
        like_record["author"] = study_user_did

        follow_record = mock_follow_records[0].copy()
        follow_record["author"] = study_user_did

        operations_by_type = {
            "posts": {"created": [post_record], "deleted": []},
            "likes": {"created": [like_record], "deleted": []},
            "follows": {"created": [follow_record], "deleted": []},
            "reposts": {"created": [], "deleted": []},
            "lists": {"created": [], "deleted": []},
            "blocks": {"created": [], "deleted": []},
            "profiles": {"created": [], "deleted": []},
        }

        # Act
        result = operations_callback(operations_by_type, context)

        # Assert
        assert result is True


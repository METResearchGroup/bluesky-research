"""Tests for record_processors/processors module."""

from unittest.mock import Mock, patch

import pytest

from services.sync.stream.context import CacheWriteContext
from services.sync.stream.record_processors.processors.follow_processor import (
    FollowProcessor,
)
from services.sync.stream.record_processors.processors.like_processor import (
    LikeProcessor,
)
from services.sync.stream.record_processors.processors.post_processor import (
    PostProcessor,
)
from services.sync.stream.tests.mock_firehose_data import (
    mock_follow_records,
    mock_like_records,
    mock_post_records,
)
from services.sync.stream.types import FollowStatus, Operation, RecordType


class TestPostProcessor:
    """Tests for PostProcessor class."""

    def test_transform_calls_transform_post(self):
        """Test that transform calls transform_post helper."""
        # Arrange
        processor = PostProcessor()
        post_record = mock_post_records[0]
        operation = Operation.CREATE

        # Act
        result = processor.transform(post_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "author_did" in result or "uri" in result

    def test_get_routing_decisions_returns_empty_for_delete(self, cache_write_context):
        """Test that get_routing_decisions returns empty list for DELETE operation."""
        # Arrange
        processor = PostProcessor()
        transformed = {"uri": "at://test/post"}
        operation = Operation.DELETE
        context = cache_write_context

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert result == []

    def test_get_routing_decisions_returns_study_user_post_decision(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns decision for study user post."""
        # Arrange
        processor = PostProcessor()
        study_user_did = "did:plc:study-user-1"
        context = cache_write_context
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        transformed = {
            "author_did": study_user_did,
            "uri": "at://did:plc:test/app.bsky.feed.post/post-uri-1",
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) >= 1
        assert any(
            d.handler_key == RecordType.POST and d.author_did == study_user_did
            for d in result
        )

    def test_get_routing_decisions_returns_in_network_post_decision(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns decision for in-network user post."""
        # Arrange
        processor = PostProcessor()
        in_network_did = "did:plc:in-network-user"
        context = cache_write_context
        context.study_user_manager.in_network_user_dids_set.add(in_network_did)

        transformed = {
            "author_did": in_network_did,
            "uri": "at://did:plc:test/app.bsky.feed.post/post-uri-1",
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) >= 1
        assert any(
            d.handler_key.value == "in_network_post" and d.author_did == in_network_did
            for d in result
        )


class TestLikeProcessor:
    """Tests for LikeProcessor class."""

    def test_transform_calls_transform_like(self):
        """Test that transform calls transform_like helper."""
        # Arrange
        processor = LikeProcessor()
        like_record = mock_like_records[0]
        operation = Operation.CREATE

        # Act
        result = processor.transform(like_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "author" in result

    def test_get_routing_decisions_returns_empty_for_delete(self, cache_write_context):
        """Test that get_routing_decisions returns empty list for DELETE operation."""
        # Arrange
        processor = LikeProcessor()
        transformed = {"uri": "at://test/like"}
        operation = Operation.DELETE
        context = cache_write_context

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert result == []

    def test_get_routing_decisions_returns_study_user_like_decision(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns decision when study user likes post."""
        # Arrange
        processor = LikeProcessor()
        study_user_did = "did:plc:study-user-1"
        context = cache_write_context
        context.study_user_manager.study_users_dids_set.add(study_user_did)

        transformed = {
            "author": study_user_did,
            "uri": "at://did:plc:test/app.bsky.feed.like/like-uri-1",
            "record": {
                "subject": {"uri": "at://did:plc:other/app.bsky.feed.post/post-uri-1"}
            },
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) >= 1
        assert any(
            d.handler_key == RecordType.LIKE and d.author_did == study_user_did
            for d in result
        )


class TestFollowProcessor:
    """Tests for FollowProcessor class."""

    def test_transform_calls_transform_follow(self):
        """Test that transform calls transform_follow helper."""
        # Arrange
        processor = FollowProcessor()
        follow_record = mock_follow_records[0]
        operation = Operation.CREATE

        # Act
        result = processor.transform(follow_record, operation)

        # Assert
        assert isinstance(result, dict)
        assert "follower_did" in result
        assert "followee_did" in result

    def test_get_routing_decisions_returns_empty_for_delete(self, cache_write_context):
        """Test that get_routing_decisions returns empty list for DELETE operation."""
        # Arrange
        processor = FollowProcessor()
        transformed = {"uri": "at://test/follow"}
        operation = Operation.DELETE
        context = cache_write_context

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert result == []

    def test_get_routing_decisions_returns_follower_decision(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns decision when follower is study user."""
        # Arrange
        processor = FollowProcessor()
        follower_did = "did:plc:study-user-1"
        followee_did = "did:plc:other-user"
        context = cache_write_context
        context.study_user_manager.study_users_dids_set.add(follower_did)

        transformed = {
            "follower_did": follower_did,
            "followee_did": followee_did,
            "uri": "at://did:plc:test/app.bsky.graph.follow/follow-uri-1",
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) >= 1
        assert any(
            d.handler_key == RecordType.FOLLOW
            and d.author_did == follower_did
            and d.follow_status == FollowStatus.FOLLOWER
            for d in result
        )

    def test_get_routing_decisions_returns_followee_decision(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns decision when followee is study user."""
        # Arrange
        processor = FollowProcessor()
        follower_did = "did:plc:other-user"
        followee_did = "did:plc:study-user-1"
        context = cache_write_context
        context.study_user_manager.study_users_dids_set.add(followee_did)

        transformed = {
            "follower_did": follower_did,
            "followee_did": followee_did,
            "uri": "at://did:plc:test/app.bsky.graph.follow/follow-uri-1",
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) >= 1
        assert any(
            d.handler_key == RecordType.FOLLOW
            and d.author_did == followee_did
            and d.follow_status == FollowStatus.FOLLOWEE
            for d in result
        )

    def test_get_routing_decisions_returns_both_decisions_when_both_study_users(
        self, cache_write_context
    ):
        """Test that get_routing_decisions returns 2 decisions when both are study users."""
        # Arrange
        processor = FollowProcessor()
        follower_did = "did:plc:study-user-1"
        followee_did = "did:plc:study-user-2"
        context = cache_write_context
        context.study_user_manager.study_users_dids_set.add(follower_did)
        context.study_user_manager.study_users_dids_set.add(followee_did)

        transformed = {
            "follower_did": follower_did,
            "followee_did": followee_did,
            "uri": "at://did:plc:test/app.bsky.graph.follow/follow-uri-1",
        }
        operation = Operation.CREATE

        # Act
        result = processor.get_routing_decisions(transformed, operation, context)

        # Assert
        assert len(result) == 2
        follower_decision = next(
            d for d in result if d.follow_status == FollowStatus.FOLLOWER
        )
        followee_decision = next(
            d for d in result if d.follow_status == FollowStatus.FOLLOWEE
        )
        assert follower_decision.author_did == follower_did
        assert followee_decision.author_did == followee_did


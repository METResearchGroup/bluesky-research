"""Tests for record_processors/router.py - route_decisions function."""

from unittest.mock import Mock

import pytest

from services.sync.stream.core.context import CacheWriteContext
from services.sync.stream.record_processors.router import route_decisions
from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.core.types import FollowStatus, Operation, RecordType


class TestRouteDecisions:
    """Tests for route_decisions function."""

    def test_route_decisions_with_empty_list(self, cache_write_context):
        """Test that route_decisions handles empty decisions list."""
        # Arrange
        decisions: list[RoutingDecision] = []
        record = {"test": "data"}
        operation = Operation.CREATE
        context = cache_write_context

        # Act (should not raise)
        route_decisions(decisions, record, operation, context)

        # Assert - no exceptions raised

    def test_route_decisions_calls_handler_write_record(
        self, cache_write_context
    ):
        """Test that route_decisions calls handler.write_record with correct parameters."""
        # Arrange
        context = cache_write_context
        mock_handler = Mock()
        context.handler_registry.register_handler(RecordType.POST.value, mock_handler)

        decision = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user",
            filename="test_file.json",
        )
        decisions = [decision]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act
        route_decisions(decisions, record, operation, context)

        # Assert
        mock_handler.write_record.assert_called_once_with(
            record=record,
            operation=operation,
            author_did=decision.author_did,
            filename=decision.filename,
            follow_status=None,
        )

    def test_route_decisions_with_follow_status(
        self, cache_write_context
    ):
        """Test that route_decisions passes follow_status to handler."""
        # Arrange
        context = cache_write_context
        mock_handler = Mock()
        context.handler_registry.register_handler(RecordType.FOLLOW.value, mock_handler)

        decision = RoutingDecision(
            handler_key=RecordType.FOLLOW,
            author_did="did:plc:test-user",
            filename="test_file.json",
            follow_status=FollowStatus.FOLLOWER,
        )
        decisions = [decision]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act
        route_decisions(decisions, record, operation, context)

        # Assert
        mock_handler.write_record.assert_called_once_with(
            record=record,
            operation=operation,
            author_did=decision.author_did,
            filename=decision.filename,
            follow_status=FollowStatus.FOLLOWER,
        )

    def test_route_decisions_with_multiple_decisions(
        self, cache_write_context
    ):
        """Test that route_decisions handles multiple routing decisions."""
        # Arrange
        context = cache_write_context
        mock_handler1 = Mock()
        mock_handler2 = Mock()
        context.handler_registry.register_handler(RecordType.POST.value, mock_handler1)
        context.handler_registry.register_handler(
            RecordType.LIKE.value, mock_handler2
        )

        decision1 = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user-1",
            filename="test_file_1.json",
        )
        decision2 = RoutingDecision(
            handler_key=RecordType.LIKE,
            author_did="did:plc:test-user-2",
            filename="test_file_2.json",
        )
        decisions = [decision1, decision2]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act
        route_decisions(decisions, record, operation, context)

        # Assert
        assert mock_handler1.write_record.call_count == 1
        assert mock_handler2.write_record.call_count == 1

    def test_route_decisions_handles_missing_handler_gracefully(
        self, cache_write_context
    ):
        """Test that route_decisions handles missing handler gracefully."""
        # Arrange
        context = cache_write_context
        decision = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user",
            filename="test_file.json",
        )
        decisions = [decision]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act (should not raise, should log error)
        route_decisions(decisions, record, operation, context)

        # Assert - no exception raised, error logged

    def test_route_decisions_handles_handler_error_gracefully(
        self, cache_write_context
    ):
        """Test that route_decisions handles handler.write_record error gracefully."""
        # Arrange
        context = cache_write_context
        mock_handler = Mock()
        mock_handler.write_record.side_effect = Exception("Handler error")
        context.handler_registry.register_handler(RecordType.POST.value, mock_handler)

        decision = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user",
            filename="test_file.json",
        )
        decisions = [decision]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act (should not raise, should log error and continue)
        route_decisions(decisions, record, operation, context)

        # Assert
        mock_handler.write_record.assert_called_once()

    def test_route_decisions_continues_after_error(
        self, cache_write_context
    ):
        """Test that route_decisions continues processing after one decision fails."""
        # Arrange
        context = cache_write_context
        mock_handler1 = Mock()
        mock_handler1.write_record.side_effect = Exception("Handler error")
        mock_handler2 = Mock()
        context.handler_registry.register_handler(RecordType.POST.value, mock_handler1)
        context.handler_registry.register_handler(
            RecordType.LIKE.value, mock_handler2
        )

        decision1 = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user-1",
            filename="test_file_1.json",
        )
        decision2 = RoutingDecision(
            handler_key=RecordType.LIKE,
            author_did="did:plc:test-user-2",
            filename="test_file_2.json",
        )
        decisions = [decision1, decision2]
        record = {"test": "data"}
        operation = Operation.CREATE

        # Act
        route_decisions(decisions, record, operation, context)

        # Assert
        assert mock_handler1.write_record.call_count == 1
        assert mock_handler2.write_record.call_count == 1  # Should still be called


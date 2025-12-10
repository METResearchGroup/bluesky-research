"""Tests for record_processors/types.py - RoutingDecision dataclass."""

import pytest

from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.types import FollowStatus, HandlerKey, RecordType


class TestRoutingDecision:
    """Tests for RoutingDecision dataclass."""

    def test_routing_decision_creation_with_required_fields(self):
        """Test that RoutingDecision can be created with required fields."""
        # Arrange
        handler_key = RecordType.POST
        author_did = "did:plc:test-user"
        filename = "test_file.json"

        # Act
        decision = RoutingDecision(
            handler_key=handler_key, author_did=author_did, filename=filename
        )

        # Assert
        assert decision.handler_key == handler_key
        assert decision.author_did == author_did
        assert decision.filename == filename
        assert decision.follow_status is None
        assert decision.metadata == {}

    def test_routing_decision_creation_with_all_fields(self):
        """Test that RoutingDecision can be created with all fields."""
        # Arrange
        handler_key = RecordType.FOLLOW
        author_did = "did:plc:test-user"
        filename = "test_file.json"
        follow_status = FollowStatus.FOLLOWER
        metadata = {"key": "value"}

        # Act
        decision = RoutingDecision(
            handler_key=handler_key,
            author_did=author_did,
            filename=filename,
            follow_status=follow_status,
            metadata=metadata,
        )

        # Assert
        assert decision.handler_key == handler_key
        assert decision.author_did == author_did
        assert decision.filename == filename
        assert decision.follow_status == follow_status
        assert decision.metadata == metadata

    def test_routing_decision_is_immutable(self):
        """Test that RoutingDecision is immutable (frozen dataclass)."""
        # Arrange
        decision = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user",
            filename="test_file.json",
        )

        # Act & Assert
        with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
            decision.author_did = "new_did"

    def test_routing_decision_raises_error_for_empty_author_did(self):
        """Test that RoutingDecision raises ValueError for empty author_did."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="author_did cannot be empty"):
            RoutingDecision(
                handler_key=RecordType.POST, author_did="", filename="test_file.json"
            )

    def test_routing_decision_raises_error_for_empty_filename(self):
        """Test that RoutingDecision raises ValueError for empty filename."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="filename cannot be empty"):
            RoutingDecision(
                handler_key=RecordType.POST,
                author_did="did:plc:test-user",
                filename="",
            )

    def test_routing_decision_defaults_metadata_to_empty_dict(self):
        """Test that RoutingDecision defaults metadata to empty dict if None."""
        # Arrange & Act
        decision = RoutingDecision(
            handler_key=RecordType.POST,
            author_did="did:plc:test-user",
            filename="test_file.json",
            metadata=None,
        )

        # Assert
        assert decision.metadata == {}

    def test_routing_decision_with_handler_key_enum(self):
        """Test that RoutingDecision works with HandlerKey enum."""
        # Arrange
        handler_key = HandlerKey.IN_NETWORK_POST
        author_did = "did:plc:test-user"
        filename = "test_file.json"

        # Act
        decision = RoutingDecision(
            handler_key=handler_key, author_did=author_did, filename=filename
        )

        # Assert
        assert decision.handler_key == handler_key


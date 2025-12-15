"""Tests for FeedGenerationOrchestrator.

This test suite verifies the orchestrator structure and basic functionality.
These tests ensure:
- Orchestrator can be instantiated with FeedConfig
- All dependencies are properly constructed
- Basic structure is correct
"""

import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator


class TestFeedGenerationOrchestrator:
    """Tests for FeedGenerationOrchestrator class."""

    def test_orchestrator_can_be_instantiated(self):
        """Verify orchestrator can be instantiated with FeedConfig."""
        # Arrange
        config = FeedConfig()

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator is not None
        assert isinstance(orchestrator, FeedGenerationOrchestrator)
        assert orchestrator.config == config

    def test_orchestrator_has_required_dependencies(self):
        """Verify orchestrator constructs all required dependencies."""
        # Arrange
        config = FeedConfig()

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator.config is not None
        assert orchestrator.athena is not None
        assert orchestrator.s3 is not None
        assert orchestrator.dynamodb is not None
        assert orchestrator.glue is not None
        assert orchestrator.logger is not None

    def test_orchestrator_config_is_injected(self):
        """Verify config is properly injected into orchestrator."""
        # Arrange
        config = FeedConfig()
        config.max_feed_length = 50  # Modify to verify it's the same instance

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator.config.max_feed_length == 50
        assert orchestrator.config is config

    def test_orchestrator_has_run_method(self):
        """Verify orchestrator has the run method."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Act & Assert
        assert hasattr(orchestrator, "run")
        assert callable(orchestrator.run)

    def test_orchestrator_has_private_helper_methods(self):
        """Verify orchestrator has expected private helper methods."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Act & Assert
        expected_methods = [
            "_load_data",
            "_score_posts",
            "_export_scores",
            "_build_post_pools",
            "_generate_feeds",
            "_export_results",
        ]

        for method_name in expected_methods:
            assert hasattr(orchestrator, method_name), (
                f"Missing expected method: {method_name}"
            )
            assert callable(getattr(orchestrator, method_name)), (
                f"Method {method_name} is not callable"
            )


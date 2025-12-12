"""Tests for record_processors/factories.py - Factory functions."""

import pytest

from services.sync.stream.record_processors.factories import (
    create_all_processors,
    create_follow_processor,
    create_like_processor,
    create_post_processor,
)
from services.sync.stream.record_processors.processors.follow_processor import (
    FollowProcessor,
)
from services.sync.stream.record_processors.processors.like_processor import (
    LikeProcessor,
)
from services.sync.stream.record_processors.processors.post_processor import (
    PostProcessor,
)
from services.sync.stream.record_processors.registry import ProcessorRegistry


class TestCreatePostProcessor:
    """Tests for create_post_processor function."""

    def test_create_post_processor_returns_post_processor(self):
        """Test that create_post_processor returns PostProcessor instance."""
        # Act
        result = create_post_processor()

        # Assert
        assert isinstance(result, PostProcessor)


class TestCreateLikeProcessor:
    """Tests for create_like_processor function."""

    def test_create_like_processor_returns_like_processor(self):
        """Test that create_like_processor returns LikeProcessor instance."""
        # Act
        result = create_like_processor()

        # Assert
        assert isinstance(result, LikeProcessor)


class TestCreateFollowProcessor:
    """Tests for create_follow_processor function."""

    def test_create_follow_processor_returns_follow_processor(self):
        """Test that create_follow_processor returns FollowProcessor instance."""
        # Act
        result = create_follow_processor()

        # Assert
        assert isinstance(result, FollowProcessor)


class TestCreateAllProcessors:
    """Tests for create_all_processors function."""

    def test_create_all_processors_returns_registry(self):
        """Test that create_all_processors returns ProcessorRegistry."""
        # Act
        result = create_all_processors()

        # Assert
        assert isinstance(result, ProcessorRegistry)

    def test_create_all_processors_registers_all_processors(self):
        """Test that create_all_processors registers all processors."""
        # Act
        registry = create_all_processors()

        # Assert
        processors = registry.list_processors()
        assert "posts" in processors
        assert "likes" in processors
        assert "follows" in processors

    def test_create_all_processors_registry_has_correct_processors(self):
        """Test that registered processors are correct types."""
        # Act
        registry = create_all_processors()

        # Assert
        assert isinstance(registry.get_processor("posts"), PostProcessor)
        assert isinstance(registry.get_processor("likes"), LikeProcessor)
        assert isinstance(registry.get_processor("follows"), FollowProcessor)


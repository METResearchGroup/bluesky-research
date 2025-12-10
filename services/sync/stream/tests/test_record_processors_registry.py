"""Tests for record_processors/registry.py - ProcessorRegistry class."""

import pytest

from services.sync.stream.record_processors.processors.like_processor import (
    LikeProcessor,
)
from services.sync.stream.record_processors.processors.post_processor import (
    PostProcessor,
)
from services.sync.stream.record_processors.registry import ProcessorRegistry


class TestProcessorRegistry:
    """Tests for ProcessorRegistry class."""

    def test_init_creates_empty_registry(self):
        """Test that ProcessorRegistry initialization creates empty registry."""
        # Arrange & Act
        registry = ProcessorRegistry()

        # Assert
        assert registry.list_processors() == []

    def test_register_processor_registers_processor(self):
        """Test that register_processor adds processor to registry."""
        # Arrange
        registry = ProcessorRegistry()
        processor = PostProcessor()
        record_type = "posts"

        # Act
        registry.register_processor(record_type, processor)

        # Assert
        assert registry.list_processors() == [record_type]
        assert registry.get_processor(record_type) == processor

    def test_register_processor_registers_multiple_processors(self):
        """Test that multiple processors can be registered."""
        # Arrange
        registry = ProcessorRegistry()
        post_processor = PostProcessor()
        like_processor = LikeProcessor()

        # Act
        registry.register_processor("posts", post_processor)
        registry.register_processor("likes", like_processor)

        # Assert
        processors = registry.list_processors()
        assert len(processors) == 2
        assert "likes" in processors
        assert "posts" in processors
        assert registry.get_processor("posts") == post_processor
        assert registry.get_processor("likes") == like_processor

    def test_get_processor_returns_registered_processor(self):
        """Test that get_processor returns the correct processor."""
        # Arrange
        registry = ProcessorRegistry()
        processor = PostProcessor()
        registry.register_processor("posts", processor)

        # Act
        result = registry.get_processor("posts")

        # Assert
        assert result == processor

    def test_get_processor_raises_keyerror_for_unregistered_type(self):
        """Test that get_processor raises KeyError for unregistered record type."""
        # Arrange
        registry = ProcessorRegistry()

        # Act & Assert
        with pytest.raises(KeyError, match="Unknown record type: posts"):
            registry.get_processor("posts")

    def test_get_processor_error_message_includes_available_types(self):
        """Test that KeyError message includes available record types."""
        # Arrange
        registry = ProcessorRegistry()
        registry.register_processor("likes", LikeProcessor())

        # Act & Assert
        with pytest.raises(KeyError) as exc_info:
            registry.get_processor("posts")
        assert "Available types: ['likes']" in str(exc_info.value)

    def test_list_processors_returns_sorted_list(self):
        """Test that list_processors returns sorted list of record types."""
        # Arrange
        registry = ProcessorRegistry()
        registry.register_processor("follows", PostProcessor())
        registry.register_processor("posts", PostProcessor())
        registry.register_processor("likes", LikeProcessor())

        # Act
        result = registry.list_processors()

        # Assert
        assert result == ["follows", "likes", "posts"]

    def test_register_processor_raises_error_for_empty_record_type(self):
        """Test that register_processor raises ValueError for empty record_type."""
        # Arrange
        registry = ProcessorRegistry()
        processor = PostProcessor()

        # Act & Assert
        with pytest.raises(ValueError, match="record_type cannot be empty"):
            registry.register_processor("", processor)

    def test_register_processor_raises_error_for_none_processor(self):
        """Test that register_processor raises ValueError for None processor."""
        # Arrange
        registry = ProcessorRegistry()

        # Act & Assert
        with pytest.raises(ValueError, match="processor cannot be None"):
            registry.register_processor("posts", None)

    def test_clear_removes_all_processors(self):
        """Test that clear removes all registered processors."""
        # Arrange
        registry = ProcessorRegistry()
        registry.register_processor("posts", PostProcessor())
        registry.register_processor("likes", LikeProcessor())

        # Act
        registry.clear()

        # Assert
        assert registry.list_processors() == []


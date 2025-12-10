"""Factory functions for creating processors with dependencies."""

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
from services.sync.stream.record_processors.registry import ProcessorRegistry


def create_post_processor(context: CacheWriteContext) -> PostProcessor:
    """Create PostProcessor instance with dependencies.

    Args:
        context: Cache write context with dependencies

    Returns:
        PostProcessor instance
    """
    return PostProcessor()


def create_like_processor(context: CacheWriteContext) -> LikeProcessor:
    """Create LikeProcessor instance with dependencies.

    Args:
        context: Cache write context with dependencies

    Returns:
        LikeProcessor instance
    """
    return LikeProcessor()


def create_follow_processor(context: CacheWriteContext) -> FollowProcessor:
    """Create FollowProcessor instance with dependencies.

    Args:
        context: Cache write context with dependencies

    Returns:
        FollowProcessor instance
    """
    return FollowProcessor()


def create_all_processors(context: CacheWriteContext) -> ProcessorRegistry:
    """Create all processors and register them in a registry.

    Creates processor instances for all record types and registers them
    in a ProcessorRegistry. The registry is ready to use after this call.

    Args:
        context: Cache write context with dependencies

    Returns:
        ProcessorRegistry with all processors registered
    """
    registry = ProcessorRegistry()

    # Create and register processors
    registry.register_processor("posts", create_post_processor(context))
    registry.register_processor("likes", create_like_processor(context))
    registry.register_processor("follows", create_follow_processor(context))

    return registry

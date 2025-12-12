"""Factory functions for creating processors."""

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


def create_post_processor() -> PostProcessor:
    return PostProcessor()


def create_like_processor() -> LikeProcessor:
    return LikeProcessor()


def create_follow_processor() -> FollowProcessor:
    return FollowProcessor()


def create_all_processors() -> ProcessorRegistry:
    """Create all processors and register them in a registry.

    Creates processor instances for all record types and registers them
    in a ProcessorRegistry. The registry is ready to use after this call.

    Returns:
        ProcessorRegistry with all processors registered
    """
    registry = ProcessorRegistry()
    registry.register_processor("posts", create_post_processor())
    registry.register_processor("likes", create_like_processor())
    registry.register_processor("follows", create_follow_processor())

    return registry

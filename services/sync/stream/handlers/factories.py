"""Factory functions for creating handlers with dependencies."""

from services.sync.stream.handlers.generic import GenericRecordHandler
from services.sync.stream.handlers.config import HandlerConfig
from services.sync.stream.handlers.configs import (
    POST_CONFIG,
    LIKE_CONFIG,
    FOLLOW_CONFIG,
    LIKE_ON_USER_POST_CONFIG,
    REPLY_TO_USER_POST_CONFIG,
    IN_NETWORK_POST_CONFIG,
)
from services.sync.stream.protocols import (
    PathManagerProtocol,
    FileUtilitiesProtocol,
)
from services.sync.stream.types import HandlerKey


def create_handler(
    config: HandlerConfig,
    path_manager: PathManagerProtocol,
    file_utilities: FileUtilitiesProtocol,
) -> GenericRecordHandler:
    """Generic factory for creating handlers with configuration.

    Args:
        config: Handler configuration
        path_manager: Path manager for constructing paths
        file_utilities: File utilities for reading and writing records

    Returns:
        Configured GenericRecordHandler instance
    """
    return GenericRecordHandler(
        config=config,
        path_manager=path_manager,
        file_utilities=file_utilities,
    )


# Registry mapping handler keys to their configs
HANDLER_CONFIG_REGISTRY: dict[HandlerKey, HandlerConfig] = {
    HandlerKey.POST: POST_CONFIG,
    HandlerKey.LIKE: LIKE_CONFIG,
    HandlerKey.FOLLOW: FOLLOW_CONFIG,
    HandlerKey.LIKE_ON_USER_POST: LIKE_ON_USER_POST_CONFIG,
    HandlerKey.REPLY_TO_USER_POST: REPLY_TO_USER_POST_CONFIG,
    HandlerKey.IN_NETWORK_POST: IN_NETWORK_POST_CONFIG,
}


def create_handlers_for_all_types(
    path_manager: PathManagerProtocol,
    file_utilities: FileUtilitiesProtocol,
) -> dict[str, GenericRecordHandler]:
    """Create all handlers at once.

    Args:
        path_manager: Path manager for constructing paths
        file_utilities: File utilities for reading and writing records

    Returns:
        Dictionary mapping handler key strings to handler instances
    """
    handlers = {}
    for handler_key, config in HANDLER_CONFIG_REGISTRY.items():
        handler = create_handler(config, path_manager, file_utilities)
        handlers[handler_key.value] = handler
    return handlers

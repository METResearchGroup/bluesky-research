"""Factory functions for creating handlers with dependencies."""

from services.sync.stream.handlers.generic import GenericRecordHandler
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
    FileWriterProtocol,
    FileReaderProtocol,
)


def create_handler(
    config,
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Generic factory for creating handlers with configuration.

    Args:
        config: Handler configuration
        path_manager: Path manager for constructing paths
        file_writer: File writer for writing records
        file_reader: File reader for reading records

    Returns:
        Configured GenericRecordHandler instance
    """
    return GenericRecordHandler(
        config=config,
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )


def create_study_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating post handler."""
    return create_handler(POST_CONFIG, path_manager, file_writer, file_reader)


def create_study_user_like_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating like handler."""
    return create_handler(LIKE_CONFIG, path_manager, file_writer, file_reader)


def create_study_user_follow_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating follow handler."""
    return create_handler(FOLLOW_CONFIG, path_manager, file_writer, file_reader)


def create_study_user_like_on_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating like_on_user_post handler."""
    return create_handler(
        LIKE_ON_USER_POST_CONFIG, path_manager, file_writer, file_reader
    )


def create_study_user_reply_to_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating reply_to_user_post handler."""
    return create_handler(
        REPLY_TO_USER_POST_CONFIG, path_manager, file_writer, file_reader
    )


def create_in_network_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> GenericRecordHandler:
    """Factory for creating in-network post handler."""
    return create_handler(
        IN_NETWORK_POST_CONFIG, path_manager, file_writer, file_reader
    )

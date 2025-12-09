"""Factory functions for creating handlers with dependencies."""

from services.sync.stream.handlers.study_user_handlers import (
    StudyUserPostHandler,
    StudyUserLikeHandler,
    StudyUserFollowHandler,
    StudyUserLikeOnUserPostHandler,
    StudyUserReplyToUserPostHandler,
)
from services.sync.stream.protocols import (
    PathManagerProtocol,
    FileWriterProtocol,
    FileReaderProtocol,
)


def create_study_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> StudyUserPostHandler:
    """Factory for creating post handler."""
    return StudyUserPostHandler(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )


def create_study_user_like_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> StudyUserLikeHandler:
    """Factory for creating like handler."""
    return StudyUserLikeHandler(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )


def create_study_user_follow_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> StudyUserFollowHandler:
    """Factory for creating follow handler."""
    return StudyUserFollowHandler(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )


def create_study_user_like_on_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> StudyUserLikeOnUserPostHandler:
    """Factory for creating like_on_user_post handler."""
    return StudyUserLikeOnUserPostHandler(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )


def create_study_user_reply_to_user_post_handler(
    path_manager: PathManagerProtocol,
    file_writer: FileWriterProtocol,
    file_reader: FileReaderProtocol,
) -> StudyUserReplyToUserPostHandler:
    """Factory for creating reply_to_user_post handler."""
    return StudyUserReplyToUserPostHandler(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )

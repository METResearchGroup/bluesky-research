"""Concrete record handlers for study user activity."""

import os
from typing import Literal
from services.sync.stream.handlers.base import BaseRecordHandler
from services.sync.stream.protocols import (
    PathManagerProtocol,
    FileWriterProtocol,
    FileReaderProtocol,
)


class StudyUserPostHandler(BaseRecordHandler):
    """Handler for study user post records."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
    ):
        super().__init__(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
            record_type="post",
        )

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        **kwargs,
    ) -> None:
        """Write post record to cache."""
        root_path = self.path_manager.get_study_user_activity_path(
            operation=operation,
            record_type="post",
        )
        full_path = os.path.join(root_path, filename)

        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all post records from cache."""
        post_dir = os.path.join(base_path, "post")
        return self.file_reader.read_all_json_in_directory(post_dir)


class StudyUserLikeHandler(BaseRecordHandler):
    """Handler for study user like records."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
    ):
        super().__init__(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
            record_type="like",
        )

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        **kwargs,
    ) -> None:
        """Write like record to cache.

        Likes are nested by post_uri_suffix, so we need to extract that.
        """
        post_uri_suffix = record["record"]["subject"]["uri"].split("/")[-1]
        root_path = self.path_manager.get_study_user_activity_path(
            operation=operation,
            record_type="like",
        )
        folder_path = os.path.join(root_path, post_uri_suffix)
        full_path = os.path.join(folder_path, filename)

        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all like records from cache."""
        likes: list[dict] = []
        filepaths: list[str] = []
        liked_posts_path = os.path.join(base_path, "like")

        for post_uri in self.file_reader.list_files(liked_posts_path):
            post_path = os.path.join(liked_posts_path, post_uri)
            for like_record in self.file_reader.list_files(post_path):
                full_path = os.path.join(post_path, like_record)
                data = self.file_reader.read_json(full_path)
                likes.append(data)
                filepaths.append(full_path)

        return likes, filepaths


class StudyUserFollowHandler(BaseRecordHandler):
    """Handler for study user follow records."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
    ):
        super().__init__(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
            record_type="follow",
        )

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        follow_status: Literal["follower", "followee"],
        **kwargs,
    ) -> None:
        """Write follow record to cache.

        Follows are nested by follow_status (follower/followee).
        """
        root_path = self.path_manager.get_study_user_activity_path(
            operation=operation,
            record_type="follow",
            follow_status=follow_status,
        )
        full_path = os.path.join(root_path, filename)

        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all follow records from cache."""
        follows: list[dict] = []
        filepaths: list[str] = []
        follow_dir = os.path.join(base_path, "follow")

        if not os.path.exists(follow_dir):
            return follows, filepaths

        for follow_type in ["followee", "follower"]:
            follow_type_path = os.path.join(follow_dir, follow_type)
            if not os.path.exists(follow_type_path):
                continue

            for filename in self.file_reader.list_files(follow_type_path):
                full_path = os.path.join(follow_type_path, filename)
                data = self.file_reader.read_json(full_path)
                follows.append(data)
                filepaths.append(full_path)

        return follows, filepaths


class StudyUserLikeOnUserPostHandler(BaseRecordHandler):
    """Handler for likes on study user posts."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
    ):
        super().__init__(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
            record_type="like_on_user_post",
        )

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        **kwargs,
    ) -> None:
        """Write like_on_user_post record to cache.

        Likes on user posts are nested by post_uri_suffix.
        """
        post_uri_suffix = record["record"]["subject"]["uri"].split("/")[-1]
        root_path = self.path_manager.get_study_user_activity_path(
            operation=operation,
            record_type="like_on_user_post",
        )
        folder_path = os.path.join(root_path, post_uri_suffix)
        full_path = os.path.join(folder_path, filename)

        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all like_on_user_post records from cache."""
        likes: list[dict] = []
        filepaths: list[str] = []
        likes_path = os.path.join(base_path, "like_on_user_post")

        if not os.path.exists(likes_path):
            return likes, filepaths

        for post_uri in self.file_reader.list_files(likes_path):
            post_path = os.path.join(likes_path, post_uri)
            if not os.path.isdir(post_path):
                continue

            for like_record in self.file_reader.list_files(post_path):
                full_path = os.path.join(post_path, like_record)
                data = self.file_reader.read_json(full_path)
                likes.append(data)
                filepaths.append(full_path)

        return likes, filepaths


class StudyUserReplyToUserPostHandler(BaseRecordHandler):
    """Handler for replies to study user posts."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
    ):
        super().__init__(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
            record_type="reply_to_user_post",
        )

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        **kwargs,
    ) -> None:
        """Write reply_to_user_post record to cache.

        Replies are nested by parent_post_uri_suffix.
        """
        parent_post_uri_suffix = record["record"]["reply"]["root"]["uri"].split("/")[-1]
        root_path = self.path_manager.get_study_user_activity_path(
            operation=operation,
            record_type="reply_to_user_post",
        )
        folder_path = os.path.join(root_path, parent_post_uri_suffix)
        full_path = os.path.join(folder_path, filename)

        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all reply_to_user_post records from cache."""
        replies: list[dict] = []
        filepaths: list[str] = []
        replies_path = os.path.join(base_path, "reply_to_user_post")

        if not os.path.exists(replies_path):
            return replies, filepaths

        for post_uri in self.file_reader.list_files(replies_path):
            post_path = os.path.join(replies_path, post_uri)
            if not os.path.isdir(post_path):
                continue

            for reply_record in self.file_reader.list_files(post_path):
                full_path = os.path.join(post_path, reply_record)
                data = self.file_reader.read_json(full_path)
                replies.append(data)
                filepaths.append(full_path)

        return replies, filepaths

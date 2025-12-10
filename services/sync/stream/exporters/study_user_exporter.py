"""Exporter for study user activity data."""

import json
import os
import traceback

from lib.log.logger import get_logger

from services.sync.stream.exporters.base import BaseActivityExporter
from services.sync.stream.protocols import (
    PathManagerProtocol,
    StorageRepositoryProtocol,
)
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.types import Operation, RecordType


logger = get_logger(__file__)


class StudyUserActivityExporter(BaseActivityExporter):
    """Exporter for study user activity data."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        storage_repository: StorageRepositoryProtocol,
        handler_registry: RecordHandlerRegistry,
    ):
        """Initialize exporter.

        Args:
            path_manager: Path manager for constructing paths
            storage_repository: Storage repository for exporting data
            handler_registry: Handler registry instance
        """
        super().__init__(storage_repository)
        self.path_manager = path_manager
        self.handler_registry = handler_registry

    def export_activity_data(self) -> list[str]:
        """Export all study user activity data from cache to storage.

        Returns:
            List of filepaths that were processed
        """
        all_posts: list[dict] = []
        all_likes: list[dict] = []
        all_follows: list[dict] = []
        all_likes_on_user_posts: list[dict] = []
        all_replies_to_user_posts: list[dict] = []
        all_filepaths: list[str] = []

        # Process both create and delete operations
        for operation in [Operation.CREATE, Operation.DELETE]:
            # Get base path for this operation
            base_path = self.path_manager.get_study_user_activity_path(
                operation=operation,
                record_type=RecordType.POST,  # Use post as base - we'll navigate from here
            )
            # Remove the "post" suffix to get the operation-level path
            base_path = os.path.dirname(base_path)

            # Check if this operation directory exists
            if not os.path.exists(base_path):
                continue

            # Use registry to get handlers and read records
            for record_type in [
                RecordType.POST,
                RecordType.LIKE,
                RecordType.FOLLOW,
                RecordType.LIKE_ON_USER_POST,
                RecordType.REPLY_TO_USER_POST,
            ]:
                try:
                    # Use enum value for handler lookup (handlers expect strings)
                    handler = self.handler_registry.get_handler(record_type.value)
                    records, filepaths = handler.read_records(base_path)

                    if record_type == RecordType.POST:
                        all_posts.extend(records)
                    elif record_type == RecordType.LIKE:
                        all_likes.extend(records)
                    elif record_type == RecordType.FOLLOW:
                        all_follows.extend(records)
                    elif record_type == RecordType.LIKE_ON_USER_POST:
                        all_likes_on_user_posts.extend(records)
                    elif record_type == RecordType.REPLY_TO_USER_POST:
                        all_replies_to_user_posts.extend(records)

                    all_filepaths.extend(filepaths)
                except KeyError as e:
                    # Record type not registered
                    logger.warning(
                        f"Record type '{record_type.value}' not registered in handler registry. "
                        f"Operation: {operation.value}, Path: {base_path}",
                        context={
                            "error": str(e),
                            "record_type": record_type.value,
                            "operation": operation.value,
                        },
                    )
                    continue
                except FileNotFoundError as e:
                    # Directory or file doesn't exist
                    logger.warning(
                        f"File or directory not found while reading records. "
                        f"Record type: {record_type.value}, Operation: {operation.value}, Path: {base_path}",
                        context={
                            "error": str(e),
                            "record_type": record_type.value,
                            "operation": operation.value,
                            "path": base_path,
                        },
                    )
                    continue
                except PermissionError as e:
                    logger.warning(
                        f"Permission denied while reading records. "
                        f"Record type: {record_type.value}, Operation: {operation.value}, Path: {base_path}",
                        context={
                            "error": str(e),
                            "record_type": record_type.value,
                            "operation": operation.value,
                            "path": base_path,
                        },
                    )
                    continue
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Malformed JSON encountered while reading records. "
                        f"Record type: {record_type.value}, Operation: {operation.value}, Path: {base_path}",
                        context={
                            "error": str(e),
                            "record_type": record_type.value,
                            "operation": operation.value,
                            "path": base_path,
                        },
                    )
                    continue
                except (OSError, IOError) as e:
                    logger.warning(
                        f"I/O error while reading records. "
                        f"Record type: {record_type.value}, Operation: {operation.value}, Path: {base_path}",
                        context={
                            "error": str(e),
                            "record_type": record_type.value,
                            "operation": operation.value,
                            "path": base_path,
                        },
                    )
                    continue
                except Exception as e:
                    # Unexpected exceptions - log with full traceback
                    logger.warning(
                        f"Unexpected error while reading records. "
                        f"Record type: {record_type.value}, Operation: {operation.value}, Path: {base_path}. "
                        f"Error: {type(e).__name__}: {str(e)}",
                        context={
                            "error": str(e),
                            "error_type": type(e).__name__,
                            "record_type": record_type.value,
                            "operation": operation.value,
                            "path": base_path,
                            "traceback": traceback.format_exc(),
                        },
                    )
                    continue

        # Export each record type to storage
        if all_posts:
            self._export_dataframe(
                data=all_posts,
                service="study_user_activity",
                record_type=RecordType.POST.value,
            )

        if all_likes:
            self._export_dataframe(
                data=all_likes,
                service="study_user_activity",
                record_type=RecordType.LIKE.value,
            )

        if all_follows:
            # Follows are exported to scraped_user_social_network service
            self._export_dataframe(
                data=all_follows,
                service="scraped_user_social_network",
                record_type=None,
            )

        if all_likes_on_user_posts:
            self._export_dataframe(
                data=all_likes_on_user_posts,
                service="study_user_activity",
                record_type=RecordType.LIKE_ON_USER_POST.value,
            )

        if all_replies_to_user_posts:
            self._export_dataframe(
                data=all_replies_to_user_posts,
                service="study_user_activity",
                record_type=RecordType.REPLY_TO_USER_POST.value,
            )

        return all_filepaths

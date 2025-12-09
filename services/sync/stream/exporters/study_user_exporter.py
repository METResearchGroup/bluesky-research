"""Exporter for study user activity data."""

import os
import pandas as pd

from typing import TYPE_CHECKING

from services.sync.stream.protocols import (
    PathManagerProtocol,
    StorageRepositoryProtocol,
)
from services.sync.stream.handlers.registry import RecordHandlerRegistry

if TYPE_CHECKING:
    pass
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.constants import timestamp_format


class StudyUserActivityExporter:
    """Exporter for study user activity data."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        storage_repository: StorageRepositoryProtocol,
        handler_registry: type[RecordHandlerRegistry] | None = None,
    ):
        """Initialize exporter.

        Args:
            path_manager: Path manager for constructing paths
            storage_repository: Storage repository for exporting data
            handler_registry: Handler registry class (defaults to global registry)
        """
        self.path_manager = path_manager
        self.storage_repository = storage_repository
        self.handler_registry = handler_registry or RecordHandlerRegistry

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
        for operation in ["create", "delete"]:
            # Get base path for this operation
            base_path = self.path_manager.get_study_user_activity_path(
                operation=operation,  # type: ignore[arg-type]
                record_type="post",  # Use post as base - we'll navigate from here
            )
            # Remove the "post" suffix to get the operation-level path
            base_path = os.path.dirname(base_path)

            # Check if this operation directory exists
            if not os.path.exists(base_path):
                continue

            # Use registry to get handlers and read records
            for record_type in [
                "post",
                "like",
                "follow",
                "like_on_user_post",
                "reply_to_user_post",
            ]:
                try:
                    handler = self.handler_registry.get_handler(record_type)
                    records, filepaths = handler.read_records(base_path)

                    if record_type == "post":
                        all_posts.extend(records)
                    elif record_type == "like":
                        all_likes.extend(records)
                    elif record_type == "follow":
                        all_follows.extend(records)
                    elif record_type == "like_on_user_post":
                        all_likes_on_user_posts.extend(records)
                    elif record_type == "reply_to_user_post":
                        all_replies_to_user_posts.extend(records)

                    all_filepaths.extend(filepaths)
                except KeyError:
                    # Record type not registered
                    continue
                except Exception:
                    # Record type directory doesn't exist or other error
                    continue

        # Export each record type to storage
        if all_posts:
            self._export_dataframe(
                data=all_posts,
                service="study_user_activity",
                record_type="post",
            )

        if all_likes:
            self._export_dataframe(
                data=all_likes,
                service="study_user_activity",
                record_type="like",
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
                record_type="like_on_user_post",
            )

        if all_replies_to_user_posts:
            self._export_dataframe(
                data=all_replies_to_user_posts,
                service="study_user_activity",
                record_type="reply_to_user_post",
            )

        return all_filepaths

    def _export_dataframe(
        self,
        data: list[dict],
        service: str,
        record_type: str | None,
    ) -> None:
        """Helper to export DataFrame to storage."""
        dtypes_map = MAP_SERVICE_TO_METADATA[service]["dtypes_map"]
        df = pd.DataFrame(data)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtypes_map)

        custom_args = {}
        if record_type:
            custom_args["record_type"] = record_type

        self.storage_repository.export_dataframe(
            df=df,
            service=service,
            record_type=record_type,
            custom_args=custom_args if custom_args else None,
        )

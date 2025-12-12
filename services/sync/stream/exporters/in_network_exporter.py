"""Exporter for in-network user activity data."""

import os

from services.sync.stream.exporters.base import BaseActivityExporter
from services.sync.stream.core.protocols import (
    PathManagerProtocol,
    StorageRepositoryProtocol,
    FileUtilitiesProtocol,
)
from services.sync.stream.core.types import Operation, RecordType


class InNetworkUserActivityExporter(BaseActivityExporter):
    """Exporter for in-network user activity data."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        storage_repository: StorageRepositoryProtocol,
        file_utilities: FileUtilitiesProtocol,
    ):
        """Initialize exporter.

        Args:
            path_manager: Path manager for constructing paths
            storage_repository: Storage repository for exporting data
            file_utilities: File utilities for reading cache files
        """
        super().__init__(storage_repository)
        self.path_manager = path_manager
        self.file_utilities = file_utilities

    def export_activity_data(self) -> list[str]:
        """Export all in-network user activity data from cache to storage.

        Only handles POST records with CREATE operation (as per current requirements).

        Returns:
            List of filepaths that were processed
        """
        all_posts: list[dict] = []
        all_filepaths: list[str] = []

        # Get base path for in-network user activity (only CREATE operation)
        # Construct path to the record_type level (where author_did directories are)
        # Note: We pass empty string for author_did to get a path with the author_did
        # component, then use os.path.dirname() to strip it and get the parent directory
        # that contains all author_did subdirectories. This is a workaround since
        # get_in_network_activity_path requires author_did but we need the parent path.
        base_path = self.path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did="",  # Empty string as workaround to get parent directory
        )
        # Remove the empty author_did component to get the operation/record_type level path
        base_path = os.path.dirname(base_path)

        # Check if this directory exists
        if not os.path.exists(base_path):
            return all_filepaths

        # Iterate over all author_did directories (uses FileUtilities for error handling)
        for author_did in self.file_utilities.list_directories(base_path):
            author_path = os.path.join(base_path, author_did)

            # Read all JSON files for this author
            records, filepaths = self.file_utilities.read_all_json_in_directory(
                author_path
            )
            all_posts.extend(records)
            all_filepaths.extend(filepaths)

        # Export posts to storage
        if all_posts:
            self._export_dataframe(
                data=all_posts,
                service="in_network_user_activity",
                record_type=None,
            )

        return all_filepaths

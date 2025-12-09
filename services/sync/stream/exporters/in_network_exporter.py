"""Exporter for in-network user activity data."""

import os

from services.sync.stream.exporters.base import BaseActivityExporter
from services.sync.stream.protocols import (
    PathManagerProtocol,
    StorageRepositoryProtocol,
    FileReaderProtocol,
)
from services.sync.stream.types import Operation, RecordType


class InNetworkUserActivityExporter(BaseActivityExporter):
    """Exporter for in-network user activity data."""

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        storage_repository: StorageRepositoryProtocol,
        file_reader: FileReaderProtocol,
    ):
        """Initialize exporter.

        Args:
            path_manager: Path manager for constructing paths
            storage_repository: Storage repository for exporting data
            file_reader: File reader for reading cache files
        """
        super().__init__(storage_repository)
        self.path_manager = path_manager
        self.file_reader = file_reader

    def export_activity_data(self) -> list[str]:
        """Export all in-network user activity data from cache to storage.

        Only handles POST records with CREATE operation (as per current requirements).

        Returns:
            List of filepaths that were processed
        """
        all_posts: list[dict] = []
        all_filepaths: list[str] = []

        # Get base path for in-network user activity (only CREATE operation)
        base_path = self.path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did="",  # We'll iterate over author_dids
        )
        # Remove the author_did suffix to get the operation/record_type level path
        base_path = os.path.dirname(os.path.dirname(base_path))

        # Check if this directory exists
        if not os.path.exists(base_path):
            return all_filepaths

        # Iterate over all author_did directories
        for author_did in os.listdir(base_path):
            author_path = os.path.join(base_path, author_did)
            if not os.path.isdir(author_path):
                continue

            # Read all JSON files for this author
            records, filepaths = self.file_reader.read_all_json_in_directory(
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

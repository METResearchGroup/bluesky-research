"""Exporter for in-network user activity data."""

import os
import pandas as pd

from services.sync.stream.protocols import (
    PathManagerProtocol,
    StorageRepositoryProtocol,
    FileReaderProtocol,
)
from services.sync.stream.types import Operation, RecordType
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.constants import timestamp_format


class InNetworkUserActivityExporter:
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
        self.path_manager = path_manager
        self.storage_repository = storage_repository
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
            self._export_dataframe(data=all_posts)

        return all_filepaths

    def _export_dataframe(self, data: list[dict]) -> None:
        """Helper to export DataFrame to storage."""
        service = "in_network_user_activity"
        dtypes_map = MAP_SERVICE_TO_METADATA[service]["dtypes_map"]
        df = pd.DataFrame(data)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtypes_map)

        # In-network activity doesn't use record_type in custom_args
        self.storage_repository.export_dataframe(
            df=df,
            service=service,
            record_type=None,
            custom_args=None,
        )

"""Base exporter with shared functionality."""

import pandas as pd
from abc import ABC, abstractmethod

from services.sync.stream.core.protocols import StorageRepositoryProtocol
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.constants import timestamp_format


class BaseActivityExporter(ABC):
    """Base class for activity exporters with shared logic.

    Provides common DataFrame conversion and export functionality.
    Subclasses implement record type-specific iteration logic.
    """

    def __init__(self, storage_repository: StorageRepositoryProtocol):
        """Initialize base exporter.

        Args:
            storage_repository: Storage repository for exporting data
        """
        self.storage_repository = storage_repository

    @abstractmethod
    def export_activity_data(self) -> list[str]:
        """Export all activity data from cache to storage.

        Returns:
            List of filepaths that were processed
        """
        pass

    def _export_dataframe(
        self,
        data: list[dict],
        service: str,
        record_type: str | None = None,
    ) -> None:
        """Helper to export DataFrame to storage.

        Args:
            data: List of record dictionaries
            service: Service name for metadata
            record_type: Optional record type for custom args
        """
        if not data:
            return

        dtypes_map = MAP_SERVICE_TO_METADATA.get(service, {}).get("dtypes_map", {})
        df = pd.DataFrame(data)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        # Only apply dtypes_map for columns that exist in the DataFrame
        if dtypes_map:
            existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
            if existing_cols:
                df = df.astype(existing_cols)

        custom_args = {}
        if record_type:
            custom_args["record_type"] = record_type

        self.storage_repository.export_dataframe(
            df=df,
            service=service,
            record_type=record_type,
            custom_args=custom_args if custom_args else None,
        )

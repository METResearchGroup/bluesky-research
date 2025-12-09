"""Storage repository implementation using adapter pattern."""

import pandas as pd

from services.sync.stream.protocols import (
    StorageAdapterProtocol,
)
from services.sync.stream.types import Operation, GenericRecordType


class StorageRepository:
    """Repository for data storage operations.

    Uses adapter pattern to abstract over different storage backends.
    """

    def __init__(self, adapter: StorageAdapterProtocol):
        """Initialize repository with storage adapter.

        Args:
            adapter: Concrete storage adapter (S3, Local, etc.)
        """
        self.adapter = adapter

    def export_dataframe(
        self,
        df: pd.DataFrame,
        service: str,
        record_type: str | None = None,
        custom_args: dict | None = None,
    ) -> None:
        """Export DataFrame using adapter."""
        self.adapter.write_dataframe(
            df=df,
            key="",  # Adapter will determine key from service
            service=service,
            custom_args=custom_args or {},
        )

    def export_jsonl_batch(
        self,
        directory: str,
        operation: Operation,
        record_type: GenericRecordType,
        compressed: bool = True,
    ) -> list[dict]:
        """Export JSONL batch using adapter."""
        return self.adapter.write_dicts_from_directory(
            directory=directory,
            key="",  # Adapter will determine key
            compressed=compressed,
        )

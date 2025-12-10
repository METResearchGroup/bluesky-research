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
        """Export DataFrame using adapter.

        Args:
            df: DataFrame to export
            service: Service name for routing/partitioning
            record_type: Optional record type (passed to custom_args for adapter use)
            custom_args: Additional arguments for the adapter
        """
        # Merge record_type into custom_args if provided, so adapter can use it
        merged_custom_args = custom_args.copy() if custom_args else {}
        if record_type is not None:
            merged_custom_args["record_type"] = record_type

        self.adapter.write_dataframe(
            df=df,
            key="",  # Adapter will determine key from service
            service=service,
            custom_args=merged_custom_args,
        )

    def export_jsonl_batch(
        self,
        directory: str,
        operation: Operation,
        record_type: GenericRecordType,
        compressed: bool = True,
    ) -> list[dict]:
        """Export JSONL batch using adapter.

        Args:
            directory: Directory containing JSON files to export
            operation: Operation type (passed for potential routing/partitioning)
            record_type: Record type (passed for potential routing/partitioning)
            compressed: Whether to compress the output

        Note:
            operation and record_type are currently not used by adapters but are
            kept in the API for future routing/partitioning support. They may be
            used by adapters via custom_args in the future.
        """
        return self.adapter.write_dicts_from_directory(
            directory=directory,
            key="",  # Adapter will determine key
            compressed=compressed,
        )

"""Concrete storage adapter implementations.

We introduce the adapter pattern here so that we can abstract storage operations
and make it backend-agnostic.
"""

import os
import pandas as pd

from services.sync.stream.protocols import PathManagerProtocol
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    write_jsons_to_local_store,
    export_data_to_local_storage,
)


class LocalStorageAdapter:
    """Local filesystem storage adapter implementation."""

    def __init__(self, path_manager: PathManagerProtocol):
        """Initialize local storage adapter.

        Args:
            path_manager: Path manager for constructing local paths
        """
        self.path_manager = path_manager

    def write_dataframe(
        self,
        df: pd.DataFrame,
        key: str,
        service: str,
        custom_args: dict | None = None,
    ) -> None:
        """Write DataFrame to local storage."""
        export_data_to_local_storage(
            df=df,
            service=service,
            custom_args=custom_args or {},
        )

    def write_jsonl(
        self,
        data: list[dict],
        key: str,
        compressed: bool = False,  # Local usually uncompressed
    ) -> None:
        """Write JSONL to local filesystem."""
        # Implementation for local JSONL writing
        full_path = os.path.join(root_local_data_directory, key)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        # ... write JSONL logic

    def write_dicts_from_directory(
        self,
        directory: str,
        key: str,
        compressed: bool = False,
    ) -> list[dict]:
        """Write JSON files from directory to local storage."""
        full_path = os.path.join(root_local_data_directory, key)
        result = write_jsons_to_local_store(
            source_directory=directory,
            export_filepath=full_path,
            compressed=compressed,
        )
        # write_jsons_to_local_store can return None, ensure we return a list
        return result if result is not None else []

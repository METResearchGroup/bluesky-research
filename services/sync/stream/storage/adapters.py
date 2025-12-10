"""Concrete storage adapter implementations.

We introduce the adapter pattern here so that we can abstract storage operations
and make it backend-agnostic.
"""

import gzip
import json
import os
import tempfile
import pandas as pd

from lib.log.logger import get_logger

from services.sync.stream.protocols import PathManagerProtocol
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    write_jsons_to_local_store,
    export_data_to_local_storage,
)

logger = get_logger(__file__)


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
        """Write JSONL to local filesystem with atomic writes.

        Args:
            data: List of dictionaries to write as JSONL
            key: Relative path/key for the output file
            compressed: Whether to compress the output with gzip

        Raises:
            OSError: If file operations fail
            IOError: If I/O operations fail
        """
        full_path = os.path.join(root_local_data_directory, key)
        target_dir = os.path.dirname(full_path)
        os.makedirs(target_dir, exist_ok=True)

        # Add .gz extension if compressed
        if compressed:
            full_path += ".gz"

        # Create temp file in target directory for atomic write
        temp_fd, temp_path = tempfile.mkstemp(
            dir=target_dir, prefix=os.path.basename(full_path) + ".tmp.", suffix=""
        )

        temp_fd_closed = False
        try:
            # Write data to temp file
            if compressed:
                # Close the file descriptor, we'll use the path with gzip.open
                os.close(temp_fd)
                temp_fd_closed = True
                with gzip.open(temp_path, "wt", encoding="utf-8") as gz_file:
                    for item in data:
                        gz_file.write(json.dumps(item, ensure_ascii=False) + "\n")
                    gz_file.flush()
                # Open the file again to fsync (gzip doesn't expose fd directly)
                with open(temp_path, "rb") as f:
                    os.fsync(f.fileno())
            else:
                with os.fdopen(temp_fd, "w", encoding="utf-8") as temp_file:
                    temp_fd_closed = True  # fdopen closes the fd when context exits
                    for item in data:
                        temp_file.write(json.dumps(item, ensure_ascii=False) + "\n")
                    temp_file.flush()
                    # fsync the file
                    os.fsync(temp_file.fileno())

            # Atomically rename temp file to final path
            os.rename(temp_path, full_path)

        except (OSError, IOError) as e:
            # Clean up temp file on error
            try:
                if not temp_fd_closed:
                    os.close(temp_fd)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass  # Ignore cleanup errors

            logger.error(
                f"Failed to write JSONL to {full_path}: {type(e).__name__}: {str(e)}",
                context={
                    "filepath": full_path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "compressed": compressed,
                    "record_count": len(data),
                },
            )
            raise
        except Exception as e:
            # Clean up temp file on unexpected error
            try:
                if not temp_fd_closed:
                    os.close(temp_fd)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass  # Ignore cleanup errors

            logger.error(
                f"Unexpected error writing JSONL to {full_path}: {type(e).__name__}: {str(e)}",
                context={
                    "filepath": full_path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "compressed": compressed,
                    "record_count": len(data),
                },
            )
            raise

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

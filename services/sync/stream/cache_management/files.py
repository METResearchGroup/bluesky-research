"""Generic file I/O utilities for reading, writing, and managing files."""

import os
import json

from lib.log.logger import get_logger

from services.sync.stream.protocols import DirectoryManagerProtocol

logger = get_logger(__file__)


class FileUtilities:
    """Generic file I/O utilities for reading, writing, and managing files.

    This class consolidates file operations (read, write, delete, list) into
    a single interface. It is not cache-specific despite being used in cache contexts.
    """

    def __init__(self, directory_manager: DirectoryManagerProtocol):
        """Initialize file utilities.

        Args:
            directory_manager: Manager for ensuring directories exist
        """
        self.directory_manager = directory_manager

    # Write operations
    def write_json(self, path: str, data: dict) -> None:
        """Write JSON data to file at path."""
        try:
            parent_dir = os.path.dirname(path)
            self.directory_manager.ensure_directory_exists(parent_dir)
            with open(path, "w") as f:
                json.dump(data, f)
        except (OSError, IOError, PermissionError) as e:
            logger.error(
                f"Failed to write JSON file: {path}",
                context={
                    "path": path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

    def write_jsonl(self, path: str, records: list[dict]) -> None:
        """Write JSONL data to file at path."""
        try:
            parent_dir = os.path.dirname(path)
            self.directory_manager.ensure_directory_exists(parent_dir)
            with open(path, "w") as f:
                for record in records:
                    f.write(json.dumps(record) + "\n")
        except (OSError, IOError, PermissionError) as e:
            logger.error(
                f"Failed to write JSONL file: {path}",
                context={
                    "path": path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "record_count": len(records),
                },
            )
            raise

    # Read operations
    def read_json(self, path: str) -> dict:
        """Read JSON file from path."""
        with open(path, "r") as f:
            return json.load(f)

    def read_all_json_in_directory(
        self, directory: str
    ) -> tuple[list[dict], list[str]]:
        """Read all JSON files in directory, returning (data, filepaths)."""
        records: list[dict] = []
        filepaths: list[str] = []

        if not os.path.exists(directory):
            return records, filepaths

        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath) and filename.endswith(".json"):
                try:
                    data = self.read_json(filepath)
                    records.append(data)
                    filepaths.append(filepath)
                except (
                    json.JSONDecodeError,
                    OSError,
                    IOError,
                    FileNotFoundError,
                    PermissionError,
                ) as e:
                    logger.warning(
                        f"Failed to read JSON file: {filepath}. Error: {type(e).__name__}: {str(e)}",
                        context={
                            "filepath": filepath,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    continue

        return records, filepaths

    # Directory operations
    def list_files(self, directory: str) -> list[str]:
        """List all files in directory."""
        if not os.path.exists(directory):
            return []
        try:
            return [
                f
                for f in os.listdir(directory)
                if os.path.isfile(os.path.join(directory, f))
            ]
        except (OSError, PermissionError) as e:
            logger.warning(
                f"Failed to list files in directory: {directory}. Error: {type(e).__name__}: {str(e)}",
                context={
                    "directory": directory,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return []

    def list_directories(self, directory: str) -> list[str]:
        """List all subdirectories in directory.

        Args:
            directory: Directory to list subdirectories from

        Returns:
            List of subdirectory names (not full paths)
        """
        if not os.path.exists(directory):
            return []
        try:
            return [
                d
                for d in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, d))
            ]
        except (OSError, PermissionError) as e:
            logger.warning(
                f"Failed to list directories in: {directory}. Error: {type(e).__name__}: {str(e)}",
                context={
                    "directory": directory,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return []

    def is_directory(self, path: str) -> bool:
        """Check if path is a directory.

        Args:
            path: Path to check

        Returns:
            True if path exists and is a directory, False otherwise
        """
        return os.path.isdir(path) if os.path.exists(path) else False

    # Delete operations
    def delete_files(self, filepaths: list[str]) -> None:
        """Delete list of files.

        Args:
            filepaths: List of file paths to delete
        """
        for filepath in filepaths:
            if os.path.exists(filepath) and os.path.isfile(filepath):
                try:
                    os.remove(filepath)
                except (OSError, PermissionError) as e:
                    logger.error(
                        f"Failed to delete file: {filepath}. Error: {type(e).__name__}: {str(e)}",
                        context={
                            "filepath": filepath,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    # Continue to next file instead of stopping
                    continue

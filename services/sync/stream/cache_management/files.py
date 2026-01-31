"""Generic file I/O utilities for reading, writing, and managing files."""

import os
import json
import atexit
import time
from typing import Any

from lib.log.logger import get_logger

from services.sync.stream.core.protocols import DirectoryManagerProtocol

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
        # Cache-write batching (used by the firehose stream).
        #
        # We buffer records per target directory and periodically flush them into a
        # single JSON file containing a list[dict]. This dramatically reduces the
        # number of filesystem writes versus "1 record -> 1 file".
        #
        # These are intentionally environment-driven so they can be tuned without
        # code changes.
        self.batch_size = max(1, int(os.getenv("SYNC_STREAM_CACHE_BATCH_SIZE", "1000")))
        self.batch_flush_interval_seconds = max(
            0, int(os.getenv("SYNC_STREAM_CACHE_BATCH_FLUSH_SECONDS", "5"))
        )
        self._batch_buffers: dict[str, list[dict]] = {}
        self._batch_first_seen_ts: dict[str, float] = {}
        self._batch_file_counter = 0

        # Best-effort flush on interpreter exit so we don't drop tail batches.
        atexit.register(self.flush_batches)

    # Write operations
    def write_json(self, path: str, data: Any) -> None:
        """Write JSON-serializable data to file at path.

        Note:
            While historically used for single-record dict writes, this method is
            also used for batched writes where `data` may be a list[dict].
        """
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

    def append_record_to_batch(self, directory: str, record: dict) -> None:
        """Buffer a record and flush to disk in batches.

        This writes files shaped like:
            batch_<epoch_ms>_<counter>.json  (content: list[dict])

        Args:
            directory: Target directory to write batch files into
            record: Record dict to buffer
        """
        # Fast-path: batching disabled
        if self.batch_size <= 1:
            filename = f"record_{int(time.time() * 1000)}_{self._batch_file_counter}.json"
            self._batch_file_counter += 1
            self.write_json(os.path.join(directory, filename), record)
            return

        buf = self._batch_buffers.get(directory)
        if buf is None:
            buf = []
            self._batch_buffers[directory] = buf
            self._batch_first_seen_ts[directory] = time.time()

        buf.append(record)

        # Flush on size
        if len(buf) >= self.batch_size:
            self._flush_directory_batch(directory)
            return

        # Flush on age (best-effort to reduce data loss on crash and avoid
        # unbounded tail buffers on low-volume directories)
        if self.batch_flush_interval_seconds > 0:
            first_ts = self._batch_first_seen_ts.get(directory)
            if first_ts is not None and (time.time() - first_ts) >= float(
                self.batch_flush_interval_seconds
            ):
                self._flush_directory_batch(directory)

    def flush_batches(self) -> None:
        """Flush any buffered batches to disk (best-effort)."""
        # Copy keys to avoid dict-size-change while flushing.
        for directory in list(self._batch_buffers.keys()):
            try:
                self._flush_directory_batch(directory)
            except Exception as e:
                logger.error(
                    f"Failed to flush batch directory: {directory}",
                    context={
                        "directory": directory,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )

    def _flush_directory_batch(self, directory: str) -> None:
        buf = self._batch_buffers.get(directory)
        if not buf:
            # Ensure metadata cleared as well
            self._batch_buffers.pop(directory, None)
            self._batch_first_seen_ts.pop(directory, None)
            return

        # Make a stable snapshot and clear buffer before writing, so the stream
        # can continue buffering even if the write is slow.
        records = list(buf)
        self._batch_buffers[directory] = []
        self._batch_first_seen_ts[directory] = time.time()

        filename = f"batch_{int(time.time() * 1000)}_{self._batch_file_counter}.json"
        self._batch_file_counter += 1
        path = os.path.join(directory, filename)

        self.write_json(path, records)

    # Read operations
    def read_json(self, path: str) -> dict:
        """Read JSON file from path."""
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (
            json.JSONDecodeError,
            OSError,
            IOError,
            FileNotFoundError,
            PermissionError,
        ) as e:
            logger.error(
                f"Failed to read JSON file: {path}. Error: {type(e).__name__}: {str(e)}",
                context={
                    "path": path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

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
                    # Support both "1 record per file" (dict) and
                    # "many records per file" (list[dict]).
                    if isinstance(data, list):
                        # Be defensive: only extend with dict-like objects.
                        records.extend([d for d in data if isinstance(d, dict)])
                    elif isinstance(data, dict):
                        records.append(data)
                    else:
                        logger.warning(
                            f"Unexpected JSON type in file: {filepath}",
                            context={
                                "filepath": filepath,
                                "json_type": type(data).__name__,
                            },
                        )
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

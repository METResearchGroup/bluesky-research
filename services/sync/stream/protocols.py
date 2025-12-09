"""Protocol definitions for sync export dependencies.

All major dependencies are defined as protocols to enable:
- Dependency injection
- Easy testing with mocks
- Type safety
"""

from typing import Protocol, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


# ============================================================================
# Path Management Protocols
# ============================================================================


class PathManagerProtocol(Protocol):
    """Protocol for path construction and management."""

    def get_local_cache_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal["post", "like", "follow"],
    ) -> str:
        """Get local cache path for general firehose records."""
        ...

    def get_study_user_activity_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal[
            "post", "like", "follow", "like_on_user_post", "reply_to_user_post"
        ],
        follow_status: Literal["follower", "followee"] | None = None,
    ) -> str:
        """Get path for study user activity records."""
        ...

    def get_in_network_activity_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal["post"],
        author_did: str,
    ) -> str:
        """Get path for in-network user activity records."""
        ...

    def get_relative_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal[
            "post", "like", "follow", "like_on_user_post", "reply_to_user_post"
        ],
        follow_status: Literal["follower", "followee"] | None = None,
    ) -> str:
        """Get relative path component (without root)."""
        ...


class DirectoryManagerProtocol(Protocol):
    """Protocol for directory lifecycle management."""

    def ensure_exists(self, path: str) -> None:
        """Ensure directory exists, creating if necessary."""
        ...

    def rebuild_all(self) -> None:
        """Rebuild all cache directory structures."""
        ...

    def delete_all(self) -> None:
        """Delete all cache directories."""
        ...

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        ...


# ============================================================================
# File I/O Protocols
# ============================================================================


class FileWriterProtocol(Protocol):
    """Protocol for writing files to cache."""

    def write_json(self, path: str, data: dict) -> None:
        """Write JSON data to file at path."""
        ...

    def write_jsonl(self, path: str, records: list[dict]) -> None:
        """Write JSONL data to file at path."""
        ...


class FileReaderProtocol(Protocol):
    """Protocol for reading files from cache."""

    def read_json(self, path: str) -> dict:
        """Read JSON file from path."""
        ...

    def list_files(self, directory: str) -> list[str]:
        """List all files in directory."""
        ...

    def read_all_json_in_directory(
        self, directory: str
    ) -> tuple[list[dict], list[str]]:
        """Read all JSON files in directory, returning (data, filepaths)."""
        ...


# ============================================================================
# Storage Repository & Adapter Protocols
# ============================================================================


class StorageAdapterProtocol(Protocol):
    """Protocol for concrete storage implementations (S3, Local, etc.).

    This is the adapter interface that concrete storage backends implement.
    """

    def write_dataframe(
        self,
        df: "pd.DataFrame",
        key: str,
        service: str,
        custom_args: dict | None = None,
    ) -> None:
        """Write DataFrame to storage.

        Args:
            df: DataFrame to write
            key: Storage key/path
            service: Service name for metadata
            custom_args: Optional custom arguments
        """
        ...

    def write_jsonl(
        self,
        data: list[dict],
        key: str,
        compressed: bool = True,
    ) -> None:
        """Write JSONL data to storage.

        Args:
            data: List of dicts to write as JSONL
            key: Storage key/path
            compressed: Whether to compress the output
        """
        ...

    def write_dicts_from_directory(
        self,
        directory: str,
        key: str,
        compressed: bool = True,
    ) -> list[dict]:
        """Write all JSON files from directory to storage.

        Args:
            directory: Local directory containing JSON files
            key: Storage key/path
            compressed: Whether to compress the output

        Returns:
            List of dicts that were written
        """
        ...


class StorageRepositoryProtocol(Protocol):
    """Protocol for storage repository (uses adapters).

    This is the repository interface that abstracts over different storage backends.
    The repository uses adapters to perform actual storage operations.
    """

    def export_dataframe(
        self,
        df: "pd.DataFrame",
        service: str,
        record_type: str | None = None,
        custom_args: dict | None = None,
    ) -> None:
        """Export DataFrame to storage.

        Args:
            df: DataFrame to export
            service: Service name (e.g., "study_user_activity")
            record_type: Optional record type for custom routing
            custom_args: Optional custom arguments
        """
        ...

    def export_jsonl_batch(
        self,
        directory: str,
        operation: Literal["create", "delete"],
        record_type: Literal["post", "like", "follow"],
        compressed: bool = True,
    ) -> list[dict]:
        """Export JSONL batch from directory.

        Args:
            directory: Local directory containing JSON files
            operation: Operation type (create/delete)
            record_type: Record type (post/like/follow)
            compressed: Whether to compress output

        Returns:
            List of dicts that were exported
        """
        ...


# ============================================================================
# Record Handler Protocol (Strategy Pattern)
# ============================================================================


class RecordHandlerProtocol(Protocol):
    """Protocol for record type handlers (strategy pattern).

    Each record type (post, like, follow, etc.) has a handler that knows
    how to write and read that specific record type.
    """

    def write_record(
        self,
        record: dict,
        operation: Literal["create", "delete"],
        author_did: str,
        filename: str,
        **kwargs,
    ) -> None:
        """Write a single record to cache.

        Args:
            record: Record data to write
            operation: Operation type (create/delete)
            author_did: Author DID
            filename: Filename for the record
            **kwargs: Additional record-type-specific arguments
        """
        ...

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all records of this type from cache.

        Args:
            base_path: Base path to read from

        Returns:
            Tuple of (records, filepaths)
        """
        ...

    def get_record_type(self) -> str:
        """Get the record type this handler manages."""
        ...


# ============================================================================
# Exporter Protocol
# ============================================================================


class ActivityExporterProtocol(Protocol):
    """Protocol for activity exporters (study user, in-network, etc.)."""

    def export_activity_data(self) -> list[str]:
        """Export all activity data from cache to storage.

        Returns:
            List of filepaths that were processed
        """
        ...

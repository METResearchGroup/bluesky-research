"""Configuration for generic record handlers."""

from dataclasses import dataclass
from typing import Optional, Protocol

from services.sync.stream.types import RecordType, Operation, FollowStatus
from services.sync.stream.protocols import PathManagerProtocol, FileUtilitiesProtocol


class PathStrategyProtocol(Protocol):
    """Protocol for path construction strategies."""

    def __call__(
        self,
        path_manager: PathManagerProtocol,
        operation: Operation,
        record_type: RecordType,
        *,
        follow_status: FollowStatus | None = None,
        author_did: str | None = None,
    ) -> str:
        """Construct base path for a record.

        Args:
            path_manager: Path manager instance
            operation: Operation type (create/delete)
            record_type: Type of record
            follow_status: Optional follow status (for follow records)
            author_did: Optional author DID (for in-network records)

        Returns:
            Base path string
        """
        ...


class NestedPathExtractorProtocol(Protocol):
    """Protocol for extracting nested path components from records."""

    def __call__(self, record: dict) -> str:
        """Extract nested path component from record.

        Args:
            record: Record dictionary

        Returns:
            Nested path component (e.g., post URI suffix)
        """
        ...


class ReadStrategyProtocol(Protocol):
    """Protocol for reading records from cache."""

    def __call__(
        self,
        file_reader: FileUtilitiesProtocol,
        base_path: str,
        record_type: RecordType,
    ) -> tuple[list[dict], list[str]]:
        """Read all records from cache.

        Args:
            file_reader: File reader instance
            base_path: Base path to read from
            record_type: Type of records to read

        Returns:
            Tuple of (records, filepaths)
        """
        ...


@dataclass
class HandlerConfig:
    """Configuration for generic record handler.

    Attributes:
        record_type: Type of record this handler manages
        path_strategy: Function that constructs the base path for writing
        nested_path_extractor: Optional function to extract nested path component
            from record dict. Used for records nested by post_uri_suffix, etc.
        requires_follow_status: Whether this handler requires follow_status parameter
        read_strategy: Optional custom read strategy. If None, uses default.
    """

    record_type: RecordType
    path_strategy: PathStrategyProtocol
    nested_path_extractor: Optional[NestedPathExtractorProtocol] = None
    requires_follow_status: bool = False
    read_strategy: Optional[ReadStrategyProtocol] = None

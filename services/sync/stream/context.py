"""Context objects for sync export system components."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    FileUtilities,
)

if TYPE_CHECKING:
    from services.sync.stream.handlers.registry import RecordHandlerRegistry
    from services.sync.stream.exporters.study_user_exporter import (
        StudyUserActivityExporter,
    )
    from services.sync.stream.exporters.in_network_exporter import (
        InNetworkUserActivityExporter,
    )
    from services.sync.stream.storage.repository import StorageRepository


@dataclass
class CacheWriteContext:
    """Context for real-time cache write operations.

    Used during the firehose stream processing phase to write records
    to the local JSON cache.

    Components:
        - path_manager: Constructs cache file paths
        - directory_manager: Manages directory lifecycle
        - file_utilities: Handles file I/O operations
        - handler_registry: Registry for record type handlers
        - study_user_manager: Manages study user identification
    """

    path_manager: CachePathManager
    directory_manager: CacheDirectoryManager
    file_utilities: FileUtilities
    handler_registry: "RecordHandlerRegistry"
    study_user_manager: "Any"  # StudyUserManager type


@dataclass
class BatchExportContext:
    """Context for batch export operations.

    Used during the batch export phase to read from cache and export
    to persistent storage.

    Components:
        - path_manager: Constructs cache file paths
        - directory_manager: Manages directory lifecycle and cleanup
        - file_utilities: Handles file I/O operations
        - storage_repository: Exports data to persistent storage
        - study_user_exporter: Exports study user activity data
        - in_network_exporter: Exports in-network user activity data
    """

    path_manager: CachePathManager
    directory_manager: CacheDirectoryManager
    file_utilities: FileUtilities
    storage_repository: "StorageRepository"
    study_user_exporter: "StudyUserActivityExporter"
    in_network_exporter: "InNetworkUserActivityExporter"

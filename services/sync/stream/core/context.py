"""Context objects for sync export system components."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.sync.stream.handlers.registry import RecordHandlerRegistry
    from services.sync.stream.record_processors.registry import ProcessorRegistry
    from services.sync.stream.exporters.study_user_exporter import (
        StudyUserActivityExporter,
    )
    from services.sync.stream.exporters.in_network_exporter import (
        InNetworkUserActivityExporter,
    )
    from services.sync.stream.storage.repository import StorageRepository
    from services.sync.stream.protocols import (
        PathManagerProtocol,
        DirectoryManagerProtocol,
        FileUtilitiesProtocol,
    )


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
        - processor_registry: Registry for record type processors
        - study_user_manager: Manages study user identification
    """

    path_manager: "PathManagerProtocol"
    directory_manager: "DirectoryManagerProtocol"
    file_utilities: "FileUtilitiesProtocol"
    handler_registry: "RecordHandlerRegistry"
    processor_registry: "ProcessorRegistry"
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

    path_manager: "PathManagerProtocol"
    directory_manager: "DirectoryManagerProtocol"
    file_utilities: "FileUtilitiesProtocol"
    storage_repository: "StorageRepository"
    study_user_exporter: "StudyUserActivityExporter"
    in_network_exporter: "InNetworkUserActivityExporter"

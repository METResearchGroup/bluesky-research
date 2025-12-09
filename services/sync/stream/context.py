"""Context object for sync export system components."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)

if TYPE_CHECKING:
    from services.sync.stream.handlers.registry import RecordHandlerRegistry
    from services.sync.stream.exporters.study_user_exporter import (
        StudyUserActivityExporter,
    )
    from services.sync.stream.storage.repository import StorageRepository


@dataclass
class SyncExportContext:
    """Context object containing all sync export system components.

    This replaces global state and enables explicit dependency injection.
    """

    path_manager: CachePathManager
    directory_manager: CacheDirectoryManager
    file_writer: CacheFileWriter
    file_reader: CacheFileReader
    handler_registry: "RecordHandlerRegistry"
    study_user_exporter: "StudyUserActivityExporter"
    storage_repository: "StorageRepository"
    study_user_manager: "Any"  # StudyUserManager type

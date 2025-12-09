"""Setup function to wire all dependencies together.

This creates a complete, configured system with all dependencies injected.
"""

from typing import TypedDict

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.handlers.factories import (
    create_study_user_post_handler,
    create_study_user_like_handler,
    create_study_user_follow_handler,
    create_study_user_like_on_user_post_handler,
    create_study_user_reply_to_user_post_handler,
)
from services.sync.stream.exporters.study_user_exporter import StudyUserActivityExporter
from services.sync.stream.exporters.in_network_exporter import (
    InNetworkUserActivityExporter,
)
from services.sync.stream.batch_exporter import BatchExporter
from services.sync.stream.storage.repository import StorageRepository
from services.sync.stream.storage.adapters import LocalStorageAdapter


class SyncExportSystemComponents(TypedDict):
    """Typed dictionary for sync export system components."""

    path_manager: CachePathManager
    directory_manager: CacheDirectoryManager
    file_writer: CacheFileWriter
    file_reader: CacheFileReader
    handler_registry: type[RecordHandlerRegistry]
    study_user_exporter: StudyUserActivityExporter
    storage_repository: StorageRepository


def setup_sync_export_system() -> SyncExportSystemComponents:
    """Set up the complete sync export system with all dependencies wired.

    Returns:
        TypedDict containing all system components
    """
    # 1. Create path manager
    path_manager = CachePathManager()

    # 2. Create directory manager
    directory_manager = CacheDirectoryManager(path_manager=path_manager)

    # 3. Create file writer and reader
    file_writer = CacheFileWriter(directory_manager=directory_manager)
    file_reader = CacheFileReader()

    # 4. Create storage adapter and repository
    storage_adapter = LocalStorageAdapter(path_manager=path_manager)

    storage_repository = StorageRepository(adapter=storage_adapter)

    # 5. Register handlers with factories (dependency injection)
    # handler_registry = RecordHandlerRegistry

    # Create factory functions that capture the dependencies
    def make_post_handler():
        return create_study_user_post_handler(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
        )

    def make_like_handler():
        return create_study_user_like_handler(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
        )

    def make_follow_handler():
        return create_study_user_follow_handler(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
        )

    def make_like_on_user_post_handler():
        return create_study_user_like_on_user_post_handler(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
        )

    def make_reply_to_user_post_handler():
        return create_study_user_reply_to_user_post_handler(
            path_manager=path_manager,
            file_writer=file_writer,
            file_reader=file_reader,
        )

    # Register all handlers
    from services.sync.stream.types import RecordType

    RecordHandlerRegistry.register_factory(RecordType.POST.value, make_post_handler)
    RecordHandlerRegistry.register_factory(RecordType.LIKE.value, make_like_handler)
    RecordHandlerRegistry.register_factory(RecordType.FOLLOW.value, make_follow_handler)
    RecordHandlerRegistry.register_factory(
        RecordType.LIKE_ON_USER_POST.value, make_like_on_user_post_handler
    )
    RecordHandlerRegistry.register_factory(
        RecordType.REPLY_TO_USER_POST.value, make_reply_to_user_post_handler
    )

    # 6. Create exporter
    exporter = StudyUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        handler_registry=RecordHandlerRegistry,  # Pass the class, exporter will use it
    )

    return SyncExportSystemComponents(
        path_manager=path_manager,
        directory_manager=directory_manager,
        file_writer=file_writer,
        file_reader=file_reader,
        handler_registry=RecordHandlerRegistry,  # Return the class
        study_user_exporter=exporter,
        storage_repository=storage_repository,
    )


def setup_batch_export_system() -> BatchExporter:
    """Set up batch export system with all dependencies.

    Returns:
        Configured BatchExporter instance
    """
    # Get components from sync export system
    components = setup_sync_export_system()

    # Create in-network exporter
    in_network_exporter = InNetworkUserActivityExporter(
        path_manager=components["path_manager"],
        storage_repository=components["storage_repository"],
        file_reader=components["file_reader"],
    )

    # Create batch exporter
    batch_exporter = BatchExporter(
        study_user_exporter=components["study_user_exporter"],
        in_network_exporter=in_network_exporter,
        directory_manager=components["directory_manager"],
        clear_filepaths=False,  # Default, can be overridden
        clear_cache=True,  # Default, can be overridden
    )

    return batch_exporter

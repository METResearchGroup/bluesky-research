"""Setup function to wire all dependencies together.

This creates a complete, configured system with all dependencies injected.
"""

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)
from services.sync.stream.context import SyncExportContext
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.handlers.factories import create_handlers_for_all_types
from services.participant_data.study_users import get_study_user_manager
from services.sync.stream.exporters.study_user_exporter import StudyUserActivityExporter
from services.sync.stream.exporters.in_network_exporter import (
    InNetworkUserActivityExporter,
)
from services.sync.stream.batch_exporter import BatchExporter
from services.sync.stream.storage.repository import StorageRepository
from services.sync.stream.storage.adapters import LocalStorageAdapter


def setup_sync_export_system() -> SyncExportContext:
    """Set up the complete sync export system with all dependencies wired.

    Returns:
        SyncExportContext containing all system components
    """
    # 1. Create path manager
    path_manager = CachePathManager()

    # 2. Create directory manager
    directory_manager = CacheDirectoryManager(path_manager=path_manager)

    # 3. Create file writer and reader
    file_writer = CacheFileWriter(directory_manager=directory_manager)
    file_reader = CacheFileReader()

    # 4. Create storage adapter and repository
    storage_adapter = LocalStorageAdapter()

    storage_repository = StorageRepository(adapter=storage_adapter)

    # 5. Create study user manager
    study_user_manager = get_study_user_manager(load_from_aws=False)

    # 6. Create handler registry and register all handlers
    handler_registry = RecordHandlerRegistry()
    handlers = create_handlers_for_all_types(
        path_manager=path_manager,
        file_writer=file_writer,
        file_reader=file_reader,
    )
    # Register all handlers
    for handler_key, handler in handlers.items():
        handler_registry.register_handler(handler_key, handler)

    # 7. Create exporter
    exporter = StudyUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        handler_registry=handler_registry,
    )

    return SyncExportContext(
        path_manager=path_manager,
        directory_manager=directory_manager,
        file_writer=file_writer,
        file_reader=file_reader,
        handler_registry=handler_registry,
        study_user_exporter=exporter,
        storage_repository=storage_repository,
        study_user_manager=study_user_manager,
    )


def setup_batch_export_system() -> BatchExporter:
    """Set up batch export system with all dependencies.

    Returns:
        Configured BatchExporter instance
    """
    # Get components from sync export system
    context = setup_sync_export_system()

    # Create in-network exporter
    in_network_exporter = InNetworkUserActivityExporter(
        path_manager=context.path_manager,
        storage_repository=context.storage_repository,
        file_reader=context.file_reader,
    )

    # Create batch exporter
    batch_exporter = BatchExporter(
        study_user_exporter=context.study_user_exporter,
        in_network_exporter=in_network_exporter,
        directory_manager=context.directory_manager,
        clear_filepaths=False,  # Default, can be overridden
        clear_cache=True,  # Default, can be overridden
    )

    return batch_exporter

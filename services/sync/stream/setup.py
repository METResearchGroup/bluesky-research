"""Setup functions to wire dependencies for cache write and batch export."""

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    FileUtilities,
)
from services.sync.stream.context import CacheWriteContext, BatchExportContext
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


def _create_shared_infrastructure():
    """Create shared infrastructure components used by both phases.

    Returns:
        Tuple of (path_manager, directory_manager, file_utilities)
    """
    path_manager = CachePathManager()
    directory_manager = CacheDirectoryManager(path_manager=path_manager)
    file_utilities = FileUtilities(directory_manager=directory_manager)
    return path_manager, directory_manager, file_utilities


def setup_cache_write_system() -> CacheWriteContext:
    """Set up the cache write system for real-time firehose processing.

    Returns:
        CacheWriteContext containing all components needed for cache writes
    """
    # Create shared infrastructure
    path_manager, directory_manager, file_utilities = _create_shared_infrastructure()

    # Create study user manager
    study_user_manager = get_study_user_manager(load_from_aws=False)

    # Create handler registry and register all handlers
    handler_registry = RecordHandlerRegistry()
    handlers = create_handlers_for_all_types(
        path_manager=path_manager,
        file_utilities=file_utilities,
    )
    # Register all handlers
    for handler_key, handler in handlers.items():
        handler_registry.register_handler(handler_key, handler)

    return CacheWriteContext(
        path_manager=path_manager,
        directory_manager=directory_manager,
        file_utilities=file_utilities,
        handler_registry=handler_registry,
        study_user_manager=study_user_manager,
    )


def setup_batch_export_system() -> BatchExporter:
    """Set up the batch export system for exporting cache to storage.

    Returns:
        Configured BatchExporter instance
    """
    # Create shared infrastructure
    path_manager, directory_manager, file_utilities = _create_shared_infrastructure()

    # Create storage adapter and repository
    storage_adapter = LocalStorageAdapter()
    storage_repository = StorageRepository(adapter=storage_adapter)

    # Create handler registry (needed for study_user_exporter)
    handler_registry = RecordHandlerRegistry()
    handlers = create_handlers_for_all_types(
        path_manager=path_manager,
        file_utilities=file_utilities,
    )
    for handler_key, handler in handlers.items():
        handler_registry.register_handler(handler_key, handler)

    # Create exporters
    study_user_exporter = StudyUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        handler_registry=handler_registry,
    )

    in_network_exporter = InNetworkUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        file_utilities=file_utilities,
    )

    # Create batch exporter
    batch_exporter = BatchExporter(
        study_user_exporter=study_user_exporter,
        in_network_exporter=in_network_exporter,
        directory_manager=directory_manager,
        file_utilities=file_utilities,
        clear_filepaths=False,  # Default, can be overridden
        clear_cache=True,  # Default, can be overridden
    )

    return batch_exporter


def create_batch_export_context() -> BatchExportContext:
    """Create BatchExportContext for batch export operations.

    This is useful if you need the context object itself rather than
    just the BatchExporter.

    Returns:
        BatchExportContext containing all components needed for batch exports
    """
    # Create shared infrastructure
    path_manager, directory_manager, file_utilities = _create_shared_infrastructure()

    # Create storage adapter and repository
    storage_adapter = LocalStorageAdapter()
    storage_repository = StorageRepository(adapter=storage_adapter)

    # Create handler registry (needed for study_user_exporter)
    handler_registry = RecordHandlerRegistry()
    handlers = create_handlers_for_all_types(
        path_manager=path_manager,
        file_utilities=file_utilities,
    )
    for handler_key, handler in handlers.items():
        handler_registry.register_handler(handler_key, handler)

    # Create exporters
    study_user_exporter = StudyUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        handler_registry=handler_registry,
    )

    in_network_exporter = InNetworkUserActivityExporter(
        path_manager=path_manager,
        storage_repository=storage_repository,
        file_utilities=file_utilities,
    )

    return BatchExportContext(
        path_manager=path_manager,
        directory_manager=directory_manager,
        file_utilities=file_utilities,
        storage_repository=storage_repository,
        study_user_exporter=study_user_exporter,
        in_network_exporter=in_network_exporter,
    )

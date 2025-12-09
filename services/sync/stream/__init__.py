"""Sync stream module - exports data from firehose to storage."""

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)
from services.sync.stream.context import SyncExportContext
from services.sync.stream.handlers.generic import GenericRecordHandler
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.handlers.factories import (
    create_study_user_post_handler,
    create_study_user_like_handler,
    create_study_user_follow_handler,
    create_study_user_like_on_user_post_handler,
    create_study_user_reply_to_user_post_handler,
)
from services.sync.stream.exporters.base import BaseActivityExporter
from services.sync.stream.exporters.study_user_exporter import StudyUserActivityExporter
from services.sync.stream.exporters.in_network_exporter import (
    InNetworkUserActivityExporter,
)
from services.sync.stream.batch_exporter import BatchExporter, export_batch
from services.sync.stream.storage.repository import StorageRepository
from services.sync.stream.storage.adapters import LocalStorageAdapter
from services.sync.stream.setup import (
    setup_sync_export_system,
    setup_batch_export_system,
)

__all__ = [
    "CachePathManager",
    "CacheDirectoryManager",
    "CacheFileWriter",
    "CacheFileReader",
    "SyncExportContext",
    "GenericRecordHandler",
    "RecordHandlerRegistry",
    "create_study_user_post_handler",
    "create_study_user_like_handler",
    "create_study_user_follow_handler",
    "create_study_user_like_on_user_post_handler",
    "create_study_user_reply_to_user_post_handler",
    "BaseActivityExporter",
    "StudyUserActivityExporter",
    "InNetworkUserActivityExporter",
    "BatchExporter",
    "export_batch",
    "StorageRepository",
    "LocalStorageAdapter",
    "setup_sync_export_system",
    "setup_batch_export_system",
]

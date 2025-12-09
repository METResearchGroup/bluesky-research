"""Sync stream module - exports data from firehose to storage."""

from services.sync.stream.export_data import (
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
from services.sync.stream.storage.repository import StorageRepository
from services.sync.stream.storage.adapters import S3StorageAdapter, LocalStorageAdapter

__all__ = [
    "CachePathManager",
    "CacheDirectoryManager",
    "CacheFileWriter",
    "CacheFileReader",
    "RecordHandlerRegistry",
    "create_study_user_post_handler",
    "create_study_user_like_handler",
    "create_study_user_follow_handler",
    "create_study_user_like_on_user_post_handler",
    "create_study_user_reply_to_user_post_handler",
    "StudyUserActivityExporter",
    "StorageRepository",
    "S3StorageAdapter",
    "LocalStorageAdapter",
]

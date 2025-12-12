"""Core domain and infrastructure layer.

This module contains foundational types, protocols, configuration,
and dependency injection used throughout the sync stream system.
"""

from services.sync.stream.core.types import (
    Operation,
    RecordType,
    GenericRecordType,
    FollowStatus,
    HandlerKey,
    OperationsByType,
)
from services.sync.stream.core.record_types import (
    NSID_TO_RECORD_TYPE,
    SUPPORTED_RECORD_TYPES,
    ALL_RECORD_TYPES,
    RecordTypeRegistry,
)
from services.sync.stream.core.context import (
    CacheWriteContext,
    BatchExportContext,
)
from services.sync.stream.core.setup import (
    setup_cache_write_system,
    setup_batch_export_system,
    create_batch_export_context,
)

__all__ = [
    "Operation",
    "RecordType",
    "GenericRecordType",
    "FollowStatus",
    "HandlerKey",
    "OperationsByType",
    "NSID_TO_RECORD_TYPE",
    "SUPPORTED_RECORD_TYPES",
    "ALL_RECORD_TYPES",
    "RecordTypeRegistry",
    "CacheWriteContext",
    "BatchExportContext",
    "setup_cache_write_system",
    "setup_batch_export_system",
    "create_batch_export_context",
]

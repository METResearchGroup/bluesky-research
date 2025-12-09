"""Configuration for generic record handlers."""

from dataclasses import dataclass
from typing import Callable, Optional
from services.sync.stream.types import RecordType


@dataclass
class HandlerConfig:
    """Configuration for generic record handler.

    Attributes:
        record_type: Type of record this handler manages
        path_strategy: Function that constructs the base path for writing.
            Signature: (path_manager, operation, record_type, **kwargs) -> str
        nested_path_extractor: Optional function to extract nested path component
            from record dict. Signature: (record: dict) -> str
            Used for records nested by post_uri_suffix, etc.
        requires_follow_status: Whether this handler requires follow_status parameter
        read_strategy: Optional custom read strategy. If None, uses default.
            Signature: (file_reader, base_path: str, record_type: RecordType) -> tuple[list[dict], list[str]]
    """

    record_type: RecordType
    path_strategy: Callable
    nested_path_extractor: Optional[Callable[[dict], str]] = None
    requires_follow_status: bool = False
    read_strategy: Optional[Callable] = None

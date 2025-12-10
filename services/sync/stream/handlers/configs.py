"""Handler configurations for different record types."""

import json
import os
from lib.log.logger import get_logger

from services.sync.stream.handlers.config import HandlerConfig
from services.sync.stream.types import RecordType, FollowStatus, Operation
from services.sync.stream.protocols import PathManagerProtocol, FileUtilitiesProtocol

logger = get_logger(__file__)


def _extract_post_uri_suffix(record: dict) -> str:
    """Extract post URI suffix from like record."""
    try:
        return record["record"]["subject"]["uri"].split("/")[-1]
    except (KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"Invalid record structure for URI extraction: {e}") from e


def _extract_parent_post_uri_suffix(record: dict) -> str:
    """Extract parent post URI suffix from reply record."""
    try:
        return record["record"]["reply"]["root"]["uri"].split("/")[-1]
    except (KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"Invalid record structure for URI extraction: {e}") from e


def _default_path_strategy(
    path_manager: PathManagerProtocol,
    operation: Operation,
    record_type: RecordType,
    *,
    follow_status: FollowStatus | None = None,
    author_did: str | None = None,
) -> str:
    """Default path strategy for study user activity."""
    return path_manager.get_study_user_activity_path(
        operation=operation,
        record_type=record_type,
        follow_status=follow_status,
    )


def _in_network_path_strategy(
    path_manager: PathManagerProtocol,
    operation: Operation,
    record_type: RecordType,
    *,
    follow_status: FollowStatus | None = None,
    author_did: str | None = None,
) -> str:
    """Path strategy for in-network user activity."""
    if not author_did:
        raise ValueError("author_did is required for in-network activity paths")
    return path_manager.get_in_network_activity_path(
        operation=operation,
        record_type=record_type,
        author_did=author_did,
    )


def _nested_read_strategy(
    file_reader: FileUtilitiesProtocol,
    base_path: str,
    record_type: RecordType,
) -> tuple[list[dict], list[str]]:
    """Read strategy for nested records (likes, replies)."""
    records: list[dict] = []
    filepaths: list[str] = []
    record_dir = os.path.join(base_path, record_type.value)

    if not file_reader.is_directory(record_dir):
        return records, filepaths

    # Iterate over nested directories (e.g., post_uri_suffix directories)
    for nested_dir_name in file_reader.list_directories(record_dir):
        nested_path = os.path.join(record_dir, nested_dir_name)

        # Read all JSON files in nested directory
        for filename in file_reader.list_files(nested_path):
            if not filename.endswith(".json"):
                continue
            full_path = os.path.join(nested_path, filename)
            try:
                data = file_reader.read_json(full_path)
                records.append(data)
                filepaths.append(full_path)
            except (
                json.JSONDecodeError,
                OSError,
                IOError,
                FileNotFoundError,
                PermissionError,
            ) as e:
                logger.warning(
                    f"Failed to read JSON file: {full_path}. Error: {type(e).__name__}: {str(e)}",
                    context={
                        "filepath": full_path,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                continue

    return records, filepaths


def _follow_read_strategy(
    file_reader: FileUtilitiesProtocol,
    base_path: str,
    record_type: RecordType,
) -> tuple[list[dict], list[str]]:
    """Read strategy for follow records (nested by follow_status)."""
    records: list[dict] = []
    filepaths: list[str] = []
    follow_dir = os.path.join(base_path, record_type.value)

    if not os.path.exists(follow_dir):
        return records, filepaths

    # Iterate over follow_status directories
    for follow_status in [FollowStatus.FOLLOWEE, FollowStatus.FOLLOWER]:
        follow_type_path = os.path.join(follow_dir, follow_status.value)
        if not os.path.exists(follow_type_path):
            continue

        for filename in file_reader.list_files(follow_type_path):
            full_path = os.path.join(follow_type_path, filename)
            try:
                data = file_reader.read_json(full_path)
                records.append(data)
                filepaths.append(full_path)
            except (
                json.JSONDecodeError,
                OSError,
                IOError,
                FileNotFoundError,
                PermissionError,
            ) as e:
                logger.warning(
                    f"Failed to read JSON file: {full_path}. Error: {type(e).__name__}: {str(e)}",
                    context={
                        "filepath": full_path,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                continue

    return records, filepaths


# Handler configurations for each record type
POST_CONFIG = HandlerConfig(
    record_type=RecordType.POST,
    path_strategy=_default_path_strategy,
)

LIKE_CONFIG = HandlerConfig(
    record_type=RecordType.LIKE,
    path_strategy=_default_path_strategy,
    nested_path_extractor=_extract_post_uri_suffix,
    read_strategy=_nested_read_strategy,
)

FOLLOW_CONFIG = HandlerConfig(
    record_type=RecordType.FOLLOW,
    path_strategy=_default_path_strategy,
    requires_follow_status=True,
    read_strategy=_follow_read_strategy,
)

LIKE_ON_USER_POST_CONFIG = HandlerConfig(
    record_type=RecordType.LIKE_ON_USER_POST,
    path_strategy=_default_path_strategy,
    nested_path_extractor=_extract_post_uri_suffix,
    read_strategy=_nested_read_strategy,
)

REPLY_TO_USER_POST_CONFIG = HandlerConfig(
    record_type=RecordType.REPLY_TO_USER_POST,
    path_strategy=_default_path_strategy,
    nested_path_extractor=_extract_parent_post_uri_suffix,
    read_strategy=_nested_read_strategy,
)

IN_NETWORK_POST_CONFIG = HandlerConfig(
    record_type=RecordType.POST,
    path_strategy=_in_network_path_strategy,
)

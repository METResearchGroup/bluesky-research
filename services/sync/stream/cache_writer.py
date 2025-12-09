"""Public-facing cache writer interface for real-time cache writes.

This module provides adapter functions that maintain backward compatibility
with data_filter.py while using the new handler-based architecture.
"""

import os
from typing import Any, Literal, Optional

from lib.log.logger import get_logger

# Global system components (lazy initialization)
_system_components: dict | None = None
_study_user_manager = None


def _get_study_user_manager():
    """Lazy initialization of study user manager."""
    global _study_user_manager
    if _study_user_manager is None:
        from services.participant_data.study_users import get_study_user_manager

        _study_user_manager = get_study_user_manager(load_from_aws=False)
    return _study_user_manager


def _get_system_components():
    """Lazy initialization of system components."""
    global _system_components
    if _system_components is None:
        from services.sync.stream.setup import setup_sync_export_system

        # Initialize system (local cache only for data_filter usage)
        (
            path_manager,
            _,
            file_writer,
            file_reader,
            handler_registry,
            _,
            _,
        ) = setup_sync_export_system()

        _system_components = {
            "path_manager": path_manager,
            "file_writer": file_writer,
            "file_reader": file_reader,
            "handler_registry": handler_registry,
        }

    return _system_components


def export_study_user_data_local(
    record: dict,
    record_type: Literal[
        "post", "follow", "like", "like_on_user_post", "reply_to_user_post"
    ],
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    kwargs: Optional[dict] = None,
    study_user_manager: Optional[Any] = None,
) -> None:
    """Writes study user activity to local cache storage.

    This is an adapter function that maintains backward compatibility
    with data_filter.py while using the new handler-based architecture.

    NOTE: When refactoring data_filter.py (issue #231), we should update
    it to use handlers directly instead of this adapter function.

    Args:
        record: Record data to write
        record_type: Type of record (post, like, follow, etc.)
        operation: Operation type (create/delete)
        author_did: Author DID
        filename: Filename for the record
        kwargs: Optional additional arguments (e.g., follow_status, user_post_type)
        study_user_manager: Optional study user manager instance. If not provided,
            will use lazy initialization. This parameter enables dependency injection
            for testing.

    Raises:
        ValueError: If record_type is unknown or required kwargs are missing
    """
    if kwargs is None:
        kwargs = {}

    components = _get_system_components()
    handler_registry = components["handler_registry"]

    try:
        handler = handler_registry.get_handler(record_type)

        # Prepare kwargs for handler.write_record()
        handler_kwargs = {}
        if record_type == "follow":
            # Follow requires follow_status
            follow_status = kwargs.get("follow_status")
            if not follow_status:
                raise ValueError(
                    f"follow_status required for follow records. "
                    f"Got kwargs: {kwargs}"
                )
            handler_kwargs["follow_status"] = follow_status

        # Write record using handler
        handler.write_record(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename,
            **handler_kwargs,
        )

        # Update StudyUserManager for posts (maintains old behavior)
        if record_type == "post" and operation == "create":
            # Use injected manager if provided, otherwise lazy initialize
            manager = (
                study_user_manager
                if study_user_manager is not None
                else _get_study_user_manager()
            )
            manager.insert_study_user_post(post_uri=record["uri"], user_did=author_did)

    except KeyError as e:
        available_types = handler_registry.list_record_types()
        raise ValueError(
            f"Unknown record type: {record_type}. "
            f"Available types: {available_types}"
        ) from e
    except Exception as e:
        # Log error and re-raise
        logger = get_logger(__name__)
        logger.error(f"Error exporting {record_type} record for user {author_did}: {e}")
        raise


def export_in_network_user_data_local(
    record: dict,
    record_type: Literal["post", "follow", "like"],
    author_did: str,
    filename: str,
    study_user_manager: Optional[Any] = None,
) -> None:
    """Writes in-network user activity to local cache storage.

    This is an adapter function that maintains backward compatibility
    with data_filter.py while using the new handler-based architecture.

    NOTE: When refactoring data_filter.py (issue #231), we should update
    it to use handlers directly instead of this adapter function.

    Args:
        record: Record data to write
        record_type: Type of record (currently only "post" is implemented)
        author_did: Author DID
        filename: Filename for the record
        study_user_manager: Optional study user manager instance. This parameter
            enables dependency injection for testing (currently unused but included
            for consistency with export_study_user_data_local).
    """
    if record_type != "post":
        # Old code had this as a no-op for non-post types
        return

    components = _get_system_components()
    path_manager = components["path_manager"]
    file_writer = components["file_writer"]

    # Get path for in-network user post
    folder_path: str = path_manager.get_in_network_activity_path(
        operation="create",
        record_type="post",
        author_did=author_did,
    )

    full_path = os.path.join(folder_path, filename)
    file_writer.write_json(full_path, record)

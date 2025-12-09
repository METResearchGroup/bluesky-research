import os
import json
from typing import Any, Literal, Optional

from services.sync.stream.protocols import DirectoryManagerProtocol


class CacheFileWriter:
    """Manages the writing of cache files."""

    def __init__(self, directory_manager: DirectoryManagerProtocol):
        """Initialize file writer.

        Args:
            directory_manager: Manager for ensuring directories exist
        """
        self.directory_manager = directory_manager

    def write_json(self, path: str, data: dict) -> None:
        """Write JSON data to file at path."""
        parent_dir = os.path.dirname(path)
        self.directory_manager.ensure_exists(parent_dir)
        with open(path, "w") as f:
            json.dump(data, f)

    def write_jsonl(self, path: str, records: list[dict]) -> None:
        """Write JSONL data to file at path."""
        parent_dir = os.path.dirname(path)
        self.directory_manager.ensure_exists(parent_dir)
        with open(path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")


class CacheFileReader:
    """Manages the reading of cache files."""

    def read_json(self, path: str) -> dict:
        """Read JSON file from path."""
        with open(path, "r") as f:
            return json.load(f)

    def list_files(self, directory: str) -> list[str]:
        """List all files in directory."""
        if not os.path.exists(directory):
            return []
        return [
            f
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]

    def read_all_json_in_directory(
        self, directory: str
    ) -> tuple[list[dict], list[str]]:
        """Read all JSON files in directory, returning (data, filepaths)."""
        records: list[dict] = []
        filepaths: list[str] = []

        if not os.path.exists(directory):
            return records, filepaths

        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath) and filename.endswith(".json"):
                data = self.read_json(filepath)
                records.append(data)
                filepaths.append(filepath)

        return records, filepaths


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

    components = {}  # TODO: implement
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
                # else _get_study_user_manager()
                else {}  # TODO: implement
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
        from lib.log.logger import get_logger

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

    components = {}  # TODO: implement
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

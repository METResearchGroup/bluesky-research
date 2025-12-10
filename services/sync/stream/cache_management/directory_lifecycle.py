"""Manages the lifecycle of the cache directory."""

import os
import shutil

from services.sync.stream.protocols import PathManagerProtocol
from services.sync.stream.types import Operation, RecordType, FollowStatus


class CacheDirectoryManager:
    """Manages the building and deletion of cache directories."""

    def __init__(self, path_manager: PathManagerProtocol):
        """Initialize directory manager.

        Args:
            path_manager: Path manager for getting paths to create
        """
        self.path_manager = path_manager

    def ensure_exists(self, path: str) -> None:
        """Ensure directory exists, creating if necessary."""
        os.makedirs(path, exist_ok=True)

    def rebuild_all(self) -> None:
        """Rebuild all cache directory structures."""
        path_mgr = self.path_manager

        # Create root write path
        os.makedirs(path_mgr.root_write_path, exist_ok=True)

        # Create generic firehose paths (create/delete for each operation type)
        for operation in [Operation.CREATE, Operation.DELETE]:
            op_path = (
                path_mgr.root_create_path
                if operation == Operation.CREATE
                else path_mgr.root_delete_path
            )
            os.makedirs(op_path, exist_ok=True)

            for op_type in path_mgr.operation_types:
                op_type_path = os.path.join(op_path, op_type)
                os.makedirs(op_type_path, exist_ok=True)

        # Create study user activity paths
        os.makedirs(path_mgr.study_user_activity_root_local_path, exist_ok=True)

        for operation in [Operation.CREATE, Operation.DELETE]:
            # Get available record types for this operation
            available_record_types = list(
                path_mgr.study_user_activity_relative_path_map[operation.value].keys()
            )
            # Remove "follow" from the list since it's a dict, not a string
            if "follow" in available_record_types:
                available_record_types.remove("follow")

            for record_type_str in available_record_types:
                # Convert string to enum
                record_type = RecordType(record_type_str)
                record_path = path_mgr.get_study_user_activity_path(
                    operation=operation,
                    record_type=record_type,
                )
                os.makedirs(record_path, exist_ok=True)

            # Follow has nested structure - handle separately
            if (
                "follow"
                in path_mgr.study_user_activity_relative_path_map[operation.value]
            ):
                for follow_status in [FollowStatus.FOLLOWEE, FollowStatus.FOLLOWER]:
                    follow_path = path_mgr.get_study_user_activity_path(
                        operation=operation,
                        record_type=RecordType.FOLLOW,
                        follow_status=follow_status,
                    )
                    os.makedirs(follow_path, exist_ok=True)

        # Create in-network user activity path
        os.makedirs(path_mgr.in_network_user_activity_root_local_path, exist_ok=True)
        os.makedirs(
            path_mgr.in_network_user_activity_create_post_local_path, exist_ok=True
        )

    def delete_all(self) -> None:
        """Delete all cache directories."""
        if os.path.exists(self.path_manager.root_write_path):
            shutil.rmtree(self.path_manager.root_write_path)

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        return os.path.exists(path)

    def delete_files(self, filepaths: list[str]) -> None:
        """Delete list of files.

        Args:
            filepaths: List of file paths to delete
        """
        for filepath in filepaths:
            if os.path.exists(filepath) and os.path.isfile(filepath):
                os.remove(filepath)

    def delete_empty_directories(self, root_path: str) -> None:
        """Recursively delete empty directories starting from root_path.

        Args:
            root_path: Root directory to start deletion from
        """
        if not os.path.exists(root_path):
            return

        # Walk bottom-up to delete empty directories
        for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
            # Skip if directory has files or subdirectories
            if filenames or dirnames:
                continue

            # Directory is empty, try to remove it
            try:
                os.rmdir(dirpath)
            except OSError:
                # Directory might not be empty due to race condition or permissions
                pass

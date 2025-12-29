"""Manages the lifecycle of the cache directory."""

import os
import shutil

from services.sync.stream.core.protocols import PathManagerProtocol
from services.sync.stream.core.types import Operation, RecordType, FollowStatus


class CacheDirectoryManager:
    """Manages the building and deletion of cache directories."""

    def __init__(self, path_manager: PathManagerProtocol):
        """Initialize directory manager.

        Args:
            path_manager: Path manager for getting paths to create
        """
        self.path_manager = path_manager

    def ensure_directory_exists(self, path: str) -> None:
        """Ensure directory exists, creating if necessary.

        Args:
            path: Directory path to ensure exists
        """
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

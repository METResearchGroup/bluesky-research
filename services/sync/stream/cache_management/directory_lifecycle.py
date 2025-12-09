"""Manages the lifecycle of the cache directory."""

import os
import shutil

from services.sync.stream.protocols import PathManagerProtocol
from services.sync.stream.cache_management.paths import CachePathManager


class CacheDirectoryManager:
    """Manages the building and deletion of cache directories."""

    def __init__(self, path_manager: PathManagerProtocol):
        """Initialize directory manager.

        Args:
            path_manager: Path manager for getting paths to create
        """
        self.path_manager = path_manager
        # Access path manager's attributes for rebuild logic
        # (You may need to expose these or refactor)

    def ensure_exists(self, path: str) -> None:
        """Ensure directory exists, creating if necessary."""
        if not os.path.exists(path):
            os.makedirs(path)

    def rebuild_all(self) -> None:
        """Rebuild all cache directory structures."""
        # Access path_manager's attributes to build all paths
        if not isinstance(self.path_manager, CachePathManager):
            raise TypeError("rebuild_all requires CachePathManager instance")

        path_mgr = self.path_manager

        # Create root write path
        if not os.path.exists(path_mgr.root_write_path):
            os.makedirs(path_mgr.root_write_path)

        # Create generic firehose paths (create/delete for each operation type)
        for operation in ["create", "delete"]:
            op_path = (
                path_mgr.root_create_path
                if operation == "create"
                else path_mgr.root_delete_path
            )
            if not os.path.exists(op_path):
                os.makedirs(op_path)

            for op_type in path_mgr.operation_types:
                op_type_path = os.path.join(op_path, op_type)
                if not os.path.exists(op_type_path):
                    os.makedirs(op_type_path)

        # Create study user activity paths
        if not os.path.exists(path_mgr.study_user_activity_root_local_path):
            os.makedirs(path_mgr.study_user_activity_root_local_path)

        for operation in ["create", "delete"]:
            # Get available record types for this operation
            available_record_types = list(
                path_mgr.study_user_activity_relative_path_map[operation].keys()
            )
            # Remove "follow" from the list since it's a dict, not a string
            if "follow" in available_record_types:
                available_record_types.remove("follow")

            for record_type in available_record_types:
                record_path = path_mgr.get_study_user_activity_path(
                    operation=operation,  # type: ignore[arg-type]
                    record_type=record_type,  # type: ignore[arg-type]
                )
                if not os.path.exists(record_path):
                    os.makedirs(record_path)

            # Follow has nested structure - handle separately
            if "follow" in path_mgr.study_user_activity_relative_path_map[operation]:
                for follow_type in ["followee", "follower"]:
                    follow_path = path_mgr.get_study_user_activity_path(
                        operation=operation,  # type: ignore[arg-type]
                        record_type="follow",
                        follow_status=follow_type,  # type: ignore[arg-type]
                    )
                    if not os.path.exists(follow_path):
                        os.makedirs(follow_path)

        # Create in-network user activity path
        if not os.path.exists(path_mgr.in_network_user_activity_root_local_path):
            os.makedirs(path_mgr.in_network_user_activity_root_local_path)
        if not os.path.exists(path_mgr.in_network_user_activity_create_post_local_path):
            os.makedirs(path_mgr.in_network_user_activity_create_post_local_path)

    def delete_all(self) -> None:
        """Delete all cache directories."""
        if not isinstance(self.path_manager, CachePathManager):
            raise TypeError("delete_all requires CachePathManager instance")

        if os.path.exists(self.path_manager.root_write_path):
            shutil.rmtree(self.path_manager.root_write_path)

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        return os.path.exists(path)

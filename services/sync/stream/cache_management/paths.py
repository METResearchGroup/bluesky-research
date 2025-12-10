"""Path construction for cache directory structure."""

import os

from services.sync.stream.types import (
    Operation,
    RecordType,
    GenericRecordType,
    FollowStatus,
)


class CachePathManager:
    """Manages path construction for cache directory structure."""

    def __init__(self):
        # move up one directory to get to the root of the sync export system
        # Note: Using os.path.abspath(__file__) works for typical development/deployment
        # but may behave unexpectedly in frozen executables, zip imports, or some
        # containerized environments. This is acceptable for the current use case.
        self.current_file_directory = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )

        # should be services/sync/stream/__local_cache__
        self.root_write_path = os.path.join(
            self.current_file_directory, "__local_cache__"
        )
        self.root_create_path = os.path.join(self.root_write_path, "create")
        self.root_delete_path = os.path.join(self.root_write_path, "delete")
        self.operation_types = ["post", "like", "follow"]

        # Helper paths for generic firehose writes.
        self.export_filepath_map = {
            "create": {
                "post": os.path.join(self.root_create_path, "post"),
                "like": os.path.join(self.root_create_path, "like"),
                "follow": os.path.join(self.root_create_path, "follow"),
            },
            "delete": {
                "post": os.path.join(self.root_delete_path, "post"),
                "like": os.path.join(self.root_delete_path, "like"),
                "follow": os.path.join(self.root_delete_path, "follow"),
            },
        }

        # Helper paths for writing user activity data.
        self.study_user_activity_root_local_path = os.path.join(
            self.root_write_path, "study_user_activity"
        )
        self.study_user_activity_create_path = os.path.join(
            self.study_user_activity_root_local_path, "create"
        )
        self.study_user_activity_delete_path = os.path.join(
            self.study_user_activity_root_local_path, "delete"
        )

        self.study_user_activity_relative_path_map = {
            "create": {
                "post": os.path.join("create", "post"),
                "like": os.path.join("create", "like"),
                "follow": {
                    "followee": os.path.join("create", "follow", "followee"),
                    "follower": os.path.join("create", "follow", "follower"),
                },
                "like_on_user_post": os.path.join("create", "like_on_user_post"),
                "reply_to_user_post": os.path.join("create", "reply_to_user_post"),
            },
            "delete": {
                "post": os.path.join("delete", "post"),
                "like": os.path.join("delete", "like"),
            },
        }

        # Helper paths for writing in-network user activity data.
        self.in_network_user_activity_root_local_path = os.path.join(
            self.root_write_path, "in_network_user_activity"
        )
        self.in_network_user_activity_create_post_local_path = os.path.join(
            self.in_network_user_activity_root_local_path, "create", "post"
        )

    def get_local_cache_path(
        self,
        operation: Operation,
        record_type: GenericRecordType,
    ) -> str:
        """Get local cache path for general firehose records."""
        return self.export_filepath_map[operation.value][record_type.value]

    def get_study_user_activity_path(
        self,
        operation: Operation,
        record_type: RecordType,
        follow_status: FollowStatus | None = None,
    ) -> str:
        """Get path for study user activity records."""
        # Validate operation exists in map
        if operation.value not in self.study_user_activity_relative_path_map:
            available_operations = list(
                self.study_user_activity_relative_path_map.keys()
            )
            raise ValueError(
                f"Unsupported operation '{operation.value}'. "
                f"Available operations: {available_operations}"
            )

        operation_map = self.study_user_activity_relative_path_map[operation.value]

        if record_type == RecordType.FOLLOW and follow_status:
            # Follow has nested structure
            if "follow" not in operation_map:
                available_record_types = list(operation_map.keys())
                raise ValueError(
                    f"Unsupported record_type 'follow' for operation '{operation.value}'. "
                    f"Available record types for '{operation.value}': {available_record_types}"
                )
            if not isinstance(operation_map["follow"], dict):
                raise ValueError(
                    f"Invalid structure: 'follow' entry for operation '{operation.value}' "
                    f"is not a nested dictionary"
                )
            if follow_status.value not in operation_map["follow"]:
                available_follow_statuses = list(operation_map["follow"].keys())
                raise ValueError(
                    f"Unsupported follow_status '{follow_status.value}' for operation '{operation.value}'. "
                    f"Available follow statuses: {available_follow_statuses}"
                )
            relative = operation_map["follow"][follow_status.value]
            return os.path.join(self.study_user_activity_root_local_path, relative)
        else:
            if record_type.value not in operation_map:
                available_record_types = list(operation_map.keys())
                raise ValueError(
                    f"Unsupported record_type '{record_type.value}' for operation '{operation.value}'. "
                    f"Available record types for '{operation.value}': {available_record_types}"
                )
            relative = operation_map[record_type.value]
            return os.path.join(self.study_user_activity_root_local_path, relative)

    def get_in_network_activity_path(
        self,
        operation: Operation,
        record_type: RecordType,
        author_did: str,
    ) -> str:
        """Get path for in-network user activity records."""
        return os.path.join(
            self.in_network_user_activity_root_local_path,
            operation.value,
            record_type.value,
            author_did,
        )

    def get_relative_path(
        self,
        operation: Operation,
        record_type: RecordType,
        follow_status: FollowStatus | None = None,
    ) -> str:
        """Get relative path component (without root)."""
        # Validate operation exists in map
        if operation.value not in self.study_user_activity_relative_path_map:
            available_operations = list(
                self.study_user_activity_relative_path_map.keys()
            )
            raise ValueError(
                f"Unsupported operation '{operation.value}'. "
                f"Available operations: {available_operations}"
            )

        operation_map = self.study_user_activity_relative_path_map[operation.value]

        if record_type == RecordType.FOLLOW and follow_status:
            if "follow" not in operation_map:
                available_record_types = list(operation_map.keys())
                raise ValueError(
                    f"Unsupported record_type 'follow' for operation '{operation.value}'. "
                    f"Available record types for '{operation.value}': {available_record_types}"
                )
            if not isinstance(operation_map["follow"], dict):
                raise ValueError(
                    f"Invalid structure: 'follow' entry for operation '{operation.value}' "
                    f"is not a nested dictionary"
                )
            if follow_status.value not in operation_map["follow"]:
                available_follow_statuses = list(operation_map["follow"].keys())
                raise ValueError(
                    f"Unsupported follow_status '{follow_status.value}' for operation '{operation.value}' "
                    f"with record_type 'follow'. Available follow statuses: {available_follow_statuses}"
                )
            return operation_map["follow"][follow_status.value]
        else:
            if record_type.value not in operation_map:
                available_record_types = list(operation_map.keys())
                raise ValueError(
                    f"Unsupported record_type '{record_type.value}' for operation '{operation.value}'. "
                    f"Available record types for '{operation.value}': {available_record_types}"
                )
            return operation_map[record_type.value]

    def get_study_user_activity_operation_path(
        self,
        operation: Operation,
    ) -> str:
        """Get base path for study user activity operation (create/delete level).

        This returns the operation-level path (e.g., .../study_user_activity/create)
        without any record type suffix, allowing callers to navigate to specific
        record type directories from this base.
        """
        return os.path.join(
            self.study_user_activity_root_local_path,
            operation.value,
        )

"""Path construction for cache directory structure."""

import os
from typing import Literal


class CachePathManager:
    """Manages path construction for cache directory structure."""

    def __init__(self):
        self.current_file_directory = os.path.dirname(os.path.abspath(__file__))
        self.root_write_path = os.path.join(
            self.current_file_directory, "__local_cache__"
        )
        self.root_create_path = os.path.join(self.root_write_path, "create")
        self.root_delete_path = os.path.join(self.root_write_path, "delete")
        self.operation_types = ["post", "like", "follow"]

        # Helper paths for generic firehose writes. This seems to be incomplete,
        # usually you'd want all operation types here.
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
        operation: Literal["create", "delete"],
        record_type: Literal["post", "like", "follow"],
    ) -> str:
        """Get local cache path for general firehose records."""
        return self.export_filepath_map[operation][record_type]

    def get_study_user_activity_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal[
            "post", "like", "follow", "like_on_user_post", "reply_to_user_post"
        ],
        follow_status: Literal["follower", "followee"] | None = None,
    ) -> str:
        """Get path for study user activity records."""
        if record_type == "follow" and follow_status:
            # Follow has nested structure
            relative = self.study_user_activity_relative_path_map[operation]["follow"][
                follow_status
            ]
            return os.path.join(self.study_user_activity_root_local_path, relative)
        else:
            relative = self.study_user_activity_relative_path_map[operation][
                record_type
            ]
            return os.path.join(self.study_user_activity_root_local_path, relative)

    def get_in_network_activity_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal["post"],
        author_did: str,
    ) -> str:
        """Get path for in-network user activity records."""
        return os.path.join(
            self.in_network_user_activity_root_local_path,
            operation,
            record_type,
            author_did,
        )

    def get_relative_path(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal[
            "post", "like", "follow", "like_on_user_post", "reply_to_user_post"
        ],
        follow_status: Literal["follower", "followee"] | None = None,
    ) -> str:
        """Get relative path component (without root)."""
        if record_type == "follow" and follow_status:
            return self.study_user_activity_relative_path_map[operation]["follow"][
                follow_status
            ]
        return self.study_user_activity_relative_path_map[operation][record_type]

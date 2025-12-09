import json
import os
import shutil
from typing import Literal

from services.sync.stream.protocols import DirectoryManagerProtocol, PathManagerProtocol


class SyncPathManager:
    """Manages path construction logic."""

    def __init__(self):
        # Define instance attributes instead of using local variables.
        self.current_file_directory = os.path.dirname(os.path.abspath(__file__))
        self.root_write_path = os.path.join(self.current_file_directory, "cache")
        self.root_create_path = os.path.join(self.root_write_path, "create")
        self.root_delete_path = os.path.join(self.root_write_path, "delete")
        self.operation_types = ["post", "like", "follow"]

        # Root S3 key for all sync exports
        self.root_s3_key = os.path.join("sync", "firehose")

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

        # Helper paths for S3 exports.
        self.s3_export_key_map = {
            "create": {
                "post": os.path.join(self.root_s3_key, "create", "post"),
                "like": os.path.join(self.root_s3_key, "create", "like"),
                "follow": os.path.join(self.root_s3_key, "create", "follow"),
            },
            "delete": {
                "post": os.path.join(self.root_s3_key, "delete", "post"),
                "like": os.path.join(self.root_s3_key, "delete", "like"),
                "follow": os.path.join(self.root_s3_key, "delete", "follow"),
            },
        }

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

    def get_s3_key(
        self,
        operation: Literal["create", "delete"],
        record_type: Literal["post", "like", "follow"],
        partition_key: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Get S3 key for general firehose records."""
        base_key = self.s3_export_key_map[operation][record_type]
        if partition_key:
            base_key = os.path.join(base_key, partition_key)
        if filename:
            base_key = os.path.join(base_key, filename)
        return base_key

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
        if not isinstance(self.path_manager, SyncPathManager):
            raise TypeError("rebuild_all requires SyncPathManager instance")

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
            for record_type in [
                "post",
                "like",
                "follow",
                "like_on_user_post",
                "reply_to_user_post",
            ]:
                record_path = path_mgr.get_study_user_activity_path(
                    operation=operation,  # type: ignore[arg-type]
                    record_type=record_type,  # type: ignore[arg-type]
                )
                if not os.path.exists(record_path):
                    os.makedirs(record_path)

                # Follow has nested structure
                if record_type == "follow":
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
        if not isinstance(self.path_manager, SyncPathManager):
            raise TypeError("delete_all requires SyncPathManager instance")

        if os.path.exists(self.path_manager.root_write_path):
            shutil.rmtree(self.path_manager.root_write_path)

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        return os.path.exists(path)


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

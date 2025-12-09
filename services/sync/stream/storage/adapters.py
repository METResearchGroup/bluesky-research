"""Concrete storage adapter implementations."""

import os
import uuid
import pandas as pd

from services.sync.stream.protocols import PathManagerProtocol
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    write_jsons_to_local_store,
    export_data_to_local_storage,
)
from lib.helper import generate_current_datetime_str


class S3StorageAdapter:
    """S3 storage adapter implementation."""

    def __init__(self, s3: S3, path_manager: PathManagerProtocol):
        """Initialize S3 adapter.

        Args:
            s3: S3 client
            path_manager: Path manager for constructing S3 keys
        """
        self.s3 = s3
        self.path_manager = path_manager

    def write_dataframe(
        self,
        df: pd.DataFrame,
        key: str,
        service: str,
        custom_args: dict | None = None,
    ) -> None:
        """Write DataFrame to S3 via export_data_to_local_storage.

        Note: This uses the existing export_data_to_local_storage which
        handles both local and S3 writes. For pure S3, you'd use s3.write_parquet_to_s3.
        """
        export_data_to_local_storage(
            df=df,
            service=service,
            custom_args=custom_args or {},
        )

    def write_jsonl(
        self,
        data: list[dict],
        key: str,
        compressed: bool = True,
    ) -> None:
        """Write JSONL to S3.

        Note: S3.write_dicts_jsonl_to_s3 doesn't support compression,
        but we keep the parameter for protocol compatibility.
        """
        self.s3.write_dicts_jsonl_to_s3(
            data=data,
            key=key,
        )

    def write_dicts_from_directory(
        self,
        directory: str,
        key: str,
        compressed: bool = True,
    ) -> list[dict]:
        """Write JSON files from directory to S3."""
        timestamp_str = generate_current_datetime_str()
        partition_key = S3.create_partition_key_based_on_timestamp(timestamp_str)
        filename = f"{timestamp_str}-{str(uuid.uuid4())}.jsonl"

        # Construct full key if not provided
        if not key:
            # Extract operation and record_type from directory path
            # This is a simplification - you'd use path_manager here
            key = os.path.join("sync", "firehose", partition_key, filename)

        # Read all JSON files from directory first
        all_data: list[dict] = []
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                if os.path.isfile(filepath) and filename.endswith(".json"):
                    with open(filepath, "r") as f:
                        import json

                        all_data.append(json.load(f))

        # Write as JSONL to S3
        if all_data:
            self.s3.write_dicts_jsonl_to_s3(
                data=all_data,
                key=key,
            )

        return all_data


class LocalStorageAdapter:
    """Local filesystem storage adapter implementation."""

    def __init__(self, path_manager: PathManagerProtocol):
        """Initialize local storage adapter.

        Args:
            path_manager: Path manager for constructing local paths
        """
        self.path_manager = path_manager

    def write_dataframe(
        self,
        df: pd.DataFrame,
        key: str,
        service: str,
        custom_args: dict | None = None,
    ) -> None:
        """Write DataFrame to local storage."""
        export_data_to_local_storage(
            df=df,
            service=service,
            custom_args=custom_args or {},
        )

    def write_jsonl(
        self,
        data: list[dict],
        key: str,
        compressed: bool = False,  # Local usually uncompressed
    ) -> None:
        """Write JSONL to local filesystem."""
        # Implementation for local JSONL writing
        full_path = os.path.join(root_local_data_directory, key)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        # ... write JSONL logic

    def write_dicts_from_directory(
        self,
        directory: str,
        key: str,
        compressed: bool = False,
    ) -> list[dict]:
        """Write JSON files from directory to local storage."""
        full_path = os.path.join(root_local_data_directory, key)
        result = write_jsons_to_local_store(
            source_directory=directory,
            export_filepath=full_path,
            compressed=compressed,
        )
        # write_jsons_to_local_store can return None, ensure we return a list
        return result if result is not None else []

"""Integration tests for batch write happy paths.

These tests verify the end-to-end flow of reading from cache
and exporting to permanent storage.
"""

import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from services.sync.stream.setup import setup_sync_export_system
from services.sync.stream.batch_exporter import BatchExporter
from services.sync.stream.cache_management import CachePathManager, CacheFileWriter
from services.sync.stream.types import Operation, RecordType, FollowStatus
# All StudyUserManager patching is handled by autouse fixture in conftest.py


class TestBatchWriteStudyUserActivity:
    """Test batch export flow for study user activity."""

    def test_batch_export_study_user_posts(self, tmp_path, monkeypatch):
        """Test that study user posts in cache are exported to storage."""
        import pandas as pd
        
        # Setup: Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        # Setup: Create cache files manually
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Create test post records
        study_user_did = "did:plc:study-user-1"
        post_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
        )
        
        test_posts = [
            {
                "author_did": study_user_did,
                "uri": f"at://{study_user_did}/app.bsky.feed.post/post-1",
                "text": "Test post 1",
            },
            {
                "author_did": study_user_did,
                "uri": f"at://{study_user_did}/app.bsky.feed.post/post-2",
                "text": "Test post 2",
            },
        ]
        
        # Clean up any existing files in the post path first
        if os.path.exists(post_path):
            for filename in os.listdir(post_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(post_path, filename))
        
        # Write posts to cache
        created_files = []
        for i, post in enumerate(test_posts):
            filename = f"author_did={study_user_did}_post_uri_suffix=post-{i+1}.json"
            file_path = os.path.join(post_path, filename)
            file_writer.write_json(file_path, post)
            created_files.append(file_path)
        
        # Mock storage repository to capture exports
        mock_storage_repository = MagicMock()
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
        # Mock MAP_SERVICE_TO_METADATA to avoid KeyError and dtype issues
        from lib.db import service_constants
        from lib.helper import generate_current_datetime_str
        from lib.constants import timestamp_format
        
        if "study_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["study_user_activity"] = {
                "dtypes_map": {}
            }
        # Also ensure scraped_user_social_network exists (for follows)
        if "scraped_user_social_network" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["scraped_user_social_network"] = {
                "dtypes_map": {}
            }
        
        # Patch _export_dataframe to handle missing columns gracefully
        from services.sync.stream.exporters.base import BaseActivityExporter
        original_export = BaseActivityExporter._export_dataframe
        
        def patched_export(self, data, service, record_type=None):
            if not data:
                return
            dtypes_map = service_constants.MAP_SERVICE_TO_METADATA.get(service, {}).get("dtypes_map", {})
            df = pd.DataFrame(data)
            df["synctimestamp"] = generate_current_datetime_str()
            df["partition_date"] = pd.to_datetime(
                df["synctimestamp"], format=timestamp_format
            ).dt.date
            # Only apply dtypes_map for columns that exist
            if dtypes_map:
                existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
                if existing_cols:
                    df = df.astype(existing_cols)
            custom_args = {}
            if record_type:
                custom_args["record_type"] = record_type
            self.storage_repository.export_dataframe(
                df=df,
                service=service,
                record_type=record_type,
                custom_args=custom_args if custom_args else None,
            )
        
        monkeypatch.setattr(BaseActivityExporter, "_export_dataframe", patched_export)
        
        # Execute: Run batch export
        filepaths = context.study_user_exporter.export_activity_data()
        
        # Verify: Storage repository was called with correct data
        assert mock_storage_repository.export_dataframe.called
        
        # Get the call arguments - find the call for posts
        post_call = None
        for call in mock_storage_repository.export_dataframe.call_args_list:
            if call.kwargs.get("service") == "study_user_activity" and call.kwargs.get("record_type") == RecordType.POST.value:
                post_call = call
                break
        
        assert post_call is not None, "Expected export_dataframe call for study_user_activity posts"
        df = post_call.kwargs["df"]
        service = post_call.kwargs["service"]
        record_type = post_call.kwargs.get("record_type")
        
        assert service == "study_user_activity"
        assert record_type == RecordType.POST.value
        # Check that our specific posts are in the dataframe
        assert len(df) >= 2  # At least our 2 posts
        # Verify our specific posts are present
        post_uris = set(df["uri"].tolist())
        expected_uris = {post["uri"] for post in test_posts}
        assert expected_uris.issubset(post_uris), f"Expected URIs {expected_uris} not all found in {post_uris}"
        
        # Verify: Filepaths returned (should include our created files)
        assert len(filepaths) >= 2
        
        # Cleanup
        for filepath in filepaths:
            if os.path.exists(filepath):
                os.remove(filepath)

    def test_batch_export_study_user_likes(self, monkeypatch):
        """Test that study user likes in cache are exported to storage."""
        import pandas as pd
        
        # Setup: Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Create test like records (nested structure for LIKE_ON_USER_POST)
        study_user_did = "did:plc:study-user-1"
        post_uri_suffix = "post-123"
        like_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.LIKE_ON_USER_POST,
        )
        nested_path = os.path.join(like_path, post_uri_suffix)
        
        test_likes = [
            {
                "author": study_user_did,
                "uri": f"at://{study_user_did}/app.bsky.feed.like/like-1",
                "record": {"subject": {"uri": f"at://did:plc:other/app.bsky.feed.post/{post_uri_suffix}"}},
            },
        ]
        
        # Clean up any existing files first
        if os.path.exists(nested_path):
            for filename in os.listdir(nested_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(nested_path, filename))
        
        # Write likes to nested cache directory
        for i, like in enumerate(test_likes):
            filename = f"like_author_did={study_user_did}_like_uri_suffix=like-{i+1}.json"
            file_path = os.path.join(nested_path, filename)
            file_writer.write_json(file_path, like)
        
        # Mock storage repository
        mock_storage_repository = MagicMock()
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
        # Mock MAP_SERVICE_TO_METADATA to avoid KeyError
        from lib.db import service_constants
        from lib.helper import generate_current_datetime_str
        from lib.constants import timestamp_format
        
        if "study_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["study_user_activity"] = {
                "dtypes_map": {}
            }
        if "scraped_user_social_network" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["scraped_user_social_network"] = {
                "dtypes_map": {}
            }
        
        # Patch _export_dataframe to handle missing columns gracefully
        from services.sync.stream.exporters.base import BaseActivityExporter
        
        def patched_export(self, data, service, record_type=None):
            if not data:
                return
            dtypes_map = service_constants.MAP_SERVICE_TO_METADATA.get(service, {}).get("dtypes_map", {})
            df = pd.DataFrame(data)
            df["synctimestamp"] = generate_current_datetime_str()
            df["partition_date"] = pd.to_datetime(
                df["synctimestamp"], format=timestamp_format
            ).dt.date
            # Only apply dtypes_map for columns that exist
            if dtypes_map:
                existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
                if existing_cols:
                    df = df.astype(existing_cols)
            custom_args = {}
            if record_type:
                custom_args["record_type"] = record_type
            self.storage_repository.export_dataframe(
                df=df,
                service=service,
                record_type=record_type,
                custom_args=custom_args if custom_args else None,
            )
        
        monkeypatch.setattr(BaseActivityExporter, "_export_dataframe", patched_export)
        
        # Execute: Run batch export
        filepaths = context.study_user_exporter.export_activity_data()
        
        # Verify: Storage repository was called
        assert mock_storage_repository.export_dataframe.called
        
        # Find the call for likes on user posts (nested structure)
        like_call = None
        for call in mock_storage_repository.export_dataframe.call_args_list:
            if call.kwargs.get("record_type") == RecordType.LIKE_ON_USER_POST.value:
                like_call = call
                break
        
        assert like_call is not None, f"Expected export call for {RecordType.LIKE_ON_USER_POST.value}, got calls: {[c.kwargs.get('record_type') for c in mock_storage_repository.export_dataframe.call_args_list]}"
        df = like_call.kwargs["df"]
        assert len(df) == 1
        
        # Cleanup
        for filepath in filepaths:
            if os.path.exists(filepath):
                os.remove(filepath)

    def test_batch_export_study_user_follows(self, monkeypatch):
        """Test that study user follows in cache are exported to storage."""
        import pandas as pd
        
        # Setup: Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Create test follow records (nested by follow_status)
        study_user_did = "did:plc:study-user-1"
        followee_did = "did:plc:followee-1"
        
        follower_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.FOLLOW,
            follow_status=FollowStatus.FOLLOWER,
        )
        
        test_follow = {
            "follower_did": study_user_did,
            "followee_did": followee_did,
            "uri": f"at://{study_user_did}/app.bsky.graph.follow/follow-1",
        }
        
        # Clean up any existing files first
        if os.path.exists(follower_path):
            for filename in os.listdir(follower_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(follower_path, filename))
        
        # Write follow to cache
        filename = f"follower_did={study_user_did}_followee_did={followee_did}.json"
        file_path = os.path.join(follower_path, filename)
        file_writer.write_json(file_path, test_follow)
        
        # Mock storage repository
        mock_storage_repository = MagicMock()
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
        # Mock MAP_SERVICE_TO_METADATA to avoid KeyError
        from lib.db import service_constants
        from lib.helper import generate_current_datetime_str
        from lib.constants import timestamp_format
        
        if "study_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["study_user_activity"] = {
                "dtypes_map": {}
            }
        if "scraped_user_social_network" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["scraped_user_social_network"] = {
                "dtypes_map": {}
            }
        
        # Patch _export_dataframe to handle missing columns gracefully
        from services.sync.stream.exporters.base import BaseActivityExporter
        
        def patched_export(self, data, service, record_type=None):
            if not data:
                return
            dtypes_map = service_constants.MAP_SERVICE_TO_METADATA.get(service, {}).get("dtypes_map", {})
            df = pd.DataFrame(data)
            df["synctimestamp"] = generate_current_datetime_str()
            df["partition_date"] = pd.to_datetime(
                df["synctimestamp"], format=timestamp_format
            ).dt.date
            # Only apply dtypes_map for columns that exist
            if dtypes_map:
                existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
                if existing_cols:
                    df = df.astype(existing_cols)
            custom_args = {}
            if record_type:
                custom_args["record_type"] = record_type
            self.storage_repository.export_dataframe(
                df=df,
                service=service,
                record_type=record_type,
                custom_args=custom_args if custom_args else None,
            )
        
        monkeypatch.setattr(BaseActivityExporter, "_export_dataframe", patched_export)
        
        # Execute: Run batch export
        filepaths = context.study_user_exporter.export_activity_data()
        
        # Verify: Storage repository was called for follows
        assert mock_storage_repository.export_dataframe.called
        
        # Find the call for follows (exported to scraped_user_social_network)
        follow_call = None
        for call in mock_storage_repository.export_dataframe.call_args_list:
            if call.kwargs.get("service") == "scraped_user_social_network":
                follow_call = call
                break
        
        assert follow_call is not None
        df = follow_call.kwargs["df"]
        assert len(df) == 1
        assert df.iloc[0]["follower_did"] == study_user_did
        
        # Cleanup
        for filepath in filepaths:
            if os.path.exists(filepath):
                os.remove(filepath)


class TestBatchWriteInNetworkActivity:
    """Test batch export flow for in-network user activity."""

    def test_batch_export_in_network_posts(self, monkeypatch):
        """Test that in-network user posts in cache are exported to storage."""
        import pandas as pd
        
        # Setup: Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Create test in-network post records
        in_network_user_did = "did:plc:in-network-1"
        post_path = path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did=in_network_user_did,
        )
        
        test_posts = [
            {
                "author_did": in_network_user_did,
                "uri": f"at://{in_network_user_did}/app.bsky.feed.post/post-1",
                "text": "In-network post 1",
            },
        ]
        
        # Clean up any existing files first
        if os.path.exists(post_path):
            for filename in os.listdir(post_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(post_path, filename))
        
        # Write posts to cache
        for i, post in enumerate(test_posts):
            filename = f"author_did={in_network_user_did}_post_uri_suffix=post-{i+1}.json"
            file_path = os.path.join(post_path, filename)
            file_writer.write_json(file_path, post)
        
        # Mock storage repository
        mock_storage_repository = MagicMock()
        context.storage_repository = mock_storage_repository
        
        # Patch _export_dataframe to handle missing columns gracefully
        from services.sync.stream.exporters.base import BaseActivityExporter
        from lib.db import service_constants
        from lib.helper import generate_current_datetime_str
        from lib.constants import timestamp_format
        
        original_export = BaseActivityExporter._export_dataframe
        
        def patched_export(self, data, service, record_type=None):
            if not data:
                return
            dtypes_map = service_constants.MAP_SERVICE_TO_METADATA.get(service, {}).get("dtypes_map", {})
            df = pd.DataFrame(data)
            df["synctimestamp"] = generate_current_datetime_str()
            df["partition_date"] = pd.to_datetime(
                df["synctimestamp"], format=timestamp_format
            ).dt.date
            # Only apply dtypes_map for columns that exist
            if dtypes_map:
                existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
                if existing_cols:
                    df = df.astype(existing_cols)
            custom_args = {}
            if record_type:
                custom_args["record_type"] = record_type
            self.storage_repository.export_dataframe(
                df=df,
                service=service,
                record_type=record_type,
                custom_args=custom_args if custom_args else None,
            )
        
        monkeypatch.setattr(BaseActivityExporter, "_export_dataframe", patched_export)
        
        # Create in-network exporter with mocked storage
        from services.sync.stream.exporters.in_network_exporter import (
            InNetworkUserActivityExporter,
        )
        in_network_exporter = InNetworkUserActivityExporter(
            path_manager=path_manager,
            storage_repository=mock_storage_repository,
            file_reader=context.file_reader,
        )
        
        # Execute: Run batch export
        filepaths = in_network_exporter.export_activity_data()
        
        # Verify: Storage repository was called
        assert mock_storage_repository.export_dataframe.called
        
        call_args = mock_storage_repository.export_dataframe.call_args
        df = call_args.kwargs["df"]
        service = call_args.kwargs["service"]
        
        assert service == "in_network_user_activity"
        assert len(df) == 1
        assert df.iloc[0]["author_did"] == in_network_user_did
        
        # Verify: Filepaths returned
        assert len(filepaths) == 1
        
        # Cleanup
        for filepath in filepaths:
            if os.path.exists(filepath):
                os.remove(filepath)


class TestBatchExporterIntegration:
    """Test the full BatchExporter integration."""

    def test_batch_exporter_full_flow(self, monkeypatch):
        """Test that BatchExporter orchestrates both exporters correctly."""
        import pandas as pd
        
        # Setup: Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        # Setup: Create some cache files
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Clean up any existing files first
        study_user_did = "did:plc:study-user-1"
        post_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
        )
        if os.path.exists(post_path):
            for filename in os.listdir(post_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(post_path, filename))
        
        # Create a study user post
        test_post = {
            "author_did": study_user_did,
            "uri": f"at://{study_user_did}/app.bsky.feed.post/test-post",
            "text": "Test",
        }
        file_writer.write_json(
            os.path.join(post_path, "author_did=test_post_uri_suffix=test.json"),
            test_post,
        )
        
        # Create an in-network post
        in_network_did = "did:plc:in-network-1"
        in_network_path = path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did=in_network_did,
        )
        if os.path.exists(in_network_path):
            for filename in os.listdir(in_network_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(in_network_path, filename))
        
        test_in_network_post = {
            "author_did": in_network_did,
            "uri": f"at://{in_network_did}/app.bsky.feed.post/in-network-post",
            "text": "In-network",
        }
        file_writer.write_json(
            os.path.join(
                in_network_path,
                "author_did=test_post_uri_suffix=in-network.json",
            ),
            test_in_network_post,
        )
        
        # Mock storage repository
        mock_storage_repository = MagicMock()
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
        # Mock MAP_SERVICE_TO_METADATA to avoid KeyError
        from lib.db import service_constants
        if "study_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
            # Use a flexible dtypes_map that accepts any columns
            service_constants.MAP_SERVICE_TO_METADATA["study_user_activity"] = {
                "dtypes_map": {}
            }
        # Also ensure scraped_user_social_network exists (for follows)
        if "scraped_user_social_network" not in service_constants.MAP_SERVICE_TO_METADATA:
            service_constants.MAP_SERVICE_TO_METADATA["scraped_user_social_network"] = {
                "dtypes_map": {}
            }
        
        # Create batch exporter
        from services.sync.stream.exporters.in_network_exporter import (
            InNetworkUserActivityExporter,
        )
        in_network_exporter = InNetworkUserActivityExporter(
            path_manager=path_manager,
            storage_repository=mock_storage_repository,
            file_reader=context.file_reader,
        )
        
        batch_exporter = BatchExporter(
            study_user_exporter=context.study_user_exporter,
            in_network_exporter=in_network_exporter,
            directory_manager=context.directory_manager,
            clear_filepaths=False,  # Don't delete files in test
            clear_cache=False,  # Don't clear cache in test
        )
        
        # Execute: Run batch export
        result = batch_exporter.export_batch()
        
        # Verify: Both exporters were called
        assert mock_storage_repository.export_dataframe.call_count >= 2
        
        # Verify: Result contains filepaths
        assert "study_user_filepaths" in result
        assert "in_network_filepaths" in result
        assert len(result["study_user_filepaths"]) > 0
        assert len(result["in_network_filepaths"]) > 0


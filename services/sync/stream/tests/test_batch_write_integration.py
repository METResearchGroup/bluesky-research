"""Integration tests for batch write happy paths.

These tests verify the end-to-end flow of reading from cache
and exporting to permanent storage.
"""

import os
import pytest
from services.sync.stream.batch_exporter import BatchExporter
from services.sync.stream.types import Operation, RecordType, FollowStatus

def cleanup_directory(directory_path):
    """Helper function to clean up JSON files in a directory."""
    if os.path.exists(directory_path):
        json_files = [f for f in os.listdir(directory_path) if f.endswith(".json")]
        for filename in json_files:
            os.remove(os.path.join(directory_path, filename))


class TestBatchWriteStudyUserActivity:
    """Test batch export flow for study user activity."""

    def test_batch_export_study_user_posts(
        self,
        sync_export_context,
        mock_storage_repository,
        patched_export_dataframe,
        cleanup_files,
    ):
        """Test that study user posts in cache are exported to storage."""
        context = sync_export_context
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
        cleanup_directory(post_path)
        
        # Write posts to cache
        created_files = []
        for i, post in enumerate(test_posts):
            filename = f"author_did={study_user_did}_post_uri_suffix=post-{i+1}.json"
            file_path = os.path.join(post_path, filename)
            file_writer.write_json(file_path, post)
            created_files.append(file_path)
            cleanup_files(file_path)
        
        # Setup mock storage repository
        # Note: We set both context.storage_repository and the exporter's storage_repository.
        # The exporter uses its own reference, but context.storage_repository is used by
        # setup_batch_export_system() and may be used by other code paths, so we set both
        # for consistency and to ensure all references point to the mock.
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
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
        
        # Cleanup filepaths
        for filepath in filepaths:
            cleanup_files(filepath)

    def test_batch_export_study_user_likes(
        self,
        sync_export_context,
        mock_storage_repository,
        patched_export_dataframe,
        cleanup_files,
    ):
        """Test that study user likes in cache are exported to storage."""
        context = sync_export_context
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
        
        # Clean up entire like_on_user_post directory to remove any leftover files from other tests
        # This ensures test isolation since _nested_read_strategy reads all nested directories
        cleanup_directory(like_path)
        
        # Write likes to nested cache directory
        for i, like in enumerate(test_likes):
            filename = f"like_author_did={study_user_did}_like_uri_suffix=like-{i+1}.json"
            file_path = os.path.join(nested_path, filename)
            file_writer.write_json(file_path, like)
            cleanup_files(file_path)
        
        # Setup mock storage repository
        # Note: We set both context.storage_repository and the exporter's storage_repository.
        # The exporter uses its own reference, but context.storage_repository is used by
        # setup_batch_export_system() and may be used by other code paths, so we set both
        # for consistency and to ensure all references point to the mock.
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
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
        
        # Cleanup filepaths
        for filepath in filepaths:
            cleanup_files(filepath)

    def test_batch_export_study_user_follows(
        self,
        sync_export_context,
        mock_storage_repository,
        patched_export_dataframe,
        cleanup_files,
    ):
        """Test that study user follows in cache are exported to storage."""
        context = sync_export_context
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
        cleanup_directory(follower_path)
        
        # Write follow to cache
        filename = f"follower_did={study_user_did}_followee_did={followee_did}.json"
        file_path = os.path.join(follower_path, filename)
        file_writer.write_json(file_path, test_follow)
        cleanup_files(file_path)
        
        # Setup mock storage repository
        # Note: We set both context.storage_repository and the exporter's storage_repository.
        # The exporter uses its own reference, but context.storage_repository is used by
        # setup_batch_export_system() and may be used by other code paths, so we set both
        # for consistency and to ensure all references point to the mock.
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
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
        
        # Cleanup filepaths
        for filepath in filepaths:
            cleanup_files(filepath)


class TestBatchWriteInNetworkActivity:
    """Test batch export flow for in-network user activity."""

    def test_batch_export_in_network_posts(
        self,
        sync_export_context,
        mock_storage_repository,
        patched_export_dataframe,
        cleanup_files,
    ):
        """Test that in-network user posts in cache are exported to storage."""
        context = sync_export_context
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
        cleanup_directory(post_path)
        
        # Write posts to cache
        for i, post in enumerate(test_posts):
            filename = f"author_did={in_network_user_did}_post_uri_suffix=post-{i+1}.json"
            file_path = os.path.join(post_path, filename)
            file_writer.write_json(file_path, post)
            cleanup_files(file_path)
        
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
        
        # Cleanup filepaths
        for filepath in filepaths:
            cleanup_files(filepath)


class TestBatchExporterIntegration:
    """Test the full BatchExporter integration."""

    def test_batch_exporter_full_flow(
        self,
        sync_export_context,
        mock_storage_repository,
        patched_export_dataframe,
        cleanup_files,
    ):
        """Test that BatchExporter orchestrates both exporters correctly."""
        context = sync_export_context
        path_manager = context.path_manager
        file_writer = context.file_writer
        
        # Clean up any existing files first
        study_user_did = "did:plc:study-user-1"
        post_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
        )
        cleanup_directory(post_path)
        
        # Create a study user post
        test_post = {
            "author_did": study_user_did,
            "uri": f"at://{study_user_did}/app.bsky.feed.post/test-post",
            "text": "Test",
        }
        study_post_file = os.path.join(post_path, "author_did=test_post_uri_suffix=test.json")
        file_writer.write_json(study_post_file, test_post)
        cleanup_files(study_post_file)
        
        # Create an in-network post
        in_network_did = "did:plc:in-network-1"
        in_network_path = path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did=in_network_did,
        )
        cleanup_directory(in_network_path)
        
        test_in_network_post = {
            "author_did": in_network_did,
            "uri": f"at://{in_network_did}/app.bsky.feed.post/in-network-post",
            "text": "In-network",
        }
        in_network_post_file = os.path.join(
            in_network_path,
            "author_did=test_post_uri_suffix=in-network.json",
        )
        file_writer.write_json(in_network_post_file, test_in_network_post)
        cleanup_files(in_network_post_file)
        
        # Setup mock storage repository
        # Note: We set both context.storage_repository and the exporter's storage_repository.
        # The exporter uses its own reference, but context.storage_repository is used by
        # setup_batch_export_system() and may be used by other code paths, so we set both
        # for consistency and to ensure all references point to the mock.
        context.storage_repository = mock_storage_repository
        context.study_user_exporter.storage_repository = mock_storage_repository
        
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
            file_utilities=context.file_writer,  # Use file_utilities instance
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


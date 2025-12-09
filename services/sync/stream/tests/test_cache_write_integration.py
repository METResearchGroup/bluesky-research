"""Integration tests for cache write happy paths.

These tests verify the end-to-end flow of writing records to cache
when records arrive from the firehose.
"""

import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock

from services.sync.stream.data_filter import (
    manage_post,
    manage_like,
    manage_follow,
)
from services.sync.stream.types import Operation, RecordType
from services.sync.stream.cache_management import CachePathManager
from services.sync.stream.tests.conftest import (
    sync_export_context,
    clean_path,
    mock_post_records_fixture,
    mock_like_records_fixture,
    mock_follow_records_fixture,
)


class TestCacheWriteStudyUserPost:
    """Test cache write flow for study user posts."""

    def test_study_user_post_creates_cache_file(
        self, mock_post_records_fixture
    ):
        """Test that a study user post is written to the correct cache location."""
        from services.sync.stream.setup import setup_sync_export_system
        
        # Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = CachePathManager()
        
        # Setup: Ensure the author is a study user
        post_record = mock_post_records_fixture[0]
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)
        
        # Modify post record to have study user as author
        post_record["author"] = study_user_did
        
        # Expected file path
        post_uri_suffix = post_record["uri"].split("/")[-1]
        expected_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
        )
        expected_file = os.path.join(
            expected_path,
            f"author_did={study_user_did}_post_uri_suffix={post_uri_suffix}.json"
        )
        
        # Execute: Write post to cache
        manage_post(
            post=post_record,
            operation=Operation.CREATE,
            context=context,
        )
        
        # Verify: File exists and contains correct data
        assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
        
        with open(expected_file, "r") as f:
            data = json.load(f)
            assert data["author_did"] == study_user_did
            assert post_uri_suffix in data["uri"]
        
        # Cleanup
        clean_path(expected_file)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)


class TestCacheWriteLikeOnStudyUserPost:
    """Test cache write flow for likes on study user posts."""

    def test_like_on_study_user_post_creates_nested_cache_file(
        self, mock_like_records_fixture
    ):
        """Test that a like on a study user's post is written to nested cache location."""
        from services.sync.stream.setup import setup_sync_export_system
        
        # Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = CachePathManager()
        
        # Setup: Create a study user post first, then like it
        study_user_did = "did:plc:study-user-2"
        liked_post_uri = "at://did:plc:study-user-2/app.bsky.feed.post/post-uri-123"
        post_uri_suffix = "post-uri-123"
        
        context.study_user_manager.study_users_dids_set.add(study_user_did)
        context.study_user_manager.post_uri_to_study_user_did_map[
            liked_post_uri
        ] = study_user_did
        
        # Get like record and create a new one with the modified subject URI
        from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef
        from atproto_client.models.app.bsky.feed.like import Record as LikeRecord
        
        original_like = mock_like_records_fixture[0]
        # Create a new LikeRecord with the modified subject
        new_subject = StrongRef(
            cid=original_like["record"].subject.cid,
            uri=liked_post_uri,
            py_type=original_like["record"].subject.py_type
        )
        new_record = LikeRecord(
            created_at=original_like["record"].created_at,
            subject=new_subject,
            py_type=original_like["record"].py_type
        )
        like_record = {
            "author": original_like["author"],
            "cid": original_like["cid"],
            "record": new_record,
            "uri": original_like["uri"]
        }
        
        # Expected nested file path
        base_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.LIKE_ON_USER_POST,
        )
        expected_file = os.path.join(
            base_path,
            post_uri_suffix,
            f"like_author_did={like_record['author']}_like_uri_suffix={like_record['uri'].split('/')[-1]}.json"
        )
        
        # Execute: Write like to cache
        manage_like(
            like=like_record,
            operation=Operation.CREATE,
            context=context,
        )
        
        # Verify: File exists in nested directory
        assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
        
        with open(expected_file, "r") as f:
            data = json.load(f)
            assert liked_post_uri in str(data)
        
        # Cleanup
        clean_path(expected_file)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)
        del context.study_user_manager.post_uri_to_study_user_did_map[
            liked_post_uri
        ]


class TestCacheWriteFollow:
    """Test cache write flow for follow records."""

    def test_study_user_follow_creates_cache_file_in_follower_directory(
        self, mock_follow_records_fixture
    ):
        """Test that a follow where study user is the follower creates file in follower/ directory."""
        from services.sync.stream.setup import setup_sync_export_system
        
        # Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = CachePathManager()
        
        # Setup: Study user is the follower
        # Use the follow record where study user is the follower (index 3)
        follow_record = mock_follow_records_fixture[3]
        study_user_did = follow_record["author"]  # Follower
        followee_did = follow_record["record"].subject  # Followee (subject is a string, not a dict)
        
        context.study_user_manager.study_users_dids_set.add(study_user_did)
        
        # Expected file path in follower directory
        from services.sync.stream.types import FollowStatus
        base_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.FOLLOW,
            follow_status=FollowStatus.FOLLOWER,
        )
        expected_file = os.path.join(
            base_path,
            f"follower_did={study_user_did}_followee_did={followee_did}.json"
        )
        
        # Execute: Write follow to cache
        manage_follow(
            follow=follow_record,
            operation=Operation.CREATE,
            context=context,
        )
        
        # Verify: File exists in follower directory
        assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
        
        with open(expected_file, "r") as f:
            data = json.load(f)
            assert data["follower_did"] == study_user_did
            assert data["followee_did"] == followee_did
        
        # Cleanup
        clean_path(expected_file)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)


class TestCacheWriteInNetworkPost:
    """Test cache write flow for in-network user posts."""

    def test_in_network_post_creates_cache_file_in_author_directory(
        self, mock_post_records_fixture
    ):
        """Test that an in-network user post is written to author-specific cache location."""
        from services.sync.stream.setup import setup_sync_export_system
        
        # Create context with mocked study user manager (patched by autouse fixture)
        context = setup_sync_export_system()
        
        path_manager = CachePathManager()
        
        # Setup: Author is an in-network user
        post_record = mock_post_records_fixture[0]
        in_network_user_did = "did:plc:in-network-user-1"
        post_record["author"] = in_network_user_did
        
        context.study_user_manager.in_network_user_dids_set.add(
            in_network_user_did
        )
        
        # Expected file path in author directory
        post_uri_suffix = post_record["uri"].split("/")[-1]
        expected_path = path_manager.get_in_network_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
            author_did=in_network_user_did,
        )
        expected_file = os.path.join(
            expected_path,
            f"author_did={in_network_user_did}_post_uri_suffix={post_uri_suffix}.json"
        )
        
        # Execute: Write post to cache
        manage_post(
            post=post_record,
            operation=Operation.CREATE,
            context=context,
        )
        
        # Verify: File exists in author directory
        assert os.path.exists(expected_file), f"Expected file not found: {expected_file}"
        
        with open(expected_file, "r") as f:
            data = json.load(f)
            assert data["author_did"] == in_network_user_did
        
        # Cleanup
        clean_path(expected_file)
        context.study_user_manager.in_network_user_dids_set.remove(
            in_network_user_did
        )


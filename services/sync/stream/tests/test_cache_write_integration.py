"""Integration tests for cache write happy paths.

These tests verify the end-to-end flow of writing records to cache
when records arrive from the firehose.
"""

import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock

from services.sync.stream.record_processors.router import route_decisions
from services.sync.stream.core.types import Operation, RecordType


def process_record(context, record_type: str, record: dict, operation: Operation):
    """Helper to process a record through the processor pipeline.
    
    Args:
        context: Cache write context
        record_type: Type of record ("posts", "likes", "follows")
        record: Raw firehose record dictionary
        operation: Operation type (CREATE or DELETE)
    
    Returns:
        Tuple of (transformed record, routing decisions)
    """
    processor_registry = context.processor_registry
    processor = processor_registry.get_processor(record_type)
    
    transformed = processor.transform(record, operation)
    decisions = processor.get_routing_decisions(transformed, operation, context)
    route_decisions(decisions, transformed, operation, context)
    return transformed, decisions
from services.sync.stream.tests.conftest import (
    cache_write_context,
    clean_path,
    mock_post_records_fixture,
    mock_like_records_fixture,
    mock_follow_records_fixture,
    path_manager,
    cleanup_files,
)

def _json_files_in_dir(directory: str) -> set[str]:
    if not os.path.exists(directory):
        return set()
    return {
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.endswith(".json")
    }


def _load_records_from_json_file(path: str) -> list[dict]:
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        return [data]
    return []


class TestCacheWriteStudyUserPost:
    """Test cache write flow for study user posts."""

    def test_study_user_post_creates_cache_file(
        self, mock_post_records_fixture, cache_write_context, path_manager, cleanup_files
    ):
        """Test that a study user post is written to the correct cache location.

        Cache writes are batched, so we assert a new batch file appears and
        contains our record.
        """
        context = cache_write_context
        
        # Setup: Ensure the author is a study user
        post_record = mock_post_records_fixture[0]
        study_user_did = "did:plc:study-user-1"
        context.study_user_manager.study_users_dids_set.add(study_user_did)
        
        # Modify post record to have study user as author
        post_record["author"] = study_user_did
        
        # Expected directory path
        post_uri_suffix = post_record["uri"].split("/")[-1]
        expected_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.POST,
        )
        before_files = _json_files_in_dir(expected_path)
        
        # Execute: Write post to cache
        process_record(context, "posts", post_record, Operation.CREATE)
        # Force flush to make batching deterministic in tests
        context.file_utilities.flush_batches()
        
        after_files = _json_files_in_dir(expected_path)
        created_files = sorted(after_files - before_files)
        assert created_files, f"Expected at least one new batch file in: {expected_path}"

        # Verify: At least one created file contains our record
        found = False
        for fp in created_files:
            for rec in _load_records_from_json_file(fp):
                if rec.get("author_did") == study_user_did and post_uri_suffix in str(
                    rec.get("uri", "")
                ):
                    found = True
                    break
            if found:
                break
        assert found, "Did not find expected post record in created batch files"
        
        # Cleanup
        for fp in created_files:
            cleanup_files(fp)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)


class TestCacheWriteLikeOnStudyUserPost:
    """Test cache write flow for likes on study user posts."""

    def test_like_on_study_user_post_creates_nested_cache_file(
        self, mock_like_records_fixture, cache_write_context, path_manager, cleanup_files
    ):
        """Test that a like on a study user's post is written to nested cache location.

        Cache writes are batched, so we assert a new batch file appears in the
        nested directory and contains our record.
        """
        context = cache_write_context
        
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
        nested_dir = os.path.join(base_path, post_uri_suffix)
        before_files = _json_files_in_dir(nested_dir)
        
        # Execute: Write like to cache
        process_record(context, "likes", like_record, Operation.CREATE)
        # Force flush to make batching deterministic in tests
        context.file_utilities.flush_batches()
        
        after_files = _json_files_in_dir(nested_dir)
        created_files = sorted(after_files - before_files)
        assert created_files, f"Expected at least one new batch file in: {nested_dir}"

        found = False
        for fp in created_files:
            for rec in _load_records_from_json_file(fp):
                if liked_post_uri in str(rec):
                    found = True
                    break
            if found:
                break
        assert found, "Did not find expected like record in created batch files"
        
        # Cleanup
        for fp in created_files:
            cleanup_files(fp)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)
        del context.study_user_manager.post_uri_to_study_user_did_map[
            liked_post_uri
        ]


class TestCacheWriteFollow:
    """Test cache write flow for follow records."""

    def test_study_user_follow_creates_cache_file_in_follower_directory(
        self, mock_follow_records_fixture, cache_write_context, path_manager, cleanup_files
    ):
        """Test that a follow where study user is the follower is written in follower/ directory.

        Cache writes are batched, so we assert a new batch file appears and
        contains our follow record.
        """
        context = cache_write_context
        
        # Setup: Study user is the follower
        # Use the follow record where study user is the follower (index 3)
        follow_record = mock_follow_records_fixture[3]
        study_user_did = follow_record["author"]  # Follower
        followee_did = follow_record["record"].subject  # Followee (subject is a string, not a dict)
        
        context.study_user_manager.study_users_dids_set.add(study_user_did)
        
        # Expected file path in follower directory
        from services.sync.stream.core.types import FollowStatus
        base_path = path_manager.get_study_user_activity_path(
            operation=Operation.CREATE,
            record_type=RecordType.FOLLOW,
            follow_status=FollowStatus.FOLLOWER,
        )
        before_files = _json_files_in_dir(base_path)
        
        # Execute: Write follow to cache
        process_record(context, "follows", follow_record, Operation.CREATE)
        # Force flush to make batching deterministic in tests
        context.file_utilities.flush_batches()
        
        after_files = _json_files_in_dir(base_path)
        created_files = sorted(after_files - before_files)
        assert created_files, f"Expected at least one new batch file in: {base_path}"

        found = False
        for fp in created_files:
            for rec in _load_records_from_json_file(fp):
                if rec.get("follower_did") == study_user_did and rec.get(
                    "followee_did"
                ) == followee_did:
                    found = True
                    break
            if found:
                break
        assert found, "Did not find expected follow record in created batch files"
        
        # Cleanup
        for fp in created_files:
            cleanup_files(fp)
        context.study_user_manager.study_users_dids_set.remove(study_user_did)


class TestCacheWriteInNetworkPost:
    """Test cache write flow for in-network user posts."""

    def test_in_network_post_creates_cache_file_in_author_directory(
        self, mock_post_records_fixture, cache_write_context, path_manager, cleanup_files
    ):
        """Test that an in-network user post is written to author-specific cache location.

        Cache writes are batched, so we assert a new batch file appears in the
        author directory and contains our record.
        """
        context = cache_write_context
        
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
        before_files = _json_files_in_dir(expected_path)
        
        # Execute: Write post to cache
        process_record(context, "posts", post_record, Operation.CREATE)
        # Force flush to make batching deterministic in tests
        context.file_utilities.flush_batches()
        
        after_files = _json_files_in_dir(expected_path)
        created_files = sorted(after_files - before_files)
        assert created_files, f"Expected at least one new batch file in: {expected_path}"

        found = False
        for fp in created_files:
            for rec in _load_records_from_json_file(fp):
                if rec.get("author_did") == in_network_user_did:
                    found = True
                    break
            if found:
                break
        assert found, "Did not find expected in-network post record in created batch files"
        
        # Cleanup
        for fp in created_files:
            cleanup_files(fp)
        context.study_user_manager.in_network_user_dids_set.remove(
            in_network_user_did
        )


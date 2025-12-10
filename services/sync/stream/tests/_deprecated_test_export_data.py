"""Tests for export_data.py."""
import os

from services.sync.stream.data_filter import operations_callback
from services.sync.stream.export_data import (
    export_batch, export_raw_sync_local_data, root_write_path
)
from services.sync.stream.tests.conftest import (
    clean_path, mock_follow_records_fixture, mock_like_records_fixture,
    mock_post_records_fixture, mock_s3_fixture, mock_study_user_manager,
    mock_s3
)


class TestExportStudyUserActivityLocalData:
    """Tests the export_raw_sync_local_data function.

    Steps:
    1. Takes the mock data and writes it locally using the same logic as
    what runs in the firehose (i.e., using `operations_callback` to run
    `manage_posts`, `manage_likes`, and `manage_follows`).
    2. Calls `export_raw_sync_local_data` on the exported data.
    3. Stubs the S3 call and checks to see if the correct data is uploaded.
    """

    def test_export_raw_sync_local_data(
        self, mock_follow_records_fixture, mock_like_records_fixture,
        mock_post_records_fixture, mock_s3_fixture, mock_study_user_manager
    ):
        # skip deleted records. We test those in individual unit tests
        # but for e2e/integration tests we care that 'create' is implemented
        # correctly.
        operations_by_type = {
            "posts": {
                # skip the second post, which is the deleted post
                "created": [
                    post for idx, post in enumerate(mock_post_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "likes": {
                # skip the second like, which is the deleted like
                "created": [
                    like for idx, like in enumerate(mock_like_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "follows": {
                # skip the second follow, which is the deleted follow
                "created": [
                    follow for idx, follow
                    in enumerate(mock_follow_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "reposts": {
                "created": [],
                "deleted": []
            }
        }
        operations_callback(operations_by_type)

        # check the stubbed writes to s3 and see what the keys were that
        # were written. Check that they match what we'd expect
        export_raw_sync_local_data()

        actual_keys: set[str] = {
            call_args.kwargs['key']
            for call_args
            in mock_s3.write_dict_json_to_s3.call_args_list
        }
        expected_keys = set([
            ### did:plc:study-user-1 ###
            ## follows ##
            "raw_sync/did:plc:study-user-1/create/follow/followee/follower_did=did:plc:generic-user-1_followee_did=did:plc:study-user-1.json",
            "raw_sync/did:plc:study-user-1/create/follow/follower/follower_did=did:plc:study-user-1_followee_did=did:plc:generic-user-1.json",
            ## likes ##
            "raw_sync/did:plc:study-user-1/create/like/generic-post-uri-1/like_author_did=did:plc:study-user-1_like_uri_suffix=like-record-suffix-456.json",
            ## post ##
            "raw_sync/did:plc:study-user-1/create/post/author_did=did:plc:study-user-1_post_uri_suffix=post-uri-1.json",
            ## reply_to_user_post ##
            "raw_sync/did:plc:study-user-1/create/reply_to_user_post/post-uri-1/author_did=did:plc:generic-user-1_post_uri_suffix=generic-post-uri-1.json",
            ### did:plc:study-user-2 ###
            ## like_on_user_post ##
            "raw_sync/did:plc:study-user-2/create/like_on_user_post/post-uri-2/like_author_did=did:plc:generic-user-1_like_uri_suffix=like-record-suffix-789.json"
        ])
        assert actual_keys == expected_keys

        # clear paths
        paths = [os.path.join(root_write_path, key) for key in actual_keys]
        for path in paths:
            clean_path(path)


class TestExportBatch():
    """Tests the `export_batch` function.

    This function exports both the general firehose data and the
    study user activity data. This test checks that the function
    calls the correct functions and that the correct data is uploaded
    to S3.
    """

    def test_export_batch(
        self, mock_follow_records_fixture, mock_like_records_fixture,
        mock_post_records_fixture, mock_s3_fixture, mock_study_user_manager
    ):
        # skip deleted records. We test those in individual unit tests
        # but for e2e/integration tests we care that 'create' is implemented
        # correctly.
        operations_by_type = {
            "posts": {
                # skip the second post, which is the deleted post
                "created": [
                    post for idx, post in enumerate(mock_post_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "likes": {
                # skip the second like, which is the deleted like
                "created": [
                    like for idx, like in enumerate(mock_like_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "follows": {
                # skip the second follow, which is the deleted follow
                "created": [
                    follow for idx, follow
                    in enumerate(mock_follow_records_fixture)
                    if idx != 1
                ],
                "deleted": []
            },
            "reposts": {
                "created": [],
                "deleted": []
            }
        }
        operations_callback(operations_by_type)
        export_batch(compressed=True, clear_cache=True, external_store=["s3"])
        ### Test 1: check that functions are called with the correct args. ###

        # Test 1a: check that `write_local_jsons_to_s3`, which is used
        # to export the general firehose posts, is called with the
        # correct arguments.

        # for all created posts/follows/likes and for all deleted posts/follows/likes,
        # there should be a key. We write a separate compressed file for each
        # created and deleted set of post/follow/like, so we should have
        # 6 keys for 6 writes.
        expected_general_firehose_keys: set[str] = set([
            "sync/firehose/create/post/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl",
            "sync/firehose/create/like/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl",
            "sync/firehose/create/follow/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl",
            "sync/firehose/delete/post/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl",
            "sync/firehose/delete/like/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl",
            "sync/firehose/delete/follow/year=2024/month=08/day=01/hour=20/minute=39/2024-08-01-20:39:38.jsonl"
        ])

        actual_general_firehose_keys = {
            call_args.kwargs['key']
            for call_args
            in mock_s3.write_local_jsons_to_s3.call_args_list
        }
        assert actual_general_firehose_keys == expected_general_firehose_keys

        # Test 1b: check that `write_dict_json_to_s3`, which is used to
        # export the study user activity data, is called with the correct
        # arguments.
        expected_raw_sync_keys = set([
            ### did:plc:study-user-1 ###
            ## follows ##
            "raw_sync/did:plc:study-user-1/create/follow/followee/follower_did=did:plc:generic-user-1_followee_did=did:plc:study-user-1.json",
            "raw_sync/did:plc:study-user-1/create/follow/follower/follower_did=did:plc:study-user-1_followee_did=did:plc:generic-user-1.json",
            ## likes ##
            "raw_sync/did:plc:study-user-1/create/like/generic-post-uri-1/like_author_did=did:plc:study-user-1_like_uri_suffix=like-record-suffix-456.json",
            ## post ##
            "raw_sync/did:plc:study-user-1/create/post/author_did=did:plc:study-user-1_post_uri_suffix=post-uri-1.json",
            ## reply_to_user_post ##
            "raw_sync/did:plc:study-user-1/create/reply_to_user_post/post-uri-1/author_did=did:plc:generic-user-1_post_uri_suffix=generic-post-uri-1.json",
            ### did:plc:study-user-2 ###
            ## like_on_user_post ##
            "raw_sync/did:plc:study-user-2/create/like_on_user_post/post-uri-2/like_author_did=did:plc:generic-user-1_like_uri_suffix=like-record-suffix-789.json"
        ])
        actual_raw_sync_keys: set[str] = {
            call_args.kwargs['key']
            for call_args
            in mock_s3.write_dict_json_to_s3.call_args_list
        }
        assert actual_raw_sync_keys == expected_raw_sync_keys  # noqa

        ### Test 2: check that there are no more files in the cache. ###
        remaining_files = []
        for _, _, files in os.walk(root_write_path):
            if files:
                remaining_files.extend(files)
        assert not remaining_files

        ### Test 3: check that the folders are rebuilt. ###
        assert os.path.exists(root_write_path)
        assert os.path.exists(os.path.join(root_write_path, "create"))
        assert os.path.exists(os.path.join(root_write_path, "create", "post"))
        assert os.path.exists(os.path.join(root_write_path, "create", "like"))
        assert os.path.exists(os.path.join(root_write_path, "create", "follow"))  # noqa
        assert os.path.exists(os.path.join(root_write_path, "delete"))
        assert os.path.exists(os.path.join(root_write_path, "delete", "post"))
        assert os.path.exists(os.path.join(root_write_path, "delete", "like"))
        assert os.path.exists(os.path.join(root_write_path, "delete", "follow"))  # noqa
        assert os.path.exists(os.path.join(root_write_path, "raw_sync"))  # noqa

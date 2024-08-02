"""Tests for export_data.py."""
import os

import pytest

from services.sync.stream.data_filter import operations_callback
from services.sync.stream.export_data import export_study_user_activity_local_data  # noqa
from services.sync.stream.tests.conftest import (
    mock_follow_records_fixture, mock_like_records_fixture,
    mock_post_records_fixture, mock_s3_fixture, mock_study_user_manager,
    mock_s3
)

# TODO: write a function to clean up all study activity data files
# (I need this generally, but I can also use it here as well).
# TODO: this should fit in the functionality of clearing all the cache files,
# and I can run that anyways.
# TODO: I should also create a unit test for the general firehose data as well?


class TestExportStudyUserActivityLocalData:
    """Tests the export_study_user_activity_local_data function.

    Steps:
    1. Takes the mock data and writes it locally using the same logic as
    what runs in the firehose (i.e., using `operations_callback` to run
    `manage_posts`, `manage_likes`, and `manage_follows`).
    2. Calls `export_study_user_activity_local_data` on the exported data.
    3. Stubs the S3 call and checks to see if the correct data is uploaded.
    """

    def test_export_study_user_activity_local_data(
        self, mock_follow_records_fixture, mock_like_records_fixture,
        mock_post_records_fixture, mock_s3_fixture, mock_study_user_manager
    ):
        operations_by_type = {
            "posts": {"created": mock_post_records_fixture, "deleted": []},
            "likes": {"created": mock_like_records_fixture, "deleted": []},
            "follows": {"created": mock_follow_records_fixture, "deleted": []}
        }
        operations_callback(operations_by_type)

        # check the stubbed writes to s3 and see what the keys were that
        # were written. Check that they match what we'd expect
        export_study_user_activity_local_data()

        actual_keys: set[str] = {
            call_args.kwargs['key']
            for call_args
            in mock_s3.write_dict_json_to_s3.call_args_list
        }
        expected_keys = set([
            ### did:plc:study-user-1 ###
            ## follows ##
            "study_user_activity/did:plc:study-user-1/create/follow/followee/follower_did=did:plc:generic-user-1_followee_did=did:plc:study-user-1.json",
            "study_user_activity/did:plc:study-user-1/create/follow/follower/follower_did=did:plc:study-user-1_followee_did=did:plc:generic-user-1.json",
            ## likes ##
            "study_user_activity/did:plc:study-user-1/create/like/generic-post-uri-1/like_author_did=did:plc:study-user-1_like_uri_suffix=like-record-suffix-456.json",
            ## post ##
            "study_user_activity/did:plc:study-user-1/create/post/author_did=did:plc:study-user-1_post_uri_suffix=post-uri-1.json",
            ## reply_to_user_post ##
            "study_user_activity/did:plc:study-user-1/create/reply_to_user_post/post-uri-1/author_did=did:plc:generic-user-1_post_uri_suffix=generic-post-uri-1.json",
            ### did:plc:study-user-2 ###
            ## like_on_user_post ##
            "study_user_activity/did:plc:study-user-2/create/like_on_user_post/post-uri-2/like_author_did=did:plc:generic-user-1_like_uri_suffix=like-record-suffix-789.json"
        ])
        assert actual_keys == expected_keys

        # TODO: clear out the files that were written to the local directory

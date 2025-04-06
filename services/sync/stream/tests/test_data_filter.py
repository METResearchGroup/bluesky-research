"""Tests for data_filter.py."""
import os
from unittest.mock import patch

from services.sync.stream.data_filter import (
    manage_follow, manage_follows, manage_like, manage_likes, manage_post,
    manage_posts
)
from services.sync.stream.export_data import (
    raw_sync_relative_path_map, raw_sync_relative_path_map,
    raw_sync_root_local_path
)
from services.sync.stream.tests.conftest import (
    clean_path, mock_study_user_manager,
    mock_logger_fixture, mock_follow_records_fixture,
    mock_like_records_fixture, mock_post_records_fixture
)


class TestManageFollow:
    """Tests for manage_follow.

    Checks that the files are written locally as intended, and then deletes them.
    """

    # NOTE: commenting out for now since we're only exporting in-network follows
    # (though we'll revisit this later when refactoring the firehose).
    # def test_manage_follow_create_default(self, mock_study_user_manager, mock_follow_records_fixture):
    #     """Test manage_follow for 'create' operation (for a generic follow record)."""  # noqa
    #     relative_filepath = raw_sync_relative_path_map["create"]["follow"]
    #     mock_follower_did = "did:plc:generic-user-2"
    #     mock_followee_did = "did:plc:generic-user-1"
    #     expected_filename = f"follower_did={mock_follower_did}_followee_did={mock_followee_did}.json"  # noqa
    #     expected_filepath = os.path.join(relative_filepath, expected_filename)

    #     manage_follow(follow=mock_follow_records_fixture[0], operation="create")
    #     assert os.path.exists(expected_filepath)
    #     clean_path(expected_filepath)

    # current behavior is to not write follows that are not in-network, so let's
    # verify that this is the case.
    def test_manage_follow_create_default(
        self,
        mock_study_user_manager,
        mock_follow_records_fixture,
        mock_logger_fixture
    ):
        """Test manage_follow for 'create' operation (for a generic follow record)."""  # noqa
        with patch("services.sync.stream.data_filter.logger", mock_logger_fixture):
            relative_filepath = raw_sync_relative_path_map["create"]["follow"]["followee"] # could be either followee/follower, the idea is this errors out.
            mock_follower_did = "did:plc:generic-user-2"
            mock_followee_did = "did:plc:generic-user-1"
            expected_filename = f"follower_did={mock_follower_did}_followee_did={mock_followee_did}.json"  # noqa
            expected_filepath = os.path.join(relative_filepath, expected_filename)

            manage_follow(follow=mock_follow_records_fixture[0], operation="create")
            all_logs = mock_logger_fixture.get_logs()
            assert not os.path.exists(expected_filepath)
            assert "User is neither follower nor followee." in all_logs

    # NOTE: commenting out for now since we're only exporting in-network follows
    # (though we'll revisit this later when refactoring the firehose).
    # def test_manage_follow_delete_default(self, mock_study_user_manager, mock_follow_records_fixture):
    #     """Test manage_follow for 'delete' operation (for a generic follow record)."""  # noqa
    #     relative_filepath = raw_sync_relative_path_map["delete"]["follow"]
    #     mock_follow_uri_suffix = "random-hash"
    #     expected_filename = f"follow_uri_suffix={mock_follow_uri_suffix}.json"
    #     expected_filepath = os.path.join(relative_filepath, expected_filename)

    #     manage_follow(follow=mock_follow_records_fixture[1], operation="delete")
    #     assert os.path.exists(expected_filepath)
    #     clean_path(expected_filepath)

    def test_manage_follow_delete_default(
        self,
        mock_study_user_manager,
        mock_follow_records_fixture,
        mock_logger_fixture
    ):
        """Test manage_follow for 'delete' operation (for a generic follow record)."""  # noqa
        with patch("services.sync.stream.data_filter.logger", mock_logger_fixture):
            relative_filepath = raw_sync_relative_path_map["create"]["follow"]["followee"] # could be either followee/follower, the idea is this errors out.
            mock_follow_uri_suffix = "random-hash"
            expected_filename = f"follow_uri_suffix={mock_follow_uri_suffix}.json"
            expected_filepath = os.path.join(relative_filepath, expected_filename)

            manage_follow(follow=mock_follow_records_fixture[1], operation="delete")
            all_logs = mock_logger_fixture.get_logs()
            assert not os.path.exists(expected_filepath)
            assert len(all_logs) == 0  # no logs to report, since deletes are skipped.


    def test_study_user_is_followed(self, mock_study_user_manager, mock_follow_records_fixture):
        """Tests logic for when a study user is being followed by another account.

        Expects that one filepath is written:
        - One for the 'follow' records of study participants.
        """  # noqa
        relative_filepath_1 = raw_sync_relative_path_map["create"]["follow"]["followee"]
        mock_follower_did = "did:plc:generic-user-1"
        mock_followee_did = "did:plc:study-user-1"
        expected_filename = f"follower_did={mock_follower_did}_followee_did={mock_followee_did}.json"  # noqa
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)

        # run check.
        manage_follow(follow=mock_follow_records_fixture[2], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

    def test_study_user_is_follower(self, mock_study_user_manager, mock_follow_records_fixture):
        """Tests logic for when a study user is following another account.

        Expects that one filepath is written:
        - One for the 'follow' records of study participants.
        """
        relative_filepath_1 = raw_sync_relative_path_map["create"]["follow"]["follower"]  # noqa
        mock_follower_did = "did:plc:study-user-1"
        mock_followee_did = "did:plc:generic-user-1"
        expected_filename = f"follower_did={mock_follower_did}_followee_did={mock_followee_did}.json"  # noqa
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)

        # run check.
        manage_follow(follow=mock_follow_records_fixture[3], operation="create")

        # check that the files were written.
        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)


class TestManageFollows:
    """Tests for manage_follows."""

    def test_manage_follows(self, mock_study_user_manager, mock_follow_records_fixture):
        """Test manage_follows."""
        # we really only need to test 'created' records since this is the
        # one that's more important (and by far, more common).
        follows = {
            "created": [
                mock_follow_records_fixture[0],
                mock_follow_records_fixture[2],
                mock_follow_records_fixture[3]
            ],
            "deleted": [mock_follow_records_fixture[1]]
        }

        # we have 4 follow records, a default create and delete, and two for
        # study users.
        # we expect 6 records: 4 in the default path and 2 in the study user
        # acitvities path.
        default_relative_create_filepath = raw_sync_relative_path_map["create"]["follow"]
        default_relative_delete_filepath = raw_sync_relative_path_map["delete"]["follow"]

        study_user_relative_fp_1 = raw_sync_relative_path_map["create"]["follow"]["followee"]
        study_user_relative_fp_2 = raw_sync_relative_path_map["create"]["follow"]["follower"]

        mock_follower_did_1 = "did:plc:generic-user-2"
        mock_followee_did_1 = "did:plc:generic-user-1"

        mock_follower_did_3 = "did:plc:generic-user-1"
        mock_followee_did_3 = "did:plc:study-user-1"

        mock_follower_did_4 = "did:plc:study-user-1"
        mock_followee_did_4 = "did:plc:generic-user-1"

        deleted_file_mock_uri_suffix = "random-hash"

        expected_filename_1 = f"follower_did={mock_follower_did_1}_followee_did={mock_followee_did_1}.json"  # noqa
        expected_filename_2 = f"follow_uri_suffix={deleted_file_mock_uri_suffix}.json"  # noqa

        expected_filename_3 = f"follower_did={mock_follower_did_3}_followee_did={mock_followee_did_3}.json"  # noqa
        expected_filename_4 = f"follower_did={mock_follower_did_4}_followee_did={mock_followee_did_4}.json"  # noqa

        # default filepaths
        expected_default_filepath_1 = os.path.join(default_relative_create_filepath, expected_filename_1)
        expected_default_filepath_2 = os.path.join(default_relative_delete_filepath, expected_filename_2)
        expected_default_filepath_3 = os.path.join(default_relative_create_filepath, expected_filename_3)
        expected_default_filepath_4 = os.path.join(default_relative_create_filepath, expected_filename_4)

        # study user filepaths
        expected_study_user_filepath_1 = os.path.join(
            raw_sync_root_local_path,
            mock_followee_did_3,
            study_user_relative_fp_1,
            expected_filename_3
        )
        expected_study_user_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_follower_did_4,
            study_user_relative_fp_2,
            expected_filename_4
        )

        manage_follows(follows=follows)

        # check default path. Should be 4 files.
        assert os.path.exists(expected_default_filepath_1)
        clean_path(expected_default_filepath_1)

        assert os.path.exists(expected_default_filepath_2)
        clean_path(expected_default_filepath_2)

        assert os.path.exists(expected_default_filepath_3)
        clean_path(expected_default_filepath_3)

        assert os.path.exists(expected_default_filepath_4)
        clean_path(expected_default_filepath_4)

        # check user activities path. Should be two files.
        assert os.path.exists(expected_study_user_filepath_1)  # user is followee
        clean_path(expected_study_user_filepath_1)

        assert os.path.exists(expected_study_user_filepath_2)  # user is follower
        clean_path(expected_study_user_filepath_2)


class TestManageLike:
    """Tests for manage_like."""

    def test_manage_create_default_like(self, mock_study_user_manager, mock_like_records_fixture):
        relative_filepath = raw_sync_relative_path_map["create"]["like"]
        mock_author_did = "did:plc:generic-user-1"
        mock_uri_suffix = "like-record-suffix-123"
        expected_filename = f"like_author_did={mock_author_did}_like_uri_suffix={mock_uri_suffix}.json"
        expected_filepath = os.path.join(relative_filepath, expected_filename)

        manage_like(like=mock_like_records_fixture[0], operation="create")
        assert os.path.exists(expected_filepath)
        clean_path(expected_filepath)

    def test_manage_delete_default_like(self, mock_study_user_manager, mock_like_records_fixture):
        relative_filepath = raw_sync_relative_path_map["delete"]["like"]
        mock_uri_suffix = "like-record-suffix-123"
        expected_filename = f"like_uri_suffix={mock_uri_suffix}.json"
        expected_filepath = os.path.join(relative_filepath, expected_filename)

        manage_like(like=mock_like_records_fixture[1], operation="delete")
        assert os.path.exists(expected_filepath)
        clean_path(expected_filepath)

    def test_study_user_likes_post(self, mock_study_user_manager, mock_like_records_fixture):
        """Tests case where the study user likes a post.

        Should create two files:
        - One in the default path.
        - One in the study user activities path.
        """
        relative_filepath_1 = raw_sync_relative_path_map["create"]["like"]
        relative_filepath_2 = raw_sync_relative_path_map["create"]["like"]
        mock_author_did = "did:plc:study-user-1"
        mock_liked_post_uri_suffix = "generic-post-uri-1"
        mock_like_uri_suffix = "like-record-suffix-456"

        expected_filename = f"like_author_did={mock_author_did}_like_uri_suffix={mock_like_uri_suffix}.json"
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)
        expected_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_author_did,
            relative_filepath_2,
            mock_liked_post_uri_suffix,
            expected_filename
        )

        manage_like(like=mock_like_records_fixture[2], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

        assert os.path.exists(expected_filepath_2)
        clean_path(expected_filepath_2)

    def test_study_user_post_is_liked(self, mock_study_user_manager, mock_like_records_fixture):
        """Tests the case where a study user's post is liked."""
        relative_filepath_1 = raw_sync_relative_path_map["create"]["like"]
        relative_filepath_2 = raw_sync_relative_path_map["create"]["like_on_user_post"]
        mock_author_did = "did:plc:generic-user-1"
        mock_liked_post_author_did = "did:plc:study-user-2"
        mock_liked_post_uri_suffix = "post-uri-2"
        mock_like_uri_suffix = "like-record-suffix-789"
        expected_filename = f"like_author_did={mock_author_did}_like_uri_suffix={mock_like_uri_suffix}.json"
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)
        expected_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_liked_post_author_did,
            relative_filepath_2,
            mock_liked_post_uri_suffix,
            expected_filename
        )

        manage_like(like=mock_like_records_fixture[3], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

        assert os.path.exists(expected_filepath_2)
        clean_path(expected_filepath_2)


class TestManageLikes:
    """Tests for manage_likes."""

    def test_manage_likes(self, mock_study_user_manager, mock_like_records_fixture):
        """Test manage_likes.

        We only care about 'created' records.
        """
        # skip the deleted record.
        likes = {
            "created": [
                mock_like_records_fixture[0],
                mock_like_records_fixture[2],
                mock_like_records_fixture[3]
            ],
            "deleted": []
        }

        # we have 3 like records, one default, one where a study user likes a
        # post, and one where a post by a study user is liked.

        # we expect 3 files in the default path and 2 in the study user activities
        default_relative_filepath = raw_sync_relative_path_map["create"]["like"]

        # URIs of like records themselves (not the posts).
        like_uri_suffix_1 = "like-record-suffix-123"
        like_uri_suffix_2 = "like-record-suffix-456"
        like_uri_suffix_3 = "like-record-suffix-789"

        # expected filenames in the default paths (3)
        mock_author_did_1 = "did:plc:generic-user-1"
        mock_author_did_2 = "did:plc:study-user-1"
        mock_author_did_3 = "did:plc:generic-user-1"

        expected_default_filename_1 = f"like_author_did={mock_author_did_1}_like_uri_suffix={like_uri_suffix_1}.json"
        expected_default_filename_2 = f"like_author_did={mock_author_did_2}_like_uri_suffix={like_uri_suffix_2}.json"
        expected_default_filename_3 = f"like_author_did={mock_author_did_3}_like_uri_suffix={like_uri_suffix_3}.json"

        expected_default_filepath_1 = os.path.join(
            default_relative_filepath, expected_default_filename_1
        )
        expected_default_filepath_2 = os.path.join(
            default_relative_filepath, expected_default_filename_2
        )
        expected_default_filepath_3 = os.path.join(
            default_relative_filepath, expected_default_filename_3
        )

        # expected filenames in the study user activities path.

        # we only care about the URI of the actual liked post if the actual liked
        # post was written by a study user.
        mock_liked_post_uri_suffix_1 = "generic-post-uri-1"  # study user likes a post
        mock_liked_post_uri_suffix_2 = "post-uri-2"  # someone likes a study user's post # noqa
        mock_liked_post_author_did_2 = "did:plc:study-user-2"

        study_user_relative_fp_1 = os.path.join(
            raw_sync_relative_path_map["create"]["like"],
            mock_liked_post_uri_suffix_1
        )
        study_user_relative_fp_2 = os.path.join(
            raw_sync_relative_path_map["create"]["like_on_user_post"],
            mock_liked_post_uri_suffix_2
        )

        expected_study_user_filepath_1 = os.path.join(  # study user likes a post
            raw_sync_root_local_path,
            mock_author_did_2,
            study_user_relative_fp_1,
            expected_default_filename_2
        )
        expected_study_user_filepath_2 = os.path.join(  # someone likes a study user's post # noqa
            raw_sync_root_local_path,
            mock_liked_post_author_did_2,
            study_user_relative_fp_2,
            expected_default_filename_3
        )

        manage_likes(likes=likes)

        # check default path. Should be 3 files.
        assert os.path.exists(expected_default_filepath_1)
        clean_path(expected_default_filepath_1)

        assert os.path.exists(expected_default_filepath_2)
        clean_path(expected_default_filepath_2)

        assert os.path.exists(expected_default_filepath_3)
        clean_path(expected_default_filepath_3)

        # check user activities path. Should be two files.
        assert os.path.exists(expected_study_user_filepath_1)
        clean_path(expected_study_user_filepath_1)

        assert os.path.exists(expected_study_user_filepath_2)
        clean_path(expected_study_user_filepath_2)


class TestManagePost:
    """Tests for manage_post."""

    def test_manage_post_create_default(self, mock_study_user_manager, mock_post_records_fixture):
        """Tests the creation of a default post from the firehose."""
        relative_filepath = raw_sync_relative_path_map["create"]["post"]
        mock_author_did = "did:plc:generic-user-1"
        mock_post_uri_suffix = "post-uri-1"
        expected_filename = f"author_did={mock_author_did}_post_uri_suffix={mock_post_uri_suffix}.json"  # noqa
        expected_filepath = os.path.join(relative_filepath, expected_filename)

        manage_post(post=mock_post_records_fixture[0], operation="create")
        assert os.path.exists(expected_filepath)
        clean_path(expected_filepath)

    def test_manage_post_delete_default(self, mock_study_user_manager, mock_post_records_fixture):
        """Tests the deletion of a default post from the firehose."""
        relative_filepath = raw_sync_relative_path_map["delete"]["post"]
        mock_post_uri_suffix = "post-uri-1"
        expected_filename = f"post_uri_suffix={mock_post_uri_suffix}.json"
        expected_filepath = os.path.join(relative_filepath, expected_filename)

        manage_post(post=mock_post_records_fixture[1], operation="delete")
        assert os.path.exists(expected_filepath)
        clean_path(expected_filepath)

    def test_study_user_writes_post(self, mock_study_user_manager, mock_post_records_fixture):
        """Tests the case where a study user writes a post.

        Expects that two files are written:
        - One in the default path.
        - One in the study user activities path.
        """
        mock_author_did = "did:plc:study-user-1"
        mock_post_uri_suffix = "post-uri-1"
        relative_filepath_1 = raw_sync_relative_path_map["create"]["post"]
        relative_filepath_2 = raw_sync_relative_path_map["create"]["post"]
        expected_filename = f"author_did={mock_author_did}_post_uri_suffix={mock_post_uri_suffix}.json"  # noqa
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)
        expected_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_author_did,
            relative_filepath_2,
            expected_filename
        )

        manage_post(post=mock_post_records_fixture[2], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

        assert os.path.exists(expected_filepath_2)
        clean_path(expected_filepath_2)

    def test_post_in_same_thread_as_study_user_post_parent(self, mock_study_user_manager, mock_post_records_fixture):
        """Tests the case where a post is in the same thread as a post written
        by a study user and that the study user post is the direct parent of
        the post, or the post that the post is responding to.

        Expects that two files are written:
        - One in the default path.
        - One in the study user activities path.
        """
        mock_author_did = "did:plc:generic-user-1"
        mock_parent_post_author_did = "did:plc:study-user-1"
        mock_post_uri_suffix = "generic-post-uri-1"
        mock_original_study_user_post_uri_suffix = "post-uri-1"

        # check for 2 files.
        relative_filepath_1 = raw_sync_relative_path_map["create"]["post"]
        relative_filepath_2 = raw_sync_relative_path_map["create"]["reply_to_user_post"]

        expected_filename = f"author_did={mock_author_did}_post_uri_suffix={mock_post_uri_suffix}.json"
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)
        expected_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_parent_post_author_did,  # the author of the parent post (the study user) # noqa
            relative_filepath_2,
            mock_original_study_user_post_uri_suffix,
            expected_filename
        )

        manage_post(post=mock_post_records_fixture[3], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

        assert os.path.exists(expected_filepath_2)
        clean_path(expected_filepath_2)

    def test_post_in_same_thread_as_study_user_post_root(self, mock_study_user_manager, mock_post_records_fixture):
        """Tests the case where a post is in the same thread as a post written
        by a study user and that the study user post is the root post of the
        thread, or the first post in the thread.

        Expects that two files are written:
        - One in the default path.
        - One in the study user activities path.
        """
        mock_author_did = "did:plc:generic-user-1"
        mock_root_post_author_did = "did:plc:study-user-1"
        mock_post_uri_suffix = "generic-post-uri-1"
        mock_original_study_user_post_uri_suffix = "post-uri-1"

        # check for 2 files.
        relative_filepath_1 = raw_sync_relative_path_map["create"]["post"]
        relative_filepath_2 = raw_sync_relative_path_map["create"]["reply_to_user_post"]

        expected_filename = f"author_did={mock_author_did}_post_uri_suffix={mock_post_uri_suffix}.json"
        expected_filepath_1 = os.path.join(relative_filepath_1, expected_filename)
        expected_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_root_post_author_did,  # the author of the root post (the study user) # noqa
            relative_filepath_2,
            mock_original_study_user_post_uri_suffix,
            expected_filename
        )

        manage_post(post=mock_post_records_fixture[4], operation="create")

        assert os.path.exists(expected_filepath_1)
        clean_path(expected_filepath_1)

        assert os.path.exists(expected_filepath_2)
        clean_path(expected_filepath_2)


class TestManagePosts:
    """Tests for manage_posts."""

    def test_manage_posts(self, mock_study_user_manager, mock_post_records_fixture):
        """Test manage_posts.

        We only care about 'created' records.
        """
        posts = {
            "created": [
                mock_post_records_fixture[0],
                mock_post_records_fixture[2],
                mock_post_records_fixture[3],
                mock_post_records_fixture[4]
            ],
            "deleted": []  # skip the deleted record.
        }

        # we have 4 records, one default, one where a study user writes a post,
        # one where a post is in the same thread as a post written by a
        # study user (and the study user's post is the parent of the post), and
        # one where a post is in the same thread as a post written by a study
        # user (and the study user's post is the root of the thread).
        default_relative_filepath = raw_sync_relative_path_map["create"]["post"]
        study_user_relative_fp_1 = raw_sync_relative_path_map["create"]["post"]
        study_user_relative_fp_2 = raw_sync_relative_path_map["create"]["reply_to_user_post"]

        # Post 1: generic post record
        mock_author_did_1 = "did:plc:generic-user-1"
        mock_post_uri_suffix_1 = "post-uri-1"
        expected_filename_1 = f"author_did={mock_author_did_1}_post_uri_suffix={mock_post_uri_suffix_1}.json"  # noqa

        # Post 2: post from a study user
        mock_author_did_2 = "did:plc:study-user-1"
        mock_post_uri_suffix_2 = "post-uri-1"
        expected_filename_2 = f"author_did={mock_author_did_2}_post_uri_suffix={mock_post_uri_suffix_2}.json"  # noqa

        # Post 3: post in the same thread as a post written by a study user
        # (study user's post is the parent of the post)
        mock_author_did_3 = "did:plc:generic-user-1"  # should be the author of the parent post
        mock_post_uri_suffix_3 = "generic-post-uri-1"
        mock_original_study_user_post_author_did_3 = "did:plc:study-user-1"
        mock_original_study_user_post_uri_suffix_3 = "post-uri-1"
        expected_filename_3 = f"author_did={mock_author_did_3}_post_uri_suffix={mock_post_uri_suffix_3}.json"

        # Post 4: post in the same thread as a post written by a study user
        # (study user's post is the root of the thread)
        mock_author_did_4 = "did:plc:generic-user-1"  # should be the author of the parent post
        mock_post_uri_suffix_4 = "generic-post-uri-1"
        mock_original_study_user_post_author_did_4 = "did:plc:study-user-1"
        mock_original_study_user_post_uri_suffix_4 = "post-uri-1"
        expected_filename_4 = f"author_did={mock_author_did_4}_post_uri_suffix={mock_post_uri_suffix_4}.json"

        # expected filenames in the default paths (4)
        expected_default_filepath_1 = os.path.join(default_relative_filepath, expected_filename_1)
        expected_default_filepath_2 = os.path.join(default_relative_filepath, expected_filename_2)
        expected_default_filepath_3 = os.path.join(default_relative_filepath, expected_filename_3)
        expected_default_filepath_4 = os.path.join(default_relative_filepath, expected_filename_4)

        # expected filenames in the study user activities path (3) - one for the
        # post written by the study user, and two for the posts that are in the
        # same thread as the study user's posts.
        expected_study_user_filepath_1 = os.path.join(
            raw_sync_root_local_path,
            mock_author_did_2,
            study_user_relative_fp_1,
            expected_filename_2
        )
        expected_study_user_filepath_2 = os.path.join(
            raw_sync_root_local_path,
            mock_original_study_user_post_author_did_3,  # author of the parent post (who is in the study)
            study_user_relative_fp_2,
            mock_original_study_user_post_uri_suffix_3,
            expected_filename_3
        )
        expected_study_user_filepath_3 = os.path.join(
            raw_sync_root_local_path,
            mock_original_study_user_post_author_did_4,  # author of the root post (who is in the study)
            study_user_relative_fp_2,
            mock_original_study_user_post_uri_suffix_4,
            expected_filename_4
        )

        manage_posts(posts=posts)

        # check default path. Should be 3 files:
        # 1. generic post record
        # 2. post from a study user
        # 3. post in the same thread as a post written by a study user
        # (whether the post is a parent or root of the thread creates the same
        # record, so when we delete the file, we only need to check for one.
        # we write the same exact file regardless of if the post is a
        # parent or root of the thread).
        assert os.path.exists(expected_default_filepath_1)
        clean_path(expected_default_filepath_1)

        assert os.path.exists(expected_default_filepath_2)
        clean_path(expected_default_filepath_2)

        assert os.path.exists(expected_default_filepath_3)
        assert os.path.exists(expected_default_filepath_4)  # same as expected_default_filepath_3
        clean_path(expected_default_filepath_3)

        # check user activities path. Should be two files.
        # 1. post from a study user
        # 2. post in the same thread as a post written by a study user
        # (whether the post is a parent or root of the thread creates the same
        # record, so when we delete the file, we only need to check for one.)
        assert os.path.exists(expected_study_user_filepath_1)
        clean_path(expected_study_user_filepath_1)

        assert os.path.exists(expected_study_user_filepath_2)
        assert os.path.exists(expected_study_user_filepath_3)  # same as expected_study_user_filepath_2
        clean_path(expected_study_user_filepath_2)

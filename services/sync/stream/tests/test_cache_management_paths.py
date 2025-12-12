"""Tests for cache_management/paths.py - CachePathManager class."""

import os

import pytest

from services.sync.stream.cache_management import CachePathManager
from services.sync.stream.core.types import Operation, RecordType, GenericRecordType, FollowStatus


class TestCachePathManager:
    """Tests for CachePathManager class."""

    def test_init(self, path_manager):
        """Test CachePathManager initialization."""
        # Arrange & Act

        # Assert
        assert path_manager.root_write_path.endswith("__local_cache__")
        assert Operation.CREATE.value in path_manager.root_create_path
        assert Operation.DELETE.value in path_manager.root_delete_path
        assert path_manager.operation_types == [GenericRecordType.POST.value, GenericRecordType.LIKE.value, GenericRecordType.FOLLOW.value]

    def test_get_local_cache_path(self, path_manager):
        """Test get_local_cache_path returns correct paths."""
        # Arrange

        # Act
        create_post_path = path_manager.get_local_cache_path(Operation.CREATE, GenericRecordType.POST)
        delete_like_path = path_manager.get_local_cache_path(Operation.DELETE, GenericRecordType.LIKE)

        # Assert
        assert Operation.CREATE.value in create_post_path
        assert GenericRecordType.POST.value in create_post_path
        assert Operation.DELETE.value in delete_like_path
        assert GenericRecordType.LIKE.value in delete_like_path

    def test_get_study_user_activity_path_post(self, path_manager):
        """Test get_study_user_activity_path for post records."""
        # Arrange

        # Act
        create_path = path_manager.get_study_user_activity_path(Operation.CREATE, RecordType.POST)
        delete_path = path_manager.get_study_user_activity_path(Operation.DELETE, RecordType.POST)

        # Assert
        assert "study_user_activity" in create_path
        assert Operation.CREATE.value in create_path
        assert RecordType.POST.value in create_path
        assert "study_user_activity" in delete_path
        assert Operation.DELETE.value in delete_path

    def test_get_study_user_activity_path_follow(self, path_manager):
        """Test get_study_user_activity_path for follow records with follow_status."""
        # Arrange

        # Act
        follower_path = path_manager.get_study_user_activity_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
        )
        followee_path = path_manager.get_study_user_activity_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWEE
        )

        # Assert
        assert RecordType.FOLLOW.value in follower_path
        assert FollowStatus.FOLLOWER.value in follower_path
        assert RecordType.FOLLOW.value in followee_path
        assert FollowStatus.FOLLOWEE.value in followee_path

    def test_get_study_user_activity_path_like_on_user_post(self, path_manager):
        """Test get_study_user_activity_path for like_on_user_post records."""
        # Arrange

        # Act
        path = path_manager.get_study_user_activity_path(Operation.CREATE, RecordType.LIKE_ON_USER_POST)

        # Assert
        assert "study_user_activity" in path
        assert RecordType.LIKE_ON_USER_POST.value in path

    def test_get_in_network_activity_path(self, path_manager):
        """Test get_in_network_activity_path returns correct path."""
        # Arrange
        author_did = "did:plc:test123"

        # Act
        path = path_manager.get_in_network_activity_path(Operation.CREATE, RecordType.POST, author_did)

        # Assert
        assert "in_network_user_activity" in path
        assert Operation.CREATE.value in path
        assert RecordType.POST.value in path
        assert author_did in path

    def test_get_relative_path(self, path_manager):
        """Test get_relative_path returns relative path component."""
        # Arrange

        # Act
        post_relative = path_manager.get_relative_path(Operation.CREATE, RecordType.POST)
        follow_relative = path_manager.get_relative_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
        )

        # Assert
        assert Operation.CREATE.value in post_relative
        assert RecordType.POST.value in post_relative
        assert RecordType.FOLLOW.value in follow_relative
        assert FollowStatus.FOLLOWER.value in follow_relative

    def test_get_study_user_activity_path_delete_like_on_user_post_raises_error(self, path_manager):
        """Test get_study_user_activity_path raises ValueError for unsupported DELETE/LIKE_ON_USER_POST."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_study_user_activity_path(
                Operation.DELETE, RecordType.LIKE_ON_USER_POST
            )
        assert "Unsupported record_type 'like_on_user_post' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_study_user_activity_path_delete_reply_to_user_post_raises_error(self, path_manager):
        """Test get_study_user_activity_path raises ValueError for unsupported DELETE/REPLY_TO_USER_POST."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_study_user_activity_path(
                Operation.DELETE, RecordType.REPLY_TO_USER_POST
            )
        assert "Unsupported record_type 'reply_to_user_post' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_study_user_activity_path_delete_follow_raises_error(self, path_manager):
        """Test get_study_user_activity_path raises ValueError for unsupported DELETE/FOLLOW."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_study_user_activity_path(
                Operation.DELETE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
            )
        assert "Unsupported record_type 'follow' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_relative_path_delete_like_on_user_post_raises_error(self, path_manager):
        """Test get_relative_path raises ValueError for unsupported DELETE/LIKE_ON_USER_POST."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_relative_path(Operation.DELETE, RecordType.LIKE_ON_USER_POST)
        assert "Unsupported record_type 'like_on_user_post' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_relative_path_delete_reply_to_user_post_raises_error(self, path_manager):
        """Test get_relative_path raises ValueError for unsupported DELETE/REPLY_TO_USER_POST."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_relative_path(Operation.DELETE, RecordType.REPLY_TO_USER_POST)
        assert "Unsupported record_type 'reply_to_user_post' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_relative_path_delete_follow_raises_error(self, path_manager):
        """Test get_relative_path raises ValueError for unsupported DELETE/FOLLOW."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_relative_path(
                Operation.DELETE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
            )
        assert "Unsupported record_type 'follow' for operation 'delete'" in str(exc_info.value)
        assert "Available record types" in str(exc_info.value)

    def test_get_study_user_activity_path_follow_requires_follow_status(self, path_manager):
        """Test get_study_user_activity_path raises ValueError when FOLLOW is used without follow_status."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_study_user_activity_path(
                Operation.CREATE, RecordType.FOLLOW, follow_status=None
            )
        assert "follow_status is required for record_type 'follow'" in str(exc_info.value)

    def test_get_relative_path_follow_requires_follow_status(self, path_manager):
        """Test get_relative_path raises ValueError when FOLLOW is used without follow_status."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            path_manager.get_relative_path(
                Operation.CREATE, RecordType.FOLLOW, follow_status=None
            )
        assert "follow_status is required for record_type 'follow'" in str(exc_info.value)


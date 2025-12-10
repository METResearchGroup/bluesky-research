"""Tests for cache_management/paths.py - CachePathManager class."""

import os
from unittest.mock import Mock

import pytest

from services.sync.stream.cache_management import CachePathManager
from services.sync.stream.types import Operation, RecordType, GenericRecordType, FollowStatus


class TestCachePathManager:
    """Tests for CachePathManager class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        mock_manager.is_study_user = Mock(return_value=False)
        mock_manager.is_study_user_post = Mock(return_value=None)
        mock_manager.is_in_network_user = Mock(return_value=False)
        # No longer needed - cache_writer.py was deleted

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


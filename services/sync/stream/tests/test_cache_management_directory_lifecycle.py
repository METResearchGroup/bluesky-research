"""Tests for cache_management/directory_lifecycle.py - CacheDirectoryManager class."""

import os
import tempfile

import pytest

from services.sync.stream.cache_management import CachePathManager, CacheDirectoryManager
from services.sync.stream.types import Operation, RecordType


class TestCacheDirectoryManager:
    """Tests for CacheDirectoryManager class."""

    def test_init(self, path_manager):
        """Test CacheDirectoryManager initialization."""
        # Act
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        # Assert
        assert dir_manager.path_manager == path_manager

    def test_ensure_directory_exists_creates_directory(self, dir_manager):
        """Test ensure_directory_exists creates directory if it doesn't exist."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test", "nested", "path")

            # Act
            dir_manager.ensure_directory_exists(test_path)

            # Assert
            assert os.path.exists(test_path)
            assert os.path.isdir(test_path)

    def test_ensure_directory_exists_does_not_fail_if_exists(self, dir_manager):
        """Test ensure_directory_exists does not fail if directory already exists."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "existing")
            os.makedirs(test_path)

            # Act & Assert (should not raise)
            dir_manager.ensure_directory_exists(test_path)
            assert os.path.exists(test_path)

    def test_rebuild_all_creates_all_directories(self, path_manager, dir_manager):
        """Test rebuild_all creates all required directory structures."""
        # Arrange

        # Use a temporary directory for testing
        with tempfile.TemporaryDirectory() as tmpdir:
            # Override root_write_path for testing
            path_manager.root_write_path = os.path.join(tmpdir, "__local_cache__")
            path_manager.root_create_path = os.path.join(
                path_manager.root_write_path, Operation.CREATE.value
            )
            path_manager.root_delete_path = os.path.join(
                path_manager.root_write_path, Operation.DELETE.value
            )
            # Update study_user_activity paths
            path_manager.study_user_activity_root_local_path = os.path.join(
                path_manager.root_write_path, "study_user_activity"
            )
            path_manager.in_network_user_activity_root_local_path = os.path.join(
                path_manager.root_write_path, "in_network_user_activity"
            )
            path_manager.in_network_user_activity_create_post_local_path = os.path.join(
                path_manager.in_network_user_activity_root_local_path, Operation.CREATE.value, RecordType.POST.value
            )

            # Act
            dir_manager.rebuild_all()

            # Assert - check key directories exist
            assert os.path.exists(path_manager.root_write_path)
            assert os.path.exists(path_manager.root_create_path)
            assert os.path.exists(path_manager.root_delete_path)
            assert os.path.exists(
                path_manager.study_user_activity_root_local_path
            )
            assert os.path.exists(
                path_manager.in_network_user_activity_root_local_path
            )

    def test_delete_all_removes_directory(self, path_manager, dir_manager):
        """Test delete_all removes the root write path."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            path_manager.root_write_path = os.path.join(tmpdir, "__local_cache__")
            os.makedirs(path_manager.root_write_path)

            # Act
            dir_manager.delete_all()

            # Assert
            assert not os.path.exists(path_manager.root_write_path)


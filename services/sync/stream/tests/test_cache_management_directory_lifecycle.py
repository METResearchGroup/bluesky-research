"""Tests for cache_management/directory_lifecycle.py - CacheDirectoryManager class."""

import os
import tempfile
from unittest.mock import Mock

import pytest

from services.sync.stream.cache_management import CachePathManager, CacheDirectoryManager
from services.sync.stream.types import Operation, RecordType


class TestCacheDirectoryManager:
    """Tests for CacheDirectoryManager class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._get_study_user_manager",
            lambda: mock_manager,
        )

    def test_init(self):
        """Test CacheDirectoryManager initialization."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        # Assert
        assert dir_manager.path_manager == path_manager

    def test_ensure_exists_creates_directory(self):
        """Test ensure_exists creates directory if it doesn't exist."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test", "nested", "path")

            # Act
            dir_manager.ensure_exists(test_path)

            # Assert
            assert os.path.exists(test_path)
            assert os.path.isdir(test_path)

    def test_ensure_exists_does_not_fail_if_exists(self):
        """Test ensure_exists does not fail if directory already exists."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "existing")
            os.makedirs(test_path)

            # Act & Assert (should not raise)
            dir_manager.ensure_exists(test_path)
            assert os.path.exists(test_path)

    def test_rebuild_all_creates_all_directories(self):
        """Test rebuild_all creates all required directory structures."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

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

    def test_rebuild_all_raises_error_for_non_sync_path_manager(self):
        """Test rebuild_all raises TypeError for non-CachePathManager."""
        # Arrange
        mock_path_manager = Mock()
        dir_manager = CacheDirectoryManager(path_manager=mock_path_manager)

        # Act & Assert
        with pytest.raises(TypeError, match="rebuild_all requires CachePathManager"):
            dir_manager.rebuild_all()

    def test_delete_all_removes_directory(self):
        """Test delete_all removes the root write path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            path_manager.root_write_path = os.path.join(tmpdir, "__local_cache__")
            os.makedirs(path_manager.root_write_path)

            # Act
            dir_manager.delete_all()

            # Assert
            assert not os.path.exists(path_manager.root_write_path)

    def test_delete_all_raises_error_for_non_sync_path_manager(self):
        """Test delete_all raises TypeError for non-CachePathManager."""
        # Arrange
        mock_path_manager = Mock()
        dir_manager = CacheDirectoryManager(path_manager=mock_path_manager)

        # Act & Assert
        with pytest.raises(TypeError, match="delete_all requires CachePathManager"):
            dir_manager.delete_all()

    def test_exists_returns_true_for_existing_path(self):
        """Test exists returns True for existing path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Act & Assert
            assert dir_manager.exists(tmpdir) is True

    def test_exists_returns_false_for_non_existing_path(self):
        """Test exists returns False for non-existing path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        non_existent = "/nonexistent/path/that/does/not/exist"

        # Act & Assert
        assert dir_manager.exists(non_existent) is False


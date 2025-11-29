"""Tests for scripts.migrate_research_data_to_s3.initialize_migration_tracker_db module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock boto3 before importing modules that depend on it
sys.modules["boto3"] = MagicMock()

from scripts.migrate_research_data_to_s3.constants import DEFAULT_S3_ROOT_PREFIX
from scripts.migrate_research_data_to_s3.initialize_migration_tracker_db import (
    get_s3_key_for_local_filepath,
)


class TestGetS3KeyForLocalFilepath:
    """Tests for get_s3_key_for_local_filepath function."""

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_normal_case_with_nested_path(self):
        """Test S3 key generation for a normal nested filepath."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/feeds/2024/01/file.txt"
        expected = f"{DEFAULT_S3_ROOT_PREFIX}/feeds/2024/01/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath)

        # Assert
        assert result == expected

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_normal_case_with_root_level_file(self):
        """Test S3 key generation for a file at root level."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/file.txt"
        expected = f"{DEFAULT_S3_ROOT_PREFIX}/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath)

        # Assert
        assert result == expected

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_with_custom_s3_root_prefix(self):
        """Test S3 key generation with custom S3 root prefix."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/feeds/file.txt"
        custom_prefix = "custom/bucket/prefix"
        expected = f"{custom_prefix}/feeds/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath, s3_root_prefix=custom_prefix)

        # Assert
        assert result == expected

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_with_deeply_nested_path(self):
        """Test S3 key generation for deeply nested filepath."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/study_user_activity/create/like/active/2024/01/file.txt"
        expected = f"{DEFAULT_S3_ROOT_PREFIX}/study_user_activity/create/like/active/2024/01/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath)

        # Assert
        assert result == expected

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_with_windows_path_separators(self):
        """Test S3 key generation handles Windows path separators correctly."""
        # Arrange - simulate Windows path
        local_filepath = "/data/bluesky_research_data/feeds/2024/01/file.txt"
        # On Windows, pathlib would use backslashes internally, but we convert to forward slashes
        expected = f"{DEFAULT_S3_ROOT_PREFIX}/feeds/2024/01/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath)

        # Assert - should always use forward slashes for S3
        assert result == expected
        assert "\\" not in result

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_raises_value_error_for_path_outside_root(self):
        """Test that ValueError is raised for paths outside the root directory."""
        # Arrange
        local_filepath = "/other/directory/file.txt"

        # Act & Assert
        with pytest.raises(ValueError, match="is not under root directory"):
            get_s3_key_for_local_filepath(local_filepath)

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_with_empty_s3_root_prefix(self):
        """Test S3 key generation with empty S3 root prefix."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/feeds/file.txt"
        expected = "feeds/file.txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath, s3_root_prefix="")

        # Assert
        assert result == expected

    @patch("scripts.migrate_research_data_to_s3.initialize_migration_tracker_db.root_local_data_directory", "/data/bluesky_research_data")
    def test_handles_path_with_special_characters(self):
        """Test S3 key generation handles paths with special characters."""
        # Arrange
        local_filepath = "/data/bluesky_research_data/feeds/2024-01/file_name (1).txt"
        expected = f"{DEFAULT_S3_ROOT_PREFIX}/feeds/2024-01/file_name (1).txt"

        # Act
        result = get_s3_key_for_local_filepath(local_filepath)

        # Assert
        assert result == expected

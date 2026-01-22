"""Tests for scripts.migrate_research_data_to_s3.run_migration module."""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import botocore
import pytest

# Mock boto3 before importing modules that depend on it
sys.modules["boto3"] = MagicMock()

from scripts.migrate_research_data_to_s3.run_migration import (
    create_progress_callback,
    migrate_file_to_s3,
    run_migration_for_prefixes,
)


class TestCreateProgressCallback:
    """Tests for create_progress_callback function."""

    def test_callback_tracks_upload_progress(self):
        """Test that callback correctly tracks upload progress."""
        file_path = "/test/file.txt"
        file_size = 1024 * 1024  # 1 MB
        
        callback = create_progress_callback(file_path, file_size)
        
        # Simulate upload progress
        callback(512 * 1024)  # 0.5 MB
        callback(256 * 1024)  # 0.25 MB more
        callback(256 * 1024)  # 0.25 MB more (total 1 MB)
        
        # Callback should not raise errors
        # (We can't easily test the internal state without exposing it)

    def test_callback_raises_on_zero_file_size(self):
        """Test that callback raises ValueError for zero file size."""
        file_path = "/test/file.txt"
        file_size = 0
        
        with pytest.raises(ValueError, match="zero or negative file size"):
            create_progress_callback(file_path, file_size)


class TestMigrateFileToS3:
    """Tests for migrate_file_to_s3 function."""

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = f.name
        yield temp_path
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        mock_client = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.client = mock_client
        mock_s3.bucket = "test-bucket"
        return mock_s3

    def test_migrate_file_to_s3_success(self, temp_file, mock_s3_client):
        """Test successful file migration."""
        success, error = migrate_file_to_s3(temp_file, "s3/test/key.txt", mock_s3_client)
        
        assert success is True
        assert error == ""
        mock_s3_client.client.upload_file.assert_called_once()

    def test_migrate_file_to_s3_file_not_found(self, mock_s3_client):
        """Test that FileNotFoundError is handled correctly."""
        success, error = migrate_file_to_s3(
            "/nonexistent/file.txt", "s3/test/key.txt", mock_s3_client
        )
        
        assert success is False
        assert "File not found" in error
        mock_s3_client.client.upload_file.assert_not_called()

    def test_migrate_file_to_s3_aws_client_error(self, temp_file, mock_s3_client):
        """Test that AWS ClientError is handled correctly."""
        # Simulate AWS error
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_s3_client.client.upload_file.side_effect = botocore.exceptions.ClientError(
            error_response, "PutObject"
        )
        
        success, error = migrate_file_to_s3(temp_file, "s3/test/key.txt", mock_s3_client)
        
        assert success is False
        assert "AWS" in error
        assert "NoSuchBucket" in error

    def test_migrate_file_to_s3_s3_client_not_initialized(self, temp_file):
        """Test that RuntimeError is raised when S3 client is not initialized."""
        mock_s3 = MagicMock()
        mock_s3.client = None
        mock_s3.bucket = "test-bucket"
        
        success, error = migrate_file_to_s3(temp_file, "s3/test/key.txt", mock_s3)
        
        assert success is False
        assert "not initialized" in error

    def test_migrate_file_to_s3_os_error(self, temp_file, mock_s3_client):
        """Test that OSError is handled correctly."""
        # Simulate OS error during file access
        with patch("os.path.getsize", side_effect=OSError("Permission denied")):
            success, error = migrate_file_to_s3(
                temp_file, "s3/test/key.txt", mock_s3_client
            )
            
            assert success is False
            assert "Permission denied" in error

    def test_migrate_file_to_s3_unexpected_error_raises(self, temp_file, mock_s3_client):
        """Test that unexpected errors are re-raised."""
        # Simulate unexpected error (e.g., AttributeError)
        mock_s3_client.client.upload_file.side_effect = AttributeError("Unexpected error")
        
        with pytest.raises(AttributeError):
            migrate_file_to_s3(temp_file, "s3/test/key.txt", mock_s3_client)

    def test_migrate_file_to_s3_calls_upload_with_callback(self, temp_file, mock_s3_client):
        """Test that upload_file is called with a callback."""
        migrate_file_to_s3(temp_file, "s3/test/key.txt", mock_s3_client)
        
        call_args = mock_s3_client.client.upload_file.call_args
        assert call_args is not None
        # Check that Callback parameter is provided
        assert "Callback" in call_args.kwargs or len(call_args.args) >= 4


class TestRunMigrationForPrefixes:
    """Tests for run_migration_for_prefixes function."""

    @patch(
        "scripts.migrate_research_data_to_s3.run_migration.run_migration_for_single_prefix"
    )
    def test_calls_run_migration_for_single_prefix_per_prefix(
        self, mock_run_single: MagicMock
    ):
        """Test that run_migration_for_prefixes calls run_migration_for_single_prefix for each prefix."""
        # Arrange
        prefixes = ["ml_inference_intergroup/active", "ml_inference_intergroup/cache"]
        mock_tracker = MagicMock()
        mock_s3 = MagicMock()

        # Act
        run_migration_for_prefixes(prefixes, mock_tracker, mock_s3)

        # Assert
        assert mock_run_single.call_count == 2
        mock_run_single.assert_any_call(
            "ml_inference_intergroup/active", mock_tracker, mock_s3
        )
        mock_run_single.assert_any_call(
            "ml_inference_intergroup/cache", mock_tracker, mock_s3
        )

    @patch(
        "scripts.migrate_research_data_to_s3.run_migration.run_migration_for_single_prefix"
    )
    def test_empty_prefixes_no_calls(self, mock_run_single: MagicMock):
        """Test that run_migration_for_prefixes with empty list does not call run_migration_for_single_prefix."""
        # Arrange
        prefixes: list[str] = []
        mock_tracker = MagicMock()
        mock_s3 = MagicMock()

        # Act
        run_migration_for_prefixes(prefixes, mock_tracker, mock_s3)

        # Assert
        mock_run_single.assert_not_called()


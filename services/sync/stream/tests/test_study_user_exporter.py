"""Tests for study_user_exporter.py - StudyUserActivityExporter class."""

import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock

import pytest

from services.sync.stream.exporters.study_user_exporter import StudyUserActivityExporter
from services.sync.stream.core.types import Operation, RecordType


class TestStudyUserActivityExporter:
    """Tests for StudyUserActivityExporter class."""

    @pytest.fixture
    def mock_path_manager(self):
        """Create a mock path manager."""
        mock = Mock()
        mock.get_study_user_activity_path = Mock(
            return_value="/tmp/test/create/post"
        )
        return mock

    @pytest.fixture
    def mock_storage_repository(self):
        """Create a mock storage repository."""
        return Mock()

    @pytest.fixture
    def mock_handler_registry(self):
        """Create a mock handler registry."""
        return Mock()

    @pytest.fixture
    def exporter(
        self, mock_path_manager, mock_storage_repository, mock_handler_registry
    ):
        """Create a StudyUserActivityExporter instance."""
        return StudyUserActivityExporter(
            path_manager=mock_path_manager,
            storage_repository=mock_storage_repository,
            handler_registry=mock_handler_registry,
        )

    def test_export_activity_data_handles_keyerror(self, exporter, mock_handler_registry):
        """Test that KeyError from handler registry is caught and logged."""
        # Arrange
        mock_handler_registry.get_handler.side_effect = KeyError("Unknown record type: test")
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "not registered in handler registry" in warning_call[0][0]
                # Verify context contains error details
                context = warning_call[1].get("context", {})
                assert "error" in context
                assert "record_type" in context
                assert "operation" in context

    def test_export_activity_data_handles_filenotfounderror(self, exporter, mock_handler_registry):
        """Test that FileNotFoundError from read_records is caught and logged."""
        # Arrange
        mock_handler = Mock()
        mock_handler.read_records.side_effect = FileNotFoundError("Directory not found")
        mock_handler_registry.get_handler.return_value = mock_handler
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "File or directory not found" in warning_call[0][0]

    def test_export_activity_data_handles_permissionerror(self, exporter, mock_handler_registry):
        """Test that PermissionError from read_records is caught and logged."""
        # Arrange
        mock_handler = Mock()
        mock_handler.read_records.side_effect = PermissionError("Permission denied")
        mock_handler_registry.get_handler.return_value = mock_handler
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "Permission denied" in warning_call[0][0]

    def test_export_activity_data_handles_jsondecodeerror(self, exporter, mock_handler_registry):
        """Test that json.JSONDecodeError from read_records is caught and logged."""
        # Arrange
        mock_handler = Mock()
        mock_handler.read_records.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_handler_registry.get_handler.return_value = mock_handler
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "Malformed JSON" in warning_call[0][0]

    def test_export_activity_data_handles_oserror(self, exporter, mock_handler_registry):
        """Test that OSError from read_records is caught and logged."""
        # Arrange
        mock_handler = Mock()
        mock_handler.read_records.side_effect = OSError("I/O error")
        mock_handler_registry.get_handler.return_value = mock_handler
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "I/O error" in warning_call[0][0]

    def test_export_activity_data_handles_unexpected_exception(self, exporter, mock_handler_registry):
        """Test that unexpected exceptions are caught, logged with traceback, and processing continues."""
        # Arrange
        mock_handler = Mock()
        mock_handler.read_records.side_effect = ValueError("Unexpected error")
        mock_handler_registry.get_handler.return_value = mock_handler
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                assert result == []
                # Verify warning was logged
                assert mock_logger.warning.called
                warning_call = mock_logger.warning.call_args
                assert "Unexpected error" in warning_call[0][0]
                # Verify traceback is in context
                context = warning_call[1].get("context", {})
                assert "traceback" in context
                assert "ValueError" in context.get("error_type", "")

    def test_export_activity_data_continues_after_exception(self, exporter, mock_handler_registry):
        """Test that processing continues for other record types after an exception."""
        # Arrange
        mock_handler_success = Mock()
        mock_handler_success.read_records.return_value = ([{"test": "data"}], ["/path/to/file.json"])
        
        mock_handler_error = Mock()
        mock_handler_error.read_records.side_effect = FileNotFoundError("Not found")
        
        # Create function-based side_effect that maps record_type to appropriate handler
        def get_handler_side_effect(record_type: str):
            """Return handler based on record_type, defaulting to success handler."""
            handler_map = {
                RecordType.LIKE.value: mock_handler_error,  # LIKE fails
            }
            # Default to success handler for all other record types
            return handler_map.get(record_type, mock_handler_success)
        
        mock_handler_registry.get_handler.side_effect = get_handler_side_effect
        
        with patch("services.sync.stream.exporters.study_user_exporter.logger"):
            with patch("os.path.exists", return_value=True):
                # Act
                result = exporter.export_activity_data()

                # Assert
                # Should have processed files from successful handlers
                assert len(result) > 0
                # Verify export_dataframe was called for successful record types
                assert exporter.storage_repository.export_dataframe.called

    def test_export_activity_data_skips_nonexistent_operation_directory(self, exporter):
        """Test that nonexistent operation directories are skipped."""
        # Arrange
        with patch("os.path.exists", return_value=False):
            # Act
            result = exporter.export_activity_data()

            # Assert
            assert result == []
            # Handler registry should not be called if directory doesn't exist
            assert not exporter.handler_registry.get_handler.called


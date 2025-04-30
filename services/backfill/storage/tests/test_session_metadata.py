"""Tests for session_metadata.py."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from lib.metadata.models import RunExecutionMetadata
from services.backfill.storage.session_metadata import (
    load_latest_backfilled_users,
    write_user_session_backfill_metadata_to_db,
    write_session_backfill_job_metadata_to_db,
    write_backfill_metadata_to_db,
)


class TestLoadLatestBackfilledUsers:
    """Tests for load_latest_backfilled_users function.
    
    This test class verifies that the function correctly:
    - Queries Athena with the expected SQL
    - Processes the DataFrame results correctly
    - Returns the data in the expected format
    """
    
    @pytest.fixture
    def mock_athena(self):
        """Fixture for mocking Athena."""
        with patch("services.backfill.sync.session_metadata.athena") as mock:
            yield mock
    
    def test_load_latest_backfilled_users(self, mock_athena):
        """Test normal operation of load_latest_backfilled_users.
        
        The function should:
        - Execute the expected SQL query
        - Convert the DataFrame to records
        - Return the records
        """
        # Create a mock DataFrame that would be returned by Athena
        mock_df = pd.DataFrame({
            "did": ["did:plc:user1", "did:plc:user2"],
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"]
        })
        
        # Configure the mock to return this DataFrame
        mock_athena.query_results_as_df.return_value = mock_df
        
        # Call the function
        result = load_latest_backfilled_users()
        
        # Verify Athena was called with the expected query
        mock_athena.query_results_as_df.assert_called_once()
        query_arg = mock_athena.query_results_as_df.call_args[1]["query"]
        
        # Verify the query contains the expected components
        assert "WITH ranked_users AS" in query_arg
        assert "ROW_NUMBER() OVER (PARTITION BY did ORDER BY timestamp DESC)" in query_arg
        assert "WHERE row_num = 1" in query_arg
        
        # Verify the result matches the expected format
        expected_result = [
            {"did": "did:plc:user1", "bluesky_handle": "user1.bsky.social"},
            {"did": "did:plc:user2", "bluesky_handle": "user2.bsky.social"}
        ]
        assert result == expected_result
    
    def test_load_latest_backfilled_users_empty(self, mock_athena):
        """Test load_latest_backfilled_users with empty results.
        
        When Athena returns an empty DataFrame, the function should:
        - Return an empty list
        """
        # Create an empty mock DataFrame
        mock_df = pd.DataFrame(columns=["did", "bluesky_handle"])
        
        # Configure the mock to return this empty DataFrame
        mock_athena.query_results_as_df.return_value = mock_df
        
        # Call the function
        result = load_latest_backfilled_users()
        
        # Verify Athena was called
        mock_athena.query_results_as_df.assert_called_once()
        
        # Verify an empty list was returned
        assert result == []


class TestWriteUserSessionBackfillMetadataToDB:
    """Tests for write_user_session_backfill_metadata_to_db function.
    
    This test class verifies that the function correctly:
    - Handles empty metadata lists
    - Saves metadata to DynamoDB using batch operations
    - Handles errors appropriately
    """
    
    @pytest.fixture
    def mock_batch_save(self):
        """Fixture for mocking batch_save_user_metadata."""
        with patch("services.backfill.sync.session_metadata.batch_save_user_metadata") as mock:
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.session_metadata.logger") as mock:
            yield mock

    def test_write_user_session_backfill_metadata_empty(self, mock_batch_save, mock_logger):
        """Test with empty metadata list."""
        # Call the function with an empty list
        write_user_session_backfill_metadata_to_db([])
        
        # Verify batch save was called with empty list
        mock_batch_save.assert_called_once_with([])
        
        # Verify success was logged
        mock_logger.info.assert_called_once()
        assert "Successfully wrote user backfill metadata to DynamoDB: 0 users" in mock_logger.info.call_args[0][0]

    def test_write_user_session_backfill_metadata(self, mock_batch_save, mock_logger):
        """Test with valid metadata list."""
        from services.backfill.core.models import UserBackfillMetadata
        
        # Create test metadata
        metadata_objects = [
            UserBackfillMetadata(
                did="did:plc:user1",
                bluesky_handle="user1.bsky.social",
                types="post,like",
                total_records=15,
                total_records_by_type='{"post": 5, "like": 10}',
                pds_service_endpoint="https://bsky-pds.com",
                timestamp="2024-03-26-12:00:00"
            )
        ]
        
        # Call the function
        write_user_session_backfill_metadata_to_db(metadata_objects)
        
        # Verify batch save was called with metadata
        mock_batch_save.assert_called_once_with(metadata_objects)
        
        # Verify success was logged
        mock_logger.info.assert_called_once()
        assert "Successfully wrote user backfill metadata to DynamoDB: 1 users" in mock_logger.info.call_args[0][0]


class TestWriteSessionBackfillJobMetadataToDB:
    """Tests for write_session_backfill_job_metadata_to_db function."""
    
    @pytest.fixture
    def mock_dynamodb(self):
        """Fixture for mocking DynamoDB."""
        with patch("services.backfill.sync.session_metadata.dynamodb") as mock:
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.session_metadata.logger") as mock:
            yield mock

    def test_write_session_backfill_job_metadata(self, mock_dynamodb, mock_logger):
        """Test successful write of session metadata."""
        metadata = RunExecutionMetadata(
            service="backfill_sync",
            timestamp="2024-03-26-12:00:00",
            status_code=200,
            body="Test successful",
            metadata_table_name="backfill_metadata",
            metadata="{}"
        )
        
        # Call the function
        write_session_backfill_job_metadata_to_db(metadata)
        
        # Verify DynamoDB was called correctly
        mock_dynamodb.insert_item_into_table.assert_called_once_with(
            item=metadata.model_dump(),
            table_name="backfill_metadata"
        )
        
        # Verify success was logged
        mock_logger.info.assert_called_once()
        assert "Successfully inserted session backfill metadata" in mock_logger.info.call_args[0][0]

    def test_write_session_backfill_job_metadata_error(self, mock_dynamodb, mock_logger):
        """Test error handling when writing session metadata."""
        metadata = RunExecutionMetadata(
            service="backfill_sync",
            timestamp="2024-03-26-12:00:00",
            status_code=200,
            body="Test successful",
            metadata_table_name="backfill_metadata",
            metadata="{}"
        )
        
        # Configure mock to raise exception
        mock_dynamodb.insert_item_into_table.side_effect = Exception("Test error")
        
        # Verify the error is raised
        with pytest.raises(Exception) as exc_info:
            write_session_backfill_job_metadata_to_db(metadata)
        
        assert str(exc_info.value) == "Test error"
        
        # Verify error was logged
        mock_logger.error.assert_called_once()
        assert "Error writing session backfill metadata to DynamoDB" in mock_logger.error.call_args[0][0]


class TestWriteBackfillMetadataToDB:
    """Tests for write_backfill_metadata_to_db function."""
    
    @pytest.fixture
    def mock_write_user_metadata(self):
        """Fixture for mocking write_user_session_backfill_metadata_to_db."""
        with patch("services.backfill.sync.session_metadata.write_user_session_backfill_metadata_to_db") as mock:
            yield mock
    
    @pytest.fixture
    def mock_write_session_metadata(self):
        """Fixture for mocking write_session_backfill_job_metadata_to_db."""
        with patch("services.backfill.sync.session_metadata.write_session_backfill_job_metadata_to_db") as mock:
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.session_metadata.logger") as mock:
            yield mock

    def test_write_backfill_metadata(self, mock_write_user_metadata, mock_write_session_metadata, mock_logger):
        """Test successful write of all metadata."""
        from services.backfill.core.models import UserBackfillMetadata
        
        # Create test data
        session_metadata = RunExecutionMetadata(
            service="backfill_sync",
            timestamp="2024-03-26-12:00:00",
            status_code=200,
            body="Test successful",
            metadata_table_name="backfill_metadata",
            metadata="{}"
        )
        
        user_metadata = [
            UserBackfillMetadata(
                did="did:plc:user1",
                bluesky_handle="user1.bsky.social",
                types="post,like",
                total_records=15,
                total_records_by_type='{"post": 5, "like": 10}',
                pds_service_endpoint="https://bsky-pds.com",
                timestamp="2024-03-26-12:00:00"
            )
        ]
        
        # Call the function
        write_backfill_metadata_to_db(session_metadata, user_metadata)
        
        # Verify both write functions were called
        mock_write_user_metadata.assert_called_once_with(user_metadata)
        mock_write_session_metadata.assert_called_once_with(session_metadata)
        
        # Verify completion was logged
        mock_logger.info.assert_called_with("completed writing backfill metadata to db") 
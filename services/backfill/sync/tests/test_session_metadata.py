"""Tests for session_metadata.py."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.backfill.sync.session_metadata import (
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
            "bluesky_user_handle": ["user1.bsky.social", "user2.bsky.social"]
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
            {"did": "did:plc:user1", "bluesky_user_handle": "user1.bsky.social"},
            {"did": "did:plc:user2", "bluesky_user_handle": "user2.bsky.social"}
        ]
        assert result == expected_result
    
    def test_load_latest_backfilled_users_empty(self, mock_athena):
        """Test load_latest_backfilled_users with empty results.
        
        When Athena returns an empty DataFrame, the function should:
        - Return an empty list
        """
        # Create an empty mock DataFrame
        mock_df = pd.DataFrame(columns=["did", "bluesky_user_handle"])
        
        # Configure the mock to return this empty DataFrame
        mock_athena.query_results_as_df.return_value = mock_df
        
        # Call the function
        result = load_latest_backfilled_users()
        
        # Verify Athena was called
        mock_athena.query_results_as_df.assert_called_once()
        
        # Verify an empty list was returned
        assert result == []
    
    def test_load_latest_backfilled_users_extra_columns(self, mock_athena):
        """Test load_latest_backfilled_users with extra columns in the results.
        
        When Athena returns a DataFrame with extra columns, the function should:
        - Include those columns in the returned dictionaries
        """
        # Create a mock DataFrame with extra columns
        mock_df = pd.DataFrame({
            "did": ["did:plc:user1", "did:plc:user2"],
            "bluesky_user_handle": ["user1.bsky.social", "user2.bsky.social"],
            "timestamp": ["2024-03-26-12:00:00", "2024-03-26-13:00:00"],
            "extra_col": ["value1", "value2"]
        })
        
        # Configure the mock to return this DataFrame
        mock_athena.query_results_as_df.return_value = mock_df
        
        # Call the function
        result = load_latest_backfilled_users()
        
        # Verify Athena was called
        mock_athena.query_results_as_df.assert_called_once()
        
        # Verify the result includes all columns
        assert len(result) == 2
        assert "did" in result[0]
        assert "bluesky_user_handle" in result[0]
        assert "timestamp" in result[0]
        assert "extra_col" in result[0]
        assert result[0]["extra_col"] == "value1"
        assert result[1]["extra_col"] == "value2"

    """Tests for write_user_session_backfill_metadata_to_db function.
    
    This test class verifies that the function correctly:
    - Handles empty metadata lists
    - Converts UserBackfillMetadata objects to dictionaries
    - Writes the data to S3
    """
    
    @pytest.fixture
    def mock_s3(self):
        """Fixture for mocking S3."""
        with patch("services.backfill.sync.session_metadata.s3") as mock:
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.session_metadata.logger") as mock:
            yield mock
    
    @pytest.fixture
    def mock_generate_timestamp(self):
        """Fixture for mocking generate_current_datetime_str."""
        with patch("services.backfill.sync.session_metadata.generate_current_datetime_str") as mock:
            mock.return_value = "2024-03-26-12:00:00"
            yield mock
    
    def test_write_user_session_backfill_metadata_to_db_empty(self, mock_s3, mock_logger):
        """Test with empty metadata list.
        
        When provided with an empty list, the function should:
        - Log a warning
        - Return early without writing to S3
        """
        # Call the function with an empty list
        write_user_session_backfill_metadata_to_db([])
        
        # Verify warning was logged
        mock_logger.warning.assert_called_once_with("No user backfill metadata to write to S3.")
        
        # Verify S3 was not called
        mock_s3.write_dicts_jsonl_to_s3.assert_not_called()
    
    def test_write_user_session_backfill_metadata_to_db(self, mock_s3, mock_logger, mock_generate_timestamp):
        """Test with valid metadata list.
        
        When provided with a valid list, the function should:
        - Convert the UserBackfillMetadata objects to dictionaries
        - Write the dictionaries to S3
        - Log success message
        """
        # Import here to avoid circular imports in the actual tests
        from services.backfill.sync.models import UserBackfillMetadata
        
        # Create metadata objects for testing
        metadata_objects = [
            UserBackfillMetadata(
                did="did:plc:user1",
                bluesky_handle="user1.bsky.social",
                types="post,like",
                total_records=15,
                total_records_by_type='{"post": 5, "like": 10}',
                pds_service_endpoint="https://bsky-pds.com",
                timestamp="2024-03-26-12:00:00"
            ),
            UserBackfillMetadata(
                did="did:plc:user2",
                bluesky_handle="user2.bsky.social", 
                types="post,follow",
                total_records=10,
                total_records_by_type='{"post": 3, "follow": 7}',
                pds_service_endpoint="https://bsky-pds.com",
                timestamp="2024-03-26-12:00:00"
            )
        ]
        
        # Call the function
        write_user_session_backfill_metadata_to_db(metadata_objects)
        
        # Verify S3 was called with the correct data
        mock_s3.write_dicts_jsonl_to_s3.assert_called_once()
        call_args = mock_s3.write_dicts_jsonl_to_s3.call_args[1]
        assert "data" in call_args
        assert "key" in call_args
        
        # Verify the key contains the timestamp
        assert "backfill_metadata/user_backfill_metadata_2024-03-26-12:00:00.jsonl" == call_args["key"]
        
        # Verify the data contains both dictionaries
        assert len(call_args["data"]) == 2
        assert call_args["data"][0]["did"] == "did:plc:user1"
        assert call_args["data"][1]["did"] == "did:plc:user2"
        
        # Verify success was logged
        mock_logger.info.assert_called_once()
        assert "Successfully wrote user backfill metadata to S3" in mock_logger.info.call_args[0][0] 
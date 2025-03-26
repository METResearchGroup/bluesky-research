"""Tests for helper.py."""

import pytest
from unittest.mock import patch, MagicMock

from services.backfill.sync.helper import (
    validate_dids,
    do_backfills_for_users,
)


class TestValidateDids:
    """Tests for validate_dids function.
    
    This test class verifies that the DIDs validation function works correctly,
    including:
    - Deduplication of DIDs
    - Format validation (did:plc:[alphanumeric])
    - Handling of invalid DIDs
    - Handling of empty DIDs
    """
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.helper.logger") as mock:
            yield mock
    
    def test_validate_dids_empty_list(self, mock_logger):
        """Test validation of empty DID list.
        
        When provided with an empty list, the function should return an empty list.
        """
        dids = []
        result = validate_dids(dids=dids)
        assert result == []
        mock_logger.info.assert_not_called()
        mock_logger.warning.assert_not_called()
        
    def test_validate_dids_with_valid_dids(self, mock_logger):
        """Test validation of valid DIDs.
        
        When provided with valid DIDs, the function should return the same DIDs.
        """
        dids = ["did:plc:user1", "did:plc:user2"]
        result = validate_dids(dids=dids)
        assert result == dids
        mock_logger.info.assert_not_called()
        mock_logger.warning.assert_not_called()
    
    def test_validate_dids_deduplication(self, mock_logger):
        """Test deduplication of DIDs.
        
        When provided with duplicate DIDs, the function should return only unique DIDs.
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user1", "did:plc:user2"]
        result = validate_dids(dids=dids)
        assert result == ["did:plc:user1", "did:plc:user2"]
        assert mock_logger.info.call_count >= 3  # At least 3 log messages (summary and 2 duplicates)
        
    def test_validate_dids_invalid_format(self, mock_logger):
        """Test validation of DIDs with invalid format.
        
        When provided with DIDs that don't match the expected format (did:plc:[alphanumeric]),
        the function should filter them out.
        """
        dids = [
            "did:plc:user1",        # Valid
            "did:other:user2",      # Invalid: wrong namespace
            "did:plc:USER3",        # Invalid: uppercase
            "did:plc:user-4",       # Invalid: non-alphanumeric
            "did:plc:",             # Invalid: missing identifier
            "did::user5",           # Invalid: wrong format
            "user6"                 # Invalid: not a DID
        ]
        result = validate_dids(dids=dids)
        assert result == ["did:plc:user1"]
        assert mock_logger.warning.call_count >= 5  # Warning for each invalid DID
        assert mock_logger.info.call_count >= 2  # At least summary messages
        
    def test_validate_dids_empty_strings(self, mock_logger):
        """Test validation of empty DIDs.
        
        When provided with empty strings, the function should filter them out.
        """
        dids = ["did:plc:user1", "", None, "did:plc:user2"]
        result = validate_dids(dids=dids)
        assert result == ["did:plc:user1", "did:plc:user2"]
        assert mock_logger.warning.call_count >= 2  # Warning for each empty DID
        
    def test_validate_dids_mixed_issues(self, mock_logger):
        """Test validation with mixed issues.
        
        When provided with a mixture of valid, invalid, duplicate, and empty DIDs,
        the function should handle all cases correctly.
        """
        dids = [
            "did:plc:user1",        # Valid
            "did:plc:user2",        # Valid
            "did:plc:user1",        # Duplicate
            "",                     # Empty
            "did:other:user3",      # Invalid format
            "did:plc:user2",        # Duplicate
            "did:plc:user4"         # Valid
        ]
        expected = ["did:plc:user1", "did:plc:user2", "did:plc:user4"]
        result = validate_dids(dids=dids)
        assert result == expected
        assert mock_logger.warning.call_count >= 2  # Warnings for empty and invalid format
        assert mock_logger.info.call_count >= 4  # Summary and duplicate logs


class TestDoBackfillsForUsers:
    """Tests for do_backfills_for_users function.
    
    This test class verifies that the backfill process for users works correctly,
    including:
    - Proper validation of DIDs
    - Calling the run_batched_backfill function with correct parameters
    - Formatting and returning the correct metadata
    """
    
    @patch("services.backfill.sync.helper.run_batched_backfill")
    @patch("services.backfill.sync.helper.generate_current_datetime_str")
    def test_do_backfills_for_users_empty_dids(self, mock_generate_timestamp, mock_run_batched_backfill):
        """Test backfill with empty DIDs list.
        
        When provided with an empty list of DIDs, the function should:
        - Pass the empty list to run_batched_backfill
        - Format the results correctly
        - Return a properly formatted metadata object
        """
        mock_generate_timestamp.return_value = "2024-03-26-12:00:00"
        mock_run_batched_backfill.return_value = {
            "total_batches": 0,
            "did_to_backfill_counts_map": {}
        }
        
        dids = []
        result = do_backfills_for_users(dids=dids)
        
        # Verify run_batched_backfill was called with correct parameters
        mock_run_batched_backfill.assert_called_once_with(
            dids=[],
            start_timestamp=None,
            end_timestamp=None,
        )
        
        # Verify result format and contents
        assert result["backfill_timestamp"] == "2024-03-26-12:00:00"
        assert result["dids"] == []
        assert result["total_dids"] == 0
        assert result["total_batches"] == 0
        assert result["did_to_backfill_counts_map"] == {}
        assert result["event"] is None
    
    @patch("services.backfill.sync.helper.run_batched_backfill")
    @patch("services.backfill.sync.helper.generate_current_datetime_str")
    def test_do_backfills_for_users_with_dids(self, mock_generate_timestamp, mock_run_batched_backfill):
        """Test backfill with DIDs list.
        
        When provided with a list of DIDs, the function should:
        - Pass the DIDs to run_batched_backfill
        - Format the results correctly
        - Return a properly formatted metadata object with counts
        """
        mock_generate_timestamp.return_value = "2024-03-26-12:00:00"
        mock_run_batched_backfill.return_value = {
            "total_batches": 2,
            "did_to_backfill_counts_map": {
                "did:plc:user1": {
                    "post": 5,
                    "like": 10
                },
                "did:plc:user2": {
                    "post": 3,
                    "follow": 7
                }
            }
        }
        
        dids = ["did:plc:user1", "did:plc:user2"]
        start_timestamp = "2024-01-01-00:00:00"
        end_timestamp = "2024-03-01-00:00:00"
        event = {"event_id": "test_event"}
        
        result = do_backfills_for_users(
            dids=dids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            event=event
        )
        
        # Verify run_batched_backfill was called with correct parameters
        mock_run_batched_backfill.assert_called_once_with(
            dids=dids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        
        # Verify result format and contents
        assert result["backfill_timestamp"] == "2024-03-26-12:00:00"
        assert result["dids"] == dids
        assert result["total_dids"] == 2
        assert result["total_batches"] == 2
        assert result["did_to_backfill_counts_map"] == {
            "did:plc:user1": {
                "post": 5,
                "like": 10
            },
            "did:plc:user2": {
                "post": 3,
                "follow": 7
            }
        }
        assert result["event"] == event
        
    @patch("services.backfill.sync.helper.validate_dids")
    @patch("services.backfill.sync.helper.run_batched_backfill")
    @patch("services.backfill.sync.helper.generate_current_datetime_str")
    def test_do_backfills_for_users_with_invalid_dids(
        self, mock_generate_timestamp, mock_run_batched_backfill, mock_validate_dids
    ):
        """Test backfill with invalid DIDs.
        
        When some DIDs are filtered out by validation, the function should:
        - Only pass valid DIDs to run_batched_backfill
        - Return metadata reflecting only the valid DIDs
        """
        mock_generate_timestamp.return_value = "2024-03-26-12:00:00"
        # Simulate validation filtering out invalid DIDs
        mock_validate_dids.return_value = ["did:plc:user1"]
        mock_run_batched_backfill.return_value = {
            "total_batches": 1,
            "did_to_backfill_counts_map": {
                "did:plc:user1": {
                    "post": 5,
                    "like": 10
                }
            }
        }
        
        # Mix of valid and invalid DIDs
        dids = ["did:plc:user1", "invalid:did", "did:plc:user2"]
        
        result = do_backfills_for_users(dids=dids)
        
        # Verify validate_dids was called with all DIDs
        mock_validate_dids.assert_called_once_with(dids=dids)
        
        # Verify run_batched_backfill was called with only valid DIDs
        mock_run_batched_backfill.assert_called_once_with(
            dids=["did:plc:user1"],
            start_timestamp=None,
            end_timestamp=None,
        )
        
        # Verify result includes only valid DIDs
        assert result["dids"] == ["did:plc:user1"]
        assert result["total_dids"] == 1 
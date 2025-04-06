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
    which currently just returns the input DIDs without modification.
    """
    
    def test_validate_dids_empty_list(self):
        """Test validation of empty DID list.
        
        When provided with an empty list, the function should return an empty list.
        """
        dids = []
        result = validate_dids(dids=dids)
        assert result == []
        
    def test_validate_dids_with_valid_dids(self):
        """Test validation of valid DIDs.
        
        When provided with valid DIDs, the function should return the same DIDs.
        In the current implementation, it simply returns the input list.
        """
        dids = ["did:plc:user1", "did:plc:user2"]
        result = validate_dids(dids=dids)
        assert result == dids


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
            "did_to_backfill_counts_map": {},
            "processed_users": 0,
            "total_users": 0,
            "user_backfill_metadata": []
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
        assert result["processed_users"] == 0
        assert result["total_users"] == 0
        assert result["user_backfill_metadata"] == []
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
        user_metadata = [MagicMock(), MagicMock()]
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
            },
            "processed_users": 2,
            "total_users": 2,
            "user_backfill_metadata": user_metadata
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
        assert result["processed_users"] == 2
        assert result["total_users"] == 2
        assert result["user_backfill_metadata"] == user_metadata
        assert result["event"] == event 
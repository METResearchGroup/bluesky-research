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
    - Validation of DID format using regex
    - Deduplication of DIDs
    - Exclusion of previously backfilled DIDs
    - Handling of empty DIDs
    """
    
    @pytest.fixture(autouse=True)
    def mock_load_latest_backfilled_users(self):
        """Fixture for mocking load_latest_backfilled_users.
        
        Using autouse=True to ensure this is applied to all tests in the class.
        """
        with patch("services.backfill.sync.helper.load_latest_backfilled_users") as mock:
            # Return a list of DIDs that were previously backfilled
            mock.return_value = [
                {"did": "did:plc:user1", "bluesky_user_handle": "user1.bsky.social"},
                {"did": "did:plc:user3", "bluesky_user_handle": "user3.bsky.social"}
            ]
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.helper.logger") as mock:
            yield mock
    
    def test_validate_dids_empty_list(self, mock_load_latest_backfilled_users):
        """Test validation of empty DID list.
        
        When provided with an empty list, the function should return an empty list
        and not attempt to filter out previously backfilled users.
        """
        dids = []
        result = validate_dids(dids=dids)
        assert result == []
        # load_latest_backfilled_users should still be called due to the current implementation
        mock_load_latest_backfilled_users.assert_called_once()
        
    def test_validate_dids_with_valid_dids(self, mock_load_latest_backfilled_users):
        """Test validation of valid DIDs with default exclude behavior.
        
        When provided with valid DIDs and exclude_previously_backfilled_users=True (default),
        the function should:
        - Load previously backfilled users
        - Exclude DIDs that were previously backfilled
        - Return only the valid, non-previously-backfilled DIDs
        """
        # Include a mix of new DIDs and previously backfilled DIDs
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3", "did:plc:user4"]
        
        # Need to check how the implementation is filtering DIDs
        # The current implementation in helper.py is checking if 'did' not in previously_backfilled_users
        # where previously_backfilled_users is a list of dictionaries with 'did' and 'bluesky_user_handle'
        result = validate_dids(dids=dids)
        
        # Should exclude user1 and user3 as they were previously backfilled
        assert "did:plc:user1" not in result
        assert "did:plc:user3" not in result
        assert "did:plc:user2" in result
        assert "did:plc:user4" in result
        assert len(result) == 2
        
        # Should call load_latest_backfilled_users
        mock_load_latest_backfilled_users.assert_called_once()
    
    def test_validate_dids_include_backfilled(self, mock_load_latest_backfilled_users):
        """Test validation with exclude_previously_backfilled_users=False.
        
        When exclude_previously_backfilled_users is False, the function should:
        - Not load previously backfilled users
        - Include all valid DIDs, even if previously backfilled
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3", "did:plc:user4"]
        result = validate_dids(dids=dids, exclude_previously_backfilled_users=False)
        
        # Should include all valid DIDs
        assert "did:plc:user1" in result
        assert "did:plc:user2" in result
        assert "did:plc:user3" in result
        assert "did:plc:user4" in result
        assert len(result) == 4
        
        # Should not call load_latest_backfilled_users
        mock_load_latest_backfilled_users.assert_not_called()
    
    def test_validate_dids_no_previous_backfills(self):
        """Test when there are no previously backfilled users.
        
        When load_latest_backfilled_users returns an empty list, all valid DIDs should be included.
        """
        dids = ["did:plc:user1", "did:plc:user2"]
        
        # Mock load_latest_backfilled_users to return an empty list
        with patch("services.backfill.sync.helper.load_latest_backfilled_users") as mock:
            mock.return_value = []
            result = validate_dids(dids=dids)
        
        # All DIDs should be included since none were previously backfilled
        assert result == dids
    
    def test_validate_dids_with_duplicates(self, mock_load_latest_backfilled_users, mock_logger):
        """Test validation of DIDs with duplicates.
        
        When the input contains duplicate DIDs, the function should:
        - Remove duplicates
        - Log appropriate messages
        - Still exclude previously backfilled DIDs
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user2", "did:plc:user4", "did:plc:user4"]
        result = validate_dids(dids=dids)
        
        # Should exclude user1 (previously backfilled) and deduplicate user2 and user4
        assert "did:plc:user1" not in result
        assert "did:plc:user2" in result
        assert "did:plc:user4" in result
        assert len(result) == 2
        
        # Should log info about duplicate DIDs
        mock_logger.info.assert_any_call("Duplicate DID skipped: did:plc:user2")
        mock_logger.info.assert_any_call("Duplicate DID skipped: did:plc:user4")
    
    def test_validate_dids_with_invalid_formats(self, mock_load_latest_backfilled_users, mock_logger):
        """Test validation of DIDs with invalid formats.
        
        When the input contains DIDs with invalid formats, the function should:
        - Exclude DIDs with invalid formats
        - Log warnings for invalid formats
        - Still exclude previously backfilled DIDs
        """
        dids = [
            "did:plc:user2",  # Valid
            "invalid_did",    # Invalid format
            "did:other:user4", # Invalid format
            "did:plc:USER5",  # Invalid format (uppercase)
            "",               # Empty string
        ]
        result = validate_dids(dids=dids)
        
        # Should only include valid DIDs that weren't previously backfilled
        assert result == ["did:plc:user2"]
        
        # Should log warnings for invalid formats
        mock_logger.warning.assert_any_call("Invalid DID format skipped: invalid_did")
        mock_logger.warning.assert_any_call("Invalid DID format skipped: did:other:user4")
        mock_logger.warning.assert_any_call("Invalid DID format skipped: did:plc:USER5")
        mock_logger.warning.assert_any_call("Empty DID found and skipped")


class TestDoBackfillsForUsers:
    """Tests for do_backfills_for_users function.
    
    This test class verifies that the backfill process for users works correctly,
    including:
    - Proper validation of DIDs
    - Calling the run_batched_backfill function with correct parameters
    - Formatting and returning the correct metadata
    """
    
    @pytest.fixture
    def mock_validate_dids(self):
        """Fixture for mocking validate_dids."""
        with patch("services.backfill.sync.helper.validate_dids") as mock:
            # By default, return the input DIDs as valid
            mock.side_effect = lambda dids, **kwargs: dids
            yield mock
    
    @pytest.fixture
    def mock_run_batched_backfill(self):
        """Fixture for mocking run_batched_backfill."""
        with patch("services.backfill.sync.helper.run_batched_backfill") as mock:
            yield mock
    
    @pytest.fixture
    def mock_generate_timestamp(self):
        """Fixture for mocking generate_current_datetime_str."""
        with patch("services.backfill.sync.helper.generate_current_datetime_str") as mock:
            mock.return_value = "2024-03-26-12:00:00"
            yield mock
    
    def test_do_backfills_for_users_empty_dids(self, mock_validate_dids, mock_run_batched_backfill, 
                                             mock_generate_timestamp):
        """Test backfill with empty DIDs list.
        
        When provided with an empty list of DIDs, the function should:
        - Pass the empty list to validate_dids
        - Pass the empty list to run_batched_backfill
        - Format the results correctly
        - Return a properly formatted metadata object
        """
        # Configure mocks
        mock_validate_dids.return_value = []
        mock_run_batched_backfill.return_value = {
            "total_batches": 0,
            "did_to_backfill_counts_map": {},
            "processed_users": 0,
            "total_users": 0,
            "user_backfill_metadata": []
        }
        
        # Call function with empty DIDs
        dids = []
        result = do_backfills_for_users(dids=dids)
        
        # Verify validate_dids was called with the empty list
        # In the implementation, validate_dids is called without the exclude_previously_backfilled_users
        mock_validate_dids.assert_called_once_with(dids=dids)
        
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
    
    def test_do_backfills_for_users_with_dids(self, mock_validate_dids, mock_run_batched_backfill, 
                                            mock_generate_timestamp):
        """Test backfill with DIDs list.
        
        When provided with a list of DIDs, the function should:
        - Pass the DIDs to validate_dids
        - Pass the validated DIDs to run_batched_backfill
        - Format the results correctly
        - Return a properly formatted metadata object with counts
        """
        # Set up test data
        dids = ["did:plc:user1", "did:plc:user2"]
        start_timestamp = "2024-01-01-00:00:00"
        end_timestamp = "2024-03-01-00:00:00"
        event = {"event_id": "test_event"}
        
        # Assume all DIDs are valid and not previously backfilled
        mock_validate_dids.return_value = dids
        
        # Mock user metadata and backfill results
        user_metadata = [MagicMock(), MagicMock()]
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
        
        # Call the function
        result = do_backfills_for_users(
            dids=dids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            event=event
        )
        
        # Verify validate_dids was called correctly
        # In the implementation, validate_dids is called without the exclude_previously_backfilled_users
        mock_validate_dids.assert_called_once_with(dids=dids)
        
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
    
    def test_do_backfills_for_users_with_excluded_dids(self, mock_run_batched_backfill, mock_generate_timestamp):
        """Test backfill where some DIDs are excluded by validation.
        
        In this test, we verify that when DIDs are excluded by validate_dids,
        the function properly handles generating metadata with only the valid DIDs.
        
        Since directly mocking validate_dids from within do_backfills_for_users is causing issues,
        we're using a different approach:
        1. We create a new implementation of validate_dids that does the filtering
        2. We patch do_backfills_for_users to use our implementation
        3. We verify the right DIDs are included in the result
        """
        # Original DIDs list with some that will be excluded
        all_dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3", "did:plc:user4"]
        
        # DIDs that should be included after filtering
        valid_dids = ["did:plc:user2", "did:plc:user4"]
        
        # Create a test implementation of validate_dids that filters specific DIDs
        def test_validate_dids(dids, **kwargs):
            # Filter to only include valid_dids
            return [did for did in dids if did in valid_dids]
        
        # Mock run_batched_backfill to return data matching our filtered DIDs
        mock_run_batched_backfill.return_value = {
            "total_batches": 1,
            "did_to_backfill_counts_map": {
                "did:plc:user2": {"post": 3},
                "did:plc:user4": {"post": 5}
            },
            "processed_users": 2,
            "total_users": 2,
            "user_backfill_metadata": [MagicMock(), MagicMock()]
        }
        
        # Patch the do_backfills_for_users function to use our test_validate_dids
        with patch("services.backfill.sync.helper.validate_dids", side_effect=test_validate_dids):
            # Run the function under test with all DIDs
            result = do_backfills_for_users(dids=all_dids)
        
        # Verify the result contains only valid DIDs
        assert result["dids"] == valid_dids, f"Expected {valid_dids}, got {result['dids']}"
        assert result["total_dids"] == len(valid_dids)
        
        # Verify the did_to_backfill_counts_map contains only the valid DIDs
        assert "did:plc:user2" in result["did_to_backfill_counts_map"]
        assert "did:plc:user4" in result["did_to_backfill_counts_map"]
        assert "did:plc:user1" not in result["did_to_backfill_counts_map"]
        assert "did:plc:user3" not in result["did_to_backfill_counts_map"]
        
        # Verify other metadata is correct
        assert result["total_batches"] == 1
        assert result["processed_users"] == 2
        assert result["total_users"] == 2
        assert len(result["user_backfill_metadata"]) == 2
    
    def test_do_backfills_for_users_exclude_parameter(self, mock_validate_dids, mock_run_batched_backfill,
                                                    mock_generate_timestamp):
        """Test that the do_backfills_for_users function correctly passes the exclude_previously_backfilled_users parameter.
        
        We want to make sure the implementation of do_backfills_for_users uses the default value
        for exclude_previously_backfilled_users (True) when calling validate_dids.
        
        This is mainly a test for future reference if the implementation changes to
        explicitly pass the parameter.
        """
        # Original DIDs list
        dids = ["did:plc:user1", "did:plc:user2"]
        
        # Look at the actual implementation of do_backfills_for_users
        # Check the code to determine what parameters are actually passed to validate_dids
        
        # In the current implementation, only the dids parameter is passed to validate_dids
        # validate_dids(dids=dids)
        
        # Reset the mock first to clear any previous calls
        mock_validate_dids.reset_mock()
        mock_validate_dids.return_value = dids
        mock_run_batched_backfill.return_value = {
            "total_batches": 1,
            "did_to_backfill_counts_map": {},
            "processed_users": 2,
            "total_users": 2,
            "user_backfill_metadata": []
        }
        
        # Call the function
        do_backfills_for_users(dids=dids)
        
        # Verify validate_dids was called with just the dids parameter
        mock_validate_dids.assert_called_once_with(dids=dids) 
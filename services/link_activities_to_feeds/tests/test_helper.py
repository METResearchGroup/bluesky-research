"""Unit tests for the helper functions in the link_activities_to_feeds service."""

from datetime import datetime

import pandas as pd
import pytest

from services.link_activities_to_feeds.helper import (
    assign_session_hash,
    split_user_session_logs_by_sliding_window
)

def test_assign_session_hash_basic():
    """Tests that assign_session_hash correctly assigns bucket numbers for 5-minute windows.
    
    Tests a variety of timestamps throughout the day to verify they are assigned to the
    correct 5-minute bucket numbers. For example, 12:39am should be bucket 7 since it falls
    between 00:35-00:40.
    """
    timestamps = [
        "2024-01-01-00:39:00",  # 12:39am
        "2024-02-28-11:31:00",  # 11:31am
        "2024-03-15-14:47:00",  # 2:47pm
        "2024-06-03-16:16:00",  # 4:16pm
        "2024-08-15-18:00:00",  # 6:00pm
        "2024-11-30-21:03:00"   # 9:03pm
    ]
    window_period = 5
    expected_hashes = [
        "2024_01_01_5_7",    # 00:39 falls into bucket 7 (00:35-00:40)
        "2024_02_28_5_138",  # 11:31 falls into bucket 138 (11:30-11:35)
        "2024_03_15_5_177",  # 14:47 falls into bucket 177 (14:45-14:50)
        "2024_06_03_5_195",  # 16:16 falls into bucket 195 (16:15-16:20)
        "2024_08_15_5_216",  # 18:00 falls into bucket 216 (18:00-18:05)
        "2024_11_30_5_252"   # 21:03 falls into bucket 252 (21:00-21:05)
    ]
    for timestamp, expected_hash in zip(timestamps, expected_hashes):
        assert assign_session_hash(timestamp, window_period) == expected_hash

def test_assign_session_hash_midnight():
    """Tests that assign_session_hash correctly assigns bucket numbers for 15-minute windows.
    
    Tests the same timestamps as the basic test but with 15-minute windows instead of 5-minute
    windows. Verifies that bucket numbers are correctly adjusted for the larger window size.
    """
    timestamps = [
        "2024-01-01-00:39:00",  # 12:39am
        "2024-02-28-11:31:00",  # 11:31am
        "2024-03-15-14:47:00",  # 2:47pm
        "2024-06-03-16:16:00",  # 4:16pm
        "2024-08-15-18:00:00",  # 6:00pm
        "2024-11-30-21:03:00"   # 9:03pm
    ]
    window_period = 15
    expected_hashes = [
        "2024_01_01_15_2",   # 00:39 falls into bucket 2 (00:30-00:45)
        "2024_02_28_15_46",  # 11:31 falls into bucket 46 (11:30-11:45)
        "2024_03_15_15_59",  # 14:47 falls into bucket 59 (14:45-15:00)
        "2024_06_03_15_65",  # 16:16 falls into bucket 65 (16:15-16:30)
        "2024_08_15_15_72",  # 18:00 falls into bucket 72 (18:00-18:15)
        "2024_11_30_15_84"   # 21:03 falls into bucket 84 (21:00-21:15)
    ]
    for timestamp, expected_hash in zip(timestamps, expected_hashes):
        assert assign_session_hash(timestamp, window_period) == expected_hash

def test_assign_session_hash_different_window():
    """Tests that assign_session_hash correctly assigns bucket numbers for 30-minute windows.
    
    Tests the same timestamps as previous tests but with 30-minute windows. Verifies that
    bucket numbers are correctly calculated for half-hour intervals.
    """
    timestamps = [
        "2024-01-01-00:39:00",  # 12:39am
        "2024-02-28-11:31:00",  # 11:31am
        "2024-03-15-14:47:00",  # 2:47pm
        "2024-06-03-16:16:00",  # 4:16pm
        "2024-08-15-18:00:00",  # 6:00pm
        "2024-11-30-21:03:00"   # 9:03pm
    ]
    window_period = 30
    expected_hashes = [
        "2024_01_01_30_1",   # 00:39 falls into bucket 1 (00:30-01:00)
        "2024_02_28_30_23",  # 11:31 falls into bucket 23 (11:30-12:00)
        "2024_03_15_30_29",  # 14:47 falls into bucket 29 (14:30-15:00)
        "2024_06_03_30_32",  # 16:16 falls into bucket 32 (16:00-16:30)
        "2024_08_15_30_36",  # 18:00 falls into bucket 36 (18:00-18:30)
        "2024_11_30_30_42"   # 21:03 falls into bucket 42 (21:00-21:30)
    ]
    for timestamp, expected_hash in zip(timestamps, expected_hashes):
        assert assign_session_hash(timestamp, window_period) == expected_hash

def test_assign_session_hash_end_of_day():
    """Tests that assign_session_hash correctly handles timestamps at the end of the day.
    
    Tests timestamps at 23:59 for different dates to verify they all fall into the same
    bucket number (287 for 5-minute windows) since they're all in the 23:55-00:00 window.
    """
    timestamps = [
        "2024-01-01-23:59:00",
        "2024-02-28-23:59:00",
        "2024-03-15-23:59:00",
        "2024-06-03-23:59:00",
        "2024-08-15-23:59:00",
        "2024-11-30-23:59:00"
    ]
    window_period = 5
    expected_hashes = [
        "2024_01_01_5_287",  # 23:59 falls into bucket 287 (23:55-00:00)
        "2024_02_28_5_287",
        "2024_03_15_5_287",
        "2024_06_03_5_287",
        "2024_08_15_5_287",
        "2024_11_30_5_287"
    ]
    for timestamp, expected_hash in zip(timestamps, expected_hashes):
        assert assign_session_hash(timestamp, window_period) == expected_hash

def test_assign_session_hash_different_dates():
    """Tests that assign_session_hash generates unique hashes for different dates.
    
    Tests that timestamps from different dates generate different hashes, even if they
    fall into the same bucket number within their respective days. Also verifies that
    each hash matches its expected format and value.
    """
    timestamps = [
        "2024-01-01-00:39:00",  # 12:39am
        "2024-02-28-11:31:00",  # 11:31am
        "2024-03-15-14:47:00",  # 2:47pm
        "2024-06-03-16:16:00",  # 4:16pm
        "2024-08-15-18:00:00",  # 6:00pm
        "2024-11-30-21:03:00"   # 9:03pm
    ]
    window_period = 5
    expected_hashes = [
        "2024_01_01_5_7",    # 00:39 falls into bucket 7 (00:35-00:40)
        "2024_02_28_5_138",  # 11:31 falls into bucket 138 (11:30-11:35)
        "2024_03_15_5_177",  # 14:47 falls into bucket 177 (14:45-14:50)
        "2024_06_03_5_195",  # 16:16 falls into bucket 195 (16:15-16:20)
        "2024_08_15_5_216",  # 18:00 falls into bucket 216 (18:00-18:05)
        "2024_11_30_5_252"   # 21:03 falls into bucket 252 (21:00-21:05)
    ]
    
    # Test that all hashes are different from each other
    generated_hashes = [assign_session_hash(t, window_period) for t in timestamps]
    for i, hash1 in enumerate(generated_hashes):
        # Verify each hash matches its expected value
        assert hash1 == expected_hashes[i]
        # Verify each hash is different from subsequent hashes
        for hash2 in generated_hashes[i+1:]:
            assert hash1 != hash2

def test_split_user_session_logs_empty_df():
    """Tests that split_user_session_logs_by_sliding_window handles empty dataframes."""
    empty_df = pd.DataFrame()
    window_period = 5
    result = split_user_session_logs_by_sliding_window(empty_df, window_period)
    assert len(result) == 0

def test_split_user_session_logs_two_complete_windows():
    """Tests splitting logs into two windows, each with complete feed requests."""
    window_period = 5
    
    # Create test data with two windows, each having 4 records ending in eof
    data = {
        'activity_timestamp': [
            # First window
            '2024-01-01-12:00:00',
            '2024-01-01-12:01:00', 
            '2024-01-01-12:02:00',
            '2024-01-01-12:03:00',
            # Second window 
            '2024-01-01-12:10:00',
            '2024-01-01-12:11:00',
            '2024-01-01-12:12:00', 
            '2024-01-01-12:13:00'
        ],
        'cursor': [
            'start',
            'abc123',
            'def456',
            'eof',
            'start',
            'ghi789',
            'jkl012',
            'eof'
        ]
    }
    df = pd.DataFrame(data)
    
    result = split_user_session_logs_by_sliding_window(df, window_period)
    
    assert len(result) == 2  # Two windows
    assert len(result[0]) == 4  # First window has 4 records
    assert len(result[1]) == 4  # Second window has 4 records
    assert result[0].iloc[-1]['cursor'] == 'eof'  # First window ends with eof
    assert result[1].iloc[-1]['cursor'] == 'eof'  # Second window ends with eof

def test_split_user_session_logs_windows_with_multiple_feeds():
    """Tests splitting logs where each window contains multiple feeds."""
    window_period = 5
    
    # Create test data with two windows, each having 6 records with eof at position 4
    data = {
        'activity_timestamp': [
            # First window
            '2024-01-01-12:00:00',
            '2024-01-01-12:01:00',
            '2024-01-01-12:02:00',
            '2024-01-01-12:03:00',  # eof here
            '2024-01-01-12:04:00',
            '2024-01-01-12:04:30',
            # Second window
            '2024-01-01-12:10:00',
            '2024-01-01-12:11:00',
            '2024-01-01-12:12:00',
            '2024-01-01-12:13:00',  # eof here
            '2024-01-01-12:14:00',
            '2024-01-01-12:14:30'
        ],
        'cursor': [
            'start',
            'abc123',
            'def456',
            'eof',
            'start2',
            'ghi789',
            'start3',
            'jkl012',
            'mno345',
            'eof',
            'start4',
            'pqr678'
        ]
    }
    df = pd.DataFrame(data)
    
    result = split_user_session_logs_by_sliding_window(df, window_period)
    
    assert len(result) == 4  # Four total groups (2 per window)
    
    # First window groups
    assert len(result[0]) == 4  # First feed in window 1 (up to eof)
    assert len(result[1]) == 2  # Second feed in window 1 (remaining records)
    assert result[0].iloc[-1]['cursor'] == 'eof'
    
    # Second window groups
    assert len(result[2]) == 4  # First feed in window 2 (up to eof)
    assert len(result[3]) == 2  # Second feed in window 2 (remaining records)
    assert result[2].iloc[-1]['cursor'] == 'eof'


def test_split_user_session_logs_by_sliding_window_no_eof():
    """Tests splitting user session logs by sliding window when there are no EOF cursors.
    
    Creates test data with two time windows, each containing 3 records with no EOF cursors.
    Verifies that the records are correctly split into two groups based on the time window,
    with each group containing the expected number of records.
    """
    window_period = 5

    # Create test data with two windows, each having 3 records with no eof
    data = {
        'activity_timestamp': [
            # First window
            '2024-01-01-12:00:00',
            '2024-01-01-12:01:00', 
            '2024-01-01-12:02:00',
            # Second window
            '2024-01-01-12:10:00',
            '2024-01-01-12:11:00',
            '2024-01-01-12:12:00'
        ],
        'cursor': [
            'start1',
            'abc123',
            'def456',
            'start2', 
            'ghi789',
            'jkl012'
        ]
    }
    df = pd.DataFrame(data)
    
    result = split_user_session_logs_by_sliding_window(df, window_period)
    
    assert len(result) == 2  # Two total groups (1 per window)
    
    # First window group
    assert len(result[0]) == 3  # First feed has 3 records
    
    # Second window group  
    assert len(result[1]) == 3  # Second feed has 3 records

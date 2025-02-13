"""Tests for count_records_for_integration.py.

This test suite verifies the functionality of record counting across integrations:
- Date range calculation
- Record counting
- Output formatting
- Error handling
"""

import io
import sys
from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from services.calculate_analytics.count_records_for_integration import (
    count_records_for_dates,
    print_record_counts,
    main,
)

@pytest.fixture
def mock_df():
    """Create a mock DataFrame for testing."""
    return pd.DataFrame({
        "id": range(5),
        "value": ["test"] * 5
    })

def test_count_records_for_dates(mock_df):
    """Test counting records for multiple dates."""
    with patch("services.calculate_analytics.count_records_for_integration.load_data_from_local_storage") as mock_load:
        mock_load.return_value = mock_df
        dates = ["2024-10-01", "2024-10-02"]
        
        counts = count_records_for_dates("test_integration", dates)
        
        assert len(counts) == 2
        assert counts["2024-10-01"] == 5
        assert counts["2024-10-02"] == 5
        assert mock_load.call_count == 2

def test_count_records_with_missing_data(mock_df):
    """Test counting records when some dates have no data."""
    with patch("services.calculate_analytics.count_records_for_integration.load_data_from_local_storage") as mock_load:
        mock_load.side_effect = [mock_df, None]
        dates = ["2024-10-01", "2024-10-02"]
        
        counts = count_records_for_dates("test_integration", dates)
        
        assert len(counts) == 2
        assert counts["2024-10-01"] == 5
        assert counts["2024-10-02"] == 0

def test_count_records_with_error():
    """Test counting records when loading fails."""
    with patch("services.calculate_analytics.count_records_for_integration.load_data_from_local_storage") as mock_load:
        mock_load.side_effect = Exception("Test error")
        dates = ["2024-10-01"]
        
        counts = count_records_for_dates("test_integration", dates)
        
        assert len(counts) == 1
        assert counts["2024-10-01"] == 0

def test_print_record_counts():
    """Test printing record counts in the correct format."""
    counts = {
        "2024-10-01": 5,
        "2024-10-02": 10,
    }
    
    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        print_record_counts(
            "test_integration",
            counts,
            "2024-10-01",
            "2024-10-02"
        )
        
        output = captured_output.getvalue()
        assert "test_integration" in output
        assert "2024-10-01" in output
        assert "2024-10-02" in output
        assert "5" in output
        assert "10" in output
    finally:
        sys.stdout = sys.__stdout__

def test_main_with_valid_args():
    """Test main function with valid arguments."""
    test_args = [
        "--integration", "test_integration",
        "--start_date", "2024-10-14",
        "--end_date", "2024-10-15"
    ]
    
    with patch("sys.argv", ["script.py"] + test_args):
        with patch("services.calculate_analytics.count_records_for_integration.count_records_for_dates") as mock_count:
            with patch("services.calculate_analytics.count_records_for_integration.get_partition_dates") as mock_dates:
                mock_count.return_value = {"2024-10-14": 5, "2024-10-15": 10}
                mock_dates.return_value = ["2024-10-14", "2024-10-15"]

                # Capture stdout
                captured_output = io.StringIO()
                sys.stdout = captured_output
                
                try:
                    main()
                    
                    output = captured_output.getvalue()
                    assert "test_integration" in output
                    assert "2024-10-14" in output
                    assert "2024-10-15" in output
                    assert "5" in output
                    assert "10" in output
                finally:
                    sys.stdout = sys.__stdout__

def test_main_with_invalid_date_format():
    """Test main function with invalid date format."""
    test_args = [
        "--integration", "test_integration",
        "--start_date", "2024/10/14",
        "--end_date", "2024-10-15"
    ]
    
    with patch("sys.argv", ["script.py"] + test_args):
        with pytest.raises(SystemExit):
            main() 
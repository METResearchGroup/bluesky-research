"""Tests for check_number_records_in_db.py.

This test suite verifies the functionality of database record counting:
- Metadata parsing
- Record counting
- Output formatting
- Error handling
"""

import io
import json
import sys
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from services.calculate_analytics.check_number_records_in_db import (
    parse_metadata_batch_sizes,
    analyze_queue_records,
    print_analysis_results,
    main,
)

@pytest.fixture
def mock_df():
    """Create a mock DataFrame with metadata for testing."""
    return pd.DataFrame({
        "metadata": [
            json.dumps({"batch_size": 100, "other_field": "value"}),
            json.dumps({"batch_size": 150, "other_field": "value"}),
            json.dumps({"batch_size": 75, "other_field": "value"}),
        ]
    })

def test_parse_metadata_batch_sizes(mock_df):
    """Test parsing batch sizes from metadata."""
    batch_sizes = parse_metadata_batch_sizes(mock_df)
    
    assert len(batch_sizes) == 3
    assert batch_sizes == [100, 150, 75]

def test_parse_metadata_with_invalid_json():
    """Test handling of invalid JSON in metadata."""
    df = pd.DataFrame({
        "metadata": [
            '{"batch_size": 100}',  # Valid
            'invalid json',         # Invalid
            '{"no_batch_size": 0}'  # Missing batch_size
        ]
    })
    
    batch_sizes = parse_metadata_batch_sizes(df)
    
    assert len(batch_sizes) == 1
    assert batch_sizes == [100]

def test_analyze_queue_records(mock_df):
    """Test analyzing records from a queue."""
    with patch("services.calculate_analytics.check_number_records_in_db.load_data_from_local_storage") as mock_load:
        mock_load.return_value = mock_df
        
        total_records, batch_sizes = analyze_queue_records("test_queue")
        
        assert total_records == 325  # 100 + 150 + 75
        assert batch_sizes == [100, 150, 75]
        mock_load.assert_called_once_with(
            service="test_queue",
            output_format="df"
        )

def test_analyze_queue_with_no_data():
    """Test analyzing an empty queue."""
    with patch("services.calculate_analytics.check_number_records_in_db.load_data_from_local_storage") as mock_load:
        mock_load.return_value = None
        
        total_records, batch_sizes = analyze_queue_records("empty_queue")
        
        assert total_records == 0
        assert batch_sizes == []

def test_print_analysis_results():
    """Test printing analysis results."""
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        print_analysis_results(
            "input_test_integration",
            325,
            [100, 150, 75]
        )
        
        output = captured_output.getvalue()
        assert "Integration: test_integration" in output
        assert "Total number of records: 325" in output
        assert "Number of records per row:" in output
        assert "- 100" in output
        assert "- 150" in output
        assert "- 75" in output
    finally:
        sys.stdout = sys.__stdout__

def test_print_analysis_results_no_batches():
    """Test printing results when no batch sizes are found."""
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    try:
        print_analysis_results(
            "input_test_integration",
            0,
            []
        )
        
        output = captured_output.getvalue()
        assert "Integration: test_integration" in output
        assert "Total number of records: 0" in output
        assert "No batch size information found in metadata" in output
    finally:
        sys.stdout = sys.__stdout__

def test_main_with_valid_args():
    """Test main function with valid arguments."""
    test_args = ["--queue", "input_test_integration"]
    
    with patch("sys.argv", ["script.py"] + test_args):
        with patch("services.calculate_analytics.check_number_records_in_db.analyze_queue_records") as mock_analyze:
            mock_analyze.return_value = (325, [100, 150, 75])
            
            # Capture stdout
            captured_output = io.StringIO()
            sys.stdout = captured_output
            
            try:
                main()
                
                output = captured_output.getvalue()
                assert "Integration: test_integration" in output
                assert "Total number of records: 325" in output
                assert "- 100" in output
                assert "- 150" in output
                assert "- 75" in output
            finally:
                sys.stdout = sys.__stdout__

def test_main_with_missing_args():
    """Test main function with missing required arguments."""
    with patch("sys.argv", ["script.py"]):
        with pytest.raises(SystemExit):
            main() 
"""Tests for lib/db/manage_local_data.py"""
import os
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from lib.aws.s3 import S3
from lib.db.manage_local_data import (
    find_files_after_timestamp,
    list_filenames,
    load_data_from_local_storage,
)


@pytest.mark.parametrize("timestamp_str,expected", [
    ("2024-07-06-20:39:30", "year=2024/month=07/day=06/hour=20/minute=39"),
    ("2025-12-31-23:59:59", "year=2025/month=12/day=31/hour=23/minute=59"),
])
def test_create_partition_key_based_on_timestamp(timestamp_str, expected):
    """Test create_partition_key_based_on_timestamp"""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        s3 = S3(create_client_flag=False)
        assert (
            s3.create_partition_key_based_on_timestamp(timestamp_str) == expected
        )


# Setup for find_files_after_timestamp tests


@pytest.fixture
def setup_directory_structure():
    """Create a temporary directory with test files."""
    with TemporaryDirectory() as tmpdir:
        # Create sample directory structures and test files
        timestamps = [
            ("2024", "01", "01", "01", "01"),  # shouldn't be included
            ("2024", "07", "01", "01", "01"),  # shouldn't be included
            ("2024", "07", "05", "20", "59"),
            ("2024", "07", "05", "21", "01"),
            ("2024", "07", "06", "01", "01"),
            ("2024", "08", "01", "01", "01"),
        ]
        for year, month, day, hour, minute in timestamps:
            dir_path = os.path.join(tmpdir, f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}")
            os.makedirs(dir_path, exist_ok=True)
            test_file_path = os.path.join(dir_path, "test_file.txt")
            with open(test_file_path, "w") as f:
                f.write(f"This is a test file for {year}/{month}/{day} {hour}:{minute}.")
        yield tmpdir


def test_find_files_after_timestamp(setup_directory_structure):
    """Test find_files_after_timestamp"""
    base_path = setup_directory_structure
    # A minute before the test file's timestamp
    target_timestamp = "year=2024/month=07/day=05/hour=20/minute=58"
    files_found = find_files_after_timestamp(base_path, target_timestamp)
    assert len(files_found) == 4


@pytest.fixture
def mock_parquet_files():
    """Create a mock list of parquet files with different partition dates."""
    return [
        "/data/service/active/partition_date=2024-03-01/file1.parquet",
        "/data/service/active/partition_date=2024-03-02/file2.parquet",
        "/data/service/active/partition_date=2024-03-03/file3.parquet",
        "/data/service/active/partition_date=2024-03-04/file4.parquet",
        "/data/service/cache/partition_date=2024-02-28/file5.parquet",
    ]

@pytest.mark.parametrize("test_params", [
    {
        "partition_date": "2024-03-02",
        "start_date": None,
        "end_date": None,
        "expected_count": 1,
        "directories": ["active"],
    },
    {
        "partition_date": None,
        "start_date": "2024-03-02",
        "end_date": "2024-03-03",
        "expected_count": 2,
        "directories": ["active"],
    },
    {
        "partition_date": None,
        "start_date": "2024-02-28",
        "end_date": "2024-03-04",
        "expected_count": 5,
        "directories": ["active", "cache"],
    },
])
def test_list_filenames_with_date_filters(mock_parquet_files, test_params):
    """Test list_filenames with various date filtering scenarios."""
    with patch('os.walk') as mock_walk:
        # Setup mock to return our test files
        mock_walk.return_value = [("/data/service/active", [], mock_parquet_files)]
        
        result = list_filenames(
            service="test_service",
            directories=test_params["directories"],
            partition_date=test_params["partition_date"],
            start_partition_date=test_params["start_date"],
            end_partition_date=test_params["end_date"],
        )
        
        assert len(result) == test_params["expected_count"]

def test_list_filenames_invalid_date_params():
    """Test list_filenames raises error when both partition_date and date range are provided."""
    with pytest.raises(ValueError) as exc_info:
        list_filenames(
            service="test_service",
            partition_date="2024-03-01",
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-02",
        )
    assert "Cannot use partition_date and start_partition_date or end_partition_date together" in str(exc_info.value)

@pytest.mark.parametrize("test_params", [
    {
        "partition_date": "2024-03-02",
        "start_date": None,
        "end_date": None,
        "latest_timestamp": None,
        "expected_files": 1,
    },
    {
        "partition_date": None,
        "start_date": "2024-03-01",
        "end_date": "2024-03-03",
        "latest_timestamp": "2024-03-02T00:00:00",
        "expected_files": 2,
    },
])
def test_load_data_from_local_storage_with_date_filters(mock_parquet_files, test_params):
    """Test load_data_from_local_storage with various date filtering scenarios."""
    mock_df = pd.DataFrame({
        'timestamp': ['2024-03-02T12:00:00', '2024-03-03T12:00:00'],
        'value': [1, 2]
    })
    
    with patch('pandas.read_parquet') as mock_read_parquet, \
         patch('lib.db.manage_local_data.list_filenames') as mock_list_filenames:
        
        mock_read_parquet.return_value = mock_df
        mock_list_filenames.return_value = mock_parquet_files[:test_params["expected_files"]]
        
        result_df = load_data_from_local_storage(
            service="test_service",
            partition_date=test_params["partition_date"],
            start_partition_date=test_params["start_date"],
            end_partition_date=test_params["end_date"],
            latest_timestamp=test_params["latest_timestamp"],
        )
        
        assert len(result_df) > 0
        if test_params["latest_timestamp"]:
            assert all(pd.to_datetime(result_df['timestamp']) >= pd.to_datetime(test_params["latest_timestamp"]))

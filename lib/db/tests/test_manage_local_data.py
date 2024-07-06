"""Tests for lib/db/manage_local_data.py"""
import os
from tempfile import TemporaryDirectory

import pytest

from lib.aws.s3 import S3
from lib.db.manage_local_data import find_files_after_timestamp

# Test cases for create_partition_key_based_on_timestamp


@pytest.mark.parametrize("timestamp_str,expected", [
    ("2024-07-06-20:39:30", "year=2024/month=07/day=06/hour=20/minute=39"),
    ("2025-12-31-23:59:59", "year=2025/month=12/day=31/hour=23/minute=59"),
])
def test_create_partition_key_based_on_timestamp(timestamp_str, expected):
    """Test create_partition_key_based_on_timestamp"""
    assert (
        S3.create_partition_key_based_on_timestamp(timestamp_str) == expected
    )

# Setup for find_files_after_timestamp tests


@pytest.fixture
def setup_directory_structure():
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

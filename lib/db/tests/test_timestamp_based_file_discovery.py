"""Tests for lib/db/timestamp_based_file_discovery.py"""
import os
from tempfile import TemporaryDirectory

import pytest

from lib.db.timestamp_based_file_discovery import (
    _fetch_files_after_timestamp_day,
    _fetch_files_after_timestamp_hour,
    _fetch_files_after_timestamp_minute,
    _fetch_files_after_timestamp_month,
    _fetch_files_after_timestamp_year,
    _fetch_files_in_directory,
    _fetch_all_files_after_timestamp_day,
    _fetch_all_files_after_timestamp_hour,
    _fetch_all_files_after_timestamp_minute,
    _fetch_all_files_after_timestamp_month,
    _fetch_all_files_after_timestamp_year,
    find_files_after_timestamp,
)


@pytest.fixture
def sample_timestamp_structure():
    """Create a temporary directory with sample timestamp-based structure."""
    with TemporaryDirectory() as tmpdir:
        timestamps = [
            ("2024", "01", "01", "01", "01"),
            ("2024", "07", "01", "01", "01"),
            ("2024", "07", "05", "20", "59"),
            ("2024", "07", "05", "21", "01"),
            ("2024", "07", "06", "01", "01"),
            ("2024", "08", "01", "01", "01"),
        ]
        for year, month, day, hour, minute in timestamps:
            dir_path = os.path.join(
                tmpdir, f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}"
            )
            os.makedirs(dir_path, exist_ok=True)
            test_file_path = os.path.join(dir_path, "test_file.txt")
            with open(test_file_path, "w") as f:
                f.write(f"This is a test file for {year}/{month}/{day} {hour}:{minute}.")
        yield tmpdir


@pytest.fixture
def simple_directory_structure():
    """Create a simple directory structure for basic file fetching tests."""
    with TemporaryDirectory() as tmpdir:
        # Create a nested structure with files
        subdir1 = os.path.join(tmpdir, "subdir1")
        subdir2 = os.path.join(tmpdir, "subdir2", "nested")
        os.makedirs(subdir1, exist_ok=True)
        os.makedirs(subdir2, exist_ok=True)
        
        files = [
            os.path.join(tmpdir, "file1.txt"),
            os.path.join(subdir1, "file2.txt"),
            os.path.join(subdir2, "file3.txt"),
        ]
        for file_path in files:
            with open(file_path, "w") as f:
                f.write("test content")
        yield tmpdir


class TestFetchFilesInDirectory:
    """Tests for _fetch_files_in_directory function."""

    def test_fetches_all_files_in_directory(self, simple_directory_structure):
        """Test that all files in directory and subdirectories are fetched."""
        # Arrange
        directory_path = simple_directory_structure
        expected_files = [
            os.path.join(directory_path, "file1.txt"),
            os.path.join(directory_path, "subdir1", "file2.txt"),
            os.path.join(directory_path, "subdir2", "nested", "file3.txt"),
        ]
        
        # Act
        result = _fetch_files_in_directory(directory_path)
        
        # Assert
        assert len(result) == 3
        assert set(result) == set(expected_files)

    def test_returns_empty_list_for_empty_directory(self, tmp_path):
        """Test that empty directory returns empty list."""
        # Arrange
        empty_dir = str(tmp_path)
        
        # Act
        result = _fetch_files_in_directory(empty_dir)
        
        # Assert
        assert result == []

    def test_handles_single_file_in_root(self, tmp_path):
        """Test handling of single file in root directory."""
        # Arrange
        file_path = tmp_path / "single_file.txt"
        file_path.write_text("content")
        
        # Act
        result = _fetch_files_in_directory(str(tmp_path))
        
        # Assert
        assert len(result) == 1
        assert result[0] == str(file_path)


class TestFetchFilesAfterTimestampYear:
    """Tests for _fetch_files_after_timestamp_year function."""

    def test_fetches_files_after_year(self, sample_timestamp_structure):
        """Test that files are fetched from directories after the year."""
        # Arrange
        directory_path = os.path.join(sample_timestamp_structure, "year=2024")
        
        # Act
        result = _fetch_files_after_timestamp_year(directory_path)
        
        # Assert
        assert len(result) > 0
        assert all("year=2024" in f for f in result)


class TestFetchFilesAfterTimestampMonth:
    """Tests for _fetch_files_after_timestamp_month function."""

    def test_fetches_files_after_month(self, sample_timestamp_structure):
        """Test that files are fetched from directories after the month."""
        # Arrange
        directory_path = os.path.join(sample_timestamp_structure, "year=2024", "month=07")
        
        # Act
        result = _fetch_files_after_timestamp_month(directory_path)
        
        # Assert
        assert len(result) > 0
        assert all("month=07" in f for f in result)


class TestFetchFilesAfterTimestampDay:
    """Tests for _fetch_files_after_timestamp_day function."""

    def test_fetches_files_after_day(self, sample_timestamp_structure):
        """Test that files are fetched from directories after the day."""
        # Arrange
        directory_path = os.path.join(
            sample_timestamp_structure, "year=2024", "month=07", "day=05"
        )
        
        # Act
        result = _fetch_files_after_timestamp_day(directory_path)
        
        # Assert
        assert len(result) > 0
        assert all("day=05" in f for f in result)


class TestFetchFilesAfterTimestampHour:
    """Tests for _fetch_files_after_timestamp_hour function."""

    def test_fetches_files_after_hour(self, sample_timestamp_structure):
        """Test that files are fetched from directories after the hour."""
        # Arrange
        directory_path = os.path.join(
            sample_timestamp_structure, "year=2024", "month=07", "day=05", "hour=20"
        )
        
        # Act
        result = _fetch_files_after_timestamp_hour(directory_path)
        
        # Assert
        assert len(result) > 0
        assert all("hour=20" in f for f in result)


class TestFetchFilesAfterTimestampMinute:
    """Tests for _fetch_files_after_timestamp_minute function."""

    def test_fetches_files_after_minute(self, sample_timestamp_structure):
        """Test that files are fetched from directories after the minute."""
        # Arrange
        directory_path = os.path.join(
            sample_timestamp_structure,
            "year=2024",
            "month=07",
            "day=05",
            "hour=20",
            "minute=59",
        )
        
        # Act
        result = _fetch_files_after_timestamp_minute(directory_path)
        
        # Assert
        assert len(result) > 0
        assert all("minute=59" in f for f in result)


class TestFetchAllFilesAfterTimestampYear:
    """Tests for _fetch_all_files_after_timestamp_year function."""

    @pytest.mark.parametrize("target_year,expected_count", [
        ("year=2023", 6),  # All files in 2024
        ("year=2024", 0),  # No files after 2024
        ("year=2025", 0),  # No files after 2025
    ])
    def test_fetches_files_from_future_years(self, sample_timestamp_structure, target_year, expected_count):
        """Test that files are fetched from years greater than target year."""
        # Arrange
        base_path = sample_timestamp_structure
        timestamp_year = target_year
        
        # Act
        result = _fetch_all_files_after_timestamp_year(base_path, timestamp_year)
        
        # Assert
        assert len(result) == expected_count
        if expected_count > 0:
            assert all("year=2024" in f for f in result)


class TestFetchAllFilesAfterTimestampMonth:
    """Tests for _fetch_all_files_after_timestamp_month function."""

    @pytest.mark.parametrize("target_month,expected_count", [
        ("month=01", 5),  # All files in months 07-08 (4 in month=07, 1 in month=08)
        ("month=07", 1),  # Only files in month 08
        ("month=08", 0),  # No files after month 08
    ])
    def test_fetches_files_from_future_months(self, sample_timestamp_structure, target_month, expected_count):
        """Test that files are fetched from months greater than target month."""
        # Arrange
        year_dir_path = os.path.join(sample_timestamp_structure, "year=2024")
        timestamp_month = target_month
        
        # Act
        result = _fetch_all_files_after_timestamp_month(year_dir_path, timestamp_month)
        
        # Assert
        assert len(result) == expected_count
        if expected_count > 0:
            # Verify all files are from months after target_month
            month_num = int(target_month.split("=")[1])
            for f in result:
                # Extract month from file path
                parts = f.split(os.sep)
                file_month = None
                for part in parts:
                    if part.startswith("month="):
                        file_month = int(part.split("=")[1])
                        break
                if file_month is not None:
                    assert file_month > month_num, f"File {f} should be from month > {month_num}"


class TestFetchAllFilesAfterTimestampDay:
    """Tests for _fetch_all_files_after_timestamp_day function."""

    @pytest.mark.parametrize("target_day,expected_count", [
        ("day=05", 1),  # Files on day 06 only (this function doesn't fetch files from same day, different hour)
        ("day=06", 0),  # No files after day 06 in month 07
        ("day=07", 0),  # No files after day 07 in month 07
    ])
    def test_fetches_files_from_future_days(self, sample_timestamp_structure, target_day, expected_count):
        """Test that files are fetched from days greater than target day."""
        # Arrange
        month_dir_path = os.path.join(sample_timestamp_structure, "year=2024", "month=07")
        timestamp_day = target_day
        
        # Act
        result = _fetch_all_files_after_timestamp_day(month_dir_path, timestamp_day)
        
        # Assert
        assert len(result) == expected_count


class TestFetchAllFilesAfterTimestampHour:
    """Tests for _fetch_all_files_after_timestamp_hour function."""

    @pytest.mark.parametrize("target_hour,expected_count", [
        ("hour=20", 1),  # File at hour 21 only (this function doesn't fetch files from same day, different hour in later days)
        ("hour=21", 0),  # No files after hour 21 on day 05
    ])
    def test_fetches_files_from_future_hours(self, sample_timestamp_structure, target_hour, expected_count):
        """Test that files are fetched from hours greater than target hour."""
        # Arrange
        day_dir_path = os.path.join(
            sample_timestamp_structure, "year=2024", "month=07", "day=05"
        )
        timestamp_hour = target_hour
        
        # Act
        result = _fetch_all_files_after_timestamp_hour(day_dir_path, timestamp_hour)
        
        # Assert
        assert len(result) == expected_count


class TestFetchAllFilesAfterTimestampMinute:
    """Tests for _fetch_all_files_after_timestamp_minute function."""

    @pytest.mark.parametrize("target_minute,expected_count", [
        ("minute=58", 1),  # File at minute=59
        ("minute=59", 0),  # No files after minute=59
    ])
    def test_fetches_files_from_future_minutes(self, sample_timestamp_structure, target_minute, expected_count):
        """Test that files are fetched from minutes greater than target minute."""
        # Arrange
        hour_dir_path = os.path.join(
            sample_timestamp_structure, "year=2024", "month=07", "day=05", "hour=20"
        )
        timestamp_minute = target_minute
        
        # Act
        result = _fetch_all_files_after_timestamp_minute(hour_dir_path, timestamp_minute)
        
        # Assert
        assert len(result) == expected_count


class TestFindFilesAfterTimestamp:
    """Tests for find_files_after_timestamp function."""

    def test_finds_files_after_timestamp(self, sample_timestamp_structure):
        """Test that files after given timestamp are found."""
        # Arrange
        base_path = sample_timestamp_structure
        target_timestamp = "year=2024/month=07/day=05/hour=20/minute=58"
        
        # Act
        result = find_files_after_timestamp(base_path, target_timestamp)
        
        # Assert
        assert len(result) == 4
        # Verify files from later timestamps are included
        assert any("minute=59" in f for f in result)
        assert any("hour=21" in f for f in result)
        assert any("day=06" in f for f in result)
        assert any("month=08" in f for f in result)

    @pytest.mark.parametrize("target_timestamp,expected_count", [
        ("year=2024/month=07/day=05/hour=20/minute=58", 4),
        ("year=2024/month=07/day=05/hour=20/minute=59", 3),
        ("year=2024/month=07/day=05/hour=21/minute=00", 3),  # hour=21 minute=01, day=06, month=08
    ])
    def test_finds_correct_number_of_files(self, sample_timestamp_structure, target_timestamp, expected_count):
        """Test that correct number of files are found for various timestamps."""
        # Arrange
        base_path = sample_timestamp_structure
        
        # Act
        result = find_files_after_timestamp(base_path, target_timestamp)
        
        # Assert
        assert len(result) == expected_count

    def test_returns_empty_list_when_no_files_after_timestamp(self, tmp_path):
        """Test that empty list is returned when no files exist after timestamp."""
        # Arrange
        base_path = str(tmp_path)
        target_timestamp = "year=2024/month=01/day=01/hour=00/minute=00"
        # Create directory structure with files before timestamp
        old_dir = os.path.join(
            base_path, "year=2023", "month=12", "day=31", "hour=23", "minute=59"
        )
        os.makedirs(old_dir, exist_ok=True)
        with open(os.path.join(old_dir, "old_file.txt"), "w") as f:
            f.write("old content")
        # Create complete empty year=2024/month=01/day=01/hour=00 directory structure
        day_dir = os.path.join(base_path, "year=2024", "month=01", "day=01")
        hour_dir = os.path.join(day_dir, "hour=00")
        os.makedirs(hour_dir, exist_ok=True)
        
        # Act
        result = find_files_after_timestamp(base_path, target_timestamp)
        
        # Assert
        assert result == []

    def test_handles_empty_directory_structure(self, tmp_path):
        """Test that empty directory structure returns empty list."""
        # Arrange
        base_path = str(tmp_path)
        target_timestamp = "year=2024/month=01/day=01/hour=00/minute=00"
        # Create complete empty directory structure so os.listdir doesn't fail
        hour_dir = os.path.join(base_path, "year=2024", "month=01", "day=01", "hour=00")
        os.makedirs(hour_dir, exist_ok=True)
        
        # Act
        result = find_files_after_timestamp(base_path, target_timestamp)
        
        # Assert
        assert result == []

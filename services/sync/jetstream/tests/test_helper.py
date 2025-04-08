"""Tests for helper.py.

This test suite verifies the functionality of the helper functions used by the
Jetstream connector service. These functions handle timestamp conversions, 
validation, and data parsing.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock

import click

from lib.constants import timestamp_format
from services.sync.jetstream.helper import (
    timestamp_to_unix_microseconds,
    validate_timestamp,
    parse_handles,
)


class TestTimestampToUnixMicroseconds:
    """Tests for timestamp_to_unix_microseconds function.
    
    This class tests the function that converts a date string in YYYY-MM-DD
    format to Unix time in microseconds.
    """

    def test_basic_conversion(self):
        """Test conversion of basic timestamp to Unix microseconds.
        
        Expected input:
        - timestamp = "2023-01-01"
        
        Expected behavior:
        - Should convert to midnight at the beginning of 2023-01-01 in UTC
        - Should return integer value of microseconds since epoch
        """
        result = timestamp_to_unix_microseconds("2023-01-01")
        
        # Calculate expected value manually
        dt = datetime(2023, 1, 1, 0, 0, 0)
        expected = int(dt.timestamp() * 1_000_000)
        
        assert result == expected
        assert isinstance(result, int)

    def test_different_days(self):
        """Test conversion of different dates to Unix microseconds.
        
        Expected input:
        - Multiple different date strings
        
        Expected behavior:
        - Each date should convert to the correct integer value
        - Dates should be in ascending order of resulting values
        """
        dates = ["2022-01-01", "2022-06-15", "2023-01-01", "2023-12-31"]
        results = [timestamp_to_unix_microseconds(date) for date in dates]
        
        # Verify results are in ascending order
        assert results == sorted(results)
        
        # Verify each result is an integer
        assert all(isinstance(result, int) for result in results)
        
        # Verify specific date comparison
        assert timestamp_to_unix_microseconds("2023-01-01") > timestamp_to_unix_microseconds("2022-12-31")

    def test_current_date(self):
        """Test conversion of current date to Unix microseconds.
        
        Expected behavior:
        - Should correctly convert today's date to Unix microseconds
        - Result should be a reasonable timestamp value (not negative, not too far in future)
        """
        today = datetime.now().strftime("%Y-%m-%d")
        result = timestamp_to_unix_microseconds(today)
        
        # Should be a positive value
        assert result > 0
        
        # Should be reasonably recent (within last decade)
        ten_years_ago = int((datetime.now().timestamp() - 10 * 365 * 24 * 60 * 60) * 1_000_000)
        assert result > ten_years_ago
        
        # Should be today or earlier (not in future)
        tomorrow = int((datetime.now().timestamp() + 24 * 60 * 60) * 1_000_000)
        assert result < tomorrow

    def test_invalid_formats(self):
        """Test handling of invalid date formats.
        
        Expected input:
        - Various incorrectly formatted date strings
        
        Expected behavior:
        - Should raise ValueError for all invalid formats
        """
        invalid_formats = [
            "01-01-2023",    # Wrong order
            "2023/01/01",    # Wrong separator
            "2023-1-1",      # Missing leading zeros
            "01-Jan-2023",   # Text month
            "2023-01-01 12:00:00",  # With time
            "not_a_date",    # Not a date at all
        ]
        
        for invalid_format in invalid_formats:
            with pytest.raises(ValueError):
                timestamp_to_unix_microseconds(invalid_format)

    def test_edge_cases(self):
        """Test edge cases for timestamp conversion.
        
        Expected input:
        - Edge case dates (very old, leap years, etc.)
        
        Expected behavior:
        - Should handle all edge cases correctly
        - Should return appropriate microsecond values
        """
        # Test leap year day
        leap_year_result = timestamp_to_unix_microseconds("2020-02-29")
        assert leap_year_result > 0
        
        # Test very old date (Unix epoch start)
        # This should be exactly zero microseconds at Unix epoch start
        epoch_start = timestamp_to_unix_microseconds("1970-01-01")
        assert epoch_start == 0
        
        # Test future date
        future_date = timestamp_to_unix_microseconds("2050-01-01")
        assert future_date > timestamp_to_unix_microseconds("2023-01-01")
        
    def test_with_custom_format(self):
        """Test conversion with a custom format string.
        
        Expected input:
        - timestamp = "01/02/2023" with format "%m/%d/%Y"
        
        Expected behavior:
        - Should correctly parse the date using the provided format
        - Should return the correct Unix microseconds
        """
        # Test with MM/DD/YYYY format
        result = timestamp_to_unix_microseconds("01/02/2023", format="%m/%d/%Y")
        
        # Calculate expected value manually
        dt = datetime(2023, 1, 2, 0, 0, 0)
        expected = int(dt.timestamp() * 1_000_000)
        
        assert result == expected
        
        # Test with another custom format
        result = timestamp_to_unix_microseconds("2023-02-03 14:30:45", format="%Y-%m-%d %H:%M:%S")
        
        # Calculate expected value
        dt = datetime(2023, 2, 3, 14, 30, 45)
        expected = int(dt.timestamp() * 1_000_000)
        
        assert result == expected
        
    def test_with_default_format(self):
        """Test conversion using the default format from lib.constants.
        
        Expected input:
        - timestamp in the format defined by lib.constants.timestamp_format
        
        Expected behavior:
        - Should correctly parse the date using the default format
        - Should return the correct Unix microseconds
        """
        # Create a timestamp in the default format (YYYY-MM-DD-HH:MM:SS)
        timestamp_str = "2023-04-15-10:30:45"
        
        # Test with default format
        result = timestamp_to_unix_microseconds(timestamp_str)
        
        # Calculate expected value manually
        dt = datetime.strptime(timestamp_str, timestamp_format)
        expected = int(dt.timestamp() * 1_000_000)
        
        assert result == expected
        
    def test_format_fallback(self):
        """Test the format fallback behavior.
        
        Expected behavior:
        - Should try YYYY-MM-DD format first if no format is specified
        - Should fall back to the default format if not in YYYY-MM-DD format
        """
        # Test that a YYYY-MM-DD string works without specifying format
        date_result = timestamp_to_unix_microseconds("2023-05-20")
        date_expected = int(datetime(2023, 5, 20, 0, 0, 0).timestamp() * 1_000_000)
        assert date_result == date_expected
        
        # Test that a string in default format works without specifying format
        default_timestamp = "2023-05-20-15:45:30"
        default_result = timestamp_to_unix_microseconds(default_timestamp)
        default_expected = int(datetime.strptime(default_timestamp, timestamp_format).timestamp() * 1_000_000)
        assert default_result == default_expected


class TestValidateTimestamp:
    """Tests for validate_timestamp function.
    
    This class tests the Click callback function that validates timestamp format.
    """

    def test_valid_timestamp(self):
        """Test validation of correct timestamp format.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = "2023-01-01"
        
        Expected behavior:
        - Should return the original timestamp unchanged
        """
        ctx = Mock()
        param = Mock()
        timestamp = "2023-01-01"
        
        result = validate_timestamp(ctx, param, timestamp)
        assert result == timestamp

    def test_none_value(self):
        """Test validation of None timestamp.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = None
        
        Expected behavior:
        - Should return None
        """
        ctx = Mock()
        param = Mock()
        
        result = validate_timestamp(ctx, param, None)
        assert result is None

    def test_invalid_formats(self):
        """Test validation of incorrect timestamp formats.
        
        Expected input:
        - Various incorrectly formatted timestamp strings
        
        Expected behavior:
        - Should raise click.BadParameter for all invalid formats
        """
        ctx = Mock()
        param = Mock()
        
        invalid_formats = [
            "01-01-2023",    # Wrong order
            "2023/01/01",    # Wrong separator
            "2023-1-1",      # Missing leading zeros
            "01-Jan-2023",   # Text month
            "2023-01-01 12:00:00",  # With time
            "not_a_date",    # Not a date at all
        ]
        
        for invalid_format in invalid_formats:
            with pytest.raises(click.BadParameter):
                validate_timestamp(ctx, param, invalid_format)

    def test_edge_cases(self):
        """Test edge cases for timestamp validation.
        
        Expected input:
        - Edge case dates (very old, leap years, etc.)
        
        Expected behavior:
        - Should correctly validate all edge cases
        """
        ctx = Mock()
        param = Mock()
        
        # Test leap year day (valid)
        assert validate_timestamp(ctx, param, "2020-02-29") == "2020-02-29"
        
        # Test non-leap year February 29 (invalid)
        with pytest.raises(click.BadParameter):
            validate_timestamp(ctx, param, "2023-02-29")
        
        # Test Unix epoch start
        assert validate_timestamp(ctx, param, "1970-01-01") == "1970-01-01"
        
        # Test future date
        assert validate_timestamp(ctx, param, "2050-01-01") == "2050-01-01"
        
    def test_with_custom_format(self):
        """Test validation with a custom format string.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = "01/02/2023" with format "%m/%d/%Y"
        
        Expected behavior:
        - Should correctly validate date using the provided format
        - Should return the original string if valid
        - Should raise BadParameter if invalid format
        """
        ctx = Mock()
        param = Mock()
        
        # Test valid date with custom format
        assert validate_timestamp(ctx, param, "01/02/2023", format="%m/%d/%Y") == "01/02/2023"
        
        # Test valid date with time format
        assert validate_timestamp(ctx, param, "2023-02-03 14:30:45", format="%Y-%m-%d %H:%M:%S") == "2023-02-03 14:30:45"
        
        # Test invalid date with custom format
        with pytest.raises(click.BadParameter):
            validate_timestamp(ctx, param, "13/45/2023", format="%m/%d/%Y")
            
        # Test valid default format but invalid for custom format
        with pytest.raises(click.BadParameter):
            validate_timestamp(ctx, param, "2023-01-01", format="%m/%d/%Y")


class TestParseHandles:
    """Tests for parse_handles function.
    
    This class tests the Click callback function that parses comma-separated
    Bluesky handles into a list.
    """

    def test_basic_parsing(self):
        """Test parsing of simple comma-separated handles.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = "alice.bsky.social,bob.bsky.social"
        
        Expected behavior:
        - Should return list ["alice.bsky.social", "bob.bsky.social"]
        """
        ctx = Mock()
        param = Mock()
        handles = "alice.bsky.social,bob.bsky.social"
        
        result = parse_handles(ctx, param, handles)
        assert result == ["alice.bsky.social", "bob.bsky.social"]
    
    def test_none_value(self):
        """Test parsing of None handles.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = None
        
        Expected behavior:
        - Should return None
        """
        ctx = Mock()
        param = Mock()
        
        result = parse_handles(ctx, param, None)
        assert result is None

    def test_empty_string(self):
        """Test parsing of empty string.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = ""
        
        Expected behavior:
        - Should return empty list
        """
        ctx = Mock()
        param = Mock()
        
        result = parse_handles(ctx, param, "")
        assert result == []

    def test_whitespace_handling(self):
        """Test handling of whitespace in handles.
        
        Expected input:
        - Handles with various whitespace patterns
        
        Expected behavior:
        - Should strip whitespace and return clean handles
        - Should skip empty handles after stripping
        """
        ctx = Mock()
        param = Mock()
        
        # Test with spaces around commas
        result = parse_handles(ctx, param, "alice.bsky.social , bob.bsky.social")
        assert result == ["alice.bsky.social", "bob.bsky.social"]
        
        # Test with leading/trailing whitespace
        result = parse_handles(ctx, param, " alice.bsky.social,bob.bsky.social ")
        assert result == ["alice.bsky.social", "bob.bsky.social"]
        
        # Test with empty elements
        result = parse_handles(ctx, param, "alice.bsky.social,,bob.bsky.social")
        assert result == ["alice.bsky.social", "bob.bsky.social"]
        
        # Test with only whitespace
        result = parse_handles(ctx, param, "   ,  ,   ")
        assert result == []

    def test_single_handle(self):
        """Test parsing of a single handle.
        
        Expected input:
        - ctx = Mock()
        - param = Mock()
        - value = "alice.bsky.social"
        
        Expected behavior:
        - Should return list with single element ["alice.bsky.social"]
        """
        ctx = Mock()
        param = Mock()
        
        result = parse_handles(ctx, param, "alice.bsky.social")
        assert result == ["alice.bsky.social"]

    def test_many_handles(self):
        """Test parsing of many handles.
        
        Expected input:
        - Long list of comma-separated handles
        
        Expected behavior:
        - Should correctly parse all handles into a list
        """
        ctx = Mock()
        param = Mock()
        
        handles = ",".join([f"user{i}.bsky.social" for i in range(100)])
        result = parse_handles(ctx, param, handles)
        
        assert len(result) == 100
        assert all(f"user{i}.bsky.social" in result for i in range(100)) 
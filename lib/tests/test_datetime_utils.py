"""Tests for datetime_utils.py.

This test suite verifies the functionality of datetime utility functions:
- calculate_lookback_datetime_str: Tests lookback datetime calculation in different formats
- convert_pipeline_to_bsky_dt_format: Tests conversion from pipeline to Bluesky datetime format
- normalize_timestamp: Tests normalization of timestamps with hour 24
- try_default_ts_truncation: Tests timestamp truncation to seconds precision
- convert_bsky_dt_to_pipeline_dt: Tests conversion from Bluesky to pipeline datetime format
"""

from datetime import datetime, timezone
from unittest.mock import patch
import pytest

from lib.datetime_utils import (
    calculate_lookback_datetime_str,
    convert_bsky_dt_to_pipeline_dt,
    convert_pipeline_to_bsky_dt_format,
    normalize_timestamp,
    TimestampFormat,
    try_default_ts_truncation,
    truncate_timestamp_string,
)


class TestCalculateLookbackDatetimeStr:
    """Tests for calculate_lookback_datetime_str function."""

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_pipeline_format_default(self):
        """Test calculating lookback datetime with default pipeline format."""
        # Arrange
        lookback_days = 5
        expected = "2024-01-10-13:45:30"  # 5 days before

        # Act
        result = calculate_lookback_datetime_str(lookback_days)

        # Assert
        assert result == expected

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_pipeline_format_explicit(self):
        """Test calculating lookback datetime with explicit pipeline format."""
        # Arrange
        lookback_days = 5
        expected = "2024-01-10-13:45:30"  # 5 days before

        # Act
        result = calculate_lookback_datetime_str(
            lookback_days, format=TimestampFormat.PIPELINE
        )

        # Assert
        assert result == expected

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_bluesky_format(self):
        """Test calculating lookback datetime with Bluesky format."""
        # Arrange
        lookback_days = 5
        expected = "2024-01-10T13:45:30"  # 5 days before in Bluesky format

        # Act
        result = calculate_lookback_datetime_str(
            lookback_days, format=TimestampFormat.BLUESKY
        )

        # Assert
        assert result == expected

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_zero_lookback_days(self):
        """Test calculating lookback datetime with zero days."""
        # Arrange
        lookback_days = 0
        expected = "2024-01-15-13:45:30"  # Same day

        # Act
        result = calculate_lookback_datetime_str(lookback_days)

        # Assert
        assert result == expected

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_large_lookback_days(self):
        """Test calculating lookback datetime with large number of days."""
        # Arrange
        lookback_days = 365
        expected = "2023-01-15-13:45:30"  # 1 year before

        # Act
        result = calculate_lookback_datetime_str(lookback_days)

        # Assert
        assert result == expected

    @patch("lib.datetime_utils.current_datetime", new=datetime(2024, 1, 15, 13, 45, 30, tzinfo=timezone.utc))
    def test_month_boundary_crossing(self):
        """Test calculating lookback datetime that crosses month boundary."""
        # Arrange
        lookback_days = 20
        expected = "2023-12-26-13:45:30"  # Crosses month and year boundary

        # Act
        result = calculate_lookback_datetime_str(lookback_days)

        # Assert
        assert result == expected


class TestConvertPipelineToBskyDtFormat:
    """Tests for convert_pipeline_to_bsky_dt_format function."""

    def test_valid_pipeline_datetime(self):
        """Test converting valid pipeline datetime to Bluesky format."""
        # Arrange
        pipeline_dt = "2024-01-15-13:45:30"
        expected = "2024-01-15T13:45:30"

        # Act
        result = convert_pipeline_to_bsky_dt_format(pipeline_dt)

        # Assert
        assert result == expected

    def test_midnight_datetime(self):
        """Test converting midnight datetime."""
        # Arrange
        pipeline_dt = "2024-01-15-00:00:00"
        expected = "2024-01-15T00:00:00"

        # Act
        result = convert_pipeline_to_bsky_dt_format(pipeline_dt)

        # Assert
        assert result == expected

    def test_end_of_day_datetime(self):
        """Test converting end of day datetime."""
        # Arrange
        pipeline_dt = "2024-01-15-23:59:59"
        expected = "2024-01-15T23:59:59"

        # Act
        result = convert_pipeline_to_bsky_dt_format(pipeline_dt)

        # Assert
        assert result == expected

    def test_year_boundary(self):
        """Test converting datetime at year boundary."""
        # Arrange
        pipeline_dt = "2024-12-31-23:59:59"
        expected = "2024-12-31T23:59:59"

        # Act
        result = convert_pipeline_to_bsky_dt_format(pipeline_dt)

        # Assert
        assert result == expected

    def test_leap_year_february(self):
        """Test converting datetime in leap year February."""
        # Arrange
        pipeline_dt = "2024-02-29-12:00:00"
        expected = "2024-02-29T12:00:00"

        # Act
        result = convert_pipeline_to_bsky_dt_format(pipeline_dt)

        # Assert
        assert result == expected

    def test_invalid_format_raises_value_error(self):
        """Test that invalid datetime format raises ValueError."""
        # Arrange
        invalid_dt = "2024-01-15T13:45:30"  # Already in Bluesky format

        # Act & Assert
        with pytest.raises(ValueError):
            convert_pipeline_to_bsky_dt_format(invalid_dt)

    def test_malformed_datetime_raises_value_error(self):
        """Test that malformed datetime string raises ValueError."""
        # Arrange
        malformed_dt = "invalid-datetime-format"

        # Act & Assert
        with pytest.raises(ValueError):
            convert_pipeline_to_bsky_dt_format(malformed_dt)

    def test_empty_string_raises_value_error(self):
        """Test that empty string raises ValueError."""
        # Arrange
        empty_dt = ""

        # Act & Assert
        with pytest.raises(ValueError):
            convert_pipeline_to_bsky_dt_format(empty_dt)


class TestNormalizeTimestamp:
    """Tests for normalize_timestamp function."""

    def test_timestamp_with_hour_24(self):
        """Test normalizing timestamp with hour 24."""
        # Arrange
        timestamp = "2024-01-15T24:00:00"
        expected = "2024-01-15T00:00:00"

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_hour_24_and_minutes(self):
        """Test normalizing timestamp with hour 24 and minutes."""
        # Arrange
        timestamp = "2024-01-15T24:30:45"
        expected = "2024-01-15T00:30:45"

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_without_hour_24(self):
        """Test normalizing timestamp without hour 24 (should return unchanged)."""
        # Arrange
        timestamp = "2024-01-15T13:45:30"
        expected = "2024-01-15T13:45:30"

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_hour_24_in_middle(self):
        """Test normalizing timestamp with T24: in the middle of string."""
        # Arrange
        timestamp = "prefix2024-01-15T24:00:00suffix"
        expected = "prefix2024-01-15T00:00:00suffix"

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected

    def test_multiple_occurrences_of_t24(self):
        """Test normalizing timestamp with multiple T24: occurrences."""
        # Arrange
        timestamp = "2024-01-15T24:00:00T24:00:00"
        expected = "2024-01-15T00:00:00T00:00:00"

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected

    def test_empty_string(self):
        """Test normalizing empty string."""
        # Arrange
        timestamp = ""
        expected = ""

        # Act
        result = normalize_timestamp(timestamp)

        # Assert
        assert result == expected


class TestTryDefaultTsTruncation:
    """Tests for try_default_ts_truncation function."""

    def test_timestamp_with_microseconds(self):
        """Test truncating timestamp with microseconds."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123456"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_milliseconds(self):
        """Test truncating timestamp with milliseconds."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_timezone(self):
        """Test truncating timestamp with timezone."""
        # Arrange
        timestamp = "2024-01-15T13:45:30+00:00"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_z_suffix(self):
        """Test truncating timestamp with Z suffix."""
        # Arrange
        timestamp = "2024-01-15T13:45:30Z"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_already_truncated(self):
        """Test truncating timestamp that is already at seconds precision."""
        # Arrange
        timestamp = "2024-01-15T13:45:30"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_shorter_than_19_chars(self):
        """Test truncating timestamp shorter than 19 characters."""
        # Arrange
        timestamp = "2024-01-15T13:45"
        expected = "2024-01-15T13:45"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_empty_string(self):
        """Test truncating empty string."""
        # Arrange
        timestamp = ""
        expected = ""

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected

    def test_very_long_timestamp(self):
        """Test truncating very long timestamp."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.12345678901234567890+00:00:00"
        expected = "2024-01-15T13:45:30"

        # Act
        result = try_default_ts_truncation(timestamp)

        # Assert
        assert result == expected


class TestConvertBskyDtToPipelineDt:
    """Tests for convert_bsky_dt_to_pipeline_dt function."""

    def test_default_bsky_format(self):
        """Test converting default Bluesky timestamp format."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_pipeline_format_already(self):
        """Test converting timestamp already in pipeline format."""
        # Arrange
        bsky_dt = "2024-01-15-13:45:30"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_format_with_microseconds_z(self):
        """Test converting format with microseconds and Z suffix."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30.123456Z"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_format_with_z_suffix(self):
        """Test converting format with Z suffix."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30Z"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_format_with_timezone_offset(self):
        """Test converting format with timezone offset."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30+00:00"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_format_with_microseconds_and_timezone(self):
        """Test converting format with microseconds and timezone."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30.123456+0000"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_format_with_microseconds_no_suffix(self):
        """Test converting format with microseconds but no suffix."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30.593559"
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_timestamp_with_hour_24(self):
        """Test converting timestamp with hour 24 (should be normalized first)."""
        # Arrange
        bsky_dt = "2024-01-15T24:00:00"
        expected = "2024-01-15-00:00:00"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_midnight_datetime(self):
        """Test converting midnight datetime."""
        # Arrange
        bsky_dt = "2024-01-15T00:00:00"
        expected = "2024-01-15-00:00:00"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_end_of_day_datetime(self):
        """Test converting end of day datetime."""
        # Arrange
        bsky_dt = "2024-01-15T23:59:59"
        expected = "2024-01-15-23:59:59"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_leap_year_february(self):
        """Test converting datetime in leap year February."""
        # Arrange
        bsky_dt = "2024-02-29T12:00:00"
        expected = "2024-02-29-12:00:00"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_year_boundary(self):
        """Test converting datetime at year boundary."""
        # Arrange
        bsky_dt = "2024-12-31T23:59:59Z"
        expected = "2024-12-31-23:59:59"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_timestamp_with_truncation_fallback(self):
        """Test converting timestamp that requires truncation fallback."""
        # Arrange
        bsky_dt = "2024-01-15T13:45:30.12345678901234567890"  # Very long
        expected = "2024-01-15-13:45:30"

        # Act
        result = convert_bsky_dt_to_pipeline_dt(bsky_dt)

        # Assert
        assert result == expected

    def test_invalid_datetime_raises_value_error(self):
        """Test that invalid datetime string raises ValueError."""
        # Arrange
        invalid_dt = "invalid-datetime-format"

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid Bluesky datetime string"):
            convert_bsky_dt_to_pipeline_dt(invalid_dt)

    def test_empty_string_raises_value_error(self):
        """Test that empty string raises ValueError."""
        # Arrange
        empty_dt = ""

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid Bluesky datetime string"):
            convert_bsky_dt_to_pipeline_dt(empty_dt)

    def test_malformed_datetime_raises_value_error(self):
        """Test that malformed datetime string raises ValueError."""
        # Arrange
        malformed_dt = "2024-13-45T25:99:99"  # Invalid month, day, hour, minute, second

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid Bluesky datetime string"):
            convert_bsky_dt_to_pipeline_dt(malformed_dt)


class TestTruncateTimestampString:
    """Tests for truncate_timestamp_string function."""

    def test_truncates_after_period(self):
        """Test truncating timestamp with microseconds (period delimiter)."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123456"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_truncates_after_plus(self):
        """Test truncating timestamp with timezone offset (plus delimiter)."""
        # Arrange
        timestamp = "2024-01-15T13:45:30+00:00"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_truncates_after_z(self):
        """Test truncating timestamp with Z suffix (Z delimiter)."""
        # Arrange
        timestamp = "2024-01-15T13:45:30Z"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_truncates_at_first_delimiter_period_before_plus(self):
        """Test truncating at first delimiter when period comes before plus."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123456+00:00"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_truncates_at_first_delimiter_plus_before_period(self):
        """Test truncating when plus comes before period - uses period since it's checked first."""
        # Arrange
        timestamp = "2024-01-15T13:45:30+00:00.123"
        expected = "2024-01-15T13:45:30+00:00"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_truncates_at_first_delimiter_z_before_others(self):
        """Test truncating when Z comes before period or plus - uses period since it's checked first."""
        # Arrange
        timestamp = "2024-01-15T13:45:30Z.123+00:00"
        expected = "2024-01-15T13:45:30Z"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_returns_unchanged_when_no_delimiters(self):
        """Test returning unchanged string when no delimiters are present."""
        # Arrange
        timestamp = "2024-01-15T13:45:30"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_empty_string(self):
        """Test truncating empty string."""
        # Arrange
        timestamp = ""
        expected = ""

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_only_delimiter_period(self):
        """Test string containing only period delimiter."""
        # Arrange
        timestamp = "2024-01-15T13:45:30."
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_only_delimiter_plus(self):
        """Test string containing only plus delimiter."""
        # Arrange
        timestamp = "2024-01-15T13:45:30+"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_only_delimiter_z(self):
        """Test string containing only Z delimiter."""
        # Arrange
        timestamp = "2024-01-15T13:45:30Z"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_multiple_periods_takes_first(self):
        """Test truncating at first period when multiple periods are present."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123.456"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_milliseconds(self):
        """Test truncating timestamp with milliseconds precision."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_timestamp_with_timezone_offset_negative(self):
        """Test truncating timestamp with negative timezone offset - no delimiter found."""
        # Arrange
        timestamp = "2024-01-15T13:45:30-05:00"
        expected = "2024-01-15T13:45:30-05:00"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_string_with_period_not_at_end(self):
        """Test truncating string with period not at expected position."""
        # Arrange
        timestamp = "2024.01.15T13:45:30"
        expected = "2024"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_string_with_plus_not_at_end(self):
        """Test truncating string with plus not at expected position."""
        # Arrange
        timestamp = "2024+01-15T13:45:30"
        expected = "2024"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_string_with_z_not_at_end(self):
        """Test truncating string with Z not at expected position."""
        # Arrange
        timestamp = "2024Z01-15T13:45:30"
        expected = "2024"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

    def test_all_delimiters_present_takes_first(self):
        """Test truncating when all delimiters are present, takes first occurrence."""
        # Arrange
        timestamp = "2024-01-15T13:45:30.123Z+00:00"
        expected = "2024-01-15T13:45:30"

        # Act
        result = truncate_timestamp_string(timestamp)

        # Assert
        assert result == expected

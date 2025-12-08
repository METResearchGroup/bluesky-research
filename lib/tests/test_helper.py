from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from lib.helper import calculate_start_end_date_for_lookback, determine_backfill_latest_timestamp


class TestCalculateStartEndDateForLookback:
    def test_normal_case(self):
        """Test calculating dates when partition date minus lookback is after min date."""
        start, end = calculate_start_end_date_for_lookback(
            partition_date="2024-10-10",
            num_days_lookback=5,
            min_lookback_date="2024-09-28",
        )
        assert start == "2024-10-05"  # 5 days before
        assert end == "2024-10-10"

    def test_min_date_case(self):
        """Test calculating dates when partition date minus lookback is before min date."""
        start, end = calculate_start_end_date_for_lookback(
            partition_date="2024-09-30",
            num_days_lookback=5,
            min_lookback_date="2024-09-28",
        )
        assert start == "2024-09-28"  # Should use min date
        assert end == "2024-09-30"


class TestDetermineBackfillLatestTimestamp:
    """Tests for determine_backfill_latest_timestamp function."""

    @patch("lib.helper.datetime")
    def test_valid_days_backfill(self, mock_datetime):
        """Test calculating timestamp with valid days backfill."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-10-12:30:45"  # 5 days before
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=5,
            backfill_period="days"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_valid_hours_backfill(self, mock_datetime):
        """Test calculating timestamp with valid hours backfill."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-15-07:30:45"  # 5 hours before
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=5,
            backfill_period="hours"
        )
        
        # Assert
        assert result == expected

    def test_returns_none_when_duration_is_none(self):
        """Test returns None when backfill_duration is None."""
        # Arrange
        # No setup needed
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=None,
            backfill_period="days"
        )
        
        # Assert
        assert result is None

    def test_returns_none_when_period_is_none(self):
        """Test returns None when backfill_period is None."""
        # Arrange
        # No setup needed
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=5,
            backfill_period=None
        )
        
        # Assert
        assert result is None

    def test_returns_none_when_both_are_none(self):
        """Test returns None when both parameters are None."""
        # Arrange
        # No setup needed
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=None,
            backfill_period=None
        )
        
        # Assert
        assert result is None

    def test_returns_none_when_period_is_invalid(self):
        """Test returns None when backfill_period is not 'days' or 'hours'."""
        # Arrange
        # No setup needed
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=5,
            backfill_period="weeks"  # type: ignore[arg-type]  # Invalid period for testing
        )
        
        # Assert
        assert result is None

    @patch("lib.helper.datetime")
    def test_zero_duration_days(self, mock_datetime):
        """Test calculating timestamp with zero days duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-15-12:30:45"  # 0 days before = same time
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=0,
            backfill_period="days"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_zero_duration_hours(self, mock_datetime):
        """Test calculating timestamp with zero hours duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-15-12:30:45"  # 0 hours before = same time
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=0,
            backfill_period="hours"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_large_duration_days(self, mock_datetime):
        """Test calculating timestamp with large days duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2023-12-16-12:30:45"  # 30 days before
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=30,
            backfill_period="days"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_large_duration_hours(self, mock_datetime):
        """Test calculating timestamp with large hours duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-14-06:30:45"  # 30 hours before
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=30,
            backfill_period="hours"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_negative_duration_days(self, mock_datetime):
        """Test calculating timestamp with negative days duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-20-12:30:45"  # -5 days = 5 days in the future
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=-5,
            backfill_period="days"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_negative_duration_hours(self, mock_datetime):
        """Test calculating timestamp with negative hours duration."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        expected = "2024-01-15-17:30:45"  # -5 hours = 5 hours in the future
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=-5,
            backfill_period="hours"
        )
        
        # Assert
        assert result == expected

    @patch("lib.helper.datetime")
    def test_timestamp_format_matches_constant(self, mock_datetime):
        """Test that returned timestamp matches the expected format."""
        # Arrange
        fixed_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        # Preserve datetime class functionality
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else datetime
        
        # Act
        result = determine_backfill_latest_timestamp(
            backfill_duration=1,
            backfill_period="days"
        )
        
        # Assert
        assert result is not None
        # Verify format: YYYY-MM-DD-HH:MM:SS
        assert len(result) == 19
        assert result.count("-") == 3
        assert result.count(":") == 2
        # Verify it can be parsed back
        parsed = datetime.strptime(result, "%Y-%m-%d-%H:%M:%S")
        assert parsed is not None

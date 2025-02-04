from lib.helper import calculate_start_end_date_for_lookback


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

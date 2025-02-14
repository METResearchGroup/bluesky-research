"""Tests for condition_aggregated.py."""

import pytest
from datetime import datetime, timedelta

from services.calculate_analytics.study_analytics.generate_reports.condition_aggregated import (
    map_date_to_static_week,
)

# Helper function to generate dates in range
def generate_dates(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start_date and end_date inclusive."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return date_list

# Expected week mappings for Wave 1
WAVE_1_EXPECTED = {
    # Week 1: Sep 30 - Oct 6
    **{date: 1 for date in generate_dates("2024-09-30", "2024-10-06")},
    # Week 2: Oct 7 - Oct 13
    **{date: 2 for date in generate_dates("2024-10-07", "2024-10-13")},
    # Week 3: Oct 14 - Oct 20
    **{date: 3 for date in generate_dates("2024-10-14", "2024-10-20")},
    # Week 4: Oct 21 - Oct 27
    **{date: 4 for date in generate_dates("2024-10-21", "2024-10-27")},
    # Week 5: Oct 28 - Nov 3
    **{date: 5 for date in generate_dates("2024-10-28", "2024-11-03")},
    # Week 6: Nov 4 - Nov 10
    **{date: 6 for date in generate_dates("2024-11-04", "2024-11-10")},
    # Week 7: Nov 11 - Nov 17
    **{date: 7 for date in generate_dates("2024-11-11", "2024-11-17")},
    # Week 8: Nov 18 - Nov 24
    **{date: 8 for date in generate_dates("2024-11-18", "2024-11-24")},
}

# Expected week mappings for Wave 2
WAVE_2_EXPECTED = {
    # Week 1: Oct 7 - Oct 13
    **{date: 1 for date in generate_dates("2024-10-07", "2024-10-13")},
    # Week 2: Oct 14 - Oct 20
    **{date: 2 for date in generate_dates("2024-10-14", "2024-10-20")},
    # Week 3: Oct 21 - Oct 27
    **{date: 3 for date in generate_dates("2024-10-21", "2024-10-27")},
    # Week 4: Oct 28 - Nov 3
    **{date: 4 for date in generate_dates("2024-10-28", "2024-11-03")},
    # Week 5: Nov 4 - Nov 10
    **{date: 5 for date in generate_dates("2024-11-04", "2024-11-10")},
    # Week 6: Nov 11 - Nov 17
    **{date: 6 for date in generate_dates("2024-11-11", "2024-11-17")},
    # Week 7: Nov 18 - Nov 24
    **{date: 7 for date in generate_dates("2024-11-18", "2024-11-24")},
    # Week 8: Nov 25 - Dec 1
    **{date: 8 for date in generate_dates("2024-11-25", "2024-12-01")},
}

def test_wave_1_dates():
    """Test that all dates for Wave 1 map to the correct week numbers."""
    for date, expected_week in WAVE_1_EXPECTED.items():
        result = map_date_to_static_week(date, wave=1)
        assert result == expected_week, f"Wave 1: Date {date} should map to week {expected_week}, got {result}"

def test_wave_2_dates():
    """Test that all dates for Wave 2 map to the correct week numbers."""
    for date, expected_week in WAVE_2_EXPECTED.items():
        result = map_date_to_static_week(date, wave=2)
        assert result == expected_week, f"Wave 2: Date {date} should map to week {expected_week}, got {result}"

def test_invalid_wave():
    """Test that invalid wave numbers raise ValueError."""
    with pytest.raises(ValueError, match="Invalid wave: 3"):
        map_date_to_static_week("2024-10-01", wave=3)

def test_specific_boundary_dates_wave_1():
    """Test specific boundary dates for Wave 1."""
    test_cases = [
        ("2024-09-30", 1),  # First day of Wave 1
        ("2024-10-06", 1),  # Last day of Week 1
        ("2024-10-07", 2),  # First day of Week 2
        ("2024-11-24", 8),  # Last day of Wave 1
    ]
    
    for date, expected_week in test_cases:
        result = map_date_to_static_week(date, wave=1)
        assert result == expected_week, f"Wave 1 boundary: Date {date} should map to week {expected_week}, got {result}"

def test_specific_boundary_dates_wave_2():
    """Test specific boundary dates for Wave 2."""
    test_cases = [
        ("2024-10-07", 1),  # First day of Wave 2
        ("2024-10-13", 1),  # Last day of Week 1
        ("2024-10-14", 2),  # First day of Week 2
        ("2024-12-01", 8),  # Last day of Wave 2
    ]
    
    for date, expected_week in test_cases:
        result = map_date_to_static_week(date, wave=2)
        assert result == expected_week, f"Wave 2 boundary: Date {date} should map to week {expected_week}, got {result}" 
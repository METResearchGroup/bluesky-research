import pandas as pd
import pytest

from services.calculate_analytics.study_analytics.generate_reports.condition_aggregated import (
    get_week_thresholds_per_user_static,
    start_date_inclusive,
    end_date_inclusive,
)


def test_get_week_thresholds_per_user_static():
    # Create test data with both wave 1 and wave 2 users
    test_data = {"bluesky_handle": ["user1", "user2", "user3"], "wave": [1, 2, 1]}
    user_handle_to_wave_df = pd.DataFrame(test_data)

    # Get the week thresholds
    result_df = get_week_thresholds_per_user_static(user_handle_to_wave_df)

    # Basic validation
    assert isinstance(result_df, pd.DataFrame)
    assert set(result_df.columns) == {"bluesky_handle", "wave", "date", "week_static"}

    # Test specific dates for wave 1 users
    wave1_data = result_df[result_df["bluesky_handle"] == "user1"]

    # Test early dates for wave 1
    early_wave1 = wave1_data[wave1_data["date"] == "2024-09-30"]
    assert early_wave1["week_static"].iloc[0] == 1

    mid_wave1 = wave1_data[wave1_data["date"] == "2024-10-15"]
    assert mid_wave1["week_static"].iloc[0] == 3

    # Test dates after wave 1 end (should be None)
    late_wave1 = wave1_data[wave1_data["date"] == "2024-11-26"]
    assert pd.isna(late_wave1["week_static"].iloc[0])

    # Test specific dates for wave 2 users
    wave2_data = result_df[result_df["bluesky_handle"] == "user2"]

    # Test early dates for wave 2 (should be None)
    early_wave2 = wave2_data[wave2_data["date"] == "2024-09-30"]
    assert pd.isna(early_wave2["week_static"].iloc[0])

    # Test valid dates for wave 2
    start_wave2 = wave2_data[wave2_data["date"] == "2024-10-07"]
    assert start_wave2["week_static"].iloc[0] == 1

    mid_wave2 = wave2_data[wave2_data["date"] == "2024-10-21"]
    assert mid_wave2["week_static"].iloc[0] == 3

    late_wave2 = wave2_data[wave2_data["date"] == "2024-11-30"]
    assert late_wave2["week_static"].iloc[0] == 8

    # Test week transitions
    def get_week_for_date(df, handle, date):
        return df[(df["bluesky_handle"] == handle) & (df["date"] == date)][
            "week_static"
        ].iloc[0]

    # Test week transitions for wave 1
    assert get_week_for_date(result_df, "user1", "2024-10-06") == 1
    assert get_week_for_date(result_df, "user1", "2024-10-07") == 2
    assert get_week_for_date(result_df, "user1", "2024-10-13") == 2
    assert get_week_for_date(result_df, "user1", "2024-10-14") == 3

    # Test week transitions for wave 2
    assert pd.isna(get_week_for_date(result_df, "user2", "2024-10-06"))
    assert get_week_for_date(result_df, "user2", "2024-10-07") == 1
    assert get_week_for_date(result_df, "user2", "2024-10-13") == 1
    assert get_week_for_date(result_df, "user2", "2024-10-14") == 2

    # Verify all dates are present
    all_dates = (
        pd.date_range(start=start_date_inclusive, end=end_date_inclusive)
        .strftime("%Y-%m-%d")
        .tolist()
    )
    for handle in ["user1", "user2", "user3"]:
        user_dates = result_df[result_df["bluesky_handle"] == handle]["date"].tolist()
        assert sorted(user_dates) == sorted(all_dates)


def test_get_week_thresholds_per_user_static_edge_cases():
    # Test with empty dataframe
    empty_df = pd.DataFrame(columns=["bluesky_handle", "wave"])
    result_empty = get_week_thresholds_per_user_static(empty_df)
    assert len(result_empty) == 0

    # Test with single user
    single_user_df = pd.DataFrame({"bluesky_handle": ["user1"], "wave": [1]})
    result_single = get_week_thresholds_per_user_static(single_user_df)
    assert len(result_single) == len(
        pd.date_range(start=start_date_inclusive, end=end_date_inclusive)
    )

    # Test invalid wave number
    with pytest.raises(ValueError):
        invalid_wave_df = pd.DataFrame({"bluesky_handle": ["user1"], "wave": [3]})
        get_week_thresholds_per_user_static(invalid_wave_df)

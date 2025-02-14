import pandas as pd
import pytest

from services.calculate_analytics.study_analytics.generate_reports.condition_aggregated import (
    get_week_thresholds_per_user_static,
    get_latest_survey_timestamp_within_period,
    get_week_threshold_for_user_dynamic,
    start_date_inclusive,
    end_date_inclusive,
    get_week_thresholds_per_user_dynamic,
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


def test_get_latest_survey_timestamp_within_period():
    # Test case 1: Single timestamp within period
    timestamps = ["2024-10-15"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result == "2024-10-15"

    # Test case 2: Multiple timestamps within period, should return latest
    timestamps = ["2024-10-15", "2024-10-16", "2024-10-14"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result == "2024-10-16"

    # Test case 3: No timestamps within period
    timestamps = ["2024-10-13", "2024-10-21"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result is None

    # Test case 4: Empty timestamp list
    timestamps = []
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result is None

    # Test case 5: List with None values
    timestamps = ["2024-10-15", None, "2024-10-16"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result == "2024-10-16"

    # Test case 6: Timestamps exactly on boundaries
    timestamps = ["2024-10-14", "2024-10-20"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result == "2024-10-20"

    # Test case 7: All timestamps outside period
    timestamps = ["2024-10-01", "2024-10-13", "2024-10-21", "2024-10-30"]
    result = get_latest_survey_timestamp_within_period(
        survey_timestamps=timestamps, start_date="2024-10-14", end_date="2024-10-20"
    )
    assert result is None


def test_get_week_threshold_for_user_dynamic():
    # Test case from docstring example (Wave 2)
    docstring_example_timestamps = [
        "2024-10-10",  # Week 1
        "2024-10-25",  # Week 3
        "2024-10-26",  # Week 3 (later timestamp)
        "2024-11-14",  # Week 6
    ]
    expected_thresholds = [
        "2024-10-10",  # Week 1: survey timestamp
        "2024-10-20",  # Week 2: end of period
        "2024-10-26",  # Week 3: latest survey timestamp
        "2024-11-03",  # Week 4: end of period
        "2024-11-10",  # Week 5: end of period
        "2024-11-14",  # Week 6: survey timestamp
        "2024-11-24",  # Week 7: end of period
        "2024-12-01",  # Week 8: end of period
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=2,
        survey_timestamps=docstring_example_timestamps,
    )
    assert result == expected_thresholds

    # Wave 1, no survey timestamps
    result = get_week_threshold_for_user_dynamic(
        wave=1,
        survey_timestamps=[],
    )
    expected_wave1_no_surveys = [
        "2024-10-06",  # Week 1
        "2024-10-13",  # Week 2
        "2024-10-20",  # Week 3
        "2024-10-27",  # Week 4
        "2024-11-03",  # Week 5
        "2024-11-10",  # Week 6
        "2024-11-17",  # Week 7
        "2024-11-24",  # Week 8
    ]
    assert result == expected_wave1_no_surveys

    # Wave 2, no survey timestamps
    result = get_week_threshold_for_user_dynamic(
        wave=2,
        survey_timestamps=[],
    )
    expected_wave2_no_surveys = [
        "2024-10-13",  # Week 1
        "2024-10-20",  # Week 2
        "2024-10-27",  # Week 3
        "2024-11-03",  # Week 4
        "2024-11-10",  # Week 5
        "2024-11-17",  # Week 6
        "2024-11-24",  # Week 7
        "2024-12-01",  # Week 8
    ]
    assert result == expected_wave2_no_surveys

    # Wave 1, survey timestamps every week (once a week)
    wave1_weekly_surveys = [
        "2024-10-02",  # Week 1
        "2024-10-09",  # Week 2
        "2024-10-16",  # Week 3
        "2024-10-23",  # Week 4
        "2024-10-30",  # Week 5
        "2024-11-06",  # Week 6
        "2024-11-13",  # Week 7
        "2024-11-20",  # Week 8
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=1,
        survey_timestamps=wave1_weekly_surveys,
    )
    assert result == wave1_weekly_surveys

    # Wave 2, survey timestamps every week (once a week)
    wave2_weekly_surveys = [
        "2024-10-09",  # Week 1
        "2024-10-16",  # Week 2
        "2024-10-23",  # Week 3
        "2024-10-30",  # Week 4
        "2024-11-06",  # Week 5
        "2024-11-13",  # Week 6
        "2024-11-20",  # Week 7
        "2024-11-27",  # Week 8
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=2,
        survey_timestamps=wave2_weekly_surveys,
    )
    assert result == wave2_weekly_surveys

    # Wave 1, survey timestamps every other week
    wave1_biweekly_surveys = [
        "2024-10-02",  # Week 1
        "2024-10-16",  # Week 3
        "2024-10-30",  # Week 5
        "2024-11-13",  # Week 7
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=1,
        survey_timestamps=wave1_biweekly_surveys,
    )
    expected_wave1_biweekly = [
        "2024-10-02",  # Week 1: survey
        "2024-10-13",  # Week 2: end of period
        "2024-10-16",  # Week 3: survey
        "2024-10-27",  # Week 4: end of period
        "2024-10-30",  # Week 5: survey
        "2024-11-10",  # Week 6: end of period
        "2024-11-13",  # Week 7: survey
        "2024-11-24",  # Week 8: end of period
    ]
    assert result == expected_wave1_biweekly

    # Wave 2, survey timestamps every other week
    wave2_biweekly_surveys = [
        "2024-10-09",  # Week 1
        "2024-10-23",  # Week 3
        "2024-11-06",  # Week 5
        "2024-11-20",  # Week 7
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=2,
        survey_timestamps=wave2_biweekly_surveys,
    )
    expected_wave2_biweekly = [
        "2024-10-09",  # Week 1: survey
        "2024-10-20",  # Week 2: end of period
        "2024-10-23",  # Week 3: survey
        "2024-11-03",  # Week 4: end of period
        "2024-11-06",  # Week 5: survey
        "2024-11-17",  # Week 6: end of period
        "2024-11-20",  # Week 7: survey
        "2024-12-01",  # Week 8: end of period
    ]
    assert result == expected_wave2_biweekly

    # Wave 1, multiple surveys in one week
    wave1_multiple_surveys = [
        "2024-10-02",  # Week 1
        "2024-10-16",  # Week 3
        "2024-10-17",  # Week 3 (later timestamp)
        "2024-10-30",  # Week 5
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=1,
        survey_timestamps=wave1_multiple_surveys,
    )
    expected_wave1_multiple = [
        "2024-10-02",  # Week 1: survey
        "2024-10-13",  # Week 2: end of period
        "2024-10-17",  # Week 3: latest survey
        "2024-10-27",  # Week 4: end of period
        "2024-10-30",  # Week 5: survey
        "2024-11-10",  # Week 6: end of period
        "2024-11-17",  # Week 7: end of period
        "2024-11-24",  # Week 8: end of period
    ]
    assert result == expected_wave1_multiple

    # Wave 2, multiple surveys in one week
    wave2_multiple_surveys = [
        "2024-10-09",  # Week 1
        "2024-10-23",  # Week 3
        "2024-10-24",  # Week 3 (later timestamp)
        "2024-11-06",  # Week 5
    ]
    result = get_week_threshold_for_user_dynamic(
        wave=2,
        survey_timestamps=wave2_multiple_surveys,
    )
    expected_wave2_multiple = [
        "2024-10-09",  # Week 1: survey
        "2024-10-20",  # Week 2: end of period
        "2024-10-24",  # Week 3: latest survey
        "2024-11-03",  # Week 4: end of period
        "2024-11-06",  # Week 5: survey
        "2024-11-17",  # Week 6: end of period
        "2024-11-24",  # Week 7: end of period
        "2024-12-01",  # Week 8: end of period
    ]
    assert result == expected_wave2_multiple

    # Test invalid wave number
    with pytest.raises(ValueError):
        get_week_threshold_for_user_dynamic(wave=3, survey_timestamps=[])


def test_get_week_thresholds_per_user_dynamic():
    # Test case from docstring example (Wave 2)
    docstring_example_timestamps = [
        "2024-10-10",  # Week 1
        None,  # Week 2
        "2024-10-25",  # Week 3
        "2024-10-26",  # Week 3 (later timestamp)
        None,  # Week 4
        None,  # Week 5
        "2024-11-14",  # Week 6
        None,  # Week 7
        None,  # Week 8
    ]
    valid_weeks_df = pd.DataFrame(
        {
            "handle": ["user1"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": docstring_example_timestamps[
                :-1
            ],  # Remove extra timestamp from Week 3
        }
    )
    user_wave_df = pd.DataFrame({"bluesky_handle": ["user1"], "wave": [2]})

    result_df = get_week_thresholds_per_user_dynamic(valid_weeks_df, user_wave_df)

    # Basic validation
    assert isinstance(result_df, pd.DataFrame)
    assert set(result_df.columns) == {"bluesky_handle", "wave", "date", "week_dynamic"}

    # Test specific dates for Wave 2 user
    def get_week_for_date(df, date):
        return df[df["date"] == date]["week_dynamic"].iloc[0]

    # Verify weeks match the docstring example
    assert get_week_for_date(result_df, "2024-10-09") == 1  # Before first survey
    assert get_week_for_date(result_df, "2024-10-10") == 1  # Survey date
    assert get_week_for_date(result_df, "2024-10-19") == 2  # Week 2
    assert get_week_for_date(result_df, "2024-10-25") == 3  # First survey in week 3
    assert get_week_for_date(result_df, "2024-10-26") == 3  # Second survey in week 3
    assert get_week_for_date(result_df, "2024-11-02") == 4  # Week 4
    assert get_week_for_date(result_df, "2024-11-09") == 5  # Week 5
    assert get_week_for_date(result_df, "2024-11-14") == 6  # Survey in week 6
    assert get_week_for_date(result_df, "2024-11-23") == 7  # Week 7
    assert get_week_for_date(result_df, "2024-11-30") == 8  # Week 8

    # Test Wave 1 with no survey timestamps
    wave1_no_surveys_df = pd.DataFrame(
        {
            "handle": ["user2"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": [None] * 8,
        }
    )
    wave1_user_df = pd.DataFrame({"bluesky_handle": ["user2"], "wave": [1]})

    result_df = get_week_thresholds_per_user_dynamic(wave1_no_surveys_df, wave1_user_df)

    # Verify weeks use default end dates for Wave 1
    assert get_week_for_date(result_df, "2024-10-06") == 1
    assert get_week_for_date(result_df, "2024-10-13") == 2
    assert get_week_for_date(result_df, "2024-10-20") == 3
    assert pd.isna(get_week_for_date(result_df, "2024-11-25"))  # After Wave 1 end

    # Test Wave 2 with no survey timestamps
    wave2_no_surveys_df = pd.DataFrame(
        {
            "handle": ["user3"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": [None] * 8,
        }
    )
    wave2_user_df = pd.DataFrame({"bluesky_handle": ["user3"], "wave": [2]})

    result_df = get_week_thresholds_per_user_dynamic(wave2_no_surveys_df, wave2_user_df)

    # Verify weeks use default end dates for Wave 2
    assert pd.isna(get_week_for_date(result_df, "2024-10-06"))  # Before Wave 2 start
    assert get_week_for_date(result_df, "2024-10-13") == 1
    assert get_week_for_date(result_df, "2024-10-20") == 2
    assert get_week_for_date(result_df, "2024-12-01") == 8

    # Test Wave 1 with weekly surveys
    wave1_weekly_surveys = [
        "2024-10-02",  # Week 1
        "2024-10-09",  # Week 2
        "2024-10-16",  # Week 3
        "2024-10-23",  # Week 4
        "2024-10-30",  # Week 5
        "2024-11-06",  # Week 6
        "2024-11-13",  # Week 7
        "2024-11-20",  # Week 8
    ]
    wave1_weekly_df = pd.DataFrame(
        {
            "handle": ["user4"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": wave1_weekly_surveys,
        }
    )
    wave1_weekly_user_df = pd.DataFrame({"bluesky_handle": ["user4"], "wave": [1]})

    result_df = get_week_thresholds_per_user_dynamic(
        wave1_weekly_df, wave1_weekly_user_df
    )

    # Verify each week ends on survey date
    for week, survey_date in enumerate(wave1_weekly_surveys, 1):
        dates_in_week = result_df[result_df["week_dynamic"] == week]["date"]
        assert all(date <= survey_date for date in dates_in_week)

    # Test Wave 2 with multiple surveys in one week
    wave2_multiple_surveys = [
        "2024-10-09",  # Week 1
        None,  # Week 2
        "2024-10-23",  # Week 3
        "2024-10-24",  # Week 3 (later timestamp)
        None,  # Week 4
        "2024-11-06",  # Week 5
        None,  # Week 6
        None,  # Week 7
    ]
    wave2_multiple_df = pd.DataFrame(
        {
            "handle": ["user5"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": wave2_multiple_surveys,
        }
    )
    wave2_multiple_user_df = pd.DataFrame({"bluesky_handle": ["user5"], "wave": [2]})

    result_df = get_week_thresholds_per_user_dynamic(
        wave2_multiple_df, wave2_multiple_user_df
    )

    # Verify week 3 uses the later survey date
    week3_dates = result_df[result_df["week_dynamic"] == 3]["date"]
    assert all(date <= "2024-10-24" for date in week3_dates)

    # Test empty dataframes
    empty_df = pd.DataFrame(columns=["handle", "survey_week", "survey_timestamp"])
    empty_user_df = pd.DataFrame(columns=["bluesky_handle", "wave"])
    result_df = get_week_thresholds_per_user_dynamic(empty_df, empty_user_df)
    assert len(result_df) == 0

    # Test invalid wave number
    invalid_wave_df = pd.DataFrame(
        {
            "handle": ["user6"] * 8,
            "survey_week": list(range(1, 9)),
            "survey_timestamp": ["2024-10-01"] + [None] * 7,
        }
    )
    invalid_wave_user_df = pd.DataFrame({"bluesky_handle": ["user6"], "wave": [3]})
    with pytest.raises(ValueError):
        get_week_thresholds_per_user_dynamic(invalid_wave_df, invalid_wave_user_df)

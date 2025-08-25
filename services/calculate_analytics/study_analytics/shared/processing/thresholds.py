"""Threshold calculation functions for analytics processing.

This module provides reusable functions for calculating weekly thresholds
and mapping dates to week numbers for study analysis.
"""

from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.shared.config import get_config

logger = get_logger(__file__)


def map_date_to_static_week(partition_date: str, wave: int) -> int:
    """Map a partition date to a static week number, based on the user's wave.

    Returns a week number between 1 and 8.

    Weeks are defined Monday -> Sunday. The dates in the wave configuration
    are the end dates of each week (inclusive), meaning these are Sundays.

    Args:
        partition_date: Date string in YYYY-MM-DD format
        wave: User's wave number (1 or 2)

    Returns:
        Week number between 1 and 8

    Raises:
        ValueError: If wave is not 1 or 2
    """
    config = get_config()
    week_config = config.weeks

    if wave == 1:
        week_end_dates = week_config.wave_1_week_end_dates_inclusive
    elif wave == 2:
        week_end_dates = week_config.wave_2_week_end_dates_inclusive
    else:
        raise ValueError(f"Invalid wave: {wave}")

    week = 1
    for end_date in week_end_dates:
        if partition_date <= end_date:
            break
        week += 1

    return week


def get_latest_survey_timestamp_within_period(
    survey_timestamps: List[str],
    start_date: str,
    end_date: str,
) -> Optional[str]:
    """Get the latest survey timestamp within a given period.

    Returns the latest survey timestamp that falls within the given period.
    If there are no survey timestamps within the period, returns None.

    Args:
        survey_timestamps: List of survey timestamps
        start_date: Start date of the period (inclusive)
        end_date: End date of the period (inclusive)

    Returns:
        Latest survey timestamp within period, or None if none found
    """
    # Filter out any None values and NaT values from survey timestamps
    survey_timestamps = [
        ts for ts in survey_timestamps if ts is not None and pd.notna(ts)
    ]

    # Return None if no valid timestamps
    if not survey_timestamps:
        return None

    # Get latest timestamp within period
    valid_timestamps = []
    for survey_timestamp in survey_timestamps:
        if isinstance(survey_timestamp, date):
            survey_timestamp = survey_timestamp.strftime("%Y-%m-%d")
        # Skip if timestamp is NaT
        if pd.isna(survey_timestamp):
            continue
        if start_date <= survey_timestamp <= end_date:
            valid_timestamps.append(survey_timestamp)

    # Return latest valid timestamp
    if valid_timestamps:
        return max(valid_timestamps)
    return None


def get_week_threshold_for_user_dynamic(
    wave: int, survey_timestamps: List[str]
) -> List[str]:
    """Returns inclusive end dates for each week, based on when the user
    filled out the survey.

    For a given time period, checks to see if there are any `survey_timestamp`
    values within that week. If so, it uses that survey timestamp as the end
    date of that week. Else, it uses the end date (inclusive) of the date range.

    Weeks go Monday -> Sunday.

    Args:
        wave: User's wave number (1 or 2)
        survey_timestamps: List of survey timestamps for the user

    Returns:
        List of week end dates (inclusive)

    Raises:
        ValueError: If wave is not 1 or 2
    """
    config = get_config()
    week_config = config.weeks

    week_thresholds: List[str] = []

    if wave == 1:
        week_starts = week_config.wave_1_week_start_dates_inclusive
        week_ends = week_config.wave_1_week_end_dates_inclusive
    elif wave == 2:
        week_starts = week_config.wave_2_week_start_dates_inclusive
        week_ends = week_config.wave_2_week_end_dates_inclusive
    else:
        raise ValueError(f"Invalid wave: {wave}")

    for start_date, end_date in zip(week_starts, week_ends):
        ts = get_latest_survey_timestamp_within_period(
            survey_timestamps=survey_timestamps,
            start_date=start_date,
            end_date=end_date,
        )
        if ts:
            week_thresholds.append(ts)
        else:
            week_thresholds.append(end_date)

    return week_thresholds


def map_date_to_dynamic_week(
    date_str: str, dynamic_week_thresholds: List[str]
) -> Optional[int]:
    """Map a date to a dynamic week number.

    Returns a week number between 1 and 8.

    Weeks go Monday -> Sunday.

    We can have weeks > 8 (e.g., week = 9), this means that the date
    in question is after the user filled out the survey in their last
    week.

    Args:
        date_str: Date string to map
        dynamic_week_thresholds: List of dynamic week thresholds

    Returns:
        Week number between 1 and 8, or None if date is beyond week 8
    """
    week = 1
    for threshold in dynamic_week_thresholds:
        if date_str <= threshold:
            break
        week += 1

    # If week is beyond the number of thresholds, return None
    if week > len(dynamic_week_thresholds):
        return None
    return week


def get_week_thresholds_per_user_static(
    user_handle_to_wave_df: pd.DataFrame,
) -> pd.DataFrame:
    """Get the week thresholds for each user, based on a Monday -> Monday
    week schedule.

    Returns a dataframe with three columns:
    - bluesky_handle: str
    - wave: int
    - date: %Y-%m-%d
    - week_static: 1-8

    Requires knowing the user's wave in order to offset their week
    cutoffs correctly.

    Args:
        user_handle_to_wave_df: DataFrame with user handle and wave information

    Returns:
        DataFrame with static week assignments for each user and date
    """
    config = get_config()
    study_config = config.study

    # If empty DataFrame, return empty DataFrame with correct columns
    if len(user_handle_to_wave_df) == 0:
        return pd.DataFrame(columns=["bluesky_handle", "wave", "date", "week_static"])

    partition_dates: List[str] = study_config.get_partition_dates()

    # Create a list of all combinations of handles and partition dates
    user_date_combinations = []
    for _, row in user_handle_to_wave_df.iterrows():
        bluesky_handle = row["bluesky_handle"]
        wave = row["wave"]
        for partition_date in partition_dates:
            # account for different waves, where wave 1 ends 11/25 and wave 2
            # ends 12/1.
            if (
                wave == 1
                and partition_date > study_config.wave_1_study_end_date_inclusive
            ):
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": None,
                    }
                )
            elif (
                wave == 2
                and partition_date < study_config.wave_2_study_start_date_inclusive
            ):
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": None,
                    }
                )
            else:
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": map_date_to_static_week(partition_date, wave),
                    }
                )

    df = pd.DataFrame(user_date_combinations)
    df = df.sort_values(["bluesky_handle", "date"])
    df = df[["bluesky_handle", "date", "wave", "week_static"]]

    return df


def get_week_thresholds_per_user_dynamic(
    qualtrics_logs: pd.DataFrame, user_handle_to_wave_df: pd.DataFrame
) -> pd.DataFrame:
    """Get the week thresholds for each user, based on when they filled out
    the survey.

    Returns a dataframe with the following columns:
    - bluesky_handle: str
    - wave: int
    - date: %Y-%m-%d
    - week_dynamic: 1-8

    Args:
        qualtrics_logs: DataFrame with survey log data
        user_handle_to_wave_df: DataFrame with user handle and wave information

    Returns:
        DataFrame with dynamic week assignments for each user and date
    """
    config = get_config()
    study_config = config.study

    partition_dates: List[str] = study_config.get_partition_dates()

    user_handle_to_wave: Dict[str, int] = {
        row["bluesky_handle"]: row["wave"]
        for _, row in user_handle_to_wave_df.iterrows()
    }

    # Sort by handle and survey_week in ascending order
    qualtrics_logs = qualtrics_logs.sort_values(
        ["handle", "survey_timestamp_date"], ascending=[True, True]
    )

    # every user has 8 survey timestamps, one per week. This was verified
    # before already.
    user_survey_timestamps_df = qualtrics_logs[["handle", "survey_timestamp_date"]]

    user_to_week_thresholds: Dict[str, List[str]] = {}
    for handle, user_df in user_survey_timestamps_df.groupby("handle"):
        user_to_week_thresholds[handle] = get_week_threshold_for_user_dynamic(
            wave=user_handle_to_wave[handle],
            survey_timestamps=user_df["survey_timestamp_date"].tolist(),
        )

    user_date_combinations = []

    for _, row in user_handle_to_wave_df.iterrows():
        bluesky_handle = row["bluesky_handle"]
        wave = row["wave"]
        end_of_week_dates = user_to_week_thresholds[bluesky_handle]
        for partition_date in partition_dates:
            # account for different waves, where wave 1 ends 11/25 and wave 2
            # ends 12/1.
            if (
                wave == 1
                and partition_date > study_config.wave_1_study_end_date_inclusive
            ):
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_dynamic": None,
                    }
                )
            elif (
                wave == 2
                and partition_date < study_config.wave_2_study_start_date_inclusive
            ):
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_dynamic": None,
                    }
                )
            else:
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_dynamic": map_date_to_dynamic_week(
                            date_str=partition_date,
                            dynamic_week_thresholds=end_of_week_dates,
                        ),
                    }
                )

    df: pd.DataFrame = pd.DataFrame(user_date_combinations)
    df = df.sort_values(["bluesky_handle", "date"])
    df = df[["bluesky_handle", "date", "wave", "week_dynamic"]]

    return df

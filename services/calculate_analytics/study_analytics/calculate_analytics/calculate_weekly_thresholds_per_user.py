"""One-off script to calculate the week thresholds for each user.

Calculates both the static and dynamic week thresholds for each user.
"""

from datetime import date
import os
from typing import Optional

import pandas as pd

from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.generate_reports.constants import (
    wave_1_study_start_date_inclusive,
    wave_1_study_end_date_inclusive,
    wave_2_study_start_date_inclusive,
    wave_2_study_end_date_inclusive,
    wave_1_week_start_dates_inclusive,
    wave_1_week_end_dates_inclusive,
    wave_2_week_start_dates_inclusive,
    wave_2_week_end_dates_inclusive,
)
from services.calculate_analytics.study_analytics.deprecated.get_fine_grained_weekly_usage_reports import (
    fix_misspelled_handle,
    DUPLICATE_WAVE_USERS,
    EXCLUDELIST_HANDLES,
)
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

start_date_inclusive = wave_1_study_start_date_inclusive
end_date_inclusive = wave_2_study_end_date_inclusive  # 2024-12-01 (inclusive)
exclude_partition_dates = ["2024-10-08"]

logger = get_logger(__file__)

current_filedir = os.path.dirname(os.path.abspath(__file__))


# TODO: include the "Wave" that they were in.
def load_user_demographic_info() -> pd.DataFrame:
    """Load the user demographic info for the study."""
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_df: pd.DataFrame = pd.DataFrame([user.model_dump() for user in users])
    # Drop rows where is_study_user is False to only keep actual study participants
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]
    return user_df


def map_date_to_static_week(partition_date: str, wave: int) -> int:
    """Map a partition date to a static week number, based on the user's wave.

    Returns a week number between 1 and 8.

    Weeks are defined Monday -> Sunday. The dates in
    `wave_1_week_end_dates_inclusive` and `wave_2_week_end_dates_inclusive`
    are the end dates of each week (inclusive), meaning these are Sundays.
    """

    week = 1

    if wave == 1:
        for end_date in wave_1_week_end_dates_inclusive:
            if partition_date <= end_date:
                break
            week += 1

    elif wave == 2:
        for end_date in wave_2_week_end_dates_inclusive:
            if partition_date <= end_date:
                break
            week += 1

    else:
        raise ValueError(f"Invalid wave: {wave}")

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
    """
    # If empty DataFrame, return empty DataFrame with correct columns
    if len(user_handle_to_wave_df) == 0:
        return pd.DataFrame(columns=["bluesky_handle", "wave", "date", "week_static"])

    partition_dates: list[str] = get_partition_dates(
        start_date=start_date_inclusive,
        end_date=end_date_inclusive,
        exclude_partition_dates=[],
    )
    # Create a list of all combinations of handles and partition dates
    user_date_combinations = []
    for _, row in user_handle_to_wave_df.iterrows():
        bluesky_handle = row["bluesky_handle"]
        wave = row["wave"]
        for partition_date in partition_dates:
            # account for different waves, where wave 1 ends 11/25 and wave 2
            # ends 12/1.
            if wave == 1 and partition_date > wave_1_study_end_date_inclusive:
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": None,
                    }
                )
            elif wave == 2 and partition_date < wave_2_study_start_date_inclusive:
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


def get_latest_survey_timestamp_within_period(
    survey_timestamps: list[str],
    start_date: str,
    end_date: str,
):
    """Get the latest survey timestamp within a given period.

    Returns the latest survey timestamp that falls within the given period.
    If there are no survey timestamps within the period, returns None.

    Args:
        survey_timestamps: list of survey timestamps
        start_date: start date of the period
        end_date: end date of the period
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
    wave: int, survey_timestamps: list[str]
) -> list[str]:
    """Returns inclusive end dates for each week, based on when the user
    filled out the survey.

    For a given time period, checks to see if there are any `survey_timestamp`
    values within that week. If so, it uses that survey timestamp as the end
    date of that week. Else, it uses the end date (inclusive) of the date range.

    Weeks go Monday -> Sunday.

    Example: let's say that a user is in Wave 2. Their weeks would be normally
    defined as:
    - 2024-10-07 -> 2024-10-13
    - 2024-10-14 -> 2024-10-20
    - 2024-10-21 -> 2024-10-27
    - 2024-10-28 -> 2024-11-03
    - 2024-11-04 -> 2024-11-10
    - 2024-11-11 -> 2024-11-17
    - 2024-11-18 -> 2024-11-24
    - 2024-11-25 -> 2024-12-01

    Let's say the user fills out surveys on:
    - 2024-10-10
    - 2024-10-25
    - 2024-10-26
    - 2024-11-14

    The algorithm would proceed as follows:
    - In the first period, 2024-10-07 -> 2024-10-13, the user filled out a survey.
    Therefore the end date is the date of the survey, 2024-10-10.
    - In the second period, 2024-10-14 -> 2024-10-20, the user did not fill out a survey.
    Therefore the end date is the end date of the period, 2024-10-20.
    - In the third period, 2024-10-21 -> 2024-10-27, the user filled out surveys
    on both 2024-10-25 and 2024-10-26. We take the later of these two values, so
    we set the end date to 2024-10-26.
    - In the fourth period, 2024-10-28 -> 2024-11-03, the user did not fill out
    a survey, so we set the end date to 2024-11-03.
    - In the fifth period, 2024-11-04 -> 2024-11-10, the user did not fill out
    a survey, so we set the end date to 2024-11-10.
    - In the sixth period, 2024-11-11 -> 2024-11-17, the user filled out a survey.
    Therefore the end date is the date of the survey, 2024-11-14.
    - In the seventh period, 2024-11-18 -> 2024-11-24, the user did not fill out
    a survey, so we set the end date to 2024-11-24.
    - In the eighth period, 2024-11-25 -> 2024-12-01, the user did not fill out
    a survey, so we set the end date to 2024-12-01.

    Therefore, the output would be:
    - 2024-10-10
    - 2024-10-20
    - 2024-10-26
    - 2024-11-03
    - 2024-11-10
    - 2024-11-14
    - 2024-11-24
    - 2024-12-01
    """
    week_thresholds: list[
        str
    ] = []  # dates inclusive, so if the first element is 10/06, then dates up to and including 10/06 are in week 1.
    if wave == 1:
        for start_date, end_date in zip(
            wave_1_week_start_dates_inclusive, wave_1_week_end_dates_inclusive
        ):
            ts = get_latest_survey_timestamp_within_period(
                survey_timestamps=survey_timestamps,
                start_date=start_date,
                end_date=end_date,
            )
            if ts:
                week_thresholds.append(ts)
            else:
                week_thresholds.append(end_date)
    elif wave == 2:
        for start_date, end_date in zip(
            wave_2_week_start_dates_inclusive, wave_2_week_end_dates_inclusive
        ):
            ts = get_latest_survey_timestamp_within_period(
                survey_timestamps, start_date, end_date
            )
            if ts:
                week_thresholds.append(ts)
            else:
                week_thresholds.append(end_date)
    else:
        raise ValueError(f"Invalid wave: {wave}")
    return week_thresholds


def map_date_to_dynamic_week(
    date: str, dynamic_week_thresholds: list[str]
) -> Optional[int]:
    """Map a date to a dynamic week number.

    Returns a week number between 1 and 8.

    Weeks go Monday -> Sunday.

    We can have weeks > 8 (e.g., week = 9), this means that the date
    in question is after the user filled out the survey in their last
    week.

    For example, for Wave 1, the last week ends 2024-11-24. We already mark
    as NaN the dates in the range 2024-09-30 -> 2024-12-01 that are after
    2024-11-24 since the users are not counted for those dates. However,
    during that last week, some users filled out their surveys before 2024-11-24,
    so we set their threshold to the date they filled out their survey and we
    say that that's the last day that their data is counted and we don't
    count any other days past that.
    """
    week = 1
    for threshold in dynamic_week_thresholds:
        if date <= threshold:
            break
        week += 1

    if week > 8:
        return None
    return week


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
    """
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date_inclusive,
        end_date=end_date_inclusive,
        exclude_partition_dates=[],
    )

    user_handle_to_wave: dict[str, int] = {
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

    user_to_week_thresholds: dict[str, list[str]] = {}
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
            if wave == 1 and partition_date > wave_1_study_end_date_inclusive:
                user_date_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_dynamic": None,
                    }
                )
            elif wave == 2 and partition_date < wave_2_study_start_date_inclusive:
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
                            date=partition_date,
                            dynamic_week_thresholds=end_of_week_dates,
                        ),
                    }
                )

    df: pd.DataFrame = pd.DataFrame(user_date_combinations)
    df = df.sort_values(["bluesky_handle", "date"])
    df = df[["bluesky_handle", "date", "wave", "week_dynamic"]]

    return df


def filter_process_users_from_qualtrics_data(
    qualtrics_df: pd.DataFrame,
    filter_duplicate_wave_users: bool = True,
) -> pd.DataFrame:
    """Filter and process the handles in the qualtrics dataframe."""
    qualtrics_df["handle"] = qualtrics_df["handle"].apply(fix_misspelled_handle)

    # remove excluded handles
    qualtrics_df = qualtrics_df[~qualtrics_df["handle"].isin(EXCLUDELIST_HANDLES)]

    if filter_duplicate_wave_users:
        # filter out duplicate wave users
        qualtrics_df = qualtrics_df[
            ~(
                (qualtrics_df["handle"].isin(DUPLICATE_WAVE_USERS.keys()))
                & (
                    qualtrics_df["wave"]
                    == qualtrics_df["handle"].map(DUPLICATE_WAVE_USERS)
                )
            )
        ]
    return qualtrics_df


def get_user_handle_to_wave_df(qualtrics_df: pd.DataFrame) -> pd.DataFrame:
    """Get a mapping of user handles to the wave they were in."""
    qualtrics_df = qualtrics_df[["handle", "wave"]]
    qualtrics_df = qualtrics_df.dropna(subset=["handle"])
    qualtrics_df["handle"] = qualtrics_df["handle"].str.lower()
    # Rename handle to bluesky_handle to match other dataframes
    qualtrics_df = qualtrics_df.rename(columns={"handle": "bluesky_handle"})
    # Get unique combinations of bluesky_handle and wave
    qualtrics_df = qualtrics_df.drop_duplicates(subset=["bluesky_handle", "wave"])
    return qualtrics_df


def generate_week_thresholds():
    """Generate the week thresholds for each user."""

    # load data from Qualtrics
    qualtrics_logs: pd.DataFrame = pd.read_csv(
        os.path.join(current_filedir, "qualtrics_logs.csv")
    )
    qualtrics_logs = filter_process_users_from_qualtrics_data(qualtrics_logs)
    qualtrics_logs = qualtrics_logs[["handle", "wave", "condition", "StartDate"]]
    qualtrics_logs["survey_timestamp_mountain_time"] = pd.to_datetime(
        qualtrics_logs["StartDate"],
        format="%m/%d/%Y %H:%M",
        errors="coerce",
    ).dt.tz_localize("America/Denver")  # Explicitly set Mountain Time timezone
    qualtrics_logs["survey_timestamp_utc"] = qualtrics_logs[
        "survey_timestamp_mountain_time"
    ].dt.tz_convert("UTC")
    qualtrics_logs["survey_timestamp_date"] = qualtrics_logs[
        "survey_timestamp_utc"
    ].dt.date
    qualtrics_logs["survey_timestamp_date"] = qualtrics_logs[
        "survey_timestamp_date"
    ].astype(str)
    qualtrics_logs = qualtrics_logs[
        ["handle", "wave", "condition", "survey_timestamp_date"]
    ]

    # grabbing this file only since we filtered out users correctly in this file.
    valid_weeks_per_bluesky_user: pd.DataFrame = pd.read_csv(
        os.path.join(current_filedir, "valid_weeks_per_bluesky_user.csv")
    )
    valid_weeks_per_bluesky_user = filter_process_users_from_qualtrics_data(
        valid_weeks_per_bluesky_user,
        filter_duplicate_wave_users=False,
    )
    valid_weeks_per_bluesky_user = valid_weeks_per_bluesky_user[["handle"]]

    # consolidate handles and use only the valid ones (people who filled out
    # the Qualtrics survey and weren't filtered out).
    valid_weeks_per_bluesky_user_bsky_handles_set = set(
        valid_weeks_per_bluesky_user["handle"]
    )
    valid_user_handles = valid_weeks_per_bluesky_user_bsky_handles_set
    qualtrics_logs = qualtrics_logs[qualtrics_logs["handle"].isin(valid_user_handles)]

    # start wrangling demographic and Qualtrics data
    user_handle_to_wave_df: pd.DataFrame = get_user_handle_to_wave_df(
        qualtrics_df=qualtrics_logs
    )
    logger.info(
        f"Loaded user handle to wave mapping with {len(user_handle_to_wave_df)} rows"
    )
    week_thresholds_per_user_static: pd.DataFrame = get_week_thresholds_per_user_static(
        user_handle_to_wave_df=user_handle_to_wave_df
    )
    logger.info(
        f"Generated week thresholds per user static with {len(week_thresholds_per_user_static)} rows"
    )
    week_thresholds_per_user_dynamic: pd.DataFrame = (
        get_week_thresholds_per_user_dynamic(
            qualtrics_logs=qualtrics_logs, user_handle_to_wave_df=user_handle_to_wave_df
        )
    )
    logger.info(
        f"Generated week thresholds per user dynamic with {len(week_thresholds_per_user_dynamic)} rows"
    )
    week_thresholds: pd.DataFrame = week_thresholds_per_user_static.merge(
        week_thresholds_per_user_dynamic,
        on=["bluesky_handle", "date", "wave"],
        how="left",
    )
    assert (
        len(week_thresholds) == len(week_thresholds_per_user_static)
    ), f"Expected {len(week_thresholds_per_user_static)} rows after merge but got {len(week_thresholds)}"
    assert (
        len(week_thresholds) == len(week_thresholds_per_user_dynamic)
    ), f"Expected {len(week_thresholds_per_user_dynamic)} rows after merge but got {len(week_thresholds)}"

    # Export week thresholds.
    week_thresholds.to_csv(
        os.path.join(current_filedir, "bluesky_per_user_week_assignments.csv")
    )
    return week_thresholds

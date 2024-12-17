"""We adapt the weekly usage reports to be fine-grained by week, based on
when the user filled out their Qualtrics weekly survey."""

from datetime import datetime, timedelta
import os
import pytz
from typing import Literal

import pandas as pd

from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

current_dir = os.path.dirname(os.path.abspath(__file__))

# these are handles that are marked as to be excluded from the study.
EXCLUDELIST_HANDLES = [
    "countryboy.bsky.social",  # account is suspended on Bluesky.
    "thelittlemage.bsky.social",  # account doesn't exist on Bluesky.
    "john.doe.bsky.social",  # account doesn't exist on Bluesky.
    "eighty.gay",  # marked as red in the FINAL_USERLIST .csv file.
    "ljameson.bsky.social",  # account is suspended on Bluesky and they don't have any logs.
    "jbouie.bsky.social",  # account is suspended on Bluesky and they don't have any logs.
]

# map of misspelled handles to correct handle
HANDLE_MAPPING = {
    "igorlollipop.bskt.social": "igorlollipop.bsky.social",
    "@skruffyyy.bsky.social": "skruffyyy.bsky.social",
}

# map of users who signed up to both Wave 1 and Wave 2, as well as which
# wave to filter.
DUPLICATE_WAVE_USERS = {
    "bearsohmy.bsky.social": 2  # filter Wave 2, keep Wave 1
}

# get timestamps for weekly cutoffs
ct_tz = pytz.timezone(
    "America/Chicago"
)  # Create central time datetime for Oct 6 2024 11:59pm
end_of_week_1_dt = ct_tz.localize(datetime(2024, 10, 6, 23, 59))

weeks_dts = [end_of_week_1_dt]
total_weeks = 9
for i in range(1, total_weeks):
    weeks_dts.append(end_of_week_1_dt + timedelta(weeks=i))

# convert to UTC
UTC_DTS = [week_dt.astimezone(pytz.UTC) for week_dt in weeks_dts]
UTC_DTS_STRS = [week_dt.strftime("%Y-%m-%d-%H:%M:%S") for week_dt in UTC_DTS]

# total days that we do the lookback for to see if a user logged in before
# filling out the survey.
LOOKBACK_DAYS = 4
LOOKBACK_HOURS = LOOKBACK_DAYS * 24

# Set pandas display options to show all columns
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


def fix_misspelled_handle(handle: str) -> str:
    if handle in HANDLE_MAPPING:
        return HANDLE_MAPPING[handle]
    return handle


def convert_qualtrics_timestamp_to_std_format(timestamp: str) -> str:
    # First parse the input timestamp
    mt_datetime = datetime.strptime(timestamp, "%m/%d/%Y %H:%M")

    # Create Mountain Time timezone object
    # pytz handles daylight savings automatically based on the date
    mt_tz = pytz.timezone("America/Denver")

    # Localize the datetime to Mountain Time
    mt_datetime = mt_tz.localize(mt_datetime)

    # Convert to UTC
    utc_datetime = mt_datetime.astimezone(pytz.UTC)

    # Format as requested
    return utc_datetime.strftime("%Y-%m-%d-%H:%M:%S")


def map_timestamp_to_survey_week(timestamp: str, wave: Literal[1, 2]) -> int:
    """Given a timestamp and a wave, map the timestamp to the week that the
    survey was sent.

    For each user, we still want to use the actual time that they filled out the
    survey as the ground truth for that week (i.e., we only count activity of that
    week to be prior to them filling out the survey), but we want to map the
    timestamp of both the Qualtrics surveys as well as the activities to the
    week that the survey was sent.

    So, this will tell us for which survey week the qualtrics survey or the
    user login is assigned to, but then we still also need to check if the
    user logged in prior to filling out the survey.

    We'll eventually have two fields for each user session log:
    - `survey_week`: the week that the survey was sent.
    - `logged_in_before_survey`: whether the user logged in before the survey was sent.
        This will be False if (1) the user logged in after the survey was sent,
        or (2) the user didn't fill out the survey that week.

    We'll set Week 1 to end on Oct 6, 2024, at 11:59pm (Central Time, USA).
    """
    week = 0

    # map the timestamp to the week
    for week_dt_str in UTC_DTS_STRS:
        week += 1
        if timestamp < week_dt_str:
            break

    if wave == 2:
        # for Wave 2, we want to subtract 1 week from the survey week
        # because the survey was sent 1 week after the end of Week 1.
        week -= 1

    return week


def remove_duplicate_responses_within_timeframe(
    df: pd.DataFrame, threshold_hours: int = 24
) -> pd.DataFrame:
    """Remove duplicate survey responses that occur within a specified timeframe.

    Algorithm steps:
    1. For each unique handle in the dataframe:
        a. Get all responses for that handle
        b. Sort responses by timestamp ascending
        c. Compare each response with the next chronological response
        d. If two consecutive responses are within threshold_hours of each other,
            mark the later response for removal
    2. Remove all marked duplicate responses

    Args:
        df: DataFrame containing survey responses with 'handle' and 'utc_timestamp' columns
        threshold_hours: Number of hours within which to consider responses as duplicates
                        (default: 24)

    Returns:
        DataFrame with duplicate responses removed
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Create a new column with datetime objects
    df["utc_timestamp_dt"] = pd.to_datetime(
        df["utc_timestamp"], format="%Y-%m-%d-%H:%M:%S"
    )

    # Process each handle's responses to remove duplicates within threshold
    handles = df["handle"].unique()
    rows_to_drop = []

    for handle in handles:
        # Get all responses for this handle, sorted by timestamp
        handle_responses = df[df["handle"] == handle].sort_values("utc_timestamp_dt")

        if len(handle_responses) > 1:
            # Compare each response with the next one
            for i in range(len(handle_responses) - 1):
                current_response = handle_responses.iloc[i]
                next_response = handle_responses.iloc[i + 1]

                # Calculate time difference in hours
                time_diff = (
                    next_response["utc_timestamp_dt"]
                    - current_response["utc_timestamp_dt"]
                ).total_seconds() / 3600

                # If responses are within threshold, mark the later one for removal
                if time_diff < threshold_hours:
                    handle = next_response["handle"]
                    survey_week = next_response["survey_week"]
                    rows_to_drop.append((handle, survey_week))

    # Remove the identified duplicate rows and the temporary datetime column
    df = df[
        ~df.apply(
            lambda row: (row["handle"], row["survey_week"]) in rows_to_drop, axis=1
        )
    ]
    df = df.drop(columns=["utc_timestamp_dt"])

    return df


def get_hours_between_two_timestamps(timestamp1: str, timestamp2: str) -> float:
    """Calculate hours between two timestamps in format YYYY-MM-DD-HH:MM:SS"""
    dt1 = datetime.strptime(timestamp1, "%Y-%m-%d-%H:%M:%S")
    dt2 = datetime.strptime(timestamp2, "%Y-%m-%d-%H:%M:%S")
    time_diff = abs(dt2 - dt1)
    return time_diff.total_seconds() / 3600


def get_user_valid_weeks(
    qualtrics_df: pd.DataFrame, user_session_logs_df: pd.DataFrame
) -> pd.DataFrame:
    """Get the valid weeks for each user based on survey completion and app usage.

    This function determines whether each user's survey responses are valid by checking if they
    actively used the app before completing the survey. For each user and survey week:

    1. Check if they completed the survey for that week
    2. If they did complete the survey:
       - Look for any app logins within LOOKBACK_HOURS (4 days) before their survey timestamp
       - Mark the week as valid if they logged in during this period
    3. If they did not complete the survey:
       - Check if they logged in during that week using predefined week cutoffs
       - Record login status but mark as invalid since survey wasn't completed

    The 4-day lookback period serves two purposes:
    1. Ensures recency - User feedback is based on recent app experience
    2. Flexibility - Accommodates users who may have used the app earlier in the week
       (e.g. Sunday) but filled out the survey later (e.g. Tuesday)

    Args:
        qualtrics_df: DataFrame containing survey responses with columns:
            - handle: User identifier
            - survey_week: Week number of the survey
            - StartDate: Survey completion timestamp
            - utc_timestamp: UTC timestamp of survey completion
        user_session_logs_df: DataFrame containing app usage logs with columns:
            - handle: User identifier
            - timestamp: Login timestamp
            - survey_week: Week number of the login

    Returns:
        DataFrame with columns:
            - handle: User identifier
            - survey_week: Week number (1-8)
            - filled_out_survey: Whether user completed survey that week
            - survey_timestamp: Local timestamp of survey completion
            - survey_timestamp_utc: UTC timestamp of survey completion
            - user_logged_in_before_survey: Whether user logged in within lookback period
            - user_login_timestamp_before_survey: Timestamp of qualifying login
            - valid_login: Whether the week is considered valid (logged in + completed survey)
    """
    qualtrics_handles = qualtrics_df["handle"].unique()
    user_session_logs_df = user_session_logs_df[
        user_session_logs_df["handle"].isin(qualtrics_handles)
    ]
    new_rows: list[dict] = []

    for handle in qualtrics_handles:
        subset_qualtrics_df = qualtrics_df[qualtrics_df["handle"] == handle]
        subset_user_session_logs_df = user_session_logs_df[
            user_session_logs_df["handle"] == handle
        ]

        has_user_session_logs = not subset_user_session_logs_df.empty
        user_session_log_timestamps = subset_user_session_logs_df["timestamp"].unique()

        for week in range(1, 9):
            res = {
                "handle": handle,
                "survey_week": week,
            }
            qualtrics_row = subset_qualtrics_df[
                subset_qualtrics_df["survey_week"] == week
            ]

            # if they didn't fill out the survey:
            if qualtrics_row.empty:
                res["filled_out_survey"] = False
                res["survey_timestamp"] = None
                res["survey_timestamp_utc"] = None

                if has_user_session_logs:
                    # if they didn't fill out a survey for that week, we'll
                    # still check if they logged in that week anyways. We'll
                    # use the hardcoded week cutoffs instead of the timestamp
                    # of when they filled out the survey.
                    valid_user_session_logs_for_week = subset_user_session_logs_df[
                        subset_user_session_logs_df["survey_week"] == week
                    ]
                    if not valid_user_session_logs_for_week.empty:
                        res["user_logged_in_before_survey"] = False
                        res["user_logged_in_during_week"] = True
                        res["user_login_timestamp_before_survey"] = None
                        res["user_login_timestamp_during_week"] = (
                            valid_user_session_logs_for_week["timestamp"].min()
                        )
                    else:
                        res["user_logged_in_before_survey"] = False
                        res["user_logged_in_during_week"] = False
                        res["user_login_timestamp_before_survey"] = None
                        res["user_login_timestamp_during_week"] = None
                else:
                    res["user_logged_in_before_survey"] = False
                    res["user_logged_in_during_week"] = False
                    res["user_login_timestamp_before_survey"] = None
                    res["user_login_timestamp_during_week"] = None

            else:
                res["filled_out_survey"] = True
                res["survey_timestamp"] = qualtrics_row["StartDate"].unique()[0]
                res["survey_timestamp_utc"] = qualtrics_row["utc_timestamp"].unique()[0]

                if has_user_session_logs:
                    # only count a user as valid if they (1) filled out the survey, and (2)
                    # logged into the app sometime within 4 days before the survey was sent.
                    for timestamp in user_session_log_timestamps:
                        if (
                            get_hours_between_two_timestamps(
                                timestamp, res["survey_timestamp_utc"]
                            )
                            < LOOKBACK_HOURS
                        ):
                            res["user_logged_in_before_survey"] = True
                            res["user_login_timestamp_before_survey"] = timestamp
                            res["user_logged_in_during_week"] = True
                            res["user_login_timestamp_during_week"] = timestamp
                            break
                    else:
                        res["user_logged_in_before_survey"] = False
                        res["user_login_timestamp_before_survey"] = None
                        res["user_logged_in_during_week"] = False
                        res["user_login_timestamp_during_week"] = None

                    # if they (1) filled out the survey but (2) didn't log in
                    # before the survey, we'll check if they logged in during
                    # that week at all. This is useful for edge cases where,
                    # say, there's a week from 11/10 to 11/17, they filled out
                    # the survey on 11/11, but they logged in on 11/12. We only
                    # want to count them as a valid week if they logged in
                    # before the survey (here, they didn't), but we should also
                    # note if they logged in at all during that week, even after
                    # filling out the survey. We record the earliest timestamp
                    # of when they logged in during that week.
                    if not res["user_logged_in_before_survey"]:
                        valid_user_session_logs_for_week = subset_user_session_logs_df[
                            subset_user_session_logs_df["survey_week"] == week
                        ]
                        if not valid_user_session_logs_for_week.empty:
                            res["user_logged_in_during_week"] = True
                            res["user_login_timestamp_during_week"] = (
                                valid_user_session_logs_for_week["timestamp"].min()
                            )
                        else:
                            res["user_logged_in_during_week"] = False
                            res["user_login_timestamp_during_week"] = None

                else:
                    res["user_logged_in_before_survey"] = False
                    res["user_login_timestamp_before_survey"] = None
                    res["user_logged_in_during_week"] = False
                    res["user_login_timestamp_during_week"] = None

            res["valid_login"] = res["user_logged_in_before_survey"]

            new_rows.append(res)

    # we count a user as valid if they logged into the app, say, sometime within
    # 4 days before they filled out the survey. This should be both (1) recent
    # enough to be relevant, and (2) account for cases where they, say, filled
    # out a survey on Tuesday but last opened the app on Sunday (we don't want to
    # punish them if they didn't happen to open the app that week, since it
    # had only been 2 days).

    new_df = pd.DataFrame(
        new_rows,
        columns=[
            "handle",
            "survey_week",
            "filled_out_survey",
            "survey_timestamp",
            "survey_timestamp_utc",
            "user_logged_in_before_survey",
            "user_login_timestamp_before_survey",
            "user_logged_in_during_week",
            "user_login_timestamp_during_week",
            "valid_login",
        ],
    )

    # Sort by handle descending and survey week ascending
    new_df = new_df.sort_values(["handle", "survey_week"], ascending=[True, True])

    return new_df


def main():
    # load data.
    qualtrics_df = pd.read_csv(os.path.join(current_dir, "qualtrics_logs.csv"))
    qualtrics_df = qualtrics_df[["StartDate", "handle", "wave"]]
    qualtrics_df = qualtrics_df.dropna(subset=["handle"])

    user_session_logs_df = pd.read_parquet(
        os.path.join(current_dir, "mapped_user_session_logs.parquet")
    )
    user_session_logs_df = user_session_logs_df[["user_did", "timestamp"]]
    study_users: list[UserToBlueskyProfileModel] = get_all_users()
    study_users_df: pd.DataFrame = pd.DataFrame(
        [user.dict() for user in study_users if user.is_study_user]
    )
    study_users_df = study_users_df[["bluesky_user_did", "bluesky_handle"]]
    bluesky_user_did_to_handle = study_users_df.set_index("bluesky_user_did")[
        "bluesky_handle"
    ].to_dict()

    # fix the handle and convert the timestamp
    qualtrics_df["utc_timestamp"] = qualtrics_df["StartDate"].apply(
        convert_qualtrics_timestamp_to_std_format
    )
    qualtrics_df["handle"] = qualtrics_df["handle"].apply(fix_misspelled_handle)

    # remove excluded handles
    qualtrics_df = qualtrics_df[~qualtrics_df["handle"].isin(EXCLUDELIST_HANDLES)]

    # filter out duplicate wave users
    qualtrics_df = qualtrics_df[
        ~(
            (qualtrics_df["handle"].isin(DUPLICATE_WAVE_USERS.keys()))
            & (qualtrics_df["wave"] == qualtrics_df["handle"].map(DUPLICATE_WAVE_USERS))
        )
    ]

    # map handle to wave
    handle_to_wave_df = qualtrics_df[["handle", "wave"]].drop_duplicates()
    duplicate_handles = handle_to_wave_df[
        handle_to_wave_df["handle"].duplicated(keep=False)
    ]
    assert duplicate_handles.empty, "Duplicate handles appearing in multiple waves."

    # map handle to wave
    map_handle_to_wave = handle_to_wave_df.set_index("handle")["wave"].to_dict()

    # Join qualtrics data with study users data
    merged_df = pd.merge(
        qualtrics_df,
        study_users_df,
        left_on="handle",
        right_on="bluesky_handle",
        how="left",
    )

    # make sure we don't miss any records.
    assert len(merged_df) == len(qualtrics_df)

    # map handle to wave
    merged_df["wave"] = merged_df["handle"].map(map_handle_to_wave)
    merged_df = merged_df[
        ["bluesky_handle", "bluesky_user_did", "utc_timestamp", "wave"]
    ]

    # map user session logs to handles and waves
    user_session_logs_df["handle"] = user_session_logs_df["user_did"].map(
        bluesky_user_did_to_handle
    )
    user_session_logs_df["wave"] = user_session_logs_df["handle"].map(
        map_handle_to_wave
    )

    # map both the qualtrics and user session logs to the week that the survey was sent.
    qualtrics_df["survey_week"] = qualtrics_df.apply(
        lambda row: map_timestamp_to_survey_week(row["utc_timestamp"], row["wave"]),
        axis=1,
    )
    user_session_logs_df["survey_week"] = user_session_logs_df.apply(
        lambda row: map_timestamp_to_survey_week(row["timestamp"], row["wave"]), axis=1
    )

    # For each handle/survey_week combination, keep only the latest response
    qualtrics_df = (
        qualtrics_df.sort_values("utc_timestamp")
        .groupby(["handle", "survey_week"], as_index=False)
        .last()
    )

    # remove rows where the survey week is 9, since there's only 8 weeks worth
    # of data (I manually checked these and they filled out the survey after
    # when they were supposed to be done with the study).
    qualtrics_df = qualtrics_df[qualtrics_df["survey_week"] != 9]

    # remove duplicate responses within a timeframe.
    qualtrics_df = remove_duplicate_responses_within_timeframe(
        df=qualtrics_df, threshold_hours=24
    )

    # get the valid weeks for each user.
    valid_weeks_df = get_user_valid_weeks(qualtrics_df, user_session_logs_df)

    # export the valid weeks df to a csv.
    valid_weeks_df.to_csv(
        os.path.join(current_dir, "valid_weeks_per_bluesky_user.csv"), index=False
    )

    print("Finished fetching valid weeks for each user.")


if __name__ == "__main__":
    main()

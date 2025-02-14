"""Generates the 'condition_aggregated.csv' file."""

from datetime import date
import gc
import os
from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
)
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.calculate_analytics.study_analytics.deprecated.get_fine_grained_weekly_usage_reports import (
    fix_misspelled_handle,
    DUPLICATE_WAVE_USERS,
    EXCLUDELIST_HANDLES,
)
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

start_date_inclusive = wave_1_study_start_date_inclusive
end_date_inclusive = wave_2_study_end_date_inclusive  # 2024-12-01 (inclusive)
exclude_partition_dates = ["2024-10-08"]

logger = get_logger(__file__)

current_filedir = os.path.dirname(os.path.abspath(__file__))


def get_posts_used_in_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Get the posts used in feeds for a given partition date."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="fetch_posts_used_in_feeds",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(
        f"Loaded {len(df)} posts used in feeds for partition date {partition_date}"
    )
    return df


def get_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Get the feeds generated for a particular partition date."""
    feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(f"Loaded {len(feeds_df)} feeds for partition date {partition_date}")
    return feeds_df


def get_perspective_api_labels_for_posts(
    posts: pd.DataFrame, partition_date: str
) -> pd.DataFrame:
    """Get the Perspective API labels for a list of posts."""
    start_date, end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date
    )
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        partition_date=partition_date,
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(
        f"Loaded {len(df)} Perspective API labels for partition date {partition_date}"
    )
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} Perspective API labels for partition date {partition_date}"
    )
    return df


def get_ime_labels_for_posts(posts: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    """Get the IME labels for a list of posts."""
    start_date, end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date
    )
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        directory="cache",
        partition_date=partition_date,
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(f"Filtered to {len(df)} IME labels for partition date {partition_date}")
    return df


# def get_sociopolitical_labels_for_posts(
#     posts: pd.DataFrame, partition_date: str
# ) -> pd.DataFrame:
#     """Get the sociopolitical labels for a list of posts."""
#     start_date, end_date = calculate_start_end_date_for_lookback(
#         partition_date=partition_date
#     )
#     df: pd.DataFrame = load_data_from_local_storage(
#         service="ml_inference_sociopolitical",
#         directory="cache",
#         partition_date=partition_date,
#         start_partition_date=start_date,
#         end_partition_date=end_date,
#     )
#     df = df[df["uri"].isin(posts["uri"])]
#     logger.info(
#         f"Filtered to {len(df)} sociopolitical labels for partition date {partition_date}"
#     )
#     return df


def map_users_to_posts_used_in_feeds(
    partition_date: str,
) -> dict[str, set[str]]:
    """Map users to the posts used in their feeds for a given partition
    date.
    """
    feeds_df: pd.DataFrame = get_feeds_for_partition_date(partition_date)
    users_to_posts: dict[str, set[str]] = {}
    for _, row in feeds_df.iterrows():
        user = row["user"]
        feed: list[dict] = load_feed_from_json_str(row["feed"])
        post_uris: list[str] = [post["item"] for post in feed]
        if user not in users_to_posts:
            users_to_posts[user] = set()
        users_to_posts[user].update(post_uris)
    return users_to_posts


def get_hydrated_posts_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Hydrate each post and create a wide table of post features."""
    posts_df: pd.DataFrame = get_posts_used_in_feeds_for_partition_date(partition_date)
    perspective_api_labels_df: pd.DataFrame = get_perspective_api_labels_for_posts(
        posts=posts_df, partition_date=partition_date
    )
    ime_labels_df: pd.DataFrame = get_ime_labels_for_posts(
        posts=posts_df, partition_date=partition_date
    )

    # NOTE: won't have complete data yet for it.
    # sociopolitical_labels_df: pd.DataFrame = get_sociopolitical_labels_for_posts(
    #     posts=posts_df, partition_date=partition_date
    # )

    # deduping
    posts_df = posts_df.drop_duplicates(subset=["uri"])
    perspective_api_labels_df = perspective_api_labels_df.drop_duplicates(
        subset=["uri"]
    )
    ime_labels_df = ime_labels_df.drop_duplicates(subset=["uri"])
    # sociopolitical_labels_df = sociopolitical_labels_df.drop_duplicates(subset=["uri"])

    # Left join each set of labels against the posts dataframe
    # This ensures we keep all posts even if they don't have certain labels
    posts_with_perspective = posts_df.merge(
        perspective_api_labels_df, on="uri", how="left"
    )

    posts_with_ime = posts_with_perspective.merge(ime_labels_df, on="uri", how="left")

    # posts_with_all_labels = posts_with_ime.merge(
    #     sociopolitical_labels_df, on="uri", how="left"
    # )

    posts_with_all_labels = posts_with_ime

    del posts_df
    del posts_with_perspective
    del posts_with_ime
    del perspective_api_labels_df
    del ime_labels_df
    gc.collect()

    logger.info(
        f"Created wide table with {len(posts_with_all_labels)} rows for partition date {partition_date}"
    )

    return posts_with_all_labels


def get_hydrated_feed_posts_per_user(partition_date: str) -> dict[str, pd.DataFrame]:
    """Get the hydrated posts for a given partition date and map them to
    the users who posted them.
    """
    posts_df: pd.DataFrame = get_hydrated_posts_for_partition_date(partition_date)
    users_to_posts: dict[str, set[str]] = map_users_to_posts_used_in_feeds(
        partition_date=partition_date
    )
    map_user_to_subset_df: dict[str, pd.DataFrame] = {}
    for user, posts in users_to_posts.items():
        subset_df = posts_df[posts_df["uri"].isin(posts)]
        logger.info(
            f"Hydrated {len(subset_df)} posts for user {user} for partition date {partition_date}"
        )
        map_user_to_subset_df[user] = subset_df
    return map_user_to_subset_df


def get_per_user_feed_averages_for_partition_date(partition_date: str) -> pd.DataFrame:
    """For each user, calculates the average feed content for a given partition
    date.

    For example, what was the average % of toxicity of the posts that appeared
    in the user's feed on the given date? How about the average % of political
    posts? The average % of IME labels?
    """
    map_user_to_posts_df: dict[str, pd.DataFrame] = get_hydrated_feed_posts_per_user(
        partition_date
    )

    # Create list to store per-user averages
    user_averages = []

    for user, posts_df in map_user_to_posts_df.items():
        logger.info(
            f"Calculating per-user averages for user {user} for partition date {partition_date}"
        )

        # Calculate averages for each feature
        averages = {
            "user": user,
            "avg_prob_toxic": posts_df["prob_toxic"].dropna().mean(),
            "avg_prob_severe_toxic": posts_df["prob_severe_toxic"].dropna().mean(),
            "avg_prob_identity_attack": posts_df["prob_identity_attack"]
            .dropna()
            .mean(),
            "avg_prob_insult": posts_df["prob_insult"].dropna().mean(),
            "avg_prob_profanity": posts_df["prob_profanity"].dropna().mean(),
            "avg_prob_threat": posts_df["prob_threat"].dropna().mean(),
            "avg_prob_affinity": posts_df["prob_affinity"].dropna().mean(),
            "avg_prob_compassion": posts_df["prob_compassion"].dropna().mean(),
            "avg_prob_constructive": posts_df["prob_constructive"].dropna().mean(),
            "avg_prob_curiosity": posts_df["prob_curiosity"].dropna().mean(),
            "avg_prob_nuance": posts_df["prob_nuance"].dropna().mean(),
            "avg_prob_personal_story": posts_df["prob_personal_story"].dropna().mean(),
            "avg_prob_reasoning": posts_df["prob_reasoning"].dropna().mean(),
            "avg_prob_respect": posts_df["prob_respect"].dropna().mean(),
            "avg_prob_alienation": posts_df["prob_alienation"].dropna().mean(),
            "avg_prob_fearmongering": posts_df["prob_fearmongering"].dropna().mean(),
            "avg_prob_generalization": posts_df["prob_generalization"].dropna().mean(),
            "avg_prob_moral_outrage": posts_df["prob_moral_outrage"].dropna().mean(),
            "avg_prob_scapegoating": posts_df["prob_scapegoating"].dropna().mean(),
            "avg_prob_sexually_explicit": posts_df["prob_sexually_explicit"]
            .dropna()
            .mean(),
            "avg_prob_flirtation": posts_df["prob_flirtation"].dropna().mean(),
            "avg_prob_spam": posts_df["prob_spam"].dropna().mean(),
            "avg_prob_emotion": posts_df["prob_emotion"].dropna().mean(),
            "avg_prob_intergroup": posts_df["prob_intergroup"].dropna().mean(),
            "avg_prob_moral": posts_df["prob_moral"].dropna().mean(),
            "avg_prob_other": posts_df["prob_other"].dropna().mean(),
        }

        # Calculate political averages
        # total_rows = len(posts_df)
        # avg_is_political = posts_df["is_sociopolitical"].dropna().mean()

        # political_averages = {
        #     "avg_is_political": avg_is_political,
        #     "avg_is_not_political": 1 - avg_is_political,
        #     "avg_is_political_left": (
        #         posts_df["political_ideology_label"].fillna("").eq("left")
        #     ).sum()
        #     / total_rows,
        #     "avg_is_political_right": (
        #         posts_df["political_ideology_label"].fillna("").eq("right")
        #     ).sum()
        #     / total_rows,
        #     "avg_is_political_moderate": (
        #         posts_df["political_ideology_label"].fillna("").eq("moderate")
        #     ).sum()
        #     / total_rows,
        #     "avg_is_political_unclear": (
        #         posts_df["political_ideology_label"].fillna("").eq("unclear")
        #     ).sum()
        #     / total_rows,
        # }

        # # Combine all averages
        # averages.update(political_averages)

        user_averages.append(averages)

    # Convert to dataframe
    averages_df = pd.DataFrame(user_averages)
    averages_df = averages_df.set_index("user")

    return averages_df


def get_per_user_feed_averages_for_study() -> pd.DataFrame:
    """Get the per-user feed averages for the study, on a daily basis.

    Returns a dataframe where eaech row is a user + date combination, and the
    values are the average scores for the posts used in feeds from those
    dates.
    """
    dfs: list[pd.DataFrame] = []
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date_inclusive,
        end_date=end_date_inclusive,
        exclude_partition_dates=exclude_partition_dates,
    )
    for partition_date in partition_dates:
        logger.info(f"Getting per-user averages for partition date: {partition_date}")
        df = get_hydrated_posts_for_partition_date(partition_date)
        df["date"] = partition_date
        dfs.append(df)
    logger.info("Concatenating all dataframes...")
    concat_df = pd.concat(dfs)
    # Sort by user and partition date in ascending order
    concat_df = concat_df.sort_values(
        ["user", "partition_date"], ascending=[True, True]
    )
    logger.info(f"Exporting a concated dataframe with {len(concat_df)} rows")
    return concat_df


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


def main():
    logger.info("Starting main function...")

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
    # load user demographics from DynamoDB
    user_demographics: pd.DataFrame = load_user_demographic_info()
    logger.info(f"Loaded user demographics with {len(user_demographics)} rows")

    # consolidate handles and use only the valid ones (people who filled out
    # the Qualtrics survey and weren't filtered out).
    user_demographics_bsky_handles_set = set(user_demographics["bluesky_handle"])
    qualtrics_logs_bsky_handles_set = set(qualtrics_logs["handle"])
    valid_weeks_per_bluesky_user_bsky_handles_set = set(
        valid_weeks_per_bluesky_user["handle"]
    )
    print(len(qualtrics_logs_bsky_handles_set))
    print(len(user_demographics_bsky_handles_set))
    print(len(valid_weeks_per_bluesky_user_bsky_handles_set))

    valid_user_handles = valid_weeks_per_bluesky_user_bsky_handles_set

    user_demographics = user_demographics[
        user_demographics["bluesky_handle"].isin(valid_user_handles)
    ]
    qualtrics_logs = qualtrics_logs[qualtrics_logs["handle"].isin(valid_user_handles)]
    valid_weeks_per_bluesky_user = valid_weeks_per_bluesky_user[
        valid_weeks_per_bluesky_user["handle"].isin(valid_user_handles)
    ]

    assert (
        set(user_demographics["bluesky_handle"])
        == set(qualtrics_logs["handle"])
        == set(valid_weeks_per_bluesky_user["handle"])
    )

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

    breakpoint()

    # join against user averages.
    per_user_averages: pd.DataFrame = get_per_user_feed_averages_for_study()
    joined_df: pd.DataFrame = per_user_averages.merge(
        user_demographics, left_on="user_did", right_on="bluesky_user_did", how="left"
    )
    assert len(joined_df) == len(
        per_user_averages
    ), f"Expected {len(per_user_averages)} rows after join but got {len(joined_df)}"

    # Join with week thresholds on bluesky_handle and date
    joined_df = joined_df.merge(
        week_thresholds_per_user_static, on=["bluesky_handle", "date"], how="left"
    )
    joined_df = joined_df.merge(
        week_thresholds_per_user_dynamic, on=["bluesky_handle", "date"], how="left"
    )
    assert len(joined_df) == len(
        per_user_averages
    ), f"Expected {len(per_user_averages)} rows after join but got {len(joined_df)}"
    joined_df.to_csv(os.path.join(current_filedir, "condition_aggregated.csv"))


if __name__ == "__main__":
    main()

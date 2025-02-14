"""Generates the 'condition_aggregated.csv' file."""

import gc
import os

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
    wave_1_study_end_date_inclusive,
    wave_2_start_date_inclusive,
    wave_1_week_end_dates_inclusive,
    wave_2_week_end_dates_inclusive,
)

start_date_inclusive = "2024-09-30"
end_date_inclusive = (
    "2024-11-30"  # 2024-12-01 should be the upper bound (i.e., not included)
)
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
    all_combinations = []
    for _, row in user_handle_to_wave_df.iterrows():
        bluesky_handle = row["bluesky_handle"]
        wave = row["wave"]
        for partition_date in partition_dates:
            # account for different waves, where wave 1 ends 11/25 and wave 2
            # ends 12/1.
            if wave == 1 and partition_date > wave_1_study_end_date_inclusive:
                all_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": None,
                    }
                )
            elif wave == 2 and partition_date < wave_2_start_date_inclusive:
                all_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": None,
                    }
                )
            else:
                all_combinations.append(
                    {
                        "bluesky_handle": bluesky_handle,
                        "wave": wave,
                        "date": partition_date,
                        "week_static": map_date_to_static_week(partition_date, wave),
                    }
                )

    # Convert to DataFrame
    df = pd.DataFrame(all_combinations)

    # Sort by handle and date
    df = df.sort_values(["bluesky_handle", "date"])

    logger.info(f"Created week thresholds DataFrame with {len(df)} rows")

    return df


def get_week_thresholds_per_user_dynamic(
    valid_weeks_per_bluesky_user: pd.DataFrame,
    user_handle_to_wave_df: pd.DataFrame,
    week_thresholds_per_user_static: pd.DataFrame,
) -> pd.DataFrame:
    """Get a mapping of user handles to the wave they were in.

    Returns:
        pd.DataFrame: DataFrame with columns 'bluesky_handle' and 'wave'

    Loads in the "valid_weeks_per_bluesky_user.csv" file and:
    - If the user filled out the survey AND they have a survey timestamp,
    we use that as the end cutoff for that week (this is how we did it
    before, where we counted a user's survey as valid only IF they went
    into the app before they filled out the survey. Therefore, the survey
    is the end date of that week, with regards to checking for user activity).
    - Else, we use the static threshold defined in
    `get_week_thresholds_per_user_static`.
    """
    user_survey_week_to_end_of_week_date: dict[tuple[str, int], str] = {}

    week_thresholds_per_user_static_dict: dict[tuple[str, int], str] = {
        (row["bluesky_handle"], row["week_static"]): row["date"]
        for _, row in week_thresholds_per_user_static.iterrows()
    }

    # determine threshold by either looking at the survey timestamp or the
    # static threshold

    # LOGIC: If they fill out the survey within the 7 day period from
    # Monday -> Sunday, then we use that date as the end of the week, and count
    # all activity starting from Monday up to that date. Otherwise, we use
    # the date of the survey as the end date (inclusive) of that week.

    # NOTE: we make all end dates inclusive.

    # Example:
    # For static weeks, we do Monday -> Sunday.
    # The dates for the Mondays are 09/30, 10/07, 10/14, 10/21, 10/28,
    # 11/04, 11/11, 11/18, 11/25, 12/02.

    # This means that for static weeks, for Wave 1, the weeks are:
    # Week 1: 09/30 - 10/06
    # Week 2: 10/07 - 10/13
    # Week 3: 10/14 - 10/20
    # Week 4: 10/21 - 10/27
    # Week 5: 10/28 - 11/03
    # Week 6: 11/04 - 11/10
    # Week 7: 11/11 - 11/17
    # Week 8: 11/18 - 11/24
    # For Wave 2, the weeks are:
    # Week 1: 10/07 - 10/13
    # Week 2: 10/14 - 10/20
    # Week 3: 10/21 - 10/27
    # Week 4: 10/28 - 11/03
    # Week 5: 11/04 - 11/10
    # Week 6: 11/11 - 11/17
    # Week 7: 11/18 - 11/24
    # Week 8: 11/25 - 12/01

    # For dynamic weeks, we do Monday -> Sunday.
    # The dates for the Mondays are 09/30, 10/07, 10/14, 10/21, 10/28,
    # 11/04, 11/11, 11/18, 11/25, 12/02.

    #
    # Then we move to
    # the next Monday -> Sunday period and check if they filled out they

    # Example:

    for _, row in valid_weeks_per_bluesky_user.iterrows():
        handle = row["handle"]
        survey_week = row["survey_week"]
        filled_out_survey = row["filled_out_survey"]
        survey_timestamp_date = row["survey_timestamp_date"]

        if filled_out_survey and survey_timestamp_date:
            user_survey_week_to_end_of_week_date[(handle, survey_week)] = (
                survey_timestamp_date
            )
        else:
            user_survey_week_to_end_of_week_date[(handle, survey_week)] = (
                week_thresholds_per_user_static_dict[(handle, survey_week)]
            )

    # Convert user_survey_week_to_end_of_week_date to user_to_end_of_week_date
    user_to_end_of_week_date: dict[str, dict[int, str]] = {}
    for (handle, week), date in user_survey_week_to_end_of_week_date.items():
        if handle not in user_to_end_of_week_date:
            user_to_end_of_week_date[handle] = {}
        user_to_end_of_week_date[handle][week] = date

    partition_dates: list[str] = get_partition_dates(
        start_date=start_date_inclusive,
        end_date=end_date_inclusive,
        exclude_partition_dates=[],
    )

    user_handle_to_wave: dict[str, int] = {
        row["bluesky_handle"]: row["wave"]
        for _, row in user_handle_to_wave_df.iterrows()
    }

    user_dates_to_dynamic_week_thresholds: list[dict] = []

    # iterate through each user and their end of week dates
    for handle, end_of_week_date_dict in user_to_end_of_week_date.items():
        # Convert any datetime.date objects to string format
        end_of_week_dates: list[str] = [
            date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else date
            for date in end_of_week_date_dict.values()
        ]
        end_of_week_dates: list[str] = sorted(end_of_week_dates)
        if len(end_of_week_dates) != 8:
            print(f"User {handle} has {len(end_of_week_dates)} end of week dates")

        updates: list[dict] = []

        wave = user_handle_to_wave[handle]

        # iterate through each partition date
        for partition_date in partition_dates:
            week = 0

            # account for different waves, where wave 1 ends 11/25 and wave 2
            # ends 12/1.
            if wave == 1 and partition_date > "2024-11-24":
                updates.append(
                    {
                        "bluesky_handle": handle,
                        "date": partition_date,
                        "week_dynamic": None,
                    }
                )
            elif wave == 2 and partition_date < "2024-10-07":
                updates.append(
                    {
                        "bluesky_handle": handle,
                        "date": partition_date,
                        "week_dynamic": None,
                    }
                )
            else:
                # iterate through each week and its end of week date
                for end_of_week_date in end_of_week_dates:
                    # if the partition date is less than or equal to the end of
                    # week date, then add the user, partition date, and week to
                    # the list
                    week += 1
                    # NOTE: is equality correct here? Perhaps not? We want
                    # the weeks to go Monday -> Sunday, so the date
                    # of the subsequent Monday should be the cap?
                    if partition_date <= end_of_week_date:
                        updates.append(
                            {
                                "bluesky_handle": handle,
                                "date": partition_date,
                                "week_dynamic": week,
                            }
                        )
                        break

        if len(updates) != len(partition_dates):
            print("SOMETHING WRONG IN THE UPDATES.")
            breakpoint()

        user_dates_to_dynamic_week_thresholds.extend(updates)

    breakpoint()

    df: pd.DataFrame = pd.DataFrame(user_dates_to_dynamic_week_thresholds)

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
    qualtrics_logs = qualtrics_logs[["handle", "wave"]]

    valid_weeks_per_bluesky_user: pd.DataFrame = pd.read_csv(
        os.path.join(current_filedir, "valid_weeks_per_bluesky_user.csv")
    )
    valid_weeks_per_bluesky_user = filter_process_users_from_qualtrics_data(
        valid_weeks_per_bluesky_user,
        filter_duplicate_wave_users=False,
    )
    valid_weeks_per_bluesky_user = valid_weeks_per_bluesky_user[
        ["handle", "survey_week", "filled_out_survey", "survey_timestamp_utc"]
    ]
    valid_weeks_per_bluesky_user["survey_timestamp_date"] = pd.to_datetime(
        valid_weeks_per_bluesky_user["survey_timestamp_utc"],
        format="%Y-%m-%d-%H:%M:%S",
        errors="coerce",
    ).dt.date

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
            valid_weeks_per_bluesky_user=valid_weeks_per_bluesky_user,
            user_handle_to_wave_df=user_handle_to_wave_df,
            week_thresholds_per_user_static=week_thresholds_per_user_static,
        )
    )
    logger.info(
        f"Generated week thresholds per user dynamic with {len(week_thresholds_per_user_dynamic)} rows"
    )
    # TODO: join stuff here before the per-user averages.
    breakpoint()
    week_thresholds: pd.DataFrame = week_thresholds_per_user_static.merge(
        week_thresholds_per_user_dynamic, on=["bluesky_handle", "date"], how="left"
    )
    print(week_thresholds.head())

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

"""Calculates the average feed content for each user."""

import pandas as pd

from lib.log.logger import get_logger
from lib.helper import get_partition_dates
from services.calculate_analytics.study_analytics.generate_reports.constants import (
    wave_1_study_start_date_inclusive,
    wave_2_study_end_date_inclusive,
)
from services.calculate_analytics.study_analytics.load_data.load_data import (
    get_hydrated_feed_posts_per_user,
)

start_date_inclusive = wave_1_study_start_date_inclusive
end_date_inclusive = wave_2_study_end_date_inclusive  # 2024-12-01 (inclusive)
exclude_partition_dates = ["2024-10-08"]

logger = get_logger(__file__)


def get_per_user_feed_averages_for_partition_date(
    partition_date: str, load_unfiltered_posts: bool = True
) -> pd.DataFrame:
    """For each user, calculates the average feed content for a given partition
    date.

    For example, what was the average % of toxicity of the posts that appeared
    in the user's feed on the given date? How about the average % of political
    posts? The average % of IME labels?
    """
    map_user_to_posts_df: dict[str, pd.DataFrame] = get_hydrated_feed_posts_per_user(
        partition_date=partition_date, load_unfiltered_posts=load_unfiltered_posts
    )

    # Create list to store per-user averages
    user_averages = []

    logger.info(f"Calculating per-user averages for partition date {partition_date}")

    for user, posts_df in map_user_to_posts_df.items():
        # Calculate averages for each feature
        averages = {
            "user": user,
            "user_did": user,
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
        total_rows = len(posts_df)
        avg_is_political = posts_df["is_sociopolitical"].dropna().mean()

        political_averages = {
            "avg_is_political": avg_is_political,
            "avg_is_not_political": 1 - avg_is_political,
            "avg_is_political_left": (
                posts_df["political_ideology_label"].fillna("").eq("left")
            ).sum()
            / total_rows,
            "avg_is_political_right": (
                posts_df["political_ideology_label"].fillna("").eq("right")
            ).sum()
            / total_rows,
            "avg_is_political_moderate": (
                posts_df["political_ideology_label"].fillna("").eq("moderate")
            ).sum()
            / total_rows,
            "avg_is_political_unclear": (
                posts_df["political_ideology_label"].fillna("").eq("unclear")
            ).sum()
            / total_rows,
        }

        # Combine all averages
        averages.update(political_averages)

        user_averages.append(averages)

    # Convert to dataframe
    averages_df = pd.DataFrame(user_averages)
    averages_df = averages_df.set_index("user")

    logger.info(
        f"Finished calculating per-user averages for partition date {partition_date}"
    )

    return averages_df


def get_per_user_feed_averages_for_study(
    load_unfiltered_posts: bool = True,
) -> pd.DataFrame:
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
        df = get_per_user_feed_averages_for_partition_date(
            partition_date=partition_date, load_unfiltered_posts=load_unfiltered_posts
        )
        df["date"] = partition_date
        dfs.append(df)
    concat_df = pd.concat(dfs)
    # Sort by user and partition date in ascending order
    concat_df = concat_df.sort_values(["user", "date"], ascending=[True, True])
    logger.info(
        f"Exporting a dataframe of per-user feed averages, with {len(concat_df)} rows"
    )
    return concat_df

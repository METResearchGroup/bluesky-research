"""Creates a report of the baseline averages for each of the integration scores
per day by source. Loads the posts used in feeds per day and then filters those
for which ones were sourced from the firehose or the most-liked posts.

Then calculates the average of the labels for these posts.
"""

import os

import pandas as pd

from lib.datetime_utils import (
    calculate_start_end_date_for_lookback,
    generate_current_datetime_str,
    get_partition_dates,
)
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.load_data.load_data import (
    get_hydrated_posts_for_partition_date,
    load_posts_used_in_feeds_by_source,
)

current_filedir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(__file__)


def calculate_averages_for_hydrated_df(
    partition_date: str, df: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the averages for the hydrated df."""

    total_rows = len(df)

    # get averages for each label.
    ime_averages = {
        "avg_prob_intergroup": df["prob_intergroup"].dropna().mean(),
        "avg_prob_moral": df["prob_moral"].dropna().mean(),
        "avg_prob_emotion": df["prob_emotion"].dropna().mean(),
        "avg_prob_other": df["prob_other"].dropna().mean(),
    }

    avg_is_political = df["is_sociopolitical"].dropna().mean()
    sociopolitical_averages = {
        "avg_is_political": avg_is_political,
        "avg_is_not_political": 1 - avg_is_political,
        "avg_is_political_left": (
            df["political_ideology_label"].fillna("").eq("left")
        ).sum()
        / total_rows,
        "avg_is_political_right": (
            df["political_ideology_label"].fillna("").eq("right")
        ).sum()
        / total_rows,
        "avg_is_political_moderate": (
            df["political_ideology_label"].fillna("").eq("moderate")
        ).sum()
        / total_rows,
        "avg_is_political_unclear": (
            df["political_ideology_label"].fillna("").eq("unclear")
        ).sum()
        / total_rows,
    }

    perspective_api_averages = {
        "avg_prob_toxic": df["prob_toxic"].dropna().mean(),
        "avg_prob_severe_toxic": df["prob_severe_toxic"].dropna().mean(),
        "avg_prob_identity_attack": df["prob_identity_attack"].dropna().mean(),
        "avg_prob_insult": df["prob_insult"].dropna().mean(),
        "avg_prob_profanity": df["prob_profanity"].dropna().mean(),
        "avg_prob_threat": df["prob_threat"].dropna().mean(),
        "avg_prob_affinity": df["prob_affinity"].dropna().mean(),
        "avg_prob_compassion": df["prob_compassion"].dropna().mean(),
        "avg_prob_constructive": df["prob_constructive"].dropna().mean(),
        "avg_prob_curiosity": df["prob_curiosity"].dropna().mean(),
        "avg_prob_nuance": df["prob_nuance"].dropna().mean(),
        "avg_prob_personal_story": df["prob_personal_story"].dropna().mean(),
        "avg_prob_reasoning": df["prob_reasoning"].dropna().mean(),
        "avg_prob_respect": df["prob_respect"].dropna().mean(),
        "avg_prob_alienation": df["prob_alienation"].dropna().mean(),
        "avg_prob_fearmongering": df["prob_fearmongering"].dropna().mean(),
        "avg_prob_generalization": df["prob_generalization"].dropna().mean(),
        "avg_prob_moral_outrage": df["prob_moral_outrage"].dropna().mean(),
        "avg_prob_scapegoating": df["prob_scapegoating"].dropna().mean(),
        "avg_prob_sexually_explicit": df["prob_sexually_explicit"].dropna().mean(),
        "avg_prob_flirtation": df["prob_flirtation"].dropna().mean(),
        "avg_prob_spam": df["prob_spam"].dropna().mean(),
    }
    averages = {
        "date": partition_date,
        **ime_averages,
        **sociopolitical_averages,
        **perspective_api_averages,
    }
    del ime_averages
    del sociopolitical_averages
    del perspective_api_averages

    averages_df = pd.DataFrame([averages])
    return averages_df


def get_firehose_baseline_averages_for_partition_date(
    partition_date: str,
) -> pd.DataFrame:
    """Get the average of the labels for the posts that were used in feeds
    and sourced from the firehose.

    Here, we don't care about per-user averages, we want the average of the
    labels for the posts that were used in feeds and sourced from the firehose.
    """
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=5,
        min_lookback_date="2024-09-28",
    )
    posts_df: pd.DataFrame = load_posts_used_in_feeds_by_source(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        source="firehose",
    )
    df: pd.DataFrame = get_hydrated_posts_for_partition_date(
        partition_date=partition_date, posts_df=posts_df
    )
    averages_df = calculate_averages_for_hydrated_df(
        partition_date=partition_date, df=df
    )
    return averages_df


def get_most_liked_baseline_averages_for_partition_date(
    partition_date: str,
) -> pd.DataFrame:
    """Get the average of the labels for the posts that were used in feeds
    and sourced from the most-liked posts.

    Here, we don't care about per-user averages, we want the average of the
    labels for the posts that were used in feeds and sourced from the most-liked
    posts.
    """
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=5,
        min_lookback_date="2024-09-28",
    )
    posts_df: pd.DataFrame = load_posts_used_in_feeds_by_source(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        source="most_liked",
    )

    df: pd.DataFrame = get_hydrated_posts_for_partition_date(
        partition_date=partition_date, posts_df=posts_df
    )
    averages_df = calculate_averages_for_hydrated_df(
        partition_date=partition_date, df=df
    )
    return averages_df


def calculate_firehose_averages_for_study(partition_dates: list[str]) -> pd.DataFrame:
    """Calculate the firehose averages for the study."""
    dfs: list[pd.DataFrame] = []
    for partition_date in partition_dates:
        df = get_firehose_baseline_averages_for_partition_date(
            partition_date=partition_date
        )
        dfs.append(df)
    concat_df = pd.concat(dfs)
    return concat_df


def calculate_most_liked_averages_for_study(partition_dates: list[str]) -> pd.DataFrame:
    """Calculate the most-liked averages for the study."""
    dfs: list[pd.DataFrame] = []
    for partition_date in partition_dates:
        df = get_most_liked_baseline_averages_for_partition_date(
            partition_date=partition_date
        )
        dfs.append(df)
    concat_df = pd.concat(dfs)
    return concat_df


def calculate_baseline_averages_for_study():
    """Calculate the baseline averages for the study and export as a .csv."""
    start_date = "2024-09-28"
    end_date = "2024-12-01"
    exclude_partition_dates = ["2024-10-08"]
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    firehose_df = calculate_firehose_averages_for_study(partition_dates=partition_dates)
    firehose_df["source"] = "firehose"
    most_liked_df = calculate_most_liked_averages_for_study(
        partition_dates=partition_dates
    )
    most_liked_df["source"] = "most_liked"
    concat_df = pd.concat([firehose_df, most_liked_df])
    concat_df.to_csv(
        os.path.join(
            current_filedir,
            f"baseline_averages_per_day_{generate_current_datetime_str()}.csv",
        )
    )
    return concat_df


if __name__ == "__main__":
    calculate_baseline_averages_for_study()

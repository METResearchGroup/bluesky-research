"""Creates a report of the proportion of each of the labels
per day across the firehose.

Does rolling average across 5 days since our baseline pool of posts to use
for a given day's posts is the most recent 5 days.
"""

import os

import pandas as pd

from lib.datetime_utils import generate_current_datetime_str
from lib.db.manage_local_data import load_data_from_local_storage
from lib.datetime_utils import calculate_start_end_date_for_lookback, get_partition_dates


current_filedir = os.path.dirname(os.path.abspath(__file__))


def load_average_perspective_api_labels_for_partition_date(partition_date: str) -> dict:
    """Get the proportion of posts with Perspective API labels above 0.5 for a given partition date."""
    start, end = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=5,
        min_lookback_date="2024-09-28",
    )
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        start_partition_date=start,
        end_partition_date=end,
    )
    proportions = {
        "prop_toxic": (df["prob_toxic"] > 0.5).mean(),
        "prop_severe_toxic": (df["prob_severe_toxic"] > 0.5).mean(),
        "prop_identity_attack": (df["prob_identity_attack"] > 0.5).mean(),
        "prop_insult": (df["prob_insult"] > 0.5).mean(),
        "prop_profanity": (df["prob_profanity"] > 0.5).mean(),
        "prop_threat": (df["prob_threat"] > 0.5).mean(),
        "prop_affinity": (df["prob_affinity"] > 0.5).mean(),
        "prop_compassion": (df["prob_compassion"] > 0.5).mean(),
        "prop_constructive": (df["prob_constructive"] > 0.5).mean(),
        "prop_curiosity": (df["prob_curiosity"] > 0.5).mean(),
        "prop_nuance": (df["prob_nuance"] > 0.5).mean(),
        "prop_personal_story": (df["prob_personal_story"] > 0.5).mean(),
        "prop_reasoning": (df["prob_reasoning"] > 0.5).mean(),
        "prop_respect": (df["prob_respect"] > 0.5).mean(),
        "prop_alienation": (df["prob_alienation"] > 0.5).mean(),
        "prop_fearmongering": (df["prob_fearmongering"] > 0.5).mean(),
        "prop_generalization": (df["prob_generalization"] > 0.5).mean(),
        "prop_moral_outrage": (df["prob_moral_outrage"] > 0.5).mean(),
        "prop_scapegoating": (df["prob_scapegoating"] > 0.5).mean(),
        "prop_sexually_explicit": (df["prob_sexually_explicit"] > 0.5).mean(),
        "prop_flirtation": (df["prob_flirtation"] > 0.5).mean(),
        "prop_spam": (df["prob_spam"] > 0.5).mean(),
    }
    del df
    return proportions


def load_average_ime_labels_for_partition_date(partition_date: str) -> dict:
    """Get the proportion of posts with IME labels above 0.5 for a given partition date."""
    start, end = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=5,
        min_lookback_date="2024-09-28",
    )
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        directory="cache",
        start_partition_date=start,
        end_partition_date=end,
    )
    proportions = {
        "prop_intergroup": (df["prob_intergroup"] > 0.5).mean(),
        "prop_moral": (df["prob_moral"] > 0.5).mean(),
        "prop_emotion": (df["prob_emotion"] > 0.5).mean(),
        "prop_other": (df["prob_other"] > 0.5).mean(),
    }
    del df
    return proportions


def load_average_sociopolitical_labels_for_partition_date(partition_date: str) -> dict:
    """Get the proportion of posts with sociopolitical labels above 0.5 for a given partition date."""
    start, end = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=5,
        min_lookback_date="2024-09-28",
    )
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_sociopolitical",
        directory="cache",
        start_partition_date=start,
        end_partition_date=end,
    )
    total_rows = len(df)
    prop_is_political = (df["is_sociopolitical"] > 0.5).mean()

    proportions = {
        "prop_is_political": prop_is_political,
        "prop_is_not_political": 1 - prop_is_political,
        "prop_is_political_left": (
            df["political_ideology_label"].fillna("").eq("left")
        ).sum()
        / total_rows,
        "prop_is_political_right": (
            df["political_ideology_label"].fillna("").eq("right")
        ).sum()
        / total_rows,
        "prop_is_political_moderate": (
            df["political_ideology_label"].fillna("").eq("moderate")
        ).sum()
        / total_rows,
        "prop_is_political_unclear": (
            df["political_ideology_label"].fillna("").eq("unclear")
        ).sum()
        / total_rows,
    }
    del df
    return proportions


def get_baseline_averages_for_partition_date(partition_date: str) -> dict:
    """Get the proportion of posts with labels above 0.5 for a given partition date."""
    ime_proportions = load_average_ime_labels_for_partition_date(partition_date)
    sociopolitical_proportions = load_average_sociopolitical_labels_for_partition_date(
        partition_date
    )
    perspective_api_proportions = (
        load_average_perspective_api_labels_for_partition_date(partition_date)
    )
    return {
        "date": partition_date,
        **ime_proportions,
        **sociopolitical_proportions,
        **perspective_api_proportions,
    }


def get_baseline_averages_for_partition_dates(
    start_date: str,
    end_date: str,
    exclude_partition_dates: list[str],
) -> pd.DataFrame:
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    baseline_proportions: list[dict] = []
    for partition_date in partition_dates:
        baseline_prop_for_date: dict = get_baseline_averages_for_partition_date(
            partition_date
        )
        baseline_proportions.append(baseline_prop_for_date)

    baseline_proportions_df = pd.DataFrame(baseline_proportions)
    baseline_proportions_df = baseline_proportions_df.sort_values(
        "date", ascending=True
    )

    timestamp = generate_current_datetime_str()
    baseline_proportions_df.to_csv(
        os.path.join(
            current_filedir,
            f"firehose_label_baseline_proportions_per_day_{timestamp}.csv",
        )
    )
    return baseline_proportions_df


def main():
    start_date = "2024-09-30"
    end_date = "2024-12-01"
    exclude_partition_dates = []

    df = get_baseline_averages_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    print(f"Exported label baseline proportions for {len(df)} partition dates")
    print(df.head())


if __name__ == "__main__":
    main()

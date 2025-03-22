"""Creates a report of the baseline averages for each of the integration scores
per day across the firehose.

Does rolling average across 5 days since our baseline pool of posts to use
for a given day's posts is the most recent 5 days.
"""

import os

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import calculate_start_end_date_for_lookback, get_partition_dates


current_filedir = os.path.dirname(os.path.abspath(__file__))


def load_average_perspective_api_labels_for_partition_date(partition_date: str) -> dict:
    """Get the average of the Perspective API labels for a given partition date."""
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
    averages = {
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
    del df
    return averages


def load_average_ime_labels_for_partition_date(partition_date: str) -> dict:
    """Get the average of the IME labels for a given partition date."""
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
    averages = {
        "avg_prob_intergroup": df["prob_intergroup"].dropna().mean(),
        "avg_prob_moral": df["prob_moral"].dropna().mean(),
        "avg_prob_emotion": df["prob_emotion"].dropna().mean(),
        "avg_prob_other": df["prob_other"].dropna().mean(),
    }
    del df
    return averages


def load_average_sociopolitical_labels_for_partition_date(partition_date: str) -> dict:
    """Get the average of the sociopolitical labels for a given partition date."""
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
    avg_is_political = df["is_sociopolitical"].dropna().mean()

    averages = {
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
    del df
    return averages


def get_baseline_averages_for_partition_date(partition_date: str) -> dict:
    """Get the average of the integration scores for a given partition date."""
    ime_averages = load_average_ime_labels_for_partition_date(partition_date)
    sociopolitical_averages = load_average_sociopolitical_labels_for_partition_date(
        partition_date
    )
    perspective_api_averages = load_average_perspective_api_labels_for_partition_date(
        partition_date
    )
    return {
        "date": partition_date,
        **ime_averages,
        **sociopolitical_averages,
        **perspective_api_averages,
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
    baseline_averages: list[dict] = []
    for partition_date in partition_dates:
        baseline_avg_for_date: dict = get_baseline_averages_for_partition_date(
            partition_date
        )
        baseline_averages.append(baseline_avg_for_date)

    baseline_averages_df = pd.DataFrame(baseline_averages)
    baseline_averages_df = baseline_averages_df.sort_values("date", ascending=True)
    baseline_averages_df.to_csv(
        os.path.join(current_filedir, "firehose_baseline_averages_per_day.csv")
    )
    return baseline_averages_df


def main():
    start_date = "2024-09-30"
    end_date = "2024-12-01"
    exclude_partition_dates = []

    df = get_baseline_averages_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    print(f"Exported baseline averages for {len(df)} partition dates")
    print(df.head())


if __name__ == "__main__":
    main()

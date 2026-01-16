"""
Aggregate per-feed constructiveness averages to per-user-per-day averages.

Input (in this directory):
- per_feed_average_constructiveness.csv
  Columns include:
    - bluesky_handle
    - condition
    - feed_generation_timestamp (format: %Y-%m-%d-%H:%M:%S)
    - prop_constructive_labeled (per-feed average over posts)

Output (written in this directory):
- per_user_per_day_average_constructiveness.csv
  Columns:
    - handle  (renamed from bluesky_handle)
    - condition
    - date    (partition date derived from feed_generation_timestamp)
    - feed_proportion_constructive (mean of per-feed averages for that user+day)
"""

from __future__ import annotations
import os

import pandas as pd

from lib.constants import study_start_date, study_end_date
from lib.datetime_utils import get_partition_dates
from lib.helper import get_partition_date_from_timestamp

BASE_DIR = os.path.dirname(__file__)
INPUT_CSV = os.path.join(BASE_DIR, "per_feed_proportion_constructive_posts.csv")
OUTPUT_CSV = os.path.join(
    BASE_DIR,
    "constructiveness_proportions_only_daily_feed_content_aggregated_results_per_user.csv",
)
REQUIRED_COLUMNS = [
    "bluesky_handle",
    "condition",
    "feed_generation_timestamp",
    "prop_constructive_labeled",
    "date",
]


def add_date_column(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["feed_generation_timestamp"].astype(str)
    df["date"] = ts.map(get_partition_date_from_timestamp)
    return df


def get_unique_study_handles_and_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """Get unique handles and conditions from the dataframe."""
    # Note: df has 'handle' column
    # (renamed from 'bluesky_handle' in aggregate_by_user_and_date)
    result: pd.DataFrame = df[["handle", "condition"]].drop_duplicates()  # type: ignore
    return result


def filter_records(df: pd.DataFrame) -> pd.DataFrame:
    """Filter records based on business logic."""

    # filter by date range
    date_range_mask = (df["date"] >= study_start_date) & (df["date"] <= study_end_date)
    df = df[date_range_mask].copy()  # type: ignore

    # filter out placeholder handle
    handle_mask = df["bluesky_handle"] != "default"
    df = df[handle_mask].copy()  # type: ignore

    # grab only relevant columns.
    columns_to_keep = REQUIRED_COLUMNS
    df = df.loc[:, columns_to_keep].copy()

    return df


def aggregate_by_user_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by user handle, condition, and date, then compute the mean of per-feed constructiveness
    for each group. Renames 'bluesky_handle' column to 'handle' for clarity.
    """
    grouped = df.groupby(
        ["bluesky_handle", "condition", "date"],
        as_index=False,
        dropna=False,
    )
    aggregated: pd.DataFrame = grouped.agg(
        feed_proportion_constructive=("prop_constructive_labeled", "mean"),
    )  # type: ignore
    return aggregated


def rename_handle_column(aggregated: pd.DataFrame) -> pd.DataFrame:
    """Rename the 'bluesky_handle' column to 'handle'."""
    result: pd.DataFrame = aggregated.rename(columns={"bluesky_handle": "handle"})  # type: ignore
    return result


def get_study_dates() -> pd.DataFrame:
    """Get all dates in the study window."""
    all_dates = get_partition_dates(
        start_date=study_start_date,
        end_date=study_end_date,
        exclude_partition_dates=[],
    )
    dates_df = pd.DataFrame({"date": all_dates})
    return dates_df


def create_user_condition_date_combinations(
    handle_condition_df: pd.DataFrame,
    dates_df: pd.DataFrame,
) -> pd.DataFrame:
    """Gets all combinations of user handles and conditions with all dates in
    the study window.

    Does so via cross-joining the handle-condition pairs with the dates.

    Creates a cartesian product ensuring every date exists for every
    (handle, condition) combination.

    Args:
        handle_condition_df: DataFrame with 'handle' and 'condition' columns
        dates_df: DataFrame with 'date' column

    Returns:
        DataFrame with all combinations of (handle, condition, date)
    """
    handle_condition_df = handle_condition_df.copy()
    dates_df = dates_df.copy()

    # Adding a common constant column (__key = 1) to both DataFrames and
    # merging on it efficiently produces all combinations of rows from both tables.
    handle_condition_df["__key"] = 1
    dates_df["__key"] = 1

    user_condition_date_combinations: pd.DataFrame = handle_condition_df.merge(
        dates_df, on="__key"
    ).drop(columns="__key")

    return user_condition_date_combinations


def fill_user_condition_date_combinations(
    user_condition_date_combinations: pd.DataFrame,
    per_user_per_day_aggregated: pd.DataFrame,
) -> pd.DataFrame:
    """Fills in the user-condition-date combinations with the
    per-user-per-day aggregated data.

    Joins on the combination of user handle, condition, and date. Imputes
    null values for dates where no data exists.

    Args:
        user_condition_date_combinations: DataFrame with columns ['handle', 'condition', 'date']
        per_user_per_day_aggregated: DataFrame with columns ['handle', 'condition', 'date', ...] containing aggregated results

    Returns:
        DataFrame with all (handle, condition, date) combinations, with columns from per_user_per_day_aggregated merged in
        and NaN values for dates with no per-user-per-day data.
    """
    # Ensure the relevant join columns exist
    merge_cols = ["handle", "condition", "date"]
    # Do a left merge to keep every user-condition-date combo,
    # imputing NaNs for missing data

    # how="left": Performs a left join, keeping all rows from
    # user_condition_date_combinations and merging matching rows from
    # per_user_per_day_aggregated; unmatched rows get NaNs.
    # validate="one_to_one": Checks that both merging DataFrames have at most
    # one matching row per join key; raises an error if this is violated
    # (prevents accidental duplicates).

    filled_user_condition_date_combinations: pd.DataFrame = (
        user_condition_date_combinations.merge(
            per_user_per_day_aggregated,
            on=merge_cols,
            how="left",
            validate="one_to_one",
        )
    )
    return filled_user_condition_date_combinations


def sort_filled_user_condition_date_combinations(
    filled_user_condition_date_combinations: pd.DataFrame,
) -> pd.DataFrame:
    """Sorts the filled user-condition-date combinations by handle, date, and
    condition in stable manner.

    kind="stable" in sort_values ensures that rows with equal sort keys keep
    their original order (i.e., sorting is stable). This is useful to preserve
    input order where sort keys are tied.
    """
    sorted_filled_user_condition_date_combinations: pd.DataFrame = (
        filled_user_condition_date_combinations.sort_values(
            by=["handle", "date", "condition"],
            kind="stable",
        )  # type: ignore
    )
    return sorted_filled_user_condition_date_combinations


def export_user_condition_date_combinations(
    sorted_filled_user_condition_date_combinations: pd.DataFrame,
) -> None:
    """Exports the sorted user-condition-date combinations to a CSV file."""
    sorted_filled_user_condition_date_combinations.to_csv(
        OUTPUT_CSV,
        index=False,
    )
    print(
        f"Wrote {len(sorted_filled_user_condition_date_combinations):,} rows to {OUTPUT_CSV}"
    )


def main() -> None:
    df: pd.DataFrame = pd.read_csv(INPUT_CSV)

    df = add_date_column(df)
    df = filter_records(df)

    per_user_per_day: pd.DataFrame = aggregate_by_user_and_date(df)
    per_user_per_day = rename_handle_column(per_user_per_day)

    handle_condition_df: pd.DataFrame = get_unique_study_handles_and_conditions(
        per_user_per_day
    )

    dates_df: pd.DataFrame = get_study_dates()

    full_user_condition_date_combinations: pd.DataFrame = (
        create_user_condition_date_combinations(
            handle_condition_df=handle_condition_df,
            dates_df=dates_df,
        )
    )

    filled_user_condition_date_combinations: pd.DataFrame = (
        fill_user_condition_date_combinations(
            user_condition_date_combinations=full_user_condition_date_combinations,
            per_user_per_day_aggregated=per_user_per_day,
        )
    )

    sorted_filled_user_condition_date_combinations: pd.DataFrame = sort_filled_user_condition_date_combinations(
        filled_user_condition_date_combinations=filled_user_condition_date_combinations,
    )

    export_user_condition_date_combinations(
        sorted_filled_user_condition_date_combinations=sorted_filled_user_condition_date_combinations,
    )


if __name__ == "__main__":
    main()

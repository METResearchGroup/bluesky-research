"""Calculate binary average classifications per label, per user, per day."""

import os

import pandas as pd

from lib.constants import current_datetime_str, project_home_directory
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.calculate_analytics.calculate_weekly_thresholds_per_user import (
    load_user_demographic_info,
)
from services.calculate_analytics.study_analytics.calculate_analytics.feed_analytics import (
    get_per_user_feed_average_labels_for_study,
)


logger = get_logger(__name__)
current_filedir = os.path.dirname(os.path.abspath(__file__))


def main():
    logger.info("Starting main function...")

    # load user demographics from DynamoDB
    user_demographics: pd.DataFrame = load_user_demographic_info()
    logger.info(f"Loaded user demographics with {len(user_demographics)} rows")

    # get per-user daily averages for each date.
    per_user_averages: pd.DataFrame = get_per_user_feed_average_labels_for_study(
        load_unfiltered_posts=True
    )

    joined_df: pd.DataFrame = per_user_averages.merge(
        user_demographics, left_on="user_did", right_on="bluesky_user_did", how="left"
    )

    # postprocessing.
    # Drop unnamed index column if it exists
    for col in ["Unnamed: 0", "bluesky_user_did"]:
        if col in joined_df.columns:
            joined_df = joined_df.drop(col, axis=1)

    # Sort rows with user_did = "default" to end of dataframe
    default_mask = joined_df["user_did"] == "default"
    joined_df = pd.concat(
        [joined_df[~default_mask], joined_df[default_mask]]
    ).reset_index(drop=True)

    # Reorder columns to put key columns first
    cols = joined_df.columns.tolist()
    key_cols = ["bluesky_handle", "user_did", "condition", "date"]
    other_cols = [col for col in cols if col not in key_cols]
    joined_df = joined_df[key_cols + other_cols]

    # export
    joined_df.to_csv(
        os.path.join(
            current_filedir,
            "prop_labeled_posts",
            f"average_daily_prop_labeled_posts_per_user_{current_datetime_str}.csv",
        )
    )

    # now, get a joined version mapping the joined_df to the week thresholds.
    week_thresholds: pd.DataFrame = pd.read_csv(
        os.path.join(
            project_home_directory,
            "services",
            "calculate_analytics",
            "study_analytics",
            "generate_reports",
            "bluesky_per_user_week_assignments.csv",
        )
    )
    week_thresholds = week_thresholds[["bluesky_handle", "date", "week_static"]]
    joined_df = joined_df.merge(
        week_thresholds, on=["bluesky_handle", "date"], how="left"
    ).sort_values(["bluesky_handle", "date"], ascending=[True, True])
    # Group by handle and week, taking mean of numeric columns and mode of categorical columns
    numeric_cols = joined_df.select_dtypes(include=["float64", "int64"]).columns
    categorical_cols = joined_df.select_dtypes(include=["object", "category"]).columns

    # Create aggregation dictionary
    agg_dict = {}
    for col in joined_df.columns:
        if col in ["bluesky_handle", "week_static"]:  # Skip groupby columns
            continue
        elif col in numeric_cols:
            agg_dict[col] = "mean"
        elif col in categorical_cols:
            agg_dict[col] = lambda x: x.mode().iloc[0] if not x.mode().empty else None

    weekly_joined_df = (
        joined_df.groupby(["bluesky_handle", "week_static"]).agg(agg_dict).reset_index()
    )
    weekly_joined_df.to_csv(
        os.path.join(
            current_filedir,
            "prop_labeled_posts",
            f"weekly_daily_prop_labeled_posts_per_user_{current_datetime_str}.csv",
        )
    )


if __name__ == "__main__":
    main()

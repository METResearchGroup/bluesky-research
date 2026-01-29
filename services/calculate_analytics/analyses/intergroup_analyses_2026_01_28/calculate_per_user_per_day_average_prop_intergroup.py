"""
Regenerate per-feed intergroup proportion from generated_feeds + ml_inference_intergroup,
then aggregate to per-user-per-day averages.

Loads feeds (generated_feeds) and intergroup labels (ml_inference_intergroup). Per-feed,
computes the proportion of posts with intergroup label = 1 among labeled posts only.
Joins with user data for handle/condition, then aggregates by user+condition+date and
exports per_user_per_day_average_prop_intergroup.csv.

Output (written in this directory):
- per_user_per_day_average_prop_intergroup.csv
  Columns:
    - handle
    - condition
    - date
    - feed_proportion_intergroup (mean of per-feed proportions for that user+day)
"""

from __future__ import annotations

import os
from typing import cast

import pandas as pd

from lib.datetime_utils import get_partition_dates
from lib.helper import get_partition_date_from_timestamp
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_END_DATE,
    STUDY_START_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import get_feeds_per_user
from services.calculate_analytics.shared.data_loading.labels import (
    load_intergroup_labels_for_date_range,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str

logger = get_logger(__file__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(
    BASE_DIR,
    "per_user_per_day_average_prop_intergroup.csv",
)
REQUIRED_COLUMNS = [
    "bluesky_handle",
    "condition",
    "feed_generation_timestamp",
    "prop_intergroup_labeled",
    "date",
]


def _collect_all_feed_uris(feeds_df: pd.DataFrame) -> set[str]:
    """Collect all post URIs that appear in any feed."""
    all_uris: set[str] = set()
    for row in feeds_df.itertuples(index=False):
        feed_str = getattr(row, "feed", None)
        if feed_str is None:
            continue
        feed = load_feed_from_json_str(feed_str)
        if isinstance(feed, list):
            for post in feed:
                uri = post.get("item")
                if uri:
                    all_uris.add(uri)
    return all_uris


def _build_per_feed_proportion_table(
    feeds_df: pd.DataFrame,
    uri_to_label: dict[str, int],
) -> pd.DataFrame:
    """Build a DataFrame with one row per feed: bluesky_user_did, feed_generation_timestamp, prop_intergroup_labeled."""
    rows: list[dict] = []
    for row in feeds_df.itertuples(index=False):
        feed_str = getattr(row, "feed", None)
        if feed_str is None:
            continue
        feed = load_feed_from_json_str(feed_str)
        if not isinstance(feed, list):
            user_did = getattr(row, "bluesky_user_did", "?")
            logger.warning(f"Feed for user {user_did} is not a list, skipping")
            continue
        labels_in_feed: list[int] = []
        for post in feed:
            uri = post.get("item")
            if not uri:
                continue
            label = uri_to_label.get(uri)
            if label in (0, 1):
                labels_in_feed.append(label)
        n_posts = len(feed)
        n_labeled = len(labels_in_feed)
        if n_labeled == 0:
            prop = float("nan")
        else:
            prop = sum(labels_in_feed) / n_labeled
        rows.append(
            {
                "bluesky_user_did": getattr(row, "bluesky_user_did"),
                "feed_generation_timestamp": getattr(row, "feed_generation_timestamp"),
                "prop_intergroup_labeled": prop,
                "n_posts_in_feed": n_posts,
                "n_labeled_posts_in_feed": n_labeled,
            }
        )
    return pd.DataFrame(rows)


def add_date_column(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["feed_generation_timestamp"].astype(str)
    df = df.copy()
    df["date"] = ts.map(get_partition_date_from_timestamp)
    return df


def filter_records(df: pd.DataFrame) -> pd.DataFrame:
    """Filter records by study date range and exclude placeholder handle."""
    date_range_mask = (df["date"] >= STUDY_START_DATE) & (df["date"] <= STUDY_END_DATE)
    filtered: pd.DataFrame = df.loc[date_range_mask].copy()
    if "bluesky_handle" in filtered.columns:
        handle_mask = filtered["bluesky_handle"] != "default"
        filtered = filtered.loc[handle_mask].copy()
    date_exclude_mask = ~filtered["date"].isin(exclude_partition_dates)
    filtered = filtered.loc[date_exclude_mask].copy()
    columns_to_keep = [c for c in REQUIRED_COLUMNS if c in filtered.columns]
    if columns_to_keep:
        filtered = filtered.loc[:, columns_to_keep].copy()
    return filtered


def aggregate_by_user_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """Group by bluesky_handle, condition, date; aggregate prop_intergroup_labeled with mean."""
    grouped = df.groupby(
        ["bluesky_handle", "condition", "date"],
        as_index=False,
        dropna=False,
    )
    aggregated = grouped.agg(
        feed_proportion_intergroup=("prop_intergroup_labeled", "mean"),
    )
    return pd.DataFrame(aggregated)


def rename_handle_column(aggregated: pd.DataFrame) -> pd.DataFrame:
    return aggregated.rename(columns={"bluesky_handle": "handle"})


def get_study_dates() -> pd.DataFrame:
    all_dates: list[str] = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    return pd.DataFrame({"date": all_dates})


def create_user_condition_date_combinations(
    handle_condition_df: pd.DataFrame,
    dates_df: pd.DataFrame,
) -> pd.DataFrame:
    handle_condition_df = handle_condition_df.copy()
    dates_df = dates_df.copy()
    handle_condition_df["__key"] = 1
    dates_df["__key"] = 1
    combined: pd.DataFrame = handle_condition_df.merge(dates_df, on="__key").drop(
        columns="__key"
    )
    return combined


def fill_user_condition_date_combinations(
    user_condition_date_combinations: pd.DataFrame,
    per_user_per_day_aggregated: pd.DataFrame,
) -> pd.DataFrame:
    merge_cols = ["handle", "condition", "date"]
    filled = user_condition_date_combinations.merge(
        per_user_per_day_aggregated,
        on=merge_cols,
        how="left",
        validate="one_to_one",
    )
    return pd.DataFrame(filled)


def sort_and_export(filled: pd.DataFrame) -> None:
    sorted_df = filled.sort_values(
        by=["handle", "date", "condition"],
        kind="stable",
    )
    sorted_df.to_csv(OUTPUT_CSV, index=False)
    logger.info(f"Wrote {len(sorted_df):,} rows to {OUTPUT_CSV}")


def main() -> None:
    # 1. Setup
    user_df, _user_date_to_week_df, valid_study_users_dids = load_user_data()
    partition_dates = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    logger.info(
        f"Loaded {len(valid_study_users_dids)} study users, {len(partition_dates)} partition dates"
    )

    # 2. Load feeds (bluesky_user_did, feed, feed_generation_timestamp only)
    feeds_df = get_feeds_per_user(valid_study_users_dids)
    logger.info(f"Loaded {len(feeds_df)} feeds")

    # 3. Collect all feed URIs
    all_feed_uris = _collect_all_feed_uris(feeds_df)
    logger.info(f"Collected {len(all_feed_uris):,} unique URIs across feeds")

    # 4. Load intergroup labels for date range (optionally filtered to feed URIs)
    uri_to_label = load_intergroup_labels_for_date_range(
        start_partition_date=STUDY_START_DATE,
        end_partition_date=STUDY_END_DATE,
        uris=all_feed_uris,
    )
    logger.info(f"Loaded {len(uri_to_label):,} intergroup labels (uri -> 0/1)")

    # 5. Build per-feed proportion table (bluesky_user_did, feed_generation_timestamp, prop_intergroup_labeled)
    per_feed_df = _build_per_feed_proportion_table(feeds_df, uri_to_label)
    logger.info(f"Built per-feed table: {len(per_feed_df)} rows")

    # 6. Join with user_df to add bluesky_handle and condition
    user_cols = user_df[["bluesky_user_did", "bluesky_handle", "condition"]]
    per_feed_df = per_feed_df.merge(
        user_cols,
        on="bluesky_user_did",
        how="left",
    )
    logger.info(f"Joined with user data: {len(per_feed_df)} rows with handle/condition")

    # 7. Add date, filter, aggregate to per-user-per-day, rename handle
    per_feed_df = add_date_column(per_feed_df)
    per_feed_df = filter_records(per_feed_df)
    per_user_per_day = aggregate_by_user_and_date(per_feed_df)
    per_user_per_day = rename_handle_column(per_user_per_day)

    # 8. Build full user-condition-date grid and fill with aggregated values
    handle_condition_df = cast(
        pd.DataFrame,
        per_user_per_day[["handle", "condition"]].drop_duplicates(),
    )
    dates_df = cast(pd.DataFrame, get_study_dates())
    full_combinations = create_user_condition_date_combinations(
        handle_condition_df=handle_condition_df,
        dates_df=dates_df,
    )
    filled = fill_user_condition_date_combinations(
        user_condition_date_combinations=full_combinations,
        per_user_per_day_aggregated=per_user_per_day,
    )

    # 9. Sort and export
    sort_and_export(filled)
    logger.info("Done.")


if __name__ == "__main__":
    main()

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
from typing import Any

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
# Columns to keep after filtering; used for the aggregation step.
COLUMNS_FOR_AGGREGATION = [
    "bluesky_handle",
    "condition",
    "feed_generation_timestamp",
    "prop_intergroup_labeled",
    "date",
]


def _feed_row_to_parsed(row: dict[str, Any]) -> dict[str, Any] | None:
    """Transform one feed row into (feed_id, bluesky_user_did, feed_generation_timestamp, uris). Returns None if row is skipped."""
    feed_str = row["feed"]
    feed_id = row["feed_id"]
    bluesky_user_did = row["bluesky_user_did"]
    feed_generation_timestamp = row["feed_generation_timestamp"]

    if feed_str is None:
        return None
    try:
        feed = load_feed_from_json_str(feed_str)
    except (ValueError, TypeError):
        logger.warning(
            f"Feed for user {bluesky_user_did} is not valid JSON, skipping",
        )
        return None

    if not isinstance(feed, list):
        logger.warning(
            f"Feed for user {bluesky_user_did} is not a list, skipping",
        )
        return None

    uris: list[str] = [x for post in feed if (x := post.get("item"))]

    return {
        "feed_id": feed_id,
        "bluesky_user_did": bluesky_user_did,
        "feed_generation_timestamp": feed_generation_timestamp,
        "uris": uris,
    }


def _parse_feeds_to_uri_lists(feeds_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Parse each feed once into (feed_id, bluesky_user_did, feed_generation_timestamp, list of URIs)."""
    parsed: list[dict[str, Any]] = []
    for row in feeds_df.to_dict("records"):
        item = _feed_row_to_parsed(row)
        if item is not None:
            parsed.append(item)
    return parsed


def _all_feed_uris(parsed: list[dict[str, Any]]) -> set[str]:
    """Collect all post URIs that appear in any parsed feed."""
    all_uris: set[str] = set()
    for p in parsed:
        all_uris.update(p["uris"])
    return all_uris


def _parsed_feed_to_row(
    p: dict[str, Any],
    uri_to_label: dict[str, int],
) -> dict[str, Any]:
    """Transform one parsed feed into a per-feed row (prop_intergroup_labeled, counts, ids)."""
    labels_in_feed = [
        uri_to_label[uri] for uri in p["uris"] if uri_to_label.get(uri) in (0, 1)
    ]
    n_posts = len(p["uris"])
    n_labeled = len(labels_in_feed)
    prop = (sum(labels_in_feed) / n_labeled) if n_labeled else float("nan")
    return {
        "bluesky_user_did": p["bluesky_user_did"],
        "feed_generation_timestamp": p["feed_generation_timestamp"],
        "prop_intergroup_labeled": prop,
        "n_posts_in_feed": n_posts,
        "n_labeled_posts_in_feed": n_labeled,
        "feed_id": p["feed_id"],
    }


def _build_per_feed_proportion_table(
    parsed: list[dict[str, Any]],
    uri_to_label: dict[str, int],
) -> pd.DataFrame:
    """Build one row per feed: feed_id, bluesky_user_did, feed_generation_timestamp, prop_intergroup_labeled."""
    rows = [_parsed_feed_to_row(p, uri_to_label) for p in parsed]
    return pd.DataFrame(rows)


def _aggregate_per_feed_to_per_user_per_day(per_feed_df: pd.DataFrame) -> pd.DataFrame:
    """Add date, filter by study window, group by (handle, condition, date), mean proportion, rename handle."""
    df = per_feed_df.copy()
    df["date"] = (
        df["feed_generation_timestamp"]
        .astype(str)
        .map(get_partition_date_from_timestamp)
    )

    in_date_range = (df["date"] >= STUDY_START_DATE) & (df["date"] <= STUDY_END_DATE)
    not_excluded_date = ~df["date"].isin(exclude_partition_dates)
    handle_ok = (
        df["bluesky_handle"] != "default" if "bluesky_handle" in df.columns else True
    )
    mask = in_date_range & not_excluded_date & handle_ok
    columns_to_keep = [c for c in COLUMNS_FOR_AGGREGATION if c in df.columns]
    filtered = df.loc[mask, columns_to_keep].copy()

    grouped = filtered.groupby(
        ["bluesky_handle", "condition", "date"],
        as_index=False,
        dropna=False,
    )
    aggregated = grouped.agg(
        feed_proportion_intergroup=("prop_intergroup_labeled", "mean"),
    )
    return aggregated.rename(columns={"bluesky_handle": "handle"})


def _expand_to_full_grid_and_export(
    per_user_per_day: pd.DataFrame,
    output_path: str,
) -> None:
    """Cross-join (handle, condition) with study dates, left-join aggregated values, sort, write CSV."""
    handle_condition_df = per_user_per_day[["handle", "condition"]].drop_duplicates()
    all_dates = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    dates_df = pd.DataFrame({"date": all_dates})

    # Cross-join via constant-key merge: every (handle, condition) × every date
    handle_condition_df = handle_condition_df.copy()
    dates_df = dates_df.copy()
    handle_condition_df["__key"] = 1
    dates_df["__key"] = 1
    full_combinations = handle_condition_df.merge(dates_df, on="__key").drop(
        columns="__key"
    )

    filled = full_combinations.merge(
        per_user_per_day,
        on=["handle", "condition", "date"],
        how="left",
        validate="one_to_one",
    )
    sorted_df = filled.sort_values(
        by=["handle", "date", "condition"],
        kind="stable",
    )
    sorted_df.to_csv(output_path, index=False)
    logger.info(f"Wrote {len(sorted_df):,} rows to {output_path}")


def main() -> None:
    user_df, _user_date_to_week_df, valid_study_users_dids = load_user_data()
    partition_dates = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    logger.info(
        f"Loaded {len(valid_study_users_dids)} study users, {len(partition_dates)} partition dates",
    )

    feeds_df = get_feeds_per_user(valid_study_users_dids)
    logger.info(f"Loaded {len(feeds_df)} feeds")

    parsed = _parse_feeds_to_uri_lists(feeds_df)
    all_feed_uris = _all_feed_uris(parsed)
    logger.info(f"Collected {len(all_feed_uris):,} unique URIs across feeds")

    uri_to_label = load_intergroup_labels_for_date_range(
        start_partition_date=STUDY_START_DATE,
        end_partition_date=STUDY_END_DATE,
        uris=all_feed_uris,
    )
    logger.info(f"Loaded {len(uri_to_label):,} intergroup labels (uri -> 0/1)")

    per_feed_df = _build_per_feed_proportion_table(parsed, uri_to_label)
    logger.info(f"Built per-feed table: {len(per_feed_df)} rows")

    per_feed_df = per_feed_df.merge(
        user_df[["bluesky_user_did", "bluesky_handle", "condition"]],
        on="bluesky_user_did",
        how="left",
    )
    logger.info(f"Joined with user data: {len(per_feed_df)} rows with handle/condition")

    per_user_per_day = _aggregate_per_feed_to_per_user_per_day(per_feed_df)
    _expand_to_full_grid_and_export(per_user_per_day, OUTPUT_CSV)
    logger.info("Done.")


if __name__ == "__main__":
    main()

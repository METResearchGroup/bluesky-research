"""Module for content analysis."""

from typing import Optional

import numpy as np
import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.shared.processing.content_label_processing import (
    flatten_content_metrics_across_record_types,
    get_metrics_for_record_types,
    get_metrics_for_user_feeds_from_partition_date,
    get_metrics_for_baseline_content,
)
from services.calculate_analytics.shared.constants import TOTAL_STUDY_WEEKS_PER_USER

logger = get_logger(__file__)


def get_daily_feed_content_per_user_metrics(
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    labels_for_feed_content: dict[str, dict],
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Given a map of user and the content they engaged with,
    as well as the labels for any engaged content, start linking them together.

    Our input looks like this:

    user_to_content_in_feeds:
        {
            "<did>": {
                "<date>": {"<uri_1", "<uri_2>", ...}
            }
        }

    labels_for_feed_content:
        {
            "<uri_1>": {
                "prob_toxic": 0.001,
                "prob_constructive": 0.0002,
                ...
            },
            "<uri_2>": {
                "prob_toxic": 0.0001,
                "prob_constructive": 0.0002,
                ...
            },
            ...
        }

    We iterate through this. For each did, date, and record_type combo,
    we look up the corresponding URIs in the hash map, and then we use this
    to calculate per-integration averages for each did+date+record_type combo.

    The output will look something like this:
    {
        "<did>": {
            "<date>": {
                "prop_toxic": 0.3,
                "prop_sociopolitical": 0.05,
                ...
            }
        }
    }
    """
    daily_feed_content_per_user_metrics: dict[
        str, dict[str, dict[str, float | None]]
    ] = {}

    for did, content_in_feeds in user_to_content_in_feeds.items():
        daily_feed_content_per_user_metrics[did] = {}

        # partition date to metrics map. Metrics is a dictionary of <label>: <metric value>.
        # e.g., {"<date>": {"<label 1>": 0.5, "<label 2>": 0.2", ...}, ...}
        partition_date_to_metrics_map: dict[str, dict[str, float | None]] = {}

        for partition_date, post_uris in content_in_feeds.items():
            # get, for the given user + date, the metrics for the posts that
            # appeared in their feeds.
            per_user_per_date_content_metrics: dict[str, float | None] = (
                get_metrics_for_user_feeds_from_partition_date(
                    post_uris=post_uris, labels_for_feed_content=labels_for_feed_content
                )
            )

            # add to the running per-user, per-date metrics map.
            partition_date_to_metrics_map[partition_date] = (
                per_user_per_date_content_metrics
            )

            del per_user_per_date_content_metrics

        # now that we've gone through all the feed content from the user,
        # per day, for a given user, we now add this to the hash map of
        # per-user, per-date feed content metrics.
        daily_feed_content_per_user_metrics[did] = partition_date_to_metrics_map

        del partition_date_to_metrics_map

    return daily_feed_content_per_user_metrics


def get_daily_engaged_content_per_user_metrics(
    user_to_content_engaged_with: dict[str, dict], labels_for_engaged_content: dict
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Given a map of user and the content they engaged with,
    as well as the labels for any engaged content, start linking them together.

    Our input looks like this:

    user_to_content_engaged_with:
        {
            "<did>": {
                "<date>": {
                    "post": ["<uri_1", "<uri_2>", ...]
                    "like": ["<uri_1", "<uri_2>", ...]
                    "repost": ["<uri_1", "<uri_2>", ...]
                    "reply": ["<uri_1", "<uri_2>", ...]
                }
            }
        }

    labels_for_engaged_content:
        {
            "<uri_1>": {
                "prob_toxic": 0.001,
                "prob_constructive": 0.0002,
                ...
            },
            "<uri_2>": {
                "prob_toxic": 0.0001,
                "prob_constructive": 0.0002,
                ...
            },
            ...
        }

    We iterate through this. For each did, date, and record_type combo,
    we look up the corresponding URIs in the hash map, and then we use this
    to calculate per-integration averages for each did+date+record_type combo.

    Our intermediate output will look something like this:

    {
        "<did>": {
            "<date>": {
                "post": {
                    # we'll calculate, for example, the number of posts with
                    # prob_toxic > 0.5 and we'll get that proportion.
                    "prop_toxic": 0.3,
                    "prop_sociopolitical": 0.05,
                    ...
                }
                "like": {
                    # we'll calculate, for example, the number of posts with
                    # prob_toxic > 0.5 and we'll get that proportion.
                    "prop_toxic": 0.3,
                    "prop_sociopolitical": 0.05,
                    ...
                }
                "repost": {
                    # Let's say that for this partition date, that user has
                    # no reposts. We impute a None.
                    "prop_toxic": None,
                    "prop_sociopolitical": None
                    ...
                }
                "reply": {
                    # Let's say that for this partition date, that user has
                    # no replies. We impute a None.
                    "prop_toxic": None,
                    "prop_sociopolitical": None
                    ...
                }
            }
        }
    }

    We'll then flatten across record types to get something like this:

    {
        "<did>": {
            "<date>": {
                "prop_posted_posts_toxic": 0.03,
                "prop_liked_posts_toxic": 0.002,
                "prop_reposted_posts_toxic": None,
                "prop_replied_posts_toxic": None,
                ...
                "prop_posted_posts_sociopolitical": 0.01,
                "prop_liked_posts_sociopolitical": 0.05,
                "prop_reposted_posts_sociopolitical": None,
                "prop_replied_posts_sociopolitical": None,
                ...
            }
        }
    }
    """
    daily_engaged_content_per_user_metrics: dict[
        str, dict[str, dict[str, float | None]]
    ] = {}

    # iterate through each user and the content they engaged with.
    for did, content_engaged_with in user_to_content_engaged_with.items():
        daily_engaged_content_per_user_metrics[did] = {}
        partition_date_to_metrics_map = {}

        # for each user, iterate through the content that they engaged
        # with per day. Inputs will look like this:
        # {"post": ["<uri_1", "<uri_2>", ...], "like": ["<uri_1", "<uri_2>", ...], ...}
        for partition_date, record_types in content_engaged_with.items():
            # iterate through each engagement type (e.g., post, like, repost, reply)
            # and the corresponding posts. Get (1) the average metrics (e.g., average toxicity)
            # and (2) the proportion metrics (e.g., the proportion of posts that are toxic).
            record_type_to_metrics_map: dict[str, dict[str, Optional[float]]] = (
                get_metrics_for_record_types(
                    record_types=record_types,
                    user_to_content_engaged_with=user_to_content_engaged_with,
                    labels_for_engaged_content=labels_for_engaged_content,
                    did=did,
                    partition_date=partition_date,
                )
            )

            # flatten the averages and proportions across record types, to build
            # a single dictionary of metrics for this user and for this date.
            # The previous for-loop, for example, creates separate dictionaries
            # for each record type, to calculate, for example, the average toxicity
            # of all posts liked by User 1 on 2024-10-01. In this following for-loop,
            # we now collapse across all record types and combine them into one
            # large dictionary that contains the averages and proportions for
            # User 1 on 2024-10-01 across all record types (e.g., prop_liked_posts_toxic,
            # prop_liked_posts_constructive, etc.).
            per_user_per_date_content_metrics: dict[str, Optional[float]] = (
                flatten_content_metrics_across_record_types(
                    record_type_to_metrics_map=record_type_to_metrics_map
                )
            )

            del record_type_to_metrics_map

            # add to the running per-user, per-date metrics map.
            partition_date_to_metrics_map[partition_date] = (
                per_user_per_date_content_metrics
            )

            del per_user_per_date_content_metrics

        # now that we've gone through all the engagement records from the users,
        # per day, for a given user, we now add this to the hash map of
        # per-user, per-date content engagement metrics.
        daily_engaged_content_per_user_metrics[did] = partition_date_to_metrics_map

    return daily_engaged_content_per_user_metrics


def get_daily_baseline_content_metrics(
    labels_for_content: dict[str, dict],
) -> dict[str, float | None]:
    """Calculate baseline metrics across all labeled posts for a given day.

    This function calculates metrics across all labeled posts without any user-specific
    filtering, providing baseline measures for the entire dataset on a given day.

    Args:
        labels_for_content: Dictionary mapping post URIs to their label dictionaries

    Returns:
        Dictionary of baseline metrics with keys like:
        - baseline_average_toxic
        - baseline_proportion_is_sociopolitical
        - baseline_average_is_valence_positive
        - baseline_proportion_is_valence_positive
        etc.
    """
    return get_metrics_for_baseline_content(labels_for_content=labels_for_content)


def get_weekly_baseline_content_metrics(
    baseline_per_day_content_label_metrics: dict[str, dict[str, float | None]],
    date_to_week_mapping: dict[str, int],
) -> dict[int, dict[str, float | None]]:
    """Get the weekly baseline content metrics.

    Groups daily baseline metrics by week and aggregates (averages) the values
    across each week.

    Args:
        baseline_per_day_content_label_metrics: Dictionary mapping dates to their baseline metrics
        date_to_week_mapping: Dictionary mapping dates to week numbers

    Returns:
        Dictionary mapping week numbers to their aggregated baseline metrics
    """
    weekly_baseline_metrics: dict[int, dict[str, float | None]] = {}

    # Group daily metrics by week
    for date, daily_metrics in baseline_per_day_content_label_metrics.items():
        if date not in date_to_week_mapping:
            continue

        week_number = date_to_week_mapping[date]

        # Initialize week if not seen before
        if week_number not in weekly_baseline_metrics:
            weekly_baseline_metrics[week_number] = {
                metric: [] for metric in daily_metrics.keys()
            }

        # Add daily metrics to the week's collection
        for metric_name, metric_value in daily_metrics.items():
            if metric_value is not None:
                weekly_baseline_metrics[week_number][metric_name].append(metric_value)

    # Calculate weekly averages
    for week_number, daily_metrics_map in weekly_baseline_metrics.items():
        weekly_baseline_metrics[week_number] = {
            metric: round(np.mean(daily_metric_values), 3)
            if daily_metric_values
            else None
            for metric, daily_metric_values in daily_metrics_map.items()
        }

    return weekly_baseline_metrics


def get_weekly_content_per_user_metrics(
    user_per_day_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    user_date_to_week_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Get the weekly engaged content per user metrics.

    user_per_day_content_label_metrics: dict[str, dict[str, Optional[float]]]
    user_date_to_week_df: pd.DataFrame

    Groups by user and week and then aggregates (averages) the values of the
    metrics across each week.

    Notes on averaging:
    - We have user + day combinations for days that the user had some form
    of engagement. We don't have records for days that these combinations don't
    exist (we impute these as None when we export the daily records,
    as per 'transform_daily_engaged_content_per_user_metrics', but we
    dont use them for averaging records).
    - For us, this means that "average" only refers to records that exist (e.g.,
    if a user only posted on Monday, then the "average toxicity of posts from the week"
    only refers to the posts on Monday).
    - If a user doesn't have any engagement data for a given week, we'll return
    None for that week.
    - We have user + date records, but on a given date the user might not have
    all record types, e.g., they might have likes but not reposts, so any
    repost_* fields are going to be None. I accounted for NoneType values in
    the metrics calculation for averages and proportions already (i.e., any None
    values are filtered out and removed for the purposes of metric calculation).
    Therefore, if we see any None values here, that literally means that we
    didn't have any records at all for that user + date + record type combo
    (e.g., if any reposts_* records are None, this means that on that date for
    that user, they had 0 reposts).
    - Therefore, for averaging, we filter out any None values and we only average
    out across the days that had records.

    The output will look something like this:

    {
        "<did>": {
            "<week>": {
                "<metric 1>": 0.03,
                "<metric 2>": 0.002,
                "<metric 3>": None,
                "<metric 4>": None,
                ...
            }
        }
    }
    """

    # create index lookup to speed up the lookup of the week for a given user and date.
    user_date_to_week_df = user_date_to_week_df.set_index(["bluesky_user_did", "date"])[
        "week"
    ]

    weekly_content_per_user_metrics: dict[str, dict[str, dict[str, float | None]]] = {}
    for user, dates_to_metrics_map in user_per_day_content_label_metrics.items():
        weekly_content_per_user_metrics[user] = {}
        content_metrics_per_week_map: dict = {}

        # iterate through each date and their metrics and add it to a running
        # aggregate list of metrics for each of the weeks.
        for date, metrics in dates_to_metrics_map.items():
            week = user_date_to_week_df.get((user, date))
            if week is None:
                continue

            # if week is not yet in `content_metrics_per_week_map`, initialize
            # the lists that will store each of the daily averages/proportions
            # for each of the labels.
            if week not in content_metrics_per_week_map:
                content_metrics_per_week_map[week] = {
                    metric: [] for metric in metrics.keys()
                }

            # iterate through each label and its daily value for the given date
            # and add to the running lists for the week.
            for metric_name, metric_value in metrics.items():
                # filter out any None values, as described in the docstring.
                if metric_value is not None:
                    content_metrics_per_week_map[week][metric_name].append(metric_value)

        # now that we've gone through all the dates and created per-user, per-week
        # lists of metrics (e.g., we have the average daily toxicity, for each
        # day, for the posts liked by a user in Week 1), we now average them out.
        # (e.g, we get the average toxicity of the posts liked by a user in Week 1).
        for week, daily_metrics_map in content_metrics_per_week_map.items():
            # iterate through each label and its daily values for the given week
            # and average them out. Then add to the running map.
            # NOTE: should only possibly have 'len(daily_metric_values) == 0'
            # for engagement data (since, for example, for a given date, you
            # might have only likes and you might not have reposts, replies, etc.).
            # For feed data, shouldn't ever have this case (feeds are made every day).
            weekly_content_per_user_metrics[user][week] = {
                metric: round(np.mean(daily_metric_values), 3)
                if daily_metric_values
                else None
                for metric, daily_metric_values in daily_metrics_map.items()
            }

    return weekly_content_per_user_metrics


# Shared across both engagement and feed content analysis.
def transform_daily_content_per_user_metrics(
    user_per_day_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    users: pd.DataFrame,
    partition_dates: list[str],
) -> pd.DataFrame:
    """Transform the daily content per user metrics into the output format required.

    user_per_day_content_label_proportions: dict[str, dict[str, dict[str, float | None]]]

    Example input:

    # For feed content:
    {
        "<did>": {
            "<date>": {
                "prop_toxic": 0.03,
                "prop_sociopolitical": 0.01,
                ...
            }
        }
    }

    # For engagement content:
    {
        "<did>": {
            "<date>": {
                "prop_posted_posts_toxic": 0.03,
                "prop_liked_posts_toxic": 0.002,
                "prop_reposted_posts_toxic": None,
                "prop_replied_posts_toxic": None,
                ...
                "prop_posted_posts_sociopolitical": 0.01,
                "prop_liked_posts_sociopolitical": 0.05,
                "prop_reposted_posts_sociopolitical": None,
                "prop_replied_posts_sociopolitical": None,
                ...
            }
        }
    }

    users: pd.DataFrame of the users.
    Contains the fields ["bluesky_handle", "bluesky_user_did", "condition"]

    Flattens the nested dictionary into a list of dictionaries of the following format:
    ```
    {
        "handle": "<handle>",
        "condition": "<condition>",
        "date": "<date>",
        "prop_posted_posts_toxic": 0.03,
        "prop_liked_posts_toxic": 0.002,
        "prop_reposted_posts_toxic": None,
        "prop_replied_posts_toxic": None,
        ...
    }
    ```

    Then we convert this to a pandas DataFrame:

    ```
    index | handle     | condition     | date     | prop_posted_posts_toxic | prop_liked_posts_toxic | prop_reposted_posts_toxic |
    0     | <handle_1> | <condition_1> | <date_1> | 0.03                    | 0.002                  | None                      |
    1     | <handle_2> | <condition_2> | <date_2> | 0.01                    | 0.05                   | None                      |
    ...   | ...        | ...           | ...      | ...                     | ...                    | ...                       |
    ...   | ...        | ...           | ...      | ...                     | ...                    | ...                       |
    ```

    For user + date combos without a record, we'll impute an empty dictionary,
    and then when we create the pandas dataframe, the columns for this date will
    be NaN.

    Simple reproducible example:
    ```
    >>> records = [{"field1": 1, "field2": 2}, {}]
    >>> df = pd.DataFrame(records)
    >>> df
    field1  field2
    0     1.0     2.0
    1     NaN     NaN
    ```
    """

    # Pre-compute user info for O(1) lookup.
    # Returns a lookup map of user DID to their handle and condition.
    user_info: dict[str, dict[str, str]] = users.set_index("bluesky_user_did")[
        ["bluesky_handle", "condition"]
    ].to_dict("index")

    # CRITICAL DATA FLOW NOTE:
    # There's a subtle but important mismatch between user sets:
    # - `users` (user_info.keys()) contains ALL valid study users (e.g., 500 users)
    # - `user_per_day_content_label_metrics` only contains users who had engagements
    #   during the analysis period (e.g., 200-400 users depending on date range)
    #
    # This happens because:
    # 1. get_content_engaged_with_per_user() only creates entries for users with actual engagement records
    # 2. get_daily_engaged_content_per_user_metrics() only processes users from user_to_content_engaged_with
    # 3. Users with zero engagements (no likes, posts, reposts, replies) are missing from the metrics dict
    #
    # The double .get() safely handles missing users by returning empty dict → NaN columns in DataFrame
    # This is especially important for subset date ranges where more users have zero engagement activity.
    records: list[dict] = [
        {
            "handle": user_info[user_did]["bluesky_handle"],
            "condition": user_info[user_did]["condition"],
            "date": date,
            **user_per_day_content_label_metrics.get(user_did, {}).get(date, {}),
        }
        for user_did in user_info.keys()
        for date in partition_dates
    ]

    df = pd.DataFrame(records)
    return df.sort_values(by=["handle", "date"], inplace=False, ascending=[True, True])


# shared across both engagement and feed content analysis.
def transform_weekly_content_per_user_metrics(
    user_per_week_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    users: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the weekly engaged content per user metrics into the output format required.

    Similar format + logic as `transform_daily_content_per_user_metrics`,
    but now we're iterating through weeks instead of days.
    """

    # define what weeks we have in the study.
    weeks = user_date_to_week_df["week"].unique().tolist()
    weeks = [week for week in weeks if not pd.isna(week) and week is not None]
    weeks = sorted(weeks, key=str)

    if len(weeks) != TOTAL_STUDY_WEEKS_PER_USER:
        logger.warning(f"Expected 8 weeks, found {len(weeks)}: {weeks}")

    user_info: dict[str, dict[str, str]] = users.set_index("bluesky_user_did")[
        ["bluesky_handle", "condition"]
    ].to_dict("index")

    # CRITICAL DATA FLOW NOTE (same issue as daily transform):
    # `user_per_week_content_label_metrics` only contains users who had engagements,
    # but we iterate over ALL valid study users. The double .get() handles missing users safely.
    records: list[dict] = [
        {
            "handle": user_info[user_did]["bluesky_handle"],
            "condition": user_info[user_did]["condition"],
            "week": week,
            **user_per_week_content_label_metrics.get(user_did, {}).get(week, {}),
        }
        for user_did in user_info.keys()
        for week in weeks
    ]

    df = pd.DataFrame(records)
    return df.sort_values(by=["handle", "week"], inplace=False, ascending=[True, True])

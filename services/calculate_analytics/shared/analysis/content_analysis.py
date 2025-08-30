"""Module for content analysis."""

from typing import Optional

import numpy as np
import pandas as pd

from services.calculate_analytics.shared.processing.content_label_processing import (
    flatten_content_metrics_across_record_types,
    get_metrics_for_record_types,
    get_metrics_for_user_feeds_from_partition_date,
)


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

        # now that we've gone through all the feed content from the user,
        # per day, for a given user, we now add this to the hash map of
        # per-user, per-date feed content metrics.
        daily_feed_content_per_user_metrics[did] = partition_date_to_metrics_map

    return daily_feed_content_per_user_metrics


# TODO: note that we might have to account some days/weeks missing, e.g.,
# Wave 2 userse won't have feeds on the first week. That might have to
# be what's accounted for when we aggregate across weeks (though should
# be taken cared of when we figure out what week each user's feeds fit in).
# NOTE: actually I might not have to account for anything special since
# Week # will account for the difference in dates (e.g., irrespective of
# when Wave 1/2 starts, their dates will be accounted for in their week). Also
# accounts for the missing 2024-10-08 feeds since the other days will exist.
def get_weekly_feed_content_per_user_metrics(
    user_per_day_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    user_date_to_week_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Get the weekly feed content per user metrics.

    user_per_day_content_label_metrics: dict[str, dict[str, dict[str, float | None]]]
    user_date_to_week_df: pd.DataFrame

    Groups by user and week and then aggregates (averages) the values of the
    metrics across each week.

    Simpler averaging logic than `get_weekly_engaged_content_per_user_metrics`
    since every user will have a feed every day.
    """
    # TODO: remove (here for ruff linter)
    print(f"user_per_day_content_label_metrics: {user_per_day_content_label_metrics}")
    print(f"user_date_to_week_df: {user_date_to_week_df}")
    pass


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
    daily_engaged_content_per_user_metrics: dict[str, dict[str, Optional[float]]] = {}

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

            # add to the running per-user, per-date metrics map.
            partition_date_to_metrics_map[partition_date] = (
                per_user_per_date_content_metrics
            )

        # now that we've gone through all the engagement records from the users,
        # per day, for a given user, we now add this to the hash map of
        # per-user, per-date content engagement metrics.
        daily_engaged_content_per_user_metrics[did] = partition_date_to_metrics_map

    return daily_engaged_content_per_user_metrics


def get_weekly_engaged_content_per_user_metrics(
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
    weekly_engaged_content_per_user_metrics: dict[str, dict[str, Optional[float]]] = {}
    for user, dates_to_metrics_map in user_per_day_content_label_metrics.items():
        subset_user_date_to_week_df = user_date_to_week_df[
            user_date_to_week_df["bluesky_user_did"] == user
        ]
        subset_map_date_to_week = dict(
            zip(
                subset_user_date_to_week_df["date"],
                subset_user_date_to_week_df["week"],
            )
        )
        content_metrics_per_week_map: dict = {}

        # iterate through each date and their metrics and add it to a running
        # aggregate list of metrics for each of the weeks.
        # TODO: should refactor to a more generic aggregation function at
        # some point as I'll do something similar for feed content analysis,
        # but will revisit later.
        for date, metrics in dates_to_metrics_map.items():
            week = subset_map_date_to_week[date]

            # if week is not yet in `content_metrics_per_week_map`, initialize
            # the lists that will store each of the daily averages/proportions
            # for each of the labels.
            if week not in content_metrics_per_week_map:
                content_metrics_per_week_map[week] = {}
                for metric_name in metrics.keys():
                    content_metrics_per_week_map[week][metric_name] = []

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
            # if week is not yet in `content_metrics_per_week_map`, initialize
            # the map that will store each of the weekly averages/proportions
            # for each of the labels.
            if week not in weekly_engaged_content_per_user_metrics[user]:
                weekly_engaged_content_per_user_metrics[user][week] = {}

            # iterate through each label and its daily values for the given week
            # and average them out. Then add to the running map.
            for metric_name, daily_metric_values in daily_metrics_map.items():
                if len(daily_metric_values) == 0:
                    weekly_engaged_content_per_user_metrics[user][week][metric_name] = (
                        None
                    )
                else:
                    weekly_engaged_content_per_user_metrics[user][week][metric_name] = (
                        round(np.mean(daily_metric_values), 3)
                    )

    return weekly_engaged_content_per_user_metrics


# Shared across both engagement and feed content analysis.
def transform_daily_content_per_user_metrics(
    user_per_day_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    users_df: pd.DataFrame,
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

    users_df: pd.DataFrame of the users.
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
    records: list[dict] = []

    # some dates won't have records (e.g., a user might not have engaged with
    # content on some dates).

    # iterate through each user and their metrics.
    for user in users_df.to_dict(orient="records"):
        user_handle = user["bluesky_handle"]
        user_condition = user["condition"]
        user_did = user["bluesky_user_did"]
        metrics = user_per_day_content_label_metrics[user_did]

        # iterate through each date and their metrics.
        for date in partition_dates:
            if date not in metrics:
                # set this as empty dictionary. Then when we create the pandas
                # dataframe, the columns for this date will be NaN
                metrics[date] = {}
            day_metrics = metrics[date]
            default_fields = {
                "handle": user_handle,
                "condition": user_condition,
                "date": date,
            }
            hydrated_metrics = {**default_fields, **day_metrics}
            records.append(hydrated_metrics)

    # take the flattened records, where one record is the record for each
    # user + date combo, and convert to a pandas dataframe.
    df = pd.DataFrame(records)

    # impute any NaNs with None.
    df = df.fillna(None)

    # sort records.
    df = df.sort_values(by=["handle", "date"], inplace=False, ascending=[True, True])

    return df


# shared across both engagement and feed content analysis.
def transform_weekly_content_per_user_metrics(
    user_per_week_content_label_metrics: dict[str, dict[str, dict[str, float | None]]],
    users_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the weekly engaged content per user metrics into the output format required.

    Similar format + logic as `transform_daily_content_per_user_metrics`,
    but now we're iterating through weeks instead of days.
    """
    records: list[dict] = []

    # define what weeks we have in the study.
    weeks = user_date_to_week_df["week"].unique().tolist()
    weeks = [week for week in weeks if not pd.isna(week) and week is not None]
    weeks = sorted(weeks, key=str)
    print(f"Weeks in the study: {weeks}")
    assert (
        len(weeks) == 8
    )  # logical check to make sure we didn't accidentally remove values.

    # iterate through each user and their metrics.
    for user in users_df.to_dict(orient="records"):
        user_handle = user["bluesky_handle"]
        user_condition = user["condition"]
        user_did = user["bluesky_user_did"]
        weekly_metrics_for_user: dict[str, dict[str, float | None]] = (
            user_per_week_content_label_metrics[user_did]
        )

        # iterate through each week and their metrics.
        for week in weeks:
            if week not in weekly_metrics_for_user:
                weekly_metrics_for_user[week] = {}
            week_metrics = weekly_metrics_for_user[week]
            default_fields = {
                "handle": user_handle,
                "condition": user_condition,
                "week": week,
            }
            hydrated_metrics = {**default_fields, **week_metrics}
            records.append(hydrated_metrics)

    # take the flattened records, where one record is the record for each
    # user + week combo, and convert to a pandas dataframe.
    df = pd.DataFrame(records)

    # impute any NaNs with None.
    df = df.fillna(None)

    # sort records.
    df = df.sort_values(by=["handle", "week"], inplace=False, ascending=[True, True])

    return df

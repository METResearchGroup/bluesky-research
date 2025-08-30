"""Module for content analysis."""

from typing import Optional

import pandas as pd

from services.calculate_analytics.shared.processing.content_label_processing import (
    flatten_content_metrics_across_record_types,
    get_metrics_for_record_types,
)


# TODO: should be the logic in get_per_user_feed_averages_for_partition_date
# (but no need to add partition_date) - this can be used for both feed content
# analysis and engagement content analysis.


# this should calculate the averages and proportions for each of the labels.
def calculate_content_label_metrics(posts_df: pd.DataFrame) -> pd.DataFrame:
    """Takes as input the posts with each of their labels, and then calculates
    the metrics for each of the labels.
    """
    pass


def get_daily_feed_content_per_user_metrics():
    pass


def transform_daily_feed_content_per_user_metrics():
    pass


def get_weekly_feed_content_per_user_metrics():
    pass


def transform_weekly_feed_content_per_user_metrics():
    pass


def get_daily_engaged_content_per_user_metrics(
    user_to_content_engaged_with: dict[str, dict], labels_for_engaged_content: dict
) -> dict[str, dict[str, Optional[float]]]:
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

    NOTE: if I wanted to convert from proportion of posts with a particular
    label to instead measure probabilities (e.g., the average probability of
    a user's liked posts on 2024-10-01), that would be trivially easy
    (just have to remove the averaging step).
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


def transform_daily_engaged_content_per_user_metrics():
    pass


def get_weekly_engaged_content_per_user_metrics():
    pass


def transform_weekly_engaged_content_per_user_metrics():
    pass

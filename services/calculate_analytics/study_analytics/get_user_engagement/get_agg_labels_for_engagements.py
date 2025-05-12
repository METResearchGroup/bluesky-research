"""Get content classifications for engaged content, and summarizes
the per-user, per-week aggregate averages.

Writes a pandas df where the columns are a pairwise combination of:

- Engagement type: post, like, repost, reply
- Content class:
    - Perspective API: toxic, constructive
    - Sociopolitical: political, not political, left, moderate, right, unclear
    - IME: intergroup, moral, emotion, other

For example, for toxic, we'll have the following columns
- prop_liked_posts_toxic
- prop_posted_posts_toxic
- prop_replied_posts_toxic
- prop_reposted_posts_toxic

Creates 4 * 12 = 48 columns related to content classification.

In addition, we have a few more columns:
- Bluesky Handle
- Week
- Condition

We export this as a .csv file.

# TODO: should look into refactoring at some point with the feed averages logic,
as this'll mostly copy that anyways. But would be good to refactor for future
purposes.
"""

import json
import os

import numpy as np
import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import get_partition_dates
from services.calculate_analytics.study_analytics.generate_reports.weekly_user_logins import (
    load_user_date_to_week_df,
)
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

current_dir = os.path.dirname(os.path.abspath(__file__))
filename = "content_classifications_engaged_content_per_user_per_week.csv"
fp = os.path.join(current_dir, filename)


def get_content_engaged_with(
    record_type: str, valid_study_users_dids: set[str]
) -> dict:
    """Get the content that was engaged with, by record type.

    Returns a dict of the following format (for "likes", for example)

    {
        "<uri_1>": [
            {
                "did": "did_A", # user who liked it.
                "date": "<YYYY-MM-DD>", the date the user liked it.
                "record_type": "like"
            },
            {
                "did": "did_B", # user who liked it.
                "date": "<YYYY-MM-DD>", the date the user liked it.
                "record_type": "like"
            }
        ],
        "<uri_2>": [
            {
                "did": "did_C", # user who liked it.
                "date": "<YYYY-MM-DD>", the date the user liked it.
                "record_type": "like"
            },
            {
                "did": "did_D", # user who liked it.
                "date": "<YYYY-MM-DD>", the date the user liked it.
                "record_type": "like"
            }
        ]
    }
    """
    custom_args = {"record_type": record_type}
    df: pd.DataFrame = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        start_partition_date="2024-09-30",
        end_partition_date="2024-12-01",
        custom_args=custom_args,
    )

    df = df.drop_duplicates(subset=["uri"])
    df = df[df["author"].isin(valid_study_users_dids)]

    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date.astype(str)

    if record_type == "reply":
        # for simplicity's sake, we'll just take the URI of the post they replied to.
        df["engaged_post_uris"] = (
            df["reply"].apply(json.loads).apply(lambda x: x["parent"]["uri"])
        )
    else:
        df["engaged_post_uris"] = (
            df["subject"].apply(json.loads).apply(lambda x: x["uri"])
        )

    content_engaged_with = {}

    for _, row in df.iterrows():
        uri = row["engaged_post_uris"]
        if uri not in content_engaged_with:
            content_engaged_with[uri] = []
        content_engaged_with[uri].append(
            {
                "did": row["author"],
                "date": row["partition_date"],
                "record_type": record_type,
            }
        )

    return content_engaged_with


def get_engaged_content(valid_study_users_dids: set[str]):
    """Loads the content engaged with.

    Returns a map of the URIs of posts engaged with on that day and a list
    of who engaged with that record and what type of engagement.

    For example, let's say that we're loading the engagements from 2024-10-01.

    Let's say that user A and user B both liked post 1, and user B reposted
    shared user C's post, post 2. That would give us a result like:

    {
        "uri_1": [
            {
                "did": "did_A",
                "date": "<partition date>", # date they engaged with the content.
                "record_type": "like",
            },
            {
                "did": "did_B",
                "date": "<partition date>",
                "record_type": "like",
            }
        ],
        "uri_2": [
            {
                "did": "did_B",
                "date": "<partition date>",
                "record_type": "reply"
            },
            {
                "did": "did_C",
                "date": "<partition date>",
                "record_type": "post"
            }
        ]
    }

    We do this in 1 shot since the number of records is small enough to do so.

    Takes as input `valid_study_users_dids`, the set of study user DIDs. We
    only want engagements related to these users (e.g., we only, in this one,
    explicitly want the posts written by study users, for example. We have
    other posts, e.g., the posts that were liked, but we want to limit this
    to the records directly linked to study users).
    """
    map_uri_to_engagements = {}

    liked_content: dict = get_content_engaged_with(
        record_type="like", valid_study_users_dids=valid_study_users_dids
    )
    posted_content: dict = get_content_engaged_with(
        record_type="post", valid_study_users_dids=valid_study_users_dids
    )
    reposted_content: dict = get_content_engaged_with(
        record_type="repost", valid_study_users_dids=valid_study_users_dids
    )
    replied_content: dict = get_content_engaged_with(
        record_type="reply", valid_study_users_dids=valid_study_users_dids
    )

    for content in [liked_content, posted_content, reposted_content, replied_content]:
        for uri, engagements in content.items():
            if uri not in map_uri_to_engagements:
                map_uri_to_engagements[uri] = []
            map_uri_to_engagements[uri].extend(engagements)

    return map_uri_to_engagements


def get_content_engaged_with_per_user(engaged_content: dict):
    """Given the dict of engaged content, iterate and return a map
    of each user DID and all of their engaged content, by date.

    Returns a map of something like:

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
    """
    user_to_engagements_map = {}

    for uri, engagements in engaged_content.items():
        for engagement in engagements:
            did = engagement["did"]
            date = engagement["date"]
            record_type = engagement["record_type"]
            if did not in user_to_engagements_map:
                user_to_engagements_map[did] = {}
            if date not in user_to_engagements_map[did]:
                user_to_engagements_map[did][date] = {
                    "post": [],
                    "like": [],
                    "repost": [],
                    "reply": [],
                }
            user_to_engagements_map[did][date][record_type].append(uri)

    return user_to_engagements_map


def get_labels_for_partition_date(
    integration: str, partition_date: str
) -> pd.DataFrame:
    df = load_data_from_local_storage(
        service=f"ml_inference_{integration}",
        directory="cache",
        partition_date=partition_date,
    )
    df = df.drop_duplicates(subset=["uri"])
    return df


def get_relevant_probs_for_label(integration: str, label_dict: dict):
    if integration == "perspective_api":
        return {
            "prob_toxic": label_dict["prob_toxic"],
            "prob_constructive": label_dict["prob_constructive"],
        }
    elif integration == "sociopolitical":
        return {
            "is_sociopolitical": label_dict["is_sociopolitical"],
            "is_not_sociopolitical": not label_dict["is_sociopolitical"],
            # TODO: need to double-check the formatting of these labels.
            "is_political_left": (label_dict["political_ideology_label"] == "left"),
            "is_political_right": (label_dict["political_ideology_label"] == "right"),
            "is_political_moderate": (
                label_dict["political_ideology_label"] == "moderate"
            ),
            "is_political_unclear": (
                label_dict["political_ideology_label"] == "unclear"
            ),
        }
    elif integration == "ime":
        return {
            "prob_intergroup": label_dict["prob_intergroup"],
            "prob_moral": label_dict["prob_moral"],
            "prob_emotion": label_dict["prob_emotion"],
            "prob_other": label_dict["prob_other"],
        }
    else:
        raise ValueError(f"Invalid integration for labeling: {integration}")


def get_labels_for_engaged_content(uris: list[str]) -> dict:
    """For the content engaged with, get their associated labels.

    Algorithm:
        - Create a hash map:
            {
                "<uri>": {
                    "prob_toxic",
                    "prob_constructive",
                    ...
                }
            }

        - For each integration
            - For each partition date range
                - Load in the labels.
                - Filter for the subset of labels that are in the
                'engaged_content' keys.
                - Update the hash map with the labels from that integration.

    Result should look something like this:
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
    """
    uri_to_labels_map = {}

    uris_to_pending_integrations: dict[str, set[str]] = {}

    for uri in uris:
        uri_to_labels_map[uri] = {}
        # pop each of these from the set as they're hydrated.
        uris_to_pending_integrations[uri] = {"perspective_api", "sociopolitical", "ime"}

    # TODO: need to check the start_date. Probably need some lookback
    # (e.g., some of the posts liked on 2024-09-28 might've been written on
    # 2024-09-24. I should have a default lookback, maybe I'll do 2 weeks).
    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-15",
        end_date="2024-12-01",
        exclude_partition_dates=[],
    )

    for integration in ["perspective_api", "sociopolitical", "ime"]:
        # load day-by-day labels for each integration, and filter for only the
        # relevant URIs.
        filtered_uris = set()
        for partition_date in partition_dates:
            labels_df = get_labels_for_partition_date(
                integration=integration, partition_date=partition_date
            )
            labels_df = labels_df[labels_df["uri"].isin(uris)]
            labels = labels_df.to_dict(orient="records")
            for label_dict in labels:
                # get the labels formatted in the way that we care about.
                relevant_label_probs = get_relevant_probs_for_label(
                    integration=integration, label_dict=label_dict
                )
                post_uri = label_dict["uri"]
                filtered_uris.add(post_uri)
                uri_to_labels_map[post_uri] = {
                    **uri_to_labels_map[post_uri],
                    **relevant_label_probs,
                }

        # remove from list of pending integrations.
        for uri in filtered_uris:
            uris_to_pending_integrations[uri].remove(integration)
            if len(uris_to_pending_integrations[uri]) == 0:
                uris_to_pending_integrations.pop(uri)

    if len(uris_to_pending_integrations) > 0:
        print(
            f"We have {len(uris_to_pending_integrations)}/{len(uris)} still missing some integration of some sort."
        )
        integration_to_missing_uris = {}
        for uri, integrations in uris_to_pending_integrations.items():
            for integration in integrations:
                if integration not in integration_to_missing_uris:
                    integration_to_missing_uris[integration] = []
                integration_to_missing_uris[integration].append(uri)
        print(f"integration_to_missing_uris={integration_to_missing_uris}")

    return uri_to_labels_map


def get_column_prefix_for_record_type(record_type: str) -> str:
    if record_type == "like":
        prefix = "prop_liked_posts"
    elif record_type == "post":
        prefix = "prop_posted_posts"
    elif record_type == "repost":
        prefix = "prop_reposted_posts"
    elif record_type == "reply":
        prefix = "prop_replied_posts"
    else:
        raise ValueError(f"Invalid post type: {record_type}")
    return prefix


def get_per_user_per_day_content_label_proportions(
    user_to_content_engaged_with: dict[str, dict], labels_for_engaged_content: dict[str]
):
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
    per_user_per_day_content_label_proportions = {}

    for did, labels_per_day in user_to_content_engaged_with.items():
        per_user_per_day_content_label_proportions[did] = {}
        partition_date_to_proportions_map = {}
        for partition_date, record_types in labels_per_day.items():
            record_type_to_aggregated_labels_collection = {}
            aggregated_labels_across_record_types = {}
            for record_type in record_types:
                # for the given record type, collect all the posts associated
                # with it, then aggregate and calculate.

                # for example, collect all the labels associated with the
                # liked posts for did A for date 2024-10-01.

                uris = user_to_content_engaged_with[did][partition_date][record_type]
                labels_collection: set[str, list] = {
                    "prob_toxic": [],
                    "prob_constructive": [],
                    "is_sociopolitical": [],
                    "is_not_sociopolitical": [],
                    "is_political_left": [],
                    "is_political_right": [],
                    "is_political_moderate": [],
                    "is_political_unclear": [],
                    "prob_intergroup": [],
                    "prob_moral": [],
                    "prob_emotion": [],
                    "prob_other": [],
                }

                for uri in uris:
                    labels_for_uri = labels_for_engaged_content[uri]
                    for label, value in labels_for_uri.items():
                        if value is not None:
                            labels_collection[label].append(value)

                # now, we aggregate across each of these and get the proportion
                # of posts for the given DID + date + record type that have
                # the given label (e.g., proportion of posts liked by DID A
                # on 2024-10-01)

                aggregated_labels_collection = {
                    "prop_toxic": (
                        round(
                            (np.array(labels_collection["prob_toxic"]) > 0.5).mean(), 3
                        )
                        if len(labels_collection["prob_toxic"]) > 0
                        else None
                    ),
                    "prop_constructive": (
                        round(
                            (
                                np.array(labels_collection["prob_constructive"]) > 0.5
                            ).mean(),
                            3,
                        )
                        if len(labels_collection["prob_constructive"]) > 0
                        else None
                    ),
                    "prop_sociopolitical": (
                        round(np.mean(labels_collection["is_sociopolitical"]), 3)
                        if len(labels_collection["is_sociopolitical"]) > 0
                        else None
                    ),
                    "prop_is_not_sociopolitical": (
                        round(np.mean(labels_collection["is_not_sociopolitical"]), 3)
                        if len(labels_collection["is_not_sociopolitical"]) > 0
                        else None
                    ),
                    "prop_is_political_left": (
                        round(np.mean(labels_collection["is_political_left"]), 3)
                        if len(labels_collection["is_political_left"]) > 0
                        else None
                    ),
                    "prop_is_political_right": (
                        round(np.mean(labels_collection["is_political_right"]), 3)
                        if len(labels_collection["is_political_right"]) > 0
                        else None
                    ),
                    "prop_is_political_moderate": (
                        round(np.mean(labels_collection["is_political_moderate"]), 3)
                        if len(labels_collection["is_political_moderate"]) > 0
                        else None
                    ),
                    "prop_is_political_unclear": (
                        round(np.mean(labels_collection["is_political_unclear"]), 3)
                        if len(labels_collection["is_political_unclear"]) > 0
                        else None
                    ),
                    "prop_intergroup": (
                        round(
                            (
                                np.array(labels_collection["prob_intergroup"]) > 0.5
                            ).mean(),
                            3,
                        )
                        if len(labels_collection["prob_intergroup"]) > 0
                        else None
                    ),
                    "prop_moral": (
                        round(
                            (np.array(labels_collection["prob_moral"]) > 0.5).mean(), 3
                        )
                        if len(labels_collection["prob_moral"]) > 0
                        else None
                    ),
                    "prop_emotion": (
                        round(
                            (np.array(labels_collection["prob_emotion"]) > 0.5).mean(),
                            3,
                        )
                        if len(labels_collection["prob_emotion"]) > 0
                        else None
                    ),
                    "prop_other": (
                        round(
                            (np.array(labels_collection["prob_other"]) > 0.5).mean(), 3
                        )
                        if len(labels_collection["prob_other"]) > 0
                        else None
                    ),
                }

                record_type_to_aggregated_labels_collection[record_type] = (
                    aggregated_labels_collection
                )

            # now that we have the per-record type proportions
            # (e.g., proportion of posts liked by DID A on 2024-10-01),
            # we construct the fields for collecting the proportions across
            # record types.
            for (
                record_type,
                aggregated_labels_collection,
            ) in record_type_to_aggregated_labels_collection.items():
                prefix = get_column_prefix_for_record_type(record_type)

                for label, value in aggregated_labels_collection.items():
                    truncated_label_name = label.replace("prop_", "")
                    full_new_label_name = f"{prefix}_{truncated_label_name}"
                    aggregated_labels_across_record_types[full_new_label_name] = value

            # now we add this to the running per-DID, per-date proportions map.
            partition_date_to_proportions_map[partition_date] = (
                aggregated_labels_across_record_types
            )

        # now that we've gone through all the dates, add to per-did map.
        per_user_per_day_content_label_proportions[did] = (
            partition_date_to_proportions_map
        )

    return per_user_per_day_content_label_proportions


# TODO: account for the weeks where they don't have any engagement at all.
# TODO: need tests to account for this. Can just put 'None' for everything
# if they just don't have activity for those weeks.
def get_per_user_to_weekly_content_label_proportions(
    user_per_day_content_label_proportions: dict, user_date_to_week_df: pd.DataFrame
):
    """Takes the per-user, per-day content label measurements and calculates
    per-user, per-week measures.

    Our input looks something like this:

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

    We'll aggregate across days of a week to get the average for each proportion.
    First, we'll loop through each day and then create a DID to week map. We append
    each `prop_{column}` measure to a list:

    {
        "<did>": {
            "<week>": {
                "prop_posted_posts_toxic": [0.03, ...],
                "prop_liked_posts_toxic": [0.002, ...],
                "prop_reposted_posts_toxic": [None, ...],
                "prop_replied_posts_toxic": [None, ...],
                ...
                "prop_posted_posts_sociopolitical": [0.01, ...],
                "prop_liked_posts_sociopolitical": [0.05, ...],
                "prop_reposted_posts_sociopolitical": [None, ...],
                "prop_replied_posts_sociopolitical": [None, ...],
                ...
            }
        }
    }

    Then for each did + week combination, we average out each trait. We filter
    out any None values and then calculate the average across the remaining
    values (e.g., if there are 7 values and 5 are none, we add up the remaining
    2 and divide by 2 instead of by 7).

    If all the values are None (e.g., if for a given week, a user had 0 reposts,
    so they'll have 0 of any `prop_reposted_posts_{column}`), we will use None.

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
    user_to_weekly_content_label_proportions: dict = {}
    for (
        user,
        dates_to_content_proportions_map,
    ) in user_per_day_content_label_proportions.items():
        subset_user_date_to_week_df = user_date_to_week_df[
            user_date_to_week_df["bluesky_user_did"] == user
        ]
        subset_map_date_to_week = dict(
            zip(
                subset_user_date_to_week_df["date"],
                subset_user_date_to_week_df["week"],
            )
        )
        content_proportions_per_week_map: dict = {}

        # aggregate records across days. Similar as aggregation process done
        # at the daily level, but now done across days and now is just
        # an np.mean() since we want, for example, the average proportion
        # of toxic liked posts for the given week.
        # at this step, should look like this. We preemptively avoid adding None
        # values so that we don't have to do a filter step during aggregation.
        # {
        #   "did": {
        #       "<week>": {
        #           "<label_1>": [0.001, 0.0003],
        #           "<label_2>": [],
        #           ...
        #       }
        #   }
        # }
        for (
            date,
            daily_props_across_record_types,
        ) in dates_to_content_proportions_map.items():
            week = subset_map_date_to_week[date]
            if week not in content_proportions_per_week_map:
                aggregated_weekly_labels_across_record_types = {}
                for label in daily_props_across_record_types:
                    aggregated_weekly_labels_across_record_types[label] = []
                content_proportions_per_week_map[week] = (
                    aggregated_weekly_labels_across_record_types
                )

            for label, prop in daily_props_across_record_types.items():
                if prop is not None:
                    content_proportions_per_week_map[week][label].append(prop)

        # now we take the averages. The output should be something like.
        #   "did": {
        #       "<week>": {
        #           "<label_1>": 0.001,
        #           "<label_2>": None, # if no values to average, return None.
        #           ...
        #       }
        #   }
        # }
        aggregated_content_proportions_per_week = {}

        for week, content_proportions in content_proportions_per_week_map.items():
            if week not in aggregated_content_proportions_per_week:
                aggregated_content_proportions_per_week[week] = {}
            for label, values in content_proportions.items():
                if len(values) == 0:
                    aggregated_content_proportions_per_week[week][label] = None
                else:
                    aggregated_content_proportions_per_week[week][label] = round(
                        np.mean(values), 3
                    )

        user_to_weekly_content_label_proportions[user] = (
            aggregated_content_proportions_per_week
        )

    return user_to_weekly_content_label_proportions


# TODO: need to account for users that have 0 engagements on a given week,
# as well as users that have no engagement for the entire study.
def transform_per_user_to_weekly_content_label_proportions(
    user_to_weekly_content_label_proportions: dict, users: list[dict]
) -> pd.DataFrame:
    """Transforms the aggregated label statistics into the output format required.

    Iterates through each of the users and gets their data
    """
    flattened_metrics_per_user_per_week: list[dict] = []
    for user in users:
        user_handle = user["bluesky_handle"]
        user_condition = user["condition"]
        user_did = user["bluesky_user_did"]
        weeks_to_metrics_map = user_to_weekly_content_label_proportions[user_did]
        for week, metrics in weeks_to_metrics_map.items():
            flattened_metrics_per_user_per_week.append(
                {
                    "handle": user_handle,
                    "condition": user_condition,
                    "week": week,
                    **metrics,
                }
            )
    return pd.DataFrame(flattened_metrics_per_user_per_week)


def main():
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_df: pd.DataFrame = pd.DataFrame([user.model_dump() for user in users])

    user_date_to_week_df = load_user_date_to_week_df()

    # we base the valid users on the Qualtrics logs. We load the users from
    # DynamoDB but some of those users aren't valid (e.g., they were
    # experimental users, they didn't do the study, etc.).
    valid_study_users_dids = set(user_date_to_week_df["bluesky_user_did"].unique())
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]
    user_df = user_df[user_df["bluesky_user_did"].isin(valid_study_users_dids)]
    users = user_df.to_dict(orient="records")

    # get all content engaged with by study users, keyed on the URI of the post.
    engaged_content: dict[str, list[dict]] = get_engaged_content(
        valid_study_user_dids=valid_study_users_dids
    )
    print(
        f"Total # of posts engaged with in some way (e.g., like/post/repost/reply): {len(engaged_content)}"
    )

    # get all the content engaged with by user, keyed on user DID.
    # keyed on user, value = another dict, with key = date, value = engagements (like/post/repost/reply)
    user_to_content_engaged_with: dict[str, dict] = get_content_engaged_with_per_user(
        engaged_content=engaged_content
    )

    print(
        f"Total number of users who have engaged with content on Bluesky: {len(user_to_content_engaged_with)}/{len(valid_study_users_dids)} total valid users."
    )

    labels_for_engaged_content: dict[str, dict] = get_labels_for_engaged_content(
        uris=engaged_content.keys()
    )

    print(
        f"Loaded labels for {len(labels_for_engaged_content)} post URIs (out of {len(engaged_content)} total content engaged with)."
    )

    user_per_day_content_label_proportions = (
        get_per_user_per_day_content_label_proportions(
            user_to_content_engaged_with=user_to_content_engaged_with,
            labels_for_engaged_content=labels_for_engaged_content,
        )
    )

    user_to_weekly_content_label_proportions = get_per_user_to_weekly_content_label_proportions(
        user_per_day_content_label_proportions=user_per_day_content_label_proportions,
        user_date_to_week_df=user_date_to_week_df,
    )

    transformed_per_user_to_weekly_content_label_proportions = transform_per_user_to_weekly_content_label_proportions(
        user_to_weekly_content_label_proportions=user_to_weekly_content_label_proportions,
        users=users,
    )

    transformed_per_user_to_weekly_content_label_proportions.to_csv(fp, index=False)


if __name__ == "__main__":
    main()

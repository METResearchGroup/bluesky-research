"""Get content classifications for engaged content, and summarizes
the per-user, per-week aggregate averages.

Writes a pandas df where the columns are a pairwise combination of:

- Engagement type: post, like, repost, reshare
- Content class:
    - Perspective API: toxic, constructive
    - Sociopolitical: political, not political, left, moderate, right, unclear
    - IME: intergroup, moral, emotion, other

For example, for toxic, we'll have the following columns
- prop_liked_posts_toxic
- prop_posted_posts_toxic
- prop_reshared_posts_toxic
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

First, we need to get the posts that users engaged with.
    - For a partition date:
        # load the user engagement records
        - Create a local hash map, for the given date and across all record types,
        of URI to user handle. This'll help with the lookup, and each data type
        will have a type-specific URI anyways.
            - Something like:
                {
                    "<uri>": [
                        {
                            "handle": "<handle_1>",
                            "timestamp": "<YYYY-MM-DD engaged with>",
                            "record_type": "<record_type>"
                        },
                        ...
                        {
                            "handle": "<handle_2>",
                            "timestamp": "<YYYY-MM-DD engaged with>",
                            "record_type": "<record_type>"
                        },
                        ...
                    ]
                }
            - This needs to be the URI of the post itself, not just the record.
            For example, for a like, it shouldn't be the like URI, but rather
            the URI of the post that was liked.
            - This needs to be a list of dicts, since multiple users can like
            the same post, for example.
            - It can be possible that the same post URI is referenced in different
            ways, i.e., for a given post, user A can like it and user B can repost
            it, so we want to account for these accordingly.
            - We need to record when each user engaged with the post, so that
            we can timestamp their activity correctly.

        - Create a map of handle to engagements per day.
            - Something like:
                {
                    "handle": {
                        "<date>": {
                            "post": [],
                            "like": [],
                            "repost": [],
                            "reshare": []
                        }
                    }
                }

        - For each record type:
            - Load all posts/likes/reposts/reshares from `raw_sync`
            - Filter for those whose author is in the study.
            - Update the local hash map accordingly.
                - Again, URI of the post should be used as the key.

        At this point, we'll have a local hash map of all the post URIs that have
        corresponding engagements (e.g., all the posts that were liked on
        2024-10-01).

        Then, we need to get the posts liked/reposted/shared. We can have a default
        lookback logic (e.g,. 7 days) to try to get the base post that was
        liked/reposted/reshared.

        Export these, something like 'posts_engaged_with', with just the URI
        and timestamp, partitioned by 'partition_date'.

        This gives, for each partition date, the posts written in those days
        that were then interacted with at some point.

After having the 'posts_engaged_with', we now hydrate those with the labels.
    - Create a hash map for posts to labels:
        - Something like:
            {
                "<uri>": {
                    "prob_toxic",
                    "prob_constructive",
                    ...
                }
            }

        I should be able to just load this in memory across all
    - For each partition date.
        - Load all the posts engaged with
        - Load all the labels
            - For each integration:
                - Load all the records for that day.
                - Filter for the URIs that exist in the hash map of URIs
                engaged with.

    - At the end, we should have a global hash map of all posts engaged with,
    whose key = URI and whose values are the labels.

Then, we loop through the Bluesky handles and start calculating the averages.
    - The format that we start with is:
        {
            "handle": {
                "<date>": {
                    "post": [],
                    "like": [],
                    "repost": [],
                    "reshare": []
                }
            }
        }
    - We loop through all of these.
        - For handle in handles:
            For date in dates:
                For record_type in ["post", "like", "repost", "reshare"]:
                    - Do a lookup of URI to hash map, to get the labels:
                        {
                            "<uri>": {
                                "prob_toxic",
                                "prob_constructive",
                                ...
                            }
                        }
                    - Calculate the proportion of each label per date.

                - For a given date, the output should be something like:
                    {
                        "<date>": {
                            "prop_liked_posts_toxic",
                            "prop_posted_posts_toxic",
                            "prop_reshared_posts_toxic",
                            "prop_reposted_posts_toxic",
                            ...
                            "prop_liked_posts_is_political",
                            "prop_posted_posts_is_political",
                            "prop_reshared_posts_is_political",
                            "prop_reposted_posts_is_political"
                        }
                    }

                - If there are no posts, i.e., for handle X on 2024-10-01, they
                didn't have any shared posts, we default to None. We don't want
                to default to 0 since a p=0 has an actual meaning.

    - We now have something of the shape:
        {
            "<handle>": {
                "<date>": {
                    "prop_liked_posts_toxic",
                    "prop_posted_posts_toxic",
                    "prop_reshared_posts_toxic",
                    "prop_reposted_posts_toxic",
                    ...
                    "prop_liked_posts_is_political",
                    "prop_posted_posts_is_political",
                    "prop_reshared_posts_is_political",
                    "prop_reposted_posts_is_political"
                }
            }
        }

    - Now we start aggregating per week:
        - We load the hash map of user, day, and week.
            - subset_user_date_to_week_df = user_date_to_week_df[
                user_date_to_week_df["bluesky_handle"] == user
            ]
            -  subset_map_date_to_week = dict(
                    zip(
                        subset_user_date_to_week_df["date"],
                        subset_user_date_to_week_df["week"],
                    )
                )
        - We create a hash map of the following format, where we append
        the proportions across each day.
            {
                "<handle>": {
                    "week_1": {
                        "prop_liked_posts_toxic": [],
                        "prop_posted_posts_toxic": [],
                        "prop_reshared_posts_toxic": [],
                        "prop_reposted_posts_toxic": [],
                        ...
                        "prop_liked_posts_is_political": [],
                        "prop_posted_posts_is_political": [],
                        "prop_reshared_posts_is_political": [],
                        "prop_reposted_posts_is_political": []
                    },
                    ...
                }
            }
        - For each partition date:
            - We get the corresponding week.
            - We append the proportions across each day to the hash map.

        - We now have our hydrated hash map. We then iterate through each week
        and find the average for that week
            - We exclude any None/NaNs. If the resulting array is empty,
            we return Nan.

            - The output can be something like this:
                {
                    "<handle>": {
                        "week_1": {
                            "prop_liked_posts_toxic": 0.0001,
                            "prop_posted_posts_toxic": None,
                            "prop_reshared_posts_toxic": None,
                            "prop_reposted_posts_toxic": 0.0005,
                            ...
                            "prop_liked_posts_is_political": 0.002,
                            "prop_posted_posts_is_political": None,
                            "prop_reshared_posts_is_political": None,
                            "prop_reposted_posts_is_political": None
                        },
                        ...
                    }
                }

Now, we flatten.
    - Take each combination of handle + week and make that a separate row.
    - Input:
        {
            "<handle>": {
                "week_1": {
                    "prop_liked_posts_toxic": 0.0001,
                    "prop_posted_posts_toxic": None,
                    "prop_reshared_posts_toxic": None,
                    "prop_reposted_posts_toxic": 0.0005,
                    ...
                    "prop_liked_posts_is_political": 0.002,
                    "prop_posted_posts_is_political": None,
                    "prop_reshared_posts_is_political": None,
                    "prop_reposted_posts_is_political": None
                },
                "week_2": {
                    "prop_liked_posts_toxic": 0.0001,
                    "prop_posted_posts_toxic": None,
                    "prop_reshared_posts_toxic": None,
                    "prop_reposted_posts_toxic": 0.0005,
                    ...
                    "prop_liked_posts_is_political": 0.002,
                    "prop_posted_posts_is_political": None,
                    "prop_reshared_posts_is_political": None,
                    "prop_reposted_posts_is_political": None
                },
                ...
            }
        }
    - Output:
        [
            {
                "handle": "<handle>",
                "week": 1,
                "prop_liked_posts_toxic": 0.0001,
                "prop_posted_posts_toxic": None,
                "prop_reshared_posts_toxic": None,
                "prop_reposted_posts_toxic": 0.0005,
                ...
                "prop_liked_posts_is_political": 0.002,
                "prop_posted_posts_is_political": None,
                "prop_reshared_posts_is_political": None,
                "prop_reposted_posts_is_political": None
            },
            {
                "handle": "<handle>",
                "week": 2,
                "prop_liked_posts_toxic": 0.0001,
                "prop_posted_posts_toxic": None,
                "prop_reshared_posts_toxic": None,
                "prop_reposted_posts_toxic": 0.0005,
                ...
                "prop_liked_posts_is_political": 0.002,
                "prop_posted_posts_is_political": None,
                "prop_reshared_posts_is_political": None,
                "prop_reposted_posts_is_political": None
            }
        ]

    - Then we convert this to a pandas dataframe and export.

"""

import json
import os

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
    df = df[df["author"]].isin(valid_study_users_dids)

    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date

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
                "record_type": "reshare"
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
    reshared_content: dict = get_content_engaged_with(
        record_type="reshare", valid_study_users_dids=valid_study_users_dids
    )

    for content in [liked_content, posted_content, reposted_content, reshared_content]:
        for uri, engagements in content.items():
            if uri not in map_uri_to_engagements:
                map_uri_to_engagements[uri] = []
            map_uri_to_engagements[uri].extend(engagements)

    return map_uri_to_engagements


def get_agg_labels_content_per_user_per_day():
    pass


def get_agg_labels_content_per_user_per_week():
    pass


def transform_labeled_content_per_user_per_week():
    pass


def main():
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_df: pd.DataFrame = pd.DataFrame([user.model_dump() for user in users])
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]
    users = user_df.to_dict(orient="records")

    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-30",
        end_date="2024-12-01",
        exclude_partition_dates=[],
    )

    print(f"partition_dates: {partition_dates}")

    user_date_to_week_df = load_user_date_to_week_df()
    valid_study_users_dids = set(user_date_to_week_df["bluesky_user_did"].unique())

    engaged_content: dict[str, list[dict]] = get_engaged_content(
        valid_study_user_dids=valid_study_users_dids
    )
    print(f"Total number of engaged content: {len(engaged_content)}")

    transformed_agg_labeled_engaged_content_per_user_per_week: pd.DataFrame = (
        pd.DataFrame()
    )
    transformed_agg_labeled_engaged_content_per_user_per_week.to_csv(fp, index=False)


if __name__ == "__main__":
    main()

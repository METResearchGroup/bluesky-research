"""
Load content engaged with by study users.
- Posts written by study users.
    - Posts
    - Replies
    - Shares/Retweets
- Likes: posts liked by study users
"""

import gc
import json

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)


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

    We filter here for 'valid_study_users_dids' instead of later on since doing
    it later on would require traversing every key to prune the corresponding
    author DID. Instead of doing that, we prune proactively.
    """
    custom_args = {"record_type": record_type}
    df: pd.DataFrame = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        start_partition_date=STUDY_START_DATE,
        end_partition_date=STUDY_END_DATE,
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
    elif record_type == "post":
        # for posts, since the author wrote the post itself, we can just
        # grab the actual post URI.
        df["engaged_post_uris"] = df["uri"]
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
    of who engaged with that record and what type of engagement. We do it in this
    way because a given post can be engaged with in different ways by different
    users (and can also be engaged with in different ways by the same user).

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

    del liked_content, posted_content, reposted_content, replied_content
    gc.collect()

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

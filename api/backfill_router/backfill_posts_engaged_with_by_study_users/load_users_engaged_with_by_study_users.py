"""Load users whose posts were either liked or reposted or replied to by
someone in the study.
"""

import json
import os

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.helper import create_batches
from lib.log.logger import get_logger
from transform.transform_raw_data import get_author_did_from_post_uri

service = "raw_sync"
logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_details_of_posts_liked_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were liked by study users.

    Loads likes and then saves the URI of the liked posts as well as the
    DID of the users who wrote those posts that were liked.
    """
    custom_args = {"record_type": "like"}
    logger.info("Loading likes data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])

    logger.info(f"(Data type: likes) Total number of likes across all users: {len(df)}")

    # get the URIs of the posts that were liked.
    liked_post_uris: pd.Series = (
        df["subject"].apply(json.loads).apply(lambda x: x["uri"])
    )

    # get the DIDs of the users who wrote those posts.
    liked_post_author_dids: pd.Series = liked_post_uris.apply(
        get_author_did_from_post_uri
    ).unique()
    liked_post_author_dids: set[str] = set(liked_post_author_dids)

    dids: set[str] = set(liked_post_author_dids)
    uris: set[str] = set(liked_post_uris)

    logger.info(
        f"(Data type: likes) Found {len(dids)} unique DIDs and {len(uris)} unique URIs"
    )

    return dids, uris


def get_details_of_posts_reposted_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were reposted by study users.

    Loads reposts and then saves the URI of the reposted posts as well as the
    DID of the users who wrote those posts that were reposted.
    """
    custom_args = {"record_type": "repost"}
    logger.info("Loading reposts data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])

    logger.info(
        f"(Data type: reposts) Total number of reposts across all users: {len(df)}"
    )

    # get the URIs of the posts that were reposted.
    reposted_post_uris: pd.Series = (
        df["subject"].apply(json.loads).apply(lambda x: x["uri"])
    )

    # get the DIDs of the users who wrote those posts.
    reposted_post_author_dids: pd.Series = reposted_post_uris.apply(
        get_author_did_from_post_uri
    ).unique()
    reposted_post_author_dids: set[str] = set(reposted_post_author_dids)

    dids: set[str] = set(reposted_post_author_dids)
    uris: set[str] = set(reposted_post_uris)

    logger.info(
        f"(Data type: reposts) Found {len(dids)} unique DIDs and {len(uris)} unique URIs"
    )

    return dids, uris


def get_details_of_posts_replied_to_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were replied to by study users.

    Loads replies and then saves the (1) URIs of the posts in the parent
    and root posts in the thread, and (2) the DIDs of the users who wrote
    those posts.
    """
    custom_args = {"record_type": "reply"}
    logger.info("Loading replies data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])

    logger.info(
        f"(Data type: replies) Total number of replies across all users: {len(df)}"
    )

    # get the URIs and DIDs of the parent and root posts in the thread.
    thread_details: list[dict] = (
        df["reply"]
        .apply(json.loads)
        .apply(
            lambda x: {
                "parent_post_uri": x["parent"]["uri"],
                "root_post_uri": x["root"]["uri"],
                "parent_post_author_did": get_author_did_from_post_uri(
                    x["parent"]["uri"]
                ),
                "root_post_author_did": get_author_did_from_post_uri(x["root"]["uri"]),
            }
        )
        .tolist()
    )

    uris: set[str] = set()
    dids: set[str] = set()
    for thread_detail in thread_details:
        uris.add(thread_detail["parent_post_uri"])
        uris.add(thread_detail["root_post_uri"])
        dids.add(thread_detail["parent_post_author_did"])
        dids.add(thread_detail["root_post_author_did"])

    logger.info(
        f"(Data type: replies) Found {len(dids)} unique DIDs and {len(uris)} unique URIs"
    )

    return dids, uris


def main():
    dids_of_liked_posts, uris_of_liked_posts = (
        get_details_of_posts_liked_by_study_users()
    )
    dids_of_reposted_posts, uris_of_reposted_posts = (
        get_details_of_posts_reposted_by_study_users()
    )
    dids_of_replied_posts, uris_of_replied_posts = (
        get_details_of_posts_replied_to_by_study_users()
    )

    dids_of_users_engaged_with = (
        dids_of_liked_posts | dids_of_reposted_posts | dids_of_replied_posts
    )
    uris_of_posts_engaged_with = (
        uris_of_liked_posts | uris_of_reposted_posts | uris_of_replied_posts
    )

    logger.info(
        f"Total number of users engaged with: {len(dids_of_users_engaged_with)}"
    )
    logger.info(
        f"Total number of posts engaged with: {len(uris_of_posts_engaged_with)}"
    )

    # export the DIDs to the SQLite queue and the URIs to
    # a parquet file.
    batch_user_dids = create_batches(list(dids_of_users_engaged_with), 100)
    items: list[dict] = [{"dids": user_dids} for user_dids in batch_user_dids]
    queue_path = os.path.join(
        current_dir, "backfill_user_dids_engaged_with_by_study_users.sqlite"
    )
    queue = Queue(
        queue_name="backfill_user_dids_engaged_with_by_study_users",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=queue_path,
    )
    queue.batch_add_items_to_queue(items=items)
    logger.info(f"Wrote {len(dids_of_users_engaged_with)} user DIDs to queue")

    # export the URIs to a parquet file.
    uris_df = pd.DataFrame({"uri": list(uris_of_posts_engaged_with)})
    uris_path = os.path.join(
        current_dir, "backfill_post_uris_engaged_with_by_study_users.parquet"
    )
    uris_df.to_parquet(uris_path)
    logger.info(f"Wrote {len(uris_of_posts_engaged_with)} post URIs to parquet")


if __name__ == "__main__":
    main()

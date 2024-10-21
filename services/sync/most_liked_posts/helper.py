"""Helper functions for getting most liked Bluesky posts in the past day/week."""  # noqa

from datetime import timedelta
import json
import os
from typing import Union

from atproto_client.models.app.bsky.feed.defs import FeedViewPost
import pandas as pd

from lib.aws.glue import Glue
from lib.aws.s3 import S3, SYNC_KEY_ROOT
from lib.aws.sqs import SQS
from lib.constants import current_datetime_str, current_datetime
from lib.db.bluesky_models.transformations import (
    TransformedFeedViewPostModel,
    TransformedRecordModel,
)
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.consolidate_post_records.helper import consolidate_feedview_post
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_language.helper import record_is_english  # noqa
from transform.bluesky_helper import get_posts_from_custom_feed_url
from transform.transform_raw_data import transform_feedview_posts

glue = Glue()
s3 = S3()
sqs = SQS("mostLikedSyncsToBeProcessedQueue")

logger = get_logger(__name__)


feed_to_info_map = {
    "today": {
        "description": "Most popular posts from the last 24 hours",
        "url": "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up",  # noqa
    },
    "week": {
        "description": "Most popular posts from the last 7 days",
        "url": "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up-weekly",  # noqa
    },
    "verified_news": {
        "description": "Headlines from verified news organisations. Maintained by @aendra.com.",  # noqa
        # "url": "https://bsky.app/profile/aendra.com/feed/verified-news",  # noqa
        "url": "https://bsky.app/profile/did:plc:kkf4naxqmweop7dv4l2iqqf5/feed/verified-news",  # noqa (ran `get_author_did_from_handle` from handle)
        "limit": 200,
    },
    "what's reposted": {
        "description": "Frequently reposted posts from the whole network",
        # "url": "https://bsky.app/profile/skyfeed.xyz/feed/whats-reposted",  # noqa
        "url": "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/whats-reposted",  # noqa
        "limit": 200,
    },
    "Top (1h)": {
        "description": "The most popular posts in the last hour",
        "url": "https://bsky.app/profile/q6gjnaw2blty4crticxkmujt/feed/top-1h",
        "limit": 400
    },
    "Top (24h)": {
        "description": "The most popular posts in the last day",
        "url": "https://bsky.app/profile/q6gjnaw2blty4crticxkmujt/feed/top-24h",
        "limit": 400
    },
    "What's Hot": {
        "description": "A copy of the old What's Hot feed, maintained by jaz.bsky.social",
        "url": "https://bsky.app/profile/q6gjnaw2blty4crticxkmujt/feed/whats-hot",
        "limit": 400
    },
    "US Politics": {
        "description": "US Politics",
        # "url": "https://bsky.app/profile/itsonelouder.com/feed/aaadhh6hwvaca",
        "url": "https://bsky.app/profile/did:plc:7mtqkeetxgxqfyhyi2dnyga2/feed/aaadhh6hwvaca",
        "limit": 200,
    },
}

current_directory = os.path.dirname(os.path.abspath(__file__))
sync_dir = "syncs"
full_sync_dir = os.path.join(current_directory, sync_dir)
# make new path if it doesn't exist and if not running in a lambda container.
if not os.path.exists(full_sync_dir) and not os.path.exists("/app"):
    os.makedirs(full_sync_dir)
sync_fp = os.path.join(full_sync_dir, f"most_liked_posts_{current_datetime_str}.jsonl")
urls_fp = os.path.join(full_sync_dir, f"urls_{current_datetime_str}.csv")

max_days_age_of_post = 2

# task_name = "get_most_liked_posts"
# mongo_collection_name, mongo_collection = (
#     get_mongodb_collection(task=task_name)
# )

root_most_liked_s3_key = os.path.join(SYNC_KEY_ROOT, "most_liked_posts")

# TODO: probably should move this elsewhere at some point.


def post_passed_filters(post: TransformedFeedViewPostModel) -> bool:
    record: TransformedRecordModel = post.record
    return record_is_english(record=record)


def filter_posts(
    posts: list[TransformedFeedViewPostModel],
) -> list[TransformedFeedViewPostModel]:
    """Filter the posts."""
    return list(filter(post_passed_filters, posts))


def get_and_transform_latest_most_liked_posts(
    feeds: list[str] = ["today", "week"],
) -> list[TransformedFeedViewPostModel]:
    """Get the latest batch of most liked posts and transform them."""
    res: list[TransformedFeedViewPostModel] = []
    for feed in feeds:
        feed_url = feed_to_info_map[feed]["url"]
        limit = feed_to_info_map[feed].get("limit", None)
        enrichment_data = {"source_feed": feed, "feed_url": feed_url}
        print(
            f"Getting most liked posts from {feed} feed with URL={feed_url}, limit={limit}"
        )
        posts: list[FeedViewPost] = get_posts_from_custom_feed_url(
            feed_url=feed_url, limit=limit
        )
        transformed_posts: list[TransformedFeedViewPostModel] = (
            transform_feedview_posts(posts=posts, enrichment_data=enrichment_data)
        )
        res.extend(transformed_posts)
        print(f"Finished processing {len(posts)} posts from {feed} feed")

    # Deduplicate the results based on the "uri" field
    unique_posts = {post.uri: post for post in res}
    res: list[TransformedFeedViewPostModel] = list(unique_posts.values())

    # get only posts that are more recent.
    min_date = (current_datetime - timedelta(days=max_days_age_of_post)).strftime(
        "%Y-%m-%d"
    )
    res: list[TransformedFeedViewPostModel] = [
        post for post in res if post.record.created_at >= min_date
    ]
    return res


def export_posts(
    posts: Union[list[ConsolidatedPostRecordModel], list[dict]],
    store_local: bool = True,
    store_remote: bool = False,
    send_sqs_message: bool = False,
) -> None:
    """Export the posts to a file, either locally as a JSON or remote in S3.

    Also sends a message to the SQS queue if the posts are stored remotely, so
    that the posts are processed by the next service.
    """
    timestamp_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=current_datetime_str
    )
    if isinstance(posts[0], ConsolidatedPostRecordModel):
        consolidated_post_dicts = [post.dict() for post in posts]
    else:
        consolidated_post_dicts = posts
    if store_local:
        dtypes_map = MAP_SERVICE_TO_METADATA["sync_most_liked_posts"]["dtypes_map"]
        df = pd.DataFrame(data=consolidated_post_dicts)
        df = df.astype(dtypes_map)  # oddly, doing dtype=dtypes_map doesn't work
        export_data_to_local_storage(
            service="sync_most_liked_posts", df=df, export_format="parquet"
        )
        logger.info(f"Exported {len(posts)} posts to local storage")
    if store_remote:
        filename = "posts.jsonl"
        full_key = os.path.join(root_most_liked_s3_key, timestamp_key, filename)
        print(f"Exporting most liked posts to S3 at {full_key}")
        s3.write_dicts_jsonl_to_s3(data=consolidated_post_dicts, key=full_key)
        print(f"Exported {len(posts)} posts to S3 at {full_key}")
    if send_sqs_message and store_remote:
        # I initially had this as an SQS message, but I had trouble implementing
        # SQS so this'll just be written to S3.
        sqs_data_payload = {
            "sync": {"source": "most_liked_feed", "s3_keys": [full_key]}
        }
        s3.send_queue_message(source="most_liked", data=sqs_data_payload)
        # glue.start_crawler("queue_messages_crawler")


def load_most_recent_local_syncs(n_latest_local: int = 1) -> list[dict]:
    """Load the most recent local sync file."""
    sync_files = os.listdir(os.path.join(current_directory, sync_dir))
    posts: list[dict] = []
    most_recent_filenames = sorted(sync_files)[-n_latest_local:]
    for filename in most_recent_filenames:
        sync_fp = os.path.join(current_directory, sync_dir, filename)
        print(f"Reading most recent sync file {sync_fp}")
        with open(sync_fp, "r") as f:
            posts.extend([json.loads(line) for line in f])
    return posts


def filter_most_recent_local_syncs(n_latest_local: int = 1) -> list[dict]:
    """Filter the most recent local sync files."""
    posts: list[dict] = load_most_recent_local_syncs(n_latest_local=n_latest_local)
    transformed_posts: list[TransformedFeedViewPostModel] = [
        TransformedFeedViewPostModel(**post) for post in posts
    ]
    filtered_posts: list[TransformedFeedViewPostModel] = filter_posts(
        posts=transformed_posts
    )
    return [post.dict() for post in filtered_posts]


def dump_most_recent_local_sync_to_remote() -> None:
    """Dump the most recent local sync to the remote MongoDB collection."""
    posts: list[dict] = load_most_recent_local_syncs()
    export_posts(posts=posts, store_local=False, store_remote=True)


def main(
    use_latest_local: bool = False,
    n_latest_local: int = 1,
    store_local: bool = True,
    store_remote: bool = True,
    feeds: list[str] = ["today", "week"],
) -> None:
    if use_latest_local:
        post_dicts: list[dict] = filter_most_recent_local_syncs(
            n_latest_local=n_latest_local
        )
    else:
        posts: list[TransformedFeedViewPostModel] = (
            get_and_transform_latest_most_liked_posts(feeds=feeds)
        )
        print(f"Loaded {len(posts)} total posts before filtering.")
        filtered_posts: list[TransformedFeedViewPostModel] = filter_posts(posts=posts)  # noqa
        consolidated_posts: list[ConsolidatedPostRecordModel] = [
            consolidate_feedview_post(post) for post in filtered_posts
        ]
        post_dicts = [post.dict() for post in consolidated_posts]
        post_dicts = [
            {**post, "embed": json.dumps(post["embed"])} for post in post_dicts
        ]
        print(f"Exporting {len(post_dicts)} total posts...")
    # NOTE: the type of the new_posts is dict but it's actually
    # the ConsolidatedPostRecordModel as a dict.
    export_posts(posts=post_dicts, store_local=store_local, store_remote=store_remote)


if __name__ == "__main__":
    # posts: list[TransformedFeedViewPostModel] = (
    #     get_and_transform_latest_most_liked_posts()
    # )
    # filtered_posts = filter_posts(posts=posts)
    # post_dicts = [post.dict() for post in filtered_posts]
    # export_posts(posts=post_dicts, store_local=True, store_remote=False)
    # export_posts(posts=post_dicts, store_local=True, store_remote=True)
    # dump_most_recent_local_sync_to_remote()

    # post_dicts: list[dict] = filter_most_recent_local_sync()
    # post_dicts = [post.dict() for post in filtered_posts]
    # export_posts(
    #     posts=post_dicts, store_local=True, store_remote=True,
    #     bulk_write_remote=True
    # )
    kwargs = {
        "use_latest_local": True,
        "n_latest_local": 2,
        "store_local": False,
        "store_remote": True,
    }
    main(**kwargs)

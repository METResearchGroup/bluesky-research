"""Helper functions for getting most liked Bluesky posts in the past day/week."""
import json
import os

from atproto_client.models.app.bsky.feed.defs import FeedViewPost

from lib.aws.s3 import S3, SYNC_KEY_ROOT
from lib.constants import current_datetime
from lib.db.bluesky_models.transformations import (
    TransformedFeedViewPostModel, TransformedRecordModel
)
from lib.db.mongodb import get_mongodb_collection
from services.preprocess_raw_data.classify_language.helper import record_is_english  # noqa
from transform.bluesky_helper import get_posts_from_custom_feed_url
from transform.transform_raw_data import transform_feedview_posts

s3 = S3()

feed_to_info_map = {
    "today": {
        "description": "Most popular posts from the last 24 hours",
        "url": "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up"
    },
    "week": {
        "description": "Most popular posts from the last 7 days",
        "url": "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up-weekly"
    }
}

current_directory = os.path.dirname(os.path.abspath(__file__))
current_datetime_str = current_datetime.strftime("%Y-%m-%d-%H:%M:%S")
sync_dir = "syncs"
full_sync_dir = os.path.join(current_directory, sync_dir)
if not os.path.exists(full_sync_dir):
    os.makedirs(full_sync_dir)
sync_fp = os.path.join(full_sync_dir, f"most_liked_posts_{current_datetime_str}.jsonl")
urls_fp = os.path.join(full_sync_dir, f"urls_{current_datetime_str}.csv")

task_name = "get_most_liked_posts"
mongo_collection_name, mongo_collection = (
    get_mongodb_collection(task=task_name)
)

# TODO: probably should move this elsewhere at some point.


def post_passed_filters(post: TransformedFeedViewPostModel) -> bool:
    record: TransformedRecordModel = post.record
    return record_is_english(record=record)


def filter_posts(
    posts: list[TransformedFeedViewPostModel]
) -> list[TransformedFeedViewPostModel]:
    """Filter the posts."""
    return list(filter(post_passed_filters, posts))


def get_and_transform_latest_most_liked_posts(
    feeds: list[str] = ["today", "week"]
) -> list[TransformedFeedViewPostModel]:
    """Get the latest batch of most liked posts and transform them."""
    res: list[TransformedFeedViewPostModel] = []
    for feed in feeds:
        feed_url = feed_to_info_map[feed]["url"]
        enrichment_data = {
            "source_feed": feed, "feed_url": feed_url
        }
        print(f"Getting most liked posts from {feed} feed with URL={feed_url}")
        posts: list[FeedViewPost] = get_posts_from_custom_feed_url(
            feed_url=feed_url, limit=None
        )
        transformed_posts: list[TransformedFeedViewPostModel] = (
            transform_feedview_posts(
                posts=posts, enrichment_data=enrichment_data
            )
        )
        res.extend(transformed_posts)
        print(f"Finished processing {len(posts)} posts from {feed} feed")
    return res


def export_posts(
    posts: list[TransformedFeedViewPostModel],
    store_local: bool = True,
    store_remote: bool = True
) -> None:
    """Export the posts to a file, either locally as a JSON or remote in S3."""
    if store_local:
        print(f"Writing {len(posts)} posts to {sync_fp}")
        with open(sync_fp, "w") as f:
            for post in posts:
                post_json = json.dumps(post)
                f.write(post_json + "\n")
        num_posts = len(posts)
        print(f"Wrote {num_posts} posts locally to {sync_fp}")

    if store_remote:
        timestamp_key = S3.create_partition_key_based_on_timestamp(
            timestamp_str=current_datetime_str
        )
        filename = "posts.jsonl"
        full_key = os.path.join(
            SYNC_KEY_ROOT, "sync_most_liked_feed", timestamp_key, filename
        )
        if isinstance(posts[0], TransformedFeedViewPostModel):
            post_dicts = [post.dict() for post in posts]
        else:
            post_dicts = posts
        s3.write_dicts_jsonl_to_s3(
            data=post_dicts, key=full_key
        )
        print(f"Exported {len(posts)} posts to S3 at {full_key}")


def load_most_recent_local_syncs(n_latest_local: int = 1) -> list[dict]:
    """Load the most recent local sync file."""
    sync_files = os.listdir(os.path.join(current_directory, sync_dir))
    posts: list[dict] = []
    most_recent_filenames = sorted(sync_files)[-n_latest_local:]
    for filename in most_recent_filenames:
        sync_fp = os.path.join(
            current_directory, sync_dir, filename
        )
        print(f"Reading most recent sync file {sync_fp}")
        with open(sync_fp, "r") as f:
            posts.extend([json.loads(line) for line in f])
    return posts


def filter_most_recent_local_syncs(n_latest_local: int = 1) -> list[dict]:
    """Filter the most recent local sync files."""
    posts: list[dict] = load_most_recent_local_syncs(
        n_latest_local=n_latest_local
    )
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
    export_posts(
        posts=posts, store_local=False, store_remote=True
    )


def main(
    use_latest_local: bool = False,
    n_latest_local: int = 1,
    store_local: bool = True,
    store_remote: bool = True,
    feeds: list[str] = ["today", "week"]
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
        post_dicts = [post.dict() for post in filtered_posts]
        print(f"Exporting {len(post_dicts)} total posts...")
    export_posts(
        posts=post_dicts, store_local=store_local,
        store_remote=store_remote
    )


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

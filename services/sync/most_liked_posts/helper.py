"""Helper functions for getting most liked Bluesky posts in the past day/week."""
import json
import os

from atproto_client.models.app.bsky.feed.defs import FeedViewPost
from pymongo.errors import DuplicateKeyError

from lib.constants import current_datetime
from lib.db.bluesky_models.transformations import (
    TransformedFeedViewPostModel, TransformedRecordModel
)
from lib.db.mongodb import get_mongodb_collection, chunk_insert_posts
from services.preprocess_raw_data.classify_language.helper import record_is_english  # noqa
from transform.bluesky_helper import get_posts_from_custom_feed_url
from transform.transform_raw_data import transform_feedview_posts

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


def transform_feedviewpost_dict_into_transformed_post(post: dict) -> TransformedFeedViewPostModel:
    """Transforms a feedview post, given as a dict (but has the original
    Bluesky FeedViewPost schema) into a TransformedFeedViewPostModel.
    """  # noqa
    pass


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
    store_remote: bool = True,
    bulk_write_remote: bool = True,
    bulk_chunk_size: int = 100
) -> None:
    """Export the posts to a file, either locally as a JSON or remote in a
    MongoDB collection."""
    if store_local:
        print(f"Writing {len(posts)} posts to {sync_fp}")
        with open(sync_fp, "w") as f:
            for post in posts:
                post_json = json.dumps(post)
                f.write(post_json + "\n")
        num_posts = len(posts)
        print(f"Wrote {num_posts} posts locally to {sync_fp}")

    if store_remote:
        duplicate_key_count = 0
        total_successful_inserts = 0
        total_posts = len(posts)
        print(f"Inserting {total_posts} posts to MongoDB collection {mongo_collection_name}")  # noqa
        formatted_posts_mongodb = [
            {"_id": post["uri"], **post}
            for post in posts
        ]
        if bulk_write_remote:
            print("Inserting into MongoDB in bulk...")
            total_successful_inserts, duplicate_key_count = chunk_insert_posts(
                posts=formatted_posts_mongodb,
                mongo_collection=mongo_collection,
                chunk_size=bulk_chunk_size
            )
            print("Finished bulk inserting into MongoDB.")
        else:
            for idx, post in enumerate(posts):
                if idx % 100 == 0:
                    print(f"Inserted {idx}/{total_posts} posts")
                try:
                    post_uri = post["uri"]
                    # set the URI as the primary key.
                    # NOTE: if this doesn't work, check if the IP address has
                    # permission to access the database.
                    mongo_collection.insert_one(
                        {"_id": post_uri, **post},
                    )
                    total_successful_inserts += 1
                except DuplicateKeyError:
                    duplicate_key_count += 1
        if duplicate_key_count > 0:
            print(f"Skipped {duplicate_key_count} duplicate posts")
        print(f"Inserted {total_successful_inserts} posts to remote MongoDB collection {mongo_collection_name}")  # noqa


def load_most_recent_local_sync() -> list[dict]:
    """Load the most recent local sync file."""
    sync_files = os.listdir(os.path.join(current_directory, sync_dir))
    most_recent_filename = sorted(sync_files)[-1]
    sync_fp = os.path.join(current_directory, sync_dir, most_recent_filename)
    print(f"Reading most recent sync file {sync_fp}")
    with open(sync_fp, "r") as f:
        posts: list[dict] = [json.loads(line) for line in f]
    return posts


def filter_most_recent_local_sync() -> list[dict]:
    """Filter the most recent local sync file."""
    posts: list[dict] = load_most_recent_local_sync()
    transformed_posts: list[TransformedFeedViewPostModel] = [
        TransformedFeedViewPostModel(**post) for post in posts
    ]
    filtered_posts: list[TransformedFeedViewPostModel] = filter_posts(
        posts=transformed_posts
    )
    return [post.dict() for post in filtered_posts]


def dump_most_recent_local_sync_to_remote() -> None:
    """Dump the most recent local sync to the remote MongoDB collection."""
    posts: list[dict] = load_most_recent_local_sync()
    export_posts(
        posts=posts, store_local=False, store_remote=True,
        bulk_write_remote=True
    )


def main(
    use_latest_local: bool = False,
    store_local: bool = True,
    store_remote: bool = True,
    bulk_write_remote: bool = True,
    feeds: list[str] = ["today", "week"]
) -> None:
    if use_latest_local:
        post_dicts: list[dict] = filter_most_recent_local_sync()
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
        store_remote=store_remote, bulk_write_remote=bulk_write_remote
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
        "store_local": False,
        "store_remote": True,
        "bulk_write_remote": True
    }
    main(**kwargs)

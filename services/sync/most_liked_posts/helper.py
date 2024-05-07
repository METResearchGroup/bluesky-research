"""Helper functions for getting most liked Bluesky posts in the past day/week."""
import json
import os
from typing import Literal

from atproto_client.models.app.bsky.feed.defs import FeedViewPost
from pymongo.errors import DuplicateKeyError

from lib.constants import current_datetime
from lib.db.mongodb import get_mongodb_collection, chunk_insert_posts
from transform.bluesky_helper import get_posts_from_custom_feed_url


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


def create_new_feedview_post_fields(
    post: FeedViewPost, source_feed: Literal["today", "week"]
) -> dict:
    res = {}
    handle = post.post.author.handle
    uri = post.post.uri.split("/")[-1]
    res["url"] = f"https://bsky.app/profile/{handle}/post/{uri}"
    metadata: dict = {
        "source_feed": source_feed, "synctimestamp": current_datetime_str
    }
    res["metadata"] = metadata
    return res


def process_feedview_posts(
    posts: list[FeedViewPost], source_feed: Literal["today", "week"]
) -> list[dict]:
    """Processes the FeedView posts.

    Example FeedViewPost, as a dictionary:

    {'post': {'author': {'associated': None,
                        'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:ywbm3iywnhzep3ckt6efhoh7/bafkreib5z4tmeiydwxh3qdslgjj5ik5prk3isjppulgwylxw2lnqkfzl5e@jpeg',
                        'did': 'did:plc:ywbm3iywnhzep3ckt6efhoh7',
                        'display_name': 'Katie Tightpussy',
                        'handle': 'juicysteak117.gay',
                        'labels': [],
                        'py_type': 'app.bsky.actor.defs#profileViewBasic',
                        'viewer': {'blocked_by': False,
                                    'blocking': None,
                                    'blocking_by_list': None,
                                    'followed_by': åNone,
                                    'following': None,
                                    'muted': False,
                                    'muted_by_list': None,
                                    'py_type': 'app.bsky.actor.defs#viewerState'}},
            'cid': 'bafyreicvm7uriskx7hrj7utwc7nbi3wgmojs5unqlzbalvc2adfghhylqi',
            'embed': {'images': [{'alt': 'a dumpy green looking cybertruck',
                                    'aspect_ratio': {'height': 1632,
                                                    'py_type': 'app.bsky.embed.images#aspectRatio',
                                                    'width': 2000},
                                    'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:ywbm3iywnhzep3ckt6efhoh7/bafkreichkqx3325tkfzxor4b7o6t2zfe6gywb2224ge4z6rauh7nvo6bxi@jpeg',
                                    'py_type': 'app.bsky.embed.images#viewImage',
                                    'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:ywbm3iywnhzep3ckt6efhoh7/bafkreichkqx3325tkfzxor4b7o6t2zfe6gywb2224ge4z6rauh7nvo6bxi@jpeg'}],
                        'py_type': 'app.bsky.embed.images#view'},
            'indexed_at': '2024-05-01T03:39:44.010Z',
            'labels': [],
            'like_count': 7101,
            'py_type': 'app.bsky.feed.defs#postView',
            'record': {'created_at': '2024-05-01T03:39:44.258Z',
                        'embed': {'images': [{'alt': 'a dumpy green looking '
                                                    'cybertruck',
                                            'aspect_ratio': {'height': 1632,
                                                                'py_type': 'app.bsky.embed.images#aspectRatio',
                                                                'width': 2000},
                                            'image': {'mime_type': 'image/jpeg',
                                                        'py_type': 'blob',
                                                        'ref': {'link': 'bafkreichkqx3325tkfzxor4b7o6t2zfe6gywb2224ge4z6rauh7nvo6bxi'},
                                                        'size': 697300},
                                            'py_type': 'app.bsky.embed.images#image'}],
                                'py_type': 'app.bsky.embed.images'},
                        'entities': None,
                        'facets': None,
                        'labels': None,
                        'langs': ['en'],
                        'py_type': 'app.bsky.feed.post',
                        'reply': None,
                        'tags': None,
                        'text': "OH MY GOD I THOUGHT THIS WAS A DUMPSTER I'M NOT "
                                'EVEN KIDDING I HAD TRASH IN MY HAND THAT I WAS '
                                'TAKING TOWARDS IT HAHAHAHAHAHAH'},
            'reply_count': 253,
            'repost_count': 1560,
            'threadgate': None,
            'uri': 'at://did:plc:ywbm3iywnhzep3ckt6efhoh7/app.bsky.feed.post/3krfkyfh3pc24',
            'viewer': {'like': None,
                        'py_type': 'app.bsky.feed.defs#viewerState',
                        'reply_disabled': None,
                        'repost': None}},
    'py_type': 'app.bsky.feed.defs#feedViewPost',
    'reason': None,
    'reply': None}
    """  # noqa
    return [
        {
            **post.dict(),
            **create_new_feedview_post_fields(
                post=post, source_feed=source_feed
            )
        }
        for post in posts
    ]


def get_latest_most_liked_posts() -> list[dict]:
    """Get the latest batch of most liked posts."""
    res: list[dict] = []
    for feed in ["today", "week"]:
        feed_url = feed_to_info_map[feed]["url"]
        print(f"Getting most liked posts from {feed} feed with URL={feed_url}")
        posts: list[FeedViewPost] = get_posts_from_custom_feed_url(
            feed_url=feed_url, limit=None
        )
        processed_posts: list[dict] = process_feedview_posts(
            posts=posts, source_feed=feed
        )
        res.extend(processed_posts)
        print(f"Finished processing {len(posts)} posts from {feed} feed")
    return res


def export_posts(
    posts: list[dict],
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
            {"_id": post["post"]["uri"], **post}
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
                    post_uri = post["post"]["uri"]
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


def dump_most_recent_local_sync_to_remote() -> None:
    """Dump the most recent local sync to the remote MongoDB collection."""
    sync_files = os.listdir(os.path.join(current_directory, sync_dir))
    most_recent_filename = sorted(sync_files)[-1]
    sync_fp = os.path.join(current_directory, sync_dir, most_recent_filename)
    print(f"Reading most recent sync file {sync_fp}")
    with open(sync_fp, "r") as f:
        posts: list[dict] = [json.loads(line) for line in f]
    export_posts(posts=posts, store_local=False, store_remote=True)


if __name__ == "__main__":
    # posts: list[dict] = get_latest_most_liked_posts()
    # export_posts(posts=posts, store_local=True, store_remote=True)
    dump_most_recent_local_sync_to_remote()

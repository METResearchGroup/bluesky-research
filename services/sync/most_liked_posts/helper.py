"""Helper functions for getting most liked Bluesky posts in the past day/week."""
import json
import os
from typing import Literal

from atproto_client.models.app.bsky.feed.defs import FeedViewPost

from lib.constants import current_datetime
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
sync_fp = os.path.join(current_directory, sync_dir, f"most_liked_posts_{current_datetime_str}.jsonl")


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


def export_posts(posts: list[dict]) -> None:
    """Export the posts to a file."""
    with open(sync_fp, "w") as f:
        for post in posts:
            post_json = json.dumps(post)
            f.write(post_json + "\n")
    num_posts = len(posts)
    print(f"Wrote {num_posts} posts to {sync_fp}")


if __name__ == "__main__":
    posts: list[dict] = get_latest_most_liked_posts()
    export_posts(posts)
    breakpoint()

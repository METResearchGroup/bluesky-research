"""Search by user."""
from typing import Literal, Optional

from atproto_client.models.app.bsky.actor.defs import ProfileViewDetailed
from atproto_client.models.app.bsky.feed.defs import FeedViewPost
from atproto_client.models.app.bsky.feed.get_author_feed import Response as AuthorFeedResponse # noqa

from lib.helper import client
from services.transform.transform_raw_data import (
    flatten_feed_view_post, FlattenedFeedViewPost
)


MAX_POSTS_PER_REQUEST = 100


def get_posts_by_user(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None,
    filter: Literal[
        "posts_with_replies",
        "posts_no_replies",
        "posts_with_media",
        "posts_and_author_threads"
    ] = "posts_with_replies",
    limit: Optional[str] = 50
) -> list[FlattenedFeedViewPost]:
    """Get all the posts written by a user (and appears in their feed).

    Includes posts, retweets and replies. Anything that would appear in the
    "posts" tab of that user's account.

    Corresponding lexicon: https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.getAuthorFeed.json

    The endpoint returns 50 posts by default and then returns a cursor. The
    endpoint has a maximmum of 100 posts per request.

    Example code for handling pagination: https://github.com/MarshalX/atproto/blob/main/examples/advanced_usage/handle_cursor_pagination.py
    """
    actor = author_handle or author_did
    if not actor:
        raise ValueError("Must provide an author handle or author did.")

    author_feed: list[FeedViewPost] = []
    total_fetched_posts: int = 0
    total_posts_to_fetch: int = limit
    cursor = None

    if limit > MAX_POSTS_PER_REQUEST:
        print(
            f"Limit of {limit} exceeds the maximum of {MAX_POSTS_PER_REQUEST}."
        )
        print(f"Will batch requests in chunks of {MAX_POSTS_PER_REQUEST}.")

    while True:
        print(f"Fetching {limit} posts for {actor}...")
        request_limit = min(limit, MAX_POSTS_PER_REQUEST)
        fetched: AuthorFeedResponse = client.get_author_feed(
            actor=actor, cursor=cursor, filter=filter, limit=request_limit
        )
        num_fetched_posts = len(fetched.feed)
        total_fetched_posts += num_fetched_posts
        author_feed.extend(fetched.feed[:total_posts_to_fetch])
        total_posts_to_fetch -= num_fetched_posts
        if not fetched.cursor:
            break
        if total_fetched_posts >= limit:
            print(f"Total fetched posts: {total_fetched_posts}")
            break
        cursor = fetched.cursor
    return [flatten_feed_view_post(post) for post in author_feed]


def get_profile_of_user(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None
) -> ProfileViewDetailed:
    """Get the profile of a user.

    Corresponding lexicon: https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.actor.getProfile.json
    - https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/actor/defs.py#L50

    Example profile:
    {
        'did': 'did:plc:eclio37ymobqex2ncko63h4r',
        'handle': 'nytimes.com',
        'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreidvvqj5jymmpaeklwkpq6gi532el447mjy2yultuukypzqm5ohfju@jpeg',
        'banner': 'https://cdn.bsky.app/img/banner/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreiaiorkgl6t2j5w3sf6nj37drvwuvriq3e3vqwf4yn3pchpwfbekta@jpeg',
        'description': 'In-depth, independent reporting to better understand the world, now on Bluesky. News tips? Share them here: http://nyti.ms/2FVHq9v',
        'display_name': 'The New York Times',
        'followers_count': 204711,
        'follows_count': 8,
        'indexed_at': '2024-01-25T23:46:23.929Z',
        'labels': [],
        'posts_count': 1984,
        'viewer': ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following='at://did:plc:w5mjarupsl6ihdrzwgnzdh4y/app.bsky.graph.follow/3kkvauysemf2p', muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'),
        'py_type': 'app.bsky.actor.defs#profileViewDetailed'
    }
    """ # noqa
    actor = author_handle or author_did
    if not actor:
        raise ValueError("Must provide an author handle or author did.")
    return client.get_profile(did=actor)


def main() -> None:
    pass


if __name__ == "__main__":
    main()

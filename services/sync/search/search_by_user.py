"""Search by user."""
from typing import Literal, Optional

from atproto_client.models.app.bsky.actor.defs import (
    ProfileView, ProfileViewDetailed
)

from lib.helper import client
from services.sync.search.helper import (
    DEFAULT_LIMIT_RESULTS_PER_REQUEST, send_request_with_pagination
)
from transform.transform_raw_data import (
    flatten_feed_view_post, FlattenedFeedViewPost
)


def get_posts_by_user(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None,
    filter: Literal[
        "posts_with_replies",
        "posts_no_replies",
        "posts_with_media",
        "posts_and_author_threads"
    ] = "posts_with_replies",
    limit: Optional[str] = DEFAULT_LIMIT_RESULTS_PER_REQUEST
) -> list[FlattenedFeedViewPost]:
    """Get all the posts written by a user (and appears in their feed).

    Includes posts, retweets and replies. Anything that would appear in the
    "posts" tab of that user's account.

    Corresponding lexicon: https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.getAuthorFeed.json

    The endpoint returns 50 posts by default and then returns a cursor. The
    endpoint has a maximmum of 100 posts per request.

    Example output post (after flattening):
    {
        'author_did': 'did:plc:e4itbqoxctxwrrfqgs2rauga',
        'author_handle': 'williambrady.bsky.social',
        'author_display_name': 'William J. Brady',
        'created_at': '2024-01-19T16:08:25.789Z',
        'text': 'Review: "14 studies implicated the YouTube recommender system in facilitating problematic content pathways, 7 produced mixed results, and two did not implicate the recommender system in facilitating pathways to problematic content" | osf.io/preprints/ps...',
        'langs': ['en'],
        'cid': 'bafyreialxgly7e5pqhzhfflqjn3wzmojhqlstuvylkaahtkxxw2p5nch3u',
        'indexed_at': '2024-01-19T16:08:25.789Z',
        'like_count': 2,
        'reply_count': 0,
        'repost_count': 0
    }
    """ # noqa
    actor = author_handle or author_did
    if not actor:
        raise ValueError("Must provide an author handle or author did.")
    author_feed = send_request_with_pagination(
        func=client.get_author_feed,
        kwargs={"actor": actor, "filter": filter},
        response_key="feed",
        limit=limit
    )
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


def get_user_follows(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None,
    limit: Optional[int] = DEFAULT_LIMIT_RESULTS_PER_REQUEST
) -> list[ProfileView]:
    """Get a list of the user's follows.

    Corresponding lexicon:
    - https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.graph.getFollows.json
    - https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/graph/get_follows.py

    Example "ProfileView" object from `getFollows` endpoint:
    {
        'did': 'did:plc:ayiedn5wwlwpwab62irpwjt4',
        'handle': 'wlamb76.bsky.social',
        'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:ayiedn5wwlwpwab62irpwjt4/bafkreihwynif3fuqoeqggxkjhcw4hdwg3srerzz5ybfvnj5q33verlehau@jpeg',
        'description': 'Senior Staff Editor, general-assignment and breaking news, The New York Times.',
        'display_name': 'William Lamb',
        'indexed_at': '2024-01-26T00:28:00.693Z',
        'labels': [],
        'viewer': ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'),
        'py_type': 'app.bsky.actor.defs#profileView'
    }
    """ # noqa
    actor = author_handle or author_did
    if not actor:
        raise ValueError("Must provide an author handle or author did.")
    follows_list: list[ProfileView] = send_request_with_pagination(
        func=client.get_follows,
        kwargs={"actor": actor},
        response_key="follows",
        limit=limit
    )
    return follows_list


def get_user_followers(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None,
    limit: Optional[int] = DEFAULT_LIMIT_RESULTS_PER_REQUEST
) -> list[ProfileView]:
    """Get a list of the user's followers.

    Corresponding lexicon:
    - https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.graph.getFollowers.json#L4

    Returns a list of followers to a user.

    Example "ProfileView" object from `getFollowers` endpoint:
    {
        'did': 'did:plc:6wfgxeck2rqqxmnlbnt45rem',
        'handle': 'mnam.bsky.social',
        'avatar': None,
        'description': None,
        'display_name': '',
        'indexed_at': '2024-02-08T08:48:45.242Z',
        'labels': [],
        'viewer': ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'),
        'py_type': 'app.bsky.actor.defs#profileView'
    }
    """ # noqa
    actor = author_handle or author_did
    if not actor:
        raise ValueError("Must provide an author handle or author did.")
    followers_list: list[ProfileView] = send_request_with_pagination(
        func=client.get_followers,
        kwargs={"actor": actor},
        response_key="followers",
        limit=limit
    )
    return followers_list


def main() -> None:
    author_handle = "nytimes.com"
    limit = 200
    posts = get_posts_by_user(author_handle=author_handle, limit=limit)
    print(f"Got {len(posts)} posts by {author_handle} (expected {limit}).")


if __name__ == "__main__":
    main()

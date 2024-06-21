"""Helper functions for feed API."""
from datetime import datetime
from typing import Optional

CURSOR_EOF = 'eof'


# TODO: should be loaded from DB.
def get_latest_feed_for_user():
    pass

# TODO: we'll have to account for pagination, so the output of the feed
# creation stage can't just be URIs, but instead it needs to also include
# metadata as well, specifically indexed_at and cid, so that Bluesky can ping
# our client and we can send the appropriate results.


def get_latest_feed(
    requester_did: str, limit: int, cursor: Optional[str] = None
):
    """Get latest feed for a user.

    Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/algos/whats_alf.py#L7
    """  # noqa
    if cursor:
        if cursor == CURSOR_EOF:
            return {
                'cursor': CURSOR_EOF,
                'feed': []
            }
        cursor_parts = cursor.split('::')
        if len(cursor_parts) != 2:
            raise ValueError('Malformed cursor')

        indexed_at, cid = cursor_parts
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000)

    posts = get_latest_feed_for_user()
    feed = [{'post': post.uri} for post in posts]

    cursor = CURSOR_EOF
    last_post = posts[-1] if posts else None
    if last_post:
        cursor = f'{int(last_post.indexed_at.timestamp() * 1000)}::{last_post.cid}'  # noqa


# TODO: should be loaded from DB.
def get_valid_dids() -> set[str]:
    """Get the set of valid DIDs. These DIDs refer to the DIDs of the users in
    the study.
    """
    return {"did:example:1234", "did:example:5678"}

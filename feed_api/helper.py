"""Helper functions for feed API."""
from datetime import datetime
from typing import Optional

from lib.db.sql.participant_data_database import get_user_to_bluesky_profiles
from services.create_feeds.models import CreatedFeedModel
from services.participant_data.models import UserToBlueskyProfileModel


CURSOR_EOF = 'eof'


# TODO: should be loaded from DB.
def get_latest_feed_for_user(requester_did: str) -> CreatedFeedModel:
    return CreatedFeedModel(
        study_user_id='1234',
        bluesky_user_did='did:example:1234',
        bluesky_user_handle='example',
        condition='reverse_chronological',
        feed_uris=[
            'https://example.com/post/1',
            'https://example.com/post/2',
            'https://example.com/post/3',
            'https://example.com/post/4',
            'https://example.com/post/5',
        ],
        timestamp='1632960000'
    )


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

        indexed_at, last_index = cursor_parts
        # TODO: this might need some parsing. Not sure if I'd really use
        # this though anyways. Maybe I could just to validate that I'm
        # requesting the same feed?
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000)
        start_position = int(last_index)
    else:
        start_position = 0

    # maybe cursor should be feed timestamp + position on the feed.
    feed: CreatedFeedModel = get_latest_feed_for_user()
    feed = [
        {'post': uri}
        for uri in feed.feed_uris[start_position:start_position + limit]
    ]

    last_feed_post_index = start_position + len(feed)
    max_feed_length = len(feed.feed_uris)

    cursor = CURSOR_EOF
    if last_feed_post_index < max_feed_length:
        cursor = f'{int(datetime.fromtimestamp(feed.timestamp) * 1000)}::{last_feed_post_index}'  # noqa

    return {
        'cursor': cursor,
        'feed': feed
    }


def get_valid_dids() -> set[str]:
    """Get the set of valid DIDs. These DIDs refer to the DIDs of the users in
    the study.
    """
    users: list[UserToBlueskyProfileModel] = get_user_to_bluesky_profiles()
    valid_dids = {user.bluesky_user_did for user in users}
    return valid_dids

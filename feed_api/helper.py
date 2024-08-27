"""Helper functions for feed API."""

from datetime import datetime
import hashlib
import json
import re
from typing import Optional

from lib.aws.athena import Athena
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


CURSOR_EOF = "eof"

athena = Athena()
logger = get_logger(__name__)


# TODO: we'll have to account for pagination, so the output of the feed
# creation stage can't just be URIs, but instead it needs to also include
# metadata as well, specifically indexed_at and cid, so that Bluesky can ping
# our client and we can send the appropriate results.
def get_latest_feed(requester_did: str, limit: int, cursor: Optional[str] = None):
    """Get latest feed for a user.

    Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/algos/whats_alf.py#L7
    """  # noqa
    if cursor:
        if cursor == CURSOR_EOF:
            return {"cursor": CURSOR_EOF, "feed": []}
        cursor_parts = cursor.split("::")
        if len(cursor_parts) != 2:
            raise ValueError("Malformed cursor")

        indexed_at, last_index = cursor_parts
        # TODO: this might need some parsing. Not sure if I'd really use
        # this though anyways. Maybe I could just to validate that I'm
        # requesting the same feed?
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000)
        start_position = int(last_index)
    else:
        start_position = 0

    # maybe cursor should be feed timestamp + position on the feed.
    # feed: CreatedFeedModel = load_latest_created_feed_for_user(
    #     bluesky_user_did=requester_did
    # )
    feed = []  # TODO: implement.
    feed = [
        {"post": uri} for uri in feed.feed_uris[start_position : start_position + limit]
    ]

    last_feed_post_index = start_position + len(feed)
    max_feed_length = len(feed.feed_uris)

    cursor = CURSOR_EOF
    if last_feed_post_index < max_feed_length:
        cursor = f"{int(datetime.fromtimestamp(feed.timestamp) * 1000)}::{last_feed_post_index}"  # noqa

    return {"cursor": cursor, "feed": feed}


def parse_feed_string(feed_string: str) -> list[dict]:
    """Parses the feed string.

    e.g.,
    >> parse_feed_string('[{item=at://did:plc:pmyqirafcp3jqdhrl7crpq7t/app.bsky.feed.post/3kzoxsoyom62q, score=2.8175526}, {item=at://did:plc:eg336dt7kyike5xkyed3iwcv/app.bsky.feed.post/3kzoptflyq72h, score=2.8040724}, {item=at://did:plc:fxr5qi344wulv4ggowdzv57y/app.bsky.feed.post/3kzo2exfi5z2r, score=2.8030896}]')
    [{'item': 'at://did:plc:pmyqirafcp3jqdhrl7crpq7t/app.bsky.feed.post/3kzoxsoyom62q', 'score': 2.8175526}, {'item': 'at://did:plc:eg336dt7kyike5xkyed3iwcv/app.bsky.feed.post/3kzoptflyq72h', 'score': 2.8040724}, {'item': 'at://did:plc:fxr5qi344wulv4ggowdzv57y/app.bsky.feed.post/3kzo2exfi5z2r', 'score': 2.8030896}]
    """  # noqa
    # Step 1: Surround keys with quotes
    feed_string = re.sub(r"(\w+)=", r'"\1":', feed_string)

    # Step 2: Surround the
    # did:plc:<some string>/app.bsky.feed.post/<some string> with quotes
    feed_string = re.sub(
        r"(at://did:plc:[\w]+/app\.bsky\.feed\.post/\w+)", r'"\1"', feed_string
    )

    # Step 3: Parse the string into a list of dicts
    feed = json.loads(feed_string)
    return feed


def hash_feed_post(post: dict) -> str:
    """Hash the post."""
    return hashlib.sha256(post["item"].encode()).hexdigest()


# NOTE: how do we manage the case where we've generated a new feed for the
# user and they're still viewing the old one? Something to consider.
# Maybe a good behavior is that on pagination, we return the new feed, though
# I think this is likely what would happen by default anyways?
# NOTE: I could also just have the feeds refresh at weird times, i.e.,
# 4am US time, making it highly unlikely for this collision to occur.
def load_latest_user_feed_from_s3(
    user_did: str, cursor: Optional[str] = None, limit: int = 30
) -> tuple[list[dict], str]:
    """Loads the latest feed for a user from S3.

    Optionally ingests a cursor for pagination purposes.
    """
    logger.info(f"Loading latest feed for user={user_did}...")
    query = f"""
    SELECT feed FROM "custom_feeds"
    WHERE user = '{user_did}'
    ORDER BY feed_generation_timestamp DESC
    LIMIT 1
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    feed: str = df_dicts[0]["feed"]
    feed_dicts: list[dict] = parse_feed_string(feed)
    timestamp = generate_current_datetime_str()

    hashed_uris_lst = [hash_feed_post(feed_dict) for feed_dict in feed_dicts]
    hash_uri_of_last_post_in_full_feed = hashed_uris_lst[-1]

    if cursor:
        logger.info(f"Cursor found and will be used to subset feed: {cursor}")
        cursor_parts = cursor.split("::")
        if len(cursor_parts) != 2:
            raise ValueError("Malformed cursor")
        _, hashed_uri = cursor_parts
        try:
            # the last index of the previous feed
            last_index = hashed_uris_lst.index(hashed_uri)
            logger.info(
                f"Hashed URI {hashed_uri} found in the current feed. Starting new feed from this index: {last_index+1}"
            )
        except ValueError:
            # NOTE: likely possible if they want to fetch a feed and
            # we've created a new feed for them.
            logger.info(
                f"Hashed URI {hashed_uri} does not exist in the current feed. Starting from beginning of feed."
            )  # noqa
            last_index = None
    else:
        last_index = 0
    start_idx = last_index + 1 if last_index else 0
    logger.info(f"Starting index of feed: {start_idx}")
    truncated_feed = feed_dicts[start_idx : start_idx + limit]
    hash_uri_of_last_post_in_feed = hash_feed_post(truncated_feed[-1])
    if hash_uri_of_last_post_in_feed == hash_uri_of_last_post_in_full_feed:
        logger.info(f"Reached the end of the feed for user={user_did}.")
        new_cursor = CURSOR_EOF
    else:
        new_cursor = f"{timestamp}::{hash_uri_of_last_post_in_feed}"

    return ([{"post": feed_dict["item"]} for feed_dict in truncated_feed], new_cursor)


def get_valid_dids() -> set[str]:
    """Get the set of valid DIDs. These DIDs refer to the DIDs of the users in
    the study.
    """
    users: list[UserToBlueskyProfileModel] = get_all_users()
    valid_dids = {user.bluesky_user_did for user in users}
    return valid_dids


if __name__ == "__main__":
    pass

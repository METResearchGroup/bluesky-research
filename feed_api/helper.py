"""Helper functions for feed API."""

from datetime import datetime
import json
import re
from typing import Optional

from lib.aws.athena import Athena
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


CURSOR_EOF = "eof"

athena = Athena()


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


def load_latest_user_feed_from_s3(user_did: str) -> list[dict]:
    """Loads the latest feed for a user from S3."""
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
    return [{"post": feed_dict["item"]} for feed_dict in feed_dicts]


# TODO: implement.
def load_test_feed_from_s3() -> list[dict]:
    test_user_did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"  # my personal DID.
    feed: list[dict] = load_latest_user_feed_from_s3(user_did=test_user_did)
    return feed


def get_valid_dids() -> set[str]:
    """Get the set of valid DIDs. These DIDs refer to the DIDs of the users in
    the study.
    """
    users: list[UserToBlueskyProfileModel] = get_all_users()
    valid_dids = {user.bluesky_user_did for user in users}
    return valid_dids


if __name__ == "__main__":
    load_test_feed_from_s3()

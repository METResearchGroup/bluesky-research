"""Helper functions for feed API."""

import hashlib
import json
import os
import re
from typing import Optional

from lib.aws.athena import Athena
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str
from lib.serverless_cache import (
    default_cache_name,
    default_ttl_seconds,
    ServerlessCache,
)
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


CURSOR_EOF = "eof"

athena = Athena()
glue = Glue()
s3 = S3()
serverless_cache = ServerlessCache()
logger = get_logger(__name__)

study_users: list[UserToBlueskyProfileModel] = get_all_users()
study_user_did_to_handle_map = {
    user.bluesky_user_did: user.bluesky_handle for user in study_users
}
valid_dids = {user.bluesky_user_did for user in study_users}

test_user_handles = [
    "testblueskyaccount.bsky.social",
    "testblueskyuserv2.bsky.social",
    "markptorres.bsky.social",
]
test_user_dids = {
    did for did in valid_dids if study_user_did_to_handle_map[did] in test_user_handles
}


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
    # at://did:<any string>/app.bsky.feed.post/<some string> with quotes
    feed_string = re.sub(
        # r"(at://did:plc:[\w]+/app\.bsky\.feed\.post/\w+)", r'"\1"', feed_string
        r"(at://did:[^/]+/app\.bsky\.feed\.post/\w+)",
        r'"\1"',
        feed_string,
    )

    # Step 3: Parse the string into a list of dicts
    feed = json.loads(feed_string)
    return feed


def hash_feed_post(post: dict) -> str:
    """Hash the post."""
    return hashlib.sha256(post["item"].encode()).hexdigest()


def load_all_latest_user_feeds_from_s3() -> dict[str, list[dict]]:
    """Loads the latest feed for all users from S3."""
    query = """
    SELECT user, feed_id, feed FROM "custom_feeds"
    WHERE (user, feed_generation_timestamp) IN (
        SELECT user, MAX(feed_generation_timestamp)
        FROM "custom_feeds"
        GROUP BY user
    )
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    feeds = [row["feed"] for row in df_dicts]
    user_feed_dicts: list[list[dict]] = [json.loads(feed) for feed in feeds]
    user_dids = [row["user"] for row in df_dicts]
    feed_ids = [row["feed_id"] for row in df_dicts]
    return {
        user_did: {"feed_id": feed_id, "feed_dicts": feed_dicts}
        for user_did, feed_id, feed_dicts in zip(user_dids, feed_ids, user_feed_dicts)
    }


def load_latest_user_feed_from_s3(user_did: str) -> tuple[list[dict], str]:
    """Loads the latest feed for a user from S3.

    Optionally ingests a cursor for pagination purposes.
    """
    logger.info(f"Loading latest feed for user={user_did}...")
    query = f"""
    SELECT feed_id, feed FROM "custom_feeds"
    WHERE user = '{user_did}'
    ORDER BY feed_generation_timestamp DESC
    LIMIT 1
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    feed: str = df_dicts[0]["feed"]
    feed_id: str = df_dicts[0]["feed_id"]
    feed_dicts: list[dict] = json.loads(feed)
    return feed_dicts, feed_id


def load_latest_user_feed(user_did: str) -> tuple[list[dict], str]:  # noqa
    """Loads latest user feed.

    Both the cache and the S3 feeds return the full complete feed. We
    use the cursor to determine which subset of the feed to return.
    """
    logger.info(
        f"Local cache miss for user={user_did} (shouldn't be, ideally). Loading latest feed from S3 and then adding to local in-memory cache."  # noqa
    )  # noqa
    feed_dicts, feed_id = load_latest_user_feed_from_s3(user_did=user_did)  # noqa
    return feed_dicts, feed_id


def create_feed_and_cursor(
    feed_dicts: list[dict], user_did: str, cursor: Optional[str] = None, limit: int = 30
) -> tuple[list[dict], Optional[str]]:
    """Loads latest user feed."""
    logger.info(f"Creating feed for user={user_did}...")
    timestamp = generate_current_datetime_str()
    hashed_uris_lst = [hash_feed_post(feed_dict) for feed_dict in feed_dicts]
    hash_uri_of_last_post_in_full_feed = hashed_uris_lst[-1]

    if cursor:
        if cursor == CURSOR_EOF:
            return ([], CURSOR_EOF)
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


def is_valid_user_did(user_did: str) -> bool:
    """Check if the user DID is valid."""
    return user_did in valid_dids


def cache_request(user_did: str, cursor: Optional[str], data: dict):
    """Cache the request. Uses a default TTL time for the cache."""
    data_json = json.dumps(data)
    cache_key = f"{user_did}::{cursor}"
    serverless_cache.set(
        cache_name=default_cache_name,
        key=cache_key,
        value=data_json,
        ttl=default_ttl_seconds,
    )


def get_cached_request(user_did: str, cursor: Optional[str]) -> Optional[dict]:
    """Get the cached request."""
    cache_key = f"{user_did}::{cursor}"
    res: Optional[str] = serverless_cache.get(
        cache_name=default_cache_name,
        key=cache_key,
    )
    if res:
        return json.loads(res)
    return None


def export_log_data(log: dict):
    """Export user session log data"""
    user_did = log["user_did"]
    handle = study_user_did_to_handle_map[user_did]
    timestamp = log["timestamp"]
    key = os.path.join("user_session_logs", f"bluesky_user_handle={handle}", timestamp)
    s3.write_dict_json_to_s3(
        data=log,
        key=key,
    )
    logger.info(f"Exported user session logs to S3 (key={key}): {log}")


if __name__ == "__main__":
    user_did = "did:plc:wvb6v45g6oxrfebnlzllhrpv"
    latest_feed, feed_id = load_latest_user_feed(user_did=user_did)
    s3_latest_feed = load_latest_user_feed_from_s3(user_did=user_did)
    res = load_all_latest_user_feeds_from_s3()
    assert latest_feed == s3_latest_feed
    assert res[user_did]["feed_dicts"] == s3_latest_feed

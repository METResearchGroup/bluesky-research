"""Processes previous feeds and:

1. Adds a 'feed_id' to each feed.
2. Dumps the 'feed' field as JSON-dumped files to S3 (instead of the complex
struct that it currently is).
    - Requires that the existing 'cached_custom_feeds' table is changed to
    have the 'feed' field be a string and not a complex struct.

This needs to be done on the "cached" feeds in order to prove that it works,
and then can be done on the "active" feeds once that's verified.

Migration plan:
1. Copy the existing cached feeds in S3 to a new location, to have a backup.
2. Load in all posts as a pandas df SELECT * on the cached feeds.
3. Add the 'feed_id' field to the df.
4. Serialize the df to a JSON-dumped string and save to the 'feed' field.
5. Upsert the df back to the cached feeds table, overwriting the existing data.
    - Group by on partition_date and then write one file per partition_date.
    This is OK since each row will now have its correct feed_id based on the
    timestamp.

It looks like you can't change the name of the table created by the Glue crawler,
so I need to change the S3 paths to "cached_custom_feeds".
"""

import json
import re

import pandas as pd

from lib.aws.athena import Athena

athena = Athena()


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


def load_cached_feeds_as_df() -> pd.DataFrame:
    query = """SELECT * FROM cached_custom_feeds"""
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    feeds = [row["feed"] for row in df_dicts]
    user_feed_dicts: list[list[dict]] = [parse_feed_string(feed) for feed in feeds]

    df = pd.DataFrame(df_dicts)
    df["feed"] = user_feed_dicts

    return df


def add_feed_id_col_to_df(df: pd.DataFrame) -> pd.DataFrame:
    df["feed_id"] = df.apply(
        lambda row: f"{row['user']}::{row['feed_generation_timestamp']}", axis=1
    )
    return df


def upsert_df_to_cached_feeds(df: pd.DataFrame) -> None:
    pass


def main():
    df = load_cached_feeds_as_df()
    df = add_feed_id_col_to_df(df)
    upsert_df_to_cached_feeds(df)

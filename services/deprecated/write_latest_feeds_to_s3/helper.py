"""Helper functions for writing the latest feeds to s3.

This module writes the latest feeds to s3, using the following key schema:
/latest_feeds/user_did={user_did}/{timestamp}/feed.json
"""

import os

from lib.aws.s3 import S3
from lib.db.sql.created_feeds_database import load_latest_created_feeds_per_user  # noqa
from services.create_feeds.models import CreatedFeedModel


s3 = S3()


def write_feeds_to_s3(latest_feeds_per_user: list[CreatedFeedModel]):
    for feed in latest_feeds_per_user:
        json_payload = {"post_uris": feed.feed_uris}
        key = os.path.join(
            "latest_feeds",
            f"user_did={feed.bluesky_user_did}",
            feed.timestamp,
            "feed.json",
        )
        s3.write_dict_json_to_s3(data=json_payload, key=key)
        print(f"Wrote feed for user {feed.bluesky_user_did} to S3 at {key}.")
    print(f"Finished writing {len(latest_feeds_per_user)} feeds to S3.")


def write_latest_feeds_to_s3():
    latest_feeds_per_user: list[CreatedFeedModel] = load_latest_created_feeds_per_user()  # noqa
    print(f"Writing {len(latest_feeds_per_user)} feeds to S3.")
    write_feeds_to_s3(latest_feeds_per_user)
    print(f"Finished writing {len(latest_feeds_per_user)} feeds to S3.")

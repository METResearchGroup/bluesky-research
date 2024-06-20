"""Helper functions for getting the latest in-network posts."""
from datetime import datetime, timedelta, timezone
from typing import Optional

from lib.db.sql.in_network_posts_database import (
    batch_insert_in_network_posts,
    get_latest_indexed_in_network_timestamp
)
from lib.db.sql.network_connections_database import get_all_followed_connections  # noqa
from lib.db.sql.preprocessing_database import get_filtered_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel
from services.update_network_connections.models import UserToConnectionModel


# set default timestamp to 1 week before the current pipeline, so we don't have
# to process old posts.
num_lookback_days_default = 7
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_lookback_days_default)
).strftime("%Y-%m-%d")

def get_latest_in_network_posts():
    """Get the latest in-network posts.

    Load all new posts that were preprocessed after the latest in-network posts
    were indexed, and then check to see which of those are in-network posts
    (i.e., posts whose author is followed by any user in the study).
    """
    latest_timestamp: Optional[str] = get_latest_indexed_in_network_timestamp()
    if not latest_timestamp:
        latest_timestamp = default_latest_timestamp
    preprocessed_posts: list[FilteredPreprocessedPostModel] = (
        get_filtered_posts(latest_preprocessing_timestamp=latest_timestamp)
    )
    print(f"Loaded {len(preprocessed_posts)} new posts since the last in-network post indexing.") # noqa

    # load all users from the network that are follows.
    followed_users: list[UserToConnectionModel] = get_all_followed_connections()  # noqa
    print(f"There are {len(followed_users)} accounts that are followed by users in the study.") # noqa

    followed_users_dids = set([user.user_did for user in followed_users])
    followed_users_handles = set([user.user_handle for user in followed_users])

    in_network_posts: list[FilteredPreprocessedPostModel] = []

    # does set lookups for now. If it scales even more, we might want to use
    # a Bloom filter. But for now it seems like, especially since we're doing
    # incremental lookups, that a set lookup is OK.
    for post in preprocessed_posts:
        if (
            post.author.did in followed_users_dids
            or post.author.handle in followed_users_handles
        ):
            in_network_posts.append(post)

    print(f"Found {len(in_network_posts)} in-network posts. Inserting to DB...") # noqa
    batch_insert_in_network_posts(in_network_posts)

"""Helper functions for updating user engagement metrics in our database."""
from typing import Optional

from lib.constants import current_datetime_str
from lib.db.sql.participant_data_database import (
    get_user_to_bluesky_profiles
)
from lib.db.sql.user_engagement_database import (
    get_most_recent_post_timestamp
)
from lib.helper import client, RateLimitedClient
from services.participant_data.models import UserToBlueskyProfileModel
from services.update_user_bluesky_engagement.models import UserEngagementMetricsModel  # noqa


def get_latest_likes(client: RateLimitedClient) -> list[str]:
    client.app.bsky.feed.get_actor_likes


def get_latest_posts_written_by_user(author_profile):
    most_recent_post_timestamp: Optional[str] = get_most_recent_post_timestamp(
        author_handle=author_profile.handle
    )
    if most_recent_post_timestamp:
        pass


def get_latest_user_engagement_metrics(
    user: UserToBlueskyProfileModel
) -> UserEngagementMetricsModel:
    """Get the latest user engagement metrics for a user."""
    author_profile = client.get_profile(user.bluesky_user_did)
    breakpoint()
    res = {
        "user_did": user.bluesky_user_did,
        "user_handle": user.bluesky_handle,
        "latest_likes": [],
        "latest_comments": [],
        "latest_reshares": [],
        "latest_follower_count": author_profile.followers_count,
        "latest_following_count": author_profile.follows_count,
        "latest_posts_written": [],
        "latest_total_posts_written_count": author_profile.posts_count,
        "update_timestamp": current_datetime_str
    }
    return UserEngagementMetricsModel(**res)


def get_latest_engagement_metrics_for_users(
    users: list[UserToBlueskyProfileModel]
) -> list[UserEngagementMetricsModel]:
    return [
        get_latest_user_engagement_metrics(user=user)
        for user in users
    ]


def update_latest_user_engagement_metrics():
    # load users
    users: list[UserToBlueskyProfileModel] = get_user_to_bluesky_profiles()
    # for each user, calculate their latest engagement metrics
    get_latest_engagement_metrics_for_users(users=users)

"""Helper functions for updating user engagement metrics in our database."""
from typing import Optional

from atproto_client.models.app.bsky.feed.defs import FeedViewPost

from lib.constants import current_datetime_str
from lib.db.bluesky_models.transformations import (
    TransformedFeedViewPostModel, TransformedRecordModel
)
from lib.db.sql.participant_data_database import (
    get_user_to_bluesky_profiles
)
from lib.db.sql.user_engagement_database import (
    get_most_recent_post_timestamp
)
from lib.helper import client, RateLimitedClient
from services.participant_data.models import UserToBlueskyProfileModel
from services.sync.search.helper import send_request_with_pagination
from services.update_user_bluesky_engagement.models import (
    PostWrittenByStudyUserModel, UserEngagementMetricsModel
)
from transform.transform_raw_data import transform_feedview_post


def get_latest_likes(client: RateLimitedClient) -> list[str]:
    client.app.bsky.feed.get_actor_likes


def get_latest_posts_written_by_user(author_profile) -> list[PostWrittenByStudyUserModel]:  # noqa
    most_recent_post_timestamp: Optional[str] = get_most_recent_post_timestamp(
        author_handle=author_profile.handle
    )
    if most_recent_post_timestamp:
        recency_callback = (
            lambda post: post.post.record.created_at
            > most_recent_post_timestamp
        )
    else:
        recency_callback = None
    kwargs = {"actor": author_profile.handle}
    res: list[FeedViewPost] = send_request_with_pagination(
        func=client.app.bsky.feed.get_author_feed,
        kwargs={"params": kwargs},
        update_params_directly=True,
        response_key="feed",
        recency_callback=recency_callback,
    )
    transformed_latest_posts: list[PostWrittenByStudyUserModel] = []
    for post in res:
        enrichment_data = {"source_feed": None, "feed_url": None}
        transformed_feedview_post: TransformedFeedViewPostModel = transform_feedview_post(  # noqa
            post=post, enrichment_data=enrichment_data
        )
        transformed_feedview_record: TransformedRecordModel = (
            transformed_feedview_post.record
        )
        uri = transformed_feedview_post.uri
        transformed_post = PostWrittenByStudyUserModel(
            uri=uri,
            cid=post.post.cid,
            indexed_at=post.post.indexed_at,
            created_at=post.post.record.created_at,
            author_did=author_profile.did,
            author_handle=author_profile.handle,
            record=transformed_feedview_record,
            text=post.post.record.text,
            synctimestamp=current_datetime_str,
            url=f"https://bsky.app/profile/{author_profile.handle}/post/{uri}",
            like_count=post.post.like_count,
            reply_count=post.post.reply_count,
            repost_count=post.post.repost_count
        )
        transformed_latest_posts.append(transformed_post)
    return transformed_latest_posts


def get_latest_user_engagement_metrics(
    user: UserToBlueskyProfileModel
) -> UserEngagementMetricsModel:
    """Get the latest user engagement metrics for a user."""
    author_profile = client.get_profile(user.bluesky_user_did)
    latest_posts: list[PostWrittenByStudyUserModel] = get_latest_posts_written_by_user(author_profile)  # noqa
    res = {
        "user_did": user.bluesky_user_did,
        "user_handle": user.bluesky_handle,
        "latest_likes": [],
        "latest_reshares": [],
        "latest_follower_count": author_profile.followers_count,
        "latest_following_count": author_profile.follows_count,
        "latest_posts_written": latest_posts,
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

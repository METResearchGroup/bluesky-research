"""Helper functions for updating user engagement metrics in our database."""
import json
from typing import Optional

from atproto_client.models.app.bsky.feed.defs import FeedViewPost, ReasonRepost
from atproto_client.models.app.bsky.feed.like import Record as LikeRecord  # noqa
from atproto_client.models.app.bsky.feed.post import GetRecordResponse
from atproto_client.models.com.atproto.repo.list_records import Record as ListRecord  # noqa

from lib.constants import current_datetime_str
from lib.db.bluesky_models.transformations import (
    TransformedFeedViewPostModel, TransformedRecordModel
)
from lib.db.sql.participant_data_database import (
    get_user_to_bluesky_profiles
)
from lib.db.sql.user_engagement_database import (
    batch_insert_engagement_metrics,
    get_most_recent_like_timestamp,
    get_most_recent_post_timestamp
)
from lib.helper import client
from services.participant_data.models import UserToBlueskyProfileModel
from services.sync.search.helper import send_request_with_pagination
from services.update_user_bluesky_engagement.models import (
    UserLikeModel, UserLikedPostModel, PostWrittenByStudyUserModel,
    UserEngagementMetricsModel
)
from transform.bluesky_helper import (
    get_post_link_given_post_uri, get_post_record_given_post_uri
)
from transform.transform_raw_data import (
    transform_feedview_post, transform_post_record
)


def get_latest_likes_by_user(author_profile) -> tuple[list[UserLikeModel], list[UserLikedPostModel]]:  # noqa
    """Get the latest posts liked by a user."""
    collection = "app.bsky.feed.like"
    repo = author_profile.did
    params = {"collection": collection, "repo": repo}
    most_recent_liked_timestamp: Optional[str] = get_most_recent_like_timestamp(author_profile.handle)  # noqa
    if most_recent_liked_timestamp:
        recency_callback = (
            lambda like: like.value.created_at
            > most_recent_liked_timestamp
        )
    else:
        recency_callback = None

    # unhydrated likes. Returned in descending order of likes, so the
    # timestamp check will work as intended.
    res: list[ListRecord] = send_request_with_pagination(
        func=client.com.atproto.repo.list_records,
        kwargs={"params": params},
        update_params_directly=True,
        response_key="records",
        recency_callback=recency_callback,
    )
    # https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/like.py#L17 # noqa
    like_records: list[LikeRecord] = [record.value for record in res]

    # get only likes related to posts (not feeds). We can also like feeds, and
    # these are saved as well, but we don't want to include those.
    post_like_records: list[LikeRecord] = [
        like for like in like_records
        if "app.bsky.feed.post" in like.subject.uri
    ]
    print(f"Processing {len(post_like_records)} new post likes...")
    if not post_like_records or len(post_like_records) == 0:
        return ([], [])

    # hydrate the liked records and transform. We get the records instead of
    # their feedview versions. There might be some None values based on if the
    # record exists or not (e.g., it might have been deleted)
    liked_record_responses: list[Optional[GetRecordResponse]] = [
        get_post_record_given_post_uri(like.subject.uri)
        for like in post_like_records
    ]

    likes: list[UserLikeModel] = []
    liked_posts: list[UserLikedPostModel] = []  # noqa

    for like, hydrated_like_record in zip(
        post_like_records, liked_record_responses
    ):
        if hydrated_like_record is None:
            # these posts are deleted, so we skip.
            continue
        liked_record = hydrated_like_record.value
        uri = hydrated_like_record.uri
        liked_record_url: Optional[str] = get_post_link_given_post_uri(uri)
        if not liked_record_url:
            print(f"Could not get post URL for post URI: {uri}, skipping...")
            continue
        transformed_liked_record = transform_post_record(liked_record)
        like_model = UserLikeModel(
            created_at=like.created_at,
            author_did="/".join(uri.split("/")[:3]),
            author_handle=liked_record_url.split("/")[-3],
            liked_by_user_did=author_profile.did,
            liked_by_user_handle=author_profile.handle,
            uri=hydrated_like_record.uri,
            cid=hydrated_like_record.cid,
            # format of like is, e.g., 2024-06-17-18:38:16. We don't need to
            # transform this to the same format as the post created_at field
            # (e.g., 2024-06-17T02:30:13.598Z) since the divergence comes
            # after the YYYY-MM-DD part, and if two posts have the same
            # YYYY-MM-DD timestamp, then the created_at format (e.g.,
            # 2024-06-17T02:30:13.598Z) will always be greater than the
            # like_synctimestamp format (e.g., 2024-06-17-18:38:16).
            like_synctimestamp=current_datetime_str
        )
        liked_post_model = UserLikedPostModel(
            uri=hydrated_like_record.uri,
            cid=hydrated_like_record.cid,
            url=liked_record_url,
            source_feed="user_liked_posts",
            synctimestamp=current_datetime_str,
            created_at=transformed_liked_record.created_at,
            text=transformed_liked_record.text,
            embed=(
                json.dumps(transformed_liked_record.embed.dict())
                if transformed_liked_record.embed
                else None
            ),
            entities=transformed_liked_record.entities,
            facets=transformed_liked_record.facets,
            labels=transformed_liked_record.labels,
            langs=transformed_liked_record.langs,
            reply_parent=transformed_liked_record.reply_parent,
            reply_root=transformed_liked_record.reply_root,
            tags=transformed_liked_record.tags
        )
        liked_posts.append(liked_post_model)
        likes.append(like_model)

    if len(likes) != len(liked_posts):
        raise ValueError(f"The number of likes {len(likes)} does not match the number of transformed records {len(liked_posts)}")  # noqa
    # return both the like and the actual record itself. We want to record
    # both the user liking the post as well as the post that was liked itself,
    # and then store these two separately (for example, two users can each like
    # the same post, and so we want to have two separate "like" instances for
    # that single post).
    return (likes, liked_posts)


def get_post_type(post: FeedViewPost) -> str:
    """Get the type of post from a FeedViewPost."""
    if post.reason:
        # unsure what other types of "Reason" values there could be
        # so specifically checking for "ReasonRepost"
        if isinstance(post.reason, ReasonRepost):
            return "reshare"
        else:
            print(f"Unknown post reason: {post.reason}")
            return "post"
    elif post.post.record.reply:
        return "comment"
    else:
        return "post"


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
    print(f"Processing {len(res)} new posts...")
    if not res or len(res) == 0:
        return []
    transformed_latest_posts: list[PostWrittenByStudyUserModel] = []
    for post in res:
        enrichment_data = {"source_feed": None, "feed_url": None}
        post_type = get_post_type(post)
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
            repost_count=post.post.repost_count,
            post_type=post_type
        )
        transformed_latest_posts.append(transformed_post)
    return transformed_latest_posts


def get_latest_user_engagement_metrics(
    user: UserToBlueskyProfileModel
) -> UserEngagementMetricsModel:
    """Get the latest user engagement metrics for a user."""
    author_profile = client.get_profile(user.bluesky_user_did)
    latest_likes, latest_liked_posts = get_latest_likes_by_user(author_profile)
    latest_posts: list[PostWrittenByStudyUserModel] = get_latest_posts_written_by_user(author_profile)  # noqa
    res = {
        "user_did": user.bluesky_user_did,
        "user_handle": user.bluesky_handle,
        "latest_likes": latest_likes,
        "latest_liked_posts": latest_liked_posts,
        "latest_follower_count": author_profile.followers_count,
        "latest_following_count": author_profile.follows_count,
        "latest_posts_written": latest_posts,
        "latest_total_posts_written_count": author_profile.posts_count,
        "update_timestamp": current_datetime_str
    }
    return UserEngagementMetricsModel(**res)


def update_latest_user_engagement_metrics():
    users: list[UserToBlueskyProfileModel] = get_user_to_bluesky_profiles()
    for user in users:
        print('-' * 20)
        print(f"Updating engagement metrics for user {user.bluesky_handle}")
        latest_engagement_metrics = (
            get_latest_user_engagement_metrics(user=user)
        )
        batch_insert_engagement_metrics([latest_engagement_metrics])
        print(f"Successfully updated engagement metrics for user {user.bluesky_handle}")  # noqa
        print('-' * 20)

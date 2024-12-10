"""Pydantic models for tracking user engagement activity."""

from typing import Optional
import typing_extensions as te

from pydantic import BaseModel, Field

from lib.db.bluesky_models.transformations import TransformedRecordModel


class UserLikeModel(BaseModel):
    """Model for an instance of a user liking a post.

    Doesn't hydrate the actual post that is liked, just the reference to the
    post. We will hydrate and save the post separately. We do this so that we
    can model the instance of a specific user liking a specific post. Multiple
    users can like the same post, and so we want to track these instances
    separately.

    Based on # https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/like.py#L17
    """  # noqa

    created_at: str = Field(
        ...,
        description="The timestamp of when the like was created (when the user liked the post).",
    )  # noqa
    author_did: str = Field(..., description="The DID of the author of the post.")  # noqa
    author_handle: str = Field(..., description="The handle of the author of the post.")  # noqa
    liked_by_user_did: str = Field(
        ..., description="The DID of the user who liked the post."
    )  # noqa
    liked_by_user_handle: str = Field(
        ..., description="The handle of the user who liked the post."
    )  # noqa
    uri: str = Field(..., description="The URI of the post that was liked.")
    cid: str = Field(..., description="The CID of the post that was liked.")
    like_synctimestamp: str = Field(..., description="The synctimestamp of the like.")  # noqa


class UserLikedPostModel(BaseModel):
    """Model for a post that was liked by a user.

    This is a hydrated version of the post that was liked. This is useful for
    tracking the actual post that was liked, and for querying the post's
    metadata. A single post can be liked by multiple users.
    """

    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    url: str = Field(..., description="The URL of the post.")
    source_feed: str = Field(..., description="The source feed of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")
    created_at: str = Field(
        ..., description="The timestamp of when the post was created on Bluesky."
    )  # noqa
    text: str = Field(..., description="The text of the post.")
    embed: Optional[str] = Field(
        default=None, description="The embeds in the post, if any."
    )
    entities: Optional[str] = Field(
        default=None,
        description="The entities of the post, if any. Separated by a separator.",
    )  # noqa
    facets: Optional[str] = Field(
        default=None,
        description="The facets of the post, if any. Separated by a separator.",
    )  # noqa
    labels: Optional[str] = Field(
        default=None,
        description="The labels of the post, if any. Separated by a separator.",
    )  # noqa
    langs: Optional[str] = Field(
        default=None, description="The languages of the post, if specified."
    )  # noqa
    reply_parent: Optional[str] = Field(
        default=None,
        description="The parent post that the post is responding to in the thread, if any.",
    )  # noqa
    reply_root: Optional[str] = Field(
        default=None, description="The root post of the thread, if any."
    )  # noqa
    tags: Optional[str] = Field(
        default=None, description="The tags of the post, if any."
    )  # noqa


class PostWrittenByStudyUserModel(BaseModel):
    """Model for a post written by a study user.

    Note that this doesn't make a distinction between posts vs. comments vs.
    retweets/reshares, since on the Bluesky side, these are all treated as
    posts.

    We can tell if a post is a comment if there is a value in the "reply"
    field, which would tell us if the post is replying to another post (i.e.,
    it is a comment).

    We can tell if a post is a retweet/repost if, in the top-level
    FeedViewPost, the "reason" field is of type "ReasonRepost" (otherwise it is
    None, which means it is a regular post/comment).
    """

    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: str = Field(
        ..., description="The timestamp of when the post was indexed by Bluesky."
    )  # noqa
    created_at: str = Field(
        ..., description="The timestamp of when the post was created on Bluesky."
    )  # noqa
    author_did: str = Field(..., description="The DID of the author of the post.")  # noqa
    author_handle: str = Field(..., description="The handle of the author of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa
    text: str = Field(..., description="The text of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    url: str = Field(..., description="The URL of the post.")
    like_count: int = Field(..., description="The like count of the post.")
    reply_count: int = Field(..., description="The reply count of the post.")
    repost_count: int = Field(..., description="The repost count of the post.")
    post_type: te.Literal["post", "comment", "reshare"] = Field(
        ...,
        description="The type of post. Bluesky treats all as the same, so we can disambiguate between if it is a post, a comment to a post, or a reshare/retweet",  # noqa
    )


class UserEngagementMetricsModel(BaseModel):
    user_did: str = Field(..., description="The user DID.")
    user_handle: str = Field(..., description="The user handle.")
    latest_likes: list[UserLikeModel] = Field(..., description="The latest likes.")  # noqa
    latest_liked_posts: list[UserLikedPostModel] = Field(
        ..., description="The latest liked posts"
    )  # noqa
    latest_follower_count: int = Field(..., description="The latest follower count.")  # noqa
    latest_following_count: int = Field(..., description="The latest following count.")  # noqa
    latest_posts_written: list[PostWrittenByStudyUserModel] = Field(
        ..., description="The latest posts written. Includes both comments and posts."
    )  # noqa
    latest_total_posts_written_count: int = Field(
        ..., description="The latest posts written count."
    )  # noqa
    update_timestamp: str = Field(..., description="The update timestamp.")

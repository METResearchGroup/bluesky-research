"""Pydantic models for tracking user engagement activity."""
from pydantic import BaseModel, Field

from lib.db.bluesky_models.transformations import TransformedRecordModel


class PostWrittenByStudyUserModel(BaseModel):
    """Model for a post written by a study user.

    Note that this doesn't make a distinction between posts vs. comments, since
    on the Bluesky side, these are all treated as posts. For example, if a user
    comments on a post, that comment is treated as a post on the Bluesky side.

    We can tell if a post is a comment if there is a value in the "reply"
    field, which would tell us if the post is replying to another post (i.e.,
    it is a comment).
    """
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: str = Field(..., description="The timestamp of when the post was indexed by Bluesky.")  # noqa
    created_at: str = Field(..., description="The timestamp of when the post was created on Bluesky.")  # noqa
    author_did: str = Field(..., description="The DID of the author of the post.")  # noqa
    author_handle: str = Field(..., description="The handle of the author of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa
    text: str = Field(..., description="The text of the post.")
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    url: str = Field(..., description="The URL of the post.")
    like_count: int = Field(..., description="The like count of the post.")
    reply_count: int = Field(..., description="The reply count of the post.")
    repost_count: int = Field(..., description="The repost count of the post.")


class UserEngagementMetricsModel(BaseModel):
    user_did: str = Field(..., description="The user DID.")
    user_handle: str = Field(..., description="The user handle.")
    latest_likes: list[str] = Field(..., description="The latest likes.")
    latest_comments: list[str] = Field(..., description="The latest comments.")
    latest_reshares: list[str] = Field(..., description="The latest reshares.")
    latest_follower_count: int = Field(..., description="The latest follower count.")  # noqa
    latest_following_count: int = Field(..., description="The latest following count.")  # noqa
    latest_posts_written: list[str] = Field(..., description="The latest posts written.")  # noqa
    latest_total_posts_written_count: int = Field(..., description="The latest posts written count.")  # noqa
    update_timestamp: str = Field(..., description="The update timestamp.")

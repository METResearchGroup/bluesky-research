"""Models for transformed fields."""
import re
from typing import Optional
import typing_extensions as te

from pydantic import BaseModel, Field, validator

from lib.db.bluesky_models.embed import ProcessedEmbed


class TransformedProfileViewBasicModel(BaseModel):
    """Model for the transformed profile view."""
    did: str = Field(..., description="The DID of the user.")
    handle: Optional[str] = Field(default=None, description="The handle of the user.")
    avatar: Optional[str] = None
    display_name: Optional[str] = Field(
        default=None, max_length=640, description="Display name of the user."
    )
    py_type: te.Literal["app.bsky.actor.defs#profileViewBasic"] = Field(
        default="app.bsky.actor.defs#profileViewBasic", alias="$type", frozen=True  # noqa
    )


class TransformedRecordModel(BaseModel):
    """Model for the transformed record."""
    created_at: str = Field(..., description="The timestamp of when the record was created on Bluesky.")  # noqa
    text: str = Field(..., description="The text of the record.")
    embed: Optional[ProcessedEmbed] = Field(default=None, description="The embeds in the record, if any.")  # noqa
    entities: Optional[str] = Field(default=None, description="The entities of the record, if any. Separated by a separator.")  # noqa
    facets: Optional[str] = Field(default=None, description="The facets of the record, if any. Separated by a separator.")  # noqa
    labels: Optional[str] = Field(default=None, description="The labels of the record, if any. Separated by a separator.")  # noqa
    langs: Optional[str] = Field(default=None, description="The languages of the record, if specified.")  # noqa
    reply_parent: Optional[str] = Field(default=None, description="The parent post that the record is responding to in the thread, if any.")  # noqa
    reply_root: Optional[str] = Field(default=None, description="The root post of the thread, if any.")  # noqa
    tags: Optional[str] = Field(default=None, description="The tags of the record, if any.")  # noqa
    py_type: Optional[te.Literal["app.bsky.feed.post", None]] = Field(default="app.bsky.feed.post", frozen=True)  # noqa


class PostMetadataModel(BaseModel):
    url: str = Field(..., description="The URL of the post.")
    source_feed: Optional[str] = Field(default=None, description="The source feed of the post.")  # noqa
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa

    @validator('synctimestamp')
    def validate_synctimestamp(cls, v):
        if not re.match(r'^\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}$', v):
            raise ValueError("synctimestamp must be in 'YYYY-MM-DD-HH:MM:SS' format (e.g., '2024-04-23-04:41:17')")  # noqa
        return v


class TransformedRecordWithAuthorModel(BaseModel):
    """Model for the transformed record post, with author information.

    Note: the author isn't guaranteed to exist. For example, we might want to
    get posts in this format based on their Record object, which doesn't have
    author information hydrated, so in those cases, the author field would be
    blank.
    """
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    author: str = Field(..., description="The DID of the author of the post.")
    metadata: PostMetadataModel = Field(..., description="The metadata of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa

    @validator('author')
    def validate_author(cls, v):
        if not v.startswith("did:"):
            raise ValueError("Author must start with 'did:'")
        return v

    @validator('uri')
    def validate_uri(cls, v):
        if not v.startswith("at://did:"):
            raise ValueError("URI must start with 'at://did:'")
        return v


class TransformedFeedViewPostModel(BaseModel):
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    metadata: PostMetadataModel = Field(..., description="The metadata of the post.")  # noqa
    author: TransformedProfileViewBasicModel = Field(..., description="The author of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa
    indexed_at: str = Field(..., description="The timestamp of when the post was indexed by Bluesky.")  # noqa
    like_count: Optional[int] = Field(default=None, description="The like count of the post.")  # noqa
    reply_count: Optional[int] = Field(default=None, description="The reply count of the post.")  # noqa
    repost_count: Optional[int] = Field(default=None, description="The repost count of the post.")  # noqa

"""Models for consolidating post records."""
from typing import Optional
import typing_extensions as te

from pydantic import BaseModel, Field

from lib.db.bluesky_models.embed import ProcessedEmbed


class ConsolidatedPostRecordMetadataModel(BaseModel):
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    url: Optional[str] = Field(..., description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated.")  # noqa
    source: te.Literal["firehose", "most_liked"] = Field(..., description="The source feed of the post. Either 'firehose' or 'most_liked'")  # noqa


class ConsolidatedMetrics(BaseModel):
    like_count: Optional[int] = Field(
        default=None, description="The like count of the post."
    )
    reply_count: Optional[int] = Field(
        default=None, description="The reply count of the post."
    )
    repost_count: Optional[int] = Field(
        default=None, description="The repost count of the post."
    )


class ConsolidatedPostRecordModel(BaseModel):
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: Optional[str] = Field(..., description="The timestamp of when the post was indexed by Bluesky.")  # noqa
    author_did: str = Field(..., description="The DID of the user.")
    author_handle: Optional[str] = Field(default=None, description="The handle of the user.")
    author_avatar: Optional[str] = None
    author_display_name: Optional[str] = Field(
        default=None, max_length=640, description="Display name of the user."
    )
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
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    url: Optional[str] = Field(default=None, description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated.")  # noqa
    source: te.Literal["firehose", "most_liked"] = Field(..., description="The source feed of the post. Either 'firehose' or 'most_liked'")  # noqa
    like_count: Optional[int] = Field(default=None, description="The like count of the post.")  # noqa
    reply_count: Optional[int] = Field(default=None, description="The reply count of the post.")  # noqa
    repost_count: Optional[int] = Field(default=None, description="The repost count of the post.")  # noqa

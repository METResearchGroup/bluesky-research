"""Models for consolidating post records."""
from typing import Optional
import typing_extensions as te

from pydantic import BaseModel, Field

from lib.db.bluesky_models.transformations import (
    PostMetadataModel,
    TransformedProfileViewBasicModel,
    TransformedRecordModel
)


class ConsolidatedPostRecordMetadataModel(BaseModel):
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    url: Optional[str] = Field(..., description="The URL of the post. Available only if the post is from feed view. Firehose posts won't have this hydrated.")  # noqa
    source: te.Literal["firehose", "most_liked"] = Field(..., description="The source feed of the post. Either 'firehose' or 'most_liked'")  # noqa
    processed_timestamp: str = Field(..., description="The timestamp when the post was processed.")  # noqa


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
    indexed_at: str = Field(..., description="The timestamp of when the post was indexed by Bluesky.")  # noqa
    author: TransformedProfileViewBasicModel = Field(..., description="The author of the post.")  # noqa
    metadata: PostMetadataModel = Field(..., description="The metadata of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa
    metrics: Optional[ConsolidatedMetrics] = Field(default=None, description="Post engagement metrics. Only available for posts from feed view, not firehose.")  # noqa

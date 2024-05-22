"""Models for consolidating post records."""
from typing import Optional

from pydantic import BaseModel, Field

from lib.db.bluesky_models.transformations import TransformedRecordModel

# TODO: TransformedRecordWithAuthorModel is essentially the
# TransformedRecordModel but unnested and with additional info. I should keep
# the TransformedRecordModel as a record tbh. I'll need to think about how
# to do that but I'll assume that I did that already. I think that the
# new version of the TransformedRecordWithAuthorModel would look something like
# the version below:


class TransformedRecordWithAuthorModel(BaseModel):
    record: TransformedRecordModel


class ConsolidatedPostRecordMetadataModel(BaseModel):
    synctimestamp: str
    url: Optional[str]
    source: str  # either "firehose" or "most_liked_feed"
    processed_timestamp: str


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
    uri: str
    cid: str
    indexed_at: str
    author: dict
    metadata: dict
    record: dict
    metrics: Optional[ConsolidatedMetrics]

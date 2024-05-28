"""Pydantic models for the filtered posts."""

from pydantic import BaseModel, Field
from typing import Optional

from lib.db.bluesky_models.transformations import (
    TransformedProfileViewBasicModel,
    TransformedRecordModel
)
from services.consolidate_post_records.models import (
    ConsolidatedPostRecordMetadataModel, ConsolidatedMetrics
)


class FilteredPreprocessedPostModel(BaseModel):
    uri: str = Field(..., description="The URI of the post.")
    cid: str = Field(..., description="The CID of the post.")
    indexed_at: Optional[str] = Field(..., description="The timestamp of when the post was indexed by Bluesky.")  # noqa
    author: TransformedProfileViewBasicModel = Field(..., description="The author of the post.")  # noqa
    metadata: ConsolidatedPostRecordMetadataModel = Field(..., description="The metadata of the post.")  # noqa
    record: TransformedRecordModel = Field(..., description="The record of the post.")  # noqa
    metrics: Optional[ConsolidatedMetrics] = Field(default=None, description="Post engagement metrics. Only available for posts from feed view, not firehose.")  # noqa
    passed_filters: bool = Field(..., description="Indicates if the post passed the filters")  # noqa
    filtered_at: str = Field(..., description="Timestamp when the post was filtered")  # noqa
    filtered_by_func: Optional[str] = Field(None, description="Function used to filter the post")  # noqa
    synctimestamp: str = Field(..., description="The synctimestamp of the post.")  # noqa
    preprocessing_timestamp: str = Field(..., description="Timestamp when the post was preprocessed")  # noqa

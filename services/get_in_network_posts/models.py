"""Pydantic models for getting in-network posts.

Matches FilteredPreprocessedPostModel except adds an additional field
for the post's indexing timestamp.
"""
from pydantic import BaseModel, Field

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


class InNetworkPostModel(BaseModel):
    post: FilteredPreprocessedPostModel = Field(..., description="The post")
    indexed_in_network_timestamp: str = Field(..., description="Timestamp when the post was indexed as in-network")  # noqa

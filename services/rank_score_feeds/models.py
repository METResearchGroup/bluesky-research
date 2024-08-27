from pydantic import BaseModel, Field
from typing import List


class CustomFeedModel(BaseModel):
    """Model for representing a custom feed for a user."""

    user: str = Field(..., description="The user identifier (DID).")
    feed: List[tuple[str, float]] = Field(
        ..., description="List of posts in the feed along with their scores."
    )
    feed_generation_timestamp: str = Field(
        ..., description="Timestamp when the feed was generated."
    )

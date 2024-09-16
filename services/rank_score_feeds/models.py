from pydantic import BaseModel, Field
from typing import List, Optional


class ScoredPostModel(BaseModel):
    """Model for representing a scored post."""

    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the record.")
    engagement_score: Optional[float] = Field(
        default=None, description="Engagement score of the post."
    )
    treatment_score: Optional[float] = Field(
        default=None, description="Treatment score of the post."
    )
    scored_timestamp: str = Field(
        ..., description="Timestamp when the post was scored."
    )  # noqa


class CustomFeedModel(BaseModel):
    """Model for representing a custom feed for a user."""

    user: str = Field(..., description="The user identifier (DID).")
    feed: List[tuple[str, float]] = Field(
        ..., description="List of posts in the feed along with their scores."
    )
    feed_generation_timestamp: str = Field(
        ..., description="Timestamp when the feed was generated."
    )

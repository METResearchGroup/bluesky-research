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
    source: str = Field(
        ..., description="The source of the post (e.g. 'firehose', 'most_liked')."
    )
    scored_timestamp: str = Field(
        ..., description="Timestamp when the post was scored."
    )  # noqa


class CustomFeedPost(BaseModel):
    """Post in a custom feed."""

    item: str = Field(..., description="The post identifier (URI).")
    is_in_network: bool = Field(
        ..., description="Whether the post is in-network or not."
    )


class CustomFeedModel(BaseModel):
    """Model for representing a custom feed for a user."""

    # keeping 'user' field for backwards compatibility.
    user: str = Field(..., description="The user identifier (DID).")
    bluesky_handle: str = Field(..., description="The Bluesky handle of the user.")  # noqa
    bluesky_user_did: str = Field(
        ..., description="The Bluesky user DID. Same as `user`."
    )  # noqa
    condition: str = Field(..., description="The condition of the study user and feed.")  # noqa
    feed_statistics: str = Field(
        ..., description="The JSON-dumped statistics of the feed."
    )  # noqa
    feed: List[CustomFeedPost] = Field(
        ...,
        description="List of posts in the feed along with if the post is in-network or not.",  # noqa
    )
    feed_generation_timestamp: str = Field(
        ..., description="Timestamp when the feed was generated."
    )

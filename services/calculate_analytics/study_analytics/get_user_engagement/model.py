"""Models for calculating user engagement metrics."""

from pydantic import BaseModel, Field


class UserEngagement(BaseModel):
    """Superclass encompassing all types of user engagement (e.g., like/repost/post/etc.)."""

    did: str = Field(description="The DID of the user.")
    handle: str = Field(description="The handle of the user.")
    engagement_type: str = Field(description="The type of engagement.")
    engagement_record_uri: str = Field(description="The URI of the engagement record.")
    related_post_uri: str = Field(description="The URI of the related post.")
    value: str = Field(
        description="The value of the engagement. JSON-dumped version of the synced record."
    )

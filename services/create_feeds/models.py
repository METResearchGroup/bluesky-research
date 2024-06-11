"""Pydantic models for the feeds."""
from pydantic import BaseModel, Field

from services.ml_inference.models import RecordClassificationMetadataModel
from services.participant_data.models import UserToBlueskyProfileModel


class UserFeedModel(BaseModel):
    user: UserToBlueskyProfileModel = Field(..., description="The user.")
    feed: list[RecordClassificationMetadataModel] = Field(..., description="The feed for the user.")  # noqa


class CreatedFeedModel(BaseModel):
    """Model for the created feed."""
    bluesky_user_did: str = Field(..., description="The Bluesky user DID.")
    feed_uris: str = Field(..., description="The URIs of the feed.")
    timestamp: str = Field(..., description="The timestamp of the feed creation.")  # noqa

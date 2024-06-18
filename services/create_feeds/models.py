"""Pydantic models for the feeds."""
from pydantic import BaseModel, Field

from services.ml_inference.models import RecordClassificationMetadataModel
from services.participant_data.models import UserToBlueskyProfileModel


class UserFeedModel(BaseModel):
    user: UserToBlueskyProfileModel = Field(..., description="The user.")
    feed: list[RecordClassificationMetadataModel] = Field(..., description="The feed for the user.")  # noqa


class CreatedFeedModel(BaseModel):
    """Model for the created feed."""
    study_user_id: str = Field(..., description="The study user ID.")
    bluesky_user_did: str = Field(..., description="The Bluesky user DID.")
    bluesky_user_handle: str = Field(..., description="The Bluesky user handle.") # noqa
    condition: str = Field(..., description="The condition the user is in.")
    feed_uris: str = Field(..., description="The URIs of the feed.")
    timestamp: str = Field(..., description="The timestamp of the feed creation.")  # noqa

"""Pydantic models for storing user profile data."""
from pydantic import BaseModel, Field
import typing_extensions as te


class UserToBlueskyProfileModel(BaseModel):
    """Stores user profile data.

    This model is used to store user profile data for users in the study.
    """
    study_user_id: str = Field(..., description="The ID of the user in the study.")  # noqa
    condition: te.Literal[
        "reverse_chronological", "engagement", "representative_diversification"
    ] = Field(..., description="The condition the user is in.")
    bluesky_handle: str = Field(..., description="The Bluesky handle of the user.")  # noqa
    bluesky_user_did: str = Field(..., description="The Bluesky user DID of the user.")  # noqa

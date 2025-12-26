"""Pydantic models for storing user profile data."""

from typing import Optional

from pydantic import BaseModel, Field
import typing_extensions as te


class CreateUserRequestModel(BaseModel):
    """Request model for creating a user."""

    bluesky_handle: str
    condition: str
    bluesky_user_did: str
    is_study_user: Optional[bool] = True


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
    is_study_user: bool = Field(
        True,
        description="Whether the user is a study user (as opposed to a mock user for testing purposes)",  # noqa
    )
    created_timestamp: str = Field(
        ..., description="The timestamp when the user was created."
    )  # noqa


class UserOperation(BaseModel):
    """Base class for controlling API operations that we can perform when
    adding a new user to the study.."""

    operation: str = Field(..., description="The operation to perform.")
    condition: Optional[
        te.Literal[
            "reverse_chronological", "engagement", "representative_diversification"
        ]
    ] = Field(..., description="The condition the user is in.")
    bluesky_user_profile_link: str = Field(
        ..., description="The link to the user's profile in Bluesky."
    )  # noqa
    is_study_user: Optional[bool] = Field(
        default=True,
        description="Whether the user is a study user or a test user.",  # noqa
    )


class SocialNetworkRelationshipModel(BaseModel):
    """Minimal model for social network relationship processing.

    Contains only the fields needed for building the user social network map.
    """

    relationship_to_study_user: te.Literal["follower", "follow"] = Field(
        ...,
        description="The relationship type: 'follower' means the follower is a study user, 'follow' means the followee is a study user.",
    )
    follow_did: str = Field(..., description="The DID of the user being followed.")
    follower_did: str = Field(..., description="The DID of the user who is following.")

"""Pydantic models for updating network connections."""
import typing_extensions as te

from pydantic import BaseModel, Field


class ConnectionModel(BaseModel):
    """Pydantic model for a single connection (followee/follower)."""
    connection_did: str = Field(..., description="The DID of the connection.")
    connection_handle: str = Field(..., description="The handle of the connection.")  # noqa
    connection_display_name: str = Field(..., description="The display name of the connection.")  # noqa
    connection_type: te.Literal["follow", "follower"] = Field(..., description="The type of the connection.")  # noqa
    synctimestamp: str = Field(..., description="The synctimestamp of the connection.")  # noqa


class UserToConnectionModel(BaseModel):
    """Pydantic model for the user to connection mapping."""
    study_user_id: str = Field(..., description="The study user ID.")
    user_did: str = Field(..., description="The DID of the user.")
    user_handle: str = Field(..., description="The handle of the user.")
    connection_did: str = Field(..., description="The DID of the connection.")
    connection_handle: str = Field(..., description="The handle of the connection.")  # noqa
    connection_display_name: str = Field(..., description="The display name of the connection.")  # noqa
    user_follows_connection: bool = Field(..., description="Whether the user follows the connection.")  # noqa
    connection_follows_user: bool = Field(..., description="Whether the connection follows the user.")  # noqa
    synctimestamp: str = Field(..., description="The synctimestamp of the connection.")  # noqa


class UserSocialNetworkCountsModel(BaseModel):
    """Pydantic model for the user social network counts."""
    study_user_id: str = Field(..., description="The study user ID.")
    user_did: str = Field(..., description="The DID of the user.")
    user_handle: str = Field(..., description="The handle of the user.")
    user_followers_count: int = Field(..., description="The number of followers the user has.")  # noqa
    user_following_count: int = Field(..., description="The number of users the user is following.")  # noqa
    synctimestamp: str = Field(..., description="The synctimestamp of the counts.")  # noqa

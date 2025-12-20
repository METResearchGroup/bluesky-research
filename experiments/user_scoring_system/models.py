"""Pydantic models for the experimental user scoring system.

This module defines the data models used throughout the user scoring system
for request/response validation and data structure definition.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class User(BaseModel):
    """User model for storing user profile information.
    
    Attributes:
        user_id: Unique identifier for the user
        handle: User's social media handle (e.g., @username)
        display_name: User's display name
        bio: User's bio/description
        avatar_url: URL to user's avatar image
        score: User's calculated score based on activity
        created_at: Timestamp when user was created
        updated_at: Timestamp when user was last updated
    """
    user_id: str = Field(..., description="Unique identifier for the user")
    handle: str = Field(..., description="User's social media handle")
    display_name: str = Field(..., description="User's display name")
    bio: Optional[str] = Field(None, description="User's bio/description")
    avatar_url: Optional[str] = Field(None, description="URL to user's avatar")
    score: int = Field(default=0, description="User's calculated score")
    created_at: datetime = Field(default_factory=datetime.now, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.now, description="Last update timestamp")


class Post(BaseModel):
    """Post model for storing user posts.
    
    Attributes:
        post_id: Unique identifier for the post
        user_id: ID of the user who created the post
        content: Text content of the post
        created_at: Timestamp when post was created
    """
    post_id: str = Field(..., description="Unique identifier for the post")
    user_id: str = Field(..., description="ID of the user who created the post")
    content: str = Field(..., description="Text content of the post")
    created_at: datetime = Field(default_factory=datetime.now, description="Creation timestamp")


class UserResponse(BaseModel):
    """Response model for user profile API endpoints.
    
    This model is used for API responses to ensure consistent formatting
    and include all necessary user information.
    """
    user_id: str
    handle: str
    display_name: str
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    score: int
    post_count: int
    created_at: datetime
    updated_at: datetime


class ScoreResponse(BaseModel):
    """Response model for score-specific API endpoints.
    
    Attributes:
        user_id: ID of the user
        handle: User's handle
        score: User's current score
        post_count: Number of posts used to calculate score
        last_calculated: When the score was last calculated
    """
    user_id: str
    handle: str
    score: int
    post_count: int
    last_calculated: datetime


class CreateUserRequest(BaseModel):
    """Request model for creating a new user.
    
    Attributes:
        handle: User's social media handle
        display_name: User's display name
        bio: Optional bio/description
        avatar_url: Optional avatar URL
    """
    handle: str = Field(..., description="User's social media handle")
    display_name: str = Field(..., description="User's display name")
    bio: Optional[str] = Field(None, description="User's bio/description")
    avatar_url: Optional[str] = Field(None, description="URL to user's avatar")


class CreatePostRequest(BaseModel):
    """Request model for creating a new post.
    
    Attributes:
        user_id: ID of the user creating the post
        content: Text content of the post
    """
    user_id: str = Field(..., description="ID of the user creating the post")
    content: str = Field(..., description="Text content of the post")


class ErrorResponse(BaseModel):
    """Standard error response model.
    
    Attributes:
        error: Error message
        detail: Additional error details
        code: Error code for programmatic handling
    """
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Additional error details")
    code: Optional[str] = Field(None, description="Error code")
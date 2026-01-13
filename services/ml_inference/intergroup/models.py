from typing import Optional

from pydantic import BaseModel, Field


class IntergroupLabelModel(BaseModel):
    """Model for intergroup label."""

    uri: str = Field(..., description="The URI of the post.")
    text: str = Field(..., description="The text of the post.")
    preprocessing_timestamp: str = Field(
        ..., description="The preprocessing_timestamp of the post."
    )
    was_successfully_labeled: bool = Field(
        ..., description="Whether the post was successfully labeled."
    )
    reason: Optional[str] = Field(..., description="The reason for the label.")
    label: int = Field(..., description="The label for the post.")

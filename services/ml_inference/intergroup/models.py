from typing import Optional

from pydantic import BaseModel, Field


class LabelChoiceModel(BaseModel):
    """Lightweight model for LLM response containing only the label choice."""

    label: int = Field(..., description="The binary label for the post (0 or 1).")


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
    reason: Optional[str] = Field(default=None, description="The reason for the label.")
    label: int = Field(..., description="The label for the post.")

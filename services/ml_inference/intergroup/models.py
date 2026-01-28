from typing import Optional

from pydantic import BaseModel, Field


class LabelChoiceModel(BaseModel):
    """Lightweight model for LLM response containing only the label choice."""

    label: int = Field(..., description="The binary label for the post (0 or 1).")


class BatchedLabelChoiceModel(BaseModel):
    """Model for batched label choice."""

    labels: list[LabelChoiceModel] = Field(..., description="The labels for the posts.")


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
    label_timestamp: str = Field(
        ..., description="The timestamp when the label was generated."
    )
    reason: Optional[str] = Field(default=None, description="The reason for the label.")
    label: int = Field(..., description="The label for the post.")

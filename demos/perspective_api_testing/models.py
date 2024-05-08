"""Model classes for Perspective API classification."""

from pydantic import BaseModel, Field


class PerspectiveAPIClassification(BaseModel):
    """Pydantic model for the Perspective API classification."""
    uri: str = Field(..., description="Unique identifier for the post")
    text: str = Field(..., description="Text content of the post")
    classifications: dict = Field(..., description="Classifications from the Perspective API")  # noqa

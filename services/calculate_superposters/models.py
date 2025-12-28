"""Models for calculating superposters."""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class CalculateSuperposterSource(str, Enum):
    """Source for loading superposter data."""

    LOCAL = "local"
    REMOTE = "remote"


class SuperposterModel(BaseModel):
    author_did: str = Field(..., description="The DID of the superposter")
    count: int = Field(..., description="The number of posts by this superposter")


class SuperposterCalculationModel(BaseModel):
    insert_date_timestamp: str = Field(
        ..., description="Timestamp of when the calculation was inserted"
    )
    insert_date: str = Field(..., description="Date of the calculation insertion")
    superposters: List[SuperposterModel] = Field(
        ..., description="List of superposters"
    )
    method: str = Field(
        ..., description="Method used for calculation: 'top_n_percent' or 'threshold'"
    )
    top_n_percent: Optional[float] = Field(
        None, description="Percentage used if method is 'top_n_percent'"
    )
    threshold: Optional[int] = Field(
        None, description="Threshold used if method is 'threshold'"
    )

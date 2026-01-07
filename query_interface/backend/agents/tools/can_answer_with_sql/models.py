from pydantic import BaseModel, Field


class SQLAnswerabilityResult(BaseModel):
    """Result indicating whether a query can be answered with SQL."""

    can_answer: bool = Field(description="Whether the query can be answered with SQL")
    reason: str = Field(description="Brief explanation for the decision")

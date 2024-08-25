"""Models for generating vector embeddings."""

from pydantic import BaseModel, Field


class PostSimilarityScoreModel(BaseModel):
    """Pydantic model for post similarity scores."""

    uri: str = Field(..., description="The URI of the post.")
    similarity_score: float = Field(
        ..., description="Cosine similarity score of the post."
    )
    insert_timestamp: str = Field(
        ..., description="Timestamp when the similarity score was inserted."
    )
    most_liked_average_embedding_key: str = Field(
        ...,
        description="S3 key of the average most liked feed embedding used for comparison.",
    )

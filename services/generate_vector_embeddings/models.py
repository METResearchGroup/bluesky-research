"""Models for generating vector embeddings."""

from pydantic import BaseModel, Field, field_validator


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


class PostEmbeddingModel(BaseModel):
    """One post row stored in Parquet or similar after offline embedding."""

    uri: str = Field(..., description="AT URI of the embedded post.")
    embedding: list[float] = Field(..., description="Dense embedding vector.")
    embedding_model: str = Field(
        ..., description="Model identifier (e.g. Hugging Face model id)."
    )
    embedding_model_revision: str = Field(
        ...,
        description="Model revision (Git ref, revision hash, or snapshot id).",
    )
    embedding_schema_version: str = Field(
        ...,
        description="Pipeline/schema version for reproducibility across code changes.",
    )
    insert_timestamp: str = Field(
        ..., description="Timestamp when this embedding row was written."
    )

    @field_validator("embedding")
    @classmethod
    def embedding_non_empty(cls, value: list[float]) -> list[float]:
        if not value:
            raise ValueError("embedding must be non-empty")
        return value


class EmbeddingSessionModel(BaseModel):
    """Batch embedding job metadata (e.g. DynamoDB table vector_embedding_sessions)."""

    embedding_timestamp: str = Field(
        ...,
        description="Session timestamp; typically matches artifact path segments.",
    )
    total_embedded_posts: int = Field(
        ..., ge=0, description="Total posts embedded in this session."
    )
    s3_keys: dict[str, str] = Field(
        ...,
        description="Logical artifact name to S3 object key (embeddings, similarity, etc.).",
    )
    total_embedded_posts_by_source: dict[str, int] | None = Field(
        default=None,
        description="Optional counts by ingestion source (e.g. firehose vs most_liked).",
    )
    embedding_model: str | None = Field(
        default=None,
        description="Embedding model id for this session.",
    )
    embedding_model_revision: str | None = Field(
        default=None,
        description="Revision of embedding_model for this session.",
    )
    embedding_schema_version: str | None = Field(
        default=None,
        description="Embedding schema version for this session.",
    )


class AnnIndexSessionModel(BaseModel):
    """Metadata for a built approximate nearest-neighbour index artifact."""

    index_s3_key: str = Field(
        ..., description="S3 key of the serialized ANN index (binary)."
    )
    uri_mapping_s3_key: str = Field(
        ...,
        description="S3 key of the URI-to-vector-row mapping artifact.",
    )
    embedding_source_keys: dict[str, str] = Field(
        ...,
        description="S3 keys (or logical paths) of embedding inputs used to build the index.",
    )
    embedding_model: str = Field(
        ..., description="Model id aligned with PostEmbeddingModel.embedding_model."
    )
    embedding_model_revision: str = Field(
        ...,
        description="Model revision aligned with PostEmbeddingModel.embedding_model_revision.",
    )
    vector_dimension: int = Field(
        ..., gt=0, description="Embedding vector dimensionality."
    )
    distance_metric: str = Field(
        ...,
        description="Metric or preset used by the index (e.g. cosine, inner_product, l2).",
    )
    creation_timestamp: str = Field(
        ..., description="Timestamp when the index was built and persisted."
    )
    embedding_schema_version: str | None = Field(
        default=None,
        description="Embedding schema version aligned with PostEmbeddingModel.",
    )
    ann_backend: str | None = Field(
        default=None,
        description="ANN library identifier (e.g. faiss, hnswlib).",
    )


class QueryEmbeddingModel(BaseModel):
    """Query or profile vector used for ANN retrieval against post embeddings."""

    query_id: str = Field(
        ...,
        description="Stable id (e.g. global centroid label or user DID).",
    )
    query_type: str = Field(
        ...,
        description="Query category (e.g. global_most_liked_centroid, user_profile).",
    )
    embedding: list[float] = Field(..., description="Query/profile embedding vector.")
    embedding_model: str = Field(
        ...,
        description="Model id for item embeddings this query is comparable to.",
    )
    embedding_model_revision: str = Field(
        ...,
        description="Revision of embedding_model.",
    )
    insert_timestamp: str = Field(
        ...,
        description="When this query vector was computed or exported.",
    )
    embedding_schema_version: str | None = Field(
        default=None,
        description="Embedding schema version for reproducibility.",
    )
    source_artifact_key: str | None = Field(
        default=None,
        description="S3 key of upstream artifact (e.g. averaged embedding Parquet).",
    )
    source_post_uris: list[str] | None = Field(
        default=None,
        description="Post URIs aggregated into this vector when applicable.",
    )

    @field_validator("embedding")
    @classmethod
    def embedding_non_empty(cls, value: list[float]) -> list[float]:
        if not value:
            raise ValueError("embedding must be non-empty")
        return value

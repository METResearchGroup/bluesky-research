"""Tests for services/generate_vector_embeddings/models.py."""

import pytest
from pydantic import ValidationError

from services.generate_vector_embeddings.models import (
    AnnIndexSessionModel,
    EmbeddingSessionModel,
    PostEmbeddingModel,
    PostSimilarityScoreModel,
    QueryEmbeddingModel,
)


class TestPostSimilarityScoreModel:
    """Backward-compatible construction for downstream loaders."""

    def test_minimal_valid(self) -> None:
        model = PostSimilarityScoreModel(
            uri="at://did/post",
            similarity_score=0.73,
            insert_timestamp="2024-01-01T00:00:00Z",
            most_liked_average_embedding_key="vector_embeddings/average/foo.parquet",
        )
        assert model.uri == "at://did/post"
        assert model.similarity_score == pytest.approx(0.73)


class TestPostEmbeddingModel:
    def test_requires_all_fields(self) -> None:
        with pytest.raises(ValidationError):
            PostEmbeddingModel(
                uri="at://x",
                embedding=[0.1, 0.2],
                embedding_model="bert-base-uncased",
                embedding_model_revision="main",
                # missing embedding_schema_version, insert_timestamp
            )

    def test_rejects_empty_embedding(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            PostEmbeddingModel(
                uri="at://x",
                embedding=[],
                embedding_model="bert-base-uncased",
                embedding_model_revision="main",
                embedding_schema_version="v1",
                insert_timestamp="2024-01-01T00:00:00Z",
            )

    def test_minimal_valid(self) -> None:
        row = PostEmbeddingModel(
            uri="at://did/post",
            embedding=[0.0, 1.0, -0.5],
            embedding_model="bert-base-uncased",
            embedding_model_revision="main",
            embedding_schema_version="v1",
            insert_timestamp="2024-01-01T00:00:00Z",
        )
        assert len(row.embedding) == 3


class TestEmbeddingSessionModel:
    def test_requires_s3_keys(self) -> None:
        with pytest.raises(ValidationError):
            EmbeddingSessionModel(
                embedding_timestamp="2024-01-01T00:00:00Z",
                total_embedded_posts=1,
            )

    def test_minimal_valid(self) -> None:
        session = EmbeddingSessionModel(
            embedding_timestamp="2024-01-01T00:00:00Z",
            total_embedded_posts=10,
            s3_keys={"post_embeddings": "vector_embeddings/post_embeddings/x.parquet"},
        )
        assert session.total_embedded_posts == 10
        assert session.embedding_model is None


class TestAnnIndexSessionModel:
    def test_requires_positive_dimension(self) -> None:
        with pytest.raises(ValidationError):
            AnnIndexSessionModel(
                index_s3_key="vector_embeddings/ann/index.bin",
                uri_mapping_s3_key="vector_embeddings/ann/uris.parquet",
                embedding_source_keys={"post_embeddings": "vector_embeddings/post/x.parquet"},
                embedding_model="bert-base-uncased",
                embedding_model_revision="main",
                vector_dimension=0,
                distance_metric="cosine",
                creation_timestamp="2024-01-01T00:00:00Z",
            )

    def test_minimal_valid(self) -> None:
        meta = AnnIndexSessionModel(
            index_s3_key="vector_embeddings/ann_indices/v1/index.faiss",
            uri_mapping_s3_key="vector_embeddings/ann_indices/v1/uri_map.parquet",
            embedding_source_keys={
                "post_embeddings": "vector_embeddings/post_embeddings/v1/t.parquet",
            },
            embedding_model="bert-base-uncased",
            embedding_model_revision="main",
            vector_dimension=768,
            distance_metric="inner_product",
            creation_timestamp="2024-01-01T00:00:00Z",
            ann_backend="faiss",
        )
        assert meta.vector_dimension == 768


class TestQueryEmbeddingModel:
    def test_rejects_empty_embedding(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            QueryEmbeddingModel(
                query_id="global_most_liked_centroid",
                query_type="global_most_liked_centroid",
                embedding=[],
                embedding_model="bert-base-uncased",
                embedding_model_revision="main",
                insert_timestamp="2024-01-01T00:00:00Z",
            )

    def test_source_artifact_optional(self) -> None:
        q = QueryEmbeddingModel(
            query_id="did:plc:user",
            query_type="user_profile",
            embedding=[0.1] * 768,
            embedding_model="bert-base-uncased",
            embedding_model_revision="main",
            insert_timestamp="2024-01-01T00:00:00Z",
            source_post_uris=["at://a/1", "at://b/2"],
        )
        assert q.source_artifact_key is None
        assert len(q.source_post_uris) == 2

"""Offline query / profile vectors (global centroid first; per-user extension later)."""

from __future__ import annotations

import numpy as np

from services.generate_vector_embeddings.ann_index import l2_normalize_numpy
from services.generate_vector_embeddings.models import QueryEmbeddingModel


def build_global_most_liked_centroid(normalized_vectors: np.ndarray) -> np.ndarray:
    """Mean of row-wise normalized embeddings, renormalized for cosine geometry."""
    if normalized_vectors.size == 0:
        raise ValueError("Cannot build centroid from zero vectors.")
    if normalized_vectors.ndim != 2:
        raise ValueError("normalized_vectors must be a 2-D matrix.")
    mean = normalized_vectors.mean(axis=0)
    return l2_normalize_numpy(mean.reshape(1, -1))[0]


def global_centroid_to_query_embedding_model(
    *,
    centroid: np.ndarray,
    source_post_uris: list[str],
    source_artifact_key: str,
    embedding_model: str,
    embedding_model_revision: str,
    insert_timestamp: str,
    embedding_schema_version: str | None,
    query_id: str = "global_most_liked_centroid",
    query_type: str = "global_most_liked_centroid",
) -> QueryEmbeddingModel:
    """Package a centroid as a reproducible ``QueryEmbeddingModel``."""
    vec = centroid.astype(float).reshape(-1).tolist()
    return QueryEmbeddingModel(
        query_id=query_id,
        query_type=query_type,
        embedding=vec,
        embedding_model=embedding_model,
        embedding_model_revision=embedding_model_revision,
        insert_timestamp=insert_timestamp,
        embedding_schema_version=embedding_schema_version,
        source_artifact_key=source_artifact_key,
        source_post_uris=list(source_post_uris),
    )


def build_user_profile_vector(
    user_did: str,
    _activity_rows: list[dict] | None = None,
    _uri_to_embedding: dict[str, np.ndarray] | None = None,
) -> QueryEmbeddingModel:
    """Reserved for per-user profiles built offline from cached embeddings."""
    raise NotImplementedError(
        f"Per-user profile vectors are not implemented yet (user_did={user_did!r}); "
        "compute offline by aggregating engaged post URIs for the DID and averaging "
        "their cached embedding vectors."
    )

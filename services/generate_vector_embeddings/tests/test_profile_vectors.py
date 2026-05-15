"""Tests for offline profile / query vectors."""

from __future__ import annotations

import numpy as np
import pytest

from services.generate_vector_embeddings.ann_index import l2_normalize_numpy
from services.generate_vector_embeddings.profile_vectors import (
    build_global_most_liked_centroid,
    build_user_profile_vector,
    global_centroid_to_query_embedding_model,
)


def test_global_centroid_is_normalized():
    rng = np.random.default_rng(1)
    raw = rng.standard_normal((5, 8)).astype(np.float32)
    normed = l2_normalize_numpy(raw)
    centroid = build_global_most_liked_centroid(normed)
    assert centroid.shape == (8,)
    norm = float(np.linalg.norm(centroid))
    assert norm == pytest.approx(1.0, abs=1e-5)


def test_global_centroid_empty_raises():
    with pytest.raises(ValueError, match="zero vectors"):
        build_global_most_liked_centroid(np.zeros((0, 4), dtype=np.float32))


def test_query_embedding_model_carries_metadata():
    c = np.array([1.0, 0.0], dtype=np.float32)
    q = global_centroid_to_query_embedding_model(
        centroid=c,
        source_post_uris=["a", "b"],
        source_artifact_key="vector_embeddings/average/x.parquet",
        embedding_model="bert-base-uncased",
        embedding_model_revision="main",
        insert_timestamp="ts",
        embedding_schema_version="v1",
    )
    assert q.query_type == "global_most_liked_centroid"
    assert q.source_post_uris == ["a", "b"]
    assert q.source_artifact_key.endswith(".parquet")
    assert len(q.embedding) == 2


def test_build_user_profile_vector_not_implemented():
    with pytest.raises(NotImplementedError):
        build_user_profile_vector("did:plc:test")

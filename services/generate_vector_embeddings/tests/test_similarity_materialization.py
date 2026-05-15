"""Tests for similarity materialization (numpy / ANN only, no Transformers)."""

from __future__ import annotations

import importlib
import sys

import numpy as np
import pytest

from services.generate_vector_embeddings.ann_index import FaissFlatIPIndex
from services.generate_vector_embeddings.similarity_materialization import (
    clamp_cosine_similarity,
    dedupe_neighbors_keep_best_score,
    materialize_ann_backend_query,
    materialize_ann_search_to_models,
    materialize_inner_product_scores_for_posts,
)


def test_clamp_cosine_similarity_bounds():
    assert clamp_cosine_similarity(2.0) == 1.0
    assert clamp_cosine_similarity(-2.0) == -1.0
    assert clamp_cosine_similarity(float("nan")) == -1.0


def test_dedupe_neighbors_keeps_max_score():
    uris = ["a", "b", "c"]
    indices = np.array([0, 1, 0], dtype=np.int64)
    scores = np.array([0.5, 0.2, 0.9], dtype=np.float32)
    pairs = dedupe_neighbors_keep_best_score(indices, scores, uris)
    assert pairs[0] == ("a", pytest.approx(0.9))
    assert pairs[1] == ("b", pytest.approx(0.2))


def test_dedupe_neighbors_skips_negative_indices():
    uris = ["a"]
    indices = np.array([-1, 0], dtype=np.int64)
    scores = np.array([1.0, 0.7], dtype=np.float32)
    pairs = dedupe_neighbors_keep_best_score(indices, scores, uris)
    assert pairs == [("a", pytest.approx(0.7))]


def test_materialize_ann_search_validates_models():
    uris = ["u1", "u2"]
    indices = np.array([[1, 0]], dtype=np.int64)
    scores = np.array([[0.3, 0.9]], dtype=np.float32)
    models = materialize_ann_search_to_models(
        indices=indices,
        scores=scores,
        uri_by_ann_row=uris,
        insert_timestamp="ts",
        query_source_s3_key="s3://bucket/query.json",
    )
    assert len(models) == 2
    assert {m.uri for m in models} == {"u1", "u2"}
    assert all(-1.0 <= m.similarity_score <= 1.0 for m in models)
    for m in models:
        assert m.most_liked_average_embedding_key == "s3://bucket/query.json"


def test_materialize_ann_backend_end_to_end():
    dim = 4
    vectors = np.eye(dim, dtype=np.float32)
    idx = FaissFlatIPIndex(dim)
    idx.build(vectors)
    uris = [f"at://{i}" for i in range(dim)]
    query = vectors[2].copy()

    models = materialize_ann_backend_query(
        backend=idx,
        query_vector=query,
        uri_by_ann_row=uris,
        top_k=2,
        insert_timestamp="ts",
        query_source_s3_key="query-key",
    )
    assert models[0].uri == "at://2"
    assert models[0].similarity_score > models[1].similarity_score


def test_materialize_inner_product_scores_for_posts():
    q = np.array([1.0, 0.0], dtype=np.float32)
    mat = np.array([[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0]], dtype=np.float32)
    uris = ["a", "b", "c"]
    models = materialize_inner_product_scores_for_posts(
        query_vector=q,
        post_uris=uris,
        post_vectors=mat,
        insert_timestamp="ts",
        query_source_s3_key="k",
    )
    scores = {m.uri: m.similarity_score for m in models}
    assert scores["a"] == pytest.approx(1.0)
    assert scores["b"] == pytest.approx(0.0)
    assert scores["c"] == pytest.approx(-1.0)


def test_similarity_materialization_import_does_not_pull_transformers():
    mod_name = "services.generate_vector_embeddings.similarity_materialization"
    sys.modules.pop(mod_name, None)
    importlib.import_module(mod_name)
    assert "transformers" not in sys.modules

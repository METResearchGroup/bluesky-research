"""Offline similarity score materialization from ANN search or cached vectors.

Scores are **inner products of L2-normalized vectors**, matching cosine
similarity in [-1.0, 1.0]. This module intentionally avoids Torch / Transformers.

``PostSimilarityScoreModel.most_liked_average_embedding_key`` stores the S3 key
(or other stable locator) of the **query/profile vector artifact** used for that
materialization pass (historically the averaged most-liked embedding object).
"""

from __future__ import annotations

import math

import numpy as np

from services.generate_vector_embeddings.ann_index import AnnIndexBackend
from services.generate_vector_embeddings.models import PostSimilarityScoreModel

SIMILARITY_SCORE_MIN = -1.0
SIMILARITY_SCORE_MAX = 1.0


def clamp_cosine_similarity(value: float) -> float:
    """Clamp inner-product / cosine scores to a stable numeric range."""
    if math.isnan(value):
        return SIMILARITY_SCORE_MIN
    return max(SIMILARITY_SCORE_MIN, min(SIMILARITY_SCORE_MAX, value))


def dedupe_neighbors_keep_best_score(
    neighbor_indices: np.ndarray,
    neighbor_scores: np.ndarray,
    uri_by_ann_row: list[str],
) -> list[tuple[str, float]]:
    """Collapse duplicate neighbour URIs, keeping the highest similarity."""
    best: dict[str, float] = {}
    for ix, raw_score in zip(neighbor_indices.tolist(), neighbor_scores.tolist()):
        if ix < 0:
            continue
        if ix >= len(uri_by_ann_row):
            continue
        uri = uri_by_ann_row[ix]
        score = clamp_cosine_similarity(float(raw_score))
        prev = best.get(uri)
        if prev is None or score > prev:
            best[uri] = score
    return sorted(best.items(), key=lambda item: (-item[1], item[0]))


def materialize_ann_search_to_models(
    *,
    indices: np.ndarray,
    scores: np.ndarray,
    uri_by_ann_row: list[str],
    insert_timestamp: str,
    query_source_s3_key: str,
    query_row_index: int = 0,
) -> list[PostSimilarityScoreModel]:
    """Turn one row of FAISS ``search`` output into validated similarity rows."""
    if indices.ndim != 2 or scores.ndim != 2:
        raise ValueError("indices and scores must be 2-D (queries × neighbors).")
    if query_row_index < 0 or query_row_index >= indices.shape[0]:
        raise ValueError("query_row_index out of range.")

    pairs = dedupe_neighbors_keep_best_score(
        indices[query_row_index],
        scores[query_row_index],
        uri_by_ann_row,
    )
    return [
        PostSimilarityScoreModel(
            uri=uri,
            similarity_score=score,
            insert_timestamp=insert_timestamp,
            most_liked_average_embedding_key=query_source_s3_key,
        )
        for uri, score in pairs
    ]


def materialize_ann_backend_query(
    *,
    backend: AnnIndexBackend,
    query_vector: np.ndarray,
    uri_by_ann_row: list[str],
    top_k: int,
    insert_timestamp: str,
    query_source_s3_key: str,
) -> list[PostSimilarityScoreModel]:
    """Run a single ANN query and emit ``PostSimilarityScoreModel`` rows."""
    if query_vector.ndim != 1:
        raise ValueError("query_vector must be 1-D.")
    q = np.ascontiguousarray(query_vector.reshape(1, -1), dtype=np.float32)
    scores, indices = backend.query(q, top_k)
    return materialize_ann_search_to_models(
        indices=indices,
        scores=scores,
        uri_by_ann_row=uri_by_ann_row,
        insert_timestamp=insert_timestamp,
        query_source_s3_key=query_source_s3_key,
        query_row_index=0,
    )


def materialize_inner_product_scores_for_posts(
    *,
    query_vector: np.ndarray,
    post_uris: list[str],
    post_vectors: np.ndarray,
    insert_timestamp: str,
    query_source_s3_key: str,
) -> list[PostSimilarityScoreModel]:
    """Exact cosine / inner-product scores for arbitrary candidate posts (offline)."""
    if len(post_uris) != post_vectors.shape[0]:
        raise ValueError("post_uris length must match post_vectors row count.")
    if query_vector.ndim != 1:
        raise ValueError("query_vector must be 1-D.")
    q = query_vector.astype(np.float32, copy=False)
    mat = post_vectors.astype(np.float32, copy=False)
    raw = mat @ q

    rows: list[PostSimilarityScoreModel] = []
    for uri, score in zip(post_uris, raw.tolist(), strict=True):
        rows.append(
            PostSimilarityScoreModel(
                uri=uri,
                similarity_score=clamp_cosine_similarity(float(score)),
                insert_timestamp=insert_timestamp,
                most_liked_average_embedding_key=query_source_s3_key,
            )
        )
    return rows

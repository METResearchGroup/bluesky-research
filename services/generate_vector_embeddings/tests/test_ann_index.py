"""Tests for FAISS-based ANN indexing."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from services.generate_vector_embeddings.ann_index import (
    FaissFlatIPIndex,
    FaissHNSWIPIndex,
    choose_ann_backend,
    concat_embeddings_from_s3_parquets,
    load_ann_index_backend_from_s3,
    load_post_embeddings_numpy_from_dataframe,
    l2_normalize_numpy,
    persist_ann_artifacts,
    uri_mapping_dataframe,
)
from services.generate_vector_embeddings.models import AnnIndexSessionModel


def test_flat_ip_returns_obvious_nearest_neighbor():
    dim = 8
    vectors = np.eye(dim, dtype=np.float32)
    vectors = l2_normalize_numpy(vectors)
    query = vectors[3].reshape(1, -1)

    index = FaissFlatIPIndex(dim)
    index.build(vectors)
    scores, indices = index.query(query, top_k=3)

    assert indices[0, 0] == 3
    assert scores[0, 0] > scores[0, 1]


def test_save_load_roundtrip_matches_queries():
    rng = np.random.default_rng(0)
    dim = 16
    vectors = l2_normalize_numpy(rng.standard_normal((40, dim)).astype(np.float32))
    query = vectors[7:8]

    idx = FaissFlatIPIndex(dim)
    idx.build(vectors)
    before_scores, before_ix = idx.query(query, top_k=5)

    blob = idx.save_bytes()
    restored = FaissFlatIPIndex.load_bytes(blob, dim=dim)
    after_scores, after_ix = restored.query(query, top_k=5)

    np.testing.assert_array_equal(before_ix, after_ix)
    np.testing.assert_allclose(before_scores, after_scores, rtol=1e-5)


def test_uri_mapping_order_matches_rows():
    uris = ["a", "b", "c"]
    df = uri_mapping_dataframe(uris)
    assert df["ann_row_index"].tolist() == [0, 1, 2]
    assert df["uri"].tolist() == uris


def test_load_post_embeddings_numpy_from_dataframe_dedupes_last():
    df = pd.DataFrame(
        {
            "uri": ["x", "y", "x"],
            "embedding": [[1.0, 0.0], [0.0, 1.0], [0.0, 1.0]],
        }
    )
    uris, mat = load_post_embeddings_numpy_from_dataframe(df, dedupe_strategy="last")
    assert uris == ["y", "x"]
    assert mat.shape == (2, 2)
    np.testing.assert_allclose(mat[1], l2_normalize_numpy(np.array([[0.0, 1.0]]))[0])


def test_concat_embeddings_from_s3_parquets_merges(monkeypatch):
    calls: list[str] = []

    class FakeS3:
        def read_parquet_from_s3(self, key: str):
            calls.append(key)
            if key == "a.parquet":
                return pd.DataFrame(
                    {"uri": ["u1"], "embedding": [[1.0, 0.0]]},
                )
            if key == "b.parquet":
                return pd.DataFrame(
                    {"uri": ["u2"], "embedding": [[0.0, 1.0]]},
                )
            return None

    uris, mat = concat_embeddings_from_s3_parquets(FakeS3(), ["a.parquet", "b.parquet"])
    assert set(uris) == {"u1", "u2"}
    assert mat.shape == (2, 2)


def test_persist_ann_artifacts_writes_in_order():
    vectors = l2_normalize_numpy(
        np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], dtype=np.float32)
    )
    backend = FaissFlatIPIndex(3)
    backend.build(vectors)

    writes: list[tuple[str, str]] = []

    class FakeS3:
        bucket = "bluesky-research"

        def write_to_s3(self, blob: bytes, key: str, bucket: str = "") -> None:
            writes.append(("bytes", key))

        def write_dicts_parquet_to_s3(self, data: list[dict], key: str) -> None:
            writes.append(("parquet", key))

        def write_dict_json_to_s3(self, data: dict, key: str) -> None:
            writes.append(("json", key))

    mapping_df = uri_mapping_dataframe(["u1", "u2"])
    session = persist_ann_artifacts(
        FakeS3(),
        vector_embeddings_root="vector_embeddings",
        embedding_schema_version="v1",
        timestamp="ts",
        index_backend=backend,
        uri_mapping_df=mapping_df,
        embedding_source_keys={
            "batch": "vector_embeddings/post_embeddings/v1/x.parquet"
        },
        embedding_model="m",
        embedding_model_revision="r",
        ann_backend_id="faiss_flat_ip",
    )

    assert writes[0][0] == "bytes"
    assert writes[1][0] == "parquet"
    assert writes[2][0] == "json"
    assert isinstance(session, AnnIndexSessionModel)


def test_load_ann_index_backend_roundtrip_via_s3():
    dim = 4
    vectors = np.eye(dim, dtype=np.float32)
    idx = FaissFlatIPIndex(dim)
    idx.build(vectors)
    blob = idx.save_bytes()

    class FakeS3:
        def read_from_s3(self, key: str):
            assert key == "vector_embeddings/ann_indices/v1/run/index.faiss"
            return blob

    session = AnnIndexSessionModel(
        index_s3_key="vector_embeddings/ann_indices/v1/run/index.faiss",
        uri_mapping_s3_key="vector_embeddings/ann_indices/v1/run/map.parquet",
        embedding_source_keys={},
        embedding_model="m",
        embedding_model_revision="r",
        vector_dimension=dim,
        distance_metric="inner_product",
        creation_timestamp="t",
        ann_backend="faiss_flat_ip",
    )

    restored = load_ann_index_backend_from_s3(FakeS3(), session=session)
    scores, indices = restored.query(vectors[2:3], top_k=2)
    assert indices[0, 0] == 2


@pytest.mark.parametrize("n", [50, 600])
def test_choose_ann_backend_prefers_flat_for_small(n: int):
    dim = 8
    backend = choose_ann_backend(dim, n, force_flat=False, min_vectors_for_hnsw=512)
    if n < 512:
        assert isinstance(backend, FaissFlatIPIndex)
    else:
        assert isinstance(backend, FaissHNSWIPIndex)

"""Approximate nearest-neighbour index over cached post embeddings (FAISS).

Vectors are expected to be L2-normalized so inner-product search matches cosine
similarity (scores in roughly [-1, 1]).
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any

import faiss  # type: ignore[import-untyped]
import numpy as np
import pandas as pd

from lib.aws.s3 import S3
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.generate_vector_embeddings.models import AnnIndexSessionModel

logger = get_logger(__name__)

ANN_BACKEND_FAISS_FLAT_IP = "faiss_flat_ip"
ANN_BACKEND_FAISS_HNSW_IP = "faiss_hnsw_inner_product"
DISTANCE_METRIC_INNER_PRODUCT = "inner_product"


def l2_normalize_numpy(vectors: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    """Row-wise L2 normalization."""
    if vectors.size == 0:
        return vectors
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return vectors / norms


def _row_embedding_to_numpy(raw: Any, dim_hint: int | None = None) -> np.ndarray | None:
    """Flatten common Parquet embedding layouts to shape (dim,)."""
    if raw is None:
        return None
    arr = np.asarray(raw, dtype=np.float32).reshape(-1)
    if arr.size == 0:
        return None
    if dim_hint is not None and arr.size != dim_hint:
        return None
    return arr


def load_post_embeddings_numpy_from_dataframe(
    df: pd.DataFrame,
    *,
    embedding_column: str = "embedding",
    uri_column: str = "uri",
    dedupe_strategy: str = "last",
) -> tuple[list[str], np.ndarray]:
    """Load URIs and a dense matrix from an embeddings DataFrame.

    ``dedupe_strategy``: ``last`` keeps the final row per URI (file order).
    """
    if df.empty:
        return [], np.zeros((0, 0), dtype=np.float32)

    needed = {uri_column, embedding_column}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing columns: {sorted(missing)}")

    working = df[list(needed)].copy()
    working["_row_order"] = np.arange(len(working), dtype=np.int64)

    if dedupe_strategy == "last":
        working = working.sort_values("_row_order").drop_duplicates(
            subset=[uri_column], keep="last"
        )
    elif dedupe_strategy == "first":
        working = working.sort_values("_row_order").drop_duplicates(
            subset=[uri_column], keep="first"
        )
    else:
        raise ValueError(f"Unknown dedupe_strategy: {dedupe_strategy}")

    uris: list[str] = []
    vectors: list[np.ndarray] = []
    dim: int | None = None

    for _, row in working.iterrows():
        vec = _row_embedding_to_numpy(row[embedding_column], dim)
        if vec is None:
            continue
        if dim is None:
            dim = int(vec.shape[0])
        uris.append(str(row[uri_column]))
        vectors.append(vec)

    if not vectors:
        return [], np.zeros((0, 0), dtype=np.float32)

    matrix = np.stack(vectors, axis=0).astype(np.float32, copy=False)
    matrix = l2_normalize_numpy(matrix)
    return uris, matrix


def uri_mapping_dataframe(uris: list[str]) -> pd.DataFrame:
    """ANN row order mapping: ``ann_row_index`` aligns with FAISS ids."""
    return pd.DataFrame(
        {"ann_row_index": np.arange(len(uris), dtype=np.int64), "uri": uris}
    )


class AnnIndexBackend(ABC):
    """Minimal ANN interface for build / query / persistence."""

    @abstractmethod
    def build(self, vectors: np.ndarray) -> None:
        """Train/add normalized vectors of shape ``(n, dim)``."""

    @abstractmethod
    def query(
        self, query_vectors: np.ndarray, top_k: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return distances/scores and neighbour indices for each query row."""

    @abstractmethod
    def save_bytes(self) -> bytes:
        """Serialize index."""

    @classmethod
    @abstractmethod
    def load_bytes(cls, blob: bytes, *, dim: int) -> AnnIndexBackend:
        """Restore index from ``save_bytes`` output."""


class FaissFlatIPIndex(AnnIndexBackend):
    """Exact inner-product search (cosine on unit vectors)."""

    def __init__(self, dim: int) -> None:
        self.dim = dim
        self._index = faiss.IndexFlatIP(dim)

    def build(self, vectors: np.ndarray) -> None:
        if vectors.ndim != 2 or vectors.shape[1] != self.dim:
            raise ValueError(
                f"Expected vectors shaped (n, {self.dim}), got {vectors.shape}"
            )
        self._index.reset()
        vecs = np.ascontiguousarray(vectors.astype(np.float32, copy=False))
        self._index.add(vecs)

    def query(
        self, query_vectors: np.ndarray, top_k: int
    ) -> tuple[np.ndarray, np.ndarray]:
        if query_vectors.ndim != 2 or query_vectors.shape[1] != self.dim:
            raise ValueError(
                f"Queries must be shaped (q, {self.dim}), got {query_vectors.shape}"
            )
        q = np.ascontiguousarray(query_vectors.astype(np.float32, copy=False))
        k = min(top_k, max(self._index.ntotal, 1))
        scores, indices = self._index.search(q, k)
        return scores, indices

    def save_bytes(self) -> bytes:
        serialized = faiss.serialize_index(self._index)
        return serialized.tobytes()

    @classmethod
    def load_bytes(cls, blob: bytes, *, dim: int) -> FaissFlatIPIndex:
        arr = np.frombuffer(blob, dtype=np.uint8)
        instance = cls(dim)
        instance._index = faiss.deserialize_index(arr)
        return instance


class FaissHNSWIPIndex(AnnIndexBackend):
    """HNSW approximate graph with inner-product metric."""

    def __init__(self, dim: int, *, hnsw_m: int = 32) -> None:
        self.dim = dim
        self.hnsw_m = hnsw_m
        self._index = faiss.IndexHNSWFlat(dim, hnsw_m, faiss.METRIC_INNER_PRODUCT)

    def build(self, vectors: np.ndarray) -> None:
        if vectors.ndim != 2 or vectors.shape[1] != self.dim:
            raise ValueError(
                f"Expected vectors shaped (n, {self.dim}), got {vectors.shape}"
            )
        self._index.reset()
        vecs = np.ascontiguousarray(vectors.astype(np.float32, copy=False))
        self._index.add(vecs)

    def query(
        self, query_vectors: np.ndarray, top_k: int
    ) -> tuple[np.ndarray, np.ndarray]:
        if query_vectors.ndim != 2 or query_vectors.shape[1] != self.dim:
            raise ValueError(
                f"Queries must be shaped (q, {self.dim}), got {query_vectors.shape}"
            )
        q = np.ascontiguousarray(query_vectors.astype(np.float32, copy=False))
        k = min(top_k, max(self._index.ntotal, 1))
        scores, indices = self._index.search(q, k)
        return scores, indices

    def save_bytes(self) -> bytes:
        serialized = faiss.serialize_index(self._index)
        return serialized.tobytes()

    @classmethod
    def load_bytes(cls, blob: bytes, *, dim: int) -> FaissHNSWIPIndex:
        arr = np.frombuffer(blob, dtype=np.uint8)
        instance = cls(dim)
        instance._index = faiss.deserialize_index(arr)
        return instance


def choose_ann_backend(
    dim: int,
    num_vectors: int,
    *,
    min_vectors_for_hnsw: int = 512,
    hnsw_m: int = 32,
    force_flat: bool = False,
) -> AnnIndexBackend:
    """Pick FlatIP for small corpora/tests; HNSW for larger sets."""
    if force_flat or num_vectors < min_vectors_for_hnsw:
        return FaissFlatIPIndex(dim)
    return FaissHNSWIPIndex(dim, hnsw_m=hnsw_m)


def concat_embeddings_from_s3_parquets(
    s3: S3,
    parquet_keys: list[str],
    *,
    dedupe_strategy: str = "last",
) -> tuple[list[str], np.ndarray]:
    """Load and concatenate several Parquet embedding exports."""
    frames: list[pd.DataFrame] = []
    for key in parquet_keys:
        df = s3.read_parquet_from_s3(key)
        if df is None or df.empty:
            continue
        frames.append(df)

    if not frames:
        return [], np.zeros((0, 0), dtype=np.float32)

    merged = pd.concat(frames, ignore_index=True)
    return load_post_embeddings_numpy_from_dataframe(
        merged,
        dedupe_strategy=dedupe_strategy,
    )


def list_parquet_keys_under_prefix(s3: S3, prefix: str) -> list[str]:
    keys = s3.list_keys_given_prefix(prefix.removeprefix("/").rstrip("/"))
    return sorted(k for k in keys if k.endswith(".parquet"))


def persist_ann_artifacts(
    s3: S3,
    *,
    vector_embeddings_root: str,
    embedding_schema_version: str,
    timestamp: str,
    index_backend: AnnIndexBackend,
    uri_mapping_df: pd.DataFrame,
    embedding_source_keys: dict[str, str],
    embedding_model: str,
    embedding_model_revision: str,
    ann_backend_id: str,
) -> AnnIndexSessionModel:
    """Write index binary, URI mapping Parquet, then session JSON (ordering matters)."""
    base = os.path.join(
        vector_embeddings_root,
        "ann_indices",
        embedding_schema_version,
        timestamp,
    )
    index_key = os.path.join(base, "index.faiss")
    mapping_key = os.path.join(base, "uri_mapping.parquet")
    session_key = os.path.join(base, "ann_index_session.json")

    index_bytes = index_backend.save_bytes()
    s3.write_to_s3(blob=index_bytes, key=index_key)

    mapping_records = uri_mapping_df.to_dict(orient="records")
    s3.write_dicts_parquet_to_s3(mapping_records, mapping_key)

    dim = 0
    if hasattr(index_backend, "dim"):
        dim = int(index_backend.dim)  # type: ignore[attr-defined]

    session = AnnIndexSessionModel(
        index_s3_key=index_key,
        uri_mapping_s3_key=mapping_key,
        embedding_source_keys=embedding_source_keys,
        embedding_model=embedding_model,
        embedding_model_revision=embedding_model_revision,
        vector_dimension=dim,
        distance_metric=DISTANCE_METRIC_INNER_PRODUCT,
        creation_timestamp=timestamp,
        embedding_schema_version=embedding_schema_version,
        ann_backend=ann_backend_id,
    )

    s3.write_dict_json_to_s3(session.model_dump(), session_key)
    logger.info(
        f"Persisted ANN index to {index_key} with mapping {mapping_key} and session {session_key}"
    )
    return session


def build_ann_index_session_from_embeddings(
    uris: list[str],
    vectors: np.ndarray,
    *,
    s3: S3,
    vector_embeddings_root: str,
    embedding_schema_version: str,
    embedding_model: str,
    embedding_model_revision: str,
    embedding_source_keys: dict[str, str],
    force_flat_index: bool = False,
    timestamp: str | None = None,
) -> AnnIndexSessionModel:
    """Train index, upload artifacts, return session metadata."""
    if vectors.ndim != 2:
        raise ValueError(f"vectors must be 2-D, got shape {vectors.shape}")
    n, dim = vectors.shape[0], vectors.shape[1]
    if n == 0:
        raise ValueError("Cannot build ANN index with zero vectors.")
    if len(uris) != n:
        raise ValueError("uris length must match number of embedding rows.")

    timestamp = timestamp or generate_current_datetime_str()
    backend = choose_ann_backend(dim, n, force_flat=force_flat_index)
    backend.build(vectors)

    if isinstance(backend, FaissFlatIPIndex):
        ann_backend_id = ANN_BACKEND_FAISS_FLAT_IP
    else:
        ann_backend_id = ANN_BACKEND_FAISS_HNSW_IP

    mapping_df = uri_mapping_dataframe(uris)
    return persist_ann_artifacts(
        s3,
        vector_embeddings_root=vector_embeddings_root,
        embedding_schema_version=embedding_schema_version,
        timestamp=timestamp,
        index_backend=backend,
        uri_mapping_df=mapping_df,
        embedding_source_keys=embedding_source_keys,
        embedding_model=embedding_model,
        embedding_model_revision=embedding_model_revision,
        ann_backend_id=ann_backend_id,
    )


def load_ann_index_backend_from_s3(
    s3: S3,
    *,
    session: AnnIndexSessionModel,
) -> AnnIndexBackend:
    """Restore index implementation using recorded ``ann_backend``."""
    blob = s3.read_from_s3(key=session.index_s3_key)
    if blob is None:
        raise FileNotFoundError(f"Missing ANN index object: {session.index_s3_key}")
    dim = session.vector_dimension

    backend_name = session.ann_backend or ANN_BACKEND_FAISS_FLAT_IP
    if backend_name == ANN_BACKEND_FAISS_HNSW_IP:
        return FaissHNSWIPIndex.load_bytes(blob, dim=dim)
    return FaissFlatIPIndex.load_bytes(blob, dim=dim)


def load_uri_order_from_mapping_parquet(s3: S3, mapping_key: str) -> list[str]:
    """URI list aligned with ANN row ids."""
    df = s3.read_parquet_from_s3(mapping_key)
    if df is None or df.empty:
        return []
    df = df.sort_values("ann_row_index")
    return df["uri"].astype(str).tolist()

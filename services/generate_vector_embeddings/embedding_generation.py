"""Offline embedding generation helpers.

Tokenizer and model weights are loaded only via explicit calls so importing
`services.generate_vector_embeddings.helper` does not pull Hugging Face weights.
"""

from __future__ import annotations

import os

import torch
from transformers import AutoModel, AutoTokenizer

from lib.aws.s3 import S3

DEFAULT_EMBEDDING_SCHEMA_VERSION = "v1"

_VECTOR_EMBEDDINGS_REQUIRE_GPU_ENV = "VECTOR_EMBEDDINGS_REQUIRE_GPU"


def get_device(*, require_gpu: bool | None = None) -> torch.device:
    """Pick CUDA, MPS, or CPU.

    When ``require_gpu`` is None, reads ``VECTOR_EMBEDDINGS_REQUIRE_GPU``:
    if truthy, CPU-only hosts raise; otherwise CPU is allowed (tests/local).
    """
    if require_gpu is None:
        require_gpu = os.getenv(_VECTOR_EMBEDDINGS_REQUIRE_GPU_ENV, "").lower() in (
            "1",
            "true",
            "yes",
        )

    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available() and torch.backends.mps.is_built():
        return torch.device("mps")
    if require_gpu:
        raise ValueError(
            "GPU requested (VECTOR_EMBEDDINGS_REQUIRE_GPU) but no CUDA/MPS backend "
            "is available."
        )
    return torch.device("cpu")


def load_transformer_embedding_model(
    model_name: str,
    revision: str,
    device: torch.device,
) -> tuple[AutoTokenizer, AutoModel]:
    """Load tokenizer and encoder model onto ``device``."""
    tokenizer = AutoTokenizer.from_pretrained(model_name, revision=revision)  # nosec B615
    model = AutoModel.from_pretrained(model_name, revision=revision)  # nosec B615
    model.to(device)
    model.eval()
    return tokenizer, model


def l2_normalize_rows(embeddings: torch.Tensor, eps: float = 1e-12) -> torch.Tensor:
    """Row-wise L2 normalization for cosine / inner-product ANN compatibility."""
    if embeddings.dim() == 1:
        embeddings = embeddings.unsqueeze(0)
    norms = embeddings.norm(dim=1, keepdim=True).clamp(min=eps)
    return embeddings / norms


class EmbeddingGenerator:
    """Lazy-loaded Transformer embedding encoder."""

    def __init__(
        self,
        *,
        model_name: str,
        revision: str,
        device: torch.device,
        batch_size: int = 64,
    ) -> None:
        self.model_name = model_name
        self.revision = revision
        self.device = device
        self.batch_size = batch_size
        self._tokenizer: AutoTokenizer | None = None
        self._model: AutoModel | None = None

    def ensure_loaded(self) -> None:
        if self._model is not None and self._tokenizer is not None:
            return
        self._tokenizer, self._model = load_transformer_embedding_model(
            self.model_name,
            self.revision,
            self.device,
        )

    def embed_texts(self, texts: list[str]) -> torch.Tensor:
        """Return normalized embeddings of shape ``[len(texts), hidden_dim]``."""
        self.ensure_loaded()
        assert self._tokenizer is not None and self._model is not None

        chunks: list[torch.Tensor] = []
        for start in range(0, len(texts), self.batch_size):
            batch_texts = texts[start : start + self.batch_size]
            inputs = self._tokenizer(
                batch_texts,
                return_tensors="pt",
                padding=True,
                truncation=True,
            ).to(self.device)

            with torch.no_grad():
                outputs = self._model(**inputs)

            embeddings = outputs.last_hidden_state.mean(dim=1)
            embeddings = l2_normalize_rows(embeddings)
            chunks.append(embeddings.cpu())

            if self.device.type == "cuda":
                torch.cuda.empty_cache()

        if not chunks:
            return torch.empty(0, 0)

        return torch.cat(chunks, dim=0)


def posts_missing_embedding_keys(
    posts: list,
    embedded_uris: set[str],
) -> tuple[list, list]:
    """Split posts into those needing embeddings vs skipped URIs."""
    todo = [post for post in posts if post.uri not in embedded_uris]
    skipped = [post.uri for post in posts if post.uri in embedded_uris]
    return todo, skipped


def collect_embedded_uris_from_versioned_post_embeddings(
    s3: S3,
    *,
    vector_embeddings_root_s3_key: str,
    embedding_schema_version: str,
    embedding_model: str,
    embedding_model_revision: str,
    max_parquet_files: int | None = None,
) -> set[str]:
    """Union URIs already stored under the versioned post-embedding prefix.

    Reads Parquet objects written by this pipeline with columns ``uri``,
    ``embedding_model``, and ``embedding_model_revision``. Files without revision
    columns are ignored so strict (model, revision, schema) idempotency holds.
    """
    prefix = os.path.join(
        vector_embeddings_root_s3_key,
        "post_embeddings",
        embedding_schema_version,
        "",
    )
    keys = [
        key
        for key in s3.list_keys_given_prefix(prefix.rstrip("/"))
        if key.endswith(".parquet")
    ]
    keys.sort()
    if max_parquet_files is not None:
        keys = keys[-max_parquet_files:]

    embedded: set[str] = set()
    for key in keys:
        df = s3.read_parquet_from_s3(key)
        if df is None or df.empty:
            continue
        required = {"uri", "embedding_model", "embedding_model_revision"}
        if not required.issubset(df.columns):
            continue
        mask = (df["embedding_model"] == embedding_model) & (
            df["embedding_model_revision"] == embedding_model_revision
        )
        embedded.update(df.loc[mask, "uri"].astype(str).tolist())

    return embedded

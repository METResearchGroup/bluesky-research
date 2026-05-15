"""Tests for offline embedding generation utilities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import torch

from services.generate_vector_embeddings.embedding_generation import (
    DEFAULT_EMBEDDING_SCHEMA_VERSION,
    EmbeddingGenerator,
    collect_embedded_uris_from_versioned_post_embeddings,
    get_device,
    load_transformer_embedding_model,
    posts_missing_embedding_keys,
)


class _DummyPost:
    def __init__(self, uri: str) -> None:
        self.uri = uri


def test_embedding_generator_lazy_load():
    with patch(
        "services.generate_vector_embeddings.embedding_generation.load_transformer_embedding_model"
    ) as loader:
        gen = EmbeddingGenerator(
            model_name="dummy-model",
            revision="main",
            device=torch.device("cpu"),
            batch_size=8,
        )
        loader.assert_not_called()
        loader.return_value = (MagicMock(), MagicMock())
        gen.ensure_loaded()
        loader.assert_called_once()


def test_load_transformer_calls_from_pretrained():
    fake_tok = MagicMock()
    fake_model = MagicMock()
    with (
        patch(
            "services.generate_vector_embeddings.embedding_generation.AutoTokenizer.from_pretrained",
            return_value=fake_tok,
        ) as tok_pt,
        patch(
            "services.generate_vector_embeddings.embedding_generation.AutoModel.from_pretrained",
            return_value=fake_model,
        ) as model_pt,
    ):
        tokenizer, model = load_transformer_embedding_model(
            "m", "rev", torch.device("cpu")
        )
        tok_pt.assert_called_once_with("m", revision="rev")
        model_pt.assert_called_once_with("m", revision="rev")
        fake_model.to.assert_called_once()
        fake_model.eval.assert_called_once()
        assert tokenizer is fake_tok
        assert model is fake_model


def test_get_device_allows_cpu(monkeypatch):
    monkeypatch.delenv("VECTOR_EMBEDDINGS_REQUIRE_GPU", raising=False)
    monkeypatch.setattr(
        "torch.cuda.is_available",
        lambda: False,
    )
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: False)
    monkeypatch.setattr(torch.backends.mps, "is_built", lambda: False)
    assert get_device().type == "cpu"


def test_get_device_raises_when_gpu_required(monkeypatch):
    monkeypatch.setenv("VECTOR_EMBEDDINGS_REQUIRE_GPU", "1")
    monkeypatch.setattr(
        "torch.cuda.is_available",
        lambda: False,
    )
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: False)
    monkeypatch.setattr(torch.backends.mps, "is_built", lambda: False)
    with pytest.raises(ValueError, match="GPU requested"):
        get_device()


def test_posts_missing_embedding_keys():
    posts = [_DummyPost("a"), _DummyPost("b"), _DummyPost("c")]
    todo, skipped = posts_missing_embedding_keys(posts, {"b"})
    assert [p.uri for p in todo] == ["a", "c"]
    assert skipped == ["b"]


def test_collect_embedded_uris_filters_model_revision():
    s3 = MagicMock()
    s3.list_keys_given_prefix.return_value = [
        "vector_embeddings/post_embeddings/v1/t1.parquet",
        "vector_embeddings/post_embeddings/v1/t2.parquet",
    ]
    df1 = pd.DataFrame(
        {
            "uri": ["u1", "u2"],
            "embedding_model": ["bert-base-uncased", "other"],
            "embedding_model_revision": ["main", "main"],
        }
    )
    df2 = pd.DataFrame(
        {
            "uri": ["u3"],
            "embedding_model": ["bert-base-uncased"],
            "embedding_model_revision": ["main"],
        }
    )
    s3.read_parquet_from_s3.side_effect = [df1, df2]

    embedded = collect_embedded_uris_from_versioned_post_embeddings(
        s3,
        vector_embeddings_root_s3_key="vector_embeddings",
        embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
        embedding_model="bert-base-uncased",
        embedding_model_revision="main",
    )
    assert embedded == {"u1", "u3"}


def test_collect_embedded_uris_skips_legacy_rows_without_revision_column():
    s3 = MagicMock()
    s3.list_keys_given_prefix.return_value = [
        "vector_embeddings/post_embeddings/v1/legacy.parquet",
    ]
    df = pd.DataFrame(
        {
            "uri": ["old"],
            "embedding_model": ["bert-base-uncased"],
            "insert_timestamp": ["t"],
        }
    )
    s3.read_parquet_from_s3.return_value = df

    embedded = collect_embedded_uris_from_versioned_post_embeddings(
        s3,
        vector_embeddings_root_s3_key="vector_embeddings",
        embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
        embedding_model="bert-base-uncased",
        embedding_model_revision="main",
    )
    assert embedded == set()


def test_helper_import_does_not_load_weights(monkeypatch):
    """Importing helper must not invoke Hugging Face ``from_pretrained``."""
    import importlib
    import sys

    monkeypatch.delenv("VECTOR_EMBEDDINGS_REQUIRE_GPU", raising=False)
    for name in (
        "services.generate_vector_embeddings.helper",
        "services.generate_vector_embeddings.embedding_generation",
    ):
        sys.modules.pop(name, None)

    with (
        patch(
            "services.generate_vector_embeddings.embedding_generation.AutoTokenizer.from_pretrained"
        ) as tok_pt,
        patch(
            "services.generate_vector_embeddings.embedding_generation.AutoModel.from_pretrained"
        ) as model_pt,
    ):
        importlib.import_module("services.generate_vector_embeddings.helper")
        tok_pt.assert_not_called()
        model_pt.assert_not_called()


def test_post_embedding_row_metadata_from_legacy_helper_shape():
    """Regression: exported rows carry revision + schema for incremental jobs."""
    from services.generate_vector_embeddings.helper import (
        DEFAULT_EMBEDDING_MODEL_NAME,
        DEFAULT_EMBEDDING_MODEL_REVISION,
        _legacy_embedding_row,
    )

    post = _DummyPost("at://x")
    row = _legacy_embedding_row(
        post,
        torch.ones(1, 4),
        timestamp="ts",
        embedding_schema_version=DEFAULT_EMBEDDING_SCHEMA_VERSION,
    )
    assert row["uri"] == "at://x"
    assert row["embedding_model"] == DEFAULT_EMBEDDING_MODEL_NAME
    assert row["embedding_model_revision"] == DEFAULT_EMBEDDING_MODEL_REVISION
    assert row["embedding_schema_version"] == DEFAULT_EMBEDDING_SCHEMA_VERSION
    assert row["insert_timestamp"] == "ts"
    assert len(row["embedding"]) == 4

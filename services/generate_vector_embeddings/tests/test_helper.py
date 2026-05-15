"""Tests for vector embedding helper orchestration."""

from __future__ import annotations

from unittest.mock import patch

import services.generate_vector_embeddings.helper as helper_module


@patch.object(helper_module, "do_vector_embeddings", return_value=None)
def test_pipeline_returns_none_without_embedding_session(mock_do):
    assert helper_module.run_vector_embedding_offline_pipeline() is None


@patch.object(helper_module, "concat_embeddings_from_s3_parquets")
@patch.object(helper_module, "list_parquet_keys_under_prefix")
@patch.object(helper_module, "do_vector_embeddings")
def test_pipeline_skips_ann_when_no_versioned_parquet(
    mock_do, mock_list_keys, mock_concat
):
    mock_do.return_value = {
        "embedding_timestamp": "2026-01-01T00:00:00",
        "s3_keys": {
            "post_embeddings_versioned": (
                "vector_embeddings/post_embeddings/v1/2026-01-01T00:00:00.parquet"
            ),
        },
    }
    mock_list_keys.return_value = []
    out = helper_module.run_vector_embedding_offline_pipeline()
    assert out is not None
    assert "embedding_session" in out
    assert "ann_index_session" not in out
    mock_concat.assert_not_called()

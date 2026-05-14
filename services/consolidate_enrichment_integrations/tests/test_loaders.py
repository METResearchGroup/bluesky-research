"""Unit tests for services/consolidate_enrichment_integrations/loaders.py."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from services.consolidate_enrichment_integrations import loaders
from services.consolidate_enrichment_integrations.loaders import (
    dataframe_to_models,
    load_latest_perspective_api_labels,
    load_latest_preprocessed_posts,
    load_latest_similarity_scores,
    load_latest_sociopolitical_labels,
    load_previously_consolidated_enriched_post_uris,
)
from services.generate_vector_embeddings.models import PostSimilarityScoreModel
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def _minimal_preprocessed_row(uri: str = "at://x/post", text: str = "hello") -> dict:
    """Build a dict that satisfies FilteredPreprocessedPostModel."""
    return {
        "uri": uri,
        "cid": "cid1",
        "author_did": "did:plc:test",
        "created_at": "2024-01-01T00:00:00Z",
        "text": text,
        "passed_filters": True,
        "filtered_at": "2024-01-01T00:01:00Z",
        "synctimestamp": "2024-01-01T00:02:00Z",
        "preprocessing_timestamp": "2024-01-01T00:03:00Z",
        "source": "firehose",
    }


def _minimal_perspective_row() -> dict:
    """Build a dict that satisfies PerspectiveApiLabelsModel."""
    return {
        "uri": "at://x/post-p",
        "text": "hello",
        "preprocessing_timestamp": "2024-01-01T00:03:00Z",
        "was_successfully_labeled": True,
        "label_timestamp": "2024-01-01T00:04:00Z",
        "source": "firehose",
    }


def _minimal_sociopolitical_row() -> dict:
    """Build a dict for SociopoliticalLabelsModel."""
    return {
        "uri": "at://x/post-s",
        "text": "hello",
        "preprocessing_timestamp": "2024-01-01T00:03:00Z",
        "was_successfully_labeled": True,
        "label_timestamp": "2024-01-01T00:04:00Z",
    }


def _minimal_similarity_row() -> dict:
    """Build a dict for PostSimilarityScoreModel."""
    return {
        "uri": "at://x/sim",
        "similarity_score": 0.5,
        "insert_timestamp": "2024-01-01T00:05:00Z",
        "most_liked_average_embedding_key": "s3://bucket/key",
    }


class TestDataframeToModels:
    """Tests for dataframe_to_models function."""

    def test_returns_empty_list_when_dataframe_is_empty(self):
        """When the DataFrame has no rows, the result is an empty list."""
        df = pd.DataFrame()
        result = dataframe_to_models(df, PerspectiveApiLabelsModel)
        expected: list = []
        assert result == expected

    def test_converts_rows_to_models_without_filter(self):
        """All rows are converted when row_filter is None."""
        row = _minimal_perspective_row()
        df = pd.DataFrame([row])
        result = dataframe_to_models(df, PerspectiveApiLabelsModel)
        assert len(result) == 1
        assert isinstance(result[0], PerspectiveApiLabelsModel)
        assert result[0].uri == row["uri"]

    def test_row_filter_excludes_rows(self):
        """Rows rejected by row_filter are omitted from the output list."""
        rows = [
            _minimal_preprocessed_row(uri="at://a/1", text="keep"),
            _minimal_preprocessed_row(uri="at://a/2", text=None),
        ]
        df = pd.DataFrame(rows)
        result = dataframe_to_models(
            df,
            FilteredPreprocessedPostModel,
            row_filter=lambda r: r.get("text") is not None,
        )
        assert len(result) == 1
        assert result[0].uri == "at://a/1"


class TestLoadLatestPreprocessedPosts:
    """Tests for load_latest_preprocessed_posts function."""

    @patch("services.consolidate_enrichment_integrations.loaders.load_data_from_local_storage")
    def test_loads_service_and_passes_timestamp(self, mock_load: MagicMock):
        """Loads preprocessed_posts and forwards latest_timestamp."""
        row = _minimal_preprocessed_row()
        mock_load.return_value = pd.DataFrame([row])
        result = load_latest_preprocessed_posts("2024-01-01T00:00:00Z")
        assert len(result) == 1
        assert result[0].uri == row["uri"]
        mock_load.assert_called_once_with(
            service="preprocessed_posts",
            latest_timestamp="2024-01-01T00:00:00Z",
        )

    @patch("services.consolidate_enrichment_integrations.loaders.load_data_from_local_storage")
    def test_drops_rows_with_null_text(self, mock_load: MagicMock):
        """Posts whose text is null after parsing are excluded."""
        rows = [
            _minimal_preprocessed_row(uri="at://drop", text=None),
            _minimal_preprocessed_row(uri="at://keep", text="ok"),
        ]
        mock_load.return_value = pd.DataFrame(rows)
        result = load_latest_preprocessed_posts(None)
        assert [p.uri for p in result] == ["at://keep"]


class TestLoadPreviouslyConsolidatedEnrichedPostUris:
    """Tests for load_previously_consolidated_enriched_post_uris function."""

    @patch("services.consolidate_enrichment_integrations.loaders.load_data_from_local_storage")
    def test_returns_unique_uris_from_dataframe(self, mock_load: MagicMock):
        """Returns a set of uri values from the consolidated enriched records."""
        mock_load.return_value = pd.DataFrame(
            {"uri": ["u1", "u2", "u1"], "x": [1, 2, 3]}
        )
        result = load_previously_consolidated_enriched_post_uris()
        expected = {"u1", "u2"}
        assert result == expected
        mock_load.assert_called_once_with(
            service="consolidated_enriched_post_records",
            latest_timestamp=None,
        )


class TestLoadLatestPerspectiveApiLabels:
    """Tests for load_latest_perspective_api_labels function."""

    @patch("services.consolidate_enrichment_integrations.loaders.load_data_from_local_storage")
    def test_loads_perspective_service(self, mock_load: MagicMock):
        """Loads ml_inference_perspective_api with the given timestamp."""
        row = _minimal_perspective_row()
        mock_load.return_value = pd.DataFrame([row])
        result = load_latest_perspective_api_labels("2024-02-01T00:00:00Z")
        assert len(result) == 1
        assert result[0].uri == row["uri"]
        mock_load.assert_called_once_with(
            service="ml_inference_perspective_api",
            latest_timestamp="2024-02-01T00:00:00Z",
        )


class TestLoadLatestSociopoliticalLabels:
    """Tests for load_latest_sociopolitical_labels function."""

    @patch("services.consolidate_enrichment_integrations.loaders.load_data_from_local_storage")
    def test_loads_sociopolitical_service(self, mock_load: MagicMock):
        """Loads ml_inference_sociopolitical with the given timestamp."""
        row = _minimal_sociopolitical_row()
        mock_load.return_value = pd.DataFrame([row])
        result = load_latest_sociopolitical_labels(None)
        assert len(result) == 1
        assert result[0].was_successfully_labeled is True
        mock_load.assert_called_once_with(
            service="ml_inference_sociopolitical",
            latest_timestamp=None,
        )


class TestLoadLatestSimilarityScores:
    """Tests for load_latest_similarity_scores function."""

    @patch.object(loaders.athena, "query_results_as_df")
    def test_queries_full_table_when_timestamp_is_none(self, mock_query_df: MagicMock):
        """When timestamp is None, WHERE clause is unrestricted."""
        mock_query_df.return_value = pd.DataFrame([_minimal_similarity_row()])
        result = load_latest_similarity_scores(None)
        assert len(result) == 1
        assert isinstance(result[0], PostSimilarityScoreModel)
        mock_query_df.assert_called_once()
        called_sql = mock_query_df.call_args[0][0]
        assert "WHERE 1=1" in called_sql
        assert "insert_timestamp >" not in called_sql

    @patch.object(loaders.athena, "query_results_as_df")
    def test_filters_by_insert_timestamp_when_timestamp_set(
        self, mock_query_df: MagicMock
    ):
        """When timestamp is provided, SQL filters with insert_timestamp > timestamp."""
        mock_query_df.return_value = pd.DataFrame()
        ts = "2024-03-01T12:00:00Z"
        result = load_latest_similarity_scores(ts)
        expected: list = []
        assert result == expected
        called_sql = mock_query_df.call_args[0][0]
        assert f"insert_timestamp > '{ts}'" in called_sql

    @patch.object(loaders.athena, "query_results_as_df")
    def test_empty_string_timestamp_is_unrestricted(self, mock_query_df: MagicMock):
        """Empty string is falsy; query uses WHERE 1=1 (same as None)."""
        mock_query_df.return_value = pd.DataFrame()
        load_latest_similarity_scores("")
        called_sql = mock_query_df.call_args[0][0]
        assert "WHERE 1=1" in called_sql

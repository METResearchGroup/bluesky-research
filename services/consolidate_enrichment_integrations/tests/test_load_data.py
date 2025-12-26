"""Tests for load_data.py.

This test suite verifies the functionality of enriched post loading functions:
- load_enriched_posts: Loading and validating consolidated enriched posts
- Error handling and validation

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pydantic import ValidationError

from services.consolidate_enrichment_integrations.load_data import load_enriched_posts
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)


class TestLoadEnrichedPosts:
    """Tests for load_enriched_posts function."""

    @patch("services.consolidate_enrichment_integrations.load_data.load_data_from_local_storage")
    @patch("services.consolidate_enrichment_integrations.load_data.parse_converted_pandas_dicts")
    def test_load_enriched_posts_success(self, mock_parse_dicts, mock_load_data):
        """Test successful loading of enriched posts."""
        # Arrange
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "author_did": ["did:plc:user1", "did:plc:user2"],
            "text": ["Post 1", "Post 2"],
        })
        mock_load_data.return_value = mock_df

        mock_dicts = [
            {
                "uri": "post1",
                "author_did": "did:plc:user1",
                "text": "Post 1",
                "created_at": "2024-01-01T00:00:00Z",
            },
            {
                "uri": "post2",
                "author_did": "did:plc:user2",
                "text": "Post 2",
                "created_at": "2024-01-01T00:00:00Z",
            },
        ]
        mock_parse_dicts.return_value = mock_dicts

        # Act
        result = load_enriched_posts()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_load_data.assert_called_once_with(
            service="consolidated_enriched_post_records",
            latest_timestamp=None,
        )

    @patch("services.consolidate_enrichment_integrations.load_data.load_data_from_local_storage")
    @patch("services.consolidate_enrichment_integrations.load_data.parse_converted_pandas_dicts")
    def test_load_enriched_posts_with_timestamp(self, mock_parse_dicts, mock_load_data):
        """Test loading enriched posts with timestamp filter."""
        # Arrange
        mock_load_data.return_value = pd.DataFrame()
        mock_parse_dicts.return_value = []

        # Act
        load_enriched_posts(latest_timestamp="2024-01-01T00:00:00Z")

        # Assert
        mock_load_data.assert_called_once_with(
            service="consolidated_enriched_post_records",
            latest_timestamp="2024-01-01T00:00:00Z",
        )

    @patch("services.consolidate_enrichment_integrations.load_data.load_data_from_local_storage")
    @patch("services.consolidate_enrichment_integrations.load_data.parse_converted_pandas_dicts")
    def test_load_enriched_posts_handles_empty_data(self, mock_parse_dicts, mock_load_data):
        """Test loading with empty data."""
        # Arrange
        mock_load_data.return_value = pd.DataFrame()
        mock_parse_dicts.return_value = []

        # Act
        result = load_enriched_posts()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch("services.consolidate_enrichment_integrations.load_data.load_data_from_local_storage")
    @patch("services.consolidate_enrichment_integrations.load_data.parse_converted_pandas_dicts")
    def test_load_enriched_posts_raises_validation_error(self, mock_parse_dicts, mock_load_data):
        """Test that validation errors are raised and logged."""
        # Arrange
        mock_load_data.return_value = pd.DataFrame({"uri": ["post1"]})
        mock_parse_dicts.return_value = [{"invalid": "data"}]  # Missing required fields

        # Act & Assert
        with pytest.raises(ValidationError):
            load_enriched_posts()

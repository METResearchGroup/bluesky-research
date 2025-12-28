"""Tests for load_data.py.

This test suite verifies the functionality of superposter loading functions:
- load_latest_superposters: Loading superposter DIDs from local or remote sources
- _load_from_local_storage: Loading from local storage
- _load_from_remote: Loading from Athena
- _transform_string: Transforming Athena format strings to JSON
- Error handling and edge cases

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import pytest
import json
import pandas as pd
from unittest.mock import patch, MagicMock

from services.calculate_superposters.load_data import (
    load_latest_superposters,
    _transform_string,
    _load_from_local_storage,
    _load_from_remote,
)
from services.calculate_superposters.models import CalculateSuperposterSource


class TestTransformString:
    """Tests for _transform_string function."""

    def test_transform_string_basic(self):
        """Test basic string transformation."""
        # Arrange
        input_str = "{author_did=did:plc:jhfzhcn4lgr5bapem2lyodwm, count=5}"

        # Act
        result = _transform_string(input_str)

        # Assert
        assert '"author_did":"did:plc:jhfzhcn4lgr5bapem2lyodwm"' in result
        assert '"count":5' in result
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["author_did"] == "did:plc:jhfzhcn4lgr5bapem2lyodwm"
        assert parsed["count"] == 5

    def test_transform_string_multiple_fields(self):
        """Test transformation with multiple fields."""
        # Arrange
        input_str = "{author_did=did:plc:user1, count=10, other=value}"

        # Act
        result = _transform_string(input_str)

        # Assert
        parsed = json.loads(result)
        assert parsed["author_did"] == "did:plc:user1"
        assert parsed["count"] == 10
        assert parsed["other"] == "value"


class TestLoadFromLocalStorage:
    """Tests for _load_from_local_storage function."""

    @patch("services.calculate_superposters.load_data.load_data_from_local_storage")
    def test_load_from_local_storage_success(self, mock_load_data):
        """Test successful loading from local storage."""
        # Arrange
        mock_df = pd.DataFrame({
            "superposters": ['[{"author_did": "did:plc:user1", "count": 5}]']
        })
        mock_load_data.return_value = mock_df

        # Act
        result = _load_from_local_storage()

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["author_did"] == "did:plc:user1"
        assert result[0]["count"] == 5

    @patch("services.calculate_superposters.load_data.load_data_from_local_storage")
    def test_load_from_local_storage_empty(self, mock_load_data):
        """Test loading with empty DataFrame."""
        # Arrange
        mock_load_data.return_value = pd.DataFrame()

        # Act
        result = _load_from_local_storage()

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    @patch("services.calculate_superposters.load_data.load_data_from_local_storage")
    def test_load_from_local_storage_with_timestamp(self, mock_load_data):
        """Test loading with timestamp filter."""
        # Arrange
        mock_df = pd.DataFrame({
            "superposters": ['[{"author_did": "did:plc:user1", "count": 5}]']
        })
        mock_load_data.return_value = mock_df

        # Act
        _load_from_local_storage(latest_timestamp="2024-01-01T00:00:00Z")

        # Assert
        mock_load_data.assert_called_once_with(
            service="daily_superposters",
            latest_timestamp="2024-01-01T00:00:00Z",
        )


class TestLoadFromRemote:
    """Tests for _load_from_remote function."""

    @patch("services.calculate_superposters.load_data._get_athena")
    @patch("lib.db.data_processing.parse_converted_pandas_dicts")
    def test_load_from_remote_success(self, mock_parse_dicts, mock_get_athena):
        """Test successful loading from remote (Athena)."""
        # Arrange
        mock_athena = MagicMock()
        mock_df = pd.DataFrame({
            "superposters": [
                "{author_did=did:plc:user1, count=5}"
            ],
            "insert_date_timestamp": ["2024-01-01T00:00:00Z"],
        })
        mock_athena.query_results_as_df.return_value = mock_df
        mock_get_athena.return_value = (mock_athena, "test_db")

        mock_dicts = [
            {
                "superposters": "{author_did=did:plc:user1, count=5}",
                "insert_date_timestamp": "2024-01-01T00:00:00Z",
            }
        ]
        mock_parse_dicts.return_value = mock_dicts

        # Act
        result = _load_from_remote()

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["author_did"] == "did:plc:user1"
        assert result[0]["count"] == 5

    @patch("services.calculate_superposters.load_data._get_athena")
    @patch("lib.db.data_processing.parse_converted_pandas_dicts")
    def test_load_from_remote_empty(self, mock_parse_dicts, mock_get_athena):
        """Test loading with empty results from Athena."""
        # Arrange
        mock_athena = MagicMock()
        mock_athena.query_results_as_df.return_value = pd.DataFrame()
        mock_get_athena.return_value = (mock_athena, "test_db")
        mock_parse_dicts.return_value = []

        # Act
        result = _load_from_remote()

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0


class TestLoadLatestSuperposters:
    """Tests for load_latest_superposters function."""

    @patch("services.calculate_superposters.load_data._load_from_local_storage")
    def test_load_latest_superposters_local(self, mock_load_local):
        """Test loading from local source."""
        # Arrange
        mock_load_local.return_value = [
            {"author_did": "did:plc:user1", "count": 5},
            {"author_did": "did:plc:user2", "count": 10},
        ]

        # Act
        result = load_latest_superposters(source=CalculateSuperposterSource.LOCAL)

        # Assert
        assert isinstance(result, set)
        assert len(result) == 2
        assert "did:plc:user1" in result
        assert "did:plc:user2" in result
        mock_load_local.assert_called_once_with(latest_timestamp=None)

    @patch("services.calculate_superposters.load_data._load_from_local_storage")
    def test_load_latest_superposters_local_with_timestamp(self, mock_load_local):
        """Test loading from local source with timestamp."""
        # Arrange
        mock_load_local.return_value = [{"author_did": "did:plc:user1", "count": 5}]

        # Act
        load_latest_superposters(
            source=CalculateSuperposterSource.LOCAL,
            latest_timestamp="2024-01-01T00:00:00Z",
        )

        # Assert
        mock_load_local.assert_called_once_with(
            latest_timestamp="2024-01-01T00:00:00Z"
        )

    @patch("services.calculate_superposters.load_data._load_from_remote")
    def test_load_latest_superposters_remote(self, mock_load_remote):
        """Test loading from remote source."""
        # Arrange
        mock_load_remote.return_value = [
            {"author_did": "did:plc:user1", "count": 5},
            {"author_did": "did:plc:user2", "count": 10},
        ]

        # Act
        result = load_latest_superposters(source=CalculateSuperposterSource.REMOTE)

        # Assert
        assert isinstance(result, set)
        assert len(result) == 2
        assert "did:plc:user1" in result
        assert "did:plc:user2" in result
        mock_load_remote.assert_called_once()

    @patch("services.calculate_superposters.load_data._load_from_local_storage")
    def test_load_latest_superposters_handles_empty(self, mock_load_local):
        """Test loading with empty results."""
        # Arrange
        mock_load_local.return_value = []

        # Act
        result = load_latest_superposters(source=CalculateSuperposterSource.LOCAL)

        # Assert
        assert isinstance(result, set)
        assert len(result) == 0

    def test_load_latest_superposters_invalid_source(self):
        """Test that invalid source raises ValueError."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unknown source"):
            load_latest_superposters(source="invalid_source")  # type: ignore

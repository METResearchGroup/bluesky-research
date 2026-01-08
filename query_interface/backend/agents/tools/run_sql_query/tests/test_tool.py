"""Unit tests for run_sql_query tool."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from query_interface.backend.agents.tools.run_sql_query.exceptions import (
    SQLQueryExecutionError,
)
from query_interface.backend.agents.tools.run_sql_query.tool import run_sql_query


class TestRunSqlQuery:
    """Tests for run_sql_query function."""

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_executes_query_successfully(self, mock_get_athena_client):
        """Test that SQL query is executed successfully and returns DataFrame."""
        # Arrange
        sql = "SELECT * FROM table LIMIT 10"
        expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.return_value = expected_df
        mock_get_athena_client.return_value = mock_athena_instance

        # Act
        result = run_sql_query(sql)

        # Assert
        assert isinstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, expected_df)
        mock_get_athena_client.assert_called_once()
        mock_athena_instance.query_results_as_df.assert_called_once_with(
            query=sql,
            dtypes_map=None,
        )

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_executes_query_with_dtypes_map(self, mock_get_athena_client):
        """Test that SQL query is executed successfully with dtypes_map parameter."""
        # Arrange
        sql = "SELECT * FROM table LIMIT 10"
        dtypes_map = {"col1": "int64", "col2": "string"}
        expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.return_value = expected_df
        mock_get_athena_client.return_value = mock_athena_instance

        # Act
        result = run_sql_query(sql, dtypes_map=dtypes_map)

        # Assert
        assert isinstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, expected_df)
        mock_get_athena_client.assert_called_once()
        mock_athena_instance.query_results_as_df.assert_called_once_with(
            query=sql,
            dtypes_map=dtypes_map,
        )

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_returns_empty_dataframe(self, mock_get_athena_client):
        """Test that empty DataFrame is returned when query has no results."""
        # Arrange
        sql = "SELECT * FROM empty_table LIMIT 10"
        expected_df = pd.DataFrame()
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.return_value = expected_df
        mock_get_athena_client.return_value = mock_athena_instance

        # Act
        result = run_sql_query(sql)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        pd.testing.assert_frame_equal(result, expected_df)
        mock_athena_instance.query_results_as_df.assert_called_once_with(
            query=sql,
            dtypes_map=None,
        )

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_raises_sql_query_execution_error_on_generic_exception(
        self, mock_get_athena_client
    ):
        """Test that generic exceptions are wrapped in SQLQueryExecutionError."""
        # Arrange
        sql = "SELECT * FROM invalid_table LIMIT 10"
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.side_effect = Exception(
            "Table does not exist"
        )
        mock_get_athena_client.return_value = mock_athena_instance

        # Act & Assert
        with pytest.raises(SQLQueryExecutionError) as exc_info:
            run_sql_query(sql)

        assert f"Failed to execute SQL query '{sql}': Table does not exist" in str(
            exc_info.value
        )
        assert isinstance(exc_info.value.__cause__, Exception)

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_raises_sql_query_execution_error_on_value_error(
        self, mock_get_athena_client
    ):
        """Test that ValueError exceptions are wrapped in SQLQueryExecutionError."""
        # Arrange
        sql = "SELECT * FROM table LIMIT 10"
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.side_effect = ValueError(
            "Invalid query syntax"
        )
        mock_get_athena_client.return_value = mock_athena_instance

        # Act & Assert
        with pytest.raises(SQLQueryExecutionError) as exc_info:
            run_sql_query(sql)

        assert (
            f"Failed to execute SQL query '{sql}': Invalid query syntax"
            in str(exc_info.value)
        )
        assert isinstance(exc_info.value.__cause__, ValueError)

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_raises_sql_query_execution_error_on_athena_initialization_failure(
        self, mock_get_athena_client
    ):
        """Test that Athena initialization failures are caught and wrapped."""
        # Arrange
        sql = "SELECT * FROM table LIMIT 10"
        mock_get_athena_client.side_effect = Exception("Failed to create Athena client")

        # Act & Assert
        with pytest.raises(SQLQueryExecutionError) as exc_info:
            run_sql_query(sql)

        assert (
            f"Failed to execute SQL query '{sql}': Failed to create Athena client"
            in str(exc_info.value)
        )
        assert isinstance(exc_info.value.__cause__, Exception)

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_preserves_exception_context_in_error_message(
        self, mock_get_athena_client
    ):
        """Test that original exception message is preserved in error message."""
        # Arrange
        sql = "SELECT invalid_column FROM table LIMIT 10"
        original_error_message = "Column 'invalid_column' cannot be resolved"
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.side_effect = Exception(
            original_error_message
        )
        mock_get_athena_client.return_value = mock_athena_instance

        # Act & Assert
        with pytest.raises(SQLQueryExecutionError) as exc_info:
            run_sql_query(sql)

        error_message = str(exc_info.value)
        assert sql in error_message
        assert original_error_message in error_message

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_passes_dtypes_map_none_explicitly(self, mock_get_athena_client):
        """Test that None is explicitly passed as dtypes_map when not provided."""
        # Arrange
        sql = "SELECT * FROM table LIMIT 10"
        expected_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.return_value = expected_df
        mock_get_athena_client.return_value = mock_athena_instance

        # Act
        run_sql_query(sql)

        # Assert
        mock_athena_instance.query_results_as_df.assert_called_once_with(
            query=sql,
            dtypes_map=None,
        )

    @patch("query_interface.backend.agents.tools.run_sql_query.tool._get_athena_client")
    def test_handles_complex_dataframe_with_multiple_columns(
        self, mock_get_athena_client
    ):
        """Test that complex DataFrame with multiple columns is handled correctly."""
        # Arrange
        sql = "SELECT col1, col2, col3, col4 FROM table LIMIT 10"
        expected_df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
                "col3": [1.1, 2.2, 3.3],
                "col4": [True, False, True],
            }
        )
        mock_athena_instance = MagicMock()
        mock_athena_instance.query_results_as_df.return_value = expected_df
        mock_get_athena_client.return_value = mock_athena_instance

        # Act
        result = run_sql_query(sql)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result.columns) == 4
        pd.testing.assert_frame_equal(result, expected_df)
        mock_athena_instance.query_results_as_df.assert_called_once_with(
            query=sql,
            dtypes_map=None,
        )


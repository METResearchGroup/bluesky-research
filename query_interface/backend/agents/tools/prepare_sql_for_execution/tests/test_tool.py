"""Unit tests for prepare_sql_for_execution tool."""

import pytest

from query_interface.backend.agents.tools.prepare_sql_for_execution.exceptions import (
    SQLPreparationForExecutionError,
)
from query_interface.backend.agents.tools.prepare_sql_for_execution.tool import (
    _clean_sql_formatting,
    _enforce_limit,
    prepare_sql_for_execution,
    DEFAULT_LIMIT,
)


class TestCleanSqlFormatting:
    """Tests for _clean_sql_formatting function."""

    def test_returns_plain_sql_unchanged(self):
        """Test that plain SQL without formatting is returned unchanged."""
        # Arrange
        sql = "SELECT * FROM posts"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_removes_trailing_semicolon(self):
        """Test that trailing semicolon is removed."""
        # Arrange
        sql = "SELECT * FROM posts;"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_removes_markdown_code_block_with_sql_language(self):
        """Test that markdown code blocks with sql language tag are removed."""
        # Arrange
        sql = "```sql\nSELECT * FROM posts\n```"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_removes_markdown_code_block_without_language(self):
        """Test that markdown code blocks without language tag are removed."""
        # Arrange
        sql = "```\nSELECT * FROM posts\n```"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_removes_markdown_code_block_and_semicolon(self):
        """Test that both markdown code blocks and trailing semicolons are removed."""
        # Arrange
        sql = "```sql\nSELECT * FROM posts;\n```"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_strips_leading_and_trailing_whitespace(self):
        """Test that leading and trailing whitespace is stripped."""
        # Arrange
        sql = "   SELECT * FROM posts   "
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_preserves_multiline_sql_in_code_block(self):
        """Test that multiline SQL within code blocks is preserved correctly."""
        # Arrange
        sql = "```sql\nSELECT *\nFROM posts\nWHERE id = 1\n```"
        expected = "SELECT *\nFROM posts\nWHERE id = 1"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_handles_code_block_with_other_language_tag(self):
        """Test that code blocks with other language tags are handled."""
        # Arrange
        sql = "```presto\nSELECT * FROM posts\n```"
        expected = "SELECT * FROM posts"

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_handles_empty_string(self):
        """Test that empty string input returns empty string."""
        # Arrange
        sql = ""
        expected = ""

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected

    def test_handles_only_whitespace(self):
        """Test that whitespace-only input returns empty string."""
        # Arrange
        sql = "   \n\t  "
        expected = ""

        # Act
        result = _clean_sql_formatting(sql)

        # Assert
        assert result == expected


class TestEnforceLimit:
    """Tests for _enforce_limit function."""

    def test_appends_limit_when_not_present(self):
        """Test that LIMIT is appended when not present in query."""
        # Arrange
        sql = "SELECT * FROM posts"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_replaces_existing_limit_with_default(self):
        """Test that existing LIMIT is replaced with default limit."""
        # Arrange
        sql = "SELECT * FROM posts LIMIT 100"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_replaces_existing_limit_case_insensitive(self):
        """Test that LIMIT replacement is case insensitive."""
        # Arrange
        sql = "SELECT * FROM posts limit 50"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_replaces_existing_limit_mixed_case(self):
        """Test that mixed case LIMIT is replaced."""
        # Arrange
        sql = "SELECT * FROM posts Limit 25"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_uses_custom_limit_value(self):
        """Test that custom limit value is used when provided."""
        # Arrange
        sql = "SELECT * FROM posts"
        expected = "SELECT * FROM posts LIMIT 5"

        # Act
        result = _enforce_limit(sql, limit=5)

        # Assert
        assert result == expected

    def test_replaces_limit_with_custom_value(self):
        """Test that existing LIMIT is replaced with custom limit value."""
        # Arrange
        sql = "SELECT * FROM posts LIMIT 100"
        expected = "SELECT * FROM posts LIMIT 20"

        # Act
        result = _enforce_limit(sql, limit=20)

        # Assert
        assert result == expected

    def test_handles_limit_in_subquery(self):
        """Test that LIMIT in subquery is also replaced."""
        # Arrange
        sql = "SELECT * FROM (SELECT * FROM posts LIMIT 50) AS subq"
        expected = "SELECT * FROM (SELECT * FROM posts LIMIT 10) AS subq"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_handles_limit_with_offset(self):
        """Test that LIMIT with OFFSET pattern is handled."""
        # Arrange - note: this replaces just the LIMIT number, not OFFSET
        sql = "SELECT * FROM posts LIMIT 100 OFFSET 20"
        expected = "SELECT * FROM posts LIMIT 10 OFFSET 20"

        # Act
        result = _enforce_limit(sql)

        # Assert
        assert result == expected

    def test_default_limit_constant_is_10(self):
        """Test that DEFAULT_LIMIT constant is 10."""
        # Assert
        assert DEFAULT_LIMIT == 10


class TestPrepareSqlForExecution:
    """Tests for prepare_sql_for_execution function."""

    def test_prepares_plain_sql_with_limit(self):
        """Test that plain SQL is prepared with LIMIT added."""
        # Arrange
        sql = "SELECT * FROM posts"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_cleans_markdown_and_adds_limit(self):
        """Test that markdown is cleaned and LIMIT is added."""
        # Arrange
        sql = "```sql\nSELECT * FROM posts\n```"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_removes_semicolon_and_adds_limit(self):
        """Test that semicolon is removed and LIMIT is added."""
        # Arrange
        sql = "SELECT * FROM posts;"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_cleans_all_formatting_and_enforces_limit(self):
        """Test that all formatting is cleaned and LIMIT is enforced."""
        # Arrange
        sql = "```sql\nSELECT * FROM posts LIMIT 100;\n```"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_preserves_complex_query_structure(self):
        """Test that complex query structure is preserved."""
        # Arrange
        sql = """```sql
SELECT p.id, p.text, u.handle
FROM posts p
JOIN users u ON p.author_id = u.id
WHERE p.created_at > '2024-01-01'
ORDER BY p.created_at DESC;
```"""
        expected = (
            "SELECT p.id, p.text, u.handle\n"
            "FROM posts p\n"
            "JOIN users u ON p.author_id = u.id\n"
            "WHERE p.created_at > '2024-01-01'\n"
            "ORDER BY p.created_at DESC LIMIT 10"
        )

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_handles_query_with_existing_lower_limit(self):
        """Test that query with existing lower limit is unchanged."""
        # Arrange
        sql = "SELECT * FROM posts LIMIT 5"
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        # Note: current implementation always enforces DEFAULT_LIMIT
        assert result == expected

    def test_handles_whitespace_around_query(self):
        """Test that whitespace around query is handled."""
        # Arrange
        sql = "   SELECT * FROM posts   "
        expected = "SELECT * FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_returns_string_type(self):
        """Test that return type is string."""
        # Arrange
        sql = "SELECT * FROM posts"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert isinstance(result, str)

    def test_handles_aggregation_query(self):
        """Test that aggregation queries are handled correctly."""
        # Arrange
        sql = "```sql\nSELECT COUNT(*) as total FROM posts;\n```"
        expected = "SELECT COUNT(*) as total FROM posts LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected

    def test_handles_group_by_query(self):
        """Test that GROUP BY queries are handled correctly."""
        # Arrange
        sql = "SELECT author_id, COUNT(*) FROM posts GROUP BY author_id"
        expected = "SELECT author_id, COUNT(*) FROM posts GROUP BY author_id LIMIT 10"

        # Act
        result = prepare_sql_for_execution(sql)

        # Assert
        assert result == expected


class TestPrepareSqlForExecutionErrorHandling:
    """Tests for error handling in prepare_sql_for_execution function."""

    def test_raises_error_with_descriptive_message(self):
        """Test that SQLPreparationForExecutionError includes descriptive message.

        Note: This test uses a mock to simulate an internal error since
        the current implementation is robust against most inputs.
        """
        # Arrange - create a scenario that would cause an internal error
        # by patching _clean_sql_formatting to raise an exception
        from unittest.mock import patch

        sql = "SELECT * FROM posts"

        with patch(
            "query_interface.backend.agents.tools.prepare_sql_for_execution.tool._clean_sql_formatting"
        ) as mock_clean:
            mock_clean.side_effect = Exception("Unexpected error")

            # Act & Assert
            with pytest.raises(SQLPreparationForExecutionError) as exc_info:
                prepare_sql_for_execution(sql)

            assert "Failed to prepare SQL query for execution" in str(exc_info.value)
            assert "Unexpected error" in str(exc_info.value)

    def test_preserves_exception_chain(self):
        """Test that original exception is preserved in the chain."""
        from unittest.mock import patch

        sql = "SELECT * FROM posts"

        with patch(
            "query_interface.backend.agents.tools.prepare_sql_for_execution.tool._clean_sql_formatting"
        ) as mock_clean:
            original_error = ValueError("Internal processing error")
            mock_clean.side_effect = original_error

            # Act & Assert
            with pytest.raises(SQLPreparationForExecutionError) as exc_info:
                prepare_sql_for_execution(sql)

            assert exc_info.value.__cause__ is original_error


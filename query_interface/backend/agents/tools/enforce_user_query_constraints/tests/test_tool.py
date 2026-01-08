"""Unit tests for enforce_user_query_constraints tool."""

import pytest
from unittest.mock import patch

from query_interface.backend.agents.tools.enforce_user_query_constraints.exceptions import (
    UserQueryConstraintError,
)
from query_interface.backend.agents.tools.enforce_user_query_constraints.tool import (
    enforce_user_query_constraints,
)


class TestEnforceUserQueryConstraints:
    """Tests for enforce_user_query_constraints function."""

    def test_passes_valid_query(self):
        """Test that valid query passes all constraints."""
        # Arrange
        query = "What are the top 10 posts by engagement?"

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    def test_passes_query_at_max_length(self):
        """Test that query at exactly max length passes."""
        # Arrange
        # Create a query that's exactly 5000 characters (default max)
        query = "a" * 5000

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    def test_raises_constraint_error_for_none(self):
        """Test that None query raises UserQueryConstraintError."""
        # Arrange
        query = None

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query cannot be None"):
            enforce_user_query_constraints(query)

    def test_raises_constraint_error_for_empty_string(self):
        """Test that empty string raises UserQueryConstraintError."""
        # Arrange
        query = ""

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query cannot be an empty string"):
            enforce_user_query_constraints(query)

    def test_raises_constraint_error_for_whitespace_only_string(self):
        """Test that whitespace-only string raises UserQueryConstraintError."""
        # Arrange
        query = "   \n\t  "

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query cannot be an empty string"):
            enforce_user_query_constraints(query)

    def test_raises_constraint_error_for_non_string_type(self):
        """Test that non-string types raise UserQueryConstraintError."""
        # Arrange
        query = 123

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query must be a string"):
            enforce_user_query_constraints(query)  # type: ignore[arg-type]

    def test_raises_constraint_error_for_list_type(self):
        """Test that list type raises UserQueryConstraintError."""
        # Arrange
        query = ["not", "a", "string"]

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query must be a string"):
            enforce_user_query_constraints(query)  # type: ignore[arg-type]

    def test_raises_constraint_error_for_dict_type(self):
        """Test that dict type raises UserQueryConstraintError."""
        # Arrange
        query = {"key": "value"}

        # Act & Assert
        with pytest.raises(UserQueryConstraintError, match="Query must be a string"):
            enforce_user_query_constraints(query)  # type: ignore[arg-type]

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_raises_constraint_error_when_exceeds_max_length(self, mock_get_config):
        """Test that query exceeding max length raises UserQueryConstraintError."""
        # Arrange
        mock_get_config.return_value = 100
        query = "a" * 101  # Exceeds max length of 100

        # Act & Assert
        with pytest.raises(UserQueryConstraintError) as exc_info:
            enforce_user_query_constraints(query)

        assert "Query exceeds maximum length" in str(exc_info.value)
        assert "100" in str(exc_info.value)
        assert "101" in str(exc_info.value)

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_uses_config_value_for_max_length(self, mock_get_config):
        """Test that max length is retrieved from config."""
        # Arrange
        mock_get_config.return_value = 2000
        query = "a" * 2001  # Exceeds configured max length

        # Act & Assert
        with pytest.raises(UserQueryConstraintError) as exc_info:
            enforce_user_query_constraints(query)

        assert "2000" in str(exc_info.value)
        # get_config_value is called twice (once in condition, once in assignment)
        assert mock_get_config.call_count == 2
        mock_get_config.assert_any_call("query", "max_length")

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_falls_back_to_default_when_config_none(self, mock_get_config):
        """Test that default max length of 5000 is used when config value is None."""
        # Arrange
        mock_get_config.return_value = None
        query = "a" * 5001  # Exceeds default max length

        # Act & Assert
        with pytest.raises(UserQueryConstraintError) as exc_info:
            enforce_user_query_constraints(query)

        assert "5000" in str(exc_info.value)
        assert "5001" in str(exc_info.value)

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_passes_when_query_within_default_limit(self, mock_get_config):
        """Test that query within default limit passes when config is None."""
        # Arrange
        mock_get_config.return_value = None
        query = "a" * 4999  # Within default max length

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    def test_handles_query_with_leading_trailing_whitespace(self):
        """Test that query with leading/trailing whitespace passes if not empty after strip."""
        # Arrange
        query = "   What are the top posts?   "

        # Act & Assert
        # Should not raise any exception (whitespace is stripped for empty check,
        # but length check uses original query)
        enforce_user_query_constraints(query)

    def test_handles_multiline_query(self):
        """Test that multiline query passes if within length limit."""
        # Arrange
        query = """What are the top posts?
        Show me posts with high engagement.
        Filter by date range."""

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    def test_handles_query_with_special_characters(self):
        """Test that query with special characters passes if valid."""
        # Arrange
        query = "What are the top 10 posts? Show me @mentions and #hashtags!"

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    def test_handles_unicode_characters(self):
        """Test that query with unicode characters passes if valid."""
        # Arrange
        query = "What are the top posts? こんにちは 🚀"

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_error_message_includes_query_length(self, mock_get_config):
        """Test that error message includes the actual query length."""
        # Arrange
        mock_get_config.return_value = 100
        query = "a" * 150

        # Act & Assert
        with pytest.raises(UserQueryConstraintError) as exc_info:
            enforce_user_query_constraints(query)

        error_message = str(exc_info.value)
        assert "Query length: 150" in error_message

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_passes_query_one_char_below_limit(self, mock_get_config):
        """Test that query one character below limit passes."""
        # Arrange
        mock_get_config.return_value = 100
        query = "a" * 99  # One below limit

        # Act & Assert
        # Should not raise any exception
        enforce_user_query_constraints(query)

    @patch("query_interface.backend.agents.tools.enforce_user_query_constraints.tool.get_config_value")
    def test_raises_error_for_query_one_char_over_limit(self, mock_get_config):
        """Test that query one character over limit raises error."""
        # Arrange
        mock_get_config.return_value = 100
        query = "a" * 101  # One over limit

        # Act & Assert
        with pytest.raises(UserQueryConstraintError):
            enforce_user_query_constraints(query)


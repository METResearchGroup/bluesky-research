"""Tests for scripts.migrate_research_data_to_s3.integration_prefixes module."""

from scripts.migrate_research_data_to_s3.integration_prefixes import prefixes_for_integrations


class TestPrefixesForIntegrations:
    """Tests for prefixes_for_integrations function."""

    def test_returns_prefixes_for_single_integration(self):
        """Test that a single integration returns its active and cache prefixes."""
        # Arrange
        integration_names = ["ml_inference_intergroup"]
        expected = ["ml_inference_intergroup/active", "ml_inference_intergroup/cache"]

        # Act
        result = prefixes_for_integrations(integration_names)

        # Assert
        assert result == expected

    def test_returns_prefixes_for_multiple_integrations(self):
        """Test that multiple integrations return all matching prefixes."""
        # Arrange
        integration_names = ["ml_inference_ime", "ml_inference_valence_classifier"]
        expected = [
            "ml_inference_ime/active",
            "ml_inference_ime/cache",
            "ml_inference_valence_classifier/active",
            "ml_inference_valence_classifier/cache",
        ]

        # Act
        result = prefixes_for_integrations(integration_names)

        # Assert
        assert result == expected

    def test_returns_empty_list_when_no_match(self):
        """Test that a nonexistent integration returns no prefixes."""
        # Arrange
        integration_names = ["nonexistent_integration"]

        # Act
        result = prefixes_for_integrations(integration_names)

        # Assert
        assert result == []

    def test_returns_empty_list_for_empty_input(self):
        """Test that empty integration_names returns empty list."""
        # Arrange
        integration_names: list[str] = []

        # Act
        result = prefixes_for_integrations(integration_names)

        # Assert
        assert result == []

    def test_integration_name_exact_match(self):
        """Test that a prefix exactly equal to an integration name is included."""
        from unittest.mock import patch

        # Arrange: use a custom list where one prefix equals the integration name
        custom_prefixes = [
            "ml_inference_intergroup",
            "ml_inference_intergroup/active",
            "ml_inference_intergroup/cache",
        ]
        integration_names = ["ml_inference_intergroup"]
        expected = [
            "ml_inference_intergroup",
            "ml_inference_intergroup/active",
            "ml_inference_intergroup/cache",
        ]

        # Act
        with patch(
            "scripts.migrate_research_data_to_s3.integration_prefixes.PREFIXES_TO_MIGRATE",
            custom_prefixes,
        ):
            result = prefixes_for_integrations(integration_names)

        # Assert
        assert result == expected

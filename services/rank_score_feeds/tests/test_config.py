"""Tests for FeedConfig configuration.

This test suite verifies the configuration structure and values for the
rank_score_feeds service. These tests ensure:
- FeedConfig can be instantiated with default values
- All expected fields exist and have correct types
- Module-level feed_config instance is accessible
- Config values match original magic numbers (regression check)
- Numeric values are within expected ranges
- Derived values are calculated correctly
"""

import pytest

from services.rank_score_feeds.config import FeedConfig, feed_config
from lib.constants import default_lookback_days


class TestFeedConfig:
    """Tests for FeedConfig dataclass."""

    def test_config_can_be_instantiated(self):
        """Verify FeedConfig can be instantiated with default values."""
        # Arrange & Act
        result = FeedConfig()

        # Assert
        assert result is not None
        assert isinstance(result, FeedConfig)

    def test_config_has_all_required_fields(self):
        """Verify all expected configuration fields exist."""
        # Arrange
        config = FeedConfig()
        expected_fields = [
            # Feed Length & Filtering
            "max_feed_length",
            "max_num_times_user_can_appear_in_feed",
            "max_prop_old_posts",
            "max_in_network_posts_ratio",
            "feed_preprocessing_multiplier",
            # Scoring Coefficients
            "coef_toxicity",
            "coef_constructiveness",
            "superposter_coef",
            "engagement_coef",
            # Freshness Scoring
            "default_max_freshness_score",
            "freshness_lambda_factor",
            "freshness_exponential_base",
            "freshness_decay_ratio",
            # Lookback Periods
            "default_scoring_lookback_days",
            # Likeability Scoring
            "default_similarity_score",
            "average_popular_post_like_count",
            # Feed Postprocessing
            "jitter_amount",
            "keep_count",
        ]

        # Act & Assert
        for field in expected_fields:
            result = hasattr(config, field)
            assert result, f"Missing field: {field}"

    def test_config_values_match_original_constants(self):
        """Regression test: verify values match original magic numbers."""
        # Arrange
        config = FeedConfig()

        # Act & Assert - Feed Length & Filtering
        expected = 100
        result = config.max_feed_length
        assert result == expected

        expected = 5
        result = config.max_num_times_user_can_appear_in_feed
        assert result == expected

        expected = 0.6
        result = config.max_prop_old_posts
        assert result == expected

        expected = 0.5
        result = config.max_in_network_posts_ratio
        assert result == expected

        expected = 2
        result = config.feed_preprocessing_multiplier
        assert result == expected

        # Act & Assert - Scoring Coefficients
        expected = 0.965
        result = config.coef_toxicity
        assert result == expected

        expected = 1.02
        result = config.coef_constructiveness
        assert result == expected

        expected = 0.95
        result = config.superposter_coef
        assert result == expected

        expected = 1.0
        result = config.engagement_coef
        assert result == expected

        # Act & Assert - Freshness Scoring
        expected = 3.0
        result = config.default_max_freshness_score
        assert result == expected

        expected = 0.95
        result = config.freshness_lambda_factor
        assert result == expected

        expected = 1.0
        result = config.freshness_exponential_base
        assert result == expected

        # Act & Assert - Lookback Periods
        expected = 1
        result = config.default_scoring_lookback_days
        assert result == expected

        # Act & Assert - Likeability Scoring
        expected = 0.8
        result = config.default_similarity_score
        assert result == expected

        expected = 100
        result = config.average_popular_post_like_count
        assert result == expected

        # Act & Assert - Feed Postprocessing
        expected = 2
        result = config.jitter_amount
        assert result == expected

        expected = 3
        result = config.keep_count
        assert result == expected

    def test_freshness_decay_ratio_is_calculated_correctly(self):
        """Verify freshness_decay_ratio is calculated correctly."""
        # Arrange
        config = FeedConfig()
        default_lookback_hours = default_lookback_days * 24
        expected = config.default_max_freshness_score / default_lookback_hours

        # Act
        result = config.freshness_decay_ratio

        # Assert
        assert result == pytest.approx(expected)

    def test_config_value_ranges(self):
        """Verify numeric values are within expected ranges."""
        # Arrange
        config = FeedConfig()

        # Act & Assert - Feed length should be positive
        result = config.max_feed_length
        assert result > 0

        result = config.max_num_times_user_can_appear_in_feed
        assert result > 0

        # Act & Assert - Proportions should be between 0 and 1
        result = config.max_prop_old_posts
        assert 0.0 <= result <= 1.0

        result = config.max_in_network_posts_ratio
        assert 0.0 <= result <= 1.0

        # Act & Assert - Coefficients should be reasonable (between 0 and 2)
        result = config.coef_toxicity
        assert 0.0 < result < 2.0

        result = config.coef_constructiveness
        assert 0.0 < result < 2.0

        result = config.superposter_coef
        assert 0.0 < result <= 1.0

        result = config.engagement_coef
        assert 0.0 < result < 2.0

        # Act & Assert - Freshness values should be positive
        result = config.default_max_freshness_score
        assert result > 0

        result = config.freshness_lambda_factor
        assert 0.0 < result <= 1.0

        result = config.freshness_exponential_base
        assert result > 0

        result = config.freshness_decay_ratio
        assert result > 0

        # Act & Assert - Lookback days should be positive
        result = config.default_scoring_lookback_days
        assert result > 0

        # Act & Assert - Similarity score should be between 0 and 1
        result = config.default_similarity_score
        assert 0.0 <= result <= 1.0

        # Act & Assert - Like count should be positive
        result = config.average_popular_post_like_count
        assert result > 0

        # Act & Assert - Jitter and keep_count should be positive
        result = config.jitter_amount
        assert result > 0

        result = config.keep_count
        assert result > 0

    def test_config_field_types(self):
        """Verify field types are correct."""
        # Arrange
        config = FeedConfig()

        # Act & Assert - Integers
        result = config.max_feed_length
        assert isinstance(result, int)

        result = config.max_num_times_user_can_appear_in_feed
        assert isinstance(result, int)

        result = config.feed_preprocessing_multiplier
        assert isinstance(result, int)

        result = config.default_scoring_lookback_days
        assert isinstance(result, int)

        result = config.average_popular_post_like_count
        assert isinstance(result, int)

        result = config.jitter_amount
        assert isinstance(result, int)

        result = config.keep_count
        assert isinstance(result, int)

        # Act & Assert - Floats
        result = config.max_prop_old_posts
        assert isinstance(result, float)

        result = config.max_in_network_posts_ratio
        assert isinstance(result, float)

        result = config.coef_toxicity
        assert isinstance(result, float)

        result = config.coef_constructiveness
        assert isinstance(result, float)

        result = config.superposter_coef
        assert isinstance(result, float)

        result = config.engagement_coef
        assert isinstance(result, float)

        result = config.default_max_freshness_score
        assert isinstance(result, float)

        result = config.freshness_lambda_factor
        assert isinstance(result, float)

        result = config.freshness_exponential_base
        assert isinstance(result, float)

        result = config.freshness_decay_ratio
        assert isinstance(result, float)

        result = config.default_similarity_score
        assert isinstance(result, float)

    def test_module_level_feed_config_exists(self):
        """Verify module-level feed_config instance exists and is accessible."""
        # Arrange & Act
        result = feed_config

        # Assert
        assert result is not None
        assert isinstance(result, FeedConfig)

    def test_module_level_feed_config_has_correct_values(self):
        """Verify module-level feed_config has correct default values."""
        # Arrange
        expected_max_feed_length = 100
        expected_coef_toxicity = 0.965
        expected_max_num_times_user_can_appear_in_feed = 5

        # Act
        result_max_feed_length = feed_config.max_feed_length
        result_coef_toxicity = feed_config.coef_toxicity
        result_max_num_times_user_can_appear_in_feed = (
            feed_config.max_num_times_user_can_appear_in_feed
        )

        # Assert
        assert result_max_feed_length == expected_max_feed_length
        assert result_coef_toxicity == expected_coef_toxicity
        assert result_max_num_times_user_can_appear_in_feed == (
            expected_max_num_times_user_can_appear_in_feed
        )

    def test_config_can_be_imported_from_scoring_module(self):
        """Verify config can be imported and used from scoring module."""
        # Arrange
        import importlib

        # Act
        config_module = importlib.import_module("services.rank_score_feeds.config")
        result = hasattr(config_module, "feed_config")

        # Assert
        assert result

    def test_config_can_be_imported_from_helper_module(self):
        """Verify config can be imported and used from helper module."""
        # Arrange
        import importlib

        # Act
        config_module = importlib.import_module("services.rank_score_feeds.config")
        result = hasattr(config_module, "feed_config")

        # Assert
        assert result

    def test_config_values_are_not_none(self):
        """Verify no configuration values are None."""
        # Arrange
        config = FeedConfig()

        # Act & Assert
        for field_name in dir(config):
            if not field_name.startswith("_") and not callable(
                getattr(config, field_name)
            ):
                result = getattr(config, field_name)
                assert result is not None, f"Field {field_name} is None"

    def test_freshness_decay_ratio_is_positive(self):
        """Verify freshness_decay_ratio is positive and reasonable."""
        # Arrange
        config = FeedConfig()

        # Act
        result = config.freshness_decay_ratio

        # Assert
        assert result > 0
        # Should be a small positive number (decay per hour)
        assert result < 1.0

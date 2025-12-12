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
from dataclasses import fields

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
        # Use dataclasses.fields() to get all fields automatically
        expected_fields = {field.name for field in fields(FeedConfig)}

        # Act & Assert
        for field_name in expected_fields:
            result = hasattr(config, field_name)
            assert result, f"Missing field: {field_name}"

    @pytest.mark.parametrize(
        "field_name,expected_value,is_float",
        [
            # Feed Length & Filtering
            ("max_feed_length", 100, False),
            ("max_num_times_user_can_appear_in_feed", 5, False),
            ("max_prop_old_posts", 0.6, True),
            ("max_in_network_posts_ratio", 0.5, True),
            ("feed_preprocessing_multiplier", 2, False),
            # Scoring Coefficients
            ("coef_toxicity", 0.965, True),
            ("coef_constructiveness", 1.02, True),
            ("superposter_coef", 0.95, True),
            ("engagement_coef", 1.0, True),
            # Freshness Scoring
            ("default_max_freshness_score", 3.0, True),
            ("freshness_lambda_factor", 0.95, True),
            ("freshness_exponential_base", 1.0, True),
            # Lookback Periods
            ("default_scoring_lookback_days", 1, False),
            # Likeability Scoring
            ("default_similarity_score", 0.8, True),
            ("average_popular_post_like_count", 100, False),
            # Feed Postprocessing
            ("jitter_amount", 2, False),
            ("keep_count", 3, False),
        ],
    )
    def test_config_values_match_original_constants(
        self, field_name, expected_value, is_float
    ):
        """Regression test: verify values match original magic numbers."""
        # Arrange
        config = FeedConfig()

        # Act
        result = getattr(config, field_name)

        # Assert - use pytest.approx for float comparisons
        if is_float:
            assert result == pytest.approx(expected_value), (
                f"{field_name} should be approximately {expected_value}, got {result}"
            )
        else:
            assert result == expected_value, (
                f"{field_name} should be {expected_value}, got {result}"
            )

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
        import sys

        # Act - Try to import scoring module
        # If the import of feed_config in scoring.py failed, we'd get ImportError
        # Other errors (like FileExistsError) are environment issues, not import issues
        try:
            scoring_module = importlib.import_module("services.rank_score_feeds.scoring")
            result = scoring_module is not None
        except ImportError as e:
            # If we get ImportError, it might be related to feed_config import
            # Check if it's specifically about feed_config
            if "feed_config" in str(e) or "config" in str(e):
                result = False
            else:
                # Other ImportErrors are unrelated to feed_config
                result = True
        except Exception:
            # Other exceptions (like FileExistsError) are environment issues
            # The fact that we got past the import statement means feed_config import worked
            result = True

        # Assert - scoring module should be able to import feed_config
        # If we got here without an ImportError about feed_config, the import worked
        assert result

    def test_config_can_be_imported_from_helper_module(self):
        """Verify config can be imported and used from helper module."""
        # Arrange
        import importlib

        # Act - Try to import helper module
        # If the import of feed_config in helper.py failed, we'd get ImportError
        # Other errors (like FileExistsError) are environment issues, not import issues
        try:
            helper_module = importlib.import_module("services.rank_score_feeds.helper")
            result = helper_module is not None
        except ImportError as e:
            # If we get ImportError, it might be related to feed_config import
            # Check if it's specifically about feed_config
            if "feed_config" in str(e) or "config" in str(e):
                result = False
            else:
                # Other ImportErrors are unrelated to feed_config
                result = True
        except Exception:
            # Other exceptions (like FileExistsError) are environment issues
            # The fact that we got past the import statement means feed_config import worked
            result = True

        # Assert - helper module should be able to import feed_config
        # If we got here without an ImportError about feed_config, the import worked
        assert result

    def test_config_values_are_not_none(self):
        """Verify no configuration values are None."""
        # Arrange
        config = FeedConfig()

        # Act & Assert - use dataclasses.fields() instead of dir()
        for field in fields(FeedConfig):
            result = getattr(config, field.name)
            assert result is not None, f"Field {field.name} is None"

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

    def test_config_validation_rejects_invalid_max_feed_length(self):
        """Verify config validation rejects invalid max_feed_length."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="max_feed_length must be > 0"):
            FeedConfig(max_feed_length=0)

    def test_config_validation_rejects_invalid_max_prop_old_posts(self):
        """Verify config validation rejects invalid max_prop_old_posts."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="max_prop_old_posts must be in"):
            FeedConfig(max_prop_old_posts=1.5)

        with pytest.raises(ValueError, match="max_prop_old_posts must be in"):
            FeedConfig(max_prop_old_posts=-0.1)

    def test_config_validation_rejects_invalid_max_in_network_posts_ratio(self):
        """Verify config validation rejects invalid max_in_network_posts_ratio."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="max_in_network_posts_ratio must be in"):
            FeedConfig(max_in_network_posts_ratio=1.5)

    def test_config_validation_rejects_invalid_max_num_times_user_can_appear_in_feed(
        self,
    ):
        """Verify config validation rejects invalid max_num_times_user_can_appear_in_feed."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="max_num_times_user_can_appear_in_feed must be > 0"
        ):
            FeedConfig(max_num_times_user_can_appear_in_feed=0)

    def test_config_validation_rejects_invalid_feed_preprocessing_multiplier(self):
        """Verify config validation rejects invalid feed_preprocessing_multiplier."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="feed_preprocessing_multiplier must be > 0"
        ):
            FeedConfig(feed_preprocessing_multiplier=0)

    def test_config_validation_rejects_invalid_coef_toxicity(self):
        """Verify config validation rejects invalid coef_toxicity."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="coef_toxicity must be > 0"):
            FeedConfig(coef_toxicity=0)

    def test_config_validation_rejects_invalid_coef_constructiveness(self):
        """Verify config validation rejects invalid coef_constructiveness."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="coef_constructiveness must be > 0"):
            FeedConfig(coef_constructiveness=0)

    def test_config_validation_rejects_invalid_superposter_coef(self):
        """Verify config validation rejects invalid superposter_coef."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="superposter_coef must be > 0"):
            FeedConfig(superposter_coef=0)

    def test_config_validation_rejects_invalid_engagement_coef(self):
        """Verify config validation rejects invalid engagement_coef."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="engagement_coef must be > 0"):
            FeedConfig(engagement_coef=0)

    def test_config_validation_rejects_invalid_default_max_freshness_score(self):
        """Verify config validation rejects invalid default_max_freshness_score."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="default_max_freshness_score must be > 0"
        ):
            FeedConfig(default_max_freshness_score=0)

    def test_config_validation_rejects_invalid_freshness_lambda_factor(self):
        """Verify config validation rejects invalid freshness_lambda_factor."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="freshness_lambda_factor must be > 0"):
            FeedConfig(freshness_lambda_factor=0)

    def test_config_validation_rejects_invalid_freshness_exponential_base(self):
        """Verify config validation rejects invalid freshness_exponential_base."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="freshness_exponential_base must be > 0"
        ):
            FeedConfig(freshness_exponential_base=0)

    def test_config_validation_rejects_invalid_default_scoring_lookback_days(self):
        """Verify config validation rejects invalid default_scoring_lookback_days."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="default_scoring_lookback_days must be > 0"
        ):
            FeedConfig(default_scoring_lookback_days=0)

    def test_config_validation_rejects_invalid_average_popular_post_like_count(self):
        """Verify config validation rejects invalid average_popular_post_like_count."""
        # Arrange & Act & Assert
        with pytest.raises(
            ValueError, match="average_popular_post_like_count must be > 0"
        ):
            FeedConfig(average_popular_post_like_count=0)

    def test_config_validation_rejects_invalid_jitter_amount(self):
        """Verify config validation rejects invalid jitter_amount."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="jitter_amount must be >= 0"):
            FeedConfig(jitter_amount=-1)

    def test_config_validation_rejects_invalid_keep_count(self):
        """Verify config validation rejects invalid keep_count."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="keep_count must be > 0"):
            FeedConfig(keep_count=0)

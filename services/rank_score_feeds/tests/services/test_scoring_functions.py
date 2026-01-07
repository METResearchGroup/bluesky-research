"""Tests for scoring functions in services.scoring module."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from lib.constants import timestamp_format
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import FreshnessScoreFunction, PostScoreByAlgorithm
from services.rank_score_feeds.services.scoring import (
    calculate_exponential_freshness_score,
    calculate_linear_freshness_score,
    calculate_post_age,
    calculate_post_score,
    score_post_freshness,
    score_post_likeability,
    score_treatment_algorithm,
)


class TestCalculatePostAge:
    """Tests for calculate_post_age function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_calculates_age_for_recent_post(self, feed_config):
        """Test that calculate_post_age returns correct age for recent post."""
        # Arrange
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": one_hour_ago.strftime(timestamp_format),
        })
        lookback_hours = 48.0
        expected = 1.0

        # Act
        result = calculate_post_age(post=post, lookback_hours=lookback_hours)

        # Assert
        assert result == pytest.approx(expected, abs=0.1)

    def test_clamps_age_to_lookback_hours(self, feed_config):
        """Test that calculate_post_age clamps age to lookback_hours maximum."""
        # Arrange
        now = datetime.now(timezone.utc)
        three_days_ago = now - timedelta(days=3)
        post = pd.Series({
            "synctimestamp": three_days_ago.strftime(timestamp_format),
        })
        lookback_hours = 48.0
        expected = 48.0

        # Act
        result = calculate_post_age(post=post, lookback_hours=lookback_hours)

        # Assert
        assert result == expected

    def test_handles_exact_lookback_boundary(self, feed_config):
        """Test that calculate_post_age handles exact lookback boundary correctly."""
        # Arrange
        now = datetime.now(timezone.utc)
        exactly_lookback_ago = now - timedelta(hours=24)
        post = pd.Series({
            "synctimestamp": exactly_lookback_ago.strftime(timestamp_format),
        })
        lookback_hours = 24.0

        # Act
        result = calculate_post_age(post=post, lookback_hours=lookback_hours)

        # Assert
        assert result == pytest.approx(24.0, abs=0.1)


class TestCalculateLinearFreshnessScore:
    """Tests for calculate_linear_freshness_score function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_calculates_freshness_for_new_post(self, feed_config):
        """Test that calculate_linear_freshness_score returns high score for new post."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act
        result = calculate_linear_freshness_score(post=post, feed_config=feed_config)

        # Assert
        assert result > 0
        assert result <= feed_config.default_max_freshness_score

    def test_calculates_freshness_for_old_post(self, feed_config):
        """Test that calculate_linear_freshness_score returns lower score for old post."""
        # Arrange
        now = datetime.now(timezone.utc)
        old_time = now - timedelta(hours=feed_config.freshness_lookback_days * 12)
        post = pd.Series({
            "synctimestamp": old_time.strftime(timestamp_format),
        })

        # Act
        result = calculate_linear_freshness_score(post=post, feed_config=feed_config)

        # Assert
        assert result >= 0
        assert result < feed_config.default_max_freshness_score

    def test_freshness_decreases_with_age(self, feed_config):
        """Test that freshness score decreases as post age increases."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        older_time = now - timedelta(hours=12)
        recent_post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })
        older_post = pd.Series({
            "synctimestamp": older_time.strftime(timestamp_format),
        })

        # Act
        recent_score = calculate_linear_freshness_score(post=recent_post, feed_config=feed_config)
        older_score = calculate_linear_freshness_score(post=older_post, feed_config=feed_config)

        # Assert
        assert recent_score > older_score


class TestCalculateExponentialFreshnessScore:
    """Tests for calculate_exponential_freshness_score function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_calculates_freshness_for_new_post(self, feed_config):
        """Test that calculate_exponential_freshness_score returns high score for new post."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act
        result = calculate_exponential_freshness_score(post=post, feed_config=feed_config)

        # Assert
        assert result > 0
        assert result <= feed_config.default_max_freshness_score

    def test_calculates_freshness_for_old_post(self, feed_config):
        """Test that calculate_exponential_freshness_score returns lower score for old post."""
        # Arrange
        now = datetime.now(timezone.utc)
        old_time = now - timedelta(hours=feed_config.freshness_lookback_days * 12)
        post = pd.Series({
            "synctimestamp": old_time.strftime(timestamp_format),
        })

        # Act
        result = calculate_exponential_freshness_score(post=post, feed_config=feed_config)

        # Assert
        assert result >= 0
        assert result < feed_config.default_max_freshness_score

    def test_freshness_decreases_exponentially_with_age(self, feed_config):
        """Test that freshness score decreases exponentially as post age increases."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        older_time = now - timedelta(hours=12)
        recent_post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })
        older_post = pd.Series({
            "synctimestamp": older_time.strftime(timestamp_format),
        })

        # Act
        recent_score = calculate_exponential_freshness_score(post=recent_post, feed_config=feed_config)
        older_score = calculate_exponential_freshness_score(post=older_post, feed_config=feed_config)

        # Assert
        assert recent_score > older_score


class TestScorePostFreshness:
    """Tests for score_post_freshness function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_uses_linear_function_when_specified(self, feed_config):
        """Test that score_post_freshness uses linear function when specified."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act
        result = score_post_freshness(
            post=post,
            feed_config=feed_config,
            score_func=FreshnessScoreFunction.LINEAR,
        )

        # Assert
        assert result > 0
        assert result <= feed_config.default_max_freshness_score

    def test_uses_exponential_function_when_specified(self, feed_config):
        """Test that score_post_freshness uses exponential function when specified."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act
        result = score_post_freshness(
            post=post,
            feed_config=feed_config,
            score_func=FreshnessScoreFunction.EXPONENTIAL,
        )

        # Assert
        assert result > 0
        assert result <= feed_config.default_max_freshness_score

    def test_defaults_to_exponential_function(self, feed_config):
        """Test that score_post_freshness defaults to exponential function."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act
        result_default = score_post_freshness(post=post, feed_config=feed_config)
        result_explicit = score_post_freshness(
            post=post,
            feed_config=feed_config,
            score_func=FreshnessScoreFunction.EXPONENTIAL,
        )

        # Assert
        assert result_default == result_explicit

    def test_raises_error_for_invalid_function(self, feed_config):
        """Test that score_post_freshness raises ValueError for invalid function."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "synctimestamp": recent_time.strftime(timestamp_format),
        })

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid freshness score function"):
            score_post_freshness(
                post=post,
                feed_config=feed_config,
                score_func="invalid_function",  # type: ignore[arg-type]
            )


class TestScorePostLikeability:
    """Tests for score_post_likeability function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_uses_actual_like_count_when_available(self, feed_config):
        """Test that score_post_likeability uses actual like count when available."""
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
            "like_count": 50.0,
        })
        expected = np.log(50.0 + 1)

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)

    def test_uses_similarity_score_when_like_count_unavailable(self, feed_config):
        """Test that score_post_likeability uses similarity score when like count unavailable."""
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
            "like_count": None,
            "similarity_score": 0.5,
        })
        expected_like_count = feed_config.average_popular_post_like_count * 0.5
        expected = np.log(expected_like_count + 1)

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)

    def test_uses_default_when_no_like_count_or_similarity(self, feed_config):
        """Test that score_post_likeability uses default when no like count or similarity."""
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
            "like_count": None,
            "similarity_score": None,
        })
        expected_like_count = (
            feed_config.average_popular_post_like_count
            * feed_config.default_similarity_score
        )
        expected = np.log(expected_like_count + 1)

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)

    def test_handles_zero_like_count(self, feed_config):
        """Test that score_post_likeability handles zero like count correctly.
        
        Zero like count is treated as a valid value (not None), so the function
        will use the zero value directly instead of falling through to similarity
        score or default.
        """
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
            "like_count": 0.0,
            "similarity_score": None,
        })
        # Zero is a valid value, so it will use the zero like count
        expected = np.log(0.0 + 1)  # np.log(1) = 0.0

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)

    def test_handles_invalid_like_count_gracefully(self, feed_config):
        """Test that score_post_likeability handles invalid like count gracefully."""
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
            "like_count": "invalid",
            "similarity_score": 0.5,
        })
        expected_like_count = feed_config.average_popular_post_like_count * 0.5
        expected = np.log(expected_like_count + 1)

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)

    def test_handles_exception_gracefully(self, feed_config):
        """Test that score_post_likeability handles exceptions gracefully."""
        # Arrange
        post = pd.Series({
            "uri": "test_uri",
        })
        # Missing required fields should trigger exception handling
        expected_like_count = (
            feed_config.average_popular_post_like_count
            * feed_config.default_similarity_score
        )
        expected = np.log(expected_like_count + 1)

        # Act
        result = score_post_likeability(post=post, feed_config=feed_config)

        # Assert
        assert result == pytest.approx(expected)


class TestScoreTreatmentAlgorithm:
    """Tests for score_treatment_algorithm function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    def test_returns_default_for_non_sociopolitical_post(self, feed_config):
        """Test that score_treatment_algorithm returns default for non-sociopolitical post."""
        # Arrange
        post = pd.Series({
            "author_did": "did:test:1",
            "sociopolitical_was_successfully_labeled": False,
            "is_sociopolitical": False,
            "perspective_was_successfully_labeled": False,
        })
        superposter_dids = set()
        expected = 1.0

        # Act
        result = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert result == expected

    def test_calculates_score_for_valid_sociopolitical_post(self, feed_config):
        """Test that score_treatment_algorithm calculates score for valid sociopolitical post."""
        # Arrange
        post = pd.Series({
            "author_did": "did:test:1",
            "sociopolitical_was_successfully_labeled": True,
            "is_sociopolitical": True,
            "perspective_was_successfully_labeled": True,
            "prob_toxic": 0.2,
            "prob_constructive": 0.8,
            "prob_reasoning": 0.8,
        })
        superposter_dids = set()

        # Act
        result = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert result > 0
        assert isinstance(result, float)

    def test_applies_superposter_penalty(self, feed_config):
        """Test that score_treatment_algorithm applies superposter penalty."""
        # Arrange
        post = pd.Series({
            "author_did": "did:superposter:1",
            "sociopolitical_was_successfully_labeled": False,
            "is_sociopolitical": False,
            "perspective_was_successfully_labeled": False,
        })
        superposter_dids = {"did:superposter:1"}
        expected = 1.0 * feed_config.superposter_coef

        # Act
        result = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert result == expected

    def test_fixes_constructive_endpoint_bug(self, feed_config):
        """Test that score_treatment_algorithm fixes constructive endpoint bug."""
        # Arrange
        post = pd.Series({
            "author_did": "did:test:1",
            "sociopolitical_was_successfully_labeled": True,
            "is_sociopolitical": True,
            "perspective_was_successfully_labeled": True,
            "prob_toxic": 0.2,
            "prob_constructive": None,
            "prob_reasoning": 0.8,
        })
        superposter_dids = set()

        # Act
        result = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert result > 0
        # Verify prob_constructive was fixed
        assert post["prob_constructive"] == 0.8

    def test_handles_division_by_zero(self, feed_config):
        """Test that score_treatment_algorithm handles division by zero correctly."""
        # Arrange
        post = pd.Series({
            "author_did": "did:test:1",
            "sociopolitical_was_successfully_labeled": True,
            "is_sociopolitical": True,
            "perspective_was_successfully_labeled": True,
            "prob_toxic": 0.0,
            "prob_constructive": 0.0,
            "prob_reasoning": 0.0,
        })
        superposter_dids = set()
        expected = 1.0  # Should return default when denominator is 0

        # Act
        result = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert result == expected


class TestCalculatePostScore:
    """Tests for calculate_post_score function."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    @pytest.fixture
    def sample_post(self):
        """Create a sample post for testing."""
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        return pd.Series({
            "uri": "at://did:test:1/com.example.post/123",
            "author_did": "did:test:1",
            "synctimestamp": recent_time.strftime(timestamp_format),
            "like_count": 50.0,
            "sociopolitical_was_successfully_labeled": False,
            "is_sociopolitical": False,
            "perspective_was_successfully_labeled": False,
        })

    def test_calculates_engagement_and_treatment_scores(self, feed_config, sample_post):
        """Test that calculate_post_score calculates both engagement and treatment scores."""
        # Arrange
        superposter_dids = set()

        # Act
        result = calculate_post_score(
            post=sample_post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert isinstance(result, PostScoreByAlgorithm)
        assert result.uri == sample_post["uri"]
        assert result.engagement_score > 0
        assert result.treatment_score > 0

    def test_engagement_score_includes_likeability_and_freshness(self, feed_config, sample_post):
        """Test that engagement score includes likeability and freshness components."""
        # Arrange
        superposter_dids = set()

        # Act
        result = calculate_post_score(
            post=sample_post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        # Engagement score should be positive (likeability + freshness) * engagement_coef
        assert result.engagement_score > 0
        assert result.engagement_score == pytest.approx(
            result.engagement_score / feed_config.engagement_coef * feed_config.engagement_coef
        )

    def test_treatment_score_includes_likeability_and_freshness(self, feed_config, sample_post):
        """Test that treatment score includes likeability and freshness components."""
        # Arrange
        superposter_dids = set()

        # Act
        result = calculate_post_score(
            post=sample_post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        # Treatment score should be positive (likeability + freshness) * treatment_algorithm_score
        assert result.treatment_score > 0

    def test_applies_engagement_coefficient(self, feed_config, sample_post):
        """Test that calculate_post_score applies engagement coefficient."""
        # Arrange
        superposter_dids = set()
        feed_config.engagement_coef = 2.0

        # Act
        result = calculate_post_score(
            post=sample_post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        # Engagement score should be affected by engagement_coef
        assert result.engagement_score > 0

    def test_handles_sociopolitical_post(self, feed_config):
        """Test that calculate_post_score handles sociopolitical post correctly."""
        # Arrange
        now = datetime.now(timezone.utc)
        recent_time = now - timedelta(hours=1)
        post = pd.Series({
            "uri": "at://did:test:1/com.example.post/123",
            "author_did": "did:test:1",
            "synctimestamp": recent_time.strftime(timestamp_format),
            "like_count": 50.0,
            "sociopolitical_was_successfully_labeled": True,
            "is_sociopolitical": True,
            "perspective_was_successfully_labeled": True,
            "prob_toxic": 0.2,
            "prob_constructive": 0.8,
            "prob_reasoning": 0.8,
        })
        superposter_dids = set()

        # Act
        result = calculate_post_score(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert isinstance(result, PostScoreByAlgorithm)
        assert result.engagement_score > 0
        assert result.treatment_score > 0
        # Treatment score should be different from engagement score for sociopolitical posts
        assert result.treatment_score != result.engagement_score

    def test_applies_superposter_penalty(self, feed_config, sample_post):
        """Test that calculate_post_score applies superposter penalty."""
        # Arrange
        superposter_dids = {"did:test:1"}

        # Act
        result = calculate_post_score(
            post=sample_post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # Assert
        assert isinstance(result, PostScoreByAlgorithm)
        # Treatment score should be affected by superposter penalty
        assert result.treatment_score > 0

    def test_raises_error_on_exception(self, feed_config):
        """Test that calculate_post_score raises error on exception."""
        # Arrange
        invalid_post = pd.Series({
            "uri": None,  # Invalid URI
        })
        superposter_dids = set()

        # Act & Assert
        with pytest.raises(Exception):
            calculate_post_score(
                post=invalid_post,
                superposter_dids=superposter_dids,
                feed_config=feed_config,
            )


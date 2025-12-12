"""Configuration for rank_score_feeds service.

This module centralizes all configuration constants and magic numbers used
throughout the feed generation algorithm.
"""

from dataclasses import dataclass, field

from lib.constants import default_lookback_days


@dataclass
class FeedConfig:
    """Configuration for feed generation algorithm.

    This dataclass centralizes all configuration values used in the
    rank_score_feeds service, making it easier to understand, maintain,
    and tune the feed generation algorithm.
    """

    # Feed Length & Filtering

    # Maximum number of posts in a generated feed.
    max_feed_length: int = 100

    # Maximum number of times a single user can appear in a feed.
    # This prevents any single author from dominating the feed.
    max_num_times_user_can_appear_in_feed: int = 5

    # Maximum proportion of old posts (previously seen) in a feed.
    # Value between 0.0 and 1.0. Ensures feed always has fresh content.
    max_prop_old_posts: float = 0.6

    # Ratio of feed length to use for in-network posts.
    # Used to calculate max_in_network_posts = max_feed_length * max_in_network_posts_ratio.
    max_in_network_posts_ratio: float = 0.5

    # Multiplier for feed preprocessing length.
    # Used to calculate feed_preprocessing_length = max_feed_length * feed_preprocessing_multiplier.
    # This allows postprocessing to work on a larger candidate set before final truncation.
    feed_preprocessing_multiplier: int = 2

    # Scoring Coefficients

    # Coefficient for toxicity in treatment algorithm scoring.
    # Lower values reduce the weight of toxic content in treatment feeds.
    coef_toxicity: float = 0.965

    # Coefficient for constructiveness in treatment algorithm scoring.
    # Higher values increase the weight of constructive content in treatment feeds.
    coef_constructiveness: float = 1.02

    # Penalty coefficient for posts by superposters.
    # Applied multiplicatively to reduce the score of posts from users who post
    # very frequently. Value between 0.0 and 1.0.
    superposter_coef: float = 0.95

    # Coefficient for engagement scoring.
    # Currently set to 1.0 (no modification). Can be adjusted to tune
    # engagement feed behavior.
    engagement_coef: float = 1.0

    # Freshness Scoring

    # Maximum freshness score for posts.
    # Used as the base score for the freshest posts. Older posts receive
    # lower scores based on decay calculations.
    default_max_freshness_score: float = 3.0

    # Lambda factor for exponential freshness decay.
    # Used in exponential freshness scoring: (base * decay_factor * lambda_factor) ** post_age_hours.
    # Lower values cause faster decay.
    freshness_lambda_factor: float = 0.95

    # Base value for exponential freshness calculation.
    # Used in exponential freshness scoring formula.
    freshness_exponential_base: float = 1.0

    # Lookback Periods

    # Number of days to look back when loading previous post scores.
    # Used to cache and reuse scores for posts that were recently calculated.
    default_scoring_lookback_days: int = 1

    # Likeability Scoring

    # Default similarity score for posts without explicit similarity data.
    # Used when estimating likeability for posts that don't have similarity
    # scores to popular posts.
    default_similarity_score: float = 0.8

    # Average like count for popular posts.
    # Used to estimate expected like counts for posts based on similarity
    # to popular posts.
    average_popular_post_like_count: int = 100

    # Feed Postprocessing

    # Amount of random jitter to apply to feed ordering.
    # Allows experimentation with slight movements in feed order. Posts can
    # be shifted by up to this many positions.
    jitter_amount: int = 2

    # Number of feed files to keep in active storage before moving to cache.
    # Used for S3 TTL (time-to-live) management of generated feeds.
    keep_count: int = 3

    # Derived values (calculated from other config values)

    # Decay ratio for linear freshness scoring.
    # Calculated as: default_max_freshness_score / (default_lookback_days * 24).
    # Automatically computed from other config values.
    freshness_decay_ratio: float = field(init=False)

    def __post_init__(self) -> None:
        """Calculate derived configuration values and validate configuration."""
        # Validate critical configuration values to fail fast on invalid config
        if default_lookback_days <= 0:
            raise ValueError("default_lookback_days must be > 0")
        if self.max_feed_length <= 0:
            raise ValueError("max_feed_length must be > 0")
        if not (0.0 <= self.max_prop_old_posts <= 1.0):
            raise ValueError("max_prop_old_posts must be in [0, 1]")
        if not (0.0 <= self.max_in_network_posts_ratio <= 1.0):
            raise ValueError("max_in_network_posts_ratio must be in [0, 1]")
        if self.max_num_times_user_can_appear_in_feed <= 0:
            raise ValueError("max_num_times_user_can_appear_in_feed must be > 0")
        if self.feed_preprocessing_multiplier <= 0:
            raise ValueError("feed_preprocessing_multiplier must be > 0")
        if self.coef_toxicity <= 0:
            raise ValueError("coef_toxicity must be > 0")
        if self.coef_constructiveness <= 0:
            raise ValueError("coef_constructiveness must be > 0")
        if self.superposter_coef <= 0:
            raise ValueError("superposter_coef must be > 0")
        if self.engagement_coef <= 0:
            raise ValueError("engagement_coef must be > 0")
        if self.default_max_freshness_score <= 0:
            raise ValueError("default_max_freshness_score must be > 0")
        if self.freshness_lambda_factor <= 0:
            raise ValueError("freshness_lambda_factor must be > 0")
        if self.freshness_exponential_base <= 0:
            raise ValueError("freshness_exponential_base must be > 0")
        if self.default_scoring_lookback_days <= 0:
            raise ValueError("default_scoring_lookback_days must be > 0")
        if self.average_popular_post_like_count <= 0:
            raise ValueError("average_popular_post_like_count must be > 0")
        if self.jitter_amount < 0:
            raise ValueError("jitter_amount must be >= 0")
        if self.keep_count <= 0:
            raise ValueError("keep_count must be > 0")

        # Calculate freshness_decay_ratio from max_freshness_score and lookback hours
        default_lookback_hours = default_lookback_days * 24
        self.freshness_decay_ratio = (
            self.default_max_freshness_score / default_lookback_hours
        )


# Module-level default configuration instance
feed_config = FeedConfig()


class FeedConfigFactory:
    """Factory for creating FeedConfig instances.

    This factory class provides a clean interface for creating FeedConfig
    instances, which is useful for dependency injection and testing.
    """

    @staticmethod
    def create_default() -> FeedConfig:
        """Create a FeedConfig instance with default values.

        Returns:
            FeedConfig: A new FeedConfig instance with default values.
        """
        return FeedConfig()

    @staticmethod
    def create(**kwargs) -> FeedConfig:
        """Create a FeedConfig instance with custom values.

        Args:
            **kwargs: Configuration values to override defaults.

        Returns:
            FeedConfig: A new FeedConfig instance with specified values.

        Example:
            >>> config = FeedConfigFactory.create(max_feed_length=200)
            >>> assert config.max_feed_length == 200
        """
        return FeedConfig(**kwargs)

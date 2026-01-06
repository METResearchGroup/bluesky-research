"""Service for re-ranking feeds with business rules."""

import random

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost


class RerankingService:
    """Handles re-ranking with business rules and constraints."""

    def __init__(self, feed_config: FeedConfig):
        """Initialize with feed configuration.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config
        self.max_feed_length: int = self.config.max_feed_length
        self.feed_preprocessing_multiplier: int = (
            self.config.feed_preprocessing_multiplier
        )
        self.max_prop_old_posts: float = self.config.max_prop_old_posts
        self.jitter_amount: int = self.config.jitter_amount

    def rerank_feed(
        self,
        feed: list[CustomFeedPost],
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> list[CustomFeedPost]:
        """Postprocesses the feed using business rules and constraints."""
        postprocessed_feed: list[CustomFeedPost] = (
            self._clip_feed_to_preprocessing_window(feed=feed)
        )
        postprocessed_feed = self._enforce_fresh_content_in_feed(
            feed=postprocessed_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )
        postprocessed_feed = self._truncate_feed(
            feed=postprocessed_feed,
            max_feed_length=self.max_feed_length,
        )
        postprocessed_feed = self._jitter_feed(
            feed=postprocessed_feed, jitter_amount=self.jitter_amount
        )
        self._validate_feed_length(
            feed=postprocessed_feed, max_feed_length=self.max_feed_length
        )

        return postprocessed_feed

    def _clip_feed_to_preprocessing_window(
        self, feed: list[CustomFeedPost]
    ) -> list[CustomFeedPost]:
        """Clips the feed to the preprocessing window.

        We clip the feed to some multiple (>1) of the max feed length, so that
        we can do postprocessing on a larger candidate set (but not the full feed,
        as the end values of the feed tend to be lower value anyways)
        before final truncation and business logic.
        """
        return feed[: int(self.max_feed_length * self.feed_preprocessing_multiplier)]

    def _enforce_fresh_content_in_feed(
        self, feed: list[CustomFeedPost], uris_of_posts_used_in_previous_feeds: set[str]
    ) -> list[CustomFeedPost]:
        """Enforces fresh content in the feed.

        We ensure that there's a maximum % of old posts in the feed, so we
        always have some fresh content.
        """
        if not uris_of_posts_used_in_previous_feeds:
            return feed

        max_num_old_posts = int(self.max_feed_length * self.max_prop_old_posts)
        old_post_count = 0
        processed_feed: list[CustomFeedPost] = []

        for post in feed:
            # Case 1: Always include fresh posts.
            if post.item not in uris_of_posts_used_in_previous_feeds:
                processed_feed.append(post)
                continue

            # Case 2: Include old posts up to the max limit.
            if old_post_count < max_num_old_posts:
                old_post_count += 1
                processed_feed.append(post)

        return processed_feed

    def _truncate_feed(
        self, feed: list[CustomFeedPost], max_feed_length: int
    ) -> list[CustomFeedPost]:
        """Truncates the feed to the max feed length."""
        return feed[:max_feed_length]

    def _validate_feed_length(
        self, feed: list[CustomFeedPost], max_feed_length: int
    ) -> None:
        """Validates the feed.

        Given the filtering, the max length is the max_feed_length, and we
        want to be flagged if it's less than that (it can't be more than that).
        """
        if len(feed) < max_feed_length:
            raise ValueError(
                f"Feed length is less than max_feed_length: {len(feed)} < {max_feed_length}"
            )

    def _jitter_feed(
        self, feed: list[CustomFeedPost], jitter_amount: int
    ) -> list[CustomFeedPost]:
        """Jitters the feed by a random amount.

        This lets us experiment with slight movements in feed order,
        controlled by `jitter_amount`.
        """
        n = len(feed)
        result = feed.copy()
        # Iterate backwards to avoid index shifting issues when modifying list in-place
        for i in range(n - 1, -1, -1):
            shift = random.randint(-jitter_amount, jitter_amount)
            new_pos = max(0, min(n - 1, i + shift))
            if i != new_pos:
                result.insert(new_pos, result.pop(i))
        return result

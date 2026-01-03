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

    # TODO: refactor this.
    def rerank_feed(
        self,
        feed: list[CustomFeedPost],
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> list[CustomFeedPost]:
        """Postprocesses the feed."""
        # do feed postprocessing on a subset of the feed to save time.
        max_feed_length: int = self.config.max_feed_length
        feed_preprocessing_multiplier: int = self.config.feed_preprocessing_multiplier
        max_prop_old_posts: float = self.config.max_prop_old_posts
        jitter_amount: int = self.config.jitter_amount

        feed = feed[: int(max_feed_length * feed_preprocessing_multiplier)]

        # ensure that there's a maximum % of old posts in the feed, so we
        # always have some fresh content.
        if uris_of_posts_used_in_previous_feeds:
            max_num_old_posts = int(max_feed_length * max_prop_old_posts)
            old_post_count = 0
            processed_feed = []
            for post in feed:
                if post.item in uris_of_posts_used_in_previous_feeds:
                    if old_post_count < max_num_old_posts:
                        old_post_count += 1
                        processed_feed.append(post)
                else:
                    processed_feed.append(post)
            feed = processed_feed

        # truncate feed
        feed = feed[:max_feed_length]

        # jitter feed to slightly shuffle ordering
        feed = self._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # validate feed lengths:
        if len(feed) != max_feed_length:
            raise ValueError(
                f"Feed length is not equal to max_feed_length: {len(feed)} != {max_feed_length}"
            )

        return feed

    def _jitter_feed(
        self, feed: list[CustomFeedPost], jitter_amount: int
    ) -> list[CustomFeedPost]:
        """Jitters the feed by a random amount.

        This lets us experiment with slight movements in feed order,
        controlled by `jitter_amount`.
        """
        n = len(feed)
        result = feed.copy()
        for i in range(n):
            shift = random.randint(-jitter_amount, jitter_amount)
            new_pos = max(0, min(n - 1, i + shift))
            if i != new_pos:
                result.insert(new_pos, result.pop(i))
        return result

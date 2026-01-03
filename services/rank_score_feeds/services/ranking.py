"""Service for ranking candidates."""

import pandas as pd

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost


class RankingService:
    """Handles initial ranking of candidates."""

    def __init__(self, feed_config: FeedConfig):
        """Initialize with feed configuration.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config

    def create_ranked_candidate_feed(
        self,
        condition: str,
        in_network_candidate_post_uris: list[str],
        post_pool: pd.DataFrame,
        max_feed_length: int,
        max_in_network_posts_ratio: float,
    ) -> list[CustomFeedPost]:
        """Create a ranked candidate feed.

        Returns a list of dicts of post URIs, their scores, and whether or
        not the post is in-network.

        Priorities for posts:
        1. In-network posts
        2. Out-of-network most-liked posts
        """
        if post_pool is None or len(post_pool) == 0:
            raise ValueError(
                "post_pool cannot be None. This means that a user condition is unexpected/invalid"
            )  # noqa
        in_network_posts_df = post_pool[
            post_pool["uri"].isin(in_network_candidate_post_uris)
        ]
        out_of_network_source = (
            "most_liked"
            if condition in ["engagement", "representative_diversification"]
            else "firehose"
        )
        out_of_network_posts_df = post_pool[
            (~post_pool["uri"].isin(in_network_candidate_post_uris))
            & (post_pool["source"] == out_of_network_source)
        ]

        # get the number of in-network posts to include.
        total_in_network_posts = len(in_network_posts_df)
        max_in_network_posts = int(max_feed_length * max_in_network_posts_ratio)
        max_allowed_in_network_posts = min(total_in_network_posts, max_in_network_posts)
        in_network_posts_df = in_network_posts_df.iloc[:max_allowed_in_network_posts]

        # do edge-casing
        if len(post_pool) == 0:
            return []
        if len(in_network_candidate_post_uris) == 0:
            # if no in-network posts, return the out-of-network posts.
            feed = [
                CustomFeedPost(item=str(post["uri"]), is_in_network=False)
                for _, post in out_of_network_posts_df.iterrows()
            ]
            return feed
        in_network_post_set = set(in_network_posts_df["uri"].tolist())

        # Combine in-network and out-of-network posts while maintaining order
        output_posts_df = pd.concat([in_network_posts_df, out_of_network_posts_df])

        feed: list[CustomFeedPost] = [
            CustomFeedPost(
                item=str(post["uri"]), is_in_network=post["uri"] in in_network_post_set
            )
            for _, post in output_posts_df.iterrows()
        ]

        return feed

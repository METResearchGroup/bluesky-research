"""Service for generating candidate pools."""

import pandas as pd

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CandidatePostPools


class CandidateGenerationService:
    """Generates and manages candidate pools for ranking."""

    def __init__(self, feed_config: FeedConfig):
        """Initialize with feed configuration.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config

    def generate_candidate_pools(
        self,
        posts_df: pd.DataFrame,
    ) -> CandidatePostPools:
        """Generate three candidate pools (reverse_chronological, engagement, treatment).

        Filters the posts df and then creates sorted versions of each.

        Args:
            posts_df: DataFrame with scored posts.

        Returns:
            PostPools containing the three sorted and filtered candidate pools.
        """
        filtered_posts_df: pd.DataFrame = self._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=self.config.max_num_times_user_can_appear_in_feed,
        )

        # TODO: I can imagine an implementation where we just add the sorted rankings
        # as 3 columns in the dataframe, and then we can keep 1 copy of the
        # dataframe and slice as needed downstream, rather than doing multiple copies.
        # We will revisit that possible implementation later.

        reverse_chronological_candidate_pool_df: pd.DataFrame = filtered_posts_df[
            filtered_posts_df["source"] == "firehose"
        ].sort_values(by="synctimestamp", ascending=False)  # type: ignore[call-overload]

        engagement_candidate_pool_df: pd.DataFrame = filtered_posts_df.sort_values(
            by="engagement_score", ascending=False
        )

        treatment_candidate_pool_df: pd.DataFrame = filtered_posts_df.sort_values(
            by="treatment_score", ascending=False
        )

        return CandidatePostPools(
            reverse_chronological=reverse_chronological_candidate_pool_df,
            engagement=engagement_candidate_pool_df,
            treatment=treatment_candidate_pool_df,
        )

    def _filter_posts_by_author_count(
        self, posts_df: pd.DataFrame, max_count: int
    ) -> pd.DataFrame:
        """Returns the first X rows of each author, filtering out the rest."""
        return posts_df.groupby("author_did").head(max_count)

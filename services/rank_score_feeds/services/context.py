"""Service for building user context for personalization."""

import pandas as pd

from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.helper import calculate_in_network_posts_for_user


class UserContextService:
    """Builds user-specific context for personalization."""

    def build_in_network_context(
        self,
        scored_posts: pd.DataFrame,
        study_users: list[UserToBlueskyProfileModel],
        user_to_social_network_map: dict[str, list[str]],
    ) -> dict[str, list[str]]:
        """Calculate in-network post URIs for each user.

        Args:
            scored_posts: DataFrame with all scored posts.
            study_users: List of study users to build context for.
            user_to_social_network_map: Mapping of user DIDs to their social network.

        Returns:
            Dictionary mapping user_did to list of in-network post URIs.
        """

        curated_baseline_in_network_posts_df: pd.DataFrame = (
            self._curate_baseline_in_network_posts(posts_df=scored_posts)
        )

        # Get lists of in-network and out-of-network posts
        user_to_in_network_post_uris_map: dict[str, list[str]] = {
            user.bluesky_user_did: calculate_in_network_posts_for_user(
                user_did=user.bluesky_user_did,
                user_to_social_network_map=user_to_social_network_map,
                candidate_in_network_user_activity_posts_df=(
                    candidate_in_network_user_activity_posts_df  # type: ignore[arg-type]
                ),
            )
            for user in study_users
        }
        return user_to_in_network_post_uris_map

    def _curate_baseline_in_network_posts(self, posts_df: pd.DataFrame) -> pd.DataFrame:
        """Curate the baseline in-network posts.

        For now, we only treat firehose posts as possible in-network
        posts. We will revisit this later. This is because in our sync pipeilne,
        we explicitly look for and save posts that are from accounts followed by
        users in the study. We have two sources of posts:
        - firehose: a general sync of all posts from all accounts on Bluesky
        (though filtered for only posts from accounts we "care about", e.g., posts
        from accounts followed by users in the study).
        - most_liked: a sync of posts from the most liked accounts on Bluesky.

        We know that posts in the "most_liked" source can also be in-network. This
        can happen if a user follows a popular account, in which case that
        user's posts would be in BOTH the firehose and the most_liked sources.
        Therefore, to deduplicate, we can keep the posts from just the firehose,
        knowing that any liked posts from the "most liked" source will also be in
        the firehose source as well.

        Args:
            posts_df: DataFrame with all scored posts.

        Returns:
            DataFrame with curated baseline in-network posts.
        """
        mask = posts_df["source"] == "firehose"
        curated_df: pd.DataFrame = posts_df.loc[mask]
        return curated_df

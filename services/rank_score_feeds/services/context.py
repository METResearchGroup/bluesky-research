"""Service for building user context for personalization."""

import pandas as pd

from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.models import UserInNetworkPostsMap


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

        # define what posts can be used in the calculation of in-network posts.
        curated_baseline_in_network_posts_df: pd.DataFrame = (
            self._curate_baseline_in_network_posts(posts_df=scored_posts)
        )

        # calculate in-network posts for each user.
        user_to_in_network_post_uris_map: UserInNetworkPostsMap = self._calculate_in_network_posts_for_users(
            curated_baseline_in_network_posts_df=curated_baseline_in_network_posts_df,
            user_to_social_network_map=user_to_social_network_map,
            study_users=study_users,
        )

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

    def _calculate_in_network_posts_for_users(
        self,
        curated_baseline_in_network_posts_df: pd.DataFrame,
        user_to_social_network_map: dict[str, list[str]],
        study_users: list[UserToBlueskyProfileModel],
    ) -> UserInNetworkPostsMap:
        """Calculate in-network post URIs for each user.

        Args:
            curated_baseline_in_network_posts_df: DataFrame with curated baseline in-network posts.
            user_to_social_network_map: Mapping of user DIDs to their social network.
            study_users: List of study users to build context for.

        Returns:
            UserInNetworkPostsMap mapping user_did to list of in-network post URIs.
        """
        user_to_in_network_post_uris_map: UserInNetworkPostsMap = {
            user.bluesky_user_did: self._calculate_in_network_posts_for_user(
                user_did=user.bluesky_user_did,
                user_to_social_network_map=user_to_social_network_map,
                curated_baseline_in_network_posts_df=curated_baseline_in_network_posts_df,
            )
            for user in study_users
        }
        return user_to_in_network_post_uris_map

    def _calculate_in_network_posts_for_user(
        self,
        user_did: str,
        user_to_social_network_map: dict,
        curated_baseline_in_network_posts_df: pd.DataFrame,  # noqa
    ) -> list[str]:
        """Calculates the possible in-network and out-of-network posts.

        Loops through the posts and if it is from the most liked feed, add as out-of-network,
        otherwise it will be checked against the user's social network to see if
        that post was written by someone in that user's social network.
        """
        # get the followee/follower DIDs for the user's social network.
        # This should only be empty if the user doesn't follow anyone (which is
        # possible and has been observed) or if their social network hasn't been
        # synced yet.
        in_network_social_network_dids = user_to_social_network_map.get(user_did, [])  # noqa
        # filter the candidate in-network user activity posts to only include the
        # ones that are in the user's social network.
        in_network_post_uris: list[str] = curated_baseline_in_network_posts_df[
            curated_baseline_in_network_posts_df["author_did"].isin(
                in_network_social_network_dids
            )
        ]["uri"].tolist()
        return in_network_post_uris

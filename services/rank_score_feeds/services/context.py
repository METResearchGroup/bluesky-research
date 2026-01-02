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
        # TODO: Implement
        raise NotImplementedError

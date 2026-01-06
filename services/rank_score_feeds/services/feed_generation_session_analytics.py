from lib.constants import STUDY_CONDITIONS
from services.rank_score_feeds.models import (
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)


class FeedGenerationSessionAnalyticsService:
    def __init__(self):
        from lib.log.logger import get_logger

        self.logger = get_logger(__name__)

    def calculate_feed_generation_session_analytics(
        self,
        user_to_ranked_feed_map: dict[str, FeedWithMetadata],
        session_timestamp: str,
    ) -> FeedGenerationSessionAnalytics:
        """Calculates analytics for a given user to ranked feed map."""
        session_analytics: dict = {}

        analytics_across_all_feeds: dict = self._calculate_analytics_across_all_feeds(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
        )
        total_feeds_per_condition: dict = self._calculate_total_feeds_per_condition(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
        )
        analytics_for_prop_overlap_engagement_treatment_uris: dict = (
            self._calculate_analytics_for_prop_overlap_engagement_treatment_uris(
                user_to_ranked_feed_map=user_to_ranked_feed_map,
            )
        )

        session_analytics.update(analytics_across_all_feeds)
        session_analytics.update(total_feeds_per_condition)
        session_analytics.update(analytics_for_prop_overlap_engagement_treatment_uris)

        return FeedGenerationSessionAnalytics(
            total_feeds=session_analytics["total_feeds"],
            total_posts=session_analytics["total_posts"],
            total_in_network_posts=session_analytics["total_in_network_posts"],
            total_in_network_posts_prop=session_analytics[
                "total_in_network_posts_prop"
            ],
            total_unique_engagement_uris=session_analytics[
                "total_unique_engagement_uris"
            ],
            total_unique_treatment_uris=session_analytics[
                "total_unique_treatment_uris"
            ],
            prop_overlap_treatment_uris_in_engagement_uris=session_analytics[
                "prop_overlap_treatment_uris_in_engagement_uris"
            ],
            prop_overlap_engagement_uris_in_treatment_uris=session_analytics[
                "prop_overlap_engagement_uris_in_treatment_uris"
            ],
            total_feeds_per_condition=session_analytics["total_feeds_per_condition"],
            session_timestamp=session_timestamp,
        )

    def _calculate_analytics_across_all_feeds(
        self,
        user_to_ranked_feed_map: dict[str, FeedWithMetadata],
    ) -> dict:
        """Calculates analytics across all feeds."""
        analytics_across_all_feeds: dict = {}
        total_feeds: int = len(user_to_ranked_feed_map)
        total_posts: int = 0
        total_in_network_posts: int = 0

        for _, feed in user_to_ranked_feed_map.items():
            total_posts += len(feed.feed)
            total_in_network_posts += sum([post.is_in_network for post in feed.feed])

        total_in_network_posts_prop: float = (
            round(total_in_network_posts / total_posts, 2) if total_posts > 0 else 0.0
        )
        analytics_across_all_feeds["total_feeds"] = total_feeds
        analytics_across_all_feeds["total_posts"] = total_posts
        analytics_across_all_feeds["total_in_network_posts"] = total_in_network_posts
        analytics_across_all_feeds["total_in_network_posts_prop"] = (
            total_in_network_posts_prop
        )
        return analytics_across_all_feeds

    def _calculate_total_feeds_per_condition(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata]
    ) -> dict:
        """ "Performs analytics based on the condition of the feed."""
        total_feeds_per_condition: dict[str, int] = {}
        for condition in STUDY_CONDITIONS:
            total_feeds_per_condition[condition] = sum(
                [
                    feed.condition == condition
                    for feed in user_to_ranked_feed_map.values()
                ]
            )
        return {"total_feeds_per_condition": total_feeds_per_condition}

    def _calculate_analytics_for_prop_overlap_engagement_treatment_uris(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata]
    ) -> dict:
        """Calculate analytics related to looking at the portion of overlap between the engagement and treatment feeds."""
        analytics_for_prop_overlap_engagement_treatment_uris: dict = {}
        engagement_feed_uris: set[str] = set()
        treatment_feed_uris: set[str] = set()

        for _, feed in user_to_ranked_feed_map.items():
            if feed.condition == "engagement":
                engagement_feed_uris.update([post.item for post in feed.feed])
            elif feed.condition == "representative_diversification":
                treatment_feed_uris.update([post.item for post in feed.feed])

        # Find post URIs that appear in both the engagement and treatment feeds.
        # set.intersection(other_set) returns elements common to both sets.
        overlap_engagement_treatment_uris: set[str] = engagement_feed_uris.intersection(
            treatment_feed_uris
        )

        total_unique_engagement_uris: int = len(engagement_feed_uris)
        total_unique_treatment_uris: int = len(treatment_feed_uris)
        total_unique_overlap_engagement_treatment_uris: int = len(
            overlap_engagement_treatment_uris
        )

        # proportion of the treatment URIs that are also in the engagement URIs.
        prop_treatment_uris_in_engagement_uris: float = (
            round(
                total_unique_overlap_engagement_treatment_uris
                / total_unique_treatment_uris,
                3,
            )
            if total_unique_treatment_uris > 0
            else 0.0
        )

        # proportion of the engagement URIs that are also in the treatment URIs.
        prop_engagement_uris_in_treatment_uris: float = (
            round(
                total_unique_overlap_engagement_treatment_uris
                / total_unique_engagement_uris,
                3,
            )
            if total_unique_engagement_uris > 0
            else 0.0
        )

        analytics_for_prop_overlap_engagement_treatment_uris[
            "total_unique_engagement_uris"
        ] = total_unique_engagement_uris
        analytics_for_prop_overlap_engagement_treatment_uris[
            "total_unique_treatment_uris"
        ] = total_unique_treatment_uris
        analytics_for_prop_overlap_engagement_treatment_uris[
            "prop_overlap_treatment_uris_in_engagement_uris"
        ] = prop_treatment_uris_in_engagement_uris
        analytics_for_prop_overlap_engagement_treatment_uris[
            "prop_overlap_engagement_uris_in_treatment_uris"
        ] = prop_engagement_uris_in_treatment_uris

        return analytics_for_prop_overlap_engagement_treatment_uris

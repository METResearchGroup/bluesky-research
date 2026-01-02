from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
import pandas as pd
from lib.db.manage_local_data import export_data_to_local_storage

logger = get_logger(__name__)

class FeedGenerationSessionAnalyticsService:
    def __init__(self):
        pass


    def calculate_feed_analytics(
        self,
        user_to_ranked_feed_map: dict[str, dict],
        timestamp: str,
    ) -> dict:
        """Calculates analytics for a given user to ranked feed map."""
        session_analytics: dict = {}
        session_analytics["total_feeds"] = len(user_to_ranked_feed_map)
        session_analytics["total_posts"] = sum(
            [len(feed["feed"]) for feed in user_to_ranked_feed_map.values()]
        )  # noqa
        session_analytics["total_in_network_posts"] = sum(
            [
                sum([post.is_in_network for post in feed["feed"]])
                for feed in user_to_ranked_feed_map.values()
            ]
        )  # noqa
        session_analytics["total_in_network_posts_prop"] = (
            round(
                session_analytics["total_in_network_posts"]
                / session_analytics["total_posts"],
                2,
            )
            if session_analytics["total_posts"] > 0
            else 0.0
        )
        engagement_feed_uris: list[str] = [
            post.item
            for feed in user_to_ranked_feed_map.values()
            if feed["condition"] == "engagement"
            for post in feed["feed"]  # noqa
        ]
        treatment_feed_uris: list[str] = [
            post.item
            for feed in user_to_ranked_feed_map.values()
            if feed["condition"] == "representative_diversification"
            for post in feed["feed"]  # noqa
        ]
        overlap_engagement_uris_in_treatment_uris = set(engagement_feed_uris).intersection(
            set(treatment_feed_uris)
        )
        overlap_treatment_uris_in_engagement_uris = set(treatment_feed_uris).intersection(
            set(engagement_feed_uris)
        )
        total_unique_engagement_uris = len(set(engagement_feed_uris))
        total_unique_treatment_uris = len(set(treatment_feed_uris))
        prop_treatment_uris_in_engagement_uris = (
            round(
                len(overlap_treatment_uris_in_engagement_uris)
                / (total_unique_treatment_uris + 1),  # to avoid division by zero
                3,
            )
            if total_unique_treatment_uris > 0
            else 0.0
        )
        prop_engagement_uris_in_treatment_uris = (
            round(
                len(overlap_engagement_uris_in_treatment_uris)
                / (total_unique_engagement_uris + 1),  # to avoid division by zero
                3,
            )
            if total_unique_treatment_uris > 0
            else 0.0
        )
        session_analytics["total_unique_engagement_uris"] = total_unique_engagement_uris
        session_analytics["total_unique_treatment_uris"] = total_unique_treatment_uris
        session_analytics["prop_overlap_treatment_uris_in_engagement_uris"] = (
            prop_treatment_uris_in_engagement_uris
        )
        session_analytics["prop_overlap_engagement_uris_in_treatment_uris"] = (
            prop_engagement_uris_in_treatment_uris
        )
        session_analytics["total_feeds_per_condition"] = {}
        for condition in [
            "representative_diversification",
            "engagement",
            "reverse_chronological",
        ]:
            session_analytics["total_feeds_per_condition"][condition] = sum(
                [
                    feed["condition"] == condition
                    for feed in user_to_ranked_feed_map.values()
                ]
            )
        session_analytics["session_timestamp"] = timestamp
        return session_analytics

    def export_feed_analytics(self, analytics: dict) -> None:
        """Exports feed analytics to S3."""
        dtype_map = MAP_SERVICE_TO_METADATA["feed_analytics"]["dtypes_map"]
        df = pd.DataFrame([analytics])
        df = df.astype(dtype_map)
        export_data_to_local_storage(df=df, service="feed_analytics")
        logger.info("Exported session feed analytics.")

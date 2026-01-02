import json

from services.rank_score_feeds.models import CustomFeedPost

class FeedStatisticsService:
    """Service for calculating feed statistics."""

    def __init__(self):
        """Initialize."""

    def generate_feed_statistics(self, feed: list[CustomFeedPost]) -> str:
        """Generates statistics about a given feed."""
        feed_length = len(feed)
        total_in_network = sum([post.is_in_network for post in feed])
        prop_in_network = (
            round(total_in_network / feed_length, 3) if feed_length > 0 else 0.0
        )
        feed_statistics: dict[str, float] = {
            "prop_in_network": prop_in_network,
            "total_in_network": total_in_network,
            "feed_length": feed_length,
        }
        # needs to be JSON-dumped string for external storage.
        return json.dumps(feed_statistics)

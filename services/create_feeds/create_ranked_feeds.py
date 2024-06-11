"""Rank candidates posts for the feed of each user.

Returns the latest feeds per user, rank ordered based on the criteria in their
study condition.
"""
from services.create_feeds.condition.engagement import create_engagement_feeds
from services.create_feeds.condition.representative_diversification import (
    create_representative_diversification_feeds
)
from services.create_feeds.condition.reverse_chronological import (
    create_reverse_chronological_feeds
)
from services.create_feeds.load_data import (
    load_firehose_based_posts, load_most_liked_based_posts,
    load_perspective_api_labels
)
from services.ml_inference.models import RecordClassificationMetadataModel


def create_firehose_based_feeds(
        condition_to_user_map: dict
) -> list[RecordClassificationMetadataModel]:
    """Create feeds that are based off posts from the firehose."""
    reverse_chronological_users = condition_to_user_map["reverse_chronological"]
    if len(reverse_chronological_users) > 0:
        posts = load_firehose_based_posts()  # Classified posts that are from the firehose
        feeds = create_reverse_chronological_feeds(posts)
        return feeds
    else:
        return []


def create_most_liked_feeds(
        condition_to_user_map: dict
) -> list[RecordClassificationMetadataModel]:
    """Create feeds that are based off posts from the most liked feeds."""
    output_feeds = []

    engagement_users = condition_to_user_map["engagement"]
    representative_diversification_users = condition_to_user_map["representative_diversification"]

    if len(engagement_users) > 0 or len(representative_diversification_users) > 0:
        posts = load_most_liked_based_posts()  # classified posts that are from the most liked feed
        if len(engagement_users) > 0:
            feeds = create_engagement_feeds(engagement_users, posts)
            output_feeds.extend(feeds)
        if len(representative_diversification_users) > 0:
            labels = load_perspective_api_labels(posts)  # load Perspective API labels
            feeds = create_representative_diversification_feeds(
                representative_diversification_users, posts, labels
            )
            output_feeds.extend(feeds)
    else:
        return []


def create_feeds_per_condition(condition_to_user_map: dict):
    output_feeds = []

    firehose_feeds = create_firehose_based_feeds(condition_to_user_map)
    output_feeds.extend(firehose_feeds)

    most_liked_feeds = create_most_liked_feeds(condition_to_user_map)
    output_feeds.extend(most_liked_feeds)

    return output_feeds


if __name__ == "__main__":
    pass
